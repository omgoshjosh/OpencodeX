import type { SDKUserMessage } from "@anthropic-ai/claude-agent-sdk"
import type { ClaudeEvent } from "./claude-mapper"
import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "claude-channel" })

/**
 * Persistent per-session Claude query channels (OpencodeX-div).
 *
 * The per-turn architecture spawned a fresh CLI child with `--resume` for
 * every turn. Resuming a conversation whose previous child died mid-turn made
 * the CLI emit an immediate close-out `result` for the dangling turn before
 * the new prompt's own events; the daemon read it as the new turn's
 * completion, tore the query down at +1s, and the child ran the real turn as
 * an orphan whose every canUseTool round trip aborted with "Stream closed"
 * (live capture 2026-08-22 16:35 UTC).
 *
 * A channel keeps one streaming-input query alive per session. Turns push a
 * user message and attach a sink; events route to the active turn. Two
 * structural guards kill the stale-result class:
 *
 * - a `result` arriving when no turn is active is dropped (logged), never
 *   surfaced as anyone's completion;
 * - a `result` arriving before the first assistant output after a resume
 *   spawn is dropped as the close-out of a previous dangling turn.
 *
 * The CLI child also survives between turns, so background tasks and the
 * permission control channel stop dying at turn boundaries.
 *
 * The channel is deliberately ignorant of what the handlers do: the SDK query
 * reads the current turn's handlers through `handlers()`, and between turns
 * there are none (the transport denies control requests arriving then).
 */

/** What the channel needs from the SDK query it wraps. */
export type ChannelQuery = {
  events: AsyncIterable<ClaudeEvent>
  interrupt: () => Promise<void>
  /** Hard teardown: abort the controller backing the query. */
  abort: () => void
}

export type CreateQuery<H> = (input: {
  prompt: AsyncIterable<SDKUserMessage>
  handlers: () => H | undefined
  /** Conversation to resume, present only when the channel picks up an old one. */
  resumeID?: string
}) => Promise<ChannelQuery>

/** A push-driven AsyncIterable the SDK consumes as its prompt stream. */
export function createPushable<T>() {
  const queue: T[] = []
  let notify: (() => void) | undefined
  let ended = false
  return {
    push(value: T) {
      queue.push(value)
      notify?.()
    },
    end() {
      ended = true
      notify?.()
    },
    iterable: (async function* () {
      while (true) {
        while (queue.length > 0) yield queue.shift()!
        if (ended) return
        await new Promise<void>((resolve) => {
          notify = () => {
            notify = undefined
            resolve()
          }
        })
      }
    })(),
  }
}

function eventType(event: ClaudeEvent) {
  return (event as { type?: string }).type ?? ""
}

function isAssistantOutput(event: ClaudeEvent) {
  const type = eventType(event)
  return type === "assistant" || type === "stream_event"
}

/**
 * A result the CLI reports as a clean completion. Error results carry the
 * failure the turn needs to surface (rate limit, usage limit, upstream 5xx,
 * invalid model, rejected resume) and are never treated as stale.
 */
function isSuccessCloseOut(event: ClaudeEvent) {
  if (eventType(event) !== "result") return false
  const result = event as { subtype?: string; is_error?: boolean }
  if (result.is_error === true) return false
  return result.subtype === undefined || result.subtype === "success"
}

type TurnSink = {
  push: (event: ClaudeEvent) => void
  end: (failure?: { failure: unknown }) => void
  sawAssistantOutput: boolean
}

export class Channel<H> {
  private sink: TurnSink | undefined
  private queryPromise: Promise<ChannelQuery>
  private input = createPushable<SDKUserMessage>()
  private handlers: H | undefined
  /**
   * True until the first assistant output after a resume spawn: only inside
   * this window can the CLI's close-out result for a previous dangling turn
   * be mistaken for a live completion.
   */
  private settling: boolean
  /** True once the spawned CLI reported `system.init` (resume accepted). */
  private sawInit = false
  dead = false

  private readonly interruptGraceMs: number
  private readonly closeOutGraceMs: number
  private closeOutWatchdog: ReturnType<typeof setTimeout> | undefined
  /** Input reached EOF (for native images); finish this query, never reuse it. */
  retiring = false

  /** True while a turn is attached; an idle reaper must not close one. */
  get busy() {
    return this.sink !== undefined
  }

  constructor(
    readonly key: string,
    createQuery: CreateQuery<H>,
    options?: { resumeID?: string; interruptGraceMs?: number; closeOutGraceMs?: number },
  ) {
    this.interruptGraceMs = options?.interruptGraceMs ?? INTERRUPT_GRACE_MS
    this.closeOutGraceMs = options?.closeOutGraceMs ?? CLOSE_OUT_GRACE_MS
    this.settling = options?.resumeID !== undefined
    log.info("claude channel created", { channel: key, resumed: this.settling })
    this.queryPromise = createQuery({
      prompt: this.input.iterable,
      handlers: () => this.handlers,
      ...(options?.resumeID ? { resumeID: options.resumeID } : {}),
    })
    void this.pump()
  }

  private async pump() {
    let failure: { failure: unknown } | undefined
    try {
      const query = await this.queryPromise
      for await (const event of query.events) this.route(event)
    } catch (error) {
      failure = { failure: error }
    }
    this.dead = true
    log.info("claude channel pump ended", {
      channel: this.key,
      failed: failure !== undefined,
      ...(failure
        ? { error: failure.failure instanceof Error ? failure.failure.message : String(failure.failure) }
        : {}),
    })
    const sink = this.sink
    this.sink = undefined
    sink?.end(failure)
  }

  private route(event: ClaudeEvent) {
    if (eventType(event) === "system" && (event as { subtype?: string }).subtype === "init") this.sawInit = true
    const sink = this.sink
    if (!sink) {
      // No turn is consuming. A result here is exactly the stale close-out
      // this channel exists to contain; everything else between turns is
      // background chatter nothing downstream is keyed to receive.
      if (eventType(event) === "result") log.info("dropped out-of-turn result event", { channel: this.key })
      return
    }
    if (this.settling && this.sawInit && !sink.sawAssistantOutput && isSuccessCloseOut(event)) {
      // Post-resume close-out of a dangling previous turn: after a successful
      // init it always precedes the new turn's own assistant output.
      // Forwarding it ended real turns at +1s with an empty transcript.
      //
      // Only a *successful* close-out is dropped. A result before init, or any
      // error result, is always forwarded: that is how a rejected resume, an
      // auth failure, a rate/usage limit, an upstream 5xx, and an invalid
      // model all report themselves, and each arrives in exactly this window
      // (init is emitted at process start). The mapper's recovery paths -
      // resumeRejected, needs-login, the turn error itself - depend on seeing
      // them.
      log.info("dropped post-resume close-out result", { channel: this.key })
      // A drop must never be able to park the turn forever. If the CLI was in
      // fact done and nothing further arrives, the streaming input keeps the
      // child alive, so `pump` never ends and the driver would hold the
      // session's execution lease indefinitely. Fail the turn instead.
      this.armCloseOutWatchdog(sink)
      return
    }
    if (isAssistantOutput(event)) {
      sink.sawAssistantOutput = true
      this.settling = false
      this.clearCloseOutWatchdog()
    }
    sink.push(event)
  }

  /**
   * Bounds the blind spot the close-out guard opens: if a dropped close-out
   * was not followed by real output, end the turn with a failure so the
   * driver's event loop unparks and the execution lease is released.
   */
  private armCloseOutWatchdog(sink: TurnSink) {
    if (this.closeOutWatchdog) return
    this.closeOutWatchdog = setTimeout(() => {
      this.closeOutWatchdog = undefined
      if (this.sink !== sink || sink.sawAssistantOutput) return
      log.warn("no output after dropped close-out; closing the channel", { channel: this.key })
      // The channel goes down with the turn, not just the turn. The child may
      // still be alive and mid-answer; detaching alone would let its output
      // land in the *next* turn's sink - the previous prompt's answer
      // attributed to the new one - and would leave `settling` armed so every
      // later bare close-out costs another grace window. The next turn
      // respawns and resumes, so the only cost is one process start.
      sink.end({ failure: new Error("Claude produced no output for this turn.") })
      void this.close()
    }, this.closeOutGraceMs)
  }

  private clearCloseOutWatchdog() {
    if (!this.closeOutWatchdog) return
    clearTimeout(this.closeOutWatchdog)
    this.closeOutWatchdog = undefined
  }

  /**
   * Runs one turn: pushes the user message(s) and yields routed events until
   * the caller stops consuming (the driver breaks once the mapper reports the
   * turn finished) or the underlying query dies.
   */
  turn(messages: SDKUserMessage[], handlers: H, options?: { closeInput?: boolean }) {
    if (this.dead) throw new Error("The Claude channel is closed.")
    if (this.retiring) throw new Error("The Claude channel is retiring.")
    if (this.sink) throw new Error("A turn is already active on this Claude channel.")
    this.handlers = handlers

    const buffered: ClaudeEvent[] = []
    let notify: (() => void) | undefined
    let ended: { failure?: unknown } | undefined
    const sink: TurnSink = {
      sawAssistantOutput: false,
      push: (event) => {
        buffered.push(event)
        notify?.()
      },
      end: (failure) => {
        ended = failure ? { failure: failure.failure } : {}
        notify?.()
      },
    }
    this.sink = sink

    for (const message of messages) this.input.push(message)
    // Claude CLI 2.1.228 only preserves native image blocks when the
    // streaming-input source reaches EOF after the message. Text turns keep
    // the channel open; image turns close it and the next turn resumes on a
    // fresh channel.
    if (options?.closeInput) {
      this.retiring = true
      this.input.end()
    }

    const detach = () => {
      this.clearCloseOutWatchdog()
      if (this.sink === sink) {
        this.sink = undefined
        this.handlers = undefined
      }
    }

    const state: TurnState = {
      next: () => {
        const value = buffered.shift()
        if (value !== undefined) return { value }
        return ended ?? undefined
      },
      wait: () =>
        new Promise<void>((resolve) => {
          notify = () => {
            notify = undefined
            resolve()
          }
        }),
      detach,
    }

    return {
      events: turnEvents(state),
      interrupt: async () => {
        // Interrupt the turn, not the channel: the child survives for the
        // session's next turn. But a child too wedged to acknowledge the
        // interrupt (the failure class this channel exists for) must not hold
        // the session hostage: if the turn is still attached after the grace
        // window, the whole channel is torn down and the next turn respawns
        // with resume.
        const query = await this.queryPromise.catch(() => undefined)
        await query?.interrupt().catch(() => undefined)
        setTimeout(() => {
          if (this.sink === sink && !this.dead) {
            log.warn("claude channel unresponsive to interrupt; closing", { channel: this.key })
            void this.close()
          }
        }, this.interruptGraceMs)
      },
    }
  }

  async close() {
    this.dead = true
    this.clearCloseOutWatchdog()
    this.input.end()
    const sink = this.sink
    this.sink = undefined
    this.handlers = undefined
    const query = await this.queryPromise.catch(() => undefined)
    query?.abort()
    sink?.end()
  }
}

const INTERRUPT_GRACE_MS = 10_000
/**
 * How long a turn may produce nothing after its close-out was dropped. Long
 * enough that a slow first token is never mistaken for a wedge, short enough
 * that a session's execution lease is not held for the process lifetime.
 */
const CLOSE_OUT_GRACE_MS = 120_000
/**
 * How long a channel may sit with no turn before its CLI child is reclaimed.
 * A later turn simply respawns and resumes the conversation, so the only cost
 * of reaping early is one process start.
 */
const IDLE_TTL_MS = 15 * 60_000
const IDLE_SWEEP_MS = 60_000

/** One live turn's consumption surface, closed over by {@link turnEvents}. */
type TurnState = {
  /** A buffered event, the end marker (`{failure?}`), or nothing yet. */
  next: () => { value: ClaudeEvent } | { failure?: unknown } | undefined
  wait: () => Promise<void>
  detach: () => void
}

async function* turnEvents(state: TurnState): AsyncGenerator<ClaudeEvent> {
  try {
    while (true) {
      const step = state.next()
      if (step === undefined) {
        await state.wait()
        continue
      }
      if ("value" in step) {
        yield step.value
        continue
      }
      if ("failure" in step) throw step.failure
      return
    }
  } finally {
    state.detach()
  }
}

/**
 * Session-keyed registry. A turn whose config no longer matches the session's
 * live channel (model, effort, cwd, roster change) recycles it: the old child
 * is torn down and a fresh one resumes the same conversation.
 */
export function createChannelRegistry<H>(options?: { idleTtlMs?: number; sweepMs?: number; now?: () => number }) {
  const idleTtlMs = options?.idleTtlMs ?? IDLE_TTL_MS
  const sweepMs = options?.sweepMs ?? IDLE_SWEEP_MS
  const now = options?.now ?? (() => Date.now())
  const channels = new Map<string, { channel: Channel<H>; config: string; idleSince: number }>()
  // Serializes acquires per key: two overlapping acquires must never both
  // decide to create, or the loser's live CLI child leaks unreachable.
  const pending = new Map<string, Promise<unknown>>()
  // Each live channel owns a CLI child process for the lifetime of a
  // long-running backend, so an idle one has to be reclaimed rather than kept
  // on the chance the session speaks again. Started with the first channel and
  // stopped when the last one goes, so an idle process holds no timer.
  let sweeper: ReturnType<typeof setInterval> | undefined

  const evict = (sessionKey: string) => {
    const entry = channels.get(sessionKey)
    if (!entry) return undefined
    channels.delete(sessionKey)
    if (channels.size === 0) stopSweeper()
    return entry.channel
  }

  function stopSweeper() {
    if (!sweeper) return
    clearInterval(sweeper)
    sweeper = undefined
  }

  function startSweeper() {
    if (sweeper || idleTtlMs === Infinity) return
    sweeper = setInterval(() => void sweep(), sweepMs)
    sweeper.unref?.()
  }

  async function sweep() {
    const deadline = now() - idleTtlMs
    const expired: Channel<H>[] = []
    for (const [key, entry] of channels) {
      // A channel mid-turn is never idle, however long the turn runs.
      if (entry.channel.busy) {
        entry.idleSince = now()
        continue
      }
      if (!entry.channel.dead && entry.idleSince > deadline) continue
      channels.delete(key)
      expired.push(entry.channel)
    }
    if (channels.size === 0) stopSweeper()
    if (expired.length === 0) return
    log.info("reclaimed idle claude channels", { count: expired.length })
    await Promise.all(expired.map((channel) => channel.close().catch(() => undefined)))
  }

  return {
    get: (sessionKey: string) => channels.get(sessionKey)?.channel,
    /** Live channel count; the reaper's observable surface for tests. */
    size: () => channels.size,
    /**
     * Runs one reap pass immediately. The interval that normally drives this
     * is `unref`'d so an idle process is not held open, and an `unref`'d timer
     * is not reliably scheduled everywhere - so nothing, tests included,
     * should wait on it firing.
     */
    sweepNow: () => sweep(),
    acquire(
      sessionKey: string,
      config: string,
      createQuery: CreateQuery<H>,
      channelOptions?: { resumeID?: string },
    ): Promise<Channel<H>> {
      const acquire = async () => {
        const existing = channels.get(sessionKey)
        if (existing && !existing.channel.dead && !existing.channel.retiring && existing.config === config) {
          existing.idleSince = now()
          return existing.channel
        }
        if (existing) {
          evict(sessionKey)
          await existing.channel.close().catch(() => undefined)
        }
        const channel = new Channel<H>(sessionKey, createQuery, channelOptions)
        channels.set(sessionKey, { channel, config, idleSince: now() })
        startSweeper()
        return channel
      }
      const chained = (pending.get(sessionKey) ?? Promise.resolve()).then(acquire, acquire)
      const settled = chained.catch(() => undefined)
      pending.set(sessionKey, settled)
      // Without this the map grows one entry per session forever, holding the
      // last acquire's result alive with it.
      void settled.then(() => {
        if (pending.get(sessionKey) === settled) pending.delete(sessionKey)
      })
      return chained
    },
    /** Drops one session's channel - for session deletion. */
    async close(sessionKey: string) {
      const channel = evict(sessionKey)
      await channel?.close().catch(() => undefined)
    },
    /**
     * Reclaims every channel that is not mid-turn. Callers include instance
     * disposal, which also runs on a global config change and a plugin install
     * - not just shutdown - so a live turn is deliberately left alone rather
     * than truncated into a silent clean completion. Anything skipped here is
     * still reclaimed by the idle sweep once its turn ends.
     */
    async closeAll() {
      const open: Channel<H>[] = []
      for (const [key, entry] of channels) {
        if (entry.channel.busy) continue
        channels.delete(key)
        open.push(entry.channel)
      }
      // Unconditionally, even if a busy channel was skipped: this is a
      // teardown, and leaving a repeating timer behind keeps the process
      // alive. The next acquire restarts the sweeper, and a skipped channel
      // is re-examined then.
      stopSweeper()
      await Promise.all(open.map((channel) => channel.close().catch(() => undefined)))
    },
  }
}

export type ChannelRegistry<H> = ReturnType<typeof createChannelRegistry<H>>

export * as ClaudeChannel from "./claude-channel"
