import { stat } from "node:fs/promises"
import path from "node:path"
import z from "zod"
import type { SDKUserMessage } from "@anthropic-ai/claude-agent-sdk"
import type { ClaudeEvent } from "./claude-mapper"
import { ClaudeDelegate } from "./claude-delegate"
import { createChannelRegistry, type CreateQuery } from "./claude-channel"
import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "claude-transport" })

/**
 * Orphan-approval adoption (OpencodeX-sxp). A cross-session message delivered
 * to an idle daemon-hosted session wakes the CLI child through the harness
 * queue with no daemon turn attached, so every approval-gated tool call it
 * makes reaches `canUseTool` with no handlers and used to be denied "No turn
 * is active" - a selective wedge (allowlisted calls kept working) that read
 * as "the repo is broken". Instead of denying, the channel keeps the previous
 * turn's `adoptOrphan` (bound to that turn's instance context) and uses it to
 * open a real, visibly marked turn on the session, holding the approval until
 * the new turn's handlers attach. Invariant this restores: a wake that can
 * execute tools owns a turn.
 */
export type OrphanAdopter = (input: { toolName: string }) => Promise<void>

/** Two adoptions closer than this on one channel mean the adoption turn did not stay open (tripwire, logged). */
const ORPHAN_ADOPTION_TRIPWIRE_MS = 60_000
const ORPHAN_ADOPTION_WAIT_MS = 30_000
let orphanAdoptionCount = 0

/** How many orphan approvals were adopted in this process - the standing argument for the root fix (Layer A). */
export function orphanApprovalAdoptions() {
  return orphanAdoptionCount
}

/** Per-channel adoption state: one episode in flight at a time, shared by parallel tool calls. */
export type OrphanAdoptionState = { inflight?: Promise<boolean>; lastAt?: number }

/**
 * Asks the session layer for a turn, then waits for the channel's handlers to
 * appear. Resolves false when there is no adopter, the request was aborted,
 * or the turn never attached; parallel calls join the in-flight episode.
 */
export function adoptOrphanApproval(input: {
  sessionID: string
  toolName: string
  adopt: OrphanAdopter | undefined
  handlers: () => unknown
  state: OrphanAdoptionState
  signal?: AbortSignal
  waitMs?: number
}): Promise<boolean> {
  if (!input.adopt || input.signal?.aborted) return Promise.resolve(false)
  if (input.state.inflight) return input.state.inflight
  const episode = (async () => {
    const started = Date.now()
    const sinceLast = input.state.lastAt === undefined ? undefined : started - input.state.lastAt
    orphanAdoptionCount += 1
    log.warn("adopting orphan approval into a new turn", {
      sessionID: input.sessionID,
      toolName: input.toolName,
      adoptions: orphanAdoptionCount,
      ...(sinceLast !== undefined && sinceLast < ORPHAN_ADOPTION_TRIPWIRE_MS
        ? { tripwire: true, sinceLastMs: sinceLast }
        : {}),
    })
    try {
      await input.adopt!({ toolName: input.toolName })
    } catch (error) {
      log.error("orphan approval adoption failed", { sessionID: input.sessionID, error: String(error) })
      return false
    }
    const deadline = Date.now() + (input.waitMs ?? ORPHAN_ADOPTION_WAIT_MS)
    while (Date.now() < deadline) {
      if (input.handlers()) return true
      if (input.signal?.aborted) return false
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    log.warn("orphan approval adoption timed out waiting for a turn", { sessionID: input.sessionID })
    return false
  })()
  input.state.inflight = episode
  void episode.finally(() => {
    input.state.inflight = undefined
    input.state.lastAt = Date.now()
  })
  return episode
}

/**
 * The process boundary for headless Claude Code. Everything SDK-specific lives
 * here so the driver only ever sees plain stream-json events; swapping to a raw
 * `claude -p --output-format stream-json` spawn means reimplementing this file
 * and nothing else.
 */

export type PermissionDecision = { allow: true; input?: Record<string, unknown> } | { allow: false; message: string }

export type TransportOptions = {
  cwd: string
  /**
   * A conversation id Claude itself issued on an earlier turn, to resume in
   * place. Omitted on the first turn: Claude mints the id, and resuming one it
   * never issued fails the whole turn with `error_during_execution`.
   */
  resumeID?: string
  executable?: string
  /**
   * A value from `supportedModels()` (`"sonnet"`, `"claude-fable-5[1m]"`, ...).
   * Omitted, or `"default"`, leaves the CLI on its configured model.
   */
  model?: string
  /** Reasoning effort, surfaced in OpencodeX as the model's variant chip. */
  effort?: string
  canUseTool: (
    toolName: string,
    input: Record<string, unknown>,
    toolUseID?: string,
    signal?: AbortSignal,
  ) => Promise<PermissionDecision>
  /**
   * Swarm delegation. When present the CLI is given an in-process tool that
   * hands work back to OpencodeX, so specialists run as OpencodeX sessions on
   * their configured models instead of as Claude's own internal subagents.
   */
  delegate?: DelegateCapability
  /**
   * Opens a real turn on the session when the CLI asks for an approval with
   * no turn attached (a peer-message wake of an idle session). Kept by the
   * persistent channel after the turn ends, so it is bound to this turn's
   * instance context.
   */
  adoptOrphan?: OrphanAdopter
  signal?: AbortSignal
  /**
   * Keys a persistent per-session query channel (normally the daemon session
   * id). Present, the transport keeps one CLI child alive across the
   * session's turns instead of spawning one per turn with `--resume` — the
   * fix for the stale close-out result a resume spawn can emit
   * (OpencodeX-div). Absent, each run is its own query (the historical
   * behavior, kept for one-shot callers and as the
   * OPENCODE_CLAUDE_PER_TURN_QUERY=1 escape hatch).
   */
  channelKey?: string
}

/** The per-turn callbacks a persistent channel's fixed SDK query reads. */
export type TurnHandlers = {
  canUseTool: (
    toolName: string,
    input: Record<string, unknown>,
    extra?: { toolUseID?: string; signal?: AbortSignal },
  ) => Promise<{ behavior: "allow"; updatedInput: Record<string, unknown> } | { behavior: "deny"; message: string }>
  delegate?: DelegateCapability
  correlator: DelegateCorrelator
  /** Opens a real turn on this session when a gated call arrives with no turn attached (OpencodeX-sxp). */
  adoptOrphan?: OrphanAdopter
}

export type DelegateCapability = {
  roles: Array<{ name: string; description?: string }>
  /**
   * `toolUseID` is the orchestrator's own tool call id for this delegation,
   * present whenever the permission gate saw the call. It is what the parent
   * transcript keys its tool part by, so it is what lets a delegation link to
   * the session it spawned. `signal` is the SDK request's abort signal.
   */
  run: (input: {
    role: string
    prompt: string
    toolUseID?: string
    background?: boolean
    signal?: AbortSignal
  }) => Promise<ClaudeDelegate.Result>
}

/** The delegate tool's arguments, as both the gate and the handler see them. */
export type DelegateArgs = { role: string; prompt: string }

/**
 * The CLI hands an in-process MCP tool its arguments but not the tool call id
 * the transcript is keyed by. `canUseTool` sees both, and always resolves
 * before the CLI executes the tool (the same ordering `decidedInputs` in
 * claude-driver.ts relies on), so the id is recorded there and claimed here.
 *
 * Identical arguments queue FIFO, so an orchestrator that fans the same role
 * and prompt out twice in one turn still gives each call a distinct id rather
 * than pointing both at the first.
 *
 * Known, accepted limit: the correlator lives for one turn (created per
 * `run`), so staleness cannot cross turns - but within a turn, an allowed
 * call that never executes leaves its id queued, and a later byte-identical
 * retry claims the stale id first. Worst case is a wrong drill-down link,
 * never a broken delegation; FIFO stays because the common case is parallel
 * fan-out, where it is the only correct order.
 */
export function createDelegateCorrelator() {
  const pending = new Map<string, string[]>()
  const key = (args: DelegateArgs) => JSON.stringify([args.role, args.prompt])
  return {
    record(input: Record<string, unknown>, toolUseID?: string) {
      if (!toolUseID) return
      const { role, prompt } = input
      if (typeof role !== "string" || typeof prompt !== "string") return
      const queue = pending.get(key({ role, prompt }))
      if (queue) queue.push(toolUseID)
      else pending.set(key({ role, prompt }), [toolUseID])
    },
    claim(args: DelegateArgs) {
      const queue = pending.get(key(args))
      const toolUseID = queue?.shift()
      if (queue && queue.length === 0) pending.delete(key(args))
      return toolUseID
    },
  }
}

export type DelegateCorrelator = ReturnType<typeof createDelegateCorrelator>

/** Namespaced by the CLI as `mcp__<server>__<tool>`. */
export const DELEGATE_SERVER = "opencodex_swarm"
export const DELEGATE_TOOL = "delegate"
export const DELEGATE_TOOL_NAME = `mcp__${DELEGATE_SERVER}__${DELEGATE_TOOL}`

/** One row of the CLI's own model menu, as reported by `supportedModels()`. */
export type ClaudeModelInfo = {
  value: string
  displayName?: string
  description?: string
  resolvedModel?: string
  supportsEffort?: boolean
  supportedEffortLevels?: string[]
}

export const EFFORT_LEVELS = ["low", "medium", "high", "xhigh", "max"]

export type TransportTurn = {
  /** Ordered stream-json events for one prompt. */
  events: AsyncIterable<ClaudeEvent>
  interrupt: () => Promise<void>
}

export type ClaudeImage = {
  type: "image"
  source: {
    type: "base64"
    media_type: "image/jpeg" | "image/png" | "image/gif" | "image/webp"
    data: string
  }
}

export type ClaudePrompt = string | ReadonlyArray<{ type: "text"; text: string } | ClaudeImage>

export interface ClaudeTransport {
  /** Runs one prompt to completion, yielding events as they arrive. */
  run: (prompt: ClaudePrompt, options: TransportOptions) => TransportTurn
}

export class ClaudeNotInstalledError extends Error {
  readonly _tag = "ClaudeNotInstalledError"
}

/** The CLI's own "leave it alone" row; never forwarded as an explicit model. */
export const DEFAULT_MODEL_VALUE = "default"

/**
 * Asks the installed CLI which models this account may use. The menu is
 * host-specific - context-window suffixes like `opus[1m]` depend on the plan -
 * so the catalog is discovered rather than hard-coded.
 *
 * The prompt stream intentionally never yields: the process starts, answers the
 * control request, and is torn down without running a completion.
 */
export async function listSupportedModels(input?: { executable?: string; cwd?: string; signal?: AbortSignal }) {
  const sdk = await import("@anthropic-ai/claude-agent-sdk").catch(() => undefined)
  if (!sdk?.query) throw new ClaudeNotInstalledError("The Claude Code SDK is unavailable.")
  const executable = input?.executable ?? (await resolveClaudeExecutable())
  if (!executable) throw new ClaudeNotInstalledError("Claude Code is not installed.")
  const controller = new AbortController()
  if (input?.signal) {
    if (input.signal.aborted) controller.abort()
    else input.signal.addEventListener("abort", () => controller.abort(), { once: true })
  }
  const running = sdk.query({
    prompt: idlePrompt(),
    options: {
      cwd: input?.cwd ?? process.cwd(),
      abortController: controller,
      pathToClaudeCodeExecutable: executable,
    },
  } as Parameters<typeof sdk.query>[0]) as unknown as {
    supportedModels?: () => Promise<ClaudeModelInfo[]>
    interrupt?: () => Promise<void>
  }
  try {
    return (await running.supportedModels?.()) ?? []
  } finally {
    controller.abort()
    await running.interrupt?.().catch(() => undefined)
  }
}

async function* idlePrompt(): AsyncGenerator<never> {
  await new Promise(() => undefined)
}

/**
 * Module-scoped: the driver layer (and with it `createSdkTransport`) is
 * rebuilt per prompt-runtime scope, and a registry created per transport
 * instance would start every turn empty — respawning a CLI child per turn and
 * abandoning the previous one, which is exactly the lifecycle the persistent
 * channels exist to end. Session ids are globally unique, so one process-wide
 * registry is safe across instances.
 */
const persistentChannels = createChannelRegistry<TurnHandlers>()

/**
 * Reclaims every live CLI child. Each channel owns one, and a backend that
 * serves many sessions over a long life would otherwise accumulate them for
 * its whole lifetime: an idle reaper handles the quiet ones, and this is the
 * deterministic hook for instance disposal and shutdown. A later turn respawns
 * and resumes, so calling this is never destructive to a conversation.
 */
export function closeAllPersistentChannels() {
  return persistentChannels.closeAll()
}

/** Drops one session's channel, for session deletion. */
export function closePersistentChannel(sessionKey: string) {
  return persistentChannels.close(sessionKey)
}

export function createSdkTransport(): ClaudeTransport {
  const registry = persistentChannels

  function turnHandlers(options: TransportOptions): TurnHandlers {
    const correlator = createDelegateCorrelator()
    return {
      canUseTool: (toolName, input, extra) => resolveToolPermission(options, toolName, input, extra, correlator),
      ...(options.delegate ? { delegate: options.delegate } : {}),
      ...(options.adoptOrphan ? { adoptOrphan: options.adoptOrphan } : {}),
      correlator,
    }
  }

  function runPersistent(prompt: ClaudePrompt, options: TransportOptions, channelKey: string): TransportTurn {
    let active: ReturnType<import("./claude-channel").Channel<TurnHandlers>["turn"]> | undefined
    // An interrupt can land while the generator is still importing the SDK or
    // acquiring the channel; the flag makes it stick so the cancelled prompt
    // is never pushed into the persistent child as a headless turn.
    let cancelled = false

    async function* events(): AsyncGenerator<ClaudeEvent> {
      const sdk = await import("@anthropic-ai/claude-agent-sdk").catch(() => undefined)
      if (!sdk?.query) throw new ClaudeNotInstalledError("The Claude Code SDK is unavailable.")
      const executable = options.executable ?? (await resolveClaudeExecutable())

      const createQuery: CreateQuery<TurnHandlers> = async (input) => {
        const controller = new AbortController()
        const adoption: OrphanAdoptionState = {}
        const delegation = options.delegate
          ? delegateServer(sdk, options.delegate.roles, () => {
              const handlers = input.handlers()
              return handlers?.delegate ? { run: handlers.delegate.run, correlator: handlers.correlator } : undefined
            })
          : undefined
        const running = sdk.query({
          prompt: input.prompt,
          options: {
            cwd: options.cwd,
            abortController: controller,
            includePartialMessages: true,
            forwardSubagentText: true,
            permissionMode: "default",
            canUseTool: async (
              toolName: string,
              toolInput: Record<string, unknown>,
              extra: { toolUseID?: string; signal?: AbortSignal },
            ) => {
              let handlers = input.handlers()
              if (
                !handlers &&
                (await adoptOrphanApproval({
                  sessionID: channelKey,
                  toolName,
                  adopt: input.lastHandlers()?.adoptOrphan,
                  handlers: input.handlers,
                  state: adoption,
                  ...(extra.signal ? { signal: extra.signal } : {}),
                }))
              ) {
                handlers = input.handlers()
              }
              if (!handlers) {
                // The CLI is asking for an approval while the daemon has no
                // turn attached: proof the two turn lifecycles are desynced
                // (the daemon finished a turn the CLI is still executing).
                // The wedge this produces is selective - allowlisted calls
                // never reach here and keep working - which made it read as
                // "the repo is broken" for days while this path emitted no
                // log at all. Count every occurrence.
                log.warn("denied approval with no active turn", { channelKey, toolName })
                return { behavior: "deny" as const, message: "No turn is active for this session." }
              }
              return handlers.canUseTool(toolName, toolInput, extra)
            },
            ...(input.resumeID ? { resume: input.resumeID } : {}),
            ...(executable ? { pathToClaudeCodeExecutable: executable } : {}),
            ...(options.model && options.model !== DEFAULT_MODEL_VALUE ? { model: options.model } : {}),
            ...(options.effort && EFFORT_LEVELS.includes(options.effort) ? { effort: options.effort } : {}),
            ...(delegation ? { mcpServers: { [DELEGATE_SERVER]: delegation } } : {}),
          },
        } as Parameters<typeof sdk.query>[0]) as AsyncIterable<ClaudeEvent> & { interrupt?: () => Promise<unknown> }
        return {
          events: running,
          interrupt: async () => {
            await running.interrupt?.()
          },
          abort: () => controller.abort(),
        }
      }

      // Everything the spawned child is pinned to; a change recycles the
      // channel so the next child resumes the conversation with the new shape.
      const config = JSON.stringify([
        options.cwd,
        options.model ?? "",
        options.effort ?? "",
        executable ?? "",
        options.delegate?.roles ?? null,
      ])
      const channel = await registry.acquire(
        channelKey,
        config,
        createQuery,
        options.resumeID ? { resumeID: options.resumeID } : undefined,
      )
      if (cancelled || options.signal?.aborted) return
      // Never close the channel's input for an image turn. The EOF trick
      // (commit 9b48394dc) was for CLI 2.1.228, which dropped native image
      // blocks unless streaming input hit EOF - but stdin is also the
      // canUseTool response path, so it silently killed every approval-gated
      // tool call in the turn ("Stream closed"; live capture 2026-08-28).
      // Verified 2026-08-28 against CLI 2.1.247: images are preserved with the
      // input left open. Minimum CLI for image turns is therefore 2.1.247.
      active = channel.turn([userMessage(prompt)], turnHandlers(options))
      if (cancelled) {
        // The interrupt raced the turn start: unwind what was just attached.
        await active.interrupt()
      }
      yield* active.events
    }

    if (options.signal) {
      if (options.signal.aborted) cancelled = true
      else
        options.signal.addEventListener(
          "abort",
          () => {
            cancelled = true
            void active?.interrupt()
          },
          { once: true },
        )
    }

    return {
      events: events(),
      interrupt: async () => {
        cancelled = true
        await active?.interrupt()
      },
    }
  }

  return {
    run(prompt, options) {
      if (options.channelKey && process.env.OPENCODE_CLAUDE_PER_TURN_QUERY !== "1")
        return runPersistent(prompt, options, options.channelKey)
      const controller = new AbortController()
      if (options.signal) {
        if (options.signal.aborted) controller.abort()
        else options.signal.addEventListener("abort", () => controller.abort(), { once: true })
      }
      // Only `interrupt` is used here, and its result is discarded.
      let query: { interrupt?: () => Promise<unknown> } | undefined
      // Shared by the permission gate (which sees the tool call id) and the
      // delegate tool handler (which does not).
      const correlator = createDelegateCorrelator()

      async function* events(): AsyncIterable<ClaudeEvent> {
        const sdk = await import("@anthropic-ai/claude-agent-sdk").catch(() => undefined)
        if (!sdk?.query) throw new ClaudeNotInstalledError("The Claude Code SDK is unavailable.")
        const executable = options.executable ?? (await resolveClaudeExecutable())
        const delegation = options.delegate
          ? delegateServer(sdk, options.delegate.roles, () => ({ run: options.delegate!.run, correlator }))
          : undefined
        const running = sdk.query({
          prompt: sdkPrompt(prompt),
          options: {
            cwd: options.cwd,
            abortController: controller,
            // Deltas stream text as it is generated. Beyond live streaming, this
            // is the recovery path for prose the final assistant events lose
            // (2026-08-09 spec, Part B finding 3).
            includePartialMessages: true,
            // Without this, a subagent's own prose (text/thinking) never reaches
            // the stream at all - only its tool calls do. Forwarding tags that
            // output with parent_tool_use_id so the sidechain router can project
            // it into the child session's transcript instead of losing it.
            forwardSubagentText: true,
            // OpencodeX is the sole permission gate: Claude defers every tool
            // decision to canUseTool, which bridges to OpencodeX permission cards.
            permissionMode: "default",
            canUseTool: (toolName, input, extra) => resolveToolPermission(options, toolName, input, extra, correlator),
            ...(options.resumeID ? { resume: options.resumeID } : {}),
            ...(executable ? { pathToClaudeCodeExecutable: executable } : {}),
            ...(options.model && options.model !== DEFAULT_MODEL_VALUE ? { model: options.model } : {}),
            ...(options.effort && EFFORT_LEVELS.includes(options.effort) ? { effort: options.effort } : {}),
            ...(delegation ? { mcpServers: { [DELEGATE_SERVER]: delegation } } : {}),
          },
        } as Parameters<typeof sdk.query>[0])
        query = running
        for await (const message of running) yield message as ClaudeEvent
      }

      return {
        events: events(),
        interrupt: async () => {
          controller.abort()
          await query?.interrupt?.().catch(() => undefined)
        },
      }
    },
  }
}

/** Native content requires the SDK's streaming user-message input form. */
export function sdkPrompt(prompt: ClaudePrompt): string | AsyncIterable<SDKUserMessage> {
  if (typeof prompt === "string") return prompt
  return userPrompt(prompt)
}

/** One turn's prompt as the streaming user message a persistent channel pushes. */
export function userMessage(prompt: ClaudePrompt): SDKUserMessage {
  return {
    type: "user",
    message: { role: "user", content: typeof prompt === "string" ? prompt : [...prompt] },
    parent_tool_use_id: null,
  } as SDKUserMessage
}

async function* userPrompt(content: Exclude<ClaudePrompt, string>): AsyncGenerator<SDKUserMessage> {
  yield {
    type: "user",
    message: { role: "user", content: [...content] },
    parent_tool_use_id: null,
  }
}

export async function resolveToolPermission(
  options: TransportOptions,
  toolName: string,
  input: Record<string, unknown>,
  extra?: { toolUseID?: string; signal?: AbortSignal },
  correlator?: DelegateCorrelator,
) {
  // Claude's own subagents would run inside the CLI on its models,
  // bypassing the swarm's per-role routing. Redirect to the tool that hands
  // work back to OpencodeX.
  if (options.delegate && toolName === "Task") {
    return {
      behavior: "deny" as const,
      message: `Use ${DELEGATE_TOOL_NAME} to delegate swarm roles; the Task tool is unavailable in a swarm session.`,
    }
  }
  const decision = await options.canUseTool(toolName, input, extra?.toolUseID, extra?.signal)
  if (!decision.allow) return { behavior: "deny" as const, message: decision.message }
  const updatedInput = decision.input ?? input
  // Recorded only once the call is going to run, and against the input the
  // tool will actually receive, so the handler's own arguments are what it
  // looks the id up by.
  if (toolName === DELEGATE_TOOL_NAME) correlator?.record(updatedInput, extra?.toolUseID)
  return { behavior: "allow" as const, updatedInput }
}

/**
 * An in-process MCP server carrying the one tool a swarm orchestrator needs.
 * The handler runs inside OpencodeX, so a delegated role becomes an OpencodeX
 * subagent session on its configured model - visible in the transcript and
 * governed by OpencodeX permissions - rather than a Claude-internal subagent.
 */
export function delegateServer(
  sdk: typeof import("@anthropic-ai/claude-agent-sdk"),
  roles: DelegateCapability["roles"],
  /**
   * The current turn's delegate run + correlator. The MCP server is fixed at
   * query spawn, but on a persistent channel each turn brings a fresh bridge
   * and correlator, so the handler reads them through this accessor; between
   * turns (or on a turn without delegation) it returns undefined and the call
   * fails as an errored delegation instead of crossing turn boundaries.
   */
  current: () => { run: DelegateCapability["run"]; correlator?: DelegateCorrelator } | undefined,
) {
  const roster = roles.map((role) => `- ${role.name}${role.description ? `: ${role.description}` : ""}`).join("\n")
  return sdk.createSdkMcpServer({
    name: DELEGATE_SERVER,
    version: "1.0.0",
    instructions: `Delegate swarm roles back to OpencodeX. Available roles:\n${roster}`,
    alwaysLoad: true,
    tools: [
      sdk.tool(
        DELEGATE_TOOL,
        [
          "Delegate a task to one of this swarm's specialist roles.",
          "The role runs as its own OpencodeX session on the model configured for it.",
          "With background=true the call returns at once and the role's report is",
          "delivered to you later as a message - prefer this for anything that may",
          "take more than a minute (builds, CI, long reviews) so this session stays",
          "responsive and the human can keep talking to you. Without it the call",
          "blocks until the role finishes. Prefer several calls in one turn for",
          "independent roles.",
          "",
          "Roles:",
          roster,
        ].join("\n"),
        {
          role: z.string().describe("Exact role name from the roster."),
          prompt: z
            .string()
            .describe("Self-contained instructions: scope, expected output, and whether files may be edited."),
          background: z
            .boolean()
            .optional()
            .describe("Return immediately; the report arrives later as a message. Do not poll for it."),
        },
        async (args, extra) => {
          try {
            const turn = current()
            if (!turn) {
              return {
                isError: true,
                content: [
                  { type: "text" as const, text: ClaudeDelegate.failureMessage(ClaudeDelegate.failure("errored")) },
                ],
              }
            }
            const toolUseID = turn.correlator?.claim(args)
            const signal = requestSignal(extra)
            const result = await turn.run({
              role: args.role,
              prompt: args.prompt,
              ...(toolUseID ? { toolUseID } : {}),
              ...(args.background ? { background: true } : {}),
              ...(signal ? { signal } : {}),
            })
            if (result.ok) return { content: [{ type: "text" as const, text: result.text }] }
            return {
              isError: true,
              content: [{ type: "text" as const, text: ClaudeDelegate.failureMessage(result) }],
            }
          } catch {
            return {
              isError: true,
              content: [
                { type: "text" as const, text: ClaudeDelegate.failureMessage(ClaudeDelegate.failure("errored")) },
              ],
            }
          }
        },
        {
          // The CLI runs in-process MCP tools serially unless the tool is
          // concurrency-safe - `isConcurrencySafe()` is
          // `annotations?.readOnlyHint ?? false`. A swarm orchestrator fans
          // several minutes-long roles out in one message; without this hint
          // the second role does not start until the first returns, so
          // "parallel" delegation runs back to back. The hint only classifies
          // the delegate call itself for the CLI's scheduler: each call still
          // routes through `canUseTool` (verified empirically), and whatever a
          // delegated role does to the workspace is gated by OpencodeX's own
          // permissions on the child session, not by this annotation.
          annotations: { readOnlyHint: true },
        },
      ),
    ],
  })
}

function requestSignal(extra: unknown): AbortSignal | undefined {
  if (typeof extra !== "object" || extra === null || !("signal" in extra)) return undefined
  return extra.signal instanceof AbortSignal ? extra.signal : undefined
}

/**
 * Mirrors the GUI main-process resolver: only a real executable file counts,
 * and Windows resolves `claude.exe` because npm's shim cannot be spawned.
 */
export async function resolveClaudeExecutable(input?: { path?: string; home?: string; platform?: NodeJS.Platform }) {
  const platform = input?.platform ?? process.platform
  const names = platform === "win32" ? ["claude.exe"] : ["claude"]
  const home = input?.home ?? process.env.HOME ?? process.env.USERPROFILE
  const fromPath = (input?.path ?? process.env.PATH ?? "")
    .split(platform === "win32" ? ";" : ":")
    .filter(Boolean)
    .flatMap((directory) => names.map((name) => path.join(directory, name)))
  const native = home
    ? platform === "win32"
      ? [path.join(home, ".local", "bin", "claude.exe")]
      : [path.join(home, ".local", "bin", "claude"), "/usr/local/bin/claude", "/opt/homebrew/bin/claude"]
    : []
  for (const candidate of new Set([...fromPath, ...native])) {
    const info = await stat(candidate).catch(() => undefined)
    if (!info?.isFile()) continue
    if (platform === "win32" || (info.mode & 0o111) !== 0) return candidate
  }
  return undefined
}

export * as ClaudeTransport from "./claude-transport"
