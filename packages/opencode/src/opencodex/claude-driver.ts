import { stat } from "node:fs/promises"
import { Context, Duration, Effect, Layer, Option } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import type { SessionSchema } from "@opencode-ai/core/session/schema"
import { Agent } from "@/agent/agent"
import { EffectBridge } from "@/effect/bridge"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { Session } from "@/session/session"
import { Todo } from "@/session/todo"
import { ClaudeDriverMetadata } from "./claude-driver-metadata"
import { ClaudeDelegate } from "./claude-delegate"
import { ClaudeHandoff } from "./claude-handoff"
import { ClaudeMapper, type MapperContext } from "./claude-mapper"
import { ClaudePermission } from "./claude-permission"
import { ClaudeSidechain } from "./claude-sidechain"
import {
  ClaudeTransport,
  createSdkTransport,
  type ClaudeTransport as Transport,
  type ClaudeImage,
  type ClaudePrompt,
  type DelegateCapability,
} from "./claude-transport"

import * as Log from "@opencode-ai/core/util/log"

type SessionID = typeof SessionSchema.ID.Type

const log = Log.create({ service: "claude-driver" })

/**
 * Tools that are Claude's own control flow rather than actions the permission
 * system is meant to gate. Exported so a test can pin the list against the
 * mapper's normalized names.
 */
export const CLAUDE_CONTROL_FLOW: Permission.Ruleset = [{ permission: "plan_exit", pattern: "*", action: "allow" }]

const DELIVERY_FAILURE = "Claude response delivery failed before the turn completed."

export type LayerOptions = {
  transport?: Transport
  resolveExecutable?: () => Promise<string | undefined>
  /**
   * How long after process start a "not logged in" turn is retried once
   * (OpencodeX-zx2). 0 disables the retry; tests set it explicitly.
   */
  startupAuthRetryWindowSeconds?: number
  /** Pause before that retry; tests set 0. */
  startupAuthRetryDelayMillis?: number
}

/** Keeps iterator rejection as data so Effect interruption remains distinct. */
export async function nextClaudeEvent(iterator: AsyncIterator<ClaudeMapper.ClaudeEvent>) {
  return iterator.next().then(
    (next) => ({ next }),
    (failure: unknown) => ({ failure }),
  )
}

/**
 * Runs a session's turns through the user's local Claude Code CLI in headless
 * mode and writes the conversation into the session as native transcript parts.
 *
 * It deliberately depends on neither SessionPrompt nor SessionRunState: the
 * session loop already owns the user message and the execution lease, and calls
 * `runTurn` as its work effect. Keeping those out also keeps the import graph
 * acyclic, since SessionPrompt is what branches here.
 */
/**
 * Swarm delegation as the session loop supplies it: Effect-returning, so the
 * caller stays in Effect-land. The driver bridges it to the Promise the CLI's
 * in-process tool needs, on the same captured bridge as permission prompts.
 */
export type SwarmDelegate = {
  roles: Array<{ name: string; description?: string }>
  /** `toolUseID` is the orchestrator's tool call id for this delegation. */
  run: (input: {
    role: string
    prompt: string
    toolUseID?: string
    background?: boolean
  }) => Effect.Effect<ClaudeDelegate.Result, unknown>
}

/**
 * The bridged delegate the transport's in-process tool calls. On top of
 * ClaudeDelegate.capability's signal bridging, this records each delegation's
 * tool call id so sidechain writes can be told apart from delegate progress.
 */
function delegateCapability(bridge: EffectBridge.Shape, delegate: SwarmDelegate, delegatedCallIDs: Set<string>) {
  const capability = ClaudeDelegate.capability(bridge, delegate)
  return {
    roles: capability.roles,
    run: (input: { role: string; prompt: string; toolUseID?: string; background?: boolean; signal?: AbortSignal }) => {
      if (input.toolUseID) delegatedCallIDs.add(input.toolUseID)
      return capability.run(input)
    },
  }
}

export interface Interface {
  readonly runTurn: (input: {
    sessionID: SessionID
    parentMessageID: typeof SessionLegacy.MessageID.Type
    text: string
    images?: ClaudeImage[]
    directory: string
    /**
     * The catalog route the reader picked, used for transcript attribution.
     * For a swarm that is `swarm/<id>`, which is deliberately not the value the
     * CLI runs - see `claudeModelID`.
     */
    providerID: string
    modelID: string
    /** The model value handed to the CLI; defaults to `modelID`. */
    claudeModelID?: string
    /** The selected variant, which for this provider is the effort level. */
    variant?: string
    executionGeneration?: number
    /**
     * Present when the turn is a swarm orchestrator: gives the CLI a tool that
     * runs specialist roles back inside OpencodeX on their own models.
     */
    delegate?: SwarmDelegate
    /**
     * Present when the caller can turn a Claude subagent (sidechain) into a
     * real child session. Without it, sidechain events fall through to the
     * main mapper untouched (today's behavior).
     */
    sidechain?: {
      spawn: (input: { title: string; prompt: string }) => Effect.Effect<{ sessionID: string; userMessageID: string }>
    }
    /**
     * Opens a real turn on this session when its CLI asks for an approval
     * with no turn attached (OpencodeX-sxp). Supplied by the session loop,
     * which owns turns; the driver only bridges it.
     */
    adoptOrphan?: (input: { toolName: string }) => Effect.Effect<void>
    liveQueue?: LiveQueue
  }) => Effect.Effect<SessionLegacy.WithParts>
}

/** Durable command ownership remains in SessionPrompt; the driver only sequences its live delivery. */
export type LiveQueue = {
  reserve: () => Effect.Effect<LiveQueueOffer | undefined>
  offer: (offer: LiveQueueOffer) => Effect.Effect<boolean>
  requeue: (offer: LiveQueueOffer) => Effect.Effect<void>
  settle: (offer: LiveQueueOffer, error?: string) => Effect.Effect<void>
  failOffered: (error: string) => Effect.Effect<void>
}

export type LiveQueueOffer = {
  commandID: string
  messageID: typeof SessionLegacy.MessageID.Type
  ordinal: number
}

export class Service extends Context.Service<Service, Interface>()("@opencode/OpencodeXClaudeDriver") {}

export function makeLayer(options: LayerOptions = {}) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const todos = yield* Todo.Service
      const permission = yield* Permission.Service
      const question = yield* Question.Service
      const agents = yield* Agent.Service
      const decide = ClaudePermission.decideWith(permission, question)
      const transport = options.transport ?? createSdkTransport()

      const runTurn = Effect.fn("OpencodeXClaudeDriver.runTurn")(function* (input: {
        sessionID: SessionID
        parentMessageID: typeof SessionLegacy.MessageID.Type
        text: string
        images?: ClaudeImage[]
        directory: string
        providerID: string
        modelID: string
        claudeModelID?: string
        variant?: string
        executionGeneration?: number
        delegate?: SwarmDelegate
        sidechain?: {
          spawn: (input: {
            title: string
            prompt: string
          }) => Effect.Effect<{ sessionID: string; userMessageID: string }>
        }
        adoptOrphan?: (input: { toolName: string }) => Effect.Effect<void>
        liveQueue?: LiveQueue
      }) {
        const session = yield* sessions.get(input.sessionID).pipe(Effect.orDie)
        // Resolved per turn rather than per layer: `Agent.defaultInfo` re-reads
        // `opencodex.json` on every call and applies the user's `permission_mode`
        // to the ruleset, so switching modes in the GUI takes effect on the next
        // turn instead of needing a restart.
        const permissionAgent = yield* agents.defaultInfo()
        const ruleset = Permission.merge(
          permissionAgent?.permission ?? [],
          session.permission ?? [],
          // `ExitPlanMode` normalizes onto `plan_exit`, which the default ruleset
          // denies because it is an OpencodeX tool a model should not call
          // directly. In a mirrored session it is Claude's own control flow, and
          // the CLI has no way to recover from a denial - it would simply be
          // unable to leave plan mode. Appended last so it wins the match.
          CLAUDE_CONTROL_FLOW,
        )
        const conversation = ClaudeDriverMetadata.readConversation(session.metadata)
        // Only an id Claude issued can be resumed. The first turn passes none and
        // adopts whatever `system.init` reports; every later turn resumes that.
        const resumeID = conversation?.conversationID

        // When the permission gate rewrites a call's input - the AskUserQuestion
        // answers come back that way - the transcript should record what was
        // actually approved, not the model's original request. The decision
        // always resolves before the CLI executes the tool, so the entry is in
        // place by the time the tool_result event is mapped.
        const decidedInputs = new Map<string, Record<string, unknown>>()
        // Tool calls this turn that were handed to a swarm role. The role stamps
        // the child session onto the parent's tool part while it runs, and the
        // mapper rebuilds that part from the tool_result alone - so these are the
        // calls whose stamp has to be carried across a rewrite.
        const delegatedCallIDs = new Set<string>()
        let context: MapperContext = {
          sessionID: input.sessionID,
          parentMessageID: input.parentMessageID,
          directory: input.directory,
          providerID: input.providerID,
          modelID: input.modelID,
          nextMessageID: () => SessionLegacy.MessageID.ascending(),
          nextPartID: () => SessionLegacy.PartID.ascending(),
          now: () => Date.now(),
          decidedInput: (callID) => decidedInputs.get(callID),
        }
        let live = ClaudeMapper.initialState({
          modelID: conversation?.modelID,
          billed: conversation?.billed,
          tasks: conversation?.tasks,
        })

        // Present only when the caller (prompt-swarm) can turn a Claude subagent
        // into a real child session. Sidechains are Claude's own event stream,
        // tagged with `parent_tool_use_id` - see claude-sidechain.ts.
        const sidechain = input.sidechain
          ? ClaudeSidechain.createSidechainRouter({
              makeContext: (sessionID, parentMessageID) => ({
                ...context,
                sessionID: sessionID as typeof context.sessionID,
                parentMessageID: parentMessageID as typeof context.parentMessageID,
              }),
            })
          : undefined

        // Recursive because attaching a child can immediately flush buffered
        // writes for it, which themselves need to be interpreted. Declared here
        // (rather than alongside `applyWrites`) so it closes over `input` and
        // `sidechain`.
        const interpretSidechainActions: (actions: ClaudeSidechain.SidechainAction[]) => Effect.Effect<void> =
          Effect.fn("OpencodeXClaudeDriver.sidechain")(function* (actions) {
            for (const action of actions) {
              if (action.kind === "spawn") {
                const child = yield* ClaudeSidechain.recoverSpawnFailure(
                  input.sidechain!.spawn({ title: action.title, prompt: action.prompt }),
                )
                if (child) {
                  const flushed = sidechain!.attachChild(action.chainID, child.sessionID, child.userMessageID)
                  yield* interpretSidechainActions(flushed)
                } else {
                  // The spawn was not recovered (the child session never came into
                  // being), so attachChild will never be called for this chain -
                  // without abandoning it, its later events would buffer
                  // unboundedly in `chain.pending` for the rest of the turn.
                  sidechain!.abandonChain(action.chainID)
                }
                continue
              }
              yield* applyWrites(action.writes, action.sessionID as SessionID)
            }
          })

        const executable = yield* Effect.promise(() =>
          (options.resolveExecutable ?? ClaudeTransport.resolveClaudeExecutable)(),
        )
        if (!executable) {
          return yield* failTurn(
            context,
            live,
            "Claude Code was not found. Install it from code.claude.com/docs/en/installation, then try again.",
            "missing-cli",
          )
        }
        // A spawn into a directory that no longer exists fails inside the SDK,
        // which reports it as the binary "failed to launch ... does not match
        // this system libc" (OpencodeX-t5p, 2026-08-28). Say what is actually
        // wrong before the CLI gets a chance to guess.
        const directoryExists = yield* Effect.promise(() =>
          stat(input.directory).then(
            (info) => info.isDirectory(),
            () => false,
          ),
        )
        if (!directoryExists) {
          return yield* failTurn(
            context,
            live,
            `The session directory ${input.directory} does not exist. Recreate it or open the session in another directory.`,
            "missing-directory",
          )
        }

        // Claude keeps its own conversation state, so anything that came before is
        // invisible to a conversation it has not been resuming. Replay it once,
        // when opening a new one on a session that already has history.
        const text = resumeID ? input.text : yield* primedPrompt(input.sessionID, input.parentMessageID, input.text)
        // Images go first. With the text first, a long text block (the swarm
        // briefing, a replayed history) made Claude answer "no image was
        // attached" even though the block was in its transcript (OpencodeX-i13,
        // live captures 2026-08-28: the same image/text was seen with the
        // briefing removed). That is also Anthropic's documented ordering.
        const prompt: ClaudePrompt = input.images?.length
          ? [...input.images, ...(text ? [{ type: "text" as const, text }] : [])]
          : text

        // Captured here so a permission prompt raised inside Claude's callback
        // still carries this request's instance and workspace context.
        const bridge = yield* EffectBridge.make()
        // Turn lifecycle breadcrumbs for OpencodeX-div: the CLI aborts control
        // requests with "Stream closed" when its channel to this query dies, and
        // without these entries the daemon's log cannot say whether a live query
        // even existed for the session at that moment.
        log.info("claude turn starting", {
          sessionID: input.sessionID,
          ...(resumeID ? { resumeID } : {}),
          model: input.claudeModelID ?? input.modelID,
          // OpencodeX-e2e image-drop triage: whether native attachments made
          // it to the driver at all, and whether this turn is delegating.
          images: input.images?.length ?? 0,
          delegating: input.delegate !== undefined,
        })
        const logPermission = bridge.bind((toolName: string, toolUseID?: string) => {
          log.info("claude permission request received", { sessionID: input.sessionID, toolName, callID: toolUseID })
        })
        const turn = transport.run(prompt, {
          cwd: input.directory,
          ...(resumeID ? { resumeID } : {}),
          // One persistent query per session: the CLI child survives across
          // turns, so no per-turn resume spawn can misdeliver a stale close-out
          // result or orphan a running turn (OpencodeX-div).
          channelKey: input.sessionID,
          executable,
          model: input.claudeModelID ?? input.modelID,
          ...(input.variant ? { effort: input.variant } : {}),
          ...(input.delegate
            ? {
                delegate: delegateCapability(bridge, input.delegate, delegatedCallIDs) satisfies DelegateCapability,
              }
            : {}),
          ...(input.adoptOrphan
            ? { adoptOrphan: (orphan: { toolName: string }) => bridge.promise(input.adoptOrphan!(orphan)) }
            : {}),
          canUseTool: (toolName, toolInput, toolUseID, signal) => {
            logPermission(toolName, toolUseID)
            return bridge
              .promise(
                decide({
                  sessionID: input.sessionID,
                  toolName,
                  toolInput,
                  messageID: live.messageID,
                  callID: toolUseID,
                  ...(input.executionGeneration !== undefined
                    ? { executionGeneration: input.executionGeneration }
                    : {}),
                  ruleset,
                }),
                signal ? { signal } : undefined,
              )
              .then((decision) => {
                if (decision.allow && decision.input && toolUseID) decidedInputs.set(toolUseID, decision.input)
                return decision
              })
              .catch(() => ({ allow: false as const, message: "The permission request failed." }))
          },
        })

        const finalize = Effect.fn("OpencodeXClaudeDriver.finalize")(function* (reason: string, error?: string) {
          const closing = ClaudeMapper.finalizeAbandonedTurn(live, context, { reason, ...(error ? { error } : {}) })
          live = closing.state
          yield* applyWrites(closing.writes, input.sessionID, { delegatedCallIDs })
        })

        const saveConversation = Effect.fn("OpencodeXClaudeDriver.saveConversation")(function* () {
          // A resume that Claude rejected leaves the stored id unusable, so drop it
          // and let the next turn open a fresh conversation instead of failing forever.
          const nextID = live.claudeSessionID ?? (live.resumeRejected ? undefined : resumeID)
          yield* sessions
            .setMetadata({
              sessionID: input.sessionID,
              metadata: ClaudeDriverMetadata.withConversation(session.metadata, {
                ...(nextID ? { conversationID: nextID } : {}),
                launched: true,
                modelID: live.modelID,
                billed: live.billed,
                ...(live.authFailed ? { authState: "needs-login" as const } : { authState: "ready" as const }),
                ...(live.tasks.size > 0 ? { tasks: [...live.tasks].map(([id, task]) => ({ id, ...task })) } : {}),
              }),
            })
            .pipe(Effect.ignore)
        })

        const interruptTurn = Effect.sync(() => {
          log.info("claude turn interrupt requested", { sessionID: input.sessionID })
          void turn.interrupt().catch(() => undefined)
        })

        const iterator = turn.events[Symbol.asyncIterator]()
        // Manual iteration means no implicit `return()` on break: without this
        // release, a persistent channel's turn would stay attached after the
        // mapper finishes and every later turn on the session would fail
        // "already active". Safe on any exit path, idempotent on both
        // transports.
        const releaseTurn = Effect.sync(() => {
          void iterator.return?.().catch(() => undefined)
        })
        let deliveryFailed = false
        let offered: LiveQueueOffer | undefined
        let initialResult: SessionLegacy.WithParts | undefined
        const consume = Effect.gen(function* () {
          while (true) {
            const result = yield* Effect.promise(() => nextClaudeEvent(iterator))
            if ("failure" in result) {
              deliveryFailed = true
              break
            }
            const next = result.next
            if (next.done) break
            if (sidechain) {
              const routed = sidechain.route(next.value, live.toolParts)
              yield* interpretSidechainActions(routed.actions)
              if (routed.handled) continue
            }
            const mapped = ClaudeMapper.mapEvent(next.value, live, context)
            live = mapped.state
            yield* applyWrites(mapped.writes, input.sessionID, { delegatedCallIDs })
            if (!live.finished) continue
            const turnResult = yield* readTurn(input.sessionID, live.messageID)
            const error =
              turnResult.info.role === "assistant" &&
              turnResult.info.error &&
              turnResult.info.error.name !== "MessageAbortedError"
                ? JSON.stringify(turnResult.info.error)
                : undefined
            if (offered) {
              if (input.liveQueue) yield* input.liveQueue.settle(offered, error)
              offered = undefined
            } else initialResult = turnResult
            if (!input.liveQueue || process.env.OPENCODE_CLAUDE_LIVE_QUEUE !== "1") break
            const reserved = yield* input.liveQueue.reserve()
            if (!reserved) break
            const message = yield* sessions
              .findMessage(input.sessionID, (entry) => entry.info.id === reserved.messageID)
              .pipe(Effect.orDie)
            const text = Option.match(message, {
              onNone: () => "",
              onSome: (entry) =>
                entry.parts
                  .flatMap((part) => (part.type === "text" && !part.synthetic && !part.ignored ? [part.text] : []))
                  .join("\n")
                  .trim(),
            })
            if (!text) {
              yield* input.liveQueue.requeue(reserved)
              continue
            }
            // The offer timestamp is the delivery linearization point. Keep the
            // old sink attached until both it and the SDK push succeed.
            if (!(yield* input.liveQueue.offer(reserved))) {
              yield* input.liveQueue.requeue(reserved)
              continue
            }
            if (!turn.offer?.(text)) {
              yield* input.liveQueue.failOffered(DELIVERY_FAILURE)
              deliveryFailed = true
              deliveryFailure = DELIVERY_FAILURE
              break
            }
            offered = reserved
            context = { ...context, parentMessageID: reserved.messageID }
            live = ClaudeMapper.initialState({
              modelID: live.modelID,
              billed: live.billed,
              tasks: [...live.tasks].map(([id, task]) => ({ id, ...task })),
            })
          }
          log.info("claude turn events ended", {
            sessionID: input.sessionID,
            finished: live.finished,
            deliveryFailed,
          })
        })
        // Stopping the session interrupts this fiber, and interruption must reach
        // the CLI subprocess: without the explicit `interrupt` the child keeps
        // running headless, and without `finalize` its tool parts stay "running"
        // in the transcript forever. Runs in the interrupt handler because the
        // code after this pipe never executes on an interrupted fiber.
        yield* consume.pipe(
          Effect.ensuring(releaseTurn),
          Effect.onInterrupt(() =>
            Effect.gen(function* () {
              yield* interruptTurn
              yield* finalize("abort")
              if (sidechain)
                yield* interpretSidechainActions(
                  sidechain.finalizeAll("The turn was interrupted before this subagent finished."),
                )
              yield* saveConversation()
            }),
          ),
        )

        if (deliveryFailed || !live.finished) {
          return yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* interruptTurn
              yield* finalize("delivery-failed", DELIVERY_FAILURE)
              if (sidechain) yield* interpretSidechainActions(sidechain.finalizeAll(DELIVERY_FAILURE))
              yield* saveConversation()
              if (input.liveQueue) yield* input.liveQueue.failOffered(DELIVERY_FAILURE)
              return yield* readTurn(input.sessionID, live.messageID)
            }),
          )
        }

        // Any sidechain still open when the turn ends (the main turn finished,
        // errored, or hit its stop reason before the subagent's own tool_result
        // arrived) is abandoned - close it the same way `finalize` closes the
        // main turn, so its transcript never shows a part stuck "running".
        if (sidechain) yield* interpretSidechainActions(sidechain.finalizeAll())

        yield* saveConversation()

        const result = initialResult ?? (yield* readTurn(input.sessionID, live.messageID))
        const error =
          result.info.role === "assistant" && result.info.error && result.info.error.name !== "MessageAbortedError"
            ? JSON.stringify(result.info.error)
            : undefined
        if (offered && input.liveQueue) yield* input.liveQueue.settle(offered, error)
        return result
      })

      /**
       * Prefixes the earlier transcript when Claude is taking over a session that
       * was already running. Everything from `parentMessageID` onward is the turn
       * being sent right now, so it is excluded.
       */
      const primedPrompt = Effect.fnUntraced(function* (
        sessionID: SessionID,
        parentMessageID: typeof SessionLegacy.MessageID.Type,
        text: string,
      ) {
        const history = yield* sessions.messages({ sessionID }).pipe(Effect.orElseSucceed(() => []))
        const prior = history.slice(
          0,
          Math.max(
            0,
            history.findIndex((message) => message.info.id === parentMessageID),
          ),
        )
        return ClaudeHandoff.withHandoff(text, prior)
      })

      /** Writes a single failed assistant turn when the CLI cannot even start. */
      const failTurn = Effect.fn("OpencodeXClaudeDriver.failTurn")(function* (
        context: MapperContext,
        state: ClaudeMapper.MapperState,
        message: string,
        code: string,
      ) {
        const opened = ClaudeMapper.startTurn(state, context)
        const closing = ClaudeMapper.finalizeAbandonedTurn(opened.state, context, { reason: code, error: message })
        yield* applyWrites([...opened.writes, ...closing.writes], context.sessionID)
        return yield* readTurn(context.sessionID, closing.state.messageID)
      })

      /**
       * A delegated swarm role stamps the child session it spawned onto the
       * orchestrator's running tool part (prompt-swarm.runSwarmRole), which is
       * what the transcript's sub-agent link reads. The mapper rebuilds that part
       * from the tool_result event alone, so without carrying the stamp forward
       * the row would lose its drill-down the moment the delegation returned.
       * Only delegated calls are re-read, so an ordinary turn does no extra work.
       */
      const withDelegationStamp = Effect.fn("OpencodeXClaudeDriver.withDelegationStamp")(function* (
        part: SessionLegacy.Part,
        sessionID: SessionID,
        delegatedCallIDs?: ReadonlySet<string>,
      ) {
        if (part.type !== "tool" || part.state.status === "pending") return part
        if (!delegatedCallIDs?.has(part.callID)) return part
        const existing = yield* sessions
          .getPart({ sessionID, messageID: part.messageID, partID: part.id })
          .pipe(Effect.orElseSucceed(() => undefined))
        if (existing?.type !== "tool" || existing.state.status === "pending") return part
        const stamped = existing.state.metadata
        if (!stamped) return part
        return { ...part, state: { ...part.state, metadata: { ...stamped, ...part.state.metadata } } }
      })

      const applyWrites = Effect.fn("OpencodeXClaudeDriver.applyWrites")(function* (
        writes: ClaudeMapper.SessionWrite[],
        sessionID: SessionID,
        options?: { delegatedCallIDs?: ReadonlySet<string> },
      ) {
        for (const write of writes) {
          if (write.kind === "message") yield* sessions.updateMessage(write.message)
          else if (write.kind === "part")
            yield* sessions.updatePart(yield* withDelegationStamp(write.part, sessionID, options?.delegatedCallIDs))
          else
            // `Todo.update` can defect (e.g. a NOT NULL violation) rather than fail
            // typed, and `Effect.ignore` does not catch defects - an uncaught one
            // here would kill the whole turn over a todos projection. `catchCause`
            // recovers from both typed failures and defects.
            yield* todos
              .update({ sessionID, todos: write.todos as never })
              .pipe(Effect.catchCause((cause) => Effect.logWarning("todos update failed", { cause })))
        }
      })

      const readTurn = Effect.fn("OpencodeXClaudeDriver.readTurn")(function* (
        sessionID: SessionID,
        messageID?: typeof SessionLegacy.MessageID.Type,
      ) {
        const empty = { info: { id: messageID ?? "", sessionID }, parts: [] } as unknown as SessionLegacy.WithParts
        if (!messageID) return empty
        const found = yield* sessions
          .findMessage(sessionID, (message) => message.info.id === messageID)
          .pipe(Effect.orDie)
        return Option.getOrElse(found, () => empty)
      })

      /**
       * OpencodeX-zx2: right after a daemon restart every session's CLI child
       * respawns at once, and the first turn can come back "Not logged in"
       * although nothing about the login changed (live 2026-08-28, 11 s after
       * kickstart; the same prompt passed 20 min later). One retry after a
       * short pause, only while the process is young, so a real sign-out is
       * still reported on the second attempt. The failed assistant message is
       * removed first so the transcript does not show a phantom error.
       */
      const runTurnWithStartupRetry: Interface["runTurn"] = Effect.fn("OpencodeXClaudeDriver.runTurnWithStartupRetry")(
        function* (input) {
          const first = yield* runTurn(input)
          const error = first.info.role === "assistant" ? first.info.error : undefined
          const window = options.startupAuthRetryWindowSeconds ?? STARTUP_AUTH_RETRY_WINDOW_SECONDS
          if (!error || !isAuthFailure(error) || process.uptime() > window) return first
          log.warn("claude auth failure during startup window; retrying once", {
            sessionID: input.sessionID,
            uptimeSeconds: Math.round(process.uptime()),
          })
          yield* sessions
            .removeMessage({ sessionID: input.sessionID, messageID: first.info.id as never })
            .pipe(Effect.ignore)
          yield* Effect.sleep(Duration.millis(options.startupAuthRetryDelayMillis ?? STARTUP_AUTH_RETRY_DELAY_MILLIS))
          return yield* runTurn(input)
        },
      )

      return Service.of({ runTurn: runTurnWithStartupRetry })
    }),
  )
}

/** A sign-in failure however this line labels it: the typed auth error, or the CLI's own wording. */
function isAuthFailure(error: { name: string; data?: unknown }) {
  if (error.name === "ProviderAuthError") return true
  const message = (error.data as { message?: unknown } | undefined)?.message
  return typeof message === "string" && /not logged in|unauthorized|authentication|please run .*login/i.test(message)
}

const STARTUP_AUTH_RETRY_WINDOW_SECONDS = 180
const STARTUP_AUTH_RETRY_DELAY_MILLIS = 5_000

export const layer = makeLayer()

export const defaultLayer = layer.pipe(
  Layer.provide(Session.defaultLayer),
  Layer.provide(Todo.defaultLayer),
  Layer.provide(Permission.defaultLayer),
  Layer.provide(Question.defaultLayer),
  Layer.provide(Agent.defaultLayer),
)

export * as OpencodeXClaudeDriver from "./claude-driver"
