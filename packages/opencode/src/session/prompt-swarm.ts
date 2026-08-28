import { asc, eq } from "drizzle-orm"
import { Cause, Context, Effect, Exit, Option } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Database } from "@opencode-ai/core/database/database"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { OpencodeXSwarmRoleTable, OpencodeXSwarmTable } from "@opencode-ai/core/opencodex/sql"
import { Image } from "@/image/image"
import { OpencodeXClaudeDriver } from "@/opencodex/claude-driver"
import { ClaudeDelegate } from "@/opencodex/claude-delegate"
import { SwarmBriefing } from "@/opencodex/swarm-briefing"
import type { BackgroundJob } from "@/background/job"
import { Skill } from "@/skill"
import { CLAUDE_CODE_DEFAULT_MODEL_ID, isClaudeCodeProvider } from "@/provider/claude-code-provider"
import { isSwarmProvider } from "@/provider/swarm-provider"
import { MessageID, PartID, SessionID } from "./schema"
import type { PromptInput } from "./prompt-schema"
import {
  DELEGATION_RECORD_VERSION,
  settleDelegation,
  type DelegationOutcome,
  type DelegationRecord,
} from "./delegation-outcome"
import { Identifier } from "@/id/id"
import * as Log from "@opencode-ai/core/util/log"
import * as Session from "./session"
import { prepareImages } from "./swarm-attachments"
import { SessionStatus } from "./status"
import { hydrateFallbackModels } from "@/opencodex/swarm-model"
import { shouldAdvanceModelFallback } from "./model-fallback"

const log = Log.create({ service: "session.prompt-swarm" })

/** One swarm role as the loop reads it, ordered by `sort_order`. */
export type SwarmRoleRow = {
  name: string
  agent: string | null
  skill: string | null
  instructions: string
  provider_id: string | null
  model_id: string | null
  /** The model variant (effort level) the role runs at, when one is chosen. */
  variant: string | null
  fallback_models: string
}

export interface Deps {
  readonly claudeDriver: Context.Service.Shape<typeof OpencodeXClaudeDriver.Service>
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly sessions: Context.Service.Shape<typeof Session.Service>
  readonly skills: Context.Service.Shape<typeof Skill.Service>
  readonly prompt: (input: PromptInput) => Effect.Effect<SessionLegacy.WithParts, Image.Error>
  readonly loop: (input: { sessionID: SessionID; messageID?: MessageID }) => Effect.Effect<SessionLegacy.WithParts>
  /**
   * Runs a background delegation's role fiber and owns its lifetime. Absent
   * (older callers, tests), `background: true` delegations run inline.
   */
  readonly background?: Pick<Context.Service.Shape<typeof BackgroundJob.Service>, "start">
}

/**
 * The message a Claude turn should deliver. A queued command names its own
 * message; delivering `lastUserMessage` instead sent the newest text N times
 * and swallowed the earlier queued messages (2026-08-10 spec, problem 2b).
 */
export function claudeTurnMessage<T extends { info: { id: string; role: string } }>(
  messages: readonly T[],
  messageID: string | undefined,
): T | undefined {
  if (messageID === undefined) return messages.findLast((message) => message.info.role === "user")
  const message = messages.find((message) => message.info.id === messageID)
  return message?.info.role === "user" ? message : undefined
}

/** What the orchestrator sees the moment a background delegation starts. */
export function backgroundDelegationStarted(childSessionID: string, role: string) {
  return [
    `<task id="${childSessionID}" state="running" role="${role}">`,
    `<summary>Delegation started: ${role}</summary>`,
    "<task_result>",
    `${role} is working in the background. Its report will arrive as a message; do not poll or wait for it.`,
    "End your turn when nothing else is ready - the human can talk to you meanwhile.",
    "</task_result>",
    "</task>",
  ].join("\n")
}

/** The synthetic message that wakes the orchestrator when the role finishes. */
export function backgroundDelegationMessage(input: {
  childSessionID: string
  role: string
  state: "completed" | "error"
  text: string
}) {
  const tag = input.state === "completed" ? "task_result" : "task_error"
  const title = input.state === "completed" ? `Delegation completed: ${input.role}` : `Delegation failed: ${input.role}`
  return [
    `<task id="${input.childSessionID}" state="${input.state}" role="${input.role}">`,
    `<summary>${title}</summary>`,
    `<${tag}>`,
    input.text,
    `</${tag}>`,
    "</task>",
  ].join("\n")
}

/**
 * The two non-ordinary routes a turn can take: a swarm (a team of roles behind
 * one model id) and the local Claude Code CLI driver. Both are decided from the
 * last user message, which is why they sit together.
 */
export function make(deps: Deps) {
  const { claudeDriver, database, sessions, skills, prompt } = deps
  const { db } = database

  /**
   * Sessions on a swarm model run in place: the model resolves to the
   * orchestrator's real model (Provider.getModel handles that), and this
   * hidden part of the user message hands the orchestrator its team so it
   * delegates specialists as subagents inside the same session.
   */
  const ensureSwarmBriefing = Effect.fnUntraced(function* (sessionID: SessionID, messageID?: MessageID) {
    const context = yield* swarmContext(sessionID, messageID)
    if (!context) return
    const briefed = context.last.parts.some(
      (part) =>
        part.type === "text" && part.synthetic === true && part.text.startsWith(SwarmBriefing.SWARM_BRIEFING_MARK),
    )
    if (briefed) return
    const briefing = SwarmBriefing.buildSwarmBriefing({
      swarmID: context.swarmID,
      title: context.title,
      delegation: context.orchestratorIsClaudeCode ? "delegate-tool" : "task-tool",
      roles: context.roles.map((role) => ({
        name: role.name,
        agent: role.agent ?? undefined,
        skill: role.skill ?? undefined,
        instructions: role.instructions ?? undefined,
        providerID: role.provider_id ?? undefined,
        modelID: role.model_id ?? undefined,
      })),
    })
    if (!briefing) return
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: context.last.info.id,
      sessionID,
      type: "text",
      text: briefing,
      synthetic: true,
    })
  })

  /** The swarm behind a session's model, or undefined for an ordinary route. */
  const swarmContext = Effect.fnUntraced(function* (sessionID: SessionID, messageID?: MessageID) {
    const last = messageID ? yield* userMessage(sessionID, messageID) : yield* lastUserMessage(sessionID)
    if (!last || last.info.role !== "user") return undefined
    if (!isSwarmProvider(last.info.model.providerID)) return undefined
    const swarmID = last.info.model.modelID
    const swarm = yield* db
      .select({ id: OpencodeXSwarmTable.id, title: OpencodeXSwarmTable.title })
      .from(OpencodeXSwarmTable)
      .where(eq(OpencodeXSwarmTable.id, swarmID))
      .get()
      .pipe(Effect.orElseSucceed(() => undefined))
    if (!swarm) return undefined
    const roles = yield* db
      .select({
        name: OpencodeXSwarmRoleTable.name,
        agent: OpencodeXSwarmRoleTable.agent,
        skill: OpencodeXSwarmRoleTable.skill,
        instructions: OpencodeXSwarmRoleTable.instructions,
        provider_id: OpencodeXSwarmRoleTable.provider_id,
        model_id: OpencodeXSwarmRoleTable.model_id,
        variant: OpencodeXSwarmRoleTable.variant,
        fallback_models: OpencodeXSwarmRoleTable.fallback_models,
      })
      .from(OpencodeXSwarmRoleTable)
      .where(eq(OpencodeXSwarmRoleTable.swarm_id, swarmID))
      .orderBy(asc(OpencodeXSwarmRoleTable.sort_order))
      .all()
      .pipe(Effect.orElseSucceed(() => []))
    const orchestrator = roles[0]
    return {
      last,
      swarmID,
      title: swarm.title,
      roles,
      orchestrator,
      orchestratorIsClaudeCode: Boolean(orchestrator?.provider_id && isClaudeCodeProvider(orchestrator.provider_id)),
    }
  })

  /**
   * Records the delegated child on the orchestrator's own tool part, the way
   * the task tool does for a native subagent (src/tool/task.ts): the GUI's
   * transcript link reads `metadata.sessionId` off that part, so this stamp is
   * what turns a delegation row into a drill-down. Failing to stamp only costs
   * the link, never the delegation, so every outcome is swallowed.
   */
  const stampDelegateToolPart = Effect.fnUntraced(function* (input: {
    sessionID: SessionID
    toolUseID: string
    childSessionID: string
    role: string
  }) {
    const isCall = (part: SessionLegacy.Part) => part.type === "tool" && part.callID === input.toolUseID
    const match = yield* sessions
      .findMessage(input.sessionID, (message) => message.parts.some(isCall))
      // catchCause, not orElseSucceed: a defect here must also cost only the
      // link, never the delegation - "every outcome is swallowed" means it.
      .pipe(Effect.catchCause(() => Effect.succeed(Option.none<SessionLegacy.WithParts>())))
    const part = Option.getOrUndefined(match)?.parts.find(isCall)
    // A pending part has no metadata to hang the link on; by the time a
    // delegation runs the call is always running, so this is only a guard.
    if (part?.type !== "tool" || part.state.status === "pending") return
    yield* sessions
      .updatePart({
        ...part,
        state: {
          ...part.state,
          metadata: {
            ...part.state.metadata,
            parentSessionId: input.sessionID,
            sessionId: input.childSessionID,
            swarmRole: input.role,
          },
        },
      })
      .pipe(
        Effect.catchCause((cause) =>
          Effect.sync(() => log.error("swarm delegate stamp failed", { sessionID: input.sessionID, cause })),
        ),
      )
  })

  /**
   * Runs one specialist role as its own OpencodeX session on the model
   * configured for it, and returns its report. This is what the Claude Code
   * orchestrator's delegation tool calls, so specialists stay OpencodeX
   * sessions instead of becoming Claude's internal subagents.
   */
  const runSwarmRole = Effect.fnUntraced(function* (input: {
    sessionID: SessionID
    swarmID: string
    roles: SwarmRoleRow[]
    role: string
    prompt: string
    /**
     * The orchestrator's tool call id for this delegation, when the driver
     * could correlate one. It is what links the parent's transcript row to the
     * session this role runs in.
     */
    toolUseID?: string
    /**
     * Start the role and return at once. The report is delivered to the
     * parent later as a synthetic message (the same wake the native task
     * tool's background mode uses), so the orchestrator's turn can end and
     * the human can keep talking to it while the role works.
     */
    background?: boolean
  }) {
    const role = SwarmBriefing.matchSwarmRole(input.roles, input.role)
    // Each rejection carries the reason the orchestrator can act on. The
    // unknown-role roster is the recovery path for a mistyped free-form role
    // argument; without it the model retries the same bad name blind.
    if (!role)
      return ClaudeDelegate.failure(
        "rejected",
        `Unknown role "${input.role}". Available roles: ${input.roles.map((entry) => entry.name).join(", ")}.`,
      )
    if (!role.provider_id || !role.model_id)
      return ClaudeDelegate.failure("rejected", `Role "${role.name}" has no model configured.`)
    // A role stored on the swarm facade has no concrete model to run: a child
    // prompted on the facade would take the swarm route itself and recurse.
    if (isSwarmProvider(role.provider_id))
      return ClaudeDelegate.failure("rejected", `Role "${role.name}" is not runnable: it points at the swarm itself.`)
    const parent = yield* sessions.get(input.sessionID).pipe(Effect.orDie)
    const child = yield* sessions
      .create({
        parentID: input.sessionID,
        title: `${role.name} (swarm role)`,
        // The GUI's team view groups a swarm session's children by role, so
        // each delegation records which role it ran as.
        metadata: { opencodex: { swarmID: input.swarmID, swarmRole: role.name } },
        ...(parent.permission ? { permission: parent.permission } : {}),
      })
      .pipe(Effect.orDie)
    // Written before the role starts, so the orchestrator's transcript row
    // drills into a running delegation and not only a finished one.
    if (input.toolUseID)
      yield* stampDelegateToolPart({
        sessionID: input.sessionID,
        toolUseID: input.toolUseID,
        childSessionID: child.id,
        role: role.name,
      })
    // The role's skill is its base definition; the built-in role skills carry
    // the full role prompt. The task-tool path gets it through the specialist
    // loading the skill itself, but a delegated specialist never sees the
    // skill tool's inventory - so the body is delivered here, ahead of the
    // per-role instructions and the task.
    // The durable answer to "did this delegation work?" - and its opening
    // line. Nothing else records either: a swarm child has no job row and its
    // live status clears on return, so without this stamp the workflow graph
    // can only say "returned" and show nothing of what came back. The record
    // is run-scoped: `running` is written before the prompt starts and one
    // all-exit boundary settles it, so a defect or interruption can no longer
    // slip out without a terminal record.
    const runID = Identifier.ascending("run")
    const started: DelegationRecord = {
      version: DELEGATION_RECORD_VERSION,
      runID,
      parentSessionID: input.sessionID,
      attempt: 1,
      phase: "running",
      startedAt: Date.now(),
    }
    const stamp = (record: DelegationRecord, expectRunID?: string) =>
      sessions.stampDelegation({ sessionID: child.id, record, ...(expectRunID ? { expectRunID } : {}) }).pipe(
        Effect.catchCause((cause) =>
          Effect.sync(() => {
            log.error("swarm delegation stamp failed", { sessionID: child.id, runID, cause })
            return false
          }),
        ),
      )
    const settle = (outcome: DelegationOutcome, summary?: string) =>
      stamp(
        settleDelegation(started, {
          outcome,
          summary,
          // A background completion still has to be durably delivered to the
          // parent; the delivery stamp below only lands on a pending record.
          ...(input.background && outcome === "completed" ? { deliveryOutcome: "pending" as const } : {}),
        }),
        runID,
      ).pipe(Effect.asVoid)
    yield* stamp(started)
    const runRole: Effect.Effect<ClaudeDelegate.Result> = Effect.gen(function* () {
      // The role's skill is its base definition; the built-in role skills carry
      // the full role prompt. The task-tool path gets it through the specialist
      // loading the skill itself, but a delegated specialist never sees the
      // skill tool's inventory - so the body is delivered here, ahead of the
      // per-role instructions and the task.
      const roleSkill = role.skill ? yield* skills.get(role.skill) : undefined
      const text = [roleSkill?.content.trim(), role.instructions?.trim(), input.prompt.trim()]
        .filter(Boolean)
        .join("\n\n")
      const models = [
        { providerID: role.provider_id!, modelID: role.model_id!, variant: role.variant ?? undefined },
        ...(role.skill === "orchestrator" || role.name.trim().toLowerCase() === "orchestrator"
          ? []
          : hydrateFallbackModels(role.fallback_models, {
              providerID: role.provider_id!,
              modelID: role.model_id!,
            })),
      ]
      const userMessageID = MessageID.ascending()
      const primary = models[0]
      const initial = yield* prompt({
        messageID: userMessageID,
        sessionID: child.id,
        model: {
          providerID: ProviderV2.ID.make(primary.providerID),
          modelID: ProviderV2.ModelID.make(primary.modelID),
        },
        ...(role.agent ? { agent: role.agent } : {}),
        ...(primary.variant && primary.variant !== "default" ? { variant: primary.variant } : {}),
        parts: [{ type: "text", text }],
      })
      // `undefined` marks a turn that completed as something other than this
      // role's assistant reply; a bare throw here would become an unhandled
      // defect and kill the fiber past the role-failure handling below.
      const advance = (
        result: SessionLegacy.WithParts,
        index: number,
      ): Effect.Effect<SessionLegacy.WithParts | undefined> =>
        Effect.gen(function* () {
          const model = models[index]
          if (!model) return result
          const turn = yield* sessions.messageWithChildren({ sessionID: child.id, messageID: userMessageID })
          if (!shouldAdvanceModelFallback(turn, userMessageID)) return result
          const user = turn.find(
            (message): message is SessionLegacy.WithParts & { info: SessionLegacy.User } =>
              message.info.role === "user" && message.info.id === userMessageID,
          )
          if (!user) return result
          yield* sessions.updateMessage({
            ...user.info,
            model: {
              providerID: ProviderV2.ID.make(model.providerID),
              modelID: ProviderV2.ModelID.make(model.modelID),
              ...(model.variant && model.variant !== "default" ? { variant: model.variant } : {}),
            },
          })
          const next = yield* deps.loop({ sessionID: child.id, messageID: userMessageID })
          if (next.info.role !== "assistant" || next.info.parentID !== userMessageID) {
            return undefined
          }
          return yield* advance(next, index + 1)
        })
      const result = yield* advance(initial, 1)
      if (!result) return ClaudeDelegate.failure("errored")
      if (result.info.role === "assistant" && result.info.error) {
        return ClaudeDelegate.failure("errored")
      }
      const report = result.parts
        .flatMap((part) => (part.type === "text" && !part.synthetic && part.text.trim() ? [part.text.trim()] : []))
        .join("\n")
      if (!report) return { ok: false as const, reason: "empty-output" as const }
      return { ok: true as const, text: report }
    }).pipe(
      // Every exit settles the record: clean return, subagent error, typed
      // failure, defect, and interruption alike.
      Effect.onExit((exit) =>
        Exit.isSuccess(exit)
          ? settle(
              exit.value.ok ? "completed" : "errored",
              exit.value.ok ? exit.value.text : ClaudeDelegate.failureMessage(exit.value),
            )
          : Cause.hasInterruptsOnly(exit.cause)
            ? settle("cancelled")
            : settle("errored"),
      ),
      Effect.catch(Effect.die),
    )
    if (input.background && !deps.background) {
      // The tool contract promises background=true returns at once. Running
      // inline instead would freeze the orchestrator for the whole role - the
      // exact failure this mode exists to prevent - so refuse loudly.
      yield* settle("errored", "Background delegation is not available in this runtime.")
      return ClaudeDelegate.failure("rejected", "Background delegation is not available in this runtime.")
    }
    if (input.background && deps.background) {
      // Fire-and-forget under BackgroundJob so disposal can cancel it, then
      // wake the parent with the report. The parent is prompted through the
      // ordinary session loop, which for a Claude-hosted orchestrator opens a
      // real turn on its persistent channel - the same path that re-invokes
      // background pollers, and the reason a blocked tool call was never
      // needed to get a result back.
      const notify = Effect.fnUntraced(function* (state: "completed" | "error", text: string) {
        // Attributed to the parent's own agent, as the native task tool does:
        // an unattributed prompt would resolve the default agent's model and
        // could knock the orchestrator off the swarm facade.
        const currentParent = yield* sessions.get(input.sessionID).pipe(Effect.orDie)
        const wakeID = MessageID.ascending()
        yield* deps.prompt({
          sessionID: input.sessionID,
          messageID: wakeID,
          // Deferred: the default "immediate" delivery interrupts an in-flight
          // turn to steer it, which would abort whatever the orchestrator is
          // doing right now. A report queues behind the current turn.
          delivery: "deferred",
          ...(currentParent.agent ? { agent: currentParent.agent } : {}),
          parts: [
            {
              type: "text",
              synthetic: true,
              text: backgroundDelegationMessage({ childSessionID: child.id, role: role.name, state, text }),
            },
          ],
        })
        // If the parent was mid-turn, prompt() only awaited that run and the
        // wake message sat unanswered in the transcript; run its turn now so
        // the report is never silently orphaned.
        const answered = yield* sessions.messageWithChildren({ sessionID: input.sessionID, messageID: wakeID }).pipe(
          Effect.map((turn) =>
            turn.some((message) => message.info.role === "assistant" && message.info.parentID === wakeID),
          ),
          Effect.orElseSucceed(() => true),
        )
        if (!answered) yield* deps.loop({ sessionID: input.sessionID, messageID: wakeID })
      })
      const wake = (state: "completed" | "error", text: string) =>
        notify(state, text).pipe(
          Effect.matchCauseEffect({
            onSuccess: () =>
              sessions
                .stampDelegationDelivery({ sessionID: child.id, runID, outcome: "delivered" })
                .pipe(Effect.ignore),
            // A report that never reached the parent is a failed job, not a
            // completed one; the job status must not say otherwise.
            onFailure: () =>
              sessions
                .stampDelegationDelivery({ sessionID: child.id, runID, outcome: "failed" })
                .pipe(Effect.ignore, Effect.andThen(Effect.fail(new Error("Delegation report was not delivered")))),
          }),
        )
      yield* deps.background.start({
        id: child.id,
        type: "swarm-delegate",
        title: `${role.name} (swarm role)`,
        // The keys every BackgroundJob consumer reads: cancel-on-abort,
        // cancel-on-delete, and the parent loop's unfinished-work accounting.
        metadata: {
          parentSessionId: input.sessionID,
          sessionId: child.id,
          runID,
          swarmID: input.swarmID,
          role: role.name,
          background: true,
        },
        run: runRole.pipe(
          // Every exit wakes the parent: clean report, structured failure,
          // defect. Only an interruption stays silent (the parent asked for
          // it). Mirrors task.ts's matchCauseEffect around inject().
          Effect.matchCauseEffect({
            onSuccess: (result) =>
              result.ok
                ? wake("completed", result.text).pipe(Effect.as(result.text))
                : wake("error", ClaudeDelegate.failureMessage(result)).pipe(
                    Effect.andThen(Effect.fail(new Error(ClaudeDelegate.failureMessage(result)))),
                  ),
            onFailure: (cause) =>
              (Cause.hasInterruptsOnly(cause) ? Effect.void : wake("error", errorMessageOf(Cause.squash(cause)))).pipe(
                Effect.andThen(Effect.failCause(cause)),
              ),
          }),
        ),
      })
      return { ok: true as const, text: backgroundDelegationStarted(child.id, role.name) }
    }
    return yield* runRole
  })

  /**
   * Sessions on the "Claude subscription" model are answered by the local
   * Claude Code CLI instead of a provider API. Returns the work effect for
   * such a turn, or undefined for an ordinary session.
   */
  const claudeCodeTurn = Effect.fnUntraced(function* (sessionID: SessionID, messageID?: MessageID) {
    const session = yield* sessions.get(sessionID).pipe(Effect.orDie)
    const last = messageID ? yield* userMessage(sessionID, messageID) : yield* lastUserMessage(sessionID)
    const selected = last?.info.role === "user" ? last.info.model : session.model
    // A swarm is a facade over its orchestrator, so a swarm whose
    // orchestrator is the Claude subscription takes the driver path too -
    // with a delegation tool that keeps specialists on their own models.
    const swarm = isSwarmProvider(selected?.providerID ?? "") ? yield* swarmContext(sessionID, messageID) : undefined
    const orchestrator = swarm?.orchestrator
    const model =
      swarm?.orchestratorIsClaudeCode && orchestrator?.provider_id && orchestrator.model_id
        ? { providerID: orchestrator.provider_id, modelID: orchestrator.model_id }
        : selected
    const providerID = model?.providerID
    if (!providerID || !isClaudeCodeProvider(providerID)) return undefined
    if (!last || last.info.role !== "user") return undefined
    const promptText = last.parts
      .flatMap((part) => (part.type === "text" && part.text.trim() ? [part.text] : []))
      .join("\n")
      .trim()

    const attachments = prepareImages(last.parts)
    if (attachments.skipped.length > 0)
      log.warn("skipped unsupported swarm attachments", { reasons: attachments.skipped })
    // An image-only message has no text but is still a real turn.
    if (!promptText) {
      if (!attachments.hasImages) return undefined
      yield* ensureClaudeTitle(session, attachments.title)
    } else yield* ensureClaudeTitle(session, promptText)
    const specialists = swarm?.roles.slice(1) ?? []
    // Attribute the turn to the route the reader picked, so a swarm session
    // stays labelled with the team rather than the orchestrator's model. The
    // same attribution goes onto spawned sidechain children below: a child
    // mirrors a Claude subagent and never runs a model of its own, so letting
    // its user message fall through to default model resolution would label it
    // with whatever provider the reader last used elsewhere.
    const turnProviderID = selected?.providerID ?? providerID
    const turnModelID = (swarm ? swarm.swarmID : modelIdentifier(model)) ?? CLAUDE_CODE_DEFAULT_MODEL_ID
    const turnModel = {
      providerID: ProviderV2.ID.make(turnProviderID),
      modelID: ProviderV2.ModelID.make(turnModelID),
    }
    const execution = yield* SessionStatus.ExecutionGeneration
    return claudeDriver.runTurn({
      sessionID,
      parentMessageID: last.info.id,
      text: promptText,
      ...(attachments.images.length > 0 ? { images: attachments.images } : {}),
      directory: session.directory,
      providerID: turnProviderID,
      modelID: turnModelID,
      claudeModelID: modelIdentifier(model) ?? CLAUDE_CODE_DEFAULT_MODEL_ID,
      ...(execution?.sessionID === sessionID ? { executionGeneration: execution.generation } : {}),
      // "default" is the sentinel for "no variant" everywhere else in the loop.
      ...(selected?.variant && selected.variant !== "default" ? { variant: selected.variant } : {}),
      ...(specialists.length > 0
        ? {
            delegate: {
              roles: specialists.map((role) => ({
                name: role.name,
                description: role.skill ?? role.agent ?? undefined,
              })),
              run: (delegated) =>
                runSwarmRole({
                  sessionID,
                  swarmID: swarm!.swarmID,
                  roles: swarm!.roles,
                  role: delegated.role,
                  prompt: delegated.prompt,
                  ...(delegated.toolUseID ? { toolUseID: delegated.toolUseID } : {}),
                  ...(delegated.background ? { background: true } : {}),
                }),
            },
          }
        : {}),
      // Claude's subagents (Task tool calls) run as sidechains of the same
      // event stream, tagged with `parent_tool_use_id`. The driver's sidechain
      // router hands each one back here to become a real child session, so it
      // shows up in the session graph and transcript instead of leaking into
      // the orchestrator's own turn.
      sidechain: {
        spawn: (spawnInput: { title: string; prompt: string }) =>
          Effect.gen(function* () {
            // Avoid doubling up when the subagent's own title already says
            // "subagent" (e.g. "code-reviewer subagent"), which would otherwise
            // render as "code-reviewer subagent (@claude subagent)".
            const title = /subagent/i.test(spawnInput.title)
              ? spawnInput.title
              : `${spawnInput.title} (@claude subagent)`
            const child = yield* sessions
              .create({
                parentID: sessionID,
                title,
                ...(session.permission ? { permission: session.permission } : {}),
              })
              .pipe(Effect.orDie)
            const message = yield* prompt({
              sessionID: child.id,
              noReply: true,
              model: turnModel,
              parts: [{ type: "text", text: spawnInput.prompt || spawnInput.title }],
            }).pipe(Effect.orDie)
            return { sessionID: child.id, userMessageID: message.info.id }
          }),
      },
    })
  })

  /** User messages carry `modelID`; the session record carries `id`. */
  const modelIdentifier = (model?: { modelID?: string; id?: string }) => model?.modelID ?? model?.id

  /**
   * Claude Code has no small model to summarize with, so a session takes its
   * name from the opening request instead of an extra LLM call.
   */
  const ensureClaudeTitle = Effect.fnUntraced(function* (session: Session.Info, text: string) {
    if (session.title && !Session.isDefaultTitle(session.title)) return
    const line =
      text
        .split("\n")
        .find((value) => value.trim())
        ?.trim() ?? text.trim()
    const title = line.length > 60 ? `${line.slice(0, 60)}…` : line
    if (title) yield* sessions.setTitle({ sessionID: session.id, title }).pipe(Effect.ignore)
  })

  const lastUserMessage = Effect.fnUntraced(function* (sessionID: SessionID) {
    const match = yield* sessions.findMessage(sessionID, (message) => message.info.role === "user").pipe(Effect.orDie)
    const message = Option.getOrUndefined(match)
    return message ? claudeTurnMessage([message], undefined) : undefined
  })

  /** The message a queued command named for its own turn, if it's still a user message. */
  const userMessage = Effect.fnUntraced(function* (sessionID: SessionID, messageID: MessageID) {
    const match = yield* sessions.findMessage(sessionID, (message) => message.info.id === messageID).pipe(Effect.orDie)
    const message = Option.getOrUndefined(match)
    return message ? claudeTurnMessage([message], messageID) : undefined
  })

  return {
    ensureSwarmBriefing,
    swarmContext,
    runSwarmRole,
    claudeCodeTurn,
    ensureClaudeTitle,
    modelIdentifier,
    lastUserMessage,
    userMessage,
  }
}

function errorMessageOf(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}
