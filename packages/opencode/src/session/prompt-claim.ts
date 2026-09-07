import { and, asc, desc, eq, gt, inArray, isNull, lt, max, min, or } from "drizzle-orm"
import { Cause, Context, Duration, Effect, Exit, Fiber, Schedule, Schema, Scope } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import {
  MessageTable,
  PartTable,
  SessionCommandTable,
  SessionExecutionTable,
  SessionStatusTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { EventV2Bridge } from "@/event-v2-bridge"
import { delegationRecord, isLiveDelegation } from "./delegation-outcome"
import { InstanceState } from "@/effect/instance-state"
import { MessageID, SessionID } from "./schema"
import type { LoopInput } from "./prompt-schema"
import { SessionExecutionOwner } from "./execution-owner"
import * as Session from "./session"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly events: Context.Service.Shape<typeof EventV2Bridge.Service>
  readonly scope: Scope.Scope
  readonly loop: (input: LoopInput) => Effect.Effect<SessionLegacy.WithParts>
  /** Test seam: how long a claimed command's lease lasts. */
  readonly commandLeaseMillis?: number
  /** Test seam: the wall clock every lease decision reads. */
  readonly clock?: () => number
  readonly recoveryInterval?: Duration.Input
  readonly beforeExecutionAdmission?: (input: { sessionID: SessionID; commandID: string }) => Effect.Effect<void>
  /**
   * How long a finished-but-unsettled execution may keep renewing its lease
   * before the sweep force-settles it. Read per sweep so a config edit takes
   * effect without a restart; `experimental.stale_execution_timeout` supplies
   * it in production and tests inject a short one.
   */
  readonly staleExecutionMillis?: Effect.Effect<number>
  /**
   * Called after an execution is force-settled, so a delegated child still
   * reports to its parent through the durable delegation delivery path.
   */
  readonly onStaleExecution?: (sessionID: SessionID) => Effect.Effect<void>
}

/** Matches `experimental.stale_execution_timeout`'s documented default. */
export const STALE_EXECUTION_MILLIS = 600_000

/** A tool the turn has not finished with; part data is opaque JSON in SQL. */
const BusyToolPart = Schema.Struct({
  type: Schema.Literal("tool"),
  state: Schema.Struct({ status: Schema.Literals(["pending", "running"]) }),
})
const decodeBusyToolPart = Schema.decodeUnknownOption(BusyToolPart)

/** The newest message is the model's turn, not the next prompt already queued. */
const AssistantMessage = Schema.Struct({ role: Schema.Literal("assistant") })
const decodeAssistantMessage = Schema.decodeUnknownOption(AssistantMessage)

/** An assistant turn the mapper closed out with a completion timestamp. */
const FinishedAssistant = Schema.Struct({
  role: Schema.Literal("assistant"),
  time: Schema.Struct({ completed: Schema.Number }),
})
const decodeFinishedAssistant = Schema.decodeUnknownOption(FinishedAssistant)

/**
 * The turn's step-finish. `claude-mapper` writes this part the moment the model
 * stops, then only stamps `time.completed` on the message if nothing is left to
 * wake it - so on the stall this watchdog exists for, the step-finish part is
 * the ONLY durable record that the turn ended.
 */
const StepFinishPart = Schema.Struct({ type: Schema.Literal("step-finish") })
const decodeStepFinishPart = Schema.decodeUnknownOption(StepFinishPart)

/**
 * Durable admission for queued prompts. A prompt is a row in
 * `session_command`; exactly one process may run it at a time, which it proves
 * by holding a lease it heartbeats. Everything here is about winning, holding
 * and settling that lease — the turn itself is `loop`.
 */
export function make(deps: Deps) {
  return Effect.gen(function* () {
    const { database, events, scope, loop, beforeExecutionAdmission } = deps
    const { db } = database
    const processRunID = ensureRunID()
    const commandOwner = `local:${process.pid}:${processRunID}:${crypto.randomUUID()}`
    const commandLeaseMillis = deps.commandLeaseMillis ?? 30_000
    const clock = deps.clock ?? Date.now
    const recoveryBatchSize = 32
    const recoveryInterval = deps.recoveryInterval ?? "20 seconds"
    const launching = new Set<string>()

    const diagnostic = Effect.fnUntraced(function* (
      commandID: string,
      action: "launch" | "claim",
      cas: "not-attempted" | "ready" | "waiting" | "done",
    ) {
      const command = yield* db
        .select()
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, commandID))
        .get()
        .pipe(Effect.orDie)
      if (!command) {
        yield* Effect.logInfo("session command recovery", {
          commandID,
          statusPresent: false,
          action,
          cas,
        })
        return
      }
      const [execution, status] = yield* Effect.all(
        [
          db
            .select()
            .from(SessionExecutionTable)
            .where(eq(SessionExecutionTable.session_id, command.session_id))
            .get()
            .pipe(Effect.orDie),
          db
            .select({ sessionID: SessionStatusTable.session_id })
            .from(SessionStatusTable)
            .where(eq(SessionStatusTable.session_id, command.session_id))
            .get()
            .pipe(Effect.orDie),
        ],
        { concurrency: "unbounded" },
      )
      yield* Effect.logInfo("session command recovery", {
        commandID,
        commandAgeMillis: clock() - command.time_created,
        executionGeneration: execution?.generation,
        executionOwner: execution?.owner_id,
        executionLeaseExpiresAt: execution?.lease_expires_at,
        statusPresent: !!status,
        action,
        cas,
      })
    })

    const claimCommandTurn = Effect.fn("SessionPrompt.claimCommandTurn")(function* (commandID: string) {
      const now = clock()
      return yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionCommandTable)
                .where(eq(SessionCommandTable.id, commandID))
                .get()
              if (!current || ["succeeded", "failed", "cancelled"].includes(current.status)) {
                return { state: "done" as const }
              }
              const execution = yield* transaction
                .select({
                  state: SessionExecutionTable.state,
                  owner: SessionExecutionTable.owner_id,
                  leaseExpiresAt: SessionExecutionTable.lease_expires_at,
                })
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, current.session_id))
                .get()
              if (execution?.state === "running" && execution.leaseExpiresAt && execution.leaseExpiresAt > now) {
                return { state: "waiting" as const }
              }
              const parent = current.adopted_by
                ? yield* transaction
                    .select()
                    .from(SessionCommandTable)
                    .where(eq(SessionCommandTable.id, current.adopted_by))
                    .get()
                : undefined
              const parentLive =
                parent?.status === "running" &&
                parent.claim_generation === current.adopted_generation &&
                !!parent.owner_id &&
                parent.lease_expires_at !== null &&
                parent.lease_expires_at > now &&
                SessionExecutionOwner.alive(parent.owner_id, processRunID)
              // An adopted command belongs to a specific live parent claim.
              // Never let recovery clear or replay it while that claim holds
              // a valid lease, whether Claude has received the offer or not.
              if (parentLive) return { state: "waiting" as const }
              // A process can die after reserving an ordinary human command
              // but before writing it to Claude's streaming input. Once its
              // parent claim is stale, clear the fence and recover FIFO.
              if (current.status === "queued" && current.adopted_by && current.offered_at === null) {
                yield* transaction
                  .update(SessionCommandTable)
                  .set({ adopted_by: null, adopted_generation: null, offer_ordinal: null, time_updated: now })
                  .where(
                    and(
                      eq(SessionCommandTable.id, commandID),
                      eq(SessionCommandTable.status, "queued"),
                      eq(SessionCommandTable.adopted_by, current.adopted_by),
                      current.adopted_generation === null
                        ? isNull(SessionCommandTable.adopted_generation)
                        : eq(SessionCommandTable.adopted_generation, current.adopted_generation),
                      isNull(SessionCommandTable.offered_at),
                    ),
                  )
                  .run()
                return { state: "waiting" as const }
              }
              // An offered input may have reached Claude even if this process
              // died before observing its result. Never replay it on recovery.
              if (current.status === "queued" && current.offered_at !== null) {
                yield* transaction
                  .update(SessionCommandTable)
                  .set({
                    status: "failed",
                    error: "Live Claude offer outcome is unknown after recovery.",
                    completed_at: now,
                    time_updated: now,
                  })
                  .where(
                    and(
                      eq(SessionCommandTable.id, commandID),
                      eq(SessionCommandTable.status, "queued"),
                      current.adopted_by
                        ? eq(SessionCommandTable.adopted_by, current.adopted_by)
                        : isNull(SessionCommandTable.adopted_by),
                      current.adopted_generation === null
                        ? isNull(SessionCommandTable.adopted_generation)
                        : eq(SessionCommandTable.adopted_generation, current.adopted_generation),
                      eq(SessionCommandTable.offered_at, current.offered_at),
                    ),
                  )
                  .run()
                return { state: "done" as const }
              }
              const reclaimDeadOwner =
                current.status === "running" &&
                !!current.owner_id &&
                /^local:\d+:[^:]+:/.test(current.owner_id) &&
                !SessionExecutionOwner.alive(current.owner_id, processRunID)
              if (
                current.status === "running" &&
                current.lease_expires_at &&
                current.lease_expires_at > now &&
                !reclaimDeadOwner
              ) {
                return { state: "waiting" as const }
              }
              const active = yield* transaction
                .select({
                  id: SessionCommandTable.id,
                  created: SessionCommandTable.time_created,
                })
                .from(SessionCommandTable)
                .where(
                  and(
                    eq(SessionCommandTable.session_id, current.session_id),
                    inArray(SessionCommandTable.status, ["queued", "running"]),
                  ),
                )
                .all()
              const blocked = active.some(
                (item) =>
                  item.id !== current.id &&
                  (item.created < current.time_created ||
                    (item.created === current.time_created && item.id.localeCompare(current.id) < 0)),
              )
              if (blocked) return { state: "waiting" as const }
              const claimed = yield* transaction
                .update(SessionCommandTable)
                .set({
                  status: "running",
                  owner_id: commandOwner,
                  claim_generation: current.claim_generation + 1,
                  lease_expires_at: now + commandLeaseMillis,
                  started_at: current.started_at ?? now,
                  time_updated: now,
                })
                .where(
                  and(
                    eq(SessionCommandTable.id, commandID),
                    eq(SessionCommandTable.status, current.status),
                    eq(SessionCommandTable.claim_generation, current.claim_generation),
                    current.status === "running"
                      ? or(isNull(SessionCommandTable.lease_expires_at), lt(SessionCommandTable.lease_expires_at, now))
                      : undefined,
                  ),
                )
                .returning()
                .get()
              if (!claimed) return { state: "waiting" as const }
              return { state: "ready" as const, command: claimed }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const waitForExecutionTurn = Effect.fn("SessionPrompt.waitForExecutionTurn")(function* (
      commandID: string,
      sessionID: SessionID,
    ) {
      const [command, execution] = yield* Effect.all(
        [
          db
            .select({ status: SessionCommandTable.status })
            .from(SessionCommandTable)
            .where(eq(SessionCommandTable.id, commandID))
            .get()
            .pipe(Effect.orDie),
          db
            .select({ state: SessionExecutionTable.state, leaseExpiresAt: SessionExecutionTable.lease_expires_at })
            .from(SessionExecutionTable)
            .where(eq(SessionExecutionTable.session_id, sessionID))
            .get()
            .pipe(Effect.orDie),
        ],
        { concurrency: "unbounded" },
      )
      if (!command || ["succeeded", "failed", "cancelled"].includes(command.status)) return false
      return execution?.state !== "running" || !execution.leaseExpiresAt || execution.leaseExpiresAt <= clock()
    })

    const executeCommand = Effect.fn("SessionPrompt.executeCommand")(function* (commandID: string) {
      const claimed = yield* claimCommandTurn(commandID)
      yield* diagnostic(commandID, "claim", claimed.state).pipe(Effect.catchCause(() => Effect.void))
      if (claimed.state !== "ready") return
      const command = claimed.command
      // A session deleted while its command was still queued leaves an orphan
      // row. Running it anyway reaches Session.patch, which reads the session
      // inside a transaction and dies with NotFound - once per sweep, forever.
      // Settle the orphan instead so recovery stays quiet.
      const session = yield* db
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(eq(SessionTable.id, command.session_id))
        .get()
        .pipe(Effect.orDie)
      if (!session) {
        const skippedAt = clock()
        yield* Effect.logInfo("session command recovery skipped missing session", {
          commandID,
          sessionID: command.session_id,
          action: "skip",
        })
        yield* db
          .update(SessionCommandTable)
          .set({
            status: "cancelled",
            owner_id: null,
            lease_expires_at: null,
            completed_at: skippedAt,
            time_updated: skippedAt,
          })
          .where(
            and(
              eq(SessionCommandTable.id, commandID),
              eq(SessionCommandTable.status, "running"),
              eq(SessionCommandTable.owner_id, commandOwner),
              eq(SessionCommandTable.claim_generation, command.claim_generation),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        return
      }
      const requeue = Effect.fnUntraced(function* () {
        const completedAt = clock()
        yield* db
          .update(SessionCommandTable)
          .set({
            status: "queued",
            owner_id: null,
            lease_expires_at: null,
            error: null,
            completed_at: null,
            time_updated: completedAt,
          })
          .where(
            and(
              eq(SessionCommandTable.id, commandID),
              eq(SessionCommandTable.status, "running"),
              eq(SessionCommandTable.owner_id, commandOwner),
              eq(SessionCommandTable.claim_generation, command.claim_generation),
            ),
          )
          .run()
          .pipe(Effect.orDie)
      })
      if (beforeExecutionAdmission) yield* beforeExecutionAdmission({ sessionID: command.session_id, commandID })
      if (!(yield* waitForExecutionTurn(commandID, command.session_id))) {
        yield* requeue()
        return
      }
      const admitted = yield* db
        .select({ id: SessionCommandTable.id })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.id, commandID),
            eq(SessionCommandTable.status, "running"),
            eq(SessionCommandTable.owner_id, commandOwner),
            eq(SessionCommandTable.claim_generation, command.claim_generation),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (!admitted) return

      const heartbeat = yield* Effect.sleep(Math.floor(commandLeaseMillis / 3)).pipe(
        Effect.andThen(
          // Suspended so the clock is read on EVERY beat. Built eagerly, drizzle
          // bakes the first timestamp into the statement and every later beat
          // rewrites the same already-expiring lease.
          Effect.suspend(() => {
            const now = clock()
            return db
              .update(SessionCommandTable)
              .set({ lease_expires_at: now + commandLeaseMillis, time_updated: now })
              .where(
                and(
                  eq(SessionCommandTable.id, commandID),
                  eq(SessionCommandTable.status, "running"),
                  eq(SessionCommandTable.owner_id, commandOwner),
                  eq(SessionCommandTable.claim_generation, command.claim_generation),
                ),
              )
              .run()
              .pipe(Effect.orDie)
          }),
        ),
        Effect.repeat(Schedule.forever),
        Effect.forkIn(scope),
      )
      const exit = yield* Effect.uninterruptibleMask((restore) =>
        restore(
          loop({
            sessionID: command.session_id,
            messageID: command.message_id,
            commandID,
            claimGeneration: command.claim_generation,
            claimOwner: commandOwner,
          }).pipe(Effect.onInterrupt(() => requeue())),
        ).pipe(Effect.exit, Effect.ensuring(Fiber.interrupt(heartbeat))),
      )
      const completedAt = clock()
      // A turn that returns an errored assistant message is a FAILED command,
      // not a succeeded one: the effect succeeded, but the work did not. The
      // error itself was already published by the loop, so this only records
      // the durable outcome (an abort is a cancellation, not a failure).
      const assistantError =
        Exit.isSuccess(exit) &&
        exit.value?.info.role === "assistant" &&
        exit.value.info.error &&
        exit.value.info.error.name !== "MessageAbortedError"
          ? JSON.stringify(exit.value.info.error)
          : undefined
      if (Exit.isSuccess(exit)) {
        yield* db
          .update(SessionCommandTable)
          .set({
            status: assistantError ? "failed" : "succeeded",
            owner_id: null,
            lease_expires_at: null,
            ...(assistantError ? { error: assistantError } : {}),
            completed_at: completedAt,
            time_updated: completedAt,
          })
          .where(
            and(
              eq(SessionCommandTable.id, commandID),
              eq(SessionCommandTable.status, "running"),
              eq(SessionCommandTable.owner_id, commandOwner),
              eq(SessionCommandTable.claim_generation, command.claim_generation),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        return
      }

      if (Cause.hasInterruptsOnly(exit.cause)) {
        yield* requeue()
        return
      }

      const error = Cause.pretty(exit.cause)
      yield* db
        .update(SessionCommandTable)
        .set({
          status: "failed",
          owner_id: null,
          lease_expires_at: null,
          error,
          completed_at: completedAt,
          time_updated: completedAt,
        })
        .where(
          and(
            eq(SessionCommandTable.id, commandID),
            eq(SessionCommandTable.status, "running"),
            eq(SessionCommandTable.owner_id, commandOwner),
            eq(SessionCommandTable.claim_generation, command.claim_generation),
          ),
        )
        .run()
        .pipe(Effect.orDie)
      yield* Effect.logError("prompt_async failed").pipe(
        Effect.annotateLogs({ sessionID: command.session_id, cause: exit.cause }),
      )
      yield* events.publish(Session.Event.Error, {
        sessionID: command.session_id,
        error: new NamedError.Unknown({ message: error }).toObject(),
      })
    })

    let wakeQueued = (_sessionID: SessionID): Effect.Effect<void> => Effect.void
    const launchCommand = Effect.fn("SessionPrompt.launchCommand")(function* (commandID: string) {
      if (launching.has(commandID)) return
      launching.add(commandID)
      yield* executeCommand(commandID).pipe(
        Effect.andThen(
          db
            .select({ sessionID: SessionCommandTable.session_id, status: SessionCommandTable.status })
            .from(SessionCommandTable)
            .where(eq(SessionCommandTable.id, commandID))
            .get()
            .pipe(
              Effect.orDie,
              Effect.flatMap((command) =>
                command && ["succeeded", "failed", "cancelled"].includes(command.status)
                  ? wakeQueued(command.sessionID)
                  : Effect.void,
              ),
            ),
        ),
        Effect.catchCause((cause) => Effect.logError("prompt_async recovery failed", { commandID, cause })),
        Effect.ensuring(Effect.sync(() => launching.delete(commandID))),
        Effect.forkIn(scope, { startImmediately: true }),
      )
    })

    /**
     * Launches every command this session still owes work on: anything queued,
     * plus a `running` row whose lease has lapsed (its owner died). Pin-only
     * entry point used by `promptAsync`, so a newly queued or re-queued prompt
     * starts without waiting for the next recovery sweep.
     */
    const wakeSession = Effect.fn("SessionPrompt.wakeSession")(function* (sessionID: SessionID) {
      const now = clock()
      const commands = yield* db
        .select({ id: SessionCommandTable.id })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.session_id, sessionID),
            or(
              eq(SessionCommandTable.status, "queued"),
              and(
                eq(SessionCommandTable.status, "running"),
                or(isNull(SessionCommandTable.lease_expires_at), lt(SessionCommandTable.lease_expires_at, now)),
              ),
            ),
          ),
        )
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(commands, (command) => launchCommand(command.id), { discard: true })
    })
    wakeQueued = wakeSession

    /**
     * Whether the session is legitimately paused waiting on a delegation that
     * will wake it back up.
     *
     * A backgrounded subagent leaves the parent with the same signature as the
     * stall: no busy tool part, no new writes, no `time.completed` - the CLI is
     * holding the stream open until the child reports. Requiring
     * `time.completed` used to exclude these by accident; now it is explicit.
     *
     * "Re-wakeable" is the child that reports back into the model's turn, which
     * durably is a delegation record still in `running` whose owner process is
     * alive - the same pair `/session/status` treats as a live background job.
     * `monitoring` is deliberately excluded: that phase begins after the tool
     * call already returned to the model, so nothing is waiting on it.
     */
    const hasLiveRewakeableDelegation = Effect.fnUntraced(function* (sessionID: SessionID) {
      const children = yield* db
        .select({ metadata: SessionTable.metadata })
        .from(SessionTable)
        .where(eq(SessionTable.parent_id, sessionID))
        .all()
        .pipe(Effect.orDie)
      return children.some((child) => {
        const record = delegationRecord(child.metadata)
        return record?.phase === "running" && isLiveDelegation(record, processRunID)
      })
    })

    /**
     * Force-settles an execution whose turn is demonstrably over but whose row
     * never left `running`.
     *
     * The expired-lease reclaim above cannot see these: the owner is this live
     * process and its heartbeat keeps renewing `lease_expires_at`, so the row
     * looks busy forever. `/global/restart-readiness` counts exactly that shape
     * as active work, so one stuck row blocks every cutover indefinitely.
     *
     * Settling is safe only with durable proof that nothing is in flight, so
     * every condition below must hold: the session's newest message is an
     * assistant turn that ended (its newest part is a step-finish, or it
     * recorded `time.completed`), none of its tool parts is still pending or
     * running, the session has no live re-wakeable delegation, and no message
     * or part in the session has been written or updated for
     * `staleExecutionMillis`. `session_execution.time_updated` is deliberately
     * NOT an activity signal - it is the renewing heartbeat that hides the stall.
     *
     * `time.completed` alone is too narrow to catch this bug: the mapper stamps
     * it only when the turn is truly over, and the stall's whole shape is a
     * step-finished turn that never got that stamp.
     */
    const sweepStaleExecutions = Effect.fn("SessionPrompt.sweepStaleExecutions")(function* () {
      const ctx = yield* InstanceState.context
      const staleAfter = yield* deps.staleExecutionMillis ?? Effect.succeed(STALE_EXECUTION_MILLIS)
      if (staleAfter <= 0) return
      const now = clock()
      const candidates = yield* db
        .select({
          sessionID: SessionExecutionTable.session_id,
          owner: SessionExecutionTable.owner_id,
          generation: SessionExecutionTable.generation,
          startedAt: SessionExecutionTable.started_at,
        })
        .from(SessionExecutionTable)
        .where(
          and(
            eq(SessionExecutionTable.directory, ctx.directory),
            eq(SessionExecutionTable.state, "running"),
            // Only a lease that is still being renewed. An expired one is the
            // existing reclaim's work, and racing it would replay a turn.
            gt(SessionExecutionTable.lease_expires_at, now),
          ),
        )
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(
        candidates,
        (candidate) =>
          Effect.gen(function* () {
            // Another live process's run is its own business; only this
            // instance can know its heartbeat outlived its work.
            if (!candidate.owner?.startsWith(`local:${process.pid}:${processRunID}:`)) return
            const message = yield* db
              .select({ id: MessageTable.id, data: MessageTable.data, updated: MessageTable.time_updated })
              .from(MessageTable)
              .where(eq(MessageTable.session_id, candidate.sessionID))
              .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
              .limit(1)
              .get()
              .pipe(Effect.orDie)
            // A newer user message means the next turn is already starting.
            if (!message || decodeAssistantMessage(message.data)._tag === "None") return
            const parts = yield* db
              .select({ data: PartTable.data })
              .from(PartTable)
              .where(eq(PartTable.message_id, message.id))
              .orderBy(asc(PartTable.id))
              .all()
              .pipe(Effect.orDie)
            if (parts.some((part) => decodeBusyToolPart(part.data)._tag === "Some")) return
            // Either proof that the model stopped talking. Nothing is appended
            // after a step-finish until the turn is woken again, so it being
            // the newest part is what separates "ended" from "mid-stream".
            const newest = parts.at(-1)
            const finished =
              (newest !== undefined && decodeStepFinishPart(newest.data)._tag === "Some") ||
              decodeFinishedAssistant(message.data)._tag === "Some"
            if (!finished) return
            const activity = yield* Effect.all(
              [
                db
                  .select({ latest: max(PartTable.time_updated) })
                  .from(PartTable)
                  .where(eq(PartTable.session_id, candidate.sessionID))
                  .get()
                  .pipe(Effect.orDie),
                db
                  .select({ latest: max(MessageTable.time_updated) })
                  .from(MessageTable)
                  .where(eq(MessageTable.session_id, candidate.sessionID))
                  .get()
                  .pipe(Effect.orDie),
              ],
              { concurrency: "unbounded" },
            )
            const idleSince = Math.max(
              candidate.startedAt ?? 0,
              ...activity.map((row) => row?.latest ?? 0),
              message.updated,
            )
            if (now - idleSince < staleAfter) return
            if (yield* hasLiveRewakeableDelegation(candidate.sessionID)) return
            // CAS on the exact row this pass inspected: a concurrent settle,
            // a new generation, or a second sweep pass all lose here, so the
            // notification below runs at most once per stuck run.
            const settled = yield* db
              .update(SessionExecutionTable)
              .set({
                // The transcript shows a clean finish, so this is the state a
                // normally-released turn ends in (`SessionRunState.release`
                // with `interrupted: false`). There is no `completed` state in
                // this vocabulary and `interrupted` would falsely claim the
                // work was aborted.
                state: "idle",
                owner_id: null,
                lease_expires_at: null,
                completed_at: now,
                time_updated: now,
              })
              .where(
                and(
                  eq(SessionExecutionTable.session_id, candidate.sessionID),
                  eq(SessionExecutionTable.state, "running"),
                  eq(SessionExecutionTable.owner_id, candidate.owner),
                  eq(SessionExecutionTable.generation, candidate.generation),
                ),
              )
              .returning({ sessionID: SessionExecutionTable.session_id })
              .get()
              .pipe(Effect.orDie)
            if (!settled) return
            // The command that drove the finished turn is stuck the same way.
            // Queued siblings are untouched: they are real work that becomes
            // launchable now that the execution is free.
            yield* db
              .update(SessionCommandTable)
              .set({
                status: "cancelled",
                owner_id: null,
                lease_expires_at: null,
                completed_at: now,
                time_updated: now,
              })
              .where(
                and(eq(SessionCommandTable.session_id, candidate.sessionID), eq(SessionCommandTable.status, "running")),
              )
              .run()
              .pipe(Effect.orDie)
            yield* Effect.logWarning("stale execution force-settled", {
              sessionID: candidate.sessionID,
              executionOwner: candidate.owner,
              executionGeneration: candidate.generation,
              messageID: message.id,
              idleMillis: now - idleSince,
              staleAfterMillis: staleAfter,
              action: "force-settle",
            })
            if (deps.onStaleExecution)
              yield* deps.onStaleExecution(candidate.sessionID).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("stale execution delegation settle failed", {
                    sessionID: candidate.sessionID,
                    cause,
                  }),
                ),
              )
          }),
        { discard: true },
      )
    })

    const recover = Effect.fn("SessionPrompt.recover")(function* () {
      const ctx = yield* InstanceState.context
      // First: a force-settled execution frees admission for the queued
      // commands this same pass is about to launch. Isolated so a watchdog
      // failure never costs the rest of the sweep.
      yield* sweepStaleExecutions().pipe(
        Effect.catchCause((cause) => Effect.logWarning("stale execution sweep failed", { cause })),
      )
      const sessions = yield* db
        .select({ sessionID: SessionCommandTable.session_id, oldest: min(SessionCommandTable.time_created) })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.directory, ctx.directory),
            or(
              and(eq(SessionCommandTable.status, "queued"), isNull(SessionCommandTable.owner_id)),
              and(eq(SessionCommandTable.status, "running"), lt(SessionCommandTable.lease_expires_at, clock())),
            ),
          ),
        )
        .groupBy(SessionCommandTable.session_id)
        .orderBy(asc(min(SessionCommandTable.time_created)), asc(SessionCommandTable.session_id))
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(
        sessions,
        (session) =>
          db
            .select({ id: SessionCommandTable.id })
            .from(SessionCommandTable)
            .where(
              and(
                eq(SessionCommandTable.session_id, session.sessionID),
                eq(SessionCommandTable.directory, ctx.directory),
                or(
                  and(eq(SessionCommandTable.status, "queued"), isNull(SessionCommandTable.owner_id)),
                  and(eq(SessionCommandTable.status, "running"), lt(SessionCommandTable.lease_expires_at, clock())),
                ),
              ),
            )
            .orderBy(asc(SessionCommandTable.time_created), asc(SessionCommandTable.id))
            .limit(1)
            .get()
            .pipe(
              Effect.orDie,
              Effect.flatMap((command) =>
                command
                  ? diagnostic(command.id, "launch", "not-attempted").pipe(
                      Effect.catchCause(() => Effect.void),
                      Effect.andThen(launchCommand(command.id)),
                    )
                  : Effect.void,
              ),
            ),
        { discard: true },
      )
      const messages = yield* db
        .select({
          id: MessageTable.id,
          sessionID: MessageTable.session_id,
          data: MessageTable.data,
          created: MessageTable.time_created,
          commandID: SessionCommandTable.id,
        })
        .from(MessageTable)
        .innerJoin(SessionTable, eq(SessionTable.id, MessageTable.session_id))
        .leftJoin(
          SessionCommandTable,
          and(
            eq(SessionCommandTable.session_id, MessageTable.session_id),
            eq(SessionCommandTable.message_id, MessageTable.id),
          ),
        )
        .where(eq(SessionTable.directory, ctx.directory))
        .orderBy(desc(MessageTable.time_created))
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      for (const message of messages) {
        if (message.data.role !== "user" || message.commandID) continue
        if (
          messages.some(
            (other) =>
              other.sessionID === message.sessionID &&
              other.created > message.created &&
              other.data.role === "assistant",
          )
        )
          continue
        // Legacy transcript-only messages are observable but not recoverable;
        // repeated sweeps must not manufacture operator warnings for no-op work.
        yield* Effect.logDebug("session transcript has pending message without durable command", {
          messageID: message.id,
          sessionID: message.sessionID,
          messageAgeMillis: clock() - message.created,
          action: "transcript-only",
          cas: "not-attempted",
        }).pipe(Effect.catchCause(() => Effect.void))
      }
    })
    const recovery = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        yield* Effect.sleep(recoveryInterval).pipe(
          Effect.andThen(recover()),
          Effect.catchCause((cause) => Effect.logWarning("session command recovery sweep failed", { cause })),
          Effect.repeat(Schedule.forever),
          Effect.forkScoped,
        )
      }),
    )
    const start = Effect.fn("SessionPrompt.startRecovery")(function* () {
      yield* InstanceState.get(recovery)
    })
    yield* Effect.addFinalizer(() =>
      db
        .update(SessionCommandTable)
        .set({
          status: "queued",
          owner_id: null,
          lease_expires_at: null,
          error: null,
          completed_at: null,
          time_updated: clock(),
        })
        .where(and(eq(SessionCommandTable.status, "running"), eq(SessionCommandTable.owner_id, commandOwner)))
        .run()
        .pipe(Effect.orDie),
    )

    /**
     * Cancels a message that is still waiting in the queue (OpencodeX-8vq).
     * A human who queued a prompt behind a long turn could not take it back:
     * the only lever was abort, which kills the turn that is actually
     * working. Only a `queued` row can be withdrawn - once a command is
     * running its turn owns it, and stopping that is what abort is for.
     */
    const cancelCommand = Effect.fn("SessionPrompt.cancelCommand")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const now = clock()
      return yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionCommandTable)
                .where(
                  and(
                    eq(SessionCommandTable.session_id, input.sessionID),
                    eq(SessionCommandTable.message_id, input.messageID),
                  ),
                )
                .get()
              if (!current) return "missing" as const
              if (current.status !== "queued") return "settled" as const
              if (current.offered_at !== null) return "running" as const
              yield* transaction
                .update(SessionCommandTable)
                .set({ status: "cancelled", completed_at: now, time_updated: now })
                .where(and(eq(SessionCommandTable.id, current.id), eq(SessionCommandTable.status, "queued")))
                .run()
              return "cancelled" as const
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    return {
      cancelCommand,
      commandOwner,
      commandLeaseMillis,
      claimCommandTurn,
      waitForExecutionTurn,
      executeCommand,
      launchCommand,
      wakeSession,
      recover,
      sweepStaleExecutions,
      start,
    }
  })
}

export * as PromptClaim from "./prompt-claim"
