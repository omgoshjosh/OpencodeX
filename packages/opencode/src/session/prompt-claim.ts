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
  /**
   * In-process liveness of a session's external turn (a Claude CLI channel)
   * that the transcript cannot show: a backgrounded native subagent keeps the
   * turn open and asks for permissions while writing no message or part rows.
   * `backgroundWork` vetoes the stale settle outright - the channel bounds that
   * wait itself - and `lastActivityAt` counts as session activity.
   */
  readonly liveTurnWork?: (sessionID: SessionID) => { backgroundWork: boolean; lastActivityAt?: number } | undefined
  /**
   * Runs between a sweep's unlocked scan on the read connection and its write
   * phase on the writer, once per sweep step that found candidates. Tests only.
   */
  readonly beforeRecoveryWrite?: Effect.Effect<void>
  /**
   * Whether a `running` execution whose lease lapsed still has a live owner
   * (#51). A lapsed lease alone only proves a starved heartbeat; without this
   * seam the lapsed lease is taken at face value, as before.
   */
  readonly executionOwnerLive?: (input: { sessionID: SessionID; owner: string; generation: number }) => Effect.Effect<boolean>
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

type Reader = Pick<Database.Interface["db"], "select">
type SessionActivity = Effect.Success<ReturnType<typeof sessionActivity>>

/**
 * What the stale sweep judges a session by: its newest message and the latest
 * write to any of its messages or parts. Shaped so it runs the same way on the
 * read connection and again inside the settling transaction (OpencodeX-fs2
 * two-phase recovery); a turn that woke up in between changes one of these.
 * `session_execution.time_updated` is deliberately NOT part of it - that is
 * the renewing heartbeat that hides the stall.
 */
const sessionActivity = Effect.fnUntraced(function* (reader: Reader, sessionID: SessionID) {
  const message = yield* reader
    .select({ id: MessageTable.id, data: MessageTable.data, updated: MessageTable.time_updated })
    .from(MessageTable)
    .where(eq(MessageTable.session_id, sessionID))
    .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
    .limit(1)
    .get()
  const parts = yield* reader
    .select({ latest: max(PartTable.time_updated) })
    .from(PartTable)
    .where(eq(PartTable.session_id, sessionID))
    .get()
  const messages = yield* reader
    .select({ latest: max(MessageTable.time_updated) })
    .from(MessageTable)
    .where(eq(MessageTable.session_id, sessionID))
    .get()
  return { message, latest: Math.max(parts?.latest ?? 0, messages?.latest ?? 0) }
})

/** Compare-and-set between the two phases: the writer must still see what the unlocked scan saw. */
function sameActivity(current: SessionActivity, observed: SessionActivity) {
  return (
    current.message?.id === observed.message?.id &&
    current.message?.updated === observed.message?.updated &&
    current.latest === observed.latest
  )
}

/**
 * Durable admission for queued prompts. A prompt is a row in
 * `session_command`; exactly one process may run it at a time, which it proves
 * by holding a lease it heartbeats. Everything here is about winning, holding
 * and settling that lease — the turn itself is `loop`.
 */
export function make(deps: Deps) {
  return Effect.gen(function* () {
    const { database, events, scope, loop, beforeExecutionAdmission } = deps
    // `read` is the query_only connection (OpencodeX-fs2): the periodic sweeps
    // scan on it so a multi-second scan of a large database never holds the
    // writer's single permit. Under `OPENCODE_DB_SINGLE_CONNECTION=1` it IS the
    // writer and every statement below runs exactly where it always did.
    const { db, read } = database
    const beforeRecoveryWrite = deps.beforeRecoveryWrite ?? Effect.void
    const processRunID = ensureRunID()
    const commandOwner = `local:${process.pid}:${processRunID}:${crypto.randomUUID()}`
    const commandLeaseMillis = deps.commandLeaseMillis ?? 30_000
    const clock = deps.clock ?? Date.now
    const recoveryBatchSize = 32
    const recoveryInterval = deps.recoveryInterval ?? "20 seconds"
    const launching = new Set<string>()
    /** Commands whose turn is running in this instance, by claim generation (#51). */
    const executing = new Map<string, number>()

    /**
     * A lapsed lease on an execution only proves its heartbeat was late (#51).
     * It still blocks admission while its owner is live.
     */
    const executionBusy = Effect.fnUntraced(function* (
      sessionID: SessionID,
      execution: { state: string; owner: string | null; generation: number; leaseExpiresAt: number | null } | undefined,
      now: number,
    ) {
      if (execution?.state !== "running") return false
      if (execution.leaseExpiresAt && execution.leaseExpiresAt > now) return true
      if (!execution.owner || !deps.executionOwnerLive) return false
      return yield* deps.executionOwnerLive({ sessionID, owner: execution.owner, generation: execution.generation })
    })

    /** Whether a `running` command's owner still runs it, whatever its lease says (#51). */
    const commandOwnerLive = (commandID: string, owner: string, generation: number) => {
      if (owner === commandOwner) return executing.get(commandID) === generation
      const match = /^local:(\d+):([^:]+):/.exec(owner)
      if (!match || Number(match[1]) === process.pid) return false
      return SessionExecutionOwner.alive(owner, processRunID)
    }

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
        yield* Effect.logInfo("session command recovery").pipe(
          Effect.annotateLogs({ commandID, statusPresent: false, action, cas }),
        )
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
      yield* Effect.logInfo("session command recovery").pipe(
        Effect.annotateLogs({
          commandID,
          sessionID: command.session_id,
          commandAgeMillis: clock() - command.time_created,
          executionGeneration: execution?.generation,
          executionOwner: execution?.owner_id,
          executionLeaseExpiresAt: execution?.lease_expires_at,
          statusPresent: !!status,
          action,
          cas,
        }),
      )
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
                  generation: SessionExecutionTable.generation,
                  leaseExpiresAt: SessionExecutionTable.lease_expires_at,
                })
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, current.session_id))
                .get()
              if (yield* executionBusy(current.session_id, execution, now)) return { state: "waiting" as const }
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
              if (current.status === "running" && !reclaimDeadOwner && current.owner_id) {
                // The lease lapsed. Its owner may just have lost the DB to
                // contention for longer than the lease (#51): if it is still
                // running the turn, re-lease it for the owner and keep waiting.
                const ownerLive = commandOwnerLive(commandID, current.owner_id, current.claim_generation)
                yield* (ownerLive ? Effect.logWarning : Effect.logInfo)("session command lease reclaim decision").pipe(
                  Effect.annotateLogs({
                    commandID,
                    sessionID: current.session_id,
                    claimGeneration: current.claim_generation,
                    commandOwner: current.owner_id,
                    leaseAgeMillis: current.lease_expires_at === null ? undefined : now - current.lease_expires_at,
                    ownerLive,
                    action: ownerLive ? "keep" : "reclaim",
                  }),
                )
                if (ownerLive) {
                  yield* transaction
                    .update(SessionCommandTable)
                    .set({ lease_expires_at: now + commandLeaseMillis, time_updated: now })
                    .where(
                      and(
                        eq(SessionCommandTable.id, commandID),
                        eq(SessionCommandTable.status, "running"),
                        eq(SessionCommandTable.owner_id, current.owner_id),
                        eq(SessionCommandTable.claim_generation, current.claim_generation),
                      ),
                    )
                    .run()
                  return { state: "waiting" as const }
                }
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
            .select({
              state: SessionExecutionTable.state,
              owner: SessionExecutionTable.owner_id,
              generation: SessionExecutionTable.generation,
              leaseExpiresAt: SessionExecutionTable.lease_expires_at,
            })
            .from(SessionExecutionTable)
            .where(eq(SessionExecutionTable.session_id, sessionID))
            .get()
            .pipe(Effect.orDie),
        ],
        { concurrency: "unbounded" },
      )
      if (!command || ["succeeded", "failed", "cancelled"].includes(command.status)) return false
      return !(yield* executionBusy(sessionID, execution, clock()))
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
        yield* Effect.logInfo("session command recovery skipped missing session").pipe(
          Effect.annotateLogs({ commandID, sessionID: command.session_id, action: "skip" }),
        )
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

      const beatMillis = Math.floor(commandLeaseMillis / 3)
      let lastBeat = clock()
      executing.set(commandID, command.claim_generation)
      const heartbeat = yield* Effect.sleep(beatMillis).pipe(
        Effect.andThen(
          // Suspended so the clock is read on EVERY beat. Built eagerly, drizzle
          // bakes the first timestamp into the statement and every later beat
          // rewrites the same already-expiring lease.
          Effect.suspend(() => {
            const now = clock()
            // Late by more than one beat: the writer starved this heartbeat
            // (#51). The owner is live, so the claim path re-leases it anyway.
            const late =
              now - lastBeat > 2 * beatMillis
                ? Effect.logWarning("session command lease renewal late").pipe(
                    Effect.annotateLogs({
                      commandID,
                      sessionID: command.session_id,
                      claimGeneration: command.claim_generation,
                      lateMillis: now - lastBeat - beatMillis,
                    }),
                  )
                : Effect.void
            lastBeat = now
            return late.pipe(Effect.andThen(db
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
              .run()))
          }),
        ),
        // A failed beat (SQLITE_BUSY under contention) must not end the
        // heartbeat for the rest of the turn; the next beat retries.
        Effect.catch((error) =>
          Effect.logWarning("session command lease renewal failed").pipe(
            Effect.annotateLogs({ commandID, error: String(error) }),
          ),
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
        ).pipe(
          Effect.exit,
          Effect.ensuring(
            Fiber.interrupt(heartbeat).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (executing.get(commandID) === command.claim_generation) executing.delete(commandID)
                }),
              ),
            ),
          ),
        ),
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
        Effect.catchCause((cause) =>
          Effect.logError("prompt_async recovery failed").pipe(
            Effect.annotateLogs({ commandID, cause: Cause.pretty(cause) }),
          ),
        ),
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
      const children = yield* read
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
     * running, the session has no live re-wakeable delegation, its in-process
     * external turn (`liveTurnWork`) holds no live background work, and no
     * message, part, or in-process turn activity has happened for
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
      // Phase one, on the read connection with no lock held: find the runs
      // whose transcript proves the turn is over. Nothing here is atomic with
      // the settle below; phase two re-reads what this phase judged by.
      const candidates = yield* read
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
      const stale: Array<{
        candidate: (typeof candidates)[number] & { owner: string }
        activity: SessionActivity
        idleSince: number
      }> = []
      for (const candidate of candidates) {
        // Another live process's run is its own business; only this
        // instance can know its heartbeat outlived its work.
        const owner = candidate.owner
        if (!owner?.startsWith(`local:${process.pid}:${processRunID}:`)) continue
        const activity = yield* sessionActivity(read, candidate.sessionID).pipe(Effect.orDie)
        const message = activity.message
        // A newer user message means the next turn is already starting.
        if (!message || decodeAssistantMessage(message.data)._tag === "None") continue
        const parts = yield* read
          .select({ data: PartTable.data })
          .from(PartTable)
          .where(eq(PartTable.message_id, message.id))
          .orderBy(asc(PartTable.id))
          .all()
          .pipe(Effect.orDie)
        if (parts.some((part) => decodeBusyToolPart(part.data)._tag === "Some")) continue
        // Either proof that the model stopped talking. Nothing is appended
        // after a step-finish until the turn is woken again, so it being
        // the newest part is what separates "ended" from "mid-stream".
        const newest = parts.at(-1)
        const finished =
          (newest !== undefined && decodeStepFinishPart(newest.data)._tag === "Some") ||
          decodeFinishedAssistant(message.data)._tag === "Some"
        if (!finished) continue
        const liveWork = deps.liveTurnWork?.(candidate.sessionID)
        if (liveWork?.backgroundWork) continue
        const idleSince = Math.max(
          candidate.startedAt ?? 0,
          activity.latest,
          message.updated,
          liveWork?.lastActivityAt ?? 0,
        )
        if (now - idleSince < staleAfter) continue
        if (yield* hasLiveRewakeableDelegation(candidate.sessionID)) continue
        stale.push({ candidate: { ...candidate, owner }, activity, idleSince })
      }
      if (stale.length === 0) return
      yield* beforeRecoveryWrite
      for (const { candidate, activity, idleSince } of stale) {
        // The in-process turn is not part of the fingerprint below, so re-ask:
        // background work that started during the write gap still vetoes.
        if (deps.liveTurnWork?.(candidate.sessionID)?.backgroundWork) continue
        // Phase two, an immediate transaction on the writer. The session's
        // activity fingerprint must still be what phase one judged - a turn
        // that woke up in between (a delegation reporting back, a new
        // message, a tool part written) changes it - and the CAS on the exact
        // execution row means a concurrent settle, a new generation, or a
        // second sweep pass all lose here, so the notification below runs at
        // most once per stuck run.
        const settled = yield* db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                if (!sameActivity(yield* sessionActivity(transaction, candidate.sessionID), activity)) return false
                const row = yield* transaction
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
                if (!row) return false
                // The command that drove the finished turn is stuck the same way.
                // Queued siblings are untouched: they are real work that becomes
                // launchable now that the execution is free.
                yield* transaction
                  .update(SessionCommandTable)
                  .set({
                    status: "cancelled",
                    owner_id: null,
                    lease_expires_at: null,
                    completed_at: now,
                    time_updated: now,
                  })
                  .where(
                    and(
                      eq(SessionCommandTable.session_id, candidate.sessionID),
                      eq(SessionCommandTable.status, "running"),
                    ),
                  )
                  .run()
                return true
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.orDie)
        if (!settled) continue
        yield* Effect.logWarning("stale execution force-settled").pipe(
          Effect.annotateLogs({
            sessionID: candidate.sessionID,
            executionOwner: candidate.owner,
            executionGeneration: candidate.generation,
            messageID: activity.message?.id,
            idleMillis: now - idleSince,
            staleAfterMillis: staleAfter,
            action: "force-settle",
          }),
        )
        if (deps.onStaleExecution)
          yield* deps
            .onStaleExecution(candidate.sessionID)
            .pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("stale execution delegation settle failed").pipe(
                  Effect.annotateLogs({ sessionID: candidate.sessionID, cause: Cause.pretty(cause) }),
                ),
              ),
            )
      }
    })

    const recover = Effect.fn("SessionPrompt.recover")(function* () {
      const ctx = yield* InstanceState.context
      // First: a force-settled execution frees admission for the queued
      // commands this same pass is about to launch. Isolated so a watchdog
      // failure never costs the rest of the sweep.
      yield* sweepStaleExecutions().pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("stale execution sweep failed").pipe(Effect.annotateLogs({ cause: Cause.pretty(cause) })),
        ),
      )
      // Phase one, on the read connection: the sessions owed work and the
      // oldest launchable command in each. Nothing is claimed here - the
      // launch goes through `claimCommandTurn`, whose immediate transaction
      // re-reads the row on the writer and is the compare-and-set that a
      // command claimed or settled by someone else in between loses.
      const sessions = yield* read
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
      const launchable: string[] = []
      for (const session of sessions) {
        const command = yield* read
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
          .pipe(Effect.orDie)
        if (command) launchable.push(command.id)
      }
      if (launchable.length > 0) yield* beforeRecoveryWrite
      for (const commandID of launchable) {
        yield* diagnostic(commandID, "launch", "not-attempted").pipe(Effect.catchCause(() => Effect.void))
        yield* launchCommand(commandID)
      }
      // Diagnostic only. Bounded (#51): the old form joined message to
      // session and sorted the whole message table every sweep (20 s on a
      // 5.75 GB database). Now it starts from the directory's most recently
      // updated sessions and takes one index seek per session for its newest
      // message (message_session_time_created_id_idx). No migration needed.
      const recent = yield* read
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(eq(SessionTable.directory, ctx.directory))
        .orderBy(desc(SessionTable.time_updated), desc(SessionTable.id))
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      for (const session of recent) {
        const message = yield* read
          .select({ id: MessageTable.id, data: MessageTable.data, created: MessageTable.time_created })
          .from(MessageTable)
          .where(eq(MessageTable.session_id, session.id))
          .orderBy(desc(MessageTable.time_created), desc(MessageTable.id))
          .limit(1)
          .get()
          .pipe(Effect.orDie)
        if (!message || message.data.role !== "user") continue
        const command = yield* read
          .select({ id: SessionCommandTable.id })
          .from(SessionCommandTable)
          .where(and(eq(SessionCommandTable.session_id, session.id), eq(SessionCommandTable.message_id, message.id)))
          .get()
          .pipe(Effect.orDie)
        if (command) continue
        // Legacy transcript-only messages are observable but not recoverable;
        // repeated sweeps must not manufacture operator warnings for no-op work.
        yield* Effect.logDebug("session transcript has pending message without durable command").pipe(
          Effect.annotateLogs({
            messageID: message.id,
            sessionID: session.id,
            messageAgeMillis: clock() - message.created,
            action: "transcript-only",
            cas: "not-attempted",
          }),
          Effect.catchCause(() => Effect.void),
        )
      }
    })
    const recovery = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        yield* Effect.sleep(recoveryInterval).pipe(
          Effect.andThen(recover()),
          Effect.catchCause((cause) =>
            Effect.logWarning("session command recovery sweep failed").pipe(
              Effect.annotateLogs({ cause: Cause.pretty(cause) }),
            ),
          ),
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
