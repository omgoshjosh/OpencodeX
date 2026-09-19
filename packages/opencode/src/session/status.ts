import { InstanceState } from "@/effect/instance-state"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { NonNegativeInt } from "@opencode-ai/core/schema"
import {
  SessionExecutionTable,
  SessionInteractionTable,
  SessionStatusTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { and, eq, inArray } from "drizzle-orm"
import { Cause, Context, Duration, Effect, Layer, Option, Schedule, Schema } from "effect"
import { isSqlError, isSqlErrorReason } from "effect/unstable/sql/SqlError"
import { SessionID } from "./schema"
import { SessionExecutionOwner } from "./execution-owner"
import { delegationRecord, type DelegationRecord } from "./delegation-outcome"
import { SessionInteractionRecovery } from "./interaction-recovery"

/**
 * What a parent's delegated background children are doing right now.
 *
 * `running` answers the only question a client actually asks - "is anything
 * still working for me?" - so it counts working jobs alone. A child parked on
 * a question is not working (nobody is spending tokens; a human is), and a
 * finished child whose report has not been handed over yet is not working
 * either. Both still belong in `jobs`, because they are outstanding.
 */
const Background = Schema.Struct({
  running: Schema.Boolean,
  jobs: Schema.Array(
    Schema.Struct({
      id: SessionID,
      sessionID: SessionID,
      status: Schema.Literals(["running", "blocked", "completed"]),
      role: Schema.String,
      title: Schema.String,
      owner: Schema.String,
      /** Set once the run settled; the delegation record's own completion time. */
      completedAt: Schema.optional(NonNegativeInt),
      /** Whether the parent has durably received the report. Never `delivered` here. */
      delivery: Schema.optional(Schema.Literals(["pending", "delivering", "delivered", "failed"])),
    }),
  ),
})
type BackgroundJob = Schema.Schema.Type<typeof Background>["jobs"][number]

export const Info = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("idle"),
    background: Schema.optional(Background),
  }),
  Schema.Struct({
    type: Schema.Literal("retry"),
    attempt: NonNegativeInt,
    message: Schema.String,
    next: NonNegativeInt,
    background: Schema.optional(Background),
  }),
  Schema.Struct({
    type: Schema.Literal("busy"),
    background: Schema.optional(Background),
  }),
  Schema.Struct({
    type: Schema.Literal("blocked"),
    background: Schema.optional(Background),
    childSessionID: SessionID,
    attemptedModels: Schema.Array(Schema.String),
    error: Schema.String,
    retryAt: Schema.optional(NonNegativeInt),
  }),
  Schema.Struct({
    type: Schema.Literal("monitoring"),
    background: Schema.optional(Background),
    childSessionID: Schema.optional(SessionID),
    monitorID: Schema.optional(Schema.String),
    since: Schema.optional(NonNegativeInt),
    checkAfter: Schema.optional(NonNegativeInt),
  }),
]).annotate({ identifier: "SessionStatus" })
export type Info = Schema.Schema.Type<typeof Info>

export const Event = {
  Status: EventV2.define({
    type: "session.status",
    sync: {
      aggregate: "sessionID",
      version: 1,
    },
    schema: {
      sessionID: SessionID,
      status: Info,
    },
  }),
  // deprecated
  Idle: EventV2.define({
    type: "session.idle",
    schema: {
      sessionID: SessionID,
    },
  }),
}

export interface Interface {
  readonly recover: (sessionID?: SessionID) => Effect.Effect<void>
  readonly get: (sessionID: SessionID) => Effect.Effect<Info>
  readonly list: () => Effect.Effect<Map<SessionID, Info>>
  readonly snapshot: () => Effect.Effect<Map<SessionID, Info>>
  readonly set: (sessionID: SessionID, status: Info) => Effect.Effect<void>
  readonly setForGeneration: (
    sessionID: SessionID,
    generation: number,
    status: Info,
    owner?: string,
  ) => Effect.Effect<boolean>
  readonly refresh: (sessionID: SessionID) => Effect.Effect<void>
  readonly claimBlockedRetry: (input: { sessionID: SessionID; childSessionID: SessionID }) => Effect.Effect<boolean>
  readonly settleMonitoring: (input: {
    sessionID: SessionID
    monitorID: string
    status: Exclude<Info, { type: "monitoring" }>
  }) => Effect.Effect<boolean>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStatus") {}

export interface LayerOptions {
  readonly beforeGenerationStatusWrite?: Effect.Effect<void>
  readonly onGenerationStatusLockTimeout?: Effect.Effect<void>
  /** Runs between `recover`'s unlocked scan and its write transaction. Tests only. */
  readonly beforeRecoverWrite?: Effect.Effect<void>
}

class Options extends Context.Service<Options, LayerOptions>()("@opencode/SessionStatusOptions") {}

export const ExecutionGeneration = Context.Reference<
  { sessionID: SessionID; generation: number; owner: string } | undefined
>("@opencode/SessionStatus/ExecutionGeneration", { defaultValue: () => undefined })

const decode = Schema.decodeUnknownOption(Info)
const OWNERLESS_STALE_MILLIS = 15_000

function isLockTimeout(error: unknown) {
  if (isSqlError(error) && isSqlErrorReason(error.reason) && error.reason._tag === "LockTimeoutError") return true
  if (
    typeof error !== "object" ||
    error === null ||
    !("_tag" in error) ||
    error._tag !== "EffectDrizzleQueryError" ||
    !("cause" in error) ||
    !Cause.isCause(error.cause)
  )
    return false
  return (
    error.cause.reasons.length === 1 &&
    Cause.isFailReason(error.cause.reasons[0]) &&
    isSqlError(error.cause.reasons[0].error) &&
    isSqlErrorReason(error.cause.reasons[0].error.reason) &&
    error.cause.reasons[0].error.reason._tag === "LockTimeoutError"
  )
}

function isLockTimeoutCause(cause: Cause.Cause<unknown>) {
  return cause.reasons.length === 1 && Cause.isFailReason(cause.reasons[0]) && isLockTimeout(cause.reasons[0].error)
}

/**
 * Durable terminal evidence on the record itself. The settle boundary stamps
 * all three together, but any one of them is a run that has ended: a reader
 * must never advertise work as live while the record it is reading says the
 * work is over.
 */
function settledDelegation(record: DelegationRecord) {
  return record.phase === "settled" || record.outcome !== undefined || record.completedAt !== undefined
}

/**
 * The child's own execution row proving this run's turn already returned, for
 * a record whose settle stamp never landed (a swallowed stamp, or a job that
 * died between the turn and its exit boundary). Owner liveness cannot see
 * this: the owner is the daemon, which outlives its children by days.
 *
 * Scoped to `running`, so a `monitoring` record - whose local execution is
 * *expected* to be idle while a durable external job runs - keeps its job.
 * `completedAt` must post-date the run's start, or a reused child session's
 * previous turn would retire the new one.
 */
function returnedLocally(
  record: DelegationRecord,
  execution: { state: string; completedAt: number | null } | undefined,
) {
  if (record.phase !== "running" || !execution) return false
  if (execution.state === "running" || execution.state === "queued") return false
  return execution.completedAt !== null && execution.completedAt >= record.startedAt
}

type StatusRow = typeof SessionStatusTable.$inferSelect
type ExecutionRow = typeof SessionExecutionTable.$inferSelect
type StaleCandidate = { status: StatusRow; execution: ExecutionRow | undefined }
type Reader = Pick<Database.Interface["db"], "select">

/**
 * The stale-status scan, shaped so it runs the same way on the read connection
 * (no barrier, no lock) and again inside the immediate transaction that idles
 * the survivors. `sessionIDs` restricts it to the rows a caller already knows
 * about; `now` is the caller's clock so both passes judge staleness alike.
 */
const scanStale = Effect.fnUntraced(function* (
  reader: Reader,
  sessionIDs: readonly SessionID[] | undefined,
  now: number,
) {
  const processRunID = ensureRunID()
  const statusQuery = reader.select().from(SessionStatusTable)
  const statuses = (yield* (
    sessionIDs ? statusQuery.where(inArray(SessionStatusTable.session_id, sessionIDs)) : statusQuery
  ).all()).filter((row) => {
    const status = Option.getOrUndefined(decode(row.status))
    return status?.type === "busy" || status?.type === "retry"
  })
  if (statuses.length === 0) return [] as StaleCandidate[]
  const executionQuery = reader.select().from(SessionExecutionTable)
  const executions = new Map(
    (yield* (
      sessionIDs ? executionQuery.where(inArray(SessionExecutionTable.session_id, sessionIDs)) : executionQuery
    ).all()).map((row) => [row.session_id, row]),
  )
  return statuses.flatMap((row): StaleCandidate[] => {
    const execution = executions.get(row.session_id)
    const stale = !execution
      ? row.time_updated + OWNERLESS_STALE_MILLIS <= now
      : execution.state !== "running" ||
        !execution.owner_id ||
        !execution.lease_expires_at ||
        execution.lease_expires_at <= now ||
        !SessionExecutionOwner.alive(execution.owner_id, processRunID)
    return stale ? [{ status: row, execution }] : []
  })
})

/**
 * Compare-and-set between the two recovery phases: the row inside the write
 * transaction must still be the row the unlocked scan judged stale. Any renewal
 * in between - a heartbeat extending the lease, a new generation claiming the
 * session, a fresh status write - changes one of these fields.
 */
function unchanged(current: StaleCandidate, observed: StaleCandidate | undefined) {
  if (!observed || current.status.time_updated !== observed.status.time_updated) return false
  if (!current.execution || !observed.execution) return current.execution === observed.execution
  return (
    current.execution.generation === observed.execution.generation &&
    current.execution.lease_expires_at === observed.execution.lease_expires_at &&
    current.execution.time_updated === observed.execution.time_updated
  )
}

const configuredLayer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const options = yield* Options
    const events = yield* EventV2Bridge.Service
    const { db, read } = yield* Database.Service
    const processRunID = ensureRunID()

    // Sidebar paints and status lists are read-only projections, so they run
    // on the read connection: a full session scan here must not queue the
    // barriered writers behind the writer's single permit.
    const backgrounds = Effect.fnUntraced(function* () {
      const rows = yield* read
        .select({ id: SessionTable.id, metadata: SessionTable.metadata, title: SessionTable.title })
        .from(SessionTable)
        .all()
        .pipe(Effect.orDie)
      // A dead owner still retires a job outright - it is just no longer the
      // only way out. Everything past this point is a live owner's delegation,
      // which is exactly the case that used to project `running` forever.
      const candidates = rows.flatMap((row) => {
        const record = delegationRecord(row.metadata)
        if (!record?.background || !record.ownerID) return []
        if (!SessionExecutionOwner.alive(record.ownerID, processRunID)) return []
        return [{ row, record }]
      })
      const result = new Map<SessionID, BackgroundJob[]>()
      if (candidates.length === 0) return result
      const ids = candidates.map((candidate) => candidate.row.id)
      // Only the handful of children a live owner is still carrying, so these
      // stay indexed lookups rather than the two extra table scans a blanket
      // read would cost every time a client paints a sidebar.
      const [executions, questions] = yield* Effect.all([
        read
          .select({
            sessionID: SessionExecutionTable.session_id,
            state: SessionExecutionTable.state,
            completedAt: SessionExecutionTable.completed_at,
          })
          .from(SessionExecutionTable)
          .where(inArray(SessionExecutionTable.session_id, ids))
          .all()
          .pipe(Effect.orDie),
        read
          .select({ sessionID: SessionInteractionTable.session_id })
          .from(SessionInteractionTable)
          .where(
            and(
              inArray(SessionInteractionTable.session_id, ids),
              eq(SessionInteractionTable.kind, "question"),
              eq(SessionInteractionTable.state, "pending"),
            ),
          )
          .all()
          .pipe(Effect.orDie),
      ])
      const executionByChild = new Map(executions.map((row) => [row.sessionID, row]))
      const asking = new Set(questions.map((row) => row.sessionID))
      for (const { row, record } of candidates) {
        const identity = {
          id: row.id,
          sessionID: row.id,
          role: record.role ?? "Background task",
          title: record.title ?? row.title,
          owner: record.ownerID!,
        }
        const job = settledDelegation(record)
          ? // A settled run is the parent's business only until its report has
            // actually been handed over; anything else is finished business.
            record.deliveryOutcome && record.deliveryOutcome !== "delivered"
            ? ({
                ...identity,
                status: "completed",
                ...(record.completedAt !== undefined ? { completedAt: record.completedAt } : {}),
                delivery: record.deliveryOutcome,
              } as const)
            : undefined
          : // Task 27's durable question request is the authority on a parked
            // child; nothing here has to be inferred from a quiet transcript.
            asking.has(row.id)
            ? ({ ...identity, status: "blocked" } as const)
            : returnedLocally(record, executionByChild.get(row.id))
              ? undefined
              : ({ ...identity, status: "running" } as const)
        if (!job) continue
        const parentID = SessionID.make(record.parentSessionID)
        result.set(parentID, [...(result.get(parentID) ?? []), job])
      }
      return result
    })

    const withBackground = (status: Info, jobs: BackgroundJob[] | undefined): Info =>
      jobs?.length
        ? {
            ...status,
            background: {
              running: jobs.some((job) => job.status === "running"),
              jobs: jobs.toSorted((a, b) => a.id.localeCompare(b.id)),
            },
          }
        : status

    // `sessionID` scopes the scan to one row. Reads take that path so a status
    // lookup stays an indexed point query instead of two full table scans for
    // something as routine as painting the sidebar; the periodic sweep below
    // still covers sessions nobody is looking at.
    //
    // Two phases (OpencodeX-fs2): the scan runs on the read connection with no
    // barrier, so on a large database it neither queues the writer nor holds
    // the process-wide permit while it scans. Only the candidates it finds go
    // through the barrier and an immediate transaction, where the same scan
    // runs again against the writer and each row is compared with what the
    // first phase saw: a session renewed in between (new generation, extended
    // lease, fresh status write) no longer matches and is left alone.
    //
    // Under the kill switch (`read === db`) there is no second connection to
    // scan on, so both phases run under the barrier exactly as before; the
    // barrier is reentrant, so the write phase's own acquisition is free.
    const guarded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      read === db ? events.barrier(effect, "SessionStatus.recover") : effect
    const recover = Effect.fn("SessionStatus.recover")(function* (sessionID?: SessionID) {
      const now = Date.now()
      const broadcasts = yield* guarded(
        Effect.gen(function* () {
          const observed = yield* scanStale(read, sessionID ? [sessionID] : undefined, now).pipe(Effect.orDie)
          if (observed.length === 0) return [] as EventV2.Payload[]
          yield* options.beforeRecoverWrite ?? Effect.void
          return yield* events.barrier(
            db
              .transaction(
                (transaction) =>
                  Effect.gen(function* () {
                    const current = yield* scanStale(
                      transaction,
                      observed.map((candidate) => candidate.status.session_id),
                      now,
                    )
                    const stale = current.filter((candidate) =>
                      unchanged(
                        candidate,
                        observed.find((item) => item.status.session_id === candidate.status.session_id),
                      ),
                    )
                    const result: EventV2.Payload[] = []
                    for (const { status: row, execution } of stale) {
                      if (execution) {
                        yield* transaction
                          .update(SessionExecutionTable)
                          .set({
                            state: "interrupted",
                            owner_id: null,
                            lease_expires_at: null,
                            completed_at: now,
                            time_updated: now,
                          })
                          .where(
                            and(
                              eq(SessionExecutionTable.session_id, row.session_id),
                              eq(SessionExecutionTable.generation, execution.generation),
                            ),
                          )
                          .run()
                      }
                      yield* transaction
                        .update(SessionStatusTable)
                        .set({ status: { type: "idle" }, time_updated: now })
                        .where(eq(SessionStatusTable.session_id, row.session_id))
                        .run()
                      result.push(
                        yield* events.commit(Event.Status, { sessionID: row.session_id, status: { type: "idle" } }),
                        yield* events.commit(Event.Idle, { sessionID: row.session_id }),
                      )
                    }
                    return result
                  }),
                { behavior: "immediate" },
              )
              .pipe(Effect.orDie),
          )
        }),
      )
      yield* Effect.forEach(broadcasts, events.broadcast, { discard: true })
      yield* SessionInteractionRecovery.recoverWith({ database: { db, read }, events, sessionID })
    })

    const get = Effect.fn("SessionStatus.get")(function* (sessionID: SessionID) {
      yield* recover(sessionID)
      const row = yield* db
        .select({ status: SessionStatusTable.status })
        .from(SessionStatusTable)
        .where(eq(SessionStatusTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      const status = row
        ? Option.getOrElse(decode(row.status), () => ({ type: "idle" as const }))
        : { type: "idle" as const }
      return withBackground(status, (yield* backgrounds()).get(sessionID))
    })

    const snapshot = Effect.fn("SessionStatus.snapshot")(function* () {
      const rows = yield* read
        .select({ sessionID: SessionStatusTable.session_id, status: SessionStatusTable.status })
        .from(SessionStatusTable)
        .all()
        .pipe(Effect.orDie)
      const result = new Map<SessionID, Info>(
        rows.flatMap((row) => {
          const status = Option.getOrUndefined(decode(row.status))
          return status && status.type !== "idle" ? [[row.sessionID, status] as const] : []
        }),
      )
      for (const [sessionID, jobs] of yield* backgrounds()) {
        result.set(sessionID, withBackground(result.get(sessionID) ?? { type: "idle" }, jobs))
      }
      return result
    })

    const list = Effect.fn("SessionStatus.list")(function* () {
      yield* recover()
      return yield* snapshot()
    })

    const write = Effect.fnUntraced(function* (
      sessionID: SessionID,
      status: Info,
      generation?: number,
      retryLockTimeout = false,
      owner?: string,
    ) {
      const ctx = yield* InstanceState.context
      const now = Date.now()
      const { background: _background, ...persisted } = status
      const attempt = () =>
        events.barrier(
          db.transaction(
            (transaction) =>
              Effect.gen(function* () {
                if (generation !== undefined) {
                  const execution = yield* transaction
                    .select({
                      generation: SessionExecutionTable.generation,
                      state: SessionExecutionTable.state,
                      owner: SessionExecutionTable.owner_id,
                      cancelRequestedAt: SessionExecutionTable.cancel_requested_at,
                    })
                    .from(SessionExecutionTable)
                    .where(eq(SessionExecutionTable.session_id, sessionID))
                    .get()
                  if (execution?.generation !== generation) return undefined
                  if (
                    status.type !== "idle" &&
                    (execution.state !== "running" || execution.owner !== owner || execution.cancelRequestedAt)
                  )
                    return undefined
                }
                // A blocked child and external monitoring are independently
                // owned states. Releasing the local execution must not erase
                // either state after it has been persisted.
                if (status.type === "idle") {
                  const current = yield* transaction
                    .select({ status: SessionStatusTable.status })
                    .from(SessionStatusTable)
                    .where(eq(SessionStatusTable.session_id, sessionID))
                    .get()
                  const existing = current && Option.getOrUndefined(decode(current.status))
                  if (existing?.type === "blocked" || existing?.type === "monitoring") return undefined
                }
                yield* transaction
                  .insert(SessionStatusTable)
                  .values({
                    session_id: sessionID,
                    project_id: ctx.project.id,
                    directory: ctx.directory,
                    status: persisted,
                    time_created: now,
                    time_updated: now,
                  })
                  .onConflictDoUpdate({
                    target: SessionStatusTable.session_id,
                    set: {
                      project_id: ctx.project.id,
                      directory: ctx.directory,
                      status: persisted,
                      time_updated: now,
                    },
                  })
                  .run()
                return {
                  status: yield* events.commit(Event.Status, { sessionID, status: persisted }),
                  idle: status.type === "idle" ? yield* events.commit(Event.Idle, { sessionID }) : undefined,
                }
              }),
            { behavior: "immediate" },
          ),
        )
      if (retryLockTimeout) yield* options.beforeGenerationStatusWrite ?? Effect.void
      const committed = yield* attempt().pipe(
        Effect.catchCause((cause) => {
          if (!retryLockTimeout || !isLockTimeoutCause(cause)) return Effect.failCause(cause)
          return (options.onGenerationStatusLockTimeout ?? Effect.void).pipe(Effect.andThen(attempt()))
        }),
        Effect.orDie,
      )
      if (!committed) return false
      yield* events.broadcast(committed.status)
      if (committed.idle) yield* events.broadcast(committed.idle)
      return true
    })

    const setForGeneration = Effect.fn("SessionStatus.setForGeneration")(function* (
      sessionID: SessionID,
      generation: number,
      status: Info,
      owner?: string,
    ) {
      return yield* write(sessionID, status, generation, true, owner)
    })

    const set = Effect.fn("SessionStatus.set")(function* (sessionID: SessionID, status: Info) {
      const generation = yield* ExecutionGeneration
      if (generation?.sessionID === sessionID) {
        yield* setForGeneration(sessionID, generation.generation, status, generation.owner)
        return
      }
      yield* write(sessionID, status)
    })

    const refresh = Effect.fn("SessionStatus.refresh")(function* (sessionID: SessionID) {
      yield* events.publish(Event.Status, { sessionID, status: yield* get(sessionID) })
    })

    const claimBlockedRetry = Effect.fn("SessionStatus.claimBlockedRetry")(function* (input: {
      sessionID: SessionID
      childSessionID: SessionID
    }) {
      const now = Date.now()
      const committed = yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                const current = yield* transaction
                  .select({ status: SessionStatusTable.status })
                  .from(SessionStatusTable)
                  .where(eq(SessionStatusTable.session_id, input.sessionID))
                  .get()
                const status = current && Option.getOrUndefined(decode(current.status))
                if (status?.type !== "blocked" || status.childSessionID !== input.childSessionID) return undefined
                yield* transaction
                  .update(SessionStatusTable)
                  .set({ status: { type: "busy" }, time_updated: now })
                  .where(eq(SessionStatusTable.session_id, input.sessionID))
                  .run()
                return yield* events.commit(Event.Status, { sessionID: input.sessionID, status: { type: "busy" } })
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.orDie),
      )
      if (!committed) return false
      yield* events.broadcast(committed)
      return true
    })

    const settleMonitoring = Effect.fn("SessionStatus.settleMonitoring")(function* (input: {
      sessionID: SessionID
      monitorID: string
      status: Exclude<Info, { type: "monitoring" }>
    }) {
      const now = Date.now()
      const committed = yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                const current = yield* transaction
                  .select({ status: SessionStatusTable.status })
                  .from(SessionStatusTable)
                  .where(eq(SessionStatusTable.session_id, input.sessionID))
                  .get()
                const status = current && Option.getOrUndefined(decode(current.status))
                if (status?.type !== "monitoring" || status.monitorID !== input.monitorID) return undefined
                yield* transaction
                  .update(SessionStatusTable)
                  .set({ status: input.status, time_updated: now })
                  .where(eq(SessionStatusTable.session_id, input.sessionID))
                  .run()
                return {
                  status: yield* events.commit(Event.Status, { sessionID: input.sessionID, status: input.status }),
                  idle:
                    input.status.type === "idle"
                      ? yield* events.commit(Event.Idle, { sessionID: input.sessionID })
                      : undefined,
                }
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.orDie),
      )
      if (!committed) return false
      yield* events.broadcast(committed.status)
      if (committed.idle) yield* events.broadcast(committed.idle)
      return true
    })

    // Recovery reconciles rows whose owning execution died. It used to run on
    // every read, which meant two full table scans inside an immediate
    // transaction under the event barrier for something as routine as painting
    // the sidebar. A dead owner's status can now be stale for at most one
    // interval, which no reader can distinguish from the process dying a moment
    // later anyway.
    yield* recover()
    yield* Effect.sleep(Duration.seconds(15)).pipe(
      Effect.andThen(recover()),
      // A failed sweep (e.g. SQLITE_BUSY outliving the busy timeout) must not
      // kill the loop for the life of the process; reads still recover inline,
      // but sessions nobody reads would never reconcile again.
      Effect.catchCause((cause) =>
        Effect.logWarning("session status sweep failed").pipe(Effect.annotateLogs({ cause: Cause.pretty(cause) })),
      ),
      Effect.repeat(Schedule.forever),
      Effect.forkScoped,
    )

    return Service.of({
      recover,
      get,
      list,
      snapshot,
      set,
      setForGeneration,
      refresh,
      claimBlockedRetry,
      settleMonitoring,
    })
  }),
)

export const layerWithOptions = (options: LayerOptions = {}) =>
  configuredLayer.pipe(Layer.provide(Layer.succeed(Options, options)))

export const layer = layerWithOptions()

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(EventV2Bridge.defaultLayer))

export * as SessionStatus from "./status"
