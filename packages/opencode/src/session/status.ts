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
import { Context, Duration, Effect, Layer, Option, Schedule, Schema } from "effect"
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
  readonly setForGeneration: (sessionID: SessionID, generation: number, status: Info) => Effect.Effect<boolean>
  readonly refresh: (sessionID: SessionID) => Effect.Effect<void>
  readonly claimBlockedRetry: (input: { sessionID: SessionID; childSessionID: SessionID }) => Effect.Effect<boolean>
  readonly settleMonitoring: (input: {
    sessionID: SessionID
    monitorID: string
    status: Exclude<Info, { type: "monitoring" }>
  }) => Effect.Effect<boolean>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStatus") {}

export const ExecutionGeneration = Context.Reference<{ sessionID: SessionID; generation: number } | undefined>(
  "@opencode/SessionStatus/ExecutionGeneration",
  { defaultValue: () => undefined },
)

const decode = Schema.decodeUnknownOption(Info)
const OWNERLESS_STALE_MILLIS = 15_000

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

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const { db } = yield* Database.Service
    const processRunID = ensureRunID()

    const backgrounds = Effect.fnUntraced(function* () {
      const rows = yield* db
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
        db
          .select({
            sessionID: SessionExecutionTable.session_id,
            state: SessionExecutionTable.state,
            completedAt: SessionExecutionTable.completed_at,
          })
          .from(SessionExecutionTable)
          .where(inArray(SessionExecutionTable.session_id, ids))
          .all()
          .pipe(Effect.orDie),
        db
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
    // lookup stays an indexed point query instead of two full table scans in an
    // immediate transaction under the event barrier; the periodic sweep below
    // still covers sessions nobody is looking at.
    const recover = Effect.fn("SessionStatus.recover")(function* (sessionID?: SessionID) {
      const now = Date.now()
      const broadcasts = yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                const statusQuery = transaction.select().from(SessionStatusTable)
                const statuses = (yield* (
                  sessionID ? statusQuery.where(eq(SessionStatusTable.session_id, sessionID)) : statusQuery
                ).all()).filter((row) => {
                  const status = Option.getOrUndefined(decode(row.status))
                  return status?.type === "busy" || status?.type === "retry"
                })
                if (statuses.length === 0) return [] as EventV2.Payload[]
                const executionQuery = transaction.select().from(SessionExecutionTable)
                const executions = new Map(
                  (yield* (
                    sessionID ? executionQuery.where(eq(SessionExecutionTable.session_id, sessionID)) : executionQuery
                  ).all()).map((row) => [row.session_id, row]),
                )
                const stale = statuses.filter((row) => {
                  const execution = executions.get(row.session_id)
                  if (!execution) return row.time_updated + OWNERLESS_STALE_MILLIS <= now
                  if (execution.state !== "running" || !execution.owner_id) return true
                  if (!execution.lease_expires_at || execution.lease_expires_at <= now) return true
                  return !SessionExecutionOwner.alive(execution.owner_id, processRunID)
                })
                const result: EventV2.Payload[] = []
                for (const row of stale) {
                  const execution = executions.get(row.session_id)
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
      yield* Effect.forEach(broadcasts, events.broadcast, { discard: true })
      yield* SessionInteractionRecovery.recoverWith({ database: { db }, events, sessionID })
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
      const rows = yield* db
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

    const write = Effect.fnUntraced(function* (sessionID: SessionID, status: Info, generation?: number) {
      const ctx = yield* InstanceState.context
      const now = Date.now()
      const { background: _background, ...persisted } = status
      const committed = yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                if (generation !== undefined) {
                  const execution = yield* transaction
                    .select({ generation: SessionExecutionTable.generation, state: SessionExecutionTable.state })
                    .from(SessionExecutionTable)
                    .where(eq(SessionExecutionTable.session_id, sessionID))
                    .get()
                  if (execution?.generation !== generation) return undefined
                  if (status.type !== "idle" && execution.state !== "running") return undefined
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
          )
          .pipe(Effect.orDie),
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
    ) {
      return yield* write(sessionID, status, generation)
    })

    const set = Effect.fn("SessionStatus.set")(function* (sessionID: SessionID, status: Info) {
      const generation = yield* ExecutionGeneration
      if (generation?.sessionID === sessionID) {
        yield* setForGeneration(sessionID, generation.generation, status)
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
      Effect.catchCause((cause) => Effect.logWarning("session status sweep failed", { cause })),
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

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(EventV2Bridge.defaultLayer))

export * as SessionStatus from "./status"
