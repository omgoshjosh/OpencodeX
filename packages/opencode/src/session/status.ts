import { InstanceState } from "@/effect/instance-state"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { NonNegativeInt } from "@opencode-ai/core/schema"
import { SessionExecutionTable, SessionStatusTable } from "@opencode-ai/core/session/sql"
import { OpencodeXJobTable } from "@opencode-ai/core/opencodex/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { and, eq, inArray } from "drizzle-orm"
import { Context, Duration, Effect, Layer, Option, Schedule, Schema, Scope } from "effect"
import { SessionID } from "./schema"
import { SessionExecutionOwner } from "./execution-owner"

export const Info = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("idle"),
    pendingWake: Schema.optional(
      Schema.Struct({
        at: NonNegativeInt,
        jobID: Schema.String,
        reason: Schema.optional(Schema.String),
      }),
    ),
  }),
  Schema.Struct({
    type: Schema.Literal("retry"),
    attempt: NonNegativeInt,
    message: Schema.String,
    next: NonNegativeInt,
  }),
  Schema.Struct({
    type: Schema.Literal("busy"),
    since: Schema.optional(NonNegativeInt),
    lastActivityAt: Schema.optional(NonNegativeInt),
    runningTool: Schema.optional(
      Schema.Struct({
        title: Schema.String,
        startedAt: NonNegativeInt,
      }),
    ),
    pendingWake: Schema.optional(
      Schema.Struct({
        at: NonNegativeInt,
        jobID: Schema.String,
        reason: Schema.optional(Schema.String),
      }),
    ),
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
  readonly get: (sessionID: SessionID) => Effect.Effect<Info>
  readonly list: () => Effect.Effect<Map<SessionID, Info>>
  readonly set: (sessionID: SessionID, status: Info) => Effect.Effect<void>
  readonly setForGeneration: (sessionID: SessionID, generation: number, status: Info) => Effect.Effect<boolean>
  readonly activity: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly flushActivity: (sessionID: SessionID, generation?: number) => Effect.Effect<boolean>
  readonly toolStart: (sessionID: SessionID, title: string) => Effect.Effect<boolean>
  readonly toolEnd: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly setPendingWake: (
    sessionID: SessionID,
    pendingWake?: { jobID: string; reason?: string },
  ) => Effect.Effect<boolean>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStatus") {}

export const ExecutionGeneration = Context.Reference<{ sessionID: SessionID; generation: number } | undefined>(
  "@opencode/SessionStatus/ExecutionGeneration",
  { defaultValue: () => undefined },
)

const decode = Schema.decodeUnknownOption(Info)
const ACTIVITY_FLUSH_MILLIS = 250

function normalize(status: Info, current: Info | undefined, now: number): Info {
  if (status.type !== "busy") return status
  const previous = current?.type === "busy" ? current : undefined
  return {
    ...previous,
    ...status,
    since: status.since ?? previous?.since ?? now,
    lastActivityAt: status.lastActivityAt ?? now,
  }
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    const { db } = yield* Database.Service
    const scope = yield* Scope.Scope
    const processRunID = ensureRunID()
    const activityPending = yield* InstanceState.make(() => Effect.sync(() => new Map<SessionID, { at: number }>()))

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
                  // Status is presentation state, never execution authority. A
                  // busy row without an execution lease is a crashed/restarted
                  // writer, not proof that work is still running.
                  if (!execution) return true
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
    })

    const get = Effect.fn("SessionStatus.get")(function* (sessionID: SessionID) {
      yield* recover(sessionID)
      const row = yield* db
        .select({ status: SessionStatusTable.status })
        .from(SessionStatusTable)
        .where(eq(SessionStatusTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      return row ? Option.getOrElse(decode(row.status), () => ({ type: "idle" as const })) : { type: "idle" as const }
    })

    const list = Effect.fn("SessionStatus.list")(function* () {
      yield* recover()
      const rows = yield* db
        .select({ sessionID: SessionStatusTable.session_id, status: SessionStatusTable.status })
        .from(SessionStatusTable)
        .all()
        .pipe(Effect.orDie)
      return new Map(
        rows.flatMap((row) => {
          const status = Option.getOrUndefined(decode(row.status))
          return status && status.type !== "idle" ? [[row.sessionID, status] as const] : []
        }),
      )
    })

    const write = Effect.fnUntraced(function* (
      sessionID: SessionID,
      status: Info | ((current: Info | undefined, now: number) => Info | undefined),
      generation?: number,
    ) {
      const ctx = yield* InstanceState.context
      const now = Date.now()
      const committed = yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                const existing = yield* transaction
                  .select({ status: SessionStatusTable.status })
                  .from(SessionStatusTable)
                  .where(eq(SessionStatusTable.session_id, sessionID))
                  .get()
                const current = existing ? Option.getOrUndefined(decode(existing.status)) : undefined
                if (generation !== undefined) {
                  const execution = yield* transaction
                    .select({ generation: SessionExecutionTable.generation, state: SessionExecutionTable.state })
                    .from(SessionExecutionTable)
                    .where(eq(SessionExecutionTable.session_id, sessionID))
                    .get()
                  if (execution?.generation !== generation) return undefined
                  if ((typeof status === "function" || status.type !== "idle") && execution.state !== "running")
                    return undefined
                }
                const next = typeof status === "function" ? status(current, now) : normalize(status, current, now)
                if (!next) return undefined
                yield* transaction
                  .insert(SessionStatusTable)
                  .values({
                    session_id: sessionID,
                    project_id: ctx.project.id,
                    directory: ctx.directory,
                    status: next,
                    time_created: now,
                    time_updated: now,
                  })
                  .onConflictDoUpdate({
                    target: SessionStatusTable.session_id,
                    set: {
                      project_id: ctx.project.id,
                      directory: ctx.directory,
                      status: next,
                      time_updated: now,
                    },
                  })
                  .run()
                return {
                  status: yield* events.commit(Event.Status, { sessionID, status: next }),
                  idle: next.type === "idle" ? yield* events.commit(Event.Idle, { sessionID }) : undefined,
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
      if (status.type === "idle") yield* flushActivity(sessionID, generation)
      return yield* write(sessionID, status, generation)
    })

    const set = Effect.fn("SessionStatus.set")(function* (sessionID: SessionID, status: Info) {
      const generation = yield* ExecutionGeneration
      if (generation?.sessionID === sessionID) {
        yield* setForGeneration(sessionID, generation.generation, status)
        return
      }
      if (status.type === "idle") yield* flushActivity(sessionID)
      yield* write(sessionID, status)
    })

    const patchCurrentGeneration = Effect.fnUntraced(function* (
      sessionID: SessionID,
      patch: (current: Info, now: number) => Info | undefined,
    ) {
      const generation = yield* ExecutionGeneration
      return yield* write(
        sessionID,
        (current, now) => (current ? patch(current, now) : undefined),
        generation?.sessionID === sessionID ? generation.generation : undefined,
      )
    })

    const flushActivity = Effect.fn("SessionStatus.flushActivity")(function* (
      sessionID: SessionID,
      generation?: number,
    ) {
      const pending = yield* InstanceState.get(activityPending)
      const current = pending.get(sessionID)
      if (!current) return false
      const flushed = yield* write(
        sessionID,
        (status) =>
          status?.type === "busy"
            ? { ...status, lastActivityAt: Math.max(status.lastActivityAt ?? 0, current.at) }
            : undefined,
        generation,
      )
      if (pending.get(sessionID) === current) pending.delete(sessionID)
      return flushed
    })

    const activity = Effect.fn("SessionStatus.activity")(function* (sessionID: SessionID) {
      const pending = yield* InstanceState.get(activityPending)
      const now = Date.now()
      const current = pending.get(sessionID)
      if (current) {
        current.at = now
        return false
      }
      pending.set(sessionID, { at: now })
      yield* Effect.gen(function* () {
        yield* Effect.sleep(ACTIVITY_FLUSH_MILLIS)
        yield* flushActivity(sessionID, (yield* ExecutionGeneration)?.generation)
      }).pipe(Effect.forkIn(scope))
      return false
    })

    const toolStart = Effect.fn("SessionStatus.toolStart")(function* (sessionID: SessionID, title: string) {
      return yield* patchCurrentGeneration(sessionID, (current, now) =>
        current.type === "busy"
          ? { ...current, lastActivityAt: now, runningTool: { title, startedAt: now } }
          : undefined,
      )
    })

    const toolEnd = Effect.fn("SessionStatus.toolEnd")(function* (sessionID: SessionID) {
      return yield* patchCurrentGeneration(sessionID, (current, now) => {
        if (current.type !== "busy") return undefined
        const { runningTool: _runningTool, ...next } = current
        return { ...next, lastActivityAt: now }
      })
    })

    const setPendingWake = Effect.fn("SessionStatus.setPendingWake")(function* (
      sessionID: SessionID,
      pendingWake?: { jobID: string; reason?: string },
    ) {
      const wake = pendingWake
        ? yield* db
            .select({ timeoutAt: OpencodeXJobTable.timeout_at })
            .from(OpencodeXJobTable)
            .where(
              and(
                eq(OpencodeXJobTable.id, pendingWake.jobID),
                eq(OpencodeXJobTable.session_id, sessionID),
                inArray(OpencodeXJobTable.status, ["queued", "claimed", "running"]),
              ),
            )
            .get()
            .pipe(Effect.orDie)
        : undefined
      // A notification promise without a persisted job and deadline cannot
      // survive a restart, so reject it instead of displaying a phantom wake.
      // Narrowed once here so the closure below sees `number`, not
      // `number | null` - the schema's pendingWake.at cannot be null.
      const wakeAt = wake?.timeoutAt ?? undefined
      if (pendingWake && (!wakeAt || wakeAt <= Date.now())) return false
      return yield* patchCurrentGeneration(sessionID, (current) => {
        // pendingWake exists only on the idle variant; the previous
        // destructure-over-the-union compiled on the older checker but could
        // fabricate a busy status carrying pendingWake, which the schema
        // rejects. Narrow per variant instead.
        if (current.type === "idle") {
          const { pendingWake: _ignored, ...rest } = current
          return wakeAt !== undefined && wakeAt > 0 ? { ...rest, pendingWake: { ...pendingWake!, at: wakeAt } } : rest
        }
        if (current.type === "busy") return current
        return undefined
      })
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

    return Service.of({ get, list, set, setForGeneration, activity, flushActivity, toolStart, toolEnd, setPendingWake })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(EventV2Bridge.defaultLayer))

export * as SessionStatus from "./status"
