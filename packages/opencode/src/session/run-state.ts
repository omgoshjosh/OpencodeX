import { BackgroundJob } from "@/background/job"
import { InstanceState } from "@/effect/instance-state"
import { Runner } from "@/effect/runner"
import { Database } from "@opencode-ai/core/database/database"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { and, eq, gt, inArray, isNull, sql } from "drizzle-orm"
import { Cause, Context, Effect, Latch, Layer, Scope } from "effect"
import { isSqlError, isSqlErrorReason } from "effect/unstable/sql/SqlError"
import * as Session from "./session"
import { SessionID } from "./schema"
import { SessionStatus } from "./status"
import { SessionExecutionOwner } from "./execution-owner"

const LEASE_MILLIS = 15_000
const POLL_MILLIS = 200
const SQLITE_BUSY_MILLIS = 5_000
const RETRY_MARGIN_MILLIS = 500
const DATABASE_NOW_MILLIS = sql<number>`cast(unixepoch('subsec') * 1000 as integer)`

interface Lease {
  owner: string
  generation: number
  expiresAt: number
}

interface ActiveRunner {
  runner: Runner.Runner<SessionLegacy.WithParts>
  lease: Lease
  interrupted: boolean
  heartbeatFailed: boolean
}

export interface Interface {
  readonly assertNotBusy: (sessionID: SessionID) => Effect.Effect<void, Session.BusyError>
  readonly cancel: (sessionID: SessionID) => Effect.Effect<number>
  readonly interrupt: (sessionID: SessionID) => Effect.Effect<boolean>
  readonly ensureRunning: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
    work: Effect.Effect<SessionLegacy.WithParts>,
  ) => Effect.Effect<SessionLegacy.WithParts>
  readonly startShell: (
    sessionID: SessionID,
    onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
    work: Effect.Effect<SessionLegacy.WithParts>,
    ready?: Latch.Latch,
  ) => Effect.Effect<SessionLegacy.WithParts, Session.BusyError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionRunState") {}

export interface LayerOptions {
  readonly onHeartbeatLockTimeout?: Effect.Effect<void>
}

class Options extends Context.Service<Options, LayerOptions>()("@opencode/SessionRunStateOptions") {}

const configuredLayer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const options = yield* Options
    const background = yield* BackgroundJob.Service
    const status = yield* SessionStatus.Service
    const { db } = yield* Database.Service
    const processRunID = ensureRunID()
    const ownerPrefix = `local:${process.pid}:${processRunID}:${crypto.randomUUID()}`

    const claim = Effect.fn("SessionRunState.claim")(function* (sessionID: SessionID) {
      const ctx = yield* InstanceState.context
      return yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const now = yield* transaction.get<{ now: number }>(sql`select ${DATABASE_NOW_MILLIS} as now`)
              const execution = yield* transaction
                .select()
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, sessionID))
                .get()
              if (
                execution?.state === "running" &&
                execution.owner_id &&
                execution.lease_expires_at &&
                now &&
                execution.lease_expires_at > now.now &&
                SessionExecutionOwner.alive(execution.owner_id, processRunID)
              )
                return undefined

              const identity = {
                owner: `${ownerPrefix}:${sessionID}`,
                generation: (execution?.generation ?? 0) + 1,
              }
              const claimed = yield* transaction
                .insert(SessionExecutionTable)
                .values({
                  session_id: sessionID,
                  project_id: ctx.project.id,
                  directory: ctx.directory,
                  state: "running",
                  owner_id: identity.owner,
                  generation: identity.generation,
                  lease_expires_at: sql`${DATABASE_NOW_MILLIS} + ${LEASE_MILLIS}`,
                  cancel_requested_at: null,
                  started_at: DATABASE_NOW_MILLIS,
                  completed_at: null,
                  time_created: execution?.time_created ?? DATABASE_NOW_MILLIS,
                  time_updated: DATABASE_NOW_MILLIS,
                })
                .onConflictDoUpdate({
                  target: SessionExecutionTable.session_id,
                  set: {
                    project_id: ctx.project.id,
                    directory: ctx.directory,
                    state: "running",
                    owner_id: identity.owner,
                    generation: identity.generation,
                    lease_expires_at: sql`${DATABASE_NOW_MILLIS} + ${LEASE_MILLIS}`,
                    cancel_requested_at: null,
                    started_at: DATABASE_NOW_MILLIS,
                    completed_at: null,
                    time_updated: DATABASE_NOW_MILLIS,
                  },
                })
                .returning({ expiresAt: SessionExecutionTable.lease_expires_at })
                .get()
              if (!claimed?.expiresAt) return yield* Effect.die("session execution claim returned no lease")
              return { ...identity, expiresAt: claimed.expiresAt }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const release = Effect.fn("SessionRunState.release")(function* (
      sessionID: SessionID,
      lease: Lease,
      interrupted: boolean,
    ) {
      const now = Date.now()
      const released = yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select({ cancelRequestedAt: SessionExecutionTable.cancel_requested_at })
                .from(SessionExecutionTable)
                .where(
                  and(
                    eq(SessionExecutionTable.session_id, sessionID),
                    eq(SessionExecutionTable.owner_id, lease.owner),
                    eq(SessionExecutionTable.generation, lease.generation),
                  ),
                )
                .get()
              if (!current) return false
              yield* transaction
                .update(SessionExecutionTable)
                .set({
                  state: interrupted || current.cancelRequestedAt ? "interrupted" : "idle",
                  owner_id: null,
                  lease_expires_at: null,
                  completed_at: now,
                  time_updated: now,
                })
                .where(
                  and(
                    eq(SessionExecutionTable.session_id, sessionID),
                    eq(SessionExecutionTable.owner_id, lease.owner),
                    eq(SessionExecutionTable.generation, lease.generation),
                  ),
                )
                .run()
              return true
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      if (released) yield* status.setForGeneration(sessionID, lease.generation, { type: "idle" })
    })

    const supervise = Effect.fn("SessionRunState.supervise")(function* (
      sessionID: SessionID,
      active: ActiveRunner,
      onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
      work: Effect.Effect<SessionLegacy.WithParts>,
    ) {
      const lease = active.lease
      const terminal = (error: unknown) => {
        active.heartbeatFailed = true
        return Effect.die(error)
      }
      const renew = Effect.fn("SessionRunState.renew")(function* () {
        const pass = () =>
          Effect.gen(function* () {
            const current = yield* db
              .select({
                state: SessionExecutionTable.state,
                owner: SessionExecutionTable.owner_id,
                generation: SessionExecutionTable.generation,
                cancelRequestedAt: SessionExecutionTable.cancel_requested_at,
                leaseExpiresAt: SessionExecutionTable.lease_expires_at,
                now: DATABASE_NOW_MILLIS,
              })
              .from(SessionExecutionTable)
              .where(eq(SessionExecutionTable.session_id, sessionID))
              .get()
            if (
              !current ||
              current.state !== "running" ||
              current.owner !== lease.owner ||
              current.generation !== lease.generation ||
              current.cancelRequestedAt ||
              !current.leaseExpiresAt ||
              current.leaseExpiresAt <= current.now
            )
              return { _tag: "Interrupted" } as const

            const renewed = yield* db
              .update(SessionExecutionTable)
              .set({
                lease_expires_at: sql`${DATABASE_NOW_MILLIS} + ${LEASE_MILLIS}`,
                time_updated: DATABASE_NOW_MILLIS,
              })
              .where(
                and(
                  eq(SessionExecutionTable.session_id, sessionID),
                  eq(SessionExecutionTable.state, "running"),
                  eq(SessionExecutionTable.owner_id, lease.owner),
                  eq(SessionExecutionTable.generation, lease.generation),
                  isNull(SessionExecutionTable.cancel_requested_at),
                  gt(SessionExecutionTable.lease_expires_at, DATABASE_NOW_MILLIS),
                ),
              )
              .returning({ expiresAt: SessionExecutionTable.lease_expires_at })
              .get()
            if (!renewed?.expiresAt) return { _tag: "Interrupted" } as const
            return { _tag: "Renewed", expiresAt: renewed.expiresAt } as const
          })

        return yield* pass().pipe(
          Effect.catchTag("EffectDrizzleQueryError", (error) =>
            Effect.gen(function* () {
              if (!isLockTimeout(error)) return yield* terminal(error)
              const current = yield* db
                .get<{ now: number }>(sql`select ${DATABASE_NOW_MILLIS} as now`)
                .pipe(Effect.catch((admissionError) => terminal(admissionError)))
              if (!current || lease.expiresAt <= current.now + SQLITE_BUSY_MILLIS + RETRY_MARGIN_MILLIS)
                return yield* terminal(error)
              yield* options.onHeartbeatLockTimeout ?? Effect.void
              return yield* pass().pipe(Effect.catch((retryError) => terminal(retryError)))
            }),
          ),
        )
      })

      const monitor = Effect.gen(function* () {
        let expiresAt = lease.expiresAt
        while (true) {
          yield* Effect.sleep(POLL_MILLIS)
          const current = yield* db
            .select({
              state: SessionExecutionTable.state,
              owner: SessionExecutionTable.owner_id,
              generation: SessionExecutionTable.generation,
              cancelRequestedAt: SessionExecutionTable.cancel_requested_at,
              leaseExpiresAt: SessionExecutionTable.lease_expires_at,
              now: DATABASE_NOW_MILLIS,
            })
            .from(SessionExecutionTable)
            .where(eq(SessionExecutionTable.session_id, sessionID))
            .get()
            .pipe(Effect.catch((error) => terminal(error)))
          if (
            !current ||
            current.state !== "running" ||
            current.owner !== lease.owner ||
            current.generation !== lease.generation ||
            !current.leaseExpiresAt ||
            current.leaseExpiresAt <= current.now
          )
            return { _tag: "Interrupted" } as const
          if (current.cancelRequestedAt) return { _tag: "Interrupted" } as const
          if (current.now < expiresAt - Math.floor((LEASE_MILLIS * 3) / 4)) continue
          const result = yield* renew()
          if (result._tag === "Interrupted") return result
          expiresAt = result.expiresAt
          lease.expiresAt = result.expiresAt
        }
      })
      const result = yield* work.pipe(
        Effect.provideService(SessionStatus.ExecutionGeneration, {
          sessionID,
          generation: lease.generation,
          owner: lease.owner,
        }),
        Effect.map((value) => ({ _tag: "Completed", value }) as const),
        Effect.raceFirst(monitor),
      )
      if (result._tag === "Interrupted") return yield* onInterrupt
      return result.value
    })

    const waitForForeign = Effect.fn("SessionRunState.waitForForeign")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
    ) {
      while (true) {
        const current = yield* db
          .select({
            state: SessionExecutionTable.state,
            owner: SessionExecutionTable.owner_id,
            leaseExpiresAt: SessionExecutionTable.lease_expires_at,
          })
          .from(SessionExecutionTable)
          .where(eq(SessionExecutionTable.session_id, sessionID))
          .get()
          .pipe(Effect.orDie)
        if (!current || current.state !== "running" || !current.owner || !current.leaseExpiresAt) break
        if (current.leaseExpiresAt <= Date.now()) break
        yield* Effect.sleep(POLL_MILLIS)
      }
      return yield* onInterrupt
    })

    const state = yield* InstanceState.make(
      Effect.fn("SessionRunState.state")(function* () {
        const scope = yield* Scope.Scope
        const runners = new Map<SessionID, ActiveRunner>()
        yield* Effect.addFinalizer(
          Effect.fnUntraced(function* () {
            yield* Effect.forEach(
              runners.values(),
              (active) => {
                active.interrupted = true
                return active.runner.cancel
              },
              { concurrency: "unbounded", discard: true },
            )
            runners.clear()
          }),
        )
        return { runners, scope }
      }),
    )

    const ownedRunner = Effect.fn("SessionRunState.runner")(function* (
      sessionID: SessionID,
      lease: Lease,
      onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing) return existing
      const next: Runner.Runner<SessionLegacy.WithParts> = Runner.make<SessionLegacy.WithParts>(data.scope, {
        onIdle: Effect.gen(function* () {
          if (data.runners.get(sessionID) !== active) return
          data.runners.delete(sessionID)
          yield* release(sessionID, lease, active.interrupted).pipe(
            Effect.catchCause((cause) => (active.heartbeatFailed ? Effect.logError(cause) : Effect.failCause(cause))),
          )
        }),
        onInterrupt,
      })
      const active: ActiveRunner = { runner: next, lease, interrupted: false, heartbeatFailed: false }
      data.runners.set(sessionID, active)
      return active
    })

    const assertNotBusy = Effect.fn("SessionRunState.assertNotBusy")(function* (sessionID: SessionID) {
      const data = yield* InstanceState.get(state)
      if (data.runners.get(sessionID)?.runner.busy) return yield* busyError(sessionID)
      const current = yield* db
        .select({ state: SessionExecutionTable.state, leaseExpiresAt: SessionExecutionTable.lease_expires_at })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      if (current?.state === "running" && current.leaseExpiresAt && current.leaseExpiresAt > Date.now())
        return yield* busyError(sessionID)
      return undefined
    })

    const cancel = Effect.fn("SessionRunState.cancel")(function* (sessionID: SessionID) {
      yield* cancelBackgroundJobs(background, sessionID)
      const ctx = yield* InstanceState.context
      const now = Date.now()
      const target = yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, sessionID))
                .get()
              const active =
                current?.state === "running" &&
                !!current.owner_id &&
                !!current.lease_expires_at &&
                current.lease_expires_at > now &&
                SessionExecutionOwner.alive(current.owner_id, processRunID)
              yield* transaction
                .insert(SessionExecutionTable)
                .values({
                  session_id: sessionID,
                  project_id: ctx.project.id,
                  directory: ctx.directory,
                  state: active ? "running" : "interrupted",
                  owner_id: active ? current.owner_id : null,
                  generation: current?.generation ?? 0,
                  lease_expires_at: active ? current.lease_expires_at : null,
                  cancel_requested_at: now,
                  completed_at: active ? current.completed_at : now,
                  time_created: current?.time_created ?? now,
                  time_updated: now,
                })
                .onConflictDoUpdate({
                  target: SessionExecutionTable.session_id,
                  set: {
                    state: active ? "running" : "interrupted",
                    owner_id: active ? current?.owner_id : null,
                    lease_expires_at: active ? current?.lease_expires_at : null,
                    cancel_requested_at: now,
                    completed_at: active ? current?.completed_at : now,
                    time_updated: now,
                  },
                })
                .run()
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
                    eq(SessionCommandTable.session_id, sessionID),
                    inArray(SessionCommandTable.status, ["queued", "running"]),
                  ),
                )
                .run()
              return {
                active,
                owner: active ? current?.owner_id : undefined,
                generation: current?.generation ?? 0,
              }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)

      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (
        target.active &&
        existing?.runner.busy &&
        existing.lease.owner === target.owner &&
        existing.lease.generation === target.generation
      ) {
        existing.interrupted = true
        yield* existing.runner.cancel
        return target.generation
      }
      if (!target.active) yield* status.setForGeneration(sessionID, target.generation, { type: "idle" })
      return target.generation
    })

    /**
     * Stops the current turn so a direct ("immediate") prompt can pivot the
     * session. Unlike `cancel`, queued commands stay queued - the interrupting
     * prompt is already one of them and must launch next. The cancel request
     * is written durably so the owner's supervise monitor stops the run even
     * when another process holds the lease; the in-memory runner alone only
     * covers runs this instance started.
     */
    const interrupt = Effect.fn("SessionRunState.interrupt")(function* (sessionID: SessionID) {
      const now = Date.now()
      const target = yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, sessionID))
                .get()
              if (
                !current ||
                current.state !== "running" ||
                !current.owner_id ||
                !current.lease_expires_at ||
                current.lease_expires_at <= now
              )
                return undefined
              const alive = SessionExecutionOwner.alive(current.owner_id, processRunID)
              yield* transaction
                .update(SessionExecutionTable)
                .set(
                  alive
                    ? { cancel_requested_at: now, time_updated: now }
                    : {
                        // A dead owner's stale lease would stall the pivot
                        // until it expired; settle it like `cancel` does.
                        state: "interrupted",
                        owner_id: null,
                        lease_expires_at: null,
                        cancel_requested_at: now,
                        completed_at: now,
                        time_updated: now,
                      },
                )
                .where(
                  and(
                    eq(SessionExecutionTable.session_id, sessionID),
                    eq(SessionExecutionTable.owner_id, current.owner_id),
                    eq(SessionExecutionTable.generation, current.generation),
                  ),
                )
                .run()
              return { owner: current.owner_id, generation: current.generation }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      if (!target) return false
      const existing = (yield* InstanceState.get(state)).runners.get(sessionID)
      if (
        existing?.runner.busy &&
        existing.lease.owner === target.owner &&
        existing.lease.generation === target.generation
      ) {
        existing.interrupted = true
        yield* existing.runner.cancel
      }
      return true
    })

    const ensureRunning = Effect.fn("SessionRunState.ensureRunning")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
      work: Effect.Effect<SessionLegacy.WithParts>,
    ) {
      const data = yield* InstanceState.get(state)
      const existing = data.runners.get(sessionID)
      if (existing) return yield* existing.runner.ensureRunning(work)
      const lease = yield* claim(sessionID)
      if (!lease) return yield* waitForForeign(sessionID, onInterrupt)
      const admitted = yield* status.setForGeneration(sessionID, lease.generation, { type: "busy" }, lease.owner)
      if (!admitted) {
        yield* release(sessionID, lease, true)
        return yield* onInterrupt
      }
      const active = yield* ownedRunner(sessionID, lease, onInterrupt)
      return yield* active.runner.ensureRunning(supervise(sessionID, active, onInterrupt, work))
    })

    const startShell = Effect.fn("SessionRunState.startShell")(function* (
      sessionID: SessionID,
      onInterrupt: Effect.Effect<SessionLegacy.WithParts>,
      work: Effect.Effect<SessionLegacy.WithParts>,
      ready?: Latch.Latch,
    ) {
      const data = yield* InstanceState.get(state)
      if (data.runners.get(sessionID)?.runner.busy) return yield* busyError(sessionID)
      const lease = yield* claim(sessionID)
      if (!lease) return yield* busyError(sessionID)
      const admitted = yield* status.setForGeneration(sessionID, lease.generation, { type: "busy" }, lease.owner)
      if (!admitted) {
        yield* release(sessionID, lease, true)
        return yield* onInterrupt
      }
      const active = yield* ownedRunner(sessionID, lease, onInterrupt)
      return yield* active.runner.startShell(supervise(sessionID, active, onInterrupt, work), ready).pipe(
        Effect.catchTag("RunnerBusy", () => Effect.fail(busyError(sessionID))),
        Effect.onError(() =>
          Effect.gen(function* () {
            const current = yield* InstanceState.get(state)
            if (current.runners.get(sessionID) === active) current.runners.delete(sessionID)
            yield* release(sessionID, lease, true).pipe(
              Effect.catchCause((cause) => (active.heartbeatFailed ? Effect.logError(cause) : Effect.failCause(cause))),
            )
          }),
        ),
      )
    })

    return Service.of({ assertNotBusy, cancel, interrupt, ensureRunning, startShell })
  }),
)

export const layerWithOptions = (options: LayerOptions = {}) =>
  configuredLayer.pipe(Layer.provide(Layer.succeed(Options, options)))

export const layer = layerWithOptions()

export const defaultLayer = layer.pipe(
  Layer.provide(BackgroundJob.defaultLayer),
  Layer.provide(Database.defaultLayer),
  Layer.provide(SessionStatus.defaultLayer),
)

const cancelBackgroundJobs = Effect.fn("SessionRunState.cancelBackgroundJobs")(function* (
  background: BackgroundJob.Interface,
  sessionID: SessionID,
) {
  const jobs = yield* background.list()
  const pending = new Set<string>([sessionID])
  const cancelled = new Set<string>()
  const matches = (job: BackgroundJob.Info) => {
    if (job.status !== "running") return false
    if (cancelled.has(job.id)) return false
    if (pending.has(job.id)) return true
    if (typeof job.metadata?.sessionId === "string" && pending.has(job.metadata.sessionId)) return true
    return typeof job.metadata?.parentSessionId === "string" && pending.has(job.metadata.parentSessionId)
  }
  let batch = jobs.filter(matches)
  while (batch.length > 0) {
    yield* Effect.forEach(
      batch,
      (job) =>
        background.cancel(job.id).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              cancelled.add(job.id)
              pending.add(job.id)
              if (typeof job.metadata?.sessionId === "string") pending.add(job.metadata.sessionId)
            }),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    )
    batch = jobs.filter(matches)
  }
})

function busyError(sessionID: SessionID) {
  return new Session.BusyError({ sessionID })
}

function isLockTimeout(error: unknown) {
  if (
    typeof error !== "object" ||
    error === null ||
    !("_tag" in error) ||
    error._tag !== "EffectDrizzleQueryError" ||
    !("cause" in error) ||
    !Cause.isCause(error.cause)
  )
    return false
  return error.cause.reasons.some(
    (reason) =>
      Cause.isFailReason(reason) &&
      isSqlError(reason.error) &&
      isSqlErrorReason(reason.error.reason) &&
      reason.error.reason._tag === "LockTimeoutError",
  )
}

export * as SessionRunState from "./run-state"
