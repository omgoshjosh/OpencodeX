import { afterEach, describe, expect } from "bun:test"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecutionTable, SessionStatusTable } from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { eq } from "drizzle-orm"
import { Context, Deferred, Duration, Effect, Fiber, Layer, Ref } from "effect"
import path from "path"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { testEffect } from "../lib/effect"

/**
 * OpencodeX-fs2 PR2: `SessionStatus.recover` scans on the read connection with
 * no barrier and only then takes the barrier and an immediate transaction for
 * the rows it found. That opens a window between judging a row stale and
 * idling it; the write phase must re-check each row (generation, lease, status
 * write time) so a session renewed in that window is never idled. Built on a
 * real file database so the scan really runs on the second connection.
 */

const it = testEffect(Layer.empty)

const renewed = SessionID.make("ses_recover_renewed")
const abandoned = SessionID.make("ses_recover_abandoned")
const liveOwner = `local:${process.pid}:${ensureRunID()}:recover-test`

/** `alias` is the `OPENCODE_DB_SINGLE_CONNECTION=1` shape: `read` is the writer itself. */
const graph = Effect.fnUntraced(function* (
  filename: string,
  options: SessionStatus.LayerOptions,
  alias: "split" | "alias" = "split",
) {
  const opened = Context.get(yield* Layer.build(Layer.fresh(Database.layerFromPath(filename))), Database.Service)
  expect(opened.read).not.toBe(opened.db)
  const database = alias === "alias" ? { db: opened.db, read: opened.db } : opened
  const databaseLayer = Layer.succeed(Database.Service, database)
  const eventsLayer = EventV2Bridge.layer.pipe(Layer.provideMerge(EventV2.layer), Layer.provide(databaseLayer))
  // `Layer.build` memoizes through the fiber's memo map, so two graphs in one
  // test would otherwise share their status service and event log.
  const context = yield* Layer.build(
    Layer.fresh(
      SessionStatus.layerWithOptions(options).pipe(Layer.provide(databaseLayer), Layer.provideMerge(eventsLayer)),
    ),
  )
  return {
    database,
    status: Context.get(context, SessionStatus.Service),
    events: Context.get(context, EventV2.Service),
  }
})

/** A busy session whose running execution's lease expired a while ago: stale on any scan. */
const staleBusy = Effect.fnUntraced(function* (database: Database.Interface, sessionID: SessionID, now: number) {
  yield* database.db.insert(SessionStatusTable).values({
    session_id: sessionID,
    project_id: "prj_recover",
    directory: "/tmp/recover",
    status: { type: "busy" },
    time_created: now - 60_000,
    time_updated: now - 60_000,
  })
  yield* database.db.insert(SessionExecutionTable).values({
    session_id: sessionID,
    project_id: "prj_recover",
    directory: "/tmp/recover",
    state: "running",
    owner_id: liveOwner,
    generation: 1,
    lease_expires_at: now - 5_000,
    started_at: now - 60_000,
    time_created: now - 60_000,
    time_updated: now - 60_000,
  })
})

const rows = Effect.fnUntraced(function* (database: Database.Interface, sessionID: SessionID) {
  const status = yield* database.db
    .select()
    .from(SessionStatusTable)
    .where(eq(SessionStatusTable.session_id, sessionID))
    .get()
  const execution = yield* database.db
    .select()
    .from(SessionExecutionTable)
    .where(eq(SessionExecutionTable.session_id, sessionID))
    .get()
  return { status: status?.status, execution }
})

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("SessionStatus.recover off the barrier", () => {
  it.instance("never idles an execution renewed between the unlocked scan and the write", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const renewals = yield* Ref.make(0)
      const holder = yield* Ref.make<Database.Interface | undefined>(undefined)
      const { database, status } = yield* graph(path.join(test.directory, "status-recover.db"), {
        // A heartbeat lands after the scan judged the row stale and before the
        // write transaction opens: the lease is extended on the same generation.
        beforeRecoverWrite: Effect.gen(function* () {
          const target = yield* Ref.get(holder)
          if (!target) return
          yield* target.db
            .update(SessionExecutionTable)
            .set({ lease_expires_at: Date.now() + 60_000, time_updated: Date.now() })
            .where(eq(SessionExecutionTable.session_id, renewed))
            .run()
            .pipe(Effect.orDie)
          yield* Ref.update(renewals, (count) => count + 1)
        }),
      })
      const now = Date.now()
      yield* staleBusy(database, renewed, now)
      yield* staleBusy(database, abandoned, now)
      yield* Ref.set(holder, database)

      yield* status.recover()

      expect(yield* Ref.get(renewals)).toBe(1)
      const survivor = yield* rows(database, renewed)
      expect(survivor.status).toEqual({ type: "busy" })
      expect(survivor.execution).toMatchObject({ state: "running", generation: 1, owner_id: liveOwner })
      expect(survivor.execution!.lease_expires_at!).toBeGreaterThan(now)
      // The same pass still idled the row nobody renewed, so the write phase did run.
      const idled = yield* rows(database, abandoned)
      expect(idled.status).toEqual({ type: "idle" })
      expect(idled.execution).toMatchObject({ state: "interrupted", owner_id: null, lease_expires_at: null })
    }),
  )

  it.instance("scans past a held barrier on the read connection, but queues behind it under the kill switch", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      // Resolves when the scan phase has run: the write phase is next.
      const scanned = yield* Effect.gen(function* () {
        const split = yield* Deferred.make<void>()
        const alias = yield* Deferred.make<void>()
        return { split, alias }
      })
      const signal = (gate: Deferred.Deferred<void>): SessionStatus.LayerOptions => ({
        beforeRecoverWrite: Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
      })
      const split = yield* graph(path.join(test.directory, "status-recover-split.db"), signal(scanned.split))
      const aliased = yield* graph(path.join(test.directory, "status-recover-alias.db"), signal(scanned.alias), "alias")
      const now = Date.now()
      yield* staleBusy(split.database, abandoned, now)
      yield* staleBusy(aliased.database, abandoned, now)
      const hold = Effect.fnUntraced(function* (
        target: { events: EventV2.Interface; status: SessionStatus.Interface },
        gate: Deferred.Deferred<void>,
      ) {
        const held = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const holder = yield* Effect.forkChild(
          target.events.barrier(
            Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
            "test:hold",
          ),
        )
        yield* Deferred.await(held)
        const recovering = yield* Effect.forkChild(target.status.recover())
        const outcome = yield* Deferred.await(gate).pipe(Effect.timeoutOption(Duration.millis(300)))
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(holder)
        yield* Fiber.join(recovering)
        return outcome._tag
      })
      // The scan runs on the read connection and reaches the write phase while the barrier is held...
      expect(yield* hold(split, scanned.split)).toBe("Some")
      // ...whereas under the kill switch the whole pass waits for it, as it always did.
      expect(yield* hold(aliased, scanned.alias)).toBe("None")
      // Both idle the abandoned row once the barrier is free.
      expect((yield* rows(split.database, abandoned)).status).toEqual({ type: "idle" })
      expect((yield* rows(aliased.database, abandoned)).status).toEqual({ type: "idle" })
    }),
  )
})
