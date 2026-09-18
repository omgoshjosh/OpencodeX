import { afterEach, describe, expect } from "bun:test"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionInteractionRecovery } from "@/session/interaction-recovery"
import { SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { eq } from "drizzle-orm"
import { Context, Deferred, Duration, Effect, Fiber, Layer, Ref } from "effect"
import path from "path"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { testEffect } from "../lib/effect"

/**
 * OpencodeX-fs2 PR3: `SessionInteractionRecovery.recoverWith` scans on the
 * read connection with no barrier and only then takes the barrier and an
 * immediate transaction for the interactions it found. That opens a window
 * between judging an interaction orphaned and rejecting it; the write phase
 * must re-check each row (interaction `time_updated`, execution generation and
 * cancel flag) so an interaction renewed in that window is never rejected.
 * Built on a real file database so the scan really runs on the second
 * connection.
 */

const it = testEffect(Layer.empty)

const renewed = SessionID.make("ses_interaction_renewed")
const abandoned = SessionID.make("ses_interaction_abandoned")

/** `alias` is the `OPENCODE_DB_SINGLE_CONNECTION=1` shape: `read` is the writer itself. */
const graph = Effect.fnUntraced(function* (filename: string, alias: "split" | "alias" = "split") {
  const opened = Context.get(yield* Layer.build(Layer.fresh(Database.layerFromPath(filename))), Database.Service)
  expect(opened.read).not.toBe(opened.db)
  const database = alias === "alias" ? { db: opened.db, read: opened.db } : opened
  const databaseLayer = Layer.succeed(Database.Service, database)
  const context = yield* Layer.build(
    Layer.fresh(EventV2Bridge.layer.pipe(Layer.provideMerge(EventV2.layer), Layer.provide(databaseLayer))),
  )
  return { database, events: Context.get(context, EventV2.Service) }
})

/** A pending permission whose execution generation has a cancel requested: orphaned on any scan. */
const cancelledPermission = Effect.fnUntraced(function* (
  database: Database.Interface,
  sessionID: SessionID,
  now: number,
) {
  yield* database.db.insert(SessionExecutionTable).values({
    session_id: sessionID,
    project_id: "prj_interaction",
    directory: "/tmp/interaction",
    state: "interrupted",
    generation: 1,
    cancel_requested_at: now - 5_000,
    completed_at: now - 5_000,
    time_created: now - 60_000,
    time_updated: now - 5_000,
  })
  yield* database.db.insert(SessionInteractionTable).values({
    id: `per_${sessionID}`,
    kind: "permission",
    session_id: sessionID,
    project_id: "prj_interaction",
    directory: "/tmp/interaction",
    state: "pending",
    request_json: { id: `per_${sessionID}`, sessionID, permission: "bash", executionGeneration: 1 },
    time_created: now - 60_000,
    time_updated: now - 60_000,
  })
})

const interaction = (database: Database.Interface, sessionID: SessionID) =>
  database.db
    .select()
    .from(SessionInteractionTable)
    .where(eq(SessionInteractionTable.id, `per_${sessionID}`))
    .get()

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("SessionInteractionRecovery.recoverWith off the barrier", () => {
  it.instance("never rejects an interaction renewed between the unlocked scan and the write", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const renewals = yield* Ref.make(0)
      const { database, events } = yield* graph(path.join(test.directory, "interaction-recover.db"))
      const now = Date.now()
      yield* cancelledPermission(database, renewed, now)
      yield* cancelledPermission(database, abandoned, now)

      yield* SessionInteractionRecovery.recoverWith({
        database,
        events,
        // The session restarts after the scan judged its permission orphaned:
        // a new generation with no cancel claims it, and the permission is
        // re-issued against that generation.
        beforeWrite: Effect.gen(function* () {
          const at = Date.now()
          yield* database.db
            .update(SessionExecutionTable)
            .set({ state: "running", generation: 2, cancel_requested_at: null, completed_at: null, time_updated: at })
            .where(eq(SessionExecutionTable.session_id, renewed))
            .run()
          yield* database.db
            .update(SessionInteractionTable)
            .set({
              request_json: { id: `per_${renewed}`, sessionID: renewed, permission: "bash", executionGeneration: 2 },
              time_updated: at,
            })
            .where(eq(SessionInteractionTable.id, `per_${renewed}`))
            .run()
          yield* Ref.update(renewals, (count) => count + 1)
        }).pipe(Effect.orDie),
      })

      expect(yield* Ref.get(renewals)).toBe(1)
      const survivor = yield* interaction(database, renewed)
      expect(survivor).toMatchObject({ state: "pending", response_json: null, responded_at: null })
      // The same pass still rejected the permission nobody renewed, so the write phase did run.
      const rejected = yield* interaction(database, abandoned)
      expect(rejected).toMatchObject({ state: "rejected", response_json: { reply: "reject" } })
      expect(rejected!.responded_at).toBeGreaterThanOrEqual(now)
    }),
  )

  it.instance("scans past a held barrier on the read connection, but queues behind it under the kill switch", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const split = yield* graph(path.join(test.directory, "interaction-recover-split.db"))
      const aliased = yield* graph(path.join(test.directory, "interaction-recover-alias.db"), "alias")
      const now = Date.now()
      yield* cancelledPermission(split.database, abandoned, now)
      yield* cancelledPermission(aliased.database, abandoned, now)
      const hold = Effect.fnUntraced(function* (target: typeof split) {
        const held = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        // Resolves when the scan phase has run: the write phase is next.
        const scanned = yield* Deferred.make<void>()
        const holder = yield* Effect.forkChild(
          target.events.barrier(
            Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
            "test:hold",
          ),
        )
        yield* Deferred.await(held)
        const recovering = yield* Effect.forkChild(
          SessionInteractionRecovery.recoverWith({
            ...target,
            beforeWrite: Deferred.succeed(scanned, undefined).pipe(Effect.asVoid),
          }),
        )
        const outcome = yield* Deferred.await(scanned).pipe(Effect.timeoutOption(Duration.millis(300)))
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(holder)
        yield* Fiber.join(recovering)
        return outcome._tag
      })
      // The scan runs on the read connection and reaches the write phase while the barrier is held...
      expect(yield* hold(split)).toBe("Some")
      // ...whereas under the kill switch the whole pass waits for it, as it always did.
      expect(yield* hold(aliased)).toBe("None")
      // Both reject the abandoned permission once the barrier is free.
      expect((yield* interaction(split.database, abandoned))?.state).toBe("rejected")
      expect((yield* interaction(aliased.database, abandoned))?.state).toBe("rejected")
    }),
  )
})
