import { describe, expect } from "bun:test"
import { sql } from "drizzle-orm"
import { Cause, Context, Deferred, Effect, Fiber, Layer } from "effect"
import { isSqlError } from "effect/unstable/sql/SqlError"
import { Database } from "@opencode-ai/core/database/database"
import { tmpdir } from "./fixture/tmpdir"
import { it } from "./lib/effect"

/**
 * OpencodeX-fs2 PR1: `Database.read` is a second, `query_only` connection with
 * its own permit. On a real file (WAL) database it must never queue behind the
 * writer, and the writer must never queue behind it. Both timing tests fail
 * when `read` aliases `db` (`OPENCODE_DB_SINGLE_CONNECTION=1`): the aliased
 * permit is held begin→commit, so the other side waits out the whole hold.
 */

/** How long the holding side keeps its transaction open. */
const HOLD_MS = 500

const build = (filename: string) =>
  Effect.map(Layer.build(Database.layerFromPath(filename)), (context) => Context.get(context, Database.Service))

const fileDatabase = Effect.gen(function* () {
  const dir = yield* Effect.acquireRelease(
    Effect.promise(() => tmpdir()),
    (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
  )
  const database = yield* build(`${dir.path}/read-connection.db`)
  yield* database.db.run(sql`CREATE TABLE probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
  return database
})

// Slow enough to be a real select, small enough that its synchronous run on
// the JS thread (bun:sqlite) stays far below the hold it is measured against.
const slowSelect = sql`WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 50000) SELECT count(*) AS n FROM c`

type Db = Database.Interface["db"]
type Tx = Parameters<Parameters<Db["transaction"]>[0]>[0]

const count = (database: Db | Tx) =>
  database.get<{ n: number }>(sql`SELECT count(*) AS n FROM probe`).pipe(Effect.map((row) => row?.n ?? -1))

describe("Database.read", () => {
  it.live("a held read transaction on `read` does not delay an immediate write on `db`", () =>
    Effect.gen(function* () {
      const { db, read } = yield* fileDatabase
      const holding = yield* Deferred.make<void>()
      let committed = false
      const reader = yield* Effect.forkChild(
        read.transaction((tx) =>
          Effect.gen(function* () {
            // `begin deferred` takes its snapshot on the first table read.
            const before = yield* count(tx)
            yield* Deferred.succeed(holding, undefined)
            yield* tx.all(slowSelect)
            yield* Effect.sleep(HOLD_MS)
            // Still inside the read transaction: the writer must already be done.
            return { before, committedDuringHold: committed, seen: yield* count(tx) }
          }),
        ),
      )
      yield* Deferred.await(holding)
      const started = Date.now()
      yield* db.transaction((tx) => tx.run(sql`INSERT INTO probe (value) VALUES ('write')`), {
        behavior: "immediate",
      })
      committed = true
      const elapsed = Date.now() - started
      const held = yield* Fiber.join(reader)
      expect(elapsed).toBeLessThan(HOLD_MS)
      expect(held.committedDuringHold).toBe(true)
      // Snapshot isolation: the open read transaction never saw the new row.
      expect(held.before).toBe(0)
      expect(held.seen).toBe(0)
      expect(yield* count(read)).toBe(1)
    }),
  )

  it.live("a held `begin immediate` on `db` does not delay a select on `read`", () =>
    Effect.gen(function* () {
      const { db, read } = yield* fileDatabase
      const holding = yield* Deferred.make<void>()
      const writer = yield* Effect.forkChild(
        db.transaction(
          (tx) =>
            Effect.gen(function* () {
              yield* tx.run(sql`INSERT INTO probe (value) VALUES ('pending')`)
              yield* Deferred.succeed(holding, undefined)
              yield* Effect.sleep(HOLD_MS)
            }),
          { behavior: "immediate" },
        ),
      )
      yield* Deferred.await(holding)
      const started = Date.now()
      const seen = yield* count(read)
      const elapsed = Date.now() - started
      yield* Fiber.join(writer)
      expect(elapsed).toBeLessThan(HOLD_MS)
      // The read ran while the write was still uncommitted, so it saw the old snapshot.
      expect(seen).toBe(0)
      expect(yield* count(read)).toBe(1)
    }),
  )

  it.live("a write through `read` fails with a typed SQLITE_READONLY error", () =>
    Effect.gen(function* () {
      const { db, read } = yield* fileDatabase
      const failure = yield* read.run(sql`INSERT INTO probe (value) VALUES ('nope')`).pipe(Effect.flip)
      expect(failure._tag).toBe("EffectDrizzleQueryError")
      expect(Cause.isCause(failure.cause)).toBe(true)
      const reason = Cause.isCause(failure.cause) ? failure.cause.reasons[0] : undefined
      const error = reason && Cause.isFailReason(reason) ? reason.error : undefined
      expect(isSqlError(error)).toBe(true)
      expect(isSqlError(error) && error.reason.cause).toMatchObject({ code: "SQLITE_READONLY" })
      const inTransaction = yield* read
        .transaction((tx) => tx.run(sql`DELETE FROM probe`))
        .pipe(Effect.flip)
      expect(inTransaction._tag).toBe("EffectDrizzleQueryError")
      expect(yield* count(db)).toBe(0)
    }),
  )

  it.live("`:memory:` databases alias `read` to `db`", () =>
    Effect.gen(function* () {
      const { db, read } = yield* build(":memory:")
      expect(read).toBe(db)
    }),
  )

  it.live("OPENCODE_DB_SINGLE_CONNECTION=1 aliases `read` to `db` on a file database", () =>
    Effect.gen(function* () {
      const previous = process.env["OPENCODE_DB_SINGLE_CONNECTION"]
      process.env["OPENCODE_DB_SINGLE_CONNECTION"] = "1"
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (previous === undefined) delete process.env["OPENCODE_DB_SINGLE_CONNECTION"]
          else process.env["OPENCODE_DB_SINGLE_CONNECTION"] = previous
        }),
      )
      const { db, read } = yield* fileDatabase
      expect(read).toBe(db)
    }),
  )
})
