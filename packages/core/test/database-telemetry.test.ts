import { describe, expect } from "bun:test"
import { sql } from "drizzle-orm"
import { Clock, Context, Effect, Layer, Logger, References } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { DB_SLOW_MS } from "@opencode-ai/core/database/telemetry"
import { tmpdir } from "./fixture/tmpdir"
import { it } from "./lib/effect"

/**
 * OpencodeX-fs2 PR3: every statement on either connection measures its wait
 * for the connection permit and its own run time, and warns - with structured
 * fields, never an object as the message - only once a measure crosses
 * `DB_SLOW_MS`. The clock is scripted: each `currentTimeMillisUnsafe` call
 * returns the next entry, so the test decides how long the permit wait and the
 * statement "took" without a real slow query. One statement reads the clock
 * four times, in order: permit requested, permit acquired, statement started,
 * statement done. A transaction reads it twice for the permit, then twice per
 * statement (`begin`, the body, `commit`). A WARN line's own timestamp reads
 * it once more, after the measures it reports.
 */

type Entry = { message: unknown; level: string; fields: Record<string, unknown> }

const capture = Effect.gen(function* () {
  const entries: Entry[] = []
  const logger = Logger.make((opts) => {
    entries.push({
      message: Array.isArray(opts.message) ? opts.message.join(" ") : opts.message,
      level: opts.logLevel,
      fields: { ...opts.fiber.getRef(References.CurrentLogAnnotations) },
    })
  })
  yield* Effect.void
  return { entries, layer: Logger.layer([logger], { mergeWithExisting: false }) }
})

const scripted = (script: readonly number[]): Clock.Clock => {
  const remaining = [...script]
  let last = 0
  const millis = () => {
    if (remaining.length > 0) last = remaining.shift()!
    return last
  }
  return {
    currentTimeMillisUnsafe: millis,
    currentTimeMillis: Effect.sync(millis),
    currentTimeNanosUnsafe: () => BigInt(last) * 1_000_000n,
    currentTimeNanos: Effect.sync(() => BigInt(last) * 1_000_000n),
    sleep: (duration) => Effect.clockWith((clock) => clock.sleep(duration)),
  }
}

const fileDatabase = Effect.gen(function* () {
  const dir = yield* Effect.acquireRelease(
    Effect.promise(() => tmpdir()),
    (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
  )
  const database = Context.get(yield* Layer.build(Database.layerFromPath(`${dir.path}/telemetry.db`)), Database.Service)
  yield* database.db.run(sql`CREATE TABLE probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
  expect(database.read).not.toBe(database.db)
  return database
})

const slow = (entries: Entry[]) =>
  entries.filter((entry) => entry.message === "db_read_slow" || entry.message === "db_write_queue_slow")

describe("Database connection telemetry", () => {
  it.live("a statement on `read` that runs past the bar logs db_read_slow with its measures", () =>
    Effect.gen(function* () {
      const { read } = yield* fileDatabase
      const log = yield* capture
      yield* read
        .all(sql`SELECT count(*) AS n FROM probe`)
        .pipe(Effect.provideService(Clock.Clock, scripted([0, 0, 0, DB_SLOW_MS + 500])), Effect.provide(log.layer))
      expect(slow(log.entries)).toEqual([
        {
          message: "db_read_slow",
          level: "Warn",
          fields: {
            connection: "read",
            fiber: expect.any(Number),
            read_ms: DB_SLOW_MS + 500,
            sql: "SELECT count(*) AS n FROM probe",
          },
        },
      ])
    }),
  )

  it.live("a transaction on `db` that waits past the bar for the permit logs db_write_queue_slow", () =>
    Effect.gen(function* () {
      const { db } = yield* fileDatabase
      const log = yield* capture
      yield* db
        .transaction((tx) => tx.run(sql`INSERT INTO probe (value) VALUES ('queued')`), { behavior: "immediate" })
        .pipe(Effect.provideService(Clock.Clock, scripted([0, DB_SLOW_MS + 1])), Effect.provide(log.layer))
      expect(slow(log.entries)).toEqual([
        {
          message: "db_write_queue_slow",
          level: "Warn",
          fields: { connection: "db", scope: "transaction", fiber: expect.any(Number), write_queue_ms: DB_SLOW_MS + 1 },
        },
      ])
    }),
  )

  it.live("a single statement on `db` reports its own permit wait as scope=statement", () =>
    Effect.gen(function* () {
      const { db } = yield* fileDatabase
      const log = yield* capture
      yield* db
        .run(sql`INSERT INTO probe (value) VALUES ('single')`)
        .pipe(Effect.provideService(Clock.Clock, scripted([0, DB_SLOW_MS * 2])), Effect.provide(log.layer))
      expect(slow(log.entries)).toEqual([
        {
          message: "db_write_queue_slow",
          level: "Warn",
          fields: { connection: "db", scope: "statement", fiber: expect.any(Number), write_queue_ms: DB_SLOW_MS * 2 },
        },
      ])
    }),
  )

  it.live("measures exactly at the bar, or on the other connection's fast path, stay silent", () =>
    Effect.gen(function* () {
      const { db, read } = yield* fileDatabase
      const log = yield* capture
      // Permit wait and statement both take exactly DB_SLOW_MS: not over it.
      yield* read
        .all(sql`SELECT count(*) AS n FROM probe`)
        .pipe(
          Effect.provideService(Clock.Clock, scripted([0, DB_SLOW_MS, DB_SLOW_MS, DB_SLOW_MS * 2])),
          Effect.provide(log.layer),
        )
      // A real, fast statement on the live clock.
      yield* db.run(sql`INSERT INTO probe (value) VALUES ('fast')`).pipe(Effect.provide(log.layer))
      expect(slow(log.entries)).toEqual([])
    }),
  )
})
