import { mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { eq, sql } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Cause, Deferred, Effect, Exit, Fiber } from "effect"
import type { SqlClient as SqlClientService } from "effect/unstable/sql/SqlClient"
import { EffectDrizzleSqlite } from "../src"

const users = sqliteTable("users", {
  id: integer().primaryKey({ autoIncrement: true }),
  name: text().notNull(),
})

const run = <A, E>(effect: Effect.Effect<A, E, SqlClientService>) =>
  Effect.runPromise(
    effect.pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped),
  )

const makeDb = Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run(sql`create table users (id integer primary key autoincrement, name text not null)`)
  return db
})

const createMigrationsFolder = async () => {
  const migrationsFolder = await mkdtemp(join(tmpdir(), "effect-drizzle-sqlite-"))
  await mkdir(join(migrationsFolder, "20240101000000_create_migrated_users"), { recursive: true })
  await Bun.write(
    join(migrationsFolder, "20240101000000_create_migrated_users", "migration.sql"),
    "create table migrated_users (id integer primary key autoincrement, name text not null);",
  )
  return migrationsFolder
}

test("selects rows through Effect-yieldable query builders", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb
      yield* db.insert(users).values({ name: "Ada" })

      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Ada" }])
      expect(yield* db.select({ id: users.id }).from(users).where(eq(users.name, "Ada")).get()).toEqual({ id: 1 })
    }),
  )
})

test("commits successful transactions", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db.transaction((tx) => tx.insert(users).values({ name: "Grace" }), { behavior: "immediate" })

      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Grace" }])
    }),
  )
})

test("rolls back failed transactions", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db
        .transaction((tx) =>
          tx
            .insert(users)
            .values({ name: "Linus" })
            .pipe(Effect.andThen(Effect.fail("boom"))),
        )
        .pipe(Effect.ignore)

      expect(yield* db.select().from(users)).toEqual([])
    }),
  )
})

test("rolls back explicit transaction rollback", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      yield* db
        .transaction((tx) =>
          tx
            .insert(users)
            .values({ name: "Barbara" })
            .pipe(Effect.andThen(Effect.fail(tx.rollback()))),
        )
        .pipe(Effect.ignore)

      expect(yield* db.select().from(users)).toEqual([])
    }),
  )
})

test("reports the transaction body's error when the rollback statement fails", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      // Ending the transaction from inside the body is what SQLite itself does
      // on some statement failures: the outer `rollback` then fails with
      // "cannot rollback - no transaction is active".
      const exit = yield* db
        .transaction((tx) => tx.run(sql`rollback`).pipe(Effect.andThen(Effect.fail("boom"))))
        .pipe(Effect.exit)

      expect(exit).toEqual(Exit.fail("boom"))
    }),
  )
})

test("supports returning and rejects empty update sets", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb

      const inserted = yield* db.insert(users).values({ name: "Ada" }).returning({ id: users.id, name: users.name })
      expect(inserted).toEqual([{ id: 1, name: "Ada" }])

      const updated = yield* db.update(users).set({ name: "Grace" }).where(eq(users.id, 1)).returning()
      expect(updated).toEqual([{ id: 1, name: "Grace" }])

      const deleted = yield* db.delete(users).where(eq(users.id, 1)).returning({ id: users.id })
      expect(deleted).toEqual([{ id: 1 }])

      expect(() => db.update(users).set({ name: undefined })).toThrow("No values to set")
    }),
  )
})

test("runs migrations once and records migration metadata", async () => {
  const migrationsFolder = await createMigrationsFolder()
  try {
    await run(
      Effect.gen(function* () {
        const db = yield* EffectDrizzleSqlite.makeWithDefaults()

        yield* EffectDrizzleSqlite.migrate(db, { migrationsFolder })
        yield* EffectDrizzleSqlite.migrate(db, { migrationsFolder })
        yield* db.run(sql`insert into migrated_users (name) values ('Margaret')`)

        expect(yield* db.all<{ name: string }>(sql`select name from migrated_users`)).toEqual([{ name: "Margaret" }])
        expect(yield* db.all<{ name: string | null }>(sql`select name from __drizzle_migrations`)).toEqual([
          { name: "20240101000000_create_migrated_users" },
        ])
      }),
    )
  } finally {
    await rm(migrationsFolder, { recursive: true, force: true })
  }
})

// A pool with a single connection can keep a transaction queued for as long as
// the current one runs. A caller that has already given up must be able to leave
// that queue rather than wait for a connection it would immediately roll back.
test("a transaction queued for the connection can be interrupted before it begins", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* Effect.forkChild(
        db.transaction(() => Effect.andThen(Deferred.succeed(entered, undefined), Deferred.await(release))),
      )
      yield* Deferred.await(entered)

      const queued = yield* Effect.forkChild(db.transaction(() => db.insert(users).values({ name: "queued" })))
      yield* Effect.yieldNow
      yield* Fiber.interrupt(queued)
      const exit = yield* Fiber.await(queued)
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(holder)

      // The abandoned reservation released its permit and never reached `begin`.
      yield* db.insert(users).values({ name: "Ada" })
      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Ada" }])
    }),
  )
})

// The interrupt window is the reservation only. Whichever side of the handover
// it lands on, the permit has to come back, or the process deadlocks on the next
// query.
test("interrupting a queued transaction never leaks the connection permit", async () => {
  await run(
    Effect.gen(function* () {
      const db = yield* makeDb
      for (let attempt = 0; attempt < 200; attempt++) {
        const busy = yield* Effect.forkChild(db.transaction(() => db.select().from(users)))
        const queued = yield* Effect.forkChild(db.transaction(() => db.select().from(users)))
        // Vary how far the queued fiber has progressed when the interrupt lands.
        for (let yields = 0; yields < attempt % 5; yields++) yield* Effect.yieldNow
        yield* Fiber.interrupt(queued)
        yield* Fiber.join(busy)
      }
      yield* db.insert(users).values({ name: "Ada" })
      expect(yield* db.select().from(users)).toEqual([{ id: 1, name: "Ada" }])
    }),
  )
})
