import { describe, expect } from "bun:test"
import { existsSync } from "fs"
import { Context, Effect, Exit, Layer, Logger, References, Scope } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { activeConnections } from "@opencode-ai/core/database/telemetry"
import { tmpdir } from "./fixture/tmpdir"
import { it } from "./lib/effect"

/**
 * OpencodeX-fs2.5: one `Database` layer is exactly one writer plus one
 * `query_only` reader. Before the fix `layer` called `nativeLayer(config)`
 * twice - two layer identities, two write-capable handles - so a layer opened
 * three native connections (6 on the deployed daemon, 2 layers x 3). The
 * open/close gauge in telemetry.ts is what the assertions read.
 */

type Entry = { message: unknown; level: string; fields: Record<string, unknown> }

const capture = () => {
  const entries: Entry[] = []
  const logger = Logger.make((opts) => {
    entries.push({
      message: Array.isArray(opts.message) ? opts.message.join(" ") : opts.message,
      level: opts.logLevel,
      fields: { ...opts.fiber.getRef(References.CurrentLogAnnotations) },
    })
  })
  return { entries, layer: Logger.layer([logger], { mergeWithExisting: false }) }
}

const on = (path: string) => activeConnections().filter((connection) => connection.path === path)

const roles = (path: string) =>
  on(path)
    .map((connection) => connection.role)
    .sort()

const withSingleConnection = Effect.gen(function* () {
  const previous = process.env["OPENCODE_DB_SINGLE_CONNECTION"]
  process.env["OPENCODE_DB_SINGLE_CONNECTION"] = "1"
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      if (previous === undefined) delete process.env["OPENCODE_DB_SINGLE_CONNECTION"]
      else process.env["OPENCODE_DB_SINGLE_CONNECTION"] = previous
    }),
  )
})

describe("Database native connections", () => {
  it.live("one Database layer on a file database opens exactly one writer and one reader", () =>
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
      )
      const path = `${dir.path}/native.db`
      const log = capture()
      const scope = yield* Scope.make()
      const database = Context.get(
        yield* Layer.build(Database.layerFromPath(path)).pipe(Effect.provide(log.layer), Scope.provide(scope)),
        Database.Service,
      )
      expect(database.read).not.toBe(database.db)
      expect(roles(path)).toEqual(["reader", "writer"])
      const opens = log.entries.filter((entry) => entry.message === "db_connection_open")
      expect(opens.map((entry) => entry.level)).toEqual(["Info", "Info"])
      expect(opens.map((entry) => entry.fields["role"])).toEqual(["writer", "reader"])
      expect(opens.map((entry) => entry.fields["path"])).toEqual([path, path])
      expect(opens.map((entry) => typeof entry.fields["connection_id"])).toEqual(["number", "number"])
      expect(opens.map((entry) => entry.fields["active_connections"])).toEqual([1, 2])
      yield* Scope.close(scope, Exit.void)
      expect(on(path)).toEqual([])
      const closes = log.entries.filter((entry) => entry.message === "db_connection_close")
      expect(closes.map((entry) => entry.fields["role"])).toEqual(["reader", "writer"])
      expect(closes.map((entry) => entry.fields["active_connections"])).toEqual([1, 0])
    }),
  )

  it.live("OPENCODE_DB_SINGLE_CONNECTION=1 opens exactly one native connection", () =>
    Effect.gen(function* () {
      yield* withSingleConnection
      const dir = yield* Effect.acquireRelease(
        Effect.promise(() => tmpdir()),
        (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
      )
      const path = `${dir.path}/single.db`
      const database = Context.get(yield* Layer.build(Database.layerFromPath(path)), Database.Service)
      expect(database.read).toBe(database.db)
      expect(roles(path)).toEqual(["writer"])
    }),
  )

  it.live("closing the layer scope leaves no active connection and the directory removes cleanly", () =>
    Effect.gen(function* () {
      const dir = yield* Effect.promise(() => tmpdir())
      const path = `${dir.path}/closed.db`
      const before = activeConnections().length
      yield* Effect.scoped(Layer.build(Database.layerFromPath(path)))
      expect(on(path)).toEqual([])
      expect(activeConnections().length).toBe(before)
      // Every handle is closed, so the -wal/-shm files are released (EBUSY on Windows otherwise).
      yield* Effect.promise(() => dir[Symbol.asyncDispose]())
      expect(existsSync(dir.path)).toBe(false)
    }),
  )
})
