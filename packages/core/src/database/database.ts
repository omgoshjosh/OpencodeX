export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Context, Effect, Layer, Option } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { existsSync, readFileSync } from "fs"
import { DatabaseMigration } from "./migration"
import { Sqlite } from "./sqlite"
import { InstallationChannel } from "../installation/version"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
  /**
   * Read-only connection (`PRAGMA query_only = ON`) with its own permit, for
   * scans that must not queue the writer: a select here never delays a
   * `db.transaction` and a held `begin immediate` never delays a select here.
   * Writes through it fail with a typed `SqlError`. Aliases `db` for `:memory:`
   * databases and when `OPENCODE_DB_SINGLE_CONNECTION=1` (the kill switch).
   * Rules: never take `events.barrier` inside a `read` transaction, and never
   * span an `await`/stream inside one - a long read transaction pins the WAL.
   */
  read: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase

    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run("PRAGMA busy_timeout = 5000")
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
    yield* DatabaseMigration.apply(db)
    const read = yield* openRead(db)

    return { db, read }
  }).pipe(Effect.orDie),
)

// Opened only after the writer has migrated: the file exists and is in WAL
// mode by then, and the reader (which never sets journal_mode) inherits both.
const openRead = Effect.fnUntraced(function* (db: DatabaseShape) {
  if (Flag.OPENCODE_DB_SINGLE_CONNECTION) return db
  const client = yield* (yield* Sqlite.Read).open
  if (Option.isNone(client)) return db
  return yield* makeDatabase.pipe(Effect.provideService(SqlClient, client.value))
})

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  const authority = backendAuthority()
  if (authority) return authority
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  ) {
    return join(Global.Path.data, "opencode.db")
  }
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

function backendAuthority() {
  try {
    const selection = JSON.parse(readFileSync(join(Global.Path.state, "backend-authority.json"), "utf8")) as Partial<{
      version: number
      database: string
      updatedAt: number
    }>
    if (
      selection.version !== 1 ||
      typeof selection.database !== "string" ||
      typeof selection.updatedAt !== "number" ||
      !isAbsolute(selection.database) ||
      !existsSync(selection.database)
    )
      return undefined
    return selection.database
  } catch {
    return undefined
  }
}

export const defaultLayer = Layer.unwrap(
  Effect.gen(function* () {
    return layerFromPath(path())
  }),
).pipe(Layer.provide(Global.defaultLayer))
