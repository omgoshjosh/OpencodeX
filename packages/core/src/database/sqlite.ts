export * as Sqlite from "./sqlite"

import { Context } from "effect"
import type { Effect, Option, Scope } from "effect"
import type { SqlClient } from "effect/unstable/sql/SqlClient"
import type { drizzle } from "drizzle-orm/bun-sqlite"

export type DrizzleClient = ReturnType<typeof drizzle>
export class Native extends Context.Service<Native, unknown>()("@opencode-ai/core/database/SqliteNative") {}
export class Drizzle extends Context.Service<Drizzle, DrizzleClient>()("@opencode-ai/core/database/SqliteDrizzle") {}

/**
 * Opens the second, read-only (`PRAGMA query_only = ON`) connection to the same
 * database. Called by `Database.layer` only after migrations have been applied.
 * `None` means the backend cannot or should not open a second connection
 * (`:memory:` is per-connection) and `Database.read` must alias `db`.
 */
export interface ReadInterface {
  readonly open: Effect.Effect<Option.Option<SqlClient>, never, Scope.Scope>
}
export class Read extends Context.Service<Read, ReadInterface>()("@opencode-ai/core/database/SqliteRead") {}
