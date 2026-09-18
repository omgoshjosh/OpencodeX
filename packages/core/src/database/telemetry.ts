import { Clock } from "effect/Clock"
import * as Effect from "effect/Effect"
import type * as Fiber from "effect/Fiber"

/**
 * Per-connection timing for the two SQLite handles (OpencodeX-fs2 PR3). Every
 * statement in the process pays two separate prices that production could not
 * tell apart: the wait for the connection's `Semaphore(1)` — a transaction on
 * the same handle holds it begin→commit — and the statement itself, which runs
 * synchronously on the JS thread. `event_barrier_slow_*` only sees the sum.
 *
 * Two WARN lines, each emitted only once its own measure crosses `DB_SLOW_MS`
 * (the `event_barrier_slow_listener` bar), with structured fields via
 * `Effect.annotateLogs` — never an object as the message (OpencodeX-2kg):
 *
 *   db_write_queue_slow  write_queue_ms — waited for the permit; `scope` says
 *                        whether a single statement or a transaction queued.
 *   db_read_slow         read_ms — time inside one statement, with a preview
 *                        of its SQL.
 *
 * Both connections report both: `connection=db` is the writer, `read` the
 * `query_only` handle. The clock comes from the fiber so a test can script it;
 * the uncontended path reads it and compares, nothing more.
 */

export const DB_SLOW_MS = 1_000
const SQL_PREVIEW_CHARS = 160

export type ConnectionName = "db" | "read"
type Waiter = Fiber.Fiber<unknown, unknown>

export const now = (fiber: Waiter) => fiber.getRef(Clock).currentTimeMillisUnsafe()

export const queued = (
  connection: ConnectionName,
  scope: "statement" | "transaction",
  fiber: Waiter,
  requested: number,
) => {
  const waited = now(fiber) - requested
  if (waited <= DB_SLOW_MS) return Effect.void
  return Effect.logWarning("db_write_queue_slow").pipe(
    Effect.annotateLogs({ connection, scope, fiber: fiber.id, write_queue_ms: waited }),
  )
}

export const executed = (connection: ConnectionName, fiber: Waiter, started: number, sql: string) => {
  const elapsed = now(fiber) - started
  if (elapsed <= DB_SLOW_MS) return Effect.void
  return Effect.logWarning("db_read_slow").pipe(
    Effect.annotateLogs({ connection, fiber: fiber.id, read_ms: elapsed, sql: sql.slice(0, SQL_PREVIEW_CHARS) }),
  )
}
