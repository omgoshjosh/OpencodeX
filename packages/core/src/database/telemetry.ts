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

/**
 * Native handle lifecycle (OpencodeX-fs2.5). One `Database` layer is exactly
 * one `writer` plus one `reader`; a third open per layer is the defect this
 * gauge exists to catch. Two INFO lines, structured fields only:
 *
 *   db_connection_open   connection_id, role, path, active_connections
 *   db_connection_close  the same fields, after the handle closed
 *
 * `activeConnections()` is the gauge behind `active_connections`: every handle
 * this process has opened through a sqlite layer and not yet closed.
 */

export type ConnectionRole = "writer" | "reader"

export interface ActiveConnection {
  readonly id: number
  readonly role: ConnectionRole
  readonly path: string
}

let sequence = 0
const active = new Map<number, ActiveConnection>()

export const activeConnections = (): ReadonlyArray<ActiveConnection> => [...active.values()]

const annotate = (connection: ActiveConnection) => ({
  connection_id: connection.id,
  role: connection.role,
  path: connection.path,
  active_connections: active.size,
})

/** Registers a handle that `close` will close; the gauge counts it until then. */
export const opened = (role: ConnectionRole, path: string, close: () => void) =>
  Effect.gen(function* () {
    const connection: ActiveConnection = { id: ++sequence, role, path }
    active.set(connection.id, connection)
    yield* Effect.logInfo("db_connection_open").pipe(Effect.annotateLogs(annotate(connection)))
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Effect.sync(close)
        active.delete(connection.id)
        yield* Effect.logInfo("db_connection_close").pipe(Effect.annotateLogs(annotate(connection)))
      }),
    )
    return connection
  })
