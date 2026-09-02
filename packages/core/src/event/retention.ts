export * as EventRetention from "./retention"

import { sql } from "drizzle-orm"
import { Duration, Effect, Schedule } from "effect"
import type { Database } from "../database/database"

/**
 * Compaction for the append-only `event` journal.
 *
 * The v1 session projector is insert-or-update-by-id for every entity it owns,
 * so the only two revisions of an entity that leave a trace are the first (it
 * performs the insert, which fixes the row's `time_created` and its position in
 * the stream) and the last (it fixes the row's final `data`). Everything in
 * between is dead weight: a chatty tool call used to append tens of MB of
 * intermediate part JSON that nothing will ever read again.
 *
 * Keeping the first revision as well as the last is what makes the rewritten
 * stream *exactly* equivalent rather than nearly so. Dropping it would move an
 * entity's insert to wherever its final revision sits, which reorders inserts
 * past rows that reference them - a message whose last revision trails its own
 * parts would fail `part.message_id`'s foreign key on replay - and would stamp
 * `part.time_created` from the wrong revision.
 *
 * Created, deleted and removed events are never touched, and no aggregate is
 * ever time-pruned as a whole, so session warp and `/sync/history` still replay
 * a stream that lands on identical rows - just a much shorter one. Sequence
 * numbers become sparse, which is why `commitSyncEvent` and `replayAll` require
 * sequences to be strictly increasing rather than dense.
 */

const MAINTENANCE_INTERVAL_MS = 60_000
const MAINTENANCE_BATCH_SIZE = 5_000
const AGGREGATES_PER_PASS = 64
const DELETE_CHUNK = 500

const PART_UPDATED = "message.part.updated.1"
const MESSAGE_UPDATED = "message.updated.1"
const SESSION_UPDATED = "session.updated.1"
const scheduled = new WeakSet<object>()

/**
 * Types whose rows are revisions of an entity, and therefore compactable.
 * `session.created` / `session.deleted`, `message.removed` and
 * `message.part.removed` are absent on purpose: they are not revisions of
 * anything, and dropping one would change the replayed result.
 */
const COMPACTABLE = [PART_UPDATED, MESSAGE_UPDATED, SESSION_UPDATED]

/**
 * Identity of the entity a row revises, within its aggregate. A part revises a
 * part, a message revises a message, and `session.updated` revises the
 * aggregate itself so its key is constant.
 */
const entity = (alias: ReturnType<typeof sql.raw>) => sql`CASE ${alias}.type
  WHEN ${sql.raw(`'${PART_UPDATED}'`)} THEN json_extract(${alias}.data, '$.part.id')
  WHEN ${sql.raw(`'${MESSAGE_UPDATED}'`)} THEN json_extract(${alias}.data, '$.info.id')
  ELSE ''
END`

export const supersededQuery = (aggregates: string[], batchSize: number) =>
  sql`SELECT candidate.id
      FROM event AS candidate
      WHERE candidate.aggregate_id IN ${aggregates}
        AND candidate.type IN ${COMPACTABLE}
        AND EXISTS (
          SELECT 1 FROM event AS older
          WHERE older.aggregate_id = candidate.aggregate_id
            AND older.type = candidate.type
            AND older.seq < candidate.seq
            AND ${entity(sql.raw("older"))} = ${entity(sql.raw("candidate"))}
        )
        AND EXISTS (
          SELECT 1 FROM event AS newer
          WHERE newer.aggregate_id = candidate.aggregate_id
            AND newer.type = candidate.type
            AND newer.seq > candidate.seq
            AND ${entity(sql.raw("newer"))} = ${entity(sql.raw("candidate"))}
        )
      LIMIT ${batchSize}`

export type RetentionOptions = {
  maintenanceIntervalMs?: number
  maintenanceBatchSize?: number
  aggregatesPerPass?: number
}

export interface Retention {
  /** Runs one bounded compaction pass. Returns the number of rows deleted. */
  compact: () => Effect.Effect<number>
}

type Barrier = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>

export const make = Effect.fn("EventRetention.make")(function* (
  db: Database.Interface["db"],
  barrier: Barrier,
  options?: RetentionOptions,
) {
  const settings = {
    maintenanceIntervalMs: Math.max(1, options?.maintenanceIntervalMs ?? MAINTENANCE_INTERVAL_MS),
    maintenanceBatchSize: Math.max(1, options?.maintenanceBatchSize ?? MAINTENANCE_BATCH_SIZE),
    aggregatesPerPass: Math.max(1, options?.aggregatesPerPass ?? AGGREGATES_PER_PASS),
  }

  // The journal is only indexed by (aggregate_id, seq), so a pass walks a
  // window of aggregates rather than scanning the whole table. The cursor
  // rotates so every aggregate is visited eventually, and it stays put while a
  // window is still shedding a full batch so a backlogged aggregate drains at
  // batch-size per pass instead of once per full rotation.
  let cursor = ""

  const window = Effect.fnUntraced(function* () {
    const rows = yield* db
      .all<{ aggregate: string }>(
        sql`SELECT aggregate_id AS aggregate FROM event_sequence
            WHERE aggregate_id > ${cursor}
            ORDER BY aggregate_id
            LIMIT ${settings.aggregatesPerPass}`,
      )
      .pipe(Effect.orDie)
    return rows.map((row) => row.aggregate)
  })

  const advance = (aggregates: string[]) => {
    cursor = aggregates.length < settings.aggregatesPerPass ? "" : (aggregates.at(-1) ?? "")
  }

  /** Rows with an older and newer revision of the same entity. Correlated
   * existence checks keep the query's memory bounded; window functions made
   * SQLite materialize and sort every revision in the aggregate window before
   * applying the output limit.
   */
  const superseded = (aggregates: string[]) =>
    db.all<{ id: string }>(supersededQuery(aggregates, settings.maintenanceBatchSize)).pipe(Effect.orDie)

  const compact = Effect.fn("EventRetention.compact")(function* () {
    return (yield* pass()).deleted
  })

  const pass = Effect.fn("EventRetention.pass")(function* () {
    const started = Date.now()
    const aggregates = yield* window()
    if (aggregates.length === 0) {
      cursor = ""
      return { deleted: 0, candidates: 0, aggregates: 0, cursor, durationMs: Date.now() - started }
    }
    // The read runs outside the write transaction on purpose: a concurrent
    // append can only add a higher sequence, which cannot turn a row already
    // classified as superseded back into the first or last revision.
    const doomed = yield* superseded(aggregates)
    if (doomed.length === 0) {
      advance(aggregates)
      return {
        deleted: 0,
        candidates: 0,
        aggregates: aggregates.length,
        cursor,
        durationMs: Date.now() - started,
      }
    }
    const ids = doomed.map((row) => row.id)
    const deleted = yield* barrier(
      db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const now = Date.now()
              yield* transaction.run(sql`DELETE FROM event_cursor_lease WHERE expires_at <= ${now}`)
              const lease = yield* transaction.get<{ active: number }>(
                sql`SELECT EXISTS(SELECT 1 FROM event_cursor_lease WHERE expires_at > ${now}) AS active`,
              )
              if (lease?.active) return 0
              let deleted = 0
              for (let index = 0; index < ids.length; index += DELETE_CHUNK) {
                yield* transaction.run(sql`DELETE FROM event WHERE id IN ${ids.slice(index, index + DELETE_CHUNK)}`)
                deleted +=
                  (yield* transaction.get<{ count: number }>(sql`SELECT changes() AS count`))?.count ?? 0
              }
              return deleted
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie),
    )
    if (deleted === 0) {
      return {
        deleted,
        candidates: ids.length,
        aggregates: aggregates.length,
        cursor,
        durationMs: Date.now() - started,
      }
    }
    // A saturated batch means this window still has more to shed, so hold the
    // cursor rather than waiting a full rotation to come back to it.
    if (ids.length < settings.maintenanceBatchSize) advance(aggregates)
    return {
      deleted,
      candidates: ids.length,
      aggregates: aggregates.length,
      cursor,
      durationMs: Date.now() - started,
    }
  })

  return { compact, pass } satisfies Retention & { pass: typeof pass }
})

/** Starts the background compaction loop in the current scope. */
export const start = Effect.fnUntraced(function* (
  db: Database.Interface["db"],
  barrier: Barrier,
  options?: RetentionOptions,
) {
  const retention = yield* make(db, barrier, options)
  if (scheduled.has(db)) return retention
  scheduled.add(db)
  yield* Effect.addFinalizer(() => Effect.sync(() => scheduled.delete(db)))
  const interval = Math.max(1, options?.maintenanceIntervalMs ?? MAINTENANCE_INTERVAL_MS)
  const batchSize = Math.max(1, options?.maintenanceBatchSize ?? MAINTENANCE_BATCH_SIZE)
  yield* Effect.logInfo("event retention scheduled").pipe(Effect.annotateLogs({ intervalMs: interval, batchSize }))
  yield* Effect.sleep(Duration.millis(interval)).pipe(
    Effect.andThen(retention.pass()),
    Effect.tap((pass) =>
      Effect.logInfo("event retention pass complete").pipe(
        Effect.annotateLogs({
          actualRowsCompacted: pass.deleted,
          candidates: pass.candidates,
          aggregatesScanned: pass.aggregates,
          durationMs: pass.durationMs,
          batchSize,
          cursor: pass.cursor,
        }),
      ),
    ),
    // A failed pass (e.g. SQLITE_BUSY outliving the busy timeout in
    // multi-process use) must not kill the loop for the life of the process;
    // log it and try again next interval.
    Effect.catchCause((cause) =>
      Effect.logWarning("event retention pass failed").pipe(Effect.annotateLogs("cause", cause)),
    ),
    Effect.repeat(Schedule.forever),
    Effect.forkScoped,
  )
  return retention
})
