export * as EventRetention from "./retention"

import { sql } from "drizzle-orm"
import { Cause, Duration, Effect, Schedule } from "effect"
import { createHash } from "crypto"
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

let retentionInstances = 0

const PART_UPDATED = "message.part.updated.1"
const MESSAGE_UPDATED = "message.updated.1"
const SESSION_UPDATED = "session.updated.1"

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

type Pass = {
  instance: string
  pass: number
  durationMs: number
  cursorBefore: string
  cursorAfter: string
  windowFirst: string
  windowLast: string
  aggregateCount: number
  selectedCount: number
  deletedCount: number
  activeLease: string
  blockReason: string
  cursorAction: string
  batchSize: number
  failure: string
}

type PassDetails = Omit<Pass, "instance" | "pass" | "durationMs">

const opaque = (value: string) =>
  value ? `id-${createHash("sha256").update(value).digest("hex").slice(0, 12)}` : "none"

export const failureReason = (cause: Cause.Cause<unknown>) => {
  const message = Cause.pretty(cause).toLowerCase()
  if (message.includes("sqlite_busy")) return "sqlite_busy"
  if (message.includes("sqlite")) return "sqlite_error"
  if (message.includes("interrupt")) return "interrupted"
  return "unknown"
}

export const formatPass = (pass: Pass) =>
  [
    "event_retention_pass",
    `instance=${JSON.stringify(pass.instance)}`,
    `pass=${pass.pass}`,
    `duration_ms=${pass.durationMs}`,
    `cursor_before=${JSON.stringify(pass.cursorBefore)}`,
    `cursor_after=${JSON.stringify(pass.cursorAfter)}`,
    `window_first=${JSON.stringify(pass.windowFirst)}`,
    `window_last=${JSON.stringify(pass.windowLast)}`,
    `aggregate_count=${pass.aggregateCount}`,
    `selected_count=${pass.selectedCount}`,
    `deleted_count=${pass.deletedCount}`,
    `active_lease=${JSON.stringify(pass.activeLease)}`,
    `block_reason=${JSON.stringify(pass.blockReason)}`,
    `cursor_action=${JSON.stringify(pass.cursorAction)}`,
    `batch_size=${pass.batchSize}`,
    `failure=${JSON.stringify(pass.failure)}`,
  ].join(" ")

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
  const instance = `retention-${++retentionInstances}-${crypto.randomUUID()}`
  let passes = 0
  let passStarted = 0
  let partial: PassDetails = {
    cursorBefore: "none",
    cursorAfter: "none",
    windowFirst: "none",
    windowLast: "none",
    aggregateCount: 0,
    selectedCount: 0,
    deletedCount: 0,
    activeLease: "absent",
    blockReason: "failure",
    cursorAction: "hold",
    batchSize: settings.maintenanceBatchSize,
    failure: "none",
  }

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

  const compactResult = Effect.fn("EventRetention.compactResult")(function* () {
    const pass = ++passes
    const started = Date.now()
    const cursorBefore = cursor
    passStarted = started
    const finish = (result: PassDetails) => {
      partial = result
      return { instance, pass, durationMs: Date.now() - started, ...result } satisfies Pass
    }
    const aggregates = yield* window()
    if (aggregates.length === 0) {
      cursor = ""
      return finish({
        cursorBefore: opaque(cursorBefore),
        cursorAfter: opaque(cursor),
        windowFirst: "none",
        windowLast: "none",
        aggregateCount: 0,
        selectedCount: 0,
        deletedCount: 0,
        activeLease: "absent",
        blockReason: "no_aggregates",
        cursorAction: "reset",
        batchSize: settings.maintenanceBatchSize,
        failure: "none",
      })
    }
    // The read runs outside the write transaction on purpose: a concurrent
    // append can only add a higher sequence, which cannot turn a row already
    // classified as superseded back into the first or last revision.
    const doomed = yield* superseded(aggregates)
    partial = {
      ...partial,
      cursorBefore: opaque(cursorBefore),
      cursorAfter: opaque(cursor),
      windowFirst: opaque(aggregates[0] ?? ""),
      windowLast: opaque(aggregates.at(-1) ?? ""),
      aggregateCount: aggregates.length,
      selectedCount: doomed.length,
    }
    if (doomed.length === 0) {
      advance(aggregates)
      return finish({
        ...partial,
        cursorAfter: opaque(cursor),
        blockReason: "no_candidates",
        cursorAction: "advance",
        batchSize: settings.maintenanceBatchSize,
        failure: "none",
      })
    }
    const ids = doomed.map((row) => row.id)
    const deletion = yield* barrier(
      db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const now = Date.now()
              yield* transaction.run(sql`DELETE FROM event_cursor_lease WHERE expires_at <= ${now}`)
              const lease = yield* transaction.get<{ active: number }>(
                sql`SELECT EXISTS(SELECT 1 FROM event_cursor_lease WHERE expires_at > ${now}) AS active`,
              )
              if (lease?.active) return { deleted: 0, activeLease: "present" }
              let deleted = 0
              for (let index = 0; index < ids.length; index += DELETE_CHUNK) {
                deleted += (
                  yield* transaction.all<{ id: string }>(
                    sql`DELETE FROM event WHERE id IN ${ids.slice(index, index + DELETE_CHUNK)} RETURNING id`,
                  )
                ).length
              }
              return { deleted, activeLease: "absent" }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie),
    )
    if (deletion.activeLease === "present") {
      return finish({
        ...partial,
        deletedCount: 0,
        activeLease: "present",
        blockReason: "active_lease",
        cursorAction: "hold",
        batchSize: settings.maintenanceBatchSize,
        failure: "none",
      })
    }
    // A saturated batch means this window still has more to shed, so hold the
    // cursor rather than waiting a full rotation to come back to it.
    if (ids.length < settings.maintenanceBatchSize) advance(aggregates)
    return finish({
      ...partial,
      cursorAfter: opaque(cursor),
      deletedCount: deletion.deleted,
      activeLease: "absent",
      blockReason: deletion.deleted === 0 ? "already_deleted" : "none",
      cursorAction: ids.length < settings.maintenanceBatchSize ? "advance" : "hold",
      batchSize: settings.maintenanceBatchSize,
      failure: "none",
    })
  })

  const compact = Effect.fn("EventRetention.compact")(function* () {
    return (yield* compactResult()).deletedCount
  })

  return {
    compact,
    compactResult,
    instance,
    failure: (failure: string) => ({
      instance,
      pass: passes,
      durationMs: Date.now() - passStarted,
      ...partial,
      blockReason: "failure",
      failure,
    }),
  } satisfies Retention & {
    compactResult: () => Effect.Effect<Pass>
    instance: string
    failure: (failure: string) => Pass
  }
})

/** Starts the background compaction loop in the current scope. */
export const start = Effect.fnUntraced(function* (
  db: Database.Interface["db"],
  barrier: Barrier,
  options?: RetentionOptions,
) {
  const retention = yield* make(db, barrier, options)
  const interval = Math.max(1, options?.maintenanceIntervalMs ?? MAINTENANCE_INTERVAL_MS)
  const batchSize = Math.max(1, options?.maintenanceBatchSize ?? MAINTENANCE_BATCH_SIZE)
  yield* Effect.logInfo(
    `event_retention_scheduled instance=${JSON.stringify(retention.instance)} interval_ms=${interval} batch_size=${batchSize}`,
  )
  yield* Effect.sleep(Duration.millis(interval)).pipe(
    Effect.andThen(retention.compactResult()),
    Effect.tap((pass) => Effect.logInfo(formatPass(pass))),
    // A failed pass (e.g. SQLITE_BUSY outliving the busy timeout in
    // multi-process use) must not kill the loop for the life of the process;
    // log it and try again next interval.
    Effect.catchCause((cause) =>
      Effect.logWarning(
        formatPass(retention.failure(failureReason(cause))),
      ),
    ),
    Effect.repeat(Schedule.forever),
    Effect.forkScoped,
  )
  return retention
})
