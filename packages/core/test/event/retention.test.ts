import { describe, expect } from "bun:test"
import { asc, eq, sql } from "drizzle-orm"
import { Cause, Context, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventRetention } from "@opencode-ai/core/event/retention"
import { EventCursorLeaseTable, EventTable } from "@opencode-ai/core/event/sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { testEffect } from "../lib/effect"

const layer = Layer.mergeAll(EventV2.defaultLayer, Database.defaultLayer, SessionProjector.defaultLayer)
const it = testEffect(layer)

type Db = Database.Interface["db"]

const project = (db: Db, id: ProjectV2.ID) =>
  db
    .insert(ProjectTable)
    .values({ id, worktree: `/tmp/${id}`, sandboxes: [], time_created: 1, time_updated: 1 })
    .onConflictDoNothing({ target: ProjectTable.id })
    .run()
    .pipe(Effect.orDie)

const sessionInfo = (sessionID: SessionSchema.ID, projectID: ProjectV2.ID, title: string) =>
  ({
    id: sessionID,
    slug: "slug",
    projectID,
    directory: `/tmp/${projectID}`,
    title,
    version: "test",
    time: { created: 1_000, updated: 1_000 },
  }) as unknown as typeof SessionLegacy.Event.Created.data.Type["info"]

const textPart = (sessionID: SessionSchema.ID, messageID: SessionLegacy.MessageID, partID: SessionLegacy.PartID, text: string) =>
  ({
    id: partID,
    messageID,
    sessionID,
    type: "text",
    text,
  }) as unknown as typeof SessionLegacy.Event.PartUpdated.data.Type["part"]

const messageInfo = (sessionID: SessionSchema.ID, messageID: SessionLegacy.MessageID, cost: number) =>
  ({
    id: messageID,
    sessionID,
    role: "assistant",
    cost,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: "test",
    providerID: "test",
    mode: "build",
    path: { cwd: "/tmp", root: "/tmp" },
    time: { created: 2_000 },
  }) as unknown as typeof SessionLegacy.Event.MessageUpdated.data.Type["info"]

const withoutWriteStamp = <T extends { time_updated: number }>({ time_updated: _, ...rest }: T) => rest

const journal = (db: Db, sessionID: SessionSchema.ID) =>
  db.select().from(EventTable).where(eq(EventTable.aggregate_id, sessionID)).orderBy(asc(EventTable.seq)).all().pipe(Effect.orDie)

const projection = Effect.fnUntraced(function* (db: Db, sessionID: SessionSchema.ID) {
  return {
    sessions: yield* db.select().from(SessionTable).where(eq(SessionTable.id, sessionID)).all().pipe(Effect.orDie),
    messages: yield* db
      .select()
      .from(MessageTable)
      .where(eq(MessageTable.session_id, sessionID))
      .orderBy(asc(MessageTable.id))
      .all()
      .pipe(Effect.orDie),
    parts: yield* db
      .select()
      .from(PartTable)
      .where(eq(PartTable.session_id, sessionID))
      .orderBy(asc(PartTable.id))
      .all()
      .pipe(Effect.orDie),
  }
})

/**
 * Seeds one aggregate with several revisions of the same entities plus the
 * lifecycle events compaction must never touch, then returns the ids so the
 * assertions can talk about them by name.
 */
const seed = Effect.fnUntraced(function* (events: EventV2.Interface, db: Db) {
  const suffix = Math.random().toString(36).slice(2, 10)
  const projectID = ProjectV2.ID.make(`prj_retention_${suffix}`)
  const sessionID = SessionSchema.ID.make(`ses_retention_${suffix}`)
  const messageID = SessionLegacy.MessageID.make(`msg_retention_${suffix}`)
  const kept = SessionLegacy.PartID.make(`prt_retention_a_${suffix}`)
  const other = SessionLegacy.PartID.make(`prt_retention_b_${suffix}`)
  const removed = SessionLegacy.PartID.make(`prt_retention_c_${suffix}`)

  yield* project(db, projectID)
  yield* events.publish(SessionLegacy.Event.Created, {
    sessionID,
    info: sessionInfo(sessionID, projectID, "created"),
  })
  yield* events.publish(SessionLegacy.Event.MessageUpdated, { sessionID, info: messageInfo(sessionID, messageID, 0) })
  for (const [index, text] of ["one", "two", "three", "four"].entries()) {
    yield* events.publish(SessionLegacy.Event.PartUpdated, {
      sessionID,
      part: textPart(sessionID, messageID, kept, text),
      time: 5_000 + index,
    })
  }
  yield* events.publish(SessionLegacy.Event.PartUpdated, {
    sessionID,
    part: textPart(sessionID, messageID, other, "only"),
    time: 6_000,
  })
  yield* events.publish(SessionLegacy.Event.PartUpdated, {
    sessionID,
    part: textPart(sessionID, messageID, removed, "doomed"),
    time: 7_000,
  })
  yield* events.publish(SessionLegacy.Event.PartRemoved, { sessionID, messageID, partID: removed })
  yield* events.publish(SessionLegacy.Event.MessageUpdated, {
    sessionID,
    info: messageInfo(sessionID, messageID, 1),
  })
  yield* events.publish(SessionLegacy.Event.Updated, {
    sessionID,
    info: sessionInfo(sessionID, projectID, "renamed once"),
  })
  yield* events.publish(SessionLegacy.Event.Updated, {
    sessionID,
    info: sessionInfo(sessionID, projectID, "renamed twice"),
  })

  return { projectID, sessionID, messageID, kept, other, removed }
})

const drain = Effect.fnUntraced(function* (retention: EventRetention.Retention) {
  let total = 0
  for (let pass = 0; pass < 32; pass++) {
    const deleted = yield* retention.compact()
    total += deleted
    if (deleted === 0 && pass > 0) break
  }
  return total
})

describe("EventRetention", () => {
  it.effect("installs and uses the entity revision index", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const index = yield* db
        .all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'event_compaction_entity_idx'`)
        .pipe(Effect.orDie)
      expect(index).toHaveLength(1)

      const plan = yield* db
        .all<{ detail: string }>(
          sql`EXPLAIN QUERY PLAN ${EventRetention.supersededQuery(["missing"], 1)}`,
        )
        .pipe(Effect.orDie)
      expect(plan.some((row) => row.detail.includes("event_compaction_entity_idx"))).toBeTrue()
    }),
  )

  it.effect("drops every revision between an entity's first and last", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })

      const before = yield* journal(db, ids.sessionID)
      yield* drain(retention)
      const after = yield* journal(db, ids.sessionID)

      const partRevisions = (rows: typeof after) =>
        rows.filter((row) => row.type === "message.part.updated.1" && (row.data as any).part.id === ids.kept)

      expect(partRevisions(before).map((row) => (row.data as any).part.text)).toEqual(["one", "two", "three", "four"])
      expect(partRevisions(after).map((row) => (row.data as any).part.text)).toEqual(["one", "four"])
      // The survivors keep their original sequences, so the journal is sparse
      // but still ordered.
      expect(partRevisions(after).map((row) => row.seq)).toEqual([
        partRevisions(before)[0]!.seq,
        partRevisions(before).at(-1)!.seq,
      ])
      expect(after.map((row) => row.seq)).toEqual([...after.map((row) => row.seq)].sort((a, b) => a - b))
      expect(after.length).toBeLessThan(before.length)

      // Two revisions each for the message and the session, and the part that
      // was only ever written once is left exactly as it was.
      expect(after.filter((row) => row.type === "message.updated.1")).toHaveLength(2)
      expect(after.filter((row) => row.type === "session.updated.1")).toHaveLength(2)
      expect(
        after.filter((row) => row.type === "message.part.updated.1" && (row.data as any).part.id === ids.other),
      ).toHaveLength(1)
    }),
  )

  it.effect("pauses compaction while a history snapshot lease is active", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })
      const before = yield* journal(db, ids.sessionID)
      yield* db
        .insert(EventCursorLeaseTable)
        .values({ token: "active-history-drain", fence: 1, expires_at: Date.now() + 60_000 })
        .run()
        .pipe(Effect.orDie)

      const blocked = yield* retention.compactResult()
      expect(blocked.deletedCount).toBe(0)
      expect(blocked.activeLease).toBe("present")
      expect(blocked.blockReason).toBe("active_lease")
      expect(blocked.cursorAction).toBe("hold")
      expect(yield* journal(db, ids.sessionID)).toEqual(before)

      yield* db
        .delete(EventCursorLeaseTable)
        .where(eq(EventCursorLeaseTable.token, "active-history-drain"))
        .run()
        .pipe(Effect.orDie)
      expect(yield* retention.compact()).toBeGreaterThan(0)
    }),
  )

  it.effect("reports readable pass fields for zero-candidate, deleting, and saturated windows", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const empty = yield* EventRetention.make(db, events.barrier)
      const emptyPass = yield* empty.compactResult()
      expect(emptyPass.blockReason).toBe("no_candidates")
      expect(EventRetention.formatPass(emptyPass)).toContain("selected_count=0")
      expect(EventRetention.formatPass(emptyPass)).toContain('failure="none"')

      const ids = yield* seed(events, db)
      const deleting = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })
      const deleted = yield* deleting.compactResult()
      expect(deleted.deletedCount).toBeGreaterThan(0)
      expect(deleted.cursorAction).toBe("advance")
      const line = EventRetention.formatPass(deleted)
      for (const field of [
        "event_retention_pass instance=",
        "duration_ms=",
        "cursor_before=",
        "window_first=",
        "aggregate_count=",
        "selected_count=",
        "deleted_count=",
        "active_lease=",
        "block_reason=",
        "batch_size=",
        "failure=",
      ]) {
        expect(line).toContain(field)
      }

      for (let index = 0; index < 30; index++) {
        yield* events.publish(SessionLegacy.Event.PartUpdated, {
          sessionID: ids.sessionID,
          part: textPart(ids.sessionID, ids.messageID, ids.other, `telemetry ${index}`),
          time: 8_000 + index,
        })
      }
      const saturated = yield* EventRetention.make(db, events.barrier, {
        aggregatesPerPass: 1_000,
        maintenanceBatchSize: 5,
      })
      const held = yield* saturated.compactResult()
      expect(held.selectedCount).toBe(5)
      expect(held.cursorAction).toBe("hold")
    }),
  )

  it.effect("runs one scheduler per database and releases it with its scope", () =>
    Effect.gen(function* () {
      const db = {} as Db
      yield* Effect.scoped(
        Effect.gen(function* () {
          const first = yield* EventRetention.start(db, (effect) => effect, { maintenanceIntervalMs: 60_000 })
          const duplicate = yield* EventRetention.start(db, (effect) => effect, { maintenanceIntervalMs: 60_000 })
          expect(first.schedulerStarted).toBeTrue()
          expect(duplicate.schedulerStarted).toBeFalse()
        }),
      )
      const restarted = yield* Effect.scoped(
        EventRetention.start(db, (effect) => effect, { maintenanceIntervalMs: 60_000 }),
      )
      expect(restarted.schedulerStarted).toBeTrue()
    }),
  )

  it.effect("resets and preserves partial metrics around candidate discovery failures", () =>
    Effect.gen(function* () {
      let calls = 0
      const beforeWindow = {
        all: () => {
          calls++
          if (calls === 1) return Effect.succeed([{ aggregate: "previous-secret" }])
          if (calls === 2) return Effect.succeed([])
          return Effect.die(new Error("SQLITE_BUSY before-window"))
        },
      } as unknown as Db
      const retention = yield* EventRetention.make(beforeWindow, (effect) => effect)
      yield* retention.compactResult()
      const beforeCause = yield* retention.compactResult().pipe(Effect.sandbox, Effect.flip)
      const before = retention.failure(EventRetention.failureReason(beforeCause))
      expect(before.aggregateCount).toBe(0)
      expect(before.selectedCount).toBe(0)
      expect(before.windowFirst).toBe("none")

      calls = 0
      const duringCandidates = {
        all: () => {
          calls++
          if (calls === 1) return Effect.succeed([{ aggregate: "candidate-secret" }])
          return Effect.die(new Error("SQLITE_BUSY during-candidates"))
        },
      } as unknown as Db
      const during = yield* EventRetention.make(duringCandidates, (effect) => effect)
      const duringCause = yield* during.compactResult().pipe(Effect.sandbox, Effect.flip)
      const partial = during.failure(EventRetention.failureReason(duringCause))
      expect(partial.aggregateCount).toBe(1)
      expect(partial.selectedCount).toBe(0)
      expect(partial.windowFirst).toMatch(/^id-[0-9a-f]{12}$/)
      expect(EventRetention.formatPass(partial)).not.toContain("candidate-secret")
    }),
  )

  it.effect("collapses a long revision chain to two rows", () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(
        Layer.fresh(SessionProjector.layer).pipe(
          Layer.provideMerge(Layer.fresh(EventV2.layer)),
          Layer.provideMerge(Layer.fresh(Database.layerFromPath(":memory:"))),
        ),
      )
      const events = Context.get(context, EventV2.Service)
      const db = Context.get(context, Database.Service).db
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })

      for (let index = 0; index < 50; index++) {
        yield* events.publish(SessionLegacy.Event.PartUpdated, {
          sessionID: ids.sessionID,
          part: textPart(ids.sessionID, ids.messageID, ids.other, `chunk ${index}`),
          time: 8_000 + index,
        })
      }

      expect(yield* drain(retention)).toBeGreaterThan(50)
      const after = yield* journal(db, ids.sessionID)
      const revisions = after.filter(
        (row) => row.type === "message.part.updated.1" && (row.data as any).part.id === ids.other,
      )
      expect(revisions.map((row) => (row.data as any).part.text)).toEqual(["only", "chunk 49"])
      expect(yield* retention.compact()).toBe(0)
    }),
  )

  it.effect("drains a long revision chain in bounded batches", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, {
        aggregatesPerPass: 1_000,
        maintenanceBatchSize: 5,
      })

      for (let index = 0; index < 30; index++) {
        yield* events.publish(SessionLegacy.Event.PartUpdated, {
          sessionID: ids.sessionID,
          part: textPart(ids.sessionID, ids.messageID, ids.other, `bounded ${index}`),
          time: 8_000 + index,
        })
      }

      const deleted: number[] = []
      for (let pass = 0; pass < 16; pass++) {
        const count = yield* retention.compact()
        deleted.push(count)
        if (count === 0) break
      }
      expect(deleted.every((count) => count <= 5)).toBeTrue()
      expect(deleted.filter((count) => count === 5).length).toBeGreaterThan(1)
      const revisions = (yield* journal(db, ids.sessionID)).filter(
        (row) => row.type === "message.part.updated.1" && (row.data as any).part.id === ids.other,
      )
      expect(revisions.map((row) => (row.data as any).part.text)).toEqual(["only", "bounded 29"])
    }),
  )

  it.effect("never deletes created, deleted or removed events", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })

      yield* drain(retention)
      const after = yield* journal(db, ids.sessionID)

      expect(after.filter((row) => row.type === "session.created.1")).toHaveLength(1)
      expect(after.filter((row) => row.type === "message.part.removed.1")).toHaveLength(1)
      // The removed part's own revision is superseded by nothing, so it stays
      // put: replay still needs it to insert the row the removal deletes.
      expect(
        after.filter((row) => row.type === "message.part.updated.1" && (row.data as any).part.id === ids.removed),
      ).toHaveLength(1)
    }),
  )

  it.effect("replays the compacted journal to identical rows", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const { db } = yield* Database.Service
      const ids = yield* seed(events, db)
      const retention = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })

      const expected = yield* projection(db, ids.sessionID)
      yield* drain(retention)
      const compacted = yield* journal(db, ids.sessionID)

      // A pristine database replays the surviving stream from scratch, which is
      // exactly what session warp and /sync/history do on the far side.
      // `Layer.fresh` is what stops the memo map from handing back the layers
      // this test is already running on - the replay has to land on an empty
      // database to prove anything.
      const context = yield* Layer.build(
        Layer.fresh(SessionProjector.layer).pipe(
          Layer.provideMerge(Layer.fresh(EventV2.layer)),
          Layer.provideMerge(Layer.fresh(Database.layerFromPath(":memory:"))),
        ),
      )
      const target = Context.get(context, EventV2.Service)
      const replica = Context.get(context, Database.Service).db
      yield* project(replica, ids.projectID)
      yield* target.replayAll(
        compacted.map((row) => ({
          id: row.id,
          aggregateID: row.aggregate_id,
          seq: row.seq,
          type: row.type,
          data: row.data,
        })),
      )
      const actual = yield* projection(replica, ids.sessionID)

      expect(actual.sessions).toEqual(expected.sessions)
      // `time_updated` is a wall-clock column stamped by the writer, not
      // something the journal carries, so it is the one field a second replay
      // cannot reproduce.
      expect(actual.messages.map(withoutWriteStamp)).toEqual(expected.messages.map(withoutWriteStamp))
      expect(actual.parts.map(withoutWriteStamp)).toEqual(expected.parts.map(withoutWriteStamp))
      expect(actual.parts.map((row) => row.time_created)).toEqual(expected.parts.map((row) => row.time_created))
    }),
  )

  it.effect("reports actual deletions and retains redacted partial metrics when passes race or fail", () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(
        Layer.fresh(SessionProjector.layer).pipe(
          Layer.provideMerge(Layer.fresh(EventV2.layer)),
          Layer.provideMerge(Layer.fresh(Database.layerFromPath(":memory:"))),
        ),
      )
      const events = Context.get(context, EventV2.Service)
      const db = Context.get(context, Database.Service).db
      const ids = yield* seed(events, db)
      const second = yield* EventRetention.make(db, events.barrier, { aggregatesPerPass: 1_000 })
      const first = yield* EventRetention.make(
        db,
        (effect) =>
          Effect.gen(function* () {
            yield* second.compact()
            return yield* effect
          }),
        { aggregatesPerPass: 1_000 },
      )

      const raced = yield* first.compactResult()
      expect(raced.selectedCount).toBeGreaterThan(0)
      expect(raced.deletedCount).toBe(0)
      expect(raced.blockReason).toBe("already_deleted")

      yield* seed(events, db)
      const failing = yield* EventRetention.make(
        db,
        () => Effect.die(new Error(`SQLITE_BUSY ${ids.sessionID}`)),
        { aggregatesPerPass: 1_000 },
      )
      const cause = yield* failing.compactResult().pipe(Effect.sandbox, Effect.flip)
      const failure = failing.failure(EventRetention.failureReason(cause))
      expect(failure.selectedCount).toBeGreaterThan(0)
      expect(failure.aggregateCount).toBeGreaterThan(0)
      expect(failure.failure).toBe("sqlite_busy")
      expect(EventRetention.failureReason(Cause.die(`secret ${ids.sessionID}`))).toBe("unknown")

      const line = EventRetention.formatPass(failure)
      expect(line).not.toContain(ids.sessionID)
      expect(line).toMatch(/instance="retention-\d+-[0-9a-f-]{36}"/)
      expect(line).toMatch(/window_first="id-[0-9a-f]{12}"/)
      expect(line).toContain('failure="sqlite_busy"')
      expect(line).not.toContain("secret")
    }),
  )
})
