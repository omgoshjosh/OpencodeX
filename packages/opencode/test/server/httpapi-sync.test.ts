import { afterEach, describe, expect, mock, spyOn } from "bun:test"
import { Context, Effect, Layer, Schema } from "effect"
import { HttpClient, HttpClientRequest } from "effect/unstable/http"
import { Flag } from "@opencode-ai/core/flag/flag"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { eq } from "drizzle-orm"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { HistoryEvent, HistoryPageResponse, SyncPaths } from "../../src/server/routes/instance/httpapi/groups/sync"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { Session } from "@/session/session"
import * as Log from "@opencode-ai/core/util/log"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

void Log.init({ print: false })

const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
const context = Context.empty() as Context.Context<unknown>
const it = testEffect(Layer.mergeAll(Session.defaultLayer, Database.defaultLayer, httpApiLayer))

afterEach(async () => {
  mock.restore()
  Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
  await disposeAllInstances()
  await resetDatabase()
})

describe("sync HttpApi", () => {
  it.instance(
    "serves sync routes",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const info = spyOn(Log.create({ service: "server.sync" }), "info")
        const session = yield* Session.use.create({ title: "sync" })

        const started = yield* requestInDirectory(SyncPaths.start, tmp.directory, { method: "POST", headers })
        expect(started.status).toBe(200)
        expect(yield* started.json).toBe(true)

        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({}),
        })
        expect(history.status).toBe(200)
        const rows = (yield* history.json) as Array<{
          id: string
          aggregate_id: string
          seq: number
          type: string
          data: Record<string, unknown>
        }>
        expect(rows.map((row) => row.aggregate_id)).toContain(session.id)

        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            events: rows
              .filter((row) => row.aggregate_id === session.id)
              .map((row) => ({
                id: row.id,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              })),
          }),
        })
        expect(replayed.status).toBe(200)
        expect(yield* replayed.json).toEqual({ sessionID: session.id })
        expect(info.mock.calls.some(([message]) => message === "sync replay requested")).toBe(true)
        expect(info.mock.calls.some(([message]) => message === "sync replay complete")).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "scopes sync history to the requesting directory",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const sessionA = yield* Session.use.create({ title: "alpha" })
        const sessionB = yield* Session.use.create({ title: "beta" })

        // The hub DB is shared across projects; give sessionB a different
        // directory so we can prove history stays scoped per project. The
        // directory travels as an optional query parameter so clients that
        // omit it keep the upstream full-journal contract.
        const { db } = yield* Database.Service
        const other = path.join(tmp.directory, "other-project")
        yield* db
          .update(SessionTable)
          .set({ directory: other })
          .where(eq(SessionTable.id, sessionB.id))
          .run()
          .pipe(Effect.orDie)

        const unscoped = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({}),
        })
        expect(unscoped.status).toBe(200)
        const unscopedRows = Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* unscoped.json)
        expect(unscopedRows.map((row) => row.aggregate_id)).toContain(sessionA.id)
        expect(unscopedRows.map((row) => row.aggregate_id)).toContain(sessionB.id)

        const historyA = yield* HttpClientRequest.post(
          `${SyncPaths.history}?directory=${encodeURIComponent(tmp.directory)}`,
        ).pipe(HttpClientRequest.bodyJson({}), Effect.flatMap(HttpClient.execute))
        expect(historyA.status).toBe(200)
        const rowsA = Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* historyA.json)
        expect(rowsA.map((row) => row.aggregate_id)).toContain(sessionA.id)
        expect(rowsA.map((row) => row.aggregate_id)).not.toContain(sessionB.id)

        const historyB = yield* HttpClientRequest.post(
          `${SyncPaths.history}?directory=${encodeURIComponent(other)}`,
        ).pipe(HttpClientRequest.bodyJson({}), Effect.flatMap(HttpClient.execute))
        expect(historyB.status).toBe(200)
        const rowsB = Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* historyB.json)
        expect(rowsB.map((row) => row.aggregate_id)).toContain(sessionB.id)
        expect(rowsB.map((row) => row.aggregate_id)).not.toContain(sessionA.id)

        const pageA = yield* HttpClientRequest.post(
          `${SyncPaths.historyPage}?directory=${encodeURIComponent(tmp.directory)}`,
        ).pipe(HttpClientRequest.bodyJson({ state: {} }), Effect.flatMap(HttpClient.execute))
        const scopedPageA = Schema.decodeUnknownSync(HistoryPageResponse)(yield* pageA.json)
        expect(scopedPageA.events.map((row) => row.aggregate_id)).toContain(sessionA.id)
        expect(scopedPageA.events.map((row) => row.aggregate_id)).not.toContain(sessionB.id)

        const pageB = yield* HttpClientRequest.post(
          `${SyncPaths.historyPage}?directory=${encodeURIComponent(other)}`,
        ).pipe(HttpClientRequest.bodyJson({ state: {} }), Effect.flatMap(HttpClient.execute))
        const scopedPageB = Schema.decodeUnknownSync(HistoryPageResponse)(yield* pageB.json)
        expect(scopedPageB.events.map((row) => row.aggregate_id)).toContain(sessionB.id)
        expect(scopedPageB.events.map((row) => row.aggregate_id)).not.toContain(sessionA.id)

        const emptyDirectory = yield* HttpClientRequest.post(`${SyncPaths.history}?directory=`).pipe(
          HttpClientRequest.bodyJson({}),
          Effect.flatMap(HttpClient.execute),
        )
        expect(Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* emptyDirectory.json)).toEqual([])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "pages sync history instead of materializing the journal",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "paged sync" })
        const { db } = yield* Database.Service
        const sequence = yield* db
          .select({ seq: EventSequenceTable.seq })
          .from(EventSequenceTable)
          .where(eq(EventSequenceTable.aggregate_id, session.id))
          .get()
          .pipe(Effect.orDie)
        const appended = 600
        const appendedEvents = Array.from({ length: appended }, (_, index) => ({
          id: EventV2.ID.make(`evt_paged_${crypto.randomUUID()}`),
          aggregate_id: session.id,
          seq: sequence!.seq + index + 1,
          type: "session.updated.1",
          data: { info: { id: session.id, title: `revision ${index}` } },
        }))
        yield* db
          .insert(EventTable)
          .values(appendedEvents)
          .run()
          .pipe(Effect.orDie)
        yield* db
          .update(EventSequenceTable)
          .set({ seq: sequence!.seq + appended })
          .where(eq(EventSequenceTable.aggregate_id, session.id))
          .run()
          .pipe(Effect.orDie)

        const first = yield* requestInDirectory(
          `${SyncPaths.history}?directory=${encodeURIComponent(tmp.directory)}`,
          tmp.directory,
          { method: "POST", headers, body: JSON.stringify({}) },
        )
        const firstRows = Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* first.json)
        expect(firstRows).toHaveLength(512)

        const second = yield* requestInDirectory(
          `${SyncPaths.history}?directory=${encodeURIComponent(tmp.directory)}`,
          tmp.directory,
          {
            method: "POST",
            headers,
            body: JSON.stringify({ [session.id]: firstRows.at(-1)!.seq }),
          },
        )
        const secondRows = Schema.decodeUnknownSync(Schema.Array(HistoryEvent))(yield* second.json)
        expect(secondRows.length).toBeGreaterThan(0)
        expect(secondRows.length).toBeLessThan(512)
        expect(new Set([...firstRows, ...secondRows].map((row) => row.id)).size).toBe(
          firstRows.length + secondRows.length,
        )
        expect(secondRows[0].seq).toBeGreaterThan(firstRows.at(-1)!.seq)

        const pageOne = yield* requestInDirectory(SyncPaths.historyPage, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ state: {} }),
        })
        const firstPage = Schema.decodeUnknownSync(HistoryPageResponse)(yield* pageOne.json)
        expect(firstPage.events).toHaveLength(512)
        expect(firstPage.next).toBeString()
        yield* db
          .delete(EventTable)
          .where(eq(EventTable.id, appendedEvents.at(-1)!.id))
          .run()
          .pipe(Effect.orDie)
        const lateID = EventV2.ID.make(`evt_paged_late_${crypto.randomUUID()}`)
        yield* db
          .insert(EventTable)
          .values({
            id: lateID,
            aggregate_id: session.id,
            seq: sequence!.seq + appended + 1,
            type: "session.updated.1",
            data: { info: { id: session.id, title: "after fence" } },
          })
          .run()
          .pipe(Effect.orDie)
        const state = Object.fromEntries(firstPage.events.map((event) => [event.aggregate_id, event.seq]))
        const pageTwo = yield* requestInDirectory(SyncPaths.historyPage, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ state, cursor: firstPage.next }),
        })
        const finalPage = Schema.decodeUnknownSync(HistoryPageResponse)(yield* pageTwo.json)
        expect(finalPage.events.length).toBeGreaterThan(0)
        expect(finalPage.next).toBeNull()
        expect(finalPage.events.some((event) => event.id === lateID)).toBeFalse()
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "accepts production-scale cursor maps without an expression tree",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const response = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
          body: JSON.stringify(
            Object.fromEntries(Array.from({ length: 1_500 }, (_, index) => [`ses_${index}`, index])),
          ),
        })
        expect(response.status).toBe(200)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "validates seq values",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const cases = [
          {
            path: SyncPaths.history,
            body: { ses_1: -1 },
          },
          {
            path: SyncPaths.history,
            body: { ses_1: 1.5 },
          },
          {
            path: SyncPaths.historyPage,
            body: { state: { ses_1: -1 } },
          },
          {
            path: SyncPaths.historyPage,
            body: { state: {}, cursor: "not-a-cursor" },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: -1, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 1.5, type: "session.created", data: {} }],
            },
          },
        ]

        for (const item of cases) {
          const response = yield* requestInDirectory(item.path, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify(item.body),
          })
          expect(response.status).toBe(400)
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance.skip(
    "returns structured validation errors",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const response = yield* Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${SyncPaths.history}`, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ aggregate: -1 }),
            }),
            context,
          ),
        )

        expect(response.status).toBe(400)
        expect(response.headers.get("content-type") ?? "").toContain("application/json")
        const body = (yield* Effect.promise(() => response.json())) as Record<string, unknown>
        expect(body.success).toBe(false)
        expect(Array.isArray(body.error) || Array.isArray(body.errors)).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
