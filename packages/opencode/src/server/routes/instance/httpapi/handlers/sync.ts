import { Workspace } from "@/control-plane/workspace"
import * as InstanceState from "@/effect/instance-state"
import { Session } from "@/session/session"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { NonNegativeInt } from "@opencode-ai/core/schema"
import { EventV2Bridge } from "@/event-v2-bridge"
import {
  EventCursorLeaseTable,
  EventCursorTable,
  EventSequenceTable,
  EventTable,
} from "@opencode-ai/core/event/sql"
import { asc, getTableColumns, gt, lte, max } from "drizzle-orm"
import { and } from "drizzle-orm"
import { eq } from "drizzle-orm"
import { Effect, Option, Schema, Scope } from "effect"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import { HistoryPagePayload, HistoryPayload, ReplayPayload, SessionPayload } from "../groups/sync"
import { SessionTable } from "@opencode-ai/core/session/sql"
import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "server.sync" })
const HISTORY_LEASE_MS = 5 * 60_000
const HistoryCursor = Schema.Struct({ after: NonNegativeInt, fence: NonNegativeInt, token: Schema.String })
const decodeHistoryCursor = Schema.decodeUnknownOption(Schema.fromJsonString(HistoryCursor))

function historyCursor(value: string | undefined) {
  if (!value) return undefined
  return Option.getOrUndefined(decodeHistoryCursor(Buffer.from(value, "base64url").toString()))
}

function encodeHistoryCursor(value: typeof HistoryCursor.Type) {
  return Buffer.from(JSON.stringify(value)).toString("base64url")
}

export const syncHandlers = HttpApiBuilder.group(InstanceHttpApi, "sync", (handlers) =>
  Effect.gen(function* () {
    const workspace = yield* Workspace.Service
    const session = yield* Session.Service
    const scope = yield* Scope.Scope
    const events = yield* EventV2Bridge.Service
    const { db } = yield* Database.Service

    const start = Effect.fn("SyncHttpApi.start")(function* () {
      yield* workspace
        .startWorkspaceSyncing((yield* InstanceState.context).project.id)
        .pipe(Effect.ignore, Effect.forkIn(scope))
      return true
    })

    const replay = Effect.fn("SyncHttpApi.replay")(function* (ctx: { payload: typeof ReplayPayload.Type }) {
      const payload: EventV2.SerializedEvent[] = ctx.payload.events.map((event) => ({
        id: EventV2.ID.make(event.id),
        aggregateID: event.aggregateID,
        seq: event.seq,
        type: event.type,
        data: { ...event.data },
      }))
      const source = payload[0].aggregateID
      log.info("sync replay requested", {
        sessionID: source,
        events: payload.length,
        first: payload[0]?.seq,
        last: payload.at(-1)?.seq,
        directory: ctx.payload.directory,
      })
      yield* events.replayAll(payload)
      log.info("sync replay complete", {
        sessionID: source,
        events: payload.length,
        first: payload[0]?.seq,
        last: payload.at(-1)?.seq,
      })
      return { sessionID: source }
    })

    const steal = Effect.fn("SyncHttpApi.steal")(function* (ctx: { payload: typeof SessionPayload.Type }) {
      const workspaceID = yield* InstanceState.workspaceID
      if (!workspaceID) return yield* new HttpApiError.BadRequest({})

      yield* session.setWorkspace({ sessionID: ctx.payload.sessionID, workspaceID })

      log.info("sync session stolen", {
        sessionID: ctx.payload.sessionID,
        workspaceID,
      })

      return { sessionID: ctx.payload.sessionID }
    })

    const history = Effect.fn("SyncHttpApi.history")(function* (ctx: {
      payload: typeof HistoryPayload.Type
      query: { directory?: string }
    }) {
      const cursors = new Map(Object.entries(ctx.payload))

      // A hub can host many projects in one database. When the caller sends
      // the optional `directory` query parameter, scope the journal to
      // sessions that belong to that directory so unrelated projects sharing
      // the same hub never cross-contaminate each other's mirrors. Without
      // it, the full journal is returned, keeping the upstream contract.
      const started = Date.now()
      const aggregates =
        ctx.query.directory !== undefined
          ? yield* db
              .select({ id: SessionTable.id, seq: EventSequenceTable.seq })
              .from(SessionTable)
              .innerJoin(EventSequenceTable, eq(EventSequenceTable.aggregate_id, SessionTable.id))
              .where(eq(SessionTable.directory, ctx.query.directory))
              .orderBy(asc(SessionTable.id))
              .all()
              .pipe(Effect.orDie)
          : yield* db
              .select({ id: EventSequenceTable.aggregate_id, seq: EventSequenceTable.seq })
              .from(EventSequenceTable)
              .orderBy(asc(EventSequenceTable.aggregate_id))
              .all()
              .pipe(Effect.orDie)
      const rows: (typeof EventTable.$inferSelect)[] = []
      let scanned = 0
      for (const aggregate of aggregates) {
        const cursor = cursors.get(aggregate.id)
        if (cursor !== undefined && cursor >= aggregate.seq) continue
        const page = yield* db
          .select()
          .from(EventTable)
          .where(
            cursor === undefined
              ? eq(EventTable.aggregate_id, aggregate.id)
              : and(eq(EventTable.aggregate_id, aggregate.id), gt(EventTable.seq, cursor)),
          )
          .orderBy(asc(EventTable.seq))
          .limit(Workspace.HISTORY_PAGE_SIZE - rows.length)
          .all()
          .pipe(Effect.orDie)
        scanned++
        rows.push(...page)
        if (rows.length === Workspace.HISTORY_PAGE_SIZE) break
      }
      log.info("sync history page served", {
        directory: ctx.query.directory,
        cursors: cursors.size,
        aggregates: aggregates.length,
        scanned,
        events: rows.length,
        durationMs: Date.now() - started,
      })
      return rows
    })

    const historyPage = Effect.fn("SyncHttpApi.historyPage")(function* (ctx: {
      payload: typeof HistoryPagePayload.Type
      query: { directory?: string }
    }) {
      const cursor = historyCursor(ctx.payload.cursor)
      if (ctx.payload.cursor && !cursor) return yield* new HttpApiError.BadRequest({})
      const lease = yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const expiresAt = Date.now() + HISTORY_LEASE_MS
              if (cursor) {
                const current = yield* transaction
                  .select()
                  .from(EventCursorLeaseTable)
                  .where(eq(EventCursorLeaseTable.token, cursor.token))
                  .get()
                if (!current || current.fence !== cursor.fence || current.expires_at <= Date.now()) return undefined
                yield* transaction
                  .update(EventCursorLeaseTable)
                  .set({ expires_at: expiresAt })
                  .where(eq(EventCursorLeaseTable.token, cursor.token))
                  .run()
                return { token: cursor.token, fence: cursor.fence }
              }
              const fence =
                (yield* transaction
                  .select({ value: max(EventCursorTable.position) })
                  .from(EventCursorTable)
                  .get())?.value ?? 0
              const token = crypto.randomUUID()
              yield* transaction
                .insert(EventCursorLeaseTable)
                .values({ token, fence, expires_at: expiresAt })
                .run()
              return { token, fence }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      if (!lease) return yield* new HttpApiError.BadRequest({})
      const fence = lease.fence
      const after = cursor?.after ?? 0
      if (after > fence) return yield* new HttpApiError.BadRequest({})
      const columns = { ...getTableColumns(EventTable), position: EventCursorTable.position }
      const raw =
        ctx.query.directory !== undefined
          ? yield* db
              .select(columns)
              .from(EventCursorTable)
              .innerJoin(EventTable, eq(EventTable.id, EventCursorTable.event_id))
              .innerJoin(SessionTable, eq(SessionTable.id, EventTable.aggregate_id))
              .where(
                and(
                  gt(EventCursorTable.position, after),
                  lte(EventCursorTable.position, fence),
                  eq(SessionTable.directory, ctx.query.directory),
                ),
              )
              .orderBy(asc(EventCursorTable.position))
              .limit(Workspace.HISTORY_PAGE_SIZE)
              .all()
              .pipe(Effect.orDie)
          : yield* db
              .select(columns)
              .from(EventCursorTable)
              .innerJoin(EventTable, eq(EventTable.id, EventCursorTable.event_id))
              .where(and(gt(EventCursorTable.position, after), lte(EventCursorTable.position, fence)))
              .orderBy(asc(EventCursorTable.position))
              .limit(Workspace.HISTORY_PAGE_SIZE)
              .all()
              .pipe(Effect.orDie)
      const events = raw
        .filter((event) => event.seq > (ctx.payload.state[event.aggregate_id] ?? -1))
        .map(({ position: _, ...event }) => event)
      const last = raw.at(-1)?.position
      const next =
        raw.length === Workspace.HISTORY_PAGE_SIZE && last !== undefined && last < fence
          ? encodeHistoryCursor({ after: last, fence, token: lease.token })
          : undefined
      if (!next)
        yield* db
          .delete(EventCursorLeaseTable)
          .where(eq(EventCursorLeaseTable.token, lease.token))
          .run()
          .pipe(Effect.orDie)
      log.info("sync history keyset page served", {
        directory: ctx.query.directory,
        after,
        fence,
        scanned: raw.length,
        events: events.length,
        next: !!next,
      })
      return { events, next: next ?? null }
    })

    return handlers
      .handle("start", start)
      .handle("replay", replay)
      .handle("steal", steal)
      .handle("history", history)
      .handle("historyPage", historyPage)
  }),
)
