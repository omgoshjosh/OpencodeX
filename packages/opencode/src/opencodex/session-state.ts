import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { OpencodeXSessionStateTable } from "@opencode-ai/core/opencodex/sql"
import { NonNegativeInt } from "@opencode-ai/core/schema"
import { Context, Effect, Layer, Schema } from "effect"
import { eq, inArray } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { OpencodeXProject } from "./project"
import { OpencodeXTerminalSession } from "./terminal-session"
import { OpencodeXView } from "./view"

export const Info = Schema.Struct({
  sessionID: SessionID,
  seenAt: Schema.optional(NonNegativeInt),
  reviewedAt: Schema.optional(NonNegativeInt),
  // Server time at which a reader explicitly sent the session back to the
  // unread queue. Absent means "not explicitly marked"; readers still treat
  // activity newer than seenAt as unread.
  markedUnreadAt: Schema.optional(NonNegativeInt),
  reviewedFiles: Schema.Array(Schema.String),
  timeUpdated: NonNegativeInt,
}).annotate({ identifier: "OpencodeXSessionState" })
export type Info = Schema.Schema.Type<typeof Info>

export const UpdateInput = Schema.Struct({
  sessionID: SessionID,
  expectedReviewedFiles: Schema.optional(Schema.Array(Schema.String)),
  // The `timeUpdated` revision the caller last observed. When it is behind the
  // persisted revision the caller's mark-unread intent is dropped, so a stale
  // client can neither clear a mark it never saw nor resurrect a cleared one.
  expectedRevision: Schema.optional(NonNegativeInt),
  seenAt: Schema.optional(NonNegativeInt),
  reviewedAt: Schema.optional(NonNegativeInt),
  markedUnread: Schema.optional(Schema.Boolean),
  reviewedFiles: Schema.optional(Schema.Array(Schema.String)),
}).annotate({ identifier: "OpencodeXSessionStateUpdateInput" })
export type UpdateInput = Schema.Schema.Type<typeof UpdateInput>

export const DisplayStatus = Schema.Literals(["idle", "in_progress", "input_needed", "needs_review"]).annotate({
  identifier: "OpencodeXSessionDisplayStatus",
})
export type DisplayStatus = Schema.Schema.Type<typeof DisplayStatus>

export const UiState = Schema.Struct({
  sessionID: SessionID,
  seenAt: Schema.optional(NonNegativeInt),
  reviewedAt: Schema.optional(NonNegativeInt),
  markedUnreadAt: Schema.optional(NonNegativeInt),
  // The persisted revision this state was derived from, echoed back by clients
  // as `expectedRevision` so the server can reject stale mark-unread writes.
  revision: NonNegativeInt,
  reviewedFiles: Schema.Array(Schema.String),
  displayStatus: DisplayStatus,
  updated: Schema.Boolean,
}).annotate({ identifier: "OpencodeXSessionUiState" })
export type UiState = Schema.Schema.Type<typeof UiState>

export const SyncSnapshot = Schema.Struct({
  projects: Schema.Array(OpencodeXProject.Info),
  sessions: Schema.Array(Session.Info),
  terminalSessions: Schema.Array(OpencodeXTerminalSession.Info),
  views: Schema.Array(OpencodeXView.Info),
  sessionStatus: Schema.Record(Schema.String, SessionStatus.Info),
  permissions: Schema.Array(Permission.Request),
  questions: Schema.Array(Question.Request),
  sessionUiState: Schema.Record(Schema.String, UiState),
}).annotate({ identifier: "OpencodeXSessionSyncSnapshot" })
export type SyncSnapshot = Schema.Schema.Type<typeof SyncSnapshot>

export const SyncResponse = Schema.Union([
  Schema.Struct({
    changed: Schema.Literal(false),
    revision: Schema.String,
  }),
  Schema.Struct({
    changed: Schema.Literal(true),
    revision: Schema.String,
    snapshot: SyncSnapshot,
  }),
]).annotate({ identifier: "OpencodeXSessionSyncResponse" })
export type SyncResponse = Schema.Schema.Type<typeof SyncResponse>

export const Event = {
  Updated: EventV2.define({
    type: "opencodex.session_state.updated",
    sync: {
      aggregate: "sessionID",
      version: 1,
    },
    schema: {
      sessionID: SessionID,
      state: Info,
    },
  }),
}

export interface Interface {
  readonly get: (sessionID: SessionID) => Effect.Effect<Info | undefined>
  readonly list: (sessionIDs: readonly SessionID[]) => Effect.Effect<Record<string, Info>>
  readonly update: (input: UpdateInput) => Effect.Effect<Info, ConflictError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/OpencodeXSessionState") {}

export class ConflictError extends Schema.TaggedErrorClass<ConflictError>()("OpencodeX.SessionState.ConflictError", {
  sessionID: SessionID,
}) {}

function hydrate(row: typeof OpencodeXSessionStateTable.$inferSelect): Info {
  return {
    sessionID: row.session_id,
    ...(row.seen_at === null ? {} : { seenAt: row.seen_at }),
    ...(row.reviewed_at === null ? {} : { reviewedAt: row.reviewed_at }),
    ...(row.marked_unread_at === null ? {} : { markedUnreadAt: row.marked_unread_at }),
    reviewedFiles: row.reviewed_files,
    timeUpdated: row.time_updated,
  }
}

function maxOptional(a: number | undefined, b: number | undefined) {
  if (a === undefined) return b
  if (b === undefined) return a
  return Math.max(a, b)
}

function reviewedFiles(input: readonly string[] | undefined, current: Info | undefined) {
  if (input === undefined) return current?.reviewedFiles ?? []
  return [...new Set(input)]
}

/**
 * The unread mark is server-authoritative and last-write-wins on the persisted
 * revision. A caller whose `expectedRevision` is behind has not observed the
 * newest mark, so its intent is dropped entirely: it can neither clear a mark
 * it never saw nor resurrect one the server already cleared. That also makes a
 * retried mark-unread idempotent instead of shifting the mark forward in time.
 */
function markedUnreadAt(input: {
  current: Info | undefined
  update: UpdateInput
  revision: number
  seenAt: number | undefined
}) {
  const persisted = input.current?.markedUnreadAt
  if (input.update.expectedRevision !== undefined && (input.current?.timeUpdated ?? 0) > input.update.expectedRevision) {
    return persisted
  }
  if (input.update.markedUnread !== undefined) return input.update.markedUnread ? input.revision : undefined
  // Seeing the session clears a mark the reader has caught up with. An older
  // seen timestamp comes from a client that had not yet observed the mark, so
  // it must leave the newer mark standing.
  if (input.update.seenAt !== undefined && persisted !== undefined && (input.seenAt ?? 0) >= persisted) return undefined
  return persisted
}

export function deriveUiState(input: {
  session: Pick<Session.Info, "id" | "time"> & { parentID?: Session.Info["parentID"] }
  status?: SessionStatus.Info
  permissions: readonly Permission.Request[]
  questions: readonly Question.Request[]
  state?: Info
}): UiState {
  // A parent whose delegations run in the background reads idle itself but
  // is still working from the reader's point of view.
  const active =
    input.status?.type === "busy" ||
    input.status?.type === "retry" ||
    input.status?.type === "monitoring" ||
    (input.status?.background?.jobs.length ?? 0) > 0
  // Review is a root-session concept (mirroring the unseen-review query in
  // session-card): a delegated child's report is consumed by its parent, so
  // nothing ever marks the child reviewed and it would read "needs review"
  // forever. A finished child settles to idle instead.
  const reviewable = !input.session.parentID
  const displayStatus =
    input.permissions.length > 0 || input.questions.length > 0 || input.status?.type === "blocked"
      ? "input_needed"
      : active
        ? "in_progress"
        : reviewable && input.session.time.updated > (input.state?.reviewedAt ?? 0)
          ? "needs_review"
          : "idle"
  return {
    sessionID: input.session.id,
    ...(input.state?.seenAt === undefined ? {} : { seenAt: input.state.seenAt }),
    ...(input.state?.reviewedAt === undefined ? {} : { reviewedAt: input.state.reviewedAt }),
    ...(input.state?.markedUnreadAt === undefined ? {} : { markedUnreadAt: input.state.markedUnreadAt }),
    revision: input.state?.timeUpdated ?? 0,
    reviewedFiles: input.state?.reviewedFiles ?? [],
    displayStatus,
    // Unread is either explicit or implied by activity the reader has not seen.
    updated: input.state?.markedUnreadAt !== undefined || input.session.time.updated > (input.state?.seenAt ?? 0),
  }
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2Bridge.Service

    yield* events.project(Event.Updated, (event) => {
      const state = event.data.state
      return db
        .insert(OpencodeXSessionStateTable)
        .values([
          {
            session_id: state.sessionID,
            seen_at: state.seenAt,
            reviewed_at: state.reviewedAt,
            // Explicit null, not undefined: drizzle omits undefined columns, and
            // clearing the mark has to write the absence through.
            marked_unread_at: state.markedUnreadAt ?? null,
            reviewed_files: [...state.reviewedFiles],
            time_created: state.timeUpdated,
            time_updated: state.timeUpdated,
          },
        ])
        .onConflictDoUpdate({
          target: OpencodeXSessionStateTable.session_id,
          set: {
            seen_at: state.seenAt,
            reviewed_at: state.reviewedAt,
            marked_unread_at: state.markedUnreadAt ?? null,
            reviewed_files: [...state.reviewedFiles],
            time_updated: state.timeUpdated,
          },
        })
        .run()
        .pipe(Effect.orDie)
    })

    const get = Effect.fn("OpencodeXSessionState.get")(function* (sessionID: SessionID) {
      const row = yield* db
        .select()
        .from(OpencodeXSessionStateTable)
        .where(eq(OpencodeXSessionStateTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie)
      return row ? hydrate(row) : undefined
    })

    const list = Effect.fn("OpencodeXSessionState.list")(function* (sessionIDs: readonly SessionID[]) {
      if (sessionIDs.length === 0) return {}
      return Object.fromEntries(
        (yield* db
          .select()
          .from(OpencodeXSessionStateTable)
          .where(inArray(OpencodeXSessionStateTable.session_id, [...new Set(sessionIDs)]))
          .all()
          .pipe(Effect.orDie)).map((row) => [row.session_id, hydrate(row)]),
      )
    })

    const update = Effect.fn("OpencodeXSessionState.update")(function* (input: UpdateInput) {
      return yield* events.barrier(
        Effect.gen(function* () {
          const current = yield* get(input.sessionID)
          if (input.reviewedFiles !== undefined) {
            const expected = reviewedFiles(input.expectedReviewedFiles, undefined).toSorted()
            const persisted = (current?.reviewedFiles ?? []).toSorted()
            if (expected.length !== persisted.length || expected.some((file, index) => file !== persisted[index])) {
              return yield* new ConflictError({ sessionID: input.sessionID })
            }
          }
          const revision = Math.max(Date.now(), (current?.timeUpdated ?? 0) + 1)
          const seenAt = maxOptional(current?.seenAt, input.seenAt)
          const unreadAt = markedUnreadAt({ current, update: input, revision, seenAt })
          const state = {
            sessionID: input.sessionID,
            ...(seenAt === undefined ? {} : { seenAt }),
            ...(maxOptional(current?.reviewedAt, input.reviewedAt) === undefined
              ? {}
              : { reviewedAt: maxOptional(current?.reviewedAt, input.reviewedAt) }),
            ...(unreadAt === undefined ? {} : { markedUnreadAt: unreadAt }),
            reviewedFiles: reviewedFiles(input.reviewedFiles, current),
            timeUpdated: revision,
          }
          yield* events.publish(Event.Updated, { sessionID: input.sessionID, state })
          return state
        }),
      )
    })

    return Service.of({ get, list, update })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer), Layer.provide(EventV2Bridge.defaultLayer))

export * as OpencodeXSessionState from "./session-state"
