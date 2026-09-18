import { EventV2Bridge } from "@/event-v2-bridge"
import { PermissionID } from "@/permission/schema"
import { QuestionID } from "@/question/schema"
import { MessageID, SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { PartTable, SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { and, eq, inArray } from "drizzle-orm"
import { Effect, Option, Schema } from "effect"
import { SessionInteractionEvent } from "./interaction-event"

const Tool = Schema.Struct({ messageID: MessageID, callID: Schema.String })
const ToolPart = Schema.Struct({
  type: Schema.Literal("tool"),
  callID: Schema.String,
  state: Schema.Struct({ status: Schema.Literals(["completed", "error"]) }),
})
const decodeTool = Schema.decodeUnknownOption(Tool)
const decodeToolPart = Schema.decodeUnknownOption(ToolPart)

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

type InteractionRow = typeof SessionInteractionTable.$inferSelect
type ExecutionRow = typeof SessionExecutionTable.$inferSelect
type Candidate = { interaction: InteractionRow; execution: ExecutionRow | undefined }
type Reader = Pick<Database.Interface["db"], "select">

/**
 * The orphaned-interaction scan, shaped so it runs the same way on the read
 * connection (no barrier, no lock) and again inside the immediate transaction
 * that rejects the survivors. `ids` restricts it to interactions a caller
 * already knows about, which also keeps the execution lookup indexed.
 */
const scanOrphaned = Effect.fnUntraced(function* (
  reader: Reader,
  scope: { sessionID?: SessionID; ids?: readonly string[] },
) {
  const pending = eq(SessionInteractionTable.state, "pending")
  const rows = yield* reader
    .select()
    .from(SessionInteractionTable)
    .where(
      scope.ids
        ? and(pending, inArray(SessionInteractionTable.id, scope.ids))
        : scope.sessionID
          ? and(pending, eq(SessionInteractionTable.session_id, scope.sessionID))
          : pending,
    )
    .all()
  if (rows.length === 0) return [] as Candidate[]
  const executionQuery = reader.select().from(SessionExecutionTable)
  const executions = new Map(
    (yield* (
      scope.ids
        ? executionQuery.where(
            inArray(
              SessionExecutionTable.session_id,
              rows.map((row) => row.session_id),
            ),
          )
        : executionQuery
    ).all()).map((execution) => [execution.session_id, execution]),
  )
  const result: Candidate[] = []
  for (const row of rows) {
    const request = record(row.request_json) ? row.request_json : undefined
    const generation = typeof request?.executionGeneration === "number" ? request.executionGeneration : undefined
    const execution = executions.get(row.session_id)
    const cancelled =
      generation !== undefined && execution?.generation === generation && !!execution.cancel_requested_at
    const tool = Option.getOrUndefined(decodeTool(request?.tool))
    const terminalTool =
      !!tool &&
      (yield* reader
        .select({ data: PartTable.data })
        .from(PartTable)
        .where(and(eq(PartTable.session_id, row.session_id), eq(PartTable.message_id, tool.messageID)))
        .all()).some((part) => {
        const decoded = decodeToolPart(part.data)
        return decoded._tag === "Some" && decoded.value.callID === tool.callID
      })
    if (terminalTool || cancelled) result.push({ interaction: row, execution })
  }
  return result
})

/**
 * Compare-and-set between the two phases: the row inside the write transaction
 * must still be the row the unlocked scan judged orphaned. A renewal in between
 * - the interaction re-issued (fresh `time_updated`), a new generation claiming
 * the session, a cancel withdrawn - changes one of these fields.
 */
function unchanged(current: Candidate, observed: Candidate | undefined) {
  if (!observed || current.interaction.time_updated !== observed.interaction.time_updated) return false
  if (!current.execution || !observed.execution) return current.execution === observed.execution
  return (
    current.execution.generation === observed.execution.generation &&
    current.execution.cancel_requested_at === observed.execution.cancel_requested_at &&
    current.execution.time_updated === observed.execution.time_updated
  )
}

/**
 * Two phases (OpencodeX-fs2 PR3), the shape `SessionStatus.recover` took in
 * PR2: the scan - pending interactions, every execution, the tool parts behind
 * each candidate - runs on the read connection with no barrier, so on a large
 * database it neither queues the writer nor holds the process-wide permit.
 * Only the candidates it finds go through the barrier and an immediate
 * transaction, where the same scan runs again against the writer and each row
 * is compared with what the first phase saw. Under the kill switch
 * (`read === db`) both phases run under the barrier exactly as before.
 */
export const recoverWith = Effect.fn("SessionInteractionRecovery.recoverWith")(function* (input: {
  database: Pick<Database.Interface, "db" | "read">
  events: EventV2.Interface
  sessionID?: SessionID
  /** Runs between the unlocked scan and the write transaction. Tests only. */
  beforeWrite?: Effect.Effect<void>
}) {
  const { db, read } = input.database
  const { events, sessionID } = input
  const now = Date.now()
  const guarded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    read === db ? events.barrier(effect, "SessionInteractionRecovery.recoverWith") : effect
  const committed = yield* guarded(
    Effect.gen(function* () {
      const observed = yield* scanOrphaned(read, { sessionID }).pipe(Effect.orDie)
      if (observed.length === 0) return [] as EventV2.Payload[]
      yield* input.beforeWrite ?? Effect.void
      return yield* events.barrier(
        db
          .transaction(
            (transaction) =>
              Effect.gen(function* () {
                const current = yield* scanOrphaned(transaction, {
                  ids: observed.map((candidate) => candidate.interaction.id),
                })
                const orphaned = current.filter((candidate) =>
                  unchanged(
                    candidate,
                    observed.find((item) => item.interaction.id === candidate.interaction.id),
                  ),
                )
                const result: EventV2.Payload[] = []
                for (const { interaction: row } of orphaned) {
                  const updated = yield* transaction
                    .update(SessionInteractionTable)
                    .set({
                      state: "rejected",
                      ...(row.kind === "permission" ? { response_json: { reply: "reject" } } : {}),
                      responded_at: now,
                      time_updated: now,
                    })
                    .where(and(eq(SessionInteractionTable.id, row.id), eq(SessionInteractionTable.state, "pending")))
                    .returning({ id: SessionInteractionTable.id })
                    .get()
                  if (!updated) continue
                  if (row.kind === "question") {
                    result.push(
                      yield* events.commit(SessionInteractionEvent.QuestionRejected, {
                        sessionID: SessionID.make(row.session_id),
                        requestID: QuestionID.make(row.id),
                      }),
                    )
                    continue
                  }
                  result.push(
                    yield* events.commit(SessionInteractionEvent.PermissionReplied, {
                      sessionID: SessionID.make(row.session_id),
                      requestID: PermissionID.make(row.id),
                      reply: "reject",
                    }),
                  )
                }
                return result
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.orDie),
      )
    }),
  )
  yield* Effect.forEach(committed, events.broadcast, { discard: true })
})

export const recover = Effect.fn("SessionInteractionRecovery.recover")(function* (sessionID?: SessionID) {
  yield* recoverWith({
    database: yield* Database.Service,
    events: yield* EventV2Bridge.Service,
    sessionID,
  })
})

export * as SessionInteractionRecovery from "./interaction-recovery"
