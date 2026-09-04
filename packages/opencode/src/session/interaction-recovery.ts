import { EventV2Bridge } from "@/event-v2-bridge"
import { PermissionID } from "@/permission/schema"
import { QuestionID } from "@/question/schema"
import { MessageID, SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { PartTable, SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { and, eq } from "drizzle-orm"
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

export const recoverWith = Effect.fn("SessionInteractionRecovery.recoverWith")(function* (input: {
  database: Database.Interface
  events: EventV2.Interface
  sessionID?: SessionID
}) {
  const { db } = input.database
  const { events, sessionID } = input
  const now = Date.now()
  const committed = yield* events.barrier(
    db
      .transaction(
        (transaction) =>
          Effect.gen(function* () {
            const rows = yield* transaction
              .select()
              .from(SessionInteractionTable)
              .where(
                sessionID
                  ? and(eq(SessionInteractionTable.state, "pending"), eq(SessionInteractionTable.session_id, sessionID))
                  : eq(SessionInteractionTable.state, "pending"),
              )
              .all()
            const executions = new Map(
              (yield* transaction.select().from(SessionExecutionTable).all()).map((execution) => [
                execution.session_id,
                execution,
              ]),
            )
            const result: EventV2.Payload[] = []
            for (const row of rows) {
              const request = record(row.request_json) ? row.request_json : undefined
              const generation = typeof request?.executionGeneration === "number" ? request.executionGeneration : undefined
              const execution = executions.get(row.session_id)
              const cancelled =
                generation !== undefined && execution?.generation === generation && !!execution.cancel_requested_at
              const tool = Option.getOrUndefined(decodeTool(request?.tool))
              const terminalTool =
                !!tool &&
                !!(yield* transaction
                  .select({ data: PartTable.data })
                  .from(PartTable)
                  .where(and(eq(PartTable.session_id, row.session_id), eq(PartTable.message_id, tool.messageID)))
                  .all()).some((part) => {
                  const decoded = decodeToolPart(part.data)
                  return decoded._tag === "Some" && decoded.value.callID === tool.callID
                })
              if (!terminalTool && !cancelled) continue
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
