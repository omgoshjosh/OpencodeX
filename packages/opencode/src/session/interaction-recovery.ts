import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { PermissionID } from "@/permission/schema"
import { Question } from "@/question"
import { QuestionID } from "@/question/schema"
import { SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { PartTable, SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { and, eq } from "drizzle-orm"
import { Effect } from "effect"

export const recover = Effect.fn("SessionInteractionRecovery.recover")(function* (sessionID?: SessionID) {
  const { db } = yield* Database.Service
  const events = yield* EventV2Bridge.Service
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
              const request = row.request_json
              const generation = typeof request.executionGeneration === "number" ? request.executionGeneration : undefined
              const execution = executions.get(row.session_id)
              const live =
                execution?.state === "running" &&
                !!execution.owner_id &&
                !!execution.lease_expires_at &&
                execution.lease_expires_at > now &&
                execution.generation === generation
              const tool = request.tool
              const terminalTool =
                typeof tool === "object" &&
                tool !== null &&
                typeof tool.messageID === "string" &&
                typeof tool.callID === "string" &&
                !!(yield* transaction
                  .select({ data: PartTable.data })
                  .from(PartTable)
                  .where(and(eq(PartTable.session_id, row.session_id), eq(PartTable.message_id, tool.messageID)))
                  .all()).find(
                  (part) =>
                    part.data.type === "tool" &&
                    part.data.callID === tool.callID &&
                    (part.data.state.status === "completed" || part.data.state.status === "error"),
                )
              if (!terminalTool && (generation === undefined || live)) continue
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
                  yield* events.commit(Question.Event.Rejected, {
                    sessionID: SessionID.make(row.session_id),
                    requestID: QuestionID.make(row.id),
                  }),
                )
                continue
              }
              result.push(
                yield* events.commit(Permission.Event.Replied, {
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

export * as SessionInteractionRecovery from "./interaction-recovery"
