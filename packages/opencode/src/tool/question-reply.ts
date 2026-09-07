import { Effect, Schema } from "effect"
import { and, eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { SessionInteractionTable } from "@opencode-ai/core/session/sql"
import * as Tool from "./tool"
import { Question } from "../question"
import { QuestionID } from "../question/schema"
import { Session } from "@/session/session"
import DESCRIPTION from "./question-reply.txt"

export const Parameters = Schema.Struct({
  requestID: Schema.String.annotate({
    description: "Question request id, from the <question_request id=\"...\"> block",
  }),
  action: Schema.Literals(["answer", "reject"]).annotate({
    description: 'answer: supply answers. reject: dismiss the question so the child continues without one.',
  }),
  answers: Schema.optional(Schema.mutable(Schema.Array(Schema.mutable(Schema.Array(Schema.String))))).annotate({
    description:
      "Required when action is answer. One array of chosen labels per question, in the order the questions were listed.",
  }),
})

type Metadata = {
  requestID: string
  action: "answer" | "reject"
  outcome: "answered" | "rejected" | "already_resolved"
}

const decodeRequest = Schema.decodeUnknownOption(Question.Request)

export const QuestionReplyTool = Tool.define<
  typeof Parameters,
  Metadata,
  Question.Service | Session.Service | Database.Service
>(
  "question_reply",
  Effect.gen(function* () {
    const question = yield* Question.Service
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service

    const row = (requestID: string) =>
      db
        .select({ state: SessionInteractionTable.state, request: SessionInteractionTable.request_json })
        .from(SessionInteractionTable)
        .where(and(eq(SessionInteractionTable.id, requestID), eq(SessionInteractionTable.kind, "question")))
        .get()
        .pipe(Effect.orDie)

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context<Metadata>) =>
        Effect.gen(function* () {
          const requestID = params.requestID
          if (params.action === "answer" && !params.answers)
            return yield* Effect.die(new Error("`answers` is required when action is \"answer\"."))

          const existing = yield* row(requestID)
          if (!existing) return yield* Effect.die(new Error(`No question request ${requestID} exists.`))
          const decoded = decodeRequest(existing.request)
          if (decoded._tag === "None")
            return yield* Effect.die(new Error(`Question request ${requestID} could not be read.`))
          const request = decoded.value

          // Authorization is the whole point of routing replies through a tool:
          // only the session that structurally owns the asking session may
          // answer for it. `session.parentID` is the only thing consulted --
          // swarm metadata is descriptive and must never confer authority.
          const child = yield* sessions.get(request.sessionID).pipe(Effect.option)
          if (child._tag === "None")
            return yield* Effect.die(new Error(`Question request ${requestID} has no reachable session.`))
          if (child.value.parentID !== ctx.sessionID)
            return yield* Effect.die(
              new Error(
                `Question request ${requestID} belongs to session ${request.sessionID}, which is not a child of this session. Only the asking session's parent can respond to it.`,
              ),
            )

          const resolved = (state: string): Tool.ExecuteResult<Metadata> => ({
            title: `Question ${requestID} already ${state}`,
            output: `Question ${requestID} was already ${state} by someone else, so this call changed nothing. The child session has already been resumed.`,
            metadata: { requestID, action: params.action, outcome: "already_resolved" as const },
          })

          if (existing.state !== "pending") return resolved(existing.state)

          const target = params.action === "answer" ? "replied" : "rejected"
          if (params.action === "answer")
            yield* question
              .reply({ requestID: QuestionID.make(requestID), answers: params.answers ?? [] })
              .pipe(Effect.catchTag("Question.NotFoundError", (error) => Effect.die(new Error(error.message))))
          else
            yield* question
              .reject(QuestionID.make(requestID))
              .pipe(Effect.catchTag("Question.NotFoundError", (error) => Effect.die(new Error(error.message))))

          // `reply`/`reject` return void whether they won or lost the race with
          // a concurrent responder, so the durable state is what decides which
          // of the two happened. A lost race is not a success.
          const after = yield* row(requestID)
          if (after?.state !== target) return resolved(after?.state ?? "resolved")

          return params.action === "answer"
            ? {
                title: `Answered question ${requestID}`,
                output: `Answered question ${requestID} for session ${request.sessionID}. The child session has been resumed with those answers.`,
                metadata: { requestID, action: "answer" as const, outcome: "answered" as const },
              }
            : {
                title: `Dismissed question ${requestID}`,
                output: `Dismissed question ${requestID} for session ${request.sessionID}. The child session has been resumed without an answer.`,
                metadata: { requestID, action: "reject" as const, outcome: "rejected" as const },
              }
        }),
    }
  }),
)
