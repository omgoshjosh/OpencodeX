import { PermissionID } from "@/permission/schema"
import { QuestionID } from "@/question/schema"
import { SessionID } from "@/session/schema"
import { EventV2 } from "@opencode-ai/core/event"
import { Schema } from "effect"

export const QuestionRejected = EventV2.define({
  type: "question.rejected",
  schema: { sessionID: SessionID, requestID: QuestionID },
})

export const PermissionReplied = EventV2.define({
  type: "permission.replied",
  schema: { sessionID: SessionID, requestID: PermissionID, reply: Schema.Literals(["once", "always", "reject"]) },
})

export * as SessionInteractionEvent from "./interaction-event"
