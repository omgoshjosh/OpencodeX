import { Schema } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionID, MessageID, PartID } from "./schema"

/**
 * The wire schemas for the prompt entry points. They live here rather than in
 * `prompt.ts` so the schemas can be read without the 30-service layer around
 * them; `prompt.ts` re-exports every one of them, so no import path changes.
 */

export const ModelRef = Schema.Struct({
  providerID: ProviderV2.ID,
  modelID: ProviderV2.ModelID,
})

/**
 * A `reportTo` contract that cannot be honored (OpencodeX-k30). Raised before
 * the prompt is accepted, so a refused contract accepts nothing.
 */
export class ReportToRefusedError extends Schema.TaggedErrorClass<ReportToRefusedError>()(
  "SessionReportToRefusedError",
  {
    sessionID: SessionID,
    reportTo: SessionID,
    reason: Schema.Literals(["self", "no-reply", "owner-missing", "owner-retired", "outstanding"]),
  },
) {}

export const PromptInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
  delivery: Schema.optional(SessionSchema.Delivery),
  model: Schema.optional(ModelRef),
  agent: Schema.optional(Schema.String),
  noReply: Schema.optional(Schema.Boolean),
  reportTo: Schema.optional(SessionID).annotate({
    description:
      "Session that owns this prompt's result. When this prompt's turn settles, its final answer is delivered to that session as a tagged report with a durable consumption receipt. Refused (nothing accepted) when the owner is missing or retired, or this session still owes a report.",
  }),
  tools: Schema.optional(Schema.Record(Schema.String, Schema.Boolean)).annotate({
    description:
      "@deprecated tools and permissions have been merged, you can set permissions on the session itself now",
  }),
  format: Schema.optional(SessionLegacy.Format),
  system: Schema.optional(Schema.String),
  variant: Schema.optional(Schema.String),
  parts: Schema.Array(
    Schema.Union([
      SessionLegacy.TextPartInput,
      SessionLegacy.FilePartInput,
      SessionLegacy.AgentPartInput,
      SessionLegacy.SubtaskPartInput,
    ]).annotate({ discriminator: "type" }),
  ),
})
export type PromptInput = Schema.Schema.Type<typeof PromptInput>

export class LoopInput extends Schema.Class<LoopInput>("SessionPrompt.LoopInput")({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
  commandID: Schema.optional(Schema.String),
  claimGeneration: Schema.optional(Schema.Number),
  claimOwner: Schema.optional(Schema.String),
}) {}

export const ShellInput = Schema.Struct({
  sessionID: SessionID,
  messageID: Schema.optional(MessageID),
  delivery: Schema.optional(SessionSchema.Delivery),
  agent: Schema.String,
  model: Schema.optional(ModelRef),
  command: Schema.String,
})
export type ShellInput = Schema.Schema.Type<typeof ShellInput>

export const CommandInput = Schema.Struct({
  messageID: Schema.optional(MessageID),
  sessionID: SessionID,
  delivery: Schema.optional(SessionSchema.Delivery),
  agent: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
  arguments: Schema.String,
  command: Schema.String,
  variant: Schema.optional(Schema.String),
  // Inlined (no identifier annotation) to keep the original SDK output — the
  // PromptInput call site above references FilePartInput by ref via the
  // Schema export in message-v2.ts.
  parts: Schema.optional(
    Schema.Array(
      Schema.Union([
        Schema.Struct({
          id: Schema.optional(PartID),
          type: Schema.Literal("file"),
          mime: Schema.String,
          filename: Schema.optional(Schema.String),
          url: Schema.String,
          source: Schema.optional(SessionLegacy.FilePartSource),
        }),
      ]).annotate({ discriminator: "type" }),
    ),
  ),
})
export type CommandInput = Schema.Schema.Type<typeof CommandInput>
