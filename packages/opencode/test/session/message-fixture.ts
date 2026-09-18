import { SessionID } from "@/session/schema"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"

/**
 * Fully-typed `SessionLegacy` message and part fixtures for the model-fallback
 * and provider-exhaustion suites. The rows these build are real `WithParts`
 * values rather than narrowed literals, so a schema change that would break the
 * production path breaks these tests too.
 */

const SESSION_ID = SessionID.make("ses_fixture")
const DEFAULT_PROVIDER_ID = "openai"
const DEFAULT_MODEL_ID = "gpt-5.6-sol"

let partSequence = 0
function nextPartID() {
  partSequence += 1
  return SessionLegacy.PartID.make(`prt_fixture_${partSequence}`)
}

const EMPTY_TOKENS = {
  input: 0,
  output: 0,
  reasoning: 0,
  cache: { read: 0, write: 0 },
}

function partBase(messageID: string) {
  return {
    id: nextPartID(),
    sessionID: SESSION_ID,
    messageID: SessionLegacy.MessageID.make(messageID),
  }
}

export function userMessage(id = "msg_user"): SessionLegacy.WithParts {
  return {
    info: {
      id: SessionLegacy.MessageID.make(id),
      sessionID: SESSION_ID,
      role: "user",
      time: { created: 0 },
      agent: "test",
      model: {
        providerID: ProviderV2.ID.make(DEFAULT_PROVIDER_ID),
        modelID: ProviderV2.ModelID.make(DEFAULT_MODEL_ID),
      },
    },
    parts: [],
  }
}

export function assistantMessage(input: {
  parts?: SessionLegacy.Part[]
  error?: SessionLegacy.Assistant["error"]
  id?: string
  parentID?: string
  providerID?: string
  modelID?: string
}): SessionLegacy.WithParts {
  return {
    info: {
      id: SessionLegacy.MessageID.make(input.id ?? "msg_assistant"),
      sessionID: SESSION_ID,
      role: "assistant",
      time: { created: 0 },
      error: input.error,
      parentID: SessionLegacy.MessageID.make(input.parentID ?? "msg_user"),
      providerID: ProviderV2.ID.make(input.providerID ?? DEFAULT_PROVIDER_ID),
      modelID: ProviderV2.ModelID.make(input.modelID ?? DEFAULT_MODEL_ID),
      mode: "build",
      agent: "test",
      path: { cwd: "/tmp", root: "/tmp" },
      cost: 0,
      tokens: EMPTY_TOKENS,
    },
    parts: input.parts ?? [],
  }
}

export function textPart(
  text: string,
  options: { synthetic?: boolean; messageID?: string } = {},
): SessionLegacy.TextPart {
  return {
    ...partBase(options.messageID ?? "msg_assistant"),
    type: "text",
    text,
    ...(options.synthetic === undefined ? {} : { synthetic: options.synthetic }),
  }
}

export function reasoningPart(text: string, options: { messageID?: string } = {}): SessionLegacy.ReasoningPart {
  return {
    ...partBase(options.messageID ?? "msg_assistant"),
    type: "reasoning",
    text,
    time: { start: 0 },
  }
}

export function filePart(input: { mime: string; url: string; messageID?: string }): SessionLegacy.FilePart {
  return {
    ...partBase(input.messageID ?? "msg_assistant"),
    type: "file",
    mime: input.mime,
    url: input.url,
  }
}

export function toolPart(
  state: SessionLegacy.ToolState = { status: "pending", input: {}, raw: "" },
  options: { messageID?: string } = {},
): SessionLegacy.ToolPart {
  return {
    ...partBase(options.messageID ?? "msg_assistant"),
    type: "tool",
    callID: "call_fixture",
    tool: "test",
    state,
  }
}

export function completedToolState(): SessionLegacy.ToolStateCompleted {
  return {
    status: "completed",
    input: {},
    output: "",
    title: "test",
    metadata: {},
    time: { start: 0, end: 0 },
  }
}

export function stepStartPart(options: { messageID?: string } = {}): SessionLegacy.StepStartPart {
  return {
    ...partBase(options.messageID ?? "msg_assistant"),
    type: "step-start",
  }
}

export function stepFinishPart(options: { messageID?: string } = {}): SessionLegacy.StepFinishPart {
  return {
    ...partBase(options.messageID ?? "msg_assistant"),
    type: "step-finish",
    reason: "stop",
    cost: 0,
    tokens: EMPTY_TOKENS,
  }
}
