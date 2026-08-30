import { describe, expect } from "bun:test"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Effect, Layer, Option } from "effect"
import { Agent } from "@/agent/agent"
import { OpencodeXClaudeDriver } from "@/opencodex/claude-driver"
import type { ClaudeMapper } from "@/opencodex/claude-mapper"
import type { ClaudeImage, ClaudePrompt, ClaudeTransport, TransportOptions } from "@/opencodex/claude-transport"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { Session } from "@/session/session"
import { Todo } from "@/session/todo"
import { testEffect } from "../lib/effect"

const sessionID = "ses_claude_delivery"
const parentMessageID = "msg_user"
let script: () => AsyncIterable<ClaudeMapper.ClaudeEvent> = async function* () {}
let interrupts = 0
let interruptSettles = true
let message: SessionLegacy.Info | undefined
let parts: SessionLegacy.Part[] = []
let prompt: ClaudePrompt | undefined
let transportOptions: TransportOptions | undefined
let metadata: Record<string, unknown> = {}
let history: SessionLegacy.WithParts[] = []
let persistedMetadata: Record<string, unknown> | undefined
let removedMessages: string[] = []
let offeredPrompts: ClaudePrompt[] = []
let queuedMessages: SessionLegacy.WithParts[] = []

const transport: ClaudeTransport = {
  run(nextPrompt, nextOptions) {
    prompt = nextPrompt
    transportOptions = nextOptions
    return {
      events: script(),
      interrupt: async () => {
        interrupts += 1
        if (!interruptSettles) await new Promise(() => undefined)
      },
      offer: (next) => {
        offeredPrompts.push(next)
        return true
      },
    }
  },
}

const sessions = Layer.mock(Session.Service)({
  get: () =>
    Effect.succeed({
      id: sessionID,
      metadata,
      permission: [],
    } as unknown as Session.Info),
  messages: () => Effect.succeed(history),
  setMetadata: (input) =>
    Effect.sync(() => {
      persistedMetadata = input.metadata
    }),
  updateMessage: (next) =>
    Effect.sync(() => {
      message = next
      return next
    }),
  updatePart: (next) =>
    Effect.sync(() => {
      parts = [...parts.filter((part) => part.id !== next.id), next]
      return next
    }),
  getPart: ({ partID }) => Effect.sync(() => parts.find((part) => part.id === partID)),
  removeMessage: ({ messageID }) =>
    Effect.sync(() => {
      removedMessages.push(messageID)
      if (message?.id === messageID) message = undefined
      return messageID
    }),
  findMessage: (_sessionID, predicate) =>
    Effect.sync(() => {
      const found = message ? ({ info: message, parts } as SessionLegacy.WithParts) : undefined
      const matched = [found, ...queuedMessages].find((entry) => entry && predicate(entry))
      return matched ? Option.some(matched) : Option.none()
    }),
})
const dependencies = Layer.mergeAll(
  sessions,
  Layer.mock(Todo.Service)({ update: () => Effect.void }),
  Layer.mock(Permission.Service)({}),
  Layer.mock(Question.Service)({}),
  Layer.mock(Agent.Service)({
    defaultInfo: () => Effect.succeed({ name: "build", mode: "primary", permission: [], options: {} }),
  }),
)
const driver = OpencodeXClaudeDriver.makeLayer({
  transport,
  resolveExecutable: async () => "/test/claude",
  // The startup auth retry is exercised by its own suite below.
  startupAuthRetryWindowSeconds: 0,
}).pipe(Layer.provide(dependencies))
const it = testEffect(driver)
const retryingDriver = OpencodeXClaudeDriver.makeLayer({
  transport,
  resolveExecutable: async () => "/test/claude",
  startupAuthRetryWindowSeconds: 3600,
  startupAuthRetryDelayMillis: 0,
}).pipe(Layer.provide(dependencies))
const retrying = testEffect(retryingDriver)

function reset(
  next: () => AsyncIterable<ClaudeMapper.ClaudeEvent>,
  options?: { interruptSettles?: boolean; metadata?: Record<string, unknown>; history?: SessionLegacy.WithParts[] },
) {
  script = next
  interrupts = 0
  interruptSettles = options?.interruptSettles ?? true
  message = undefined
  parts = []
  prompt = undefined
  transportOptions = undefined
  metadata = options?.metadata ?? {}
  history = options?.history ?? []
  persistedMetadata = undefined
  removedMessages = []
  offeredPrompts = []
  queuedMessages = []
}

function runTurn(input?: {
  text?: string
  images?: ClaudeImage[]
  delegate?: OpencodeXClaudeDriver.SwarmDelegate
  directory?: string
  liveQueue?: OpencodeXClaudeDriver.LiveQueue
}) {
  return Effect.gen(function* () {
    const service = yield* OpencodeXClaudeDriver.Service
    return yield* service.runTurn({
      sessionID: sessionID as never,
      parentMessageID: parentMessageID as never,
      text: input?.text ?? "hello",
      ...(input?.images ? { images: input.images } : {}),
      ...(input?.delegate ? { delegate: input.delegate } : {}),
      ...(input?.liveQueue ? { liveQueue: input.liveQueue } : {}),
      // The driver refuses to spawn into a directory that does not exist.
      directory: input?.directory ?? process.cwd(),
      providerID: "claude-code",
      modelID: "sonnet",
    })
  })
}

async function* notLoggedIn() {
  yield {
    type: "result" as const,
    subtype: "error_during_execution",
    is_error: true,
    result: "Not logged in · Please run /login",
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 0 },
  }
}

async function* success() {
  yield { type: "result" as const, subtype: "success", total_cost_usd: 0, usage: { input_tokens: 1, output_tokens: 0 } }
}

const image: ClaudeImage = {
  type: "image",
  source: { type: "base64", media_type: "image/png", data: "AAA=" },
}

const historyMessage = (id: string, role: string, text: string) =>
  ({ info: { id, role }, parts: [{ type: "text", text }] }) as SessionLegacy.WithParts

function assistantInfo(result: SessionLegacy.WithParts) {
  if (result.info.role !== "assistant") throw new Error("Expected an assistant message")
  return result.info
}

describe("Claude driver delivery finalization", () => {
  it.effect("offers queued Claude follow-ups FIFO without detaching after the first result", () =>
    Effect.gen(function* () {
      const previous = process.env.OPENCODE_CLAUDE_LIVE_QUEUE
      process.env.OPENCODE_CLAUDE_LIVE_QUEUE = "1"
      reset(async function* () {
        yield {
          type: "result" as const,
          subtype: "success",
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
        yield {
          type: "result" as const,
          subtype: "success",
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
        yield {
          type: "result" as const,
          subtype: "success",
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
      })
      const offers = [
        { commandID: "sec_1", messageID: "msg_follow_1" as never, ordinal: 1 },
        { commandID: "sec_2", messageID: "msg_follow_2" as never, ordinal: 2 },
      ]
      const settled: string[] = []
      queuedMessages = [
        historyMessage("msg_follow_1", "user", "follow up"),
        historyMessage("msg_follow_2", "user", "follow up"),
      ]
      try {
        yield* runTurn({
          liveQueue: {
            reserve: () => Effect.sync(() => offers.shift()),
            offer: () => Effect.succeed(true),
            requeue: () => Effect.void,
            settle: (offer) => Effect.sync(() => settled.push(offer.commandID)),
            failOffered: () => Effect.void,
          },
        })

        expect(offeredPrompts).toEqual(["follow up", "follow up"])
        expect(settled).toEqual(["sec_1", "sec_2"])
      } finally {
        if (previous === undefined) delete process.env.OPENCODE_CLAUDE_LIVE_QUEUE
        else process.env.OPENCODE_CLAUDE_LIVE_QUEUE = previous
      }
    }),
  )

  it.effect("fails an offered command when its mapped assistant result has an error", () =>
    Effect.gen(function* () {
      const previous = process.env.OPENCODE_CLAUDE_LIVE_QUEUE
      process.env.OPENCODE_CLAUDE_LIVE_QUEUE = "1"
      reset(async function* () {
        yield {
          type: "result" as const,
          subtype: "success",
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
        yield {
          type: "result" as const,
          subtype: "error_max_turns",
          is_error: true,
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
      })
      queuedMessages = [historyMessage("msg_follow_error", "user", "follow up")]
      const settled: Array<{ commandID: string; error?: string }> = []
      try {
        yield* runTurn({
          liveQueue: {
            reserve: () =>
              Effect.succeed({ commandID: "sec_error", messageID: "msg_follow_error" as never, ordinal: 1 }),
            offer: () => Effect.succeed(true),
            requeue: () => Effect.void,
            settle: (offer, error) => Effect.sync(() => settled.push({ commandID: offer.commandID, error })),
            failOffered: () => Effect.void,
          },
        })

        expect(settled).toEqual([{ commandID: "sec_error", error: expect.stringContaining("UnknownError") }])
      } finally {
        if (previous === undefined) delete process.env.OPENCODE_CLAUDE_LIVE_QUEUE
        else process.env.OPENCODE_CLAUDE_LIVE_QUEUE = previous
      }
    }),
  )

  it.effect("requeues a fence-rejected offer without pushing it to Claude", () =>
    Effect.gen(function* () {
      const previous = process.env.OPENCODE_CLAUDE_LIVE_QUEUE
      process.env.OPENCODE_CLAUDE_LIVE_QUEUE = "1"
      reset(async function* () {
        yield {
          type: "result" as const,
          subtype: "success",
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 0 },
        }
      })
      queuedMessages = [historyMessage("msg_rejected", "user", "must not reach Claude")]
      const rejected = { commandID: "sec_rejected", messageID: "msg_rejected" as never, ordinal: 1 }
      const requeued: string[] = []
      let reserved = false
      try {
        yield* runTurn({
          liveQueue: {
            reserve: () => Effect.sync(() => (reserved ? undefined : ((reserved = true), rejected))),
            offer: () => Effect.succeed(false),
            requeue: (offer) => Effect.sync(() => requeued.push(offer.commandID)),
            settle: () => Effect.void,
            failOffered: () => Effect.void,
          },
        })

        expect(requeued).toEqual(["sec_rejected"])
        expect(offeredPrompts).toEqual([])
      } finally {
        if (previous === undefined) delete process.env.OPENCODE_CLAUDE_LIVE_QUEUE
        else process.env.OPENCODE_CLAUDE_LIVE_QUEUE = previous
      }
    }),
  )

  it.effect("persists one failed assistant when the stream closes without events", () =>
    Effect.gen(function* () {
      reset(async function* () {})

      const result = yield* runTurn()

      expect(interrupts).toBe(1)
      expect(result.info.id).not.toBe("")
      expect(result.info).toMatchObject({
        time: { completed: expect.any(Number) },
        error: {
          name: "UnknownError",
          data: { message: "Claude response delivery failed before the turn completed." },
        },
      })
      expect(result.parts.map((part) => part.type)).toEqual(["step-start", "step-finish"])
      expect(new Set(result.parts.map((part) => part.messageID))).toEqual(new Set([result.info.id]))
    }),
  )

  it.effect("persists a generic failure when the iterator rejects", () =>
    Effect.gen(function* () {
      reset(() => ({
        [Symbol.asyncIterator]: () => ({ next: () => Promise.reject({ secret: "do-not-persist", value: 1n }) }),
      }))

      const result = yield* runTurn()

      expect(interrupts).toBe(1)
      expect(JSON.stringify(result)).not.toContain("do-not-persist")
      expect(assistantInfo(result).error).toMatchObject({
        name: "UnknownError",
        data: { message: "Claude response delivery failed before the turn completed." },
      })
    }),
  )

  // OpencodeX-zx2: right after a daemon restart the CLI can report "not
  // logged in" once and be fine a moment later.
  retrying.effect("retries a startup-window auth failure once and drops the failed message", () =>
    Effect.gen(function* () {
      let attempt = 0
      reset(() => {
        attempt += 1
        if (attempt === 1) return notLoggedIn()
        return success()
      })

      const result = yield* runTurn()

      expect(attempt).toBe(2)
      expect(assistantInfo(result).error).toBeUndefined()
      expect(removedMessages).toHaveLength(1)
      expect(persistedMetadata).toMatchObject({ claudeCode: { authState: "ready" } })
    }),
  )

  retrying.effect("does not retry a second auth failure", () =>
    Effect.gen(function* () {
      let attempt = 0
      reset(() => {
        attempt += 1
        return notLoggedIn()
      })

      const result = yield* runTurn()

      expect(attempt).toBe(2)
      expect(JSON.stringify(assistantInfo(result).error)).toMatch(/not logged in/i)
    }),
  )

  it.effect("persists the failure when transport interruption never settles", () =>
    Effect.gen(function* () {
      reset(async function* () {}, { interruptSettles: false })

      const result = yield* runTurn()

      expect(interrupts).toBe(1)
      expect(assistantInfo(result).error).toMatchObject({
        name: "UnknownError",
        data: { message: "Claude response delivery failed before the turn completed." },
      })
    }),
  )

  it.effect("preserves partial text and fails the same assistant", () =>
    Effect.gen(function* () {
      reset(async function* () {
        yield { type: "assistant", message: { id: "m1", content: [{ type: "text", text: "Partial answer" }] } }
      })

      const result = yield* runTurn()

      expect(interrupts).toBe(1)
      expect(result.parts.find((part) => part.type === "text")).toMatchObject({ text: "Partial answer" })
      expect(assistantInfo(result).error?.name).toBe("UnknownError")
    }),
  )

  it.effect("fails a turn whose session directory is gone without spawning", () =>
    Effect.gen(function* () {
      reset(success)

      const result = yield* runTurn({ directory: "/nonexistent/opencodex-driver-test" })

      expect(prompt).toBeUndefined()
      expect(assistantInfo(result).error).toMatchObject({
        name: "UnknownError",
        data: { message: expect.stringContaining("/nonexistent/opencodex-driver-test does not exist") },
      })
    }),
  )

  it.effect("does not interrupt or fail a terminal result-only turn", () =>
    Effect.gen(function* () {
      reset(async function* () {
        yield { type: "result", subtype: "success", total_cost_usd: 0, usage: { input_tokens: 1, output_tokens: 0 } }
      })

      const result = yield* runTurn()

      expect(interrupts).toBe(0)
      expect(assistantInfo(result).error).toBeUndefined()
      expect(result.parts.map((part) => part.type)).toEqual(["step-start", "step-finish"])
    }),
  )
})

/**
 * A delegated role stamps the child session onto the orchestrator's tool part
 * while it runs (prompt-swarm.runSwarmRole), and that stamp is the transcript's
 * only route into the child. The mapper rebuilds the part from the tool_result
 * alone, so the driver has to carry the stamp across that rewrite.
 */
describe("Claude driver swarm delegation", () => {
  it.effect("keeps the delegated child on the tool part after the delegation returns", () =>
    Effect.gen(function* () {
      reset(delegationStream)

      const result = yield* runTurn({ delegate: { roles: [{ name: "Coder" }], run: () => stampChild } })

      const part = result.parts.find((item) => item.type === "tool")
      expect(part).toMatchObject({
        tool: "task",
        callID: "toolu_1",
        state: {
          status: "completed",
          output: "the role's report",
          // The mapper's own metadata still lands; the stamp rides alongside.
          metadata: { sessionId: "ses_child", swarmRole: "Coder" },
        },
      })
    }),
  )

  it.effect("carries the stamp onto a delegation the turn abandons", () =>
    Effect.gen(function* () {
      reset(async function* () {
        yield toolUse()
        yield { type: "text", text: await transportOptions!.delegate!.run(delegated) } as never
      })

      const result = yield* runTurn({ delegate: { roles: [{ name: "Coder" }], run: () => stampChild } })

      expect(result.parts.find((item) => item.type === "tool")).toMatchObject({
        state: { status: "error", metadata: { sessionId: "ses_child", interrupted: true } },
      })
    }),
  )
})

const delegated = { role: "Coder", prompt: "Ship it", toolUseID: "toolu_1" }

const toolUse = () => ({
  type: "assistant" as const,
  message: {
    id: "m1",
    content: [
      {
        type: "tool_use",
        id: "toolu_1",
        name: "mcp__opencodex_swarm__delegate",
        input: { role: "Coder", prompt: "Ship it" },
      },
    ],
  },
})

/** Mirrors what prompt-swarm writes onto the parent's running tool part. */
const stampChild = Effect.sync(() => {
  const part = parts.find((item) => item.type === "tool")
  if (part?.type === "tool" && part.state.status === "running")
    parts = [
      ...parts.filter((item) => item.id !== part.id),
      { ...part, state: { ...part.state, metadata: { sessionId: "ses_child", swarmRole: "Coder" } } },
    ]
  return { ok: true as const, text: "the role's report" }
})

async function* delegationStream() {
  yield toolUse()
  const report = await transportOptions!.delegate!.run(delegated)
  yield {
    type: "user" as const,
    message: {
      content: [{ type: "tool_result", tool_use_id: "toolu_1", content: report.ok ? report.text : "failed" }],
    },
  }
  yield { type: "result" as const, subtype: "success", total_cost_usd: 0, usage: { input_tokens: 1, output_tokens: 0 } }
}

describe("Claude driver prompt assembly", () => {
  it.effect("preserves the text-only transport prompt", () =>
    Effect.gen(function* () {
      reset(success)
      yield* runTurn()
      expect(prompt).toBe("hello")
    }),
  )

  it.effect("sends an image-only turn without an empty text block", () =>
    Effect.gen(function* () {
      reset(success)
      yield* runTurn({ text: "", images: [image] })
      expect(prompt).toEqual([image])
    }),
  )

  // Images lead the prompt: with a long text block first (swarm briefing,
  // replayed history) Claude reported "no image was attached" (OpencodeX-i13).
  it.effect("places images before text in a mixed turn", () =>
    Effect.gen(function* () {
      reset(success)
      yield* runTurn({ text: "describe this", images: [image] })
      expect(prompt).toEqual([image, { type: "text", text: "describe this" }])
    }),
  )

  it.effect("places the handoff primer after a first-turn image", () =>
    Effect.gen(function* () {
      reset(success, {
        history: [historyMessage("msg_prior", "user", "Earlier request"), historyMessage(parentMessageID, "user", "")],
      })
      yield* runTurn({ text: "", images: [image] })
      expect(prompt).toEqual([image, { type: "text", text: expect.stringContaining("User: Earlier request") }])
      expect((prompt as Exclude<ClaudePrompt, string>)[1]).toMatchObject({ text: expect.stringMatching(/\n\n$/) })
    }),
  )

  it.effect("does not replay a primer when resuming an image turn", () =>
    Effect.gen(function* () {
      reset(success, {
        metadata: { claudeCode: { conversationID: "conversation-1", launched: true } },
        history: [historyMessage("msg_prior", "user", "Earlier request"), historyMessage(parentMessageID, "user", "")],
      })
      yield* runTurn({ text: "", images: [image] })
      expect(prompt).toEqual([image])
      expect(transportOptions?.resumeID).toBe("conversation-1")
    }),
  )
})
