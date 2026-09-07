/**
 * `question_reply` is how a parent answers a child that is blocked on a
 * question. The authorization check is the reason it exists as a tool rather
 * than a prompt instruction: a session may only respond for a session it
 * structurally owns (`session.parentID`), never for a sibling or a stranger.
 */
import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber, Layer, Queue } from "effect"
import { eq } from "drizzle-orm"
import { QuestionReplyTool } from "../../src/tool/question-reply"
import { Question } from "../../src/question"
import { Session } from "@/session/session"
import { SessionID, MessageID } from "../../src/session/schema"
import { SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { Agent } from "../../src/agent/agent"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Truncate } from "@/tool/truncate"
import { testEffect } from "../lib/effect"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"

const it = testEffect(
  Layer.mergeAll(
    Question.layer.pipe(Layer.provide(Database.defaultLayer), Layer.provideMerge(EventV2Bridge.defaultLayer)),
    Session.defaultLayer,
    Database.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    Truncate.defaultLayer,
    Agent.defaultLayer,
  ),
)

const context = (sessionID: SessionID) => ({
  sessionID,
  directory: process.cwd(),
  messageID: MessageID.make("msg_question_reply_test"),
  callID: "test-call",
  agent: "test-agent",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
})

const QUESTIONS = [
  {
    question: "Which database should the worker use?",
    header: "Database",
    options: [
      { label: "Postgres", description: "Managed Postgres" },
      { label: "SQLite", description: "Embedded SQLite" },
    ],
  },
]

const pending = Effect.fn("QuestionReplyTest.pending")(function* () {
  const question = yield* Question.Service
  const events = yield* EventV2Bridge.Service
  const asked = yield* Queue.unbounded<void>()
  const off = yield* events.listen((event) => {
    if (event.type === Question.Event.Asked.type) Queue.offerUnsafe(asked, undefined)
    return Effect.void
  })
  yield* Effect.addFinalizer(() => off)
  for (;;) {
    const item = (yield* question.list())[0]
    if (item) return item
    yield* Queue.take(asked).pipe(Effect.timeout("2 seconds"))
  }
})

/** A parent, a child that asks, and the tool ready to answer for it. */
const scenario = Effect.fn("QuestionReplyTest.scenario")(function* () {
  const sessions = yield* Session.Service
  const question = yield* Question.Service
  const tool = yield* Effect.flatMap(QuestionReplyTool, (info) => info.init())
  const parent = yield* sessions.create({ title: "orchestrator" })
  const child = yield* sessions.create({ parentID: parent.id, title: "worker" })
  const fiber = yield* question.ask({ sessionID: child.id, questions: QUESTIONS }).pipe(Effect.forkScoped)
  const request = yield* pending()
  return { sessions, parent, child, tool, fiber, request }
})

const state = Effect.fn("QuestionReplyTest.state")(function* (requestID: string) {
  const { db } = yield* Database.Service
  return yield* db
    .select({ state: SessionInteractionTable.state })
    .from(SessionInteractionTable)
    .where(eq(SessionInteractionTable.id, requestID))
    .get()
    .pipe(Effect.orDie)
})

describe("tool.question_reply", () => {
  it.instance("answers for a child and resumes it", () =>
    Effect.gen(function* () {
      const { tool, fiber, request, parent } = yield* scenario()

      const result = yield* tool.execute(
        { requestID: String(request.id), action: "answer", answers: [["Postgres"]] },
        context(parent.id),
      )
      expect(result.metadata).toMatchObject({ outcome: "answered" })

      expect(yield* Fiber.join(fiber)).toEqual([["Postgres"]])
      expect(yield* state(String(request.id))).toEqual({ state: "replied" })
    }),
  )

  it.instance("rejects for a child and resumes it with a rejection", () =>
    Effect.gen(function* () {
      const { tool, fiber, request, parent } = yield* scenario()

      const result = yield* tool.execute({ requestID: String(request.id), action: "reject" }, context(parent.id))
      expect(result.metadata).toMatchObject({ outcome: "rejected" })

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Question.RejectedError)
      expect(yield* state(String(request.id))).toEqual({ state: "rejected" })
    }),
  )

  it.instance("refuses a caller that does not own the asking session", () =>
    Effect.gen(function* () {
      const { sessions, tool, fiber, request } = yield* scenario()
      const stranger = yield* sessions.create({ title: "unrelated" })

      const exit = yield* tool
        .execute({ requestID: String(request.id), action: "answer", answers: [["Postgres"]] }, context(stranger.id))
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(String(Cause.squash(exit.cause))).toContain("not a child of this session")

      // Refused, not resolved: the question is still the parent's (or a human's) to answer.
      expect(yield* state(String(request.id))).toEqual({ state: "pending" })
      yield* Question.Service.use((svc) => svc.reject(request.id))
      yield* Fiber.await(fiber)
    }),
  )

  it.instance("reports a question somebody else already resolved instead of claiming success", () =>
    Effect.gen(function* () {
      const { tool, fiber, request, parent } = yield* scenario()
      // The human got there first through the HTTP path.
      yield* Question.Service.use((svc) => svc.reply({ requestID: request.id, answers: [["SQLite"]] }))
      yield* Fiber.join(fiber)

      const result = yield* tool.execute(
        { requestID: String(request.id), action: "answer", answers: [["Postgres"]] },
        context(parent.id),
      )
      expect(result.metadata).toMatchObject({ outcome: "already_resolved" })
      expect(result.output).toContain("already")
      // The human's answer stands.
      expect(yield* state(String(request.id))).toEqual({ state: "replied" })
    }),
  )

  it.instance("requires answers when answering", () =>
    Effect.gen(function* () {
      const { tool, fiber, request, parent } = yield* scenario()

      const exit = yield* tool.execute({ requestID: String(request.id), action: "answer" }, context(parent.id)).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(String(Cause.squash(exit.cause))).toContain("`answers` is required")

      yield* Question.Service.use((svc) => svc.reject(request.id))
      yield* Fiber.await(fiber)
    }),
  )
})
