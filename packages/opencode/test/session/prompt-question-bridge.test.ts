/**
 * A child session's question has to reach its parent as durable work.
 *
 * Production, 2026-09-06: child `ses_f85dec2caffeNzSH2FzHGaVjx2` raised a
 * question and blocked for ~43 minutes. Its `session.parentID` named a live
 * parent, but nothing told that parent, so only a human or the operator's
 * 30-minute sweep could have noticed. These tests hold the bridge to the
 * standard the previous durability fixes set: the row must exist AND the
 * parent must actually earn a turn from it, exactly once, without the child's
 * `ask` ever depending on any of that succeeding.
 */
import { NodeFileSystem } from "@effect/platform-node"
import { OpencodeXGoal } from "@/opencodex/goal"
import { OpencodeXJob } from "@/opencodex/job"
import { OpencodeXClaudeDriver } from "@/opencodex/claude-driver"
import { Database } from "@opencode-ai/core/database/database"
import { and, eq } from "drizzle-orm"
import { EventV2Bridge } from "@/event-v2-bridge"
import { FetchHttpClient } from "effect/unstable/http"
import { expect } from "bun:test"
import { Effect, Fiber, Layer, Schema } from "effect"
import path from "path"
import { Agent as AgentSvc } from "../../src/agent/agent"
import { BackgroundJob } from "@/background/job"
import { Command } from "../../src/command"
import { Config } from "@/config/config"
import { LSP } from "@/lsp/lsp"
import { MCP } from "../../src/mcp"
import { Permission } from "../../src/permission"
import { Plugin } from "../../src/plugin"
import { Provider as ProviderSvc } from "@/provider/provider"
import { Env } from "../../src/env"
import { Git } from "../../src/git"
import { Image } from "../../src/image/image"
import { Question } from "../../src/question"
import { QuestionID } from "../../src/question/schema"

const encodeQuestionID = Schema.encodeSync(QuestionID)
import { Todo } from "../../src/session/todo"
import { Session } from "@/session/session"
import { SessionCommandTable, SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { LLM } from "../../src/session/llm"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionPromptRecovery } from "../../src/session/prompt-recovery"
import { SessionQuestionNotify } from "../../src/session/question-notify"
import { SessionRevert } from "../../src/session/revert"
import { SessionRunState } from "../../src/session/run-state"
import { MessageID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { Skill } from "../../src/skill"
import { SystemPrompt } from "../../src/session/system"
import { Snapshot } from "../../src/snapshot"
import { ToolRegistry } from "@/tool/registry"
import { GuiBridge } from "@/opencodex/gui-bridge"
import { Truncate } from "@/tool/truncate"
import * as Log from "@opencode-ai/core/util/log"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Ripgrep } from "../../src/file/ripgrep"
import { Format } from "../../src/format"
import { Reference } from "../../src/reference/reference"
import { RepositoryCache } from "../../src/reference/repository-cache"
import { TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect, testEffectShared } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { OpencodeXProject } from "@/opencodex/project"

void Log.init({ print: false })

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ProviderV2.ModelID.make("test-model"),
}

const mcp = Layer.succeed(
  MCP.Service,
  MCP.Service.of({
    status: () => Effect.succeed({}),
    clients: () => Effect.succeed({}),
    tools: () => Effect.succeed({}),
    prompts: () => Effect.succeed({}),
    resources: () => Effect.succeed({}),
    add: () => Effect.succeed({ status: { status: "disabled" as const } }),
    connect: () => Effect.void,
    disconnect: () => Effect.void,
    getPrompt: () => Effect.succeed(undefined),
    readResource: () => Effect.succeed(undefined),
    startAuth: () => Effect.die("unexpected MCP auth"),
    authenticate: () => Effect.die("unexpected MCP auth"),
    finishAuth: () => Effect.die("unexpected MCP auth"),
    removeAuth: () => Effect.void,
    supportsOAuth: () => Effect.succeed(false),
    hasStoredTokens: () => Effect.succeed(false),
    getAuthStatus: () => Effect.succeed("not_authenticated" as const),
  }),
)

const lsp = Layer.succeed(
  LSP.Service,
  LSP.Service.of({
    init: () => Effect.void,
    status: () => Effect.succeed([]),
    hasClients: () => Effect.succeed(false),
    touchFile: () => Effect.void,
    diagnostics: () => Effect.succeed({}),
    hover: () => Effect.succeed(undefined),
    definition: () => Effect.succeed([]),
    references: () => Effect.succeed([]),
    implementation: () => Effect.succeed([]),
    documentSymbol: () => Effect.succeed([]),
    workspaceSymbol: () => Effect.succeed([]),
    prepareCallHierarchy: () => Effect.succeed([]),
    incomingCalls: () => Effect.succeed([]),
    outgoingCalls: () => Effect.succeed([]),
  }),
)

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const status = SessionStatus.layer.pipe(
  Layer.provide(Database.defaultLayer),
  Layer.provideMerge(EventV2Bridge.defaultLayer),
)
const run = SessionRunState.layer.pipe(Layer.provide(status))
const infra = Layer.mergeAll(NodeFileSystem.layer, CrossSpawnSpawner.defaultLayer)

function makePrompt() {
  const deps = Layer.mergeAll(
    Session.defaultLayer,
    Snapshot.defaultLayer,
    LLM.defaultLayer,
    Env.defaultLayer,
    AgentSvc.defaultLayer,
    Command.defaultLayer,
    Permission.defaultLayer,
    Plugin.defaultLayer,
    Config.defaultLayer,
    ProviderSvc.defaultLayer,
    lsp,
    mcp,
    AppFileSystem.defaultLayer,
    BackgroundJob.defaultLayer,
    status,
    Database.defaultLayer,
    EventV2Bridge.defaultLayer,
  ).pipe(Layer.provideMerge(infra))
  const question = Question.layer.pipe(Layer.provideMerge(deps))
  const todo = Todo.layer.pipe(Layer.provideMerge(deps))
  const registry = ToolRegistry.layer.pipe(
    Layer.provide(GuiBridge.defaultLayer),
    Layer.provide(OpencodeXGoal.layer),
    Layer.provide(OpencodeXJob.layer),
    Layer.provide(Skill.defaultLayer),
    Layer.provide(FetchHttpClient.layer),
    Layer.provide(CrossSpawnSpawner.defaultLayer),
    Layer.provide(RepositoryCache.defaultLayer),
    Layer.provide(Git.defaultLayer),
    Layer.provide(Reference.defaultLayer),
    Layer.provide(Ripgrep.defaultLayer),
    Layer.provide(Format.defaultLayer),
    Layer.provide(RuntimeFlags.layer({})),
    Layer.provide(Layer.mock(OpencodeXProject.Service)({})),
    Layer.provideMerge(todo),
    Layer.provideMerge(question),
    Layer.provideMerge(deps),
  )
  const trunc = Truncate.layer.pipe(Layer.provideMerge(deps))
  const proc = SessionProcessor.layer.pipe(
    Layer.provide(summary),
    Layer.provide(Image.defaultLayer),
    Layer.provide(RuntimeFlags.layer({})),
    Layer.provideMerge(deps),
  )
  const compact = SessionCompaction.layer.pipe(
    Layer.provide(RuntimeFlags.layer({})),
    Layer.provideMerge(proc),
    Layer.provideMerge(deps),
  )
  return SessionPrompt.layer.pipe(
    Layer.provide(OpencodeXClaudeDriver.defaultLayer),
    Layer.provide(SessionRevert.defaultLayer),
    Layer.provide(Skill.defaultLayer),
    Layer.provide(Image.defaultLayer),
    Layer.provide(Reference.defaultLayer),
    Layer.provide(summary),
    Layer.provideMerge(run),
    Layer.provideMerge(compact),
    Layer.provideMerge(proc),
    Layer.provideMerge(registry),
    Layer.provideMerge(trunc),
    Layer.provideMerge(todo),
    Layer.provide(Instruction.defaultLayer),
    Layer.provide(SystemPrompt.defaultLayer),
    Layer.provide(RuntimeFlags.layer({})),
    Layer.provideMerge(deps),
    Layer.provide(summary),
  )
}

const it = testEffect(Layer.mergeAll(TestLLMServer.layer, makePrompt()))

const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: { apiKey: "test-key", baseURL: "http://localhost:1/v1" },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: { ...cfg.provider.test, options: { ...cfg.provider.test.options, baseURL: url } },
    },
  }
}

const useServerConfig = Effect.fn("test.useServerConfig")(function* () {
  const { directory: dir } = yield* TestInstance
  const llm = yield* TestLLMServer
  const fs = yield* AppFileSystem.Service
  yield* fs.writeWithDirs(
    path.join(dir, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...providerCfg(llm.url) }),
  )
  return { dir, llm }
})

/**
 * The human's escape hatch, through the production route tree. `webHandler`
 * resolves its own service graph for the directory, so this also proves the
 * resume path is durable rather than in-memory: a reply that never touches
 * this test's `Question` instance still has to wake the child.
 */
const httpReply = Effect.fn("test.httpReply")(function* (
  requestID: QuestionID,
  answers: ReadonlyArray<ReadonlyArray<string>>,
) {
  const { directory } = yield* TestInstance
  const response = yield* Effect.promise(() =>
    HttpApiApp.webHandler().handler(
      new Request(`http://localhost/question/${encodeQuestionID(requestID)}/reply`, {
        method: "POST",
        body: JSON.stringify({ answers }),
        headers: { "x-opencode-directory": directory, "content-type": "application/json" },
      }),
      HttpApiApp.context,
    ),
  )
  expect(response.status).toBe(200)
})

const QUESTIONS: ReadonlyArray<Question.Info> = [
  {
    question: "Which database should the worker use?",
    header: "Database",
    options: [
      { label: "Postgres", description: "Managed Postgres" },
      { label: "SQLite", description: "Embedded SQLite" },
    ],
  },
]

const askChild = Effect.fn("test.askChild")(function* (sessionID: SessionID) {
  const question = yield* Question.Service
  return yield* question.ask({ sessionID, questions: QUESTIONS })
})

const pendingRequest = pollWithTimeout(
  Effect.gen(function* () {
    const question = yield* Question.Service
    return (yield* question.list())[0]
  }),
  "no question ever became pending",
  "5 seconds",
)

/** Every durable command row the bridge wrote for `requestID`. */
const notificationID = (requestID: QuestionID) => MessageID.make(`msg_question_${encodeQuestionID(requestID)}`)

const commandRows = Effect.fn("test.commandRows")(function* (requestID: QuestionID) {
  const { db } = yield* Database.Service
  return yield* db
    .select()
    .from(SessionCommandTable)
    .where(eq(SessionCommandTable.message_id, notificationID(requestID)))
    .all()
    .pipe(Effect.orDie)
})

const waitForCommand = (requestID: QuestionID) =>
  pollWithTimeout(
    commandRows(requestID).pipe(Effect.map((rows) => (rows.length > 0 ? rows : undefined))),
    `no session_command row for question ${encodeQuestionID(requestID)}`,
    "10 seconds",
  )

/**
 * An idle parent already holds a finished human turn, so the notification only
 * earns a model call if the loop recognises the `question_request` tag. A
 * parent with no history would run a turn either way and prove nothing.
 */
const seedIdleParent = Effect.fn("test.seedIdleParent")(function* (sessionID: SessionID) {
  const prompt = yield* SessionPrompt.Service
  yield* prompt.prompt({ sessionID, model: ref, parts: [{ type: "text", text: "stand by" }] })
})

it.instance("notifies an idle parent and gives it a turn", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig()
    const sessions = yield* Session.Service

    const parent = yield* sessions.create({ title: "orchestrator" })
    const child = yield* sessions.create({
      parentID: parent.id,
      title: "worker (@build subagent)",
      metadata: { opencodex: { swarmID: "swarm_test", swarmRole: "implementer" } },
    })
    yield* seedIdleParent(parent.id)
    const seeded = yield* llm.calls

    const fiber = yield* askChild(child.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest

    const rows = yield* waitForCommand(request.id)
    expect(rows).toHaveLength(1)
    expect(rows[0]?.session_id).toBe(parent.id)

    const notification = yield* pollWithTimeout(
      Effect.gen(function* () {
        const messages = yield* sessions.messages({ sessionID: parent.id })
        return messages.find((message) => message.info.id === notificationID(request.id))
      }),
      "notification message never landed",
      "10 seconds",
    )
    expect(notification.parts).toMatchObject([{ type: "text", synthetic: true, metadata: { question_request: true } }])
    const text = notification.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("")
    expect(text).toContain(encodeQuestionID(request.id))
    expect(text).toContain(child.id)
    expect(text).toContain("implementer")
    expect(text).toContain("Which database should the worker use?")
    expect(text).toContain("Postgres")
    expect(text).toContain("question_reply")

    // The point of the whole change: the parent has to actually run. A durable
    // row nobody consumes is the failure class this fixes, so the evidence is
    // a model call carrying the notification -- polled, because the assistant
    // row is written before the request reaches the model.
    const prompted = yield* pollWithTimeout(
      llm.inputs.pipe(
        Effect.map((bodies) =>
          bodies.find((body) => JSON.stringify(body).includes(encodeQuestionID(request.id))),
        ),
      ),
      "the parent never took a turn on the question notification",
      "15 seconds",
    )
    expect(JSON.stringify(prompted)).toContain("question_request")
    expect(yield* llm.calls).toBeGreaterThan(seeded)

    // ...and the turn it earned finished as a normal assistant reply.
    const answered = yield* pollWithTimeout(
      Effect.gen(function* () {
        const messages = yield* sessions.messages({ sessionID: parent.id })
        return messages.find(
          (message) =>
            message.info.role === "assistant" &&
            message.info.parentID === notificationID(request.id) &&
            !!message.info.finish,
        )
      }),
      "the parent's question turn never finished",
      "15 seconds",
    )
    expect(answered.info.role).toBe("assistant")

    yield* Question.Service.use((svc) => svc.reject(request.id))
    yield* Fiber.await(fiber)
  }),
)

it.instance("queues behind a running parent turn without interrupting it", () =>
  Effect.gen(function* () {
    const { llm } = yield* useServerConfig()
    const prompt = yield* SessionPrompt.Service
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service

    const parent = yield* sessions.create({ title: "orchestrator" })
    const child = yield* sessions.create({ parentID: parent.id, title: "worker" })

    yield* llm.hang
    yield* prompt.promptAsync({ sessionID: parent.id, model: ref, parts: [{ type: "text", text: "long turn" }] })
    yield* llm.wait(1)

    const fiber = yield* askChild(child.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest
    const rows = yield* waitForCommand(request.id)

    expect(rows).toHaveLength(1)
    expect(rows[0]?.status).toBe("queued")
    // Deferred delivery must not steer or abort the turn already in flight.
    const execution = yield* db
      .select()
      .from(SessionExecutionTable)
      .where(eq(SessionExecutionTable.session_id, parent.id))
      .get()
      .pipe(Effect.orDie)
    expect(execution?.cancel_requested_at).toBeFalsy()
    const running = yield* db
      .select()
      .from(SessionCommandTable)
      .where(
        and(eq(SessionCommandTable.session_id, parent.id), eq(SessionCommandTable.message_id, notificationID(request.id))),
      )
      .all()
      .pipe(Effect.orDie)
    expect(running).toHaveLength(1)
    const notification = (yield* sessions.messages({ sessionID: parent.id })).filter(
      (message) => message.info.id === notificationID(request.id),
    )
    expect(notification).toHaveLength(1)
    expect(notification[0]?.parts.every((part) => part.type !== "text" || part.metadata?.steering !== true)).toBe(true)

    yield* Question.Service.use((svc) => svc.reject(request.id))
    yield* Fiber.await(fiber)
    yield* prompt.cancel(parent.id)
  }),
)

it.instance("writes one row and one message however often it is notified", () =>
  Effect.gen(function* () {
    yield* useServerConfig()
    const sessions = yield* Session.Service

    const parent = yield* sessions.create({ title: "orchestrator" })
    const child = yield* sessions.create({ parentID: parent.id, title: "worker" })

    const fiber = yield* askChild(child.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest
    yield* waitForCommand(request.id)

    // A second hook fire (a retry) and a recovery pass over the same still
    // pending row both land on the deterministic message id.
    yield* SessionQuestionNotify.notify(request)
    yield* SessionPromptRecovery.run()
    yield* SessionQuestionNotify.notify(request)

    expect(yield* commandRows(request.id)).toHaveLength(1)
    expect(
      (yield* sessions.messages({ sessionID: parent.id })).filter(
        (message) => message.info.id === notificationID(request.id),
      ),
    ).toHaveLength(1)

    yield* Question.Service.use((svc) => svc.reject(request.id))
    yield* Fiber.await(fiber)
  }),
)

it.instance("a failing notifier never blocks or breaks the child's ask", () =>
  Effect.gen(function* () {
    yield* useServerConfig()
    const sessions = yield* Session.Service

    const parent = yield* sessions.create({ title: "orchestrator" })
    const child = yield* sessions.create({ parentID: parent.id, title: "worker" })

    // Both failure modes an out-of-process listener can have: a defect, and a
    // handler that never returns. Neither may reach `ask`.
    const unregisterBroken = SessionQuestionNotify.register(() => Effect.die(new Error("notifier exploded")))
    const unregisterSlow = SessionQuestionNotify.register(() => Effect.never)
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        unregisterBroken()
        unregisterSlow()
      }),
    )

    const fiber = yield* askChild(child.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest
    // The real listener still ran despite its neighbours.
    yield* waitForCommand(request.id)

    yield* Question.Service.use((svc) => svc.reply({ requestID: request.id, answers: [["Postgres"]] }))
    expect(yield* Fiber.join(fiber)).toEqual([["Postgres"]])
  }),
)

/**
 * Requirement C: the human escape hatch must not regress.
 *
 * This one runs against the production route graph rather than the harness
 * above, through the shared memo map, so `Question`, `Session` and
 * `SessionPrompt` resolve to the very instances `HttpApiApp.webHandler` uses.
 * That is the only way `/question/:requestID/reply` can be exercised for real:
 * tests run on `OPENCODE_DB=:memory:`, so a separately built graph would not
 * even see the pending row.
 */
const production = testEffectShared(
  Layer.mergeAll(Question.defaultLayer, Session.defaultLayer, Database.defaultLayer, SessionPrompt.defaultLayer),
)

production.instance("a notified question a parent leaves alone is still answerable by a human", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service

    const parent = yield* sessions.create({ title: "orchestrator" })
    const child = yield* sessions.create({ parentID: parent.id, title: "worker" })

    const fiber = yield* askChild(child.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest
    // The parent was told...
    yield* waitForCommand(request.id)

    // ...declined to decide, so the request is still pending, and the human's
    // HTTP path still answers it and still resumes the child.
    expect((yield* Question.Service.use((svc) => svc.list())).map((item) => item.id)).toContain(request.id)
    yield* httpReply(request.id, [["SQLite"]])

    expect(yield* Fiber.join(fiber)).toEqual([["SQLite"]])
    expect(
      yield* db
        .select({ state: SessionInteractionTable.state })
        .from(SessionInteractionTable)
        .where(eq(SessionInteractionTable.id, encodeQuestionID(request.id)))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ state: "replied" })
  }),
)

it.instance("leaves a session with no parent to its human", () =>
  Effect.gen(function* () {
    yield* useServerConfig()
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service

    const solo = yield* sessions.create({ title: "top level" })
    const fiber = yield* askChild(solo.id).pipe(Effect.forkScoped)
    const request = yield* pendingRequest

    expect(yield* commandRows(request.id)).toHaveLength(0)
    expect(yield* db.select().from(SessionCommandTable).all().pipe(Effect.orDie)).toHaveLength(0)

    yield* Question.Service.use((svc) => svc.reject(request.id))
    yield* Fiber.await(fiber)
  }),
)
