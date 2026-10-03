/**
 * OpencodeX-k30: a delegated report is "accepted" when its tagged message and
 * command row are durable, and "consumed" only when a FINISHED owner turn
 * answered it under its own succeeded command. These tests drive the REAL
 * production wiring - `SessionPrompt.layer`'s recovery notifier
 * (`SessionPromptRecovery.run`), `promptAsync`, the durable command claim and
 * `executeCommand`, and the receipt reconciler (`reconcileReports`) - against
 * a scripted LLM server. Nothing here re-implements admission.
 *
 * Incident context (2026-10-03, PDT): the G1f reviewer finished locally at
 * 00:22 with NO callback; its result never reached, and was never accepted
 * by, the coordinator, which only acted after a manual wake at 07:56. The
 * informational prompt msg_0ff50069e001OPTRykNMmreaXc was a deliberate
 * no-reply input with an intentionally empty answer; a prompt without an
 * explicit report contract must never be auto-reprompted.
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
import { Effect, Exit, Fiber, Layer, Scope } from "effect"
import path from "path"
import { pathToFileURL } from "url"
import { RESERVATION_GRACE_MS } from "../../src/session/delegation-recovery"
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
import { Todo } from "../../src/session/todo"
import { Session } from "@/session/session"
import type { SessionLegacy } from "@opencode-ai/core/session/legacy"
import {
  SessionCommandTable,
  SessionExecutionTable,
  SessionInteractionTable,
} from "@opencode-ai/core/session/sql"
import { OpencodeXGoalNodeTable, OpencodeXGoalTable, OpencodeXProjectTable } from "@opencode-ai/core/opencodex/sql"
import { LLM } from "../../src/session/llm"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { SessionCompaction } from "../../src/session/compaction"
import { SessionSummary } from "../../src/session/summary"
import { Instruction } from "../../src/session/instruction"
import { SessionProcessor } from "../../src/session/processor"
import { SessionPrompt } from "../../src/session/prompt"
import { SessionPromptRecovery } from "../../src/session/prompt-recovery"
import * as PromptClaim from "../../src/session/prompt-claim"
import { MAX_CONTINUATIONS } from "../../src/session/delegation-continuation"
import {
  DELEGATION_RECORD_VERSION,
  delegationRecord,
  MAX_DELIVERY_ATTEMPTS,
  type DelegationRecord,
} from "../../src/session/delegation-outcome"
import { answers, reportAnswered } from "../../src/session/report-receipt"
import { initialState, mapEvent, startTurn, type ClaudeEvent, type SessionWrite } from "../../src/opencodex/claude-mapper"
import { SessionRevert } from "../../src/session/revert"
import { SessionRunState } from "../../src/session/run-state"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
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
import { InstanceState } from "@/effect/instance-state"
import { TestInstance } from "../fixture/fixture"
import { pollWithTimeout, testEffect } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { OpencodeXProject } from "@/opencodex/project"

void Log.init({ print: false })

const ref = { providerID: ProviderV2.ID.make("test"), modelID: ProviderV2.ModelID.make("test-model") }

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

const status = SessionStatus.layer.pipe(Layer.provide(Database.defaultLayer), Layer.provideMerge(EventV2Bridge.defaultLayer))
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

const providerCfg = (url: string) => ({
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
      options: { apiKey: "test-key", baseURL: url },
    },
  },
})

const TERMINAL = ["succeeded", "failed", "cancelled"]
let runCounter = 0

/** Real services, a configured provider, and an idle owner with a finished human turn. */
const boot = Effect.fnUntraced(function* (ownerTitle = "coordinator") {
  const { directory } = yield* TestInstance
  const llm = yield* TestLLMServer
  const fs = yield* AppFileSystem.Service
  yield* fs.writeWithDirs(
    path.join(directory, "opencode.json"),
    JSON.stringify({ $schema: "https://opencode.ai/config.json", ...providerCfg(llm.url) }),
  )
  const prompt = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const database = yield* Database.Service
  const db = database.db
  const owner = yield* sessions.create({ title: ownerTitle })
  yield* prompt.prompt({ sessionID: owner.id, model: ref, parts: [{ type: "text", text: "stand by" }] })
  const runID = `run_k30_${++runCounter}_${Date.now()}`
  const reportID = MessageID.make(`msg_delegation_recovery_${runID}`)
  const marker = `REVIEW-VERDICT-${runID}`

  const command = (messageID: MessageID = reportID, sessionID: SessionID = owner.id) =>
    db
      .select()
      .from(SessionCommandTable)
      .where(and(eq(SessionCommandTable.session_id, sessionID), eq(SessionCommandTable.message_id, messageID)))
      .all()
      .pipe(Effect.orDie)
  const settled = (messageID: MessageID = reportID, sessionID: SessionID = owner.id) =>
    pollWithTimeout(
      command(messageID, sessionID).pipe(Effect.map(([row]) => (row && TERMINAL.includes(row.status) ? row : undefined))),
      `command ${messageID} never settled`,
      "15 seconds",
    )
  const record = (childID: SessionID) => sessions.get(childID).pipe(Effect.map((s) => delegationRecord(s.metadata)))
  /** Provider turns that carried the report text. */
  const reportTurns = llm.inputs.pipe(Effect.map((inputs) => inputs.filter((i) => JSON.stringify(i).includes(marker)).length))
  const messages = (id: MessageID, sessionID: SessionID = owner.id) =>
    sessions.messages({ sessionID }).pipe(Effect.map((all) => all.filter((m) => m.info.id === id)))
  /** A settled background child whose report is pending delivery to `owner`. */
  const child = Effect.fnUntraced(function* (
    overrides: Partial<DelegationRecord> = {},
    parentID = owner.id,
    structural = true,
  ) {
    const created = yield* sessions.create({ ...(structural ? { parentID } : {}), title: "reviewer" })
    yield* sessions.stampDelegation({
      sessionID: created.id,
      record: {
        version: DELEGATION_RECORD_VERSION,
        runID,
        parentSessionID: parentID,
        attempt: 1,
        phase: "settled",
        outcome: "completed",
        startedAt: 1,
        completedAt: 2,
        // Native background shape: the swarm wake owns `background: true`
        // records; this exercises prompt.ts's recovery notifier.
        mode: "background",
        role: "reviewer",
        ownerID: "local:999999:dead:run_k30",
        summary: marker,
        deliveryOutcome: "pending",
        ...overrides,
      },
    })
    return created
  })
  /** Restart recovery: the production delegation recovery + notifier + command sweep. */
  const recover = SessionPromptRecovery.run()
  /** Ages the report command's settlement past every continuation backoff. */
  const age = (messageID: MessageID = reportID) =>
    db
      .update(SessionCommandTable)
      .set({ completed_at: 1 })
      .where(and(eq(SessionCommandTable.session_id, owner.id), eq(SessionCommandTable.message_id, messageID)))
      .run()
      .pipe(Effect.orDie)
  const interaction = (state: "pending" | "replied") =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      yield* db
        .insert(SessionInteractionTable)
        .values({
          id: `int_${owner.id}`,
          kind: "permission",
          session_id: owner.id,
          project_id: ctx.project.id,
          directory: ctx.directory,
          state,
          request_json: {},
        })
        .onConflictDoUpdate({ target: SessionInteractionTable.id, set: { state } })
        .run()
        .pipe(Effect.orDie)
    })
  return {
    llm,
    prompt,
    sessions,
    database,
    db,
    owner,
    runID,
    reportID,
    marker,
    command,
    settled,
    record,
    reportTurns,
    messages,
    child,
    recover,
    age,
    interaction,
  }
})

it.instance("idle owner: one tagged deferred report, one turn, consumed only after it answered", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.recover
    expect((yield* h.settled()).status).toBe("succeeded")
    const [message] = yield* h.messages(h.reportID)
    expect(message?.parts).toMatchObject([{ type: "text", synthetic: true, metadata: { task_report: true } }])
    expect(yield* h.reportTurns).toBe(1)
    // Accepted is not consumed until the reconciler sees the linked answer.
    expect((yield* h.record(reviewer.id))?.reportMessageID).toBe(h.reportID)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
    yield* h.recover
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("busy owner: the recovery notifier defers behind the live turn without steering it (defect 5)", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const gate = Promise.withResolvers<void>()
    yield* h.llm.hold("first done", gate.promise)
    const busyID = MessageID.ascending()
    yield* h.prompt.promptAsync({ sessionID: h.owner.id, messageID: busyID, model: ref, parts: [{ type: "text", text: "long work" }] })
    yield* h.llm.wait(2)
    const reviewer = yield* h.child()
    yield* h.recover
    expect((yield* h.command())[0]?.status).toBe("queued")
    expect(yield* h.reportTurns).toBe(0)
    gate.resolve()
    expect((yield* h.settled(busyID)).status).toBe("succeeded")
    expect((yield* h.settled()).status).toBe("succeeded")
    const busyAnswer = (yield* h.sessions.messages({ sessionID: h.owner.id })).find(
      (m) => m.info.role === "assistant" && m.info.parentID === busyID,
    )
    // Not aborted to steer: the live turn finished normally.
    expect(busyAnswer?.info.role === "assistant" ? busyAnswer.info.error : "missing").toBeUndefined()
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("dropped acceptance: a failed report turn is not consumed; failed command + restart continue once", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    expect((yield* h.record(reviewer.id))?.deliveryOutcome).toBe("delivered")
    // Restart: no second report message, no replay of the failed command.
    yield* h.recover
    yield* h.prompt.reconcileReports()
    expect(yield* h.messages(h.reportID)).toHaveLength(1)
    expect(yield* h.command()).toHaveLength(1)
    expect(yield* h.reportTurns).toBe(1)
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeUndefined()
    // Past the backoff the idle owner gets exactly one continuation.
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(2)
    yield* h.prompt.reconcileReports()
    const done = yield* h.record(reviewer.id)
    expect(done?.continuationAttempts).toBe(1)
    expect(done?.consumedAt).toBeNumber()
  }),
)

it.instance("duplicate: concurrent and repeated recovery produce one message, one command, one turn", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    yield* h.child()
    yield* Effect.all([h.recover, h.recover], { concurrency: "unbounded", discard: true })
    yield* h.settled()
    yield* h.recover
    yield* h.prompt.reconcileReports()
    expect(yield* h.messages(h.reportID)).toHaveLength(1)
    expect(yield* h.command()).toHaveLength(1)
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("restart: crash after the report persisted but before the delivered stamp delivers nothing twice", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child({ deliveryOutcome: "delivering", deliveryClaimedAt: 1, deliveryClaimToken: "run_dead" })
    // The dead process's notifier had already persisted the tagged report.
    yield* h.prompt.promptAsync({
      sessionID: h.owner.id,
      messageID: h.reportID,
      delivery: "deferred",
      parts: [{ type: "text", synthetic: true, metadata: { task_report: true }, text: h.marker }],
    })
    yield* h.settled()
    yield* h.recover
    const stamped = yield* h.record(reviewer.id)
    expect(stamped?.deliveryOutcome).toBe("delivered")
    expect(stamped?.reportMessageID).toBe(h.reportID)
    expect(yield* h.messages(h.reportID)).toHaveLength(1)
    expect(yield* h.reportTurns).toBe(1)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("task/live/recovery id mismatch: a persisted task report is reused, never re-sent as a recovery report", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const taskID = MessageID.make(`msg_task_report_${h.runID}`)
    const reviewer = yield* h.child({ deliveryOutcome: "delivering", deliveryClaimedAt: 1, deliveryClaimToken: "run_dead" })
    yield* h.prompt.promptAsync({
      sessionID: h.owner.id,
      messageID: taskID,
      delivery: "deferred",
      parts: [{ type: "text", synthetic: true, metadata: { task_report: true }, text: h.marker }],
    })
    yield* h.settled(taskID)
    yield* h.recover
    expect(yield* h.messages(h.reportID)).toHaveLength(0)
    expect((yield* h.record(reviewer.id))?.reportMessageID).toBe(taskID)
    expect(yield* h.reportTurns).toBe(1)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("retired owner by handoff title (not archived) is never auto-run (defect 3)", () =>
  Effect.gen(function* () {
    const h = yield* boot("reliability (handed off 10/3)")
    const reviewer = yield* h.child()
    yield* h.recover
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(0)
    const stamped = yield* h.record(reviewer.id)
    expect(stamped?.escalation).toBe("owner-retired")
    expect(stamped?.reportMessageID).toBeUndefined()
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(0)
  }),
)

it.instance("archived owner, human-cancelled owner and terminal goal record the report without a turn", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const ctx = yield* InstanceState.context
    // Archived.
    const archived = yield* h.sessions.create({ title: "old coordinator" })
    yield* h.sessions.setArchived({ sessionID: archived.id, time: Date.now() })
    const a = yield* h.child({ runID: `${h.runID}_a` }, archived.id)
    // Human cancelled the owner's execution.
    const cancelled = yield* h.sessions.create({ title: "paused coordinator" })
    yield* h.db
      .insert(SessionExecutionTable)
      .values({
        session_id: cancelled.id,
        project_id: ctx.project.id,
        directory: ctx.directory,
        state: "interrupted",
        generation: 1,
        cancel_requested_at: Date.now(),
      })
      .run()
      .pipe(Effect.orDie)
    const c = yield* h.child({ runID: `${h.runID}_c` }, cancelled.id)
    // The reporting child's goal is already terminal.
    const g = yield* h.child({ runID: `${h.runID}_g` })
    yield* h.db.insert(OpencodeXProjectTable).values({ id: `oxp_${h.runID}`, project_id: ctx.project.id }).run().pipe(Effect.orDie)
    yield* h.db
      .insert(OpencodeXGoalTable)
      .values({
        id: `goal_${h.runID}`,
        opencodex_project_id: `oxp_${h.runID}`,
        title: "g",
        statement: "g",
        success_criteria_json: [],
        status: "completed",
        source: "test",
        owner_session_id: h.owner.id,
      })
      .run()
      .pipe(Effect.orDie)
    yield* h.db
      .insert(OpencodeXGoalNodeTable)
      .values({ id: "n1", goal_id: `goal_${h.runID}`, kind: "task", title: "n", brief: "n", status: "completed", session_id: g.id })
      .run()
      .pipe(Effect.orDie)
    yield* h.recover
    for (const [id, escalation] of [
      [a.id, "owner-retired"],
      [c.id, "cancelled"],
      [g.id, "goal-terminal"],
    ] as const) {
      const stamped = yield* pollWithTimeout(
        h.record(id).pipe(Effect.map((r) => (r?.deliveryOutcome === "delivered" ? r : undefined))),
        `report for ${id} never recorded`,
      )
      expect(stamped.escalation).toBe(escalation)
    }
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(0)
  }),
)

it.instance("held owner (pending permission): receipt waits, then continues once the hold clears (defect 4)", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    yield* h.interaction("pending")
    const reviewer = yield* h.child()
    yield* h.recover
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(0)
    expect((yield* h.record(reviewer.id))?.reportMessageID).toBe(h.reportID)
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(0)
    expect((yield* h.record(reviewer.id))?.continuationAttempts ?? 0).toBe(0)
    yield* h.interaction("replied")
    yield* h.prompt.reconcileReports()
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(1)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("the requeue transaction itself refuses a held, retired or busy owner (defect 4)", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    // The real requeueReport, called directly: no outer reconciler read.
    const claim = yield* PromptClaim.make({
      database: h.database,
      events: yield* EventV2Bridge.Service,
      scope: yield* Scope.Scope,
      loop: () => Effect.die("must not run"),
    })
    const input = { sessionID: h.owner.id, messageID: h.reportID, childID: reviewer.id }
    yield* h.interaction("pending")
    expect(yield* claim.requeueReport(input)).toBe("held")
    yield* h.interaction("replied")
    // A QUEUED owner is never stacked: another command waits there.
    const ctx = yield* InstanceState.context
    yield* h.db
      .insert(SessionCommandTable)
      .values({
        id: `sec_other_${h.runID}`,
        session_id: h.owner.id,
        message_id: MessageID.ascending(),
        project_id: ctx.project.id,
        directory: ctx.directory,
        status: "queued",
        time_created: Date.now(),
        time_updated: Date.now(),
      })
      .run()
      .pipe(Effect.orDie)
    expect(yield* claim.requeueReport(input)).toBe("busy")
    yield* h.sessions.setTitle({ sessionID: h.owner.id, title: "coordinator (handed off 10/3)" })
    expect(yield* claim.requeueReport(input)).toBe("owner-retired")
    expect((yield* h.command())[0]?.status).toBe("failed")
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("informational prompt without a report contract is never reprompted (msg_0ff50069 shape)", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const info = MessageID.ascending()
    yield* h.llm.text("")
    yield* h.prompt.promptAsync({
      sessionID: h.owner.id,
      messageID: info,
      parts: [{ type: "text", text: `FYI ${h.marker}: informational routing, no reply needed.` }],
    })
    expect((yield* h.settled(info)).status).toBe("succeeded")
    yield* h.age(info)
    yield* h.recover
    yield* h.prompt.reconcileReports()
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(1)
    expect((yield* h.command(info))[0]?.status).toBe("succeeded")
  }),
)

it.instance("retries are bounded, then escalate with no further prompts", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    for (let i = 0; i <= MAX_CONTINUATIONS; i++) yield* h.llm.error(400, { error: { message: "still limited" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    for (let i = 0; i < MAX_CONTINUATIONS; i++) {
      yield* h.age()
      yield* h.prompt.reconcileReports()
      expect((yield* h.settled()).status).toBe("failed")
    }
    expect(yield* h.reportTurns).toBe(MAX_CONTINUATIONS + 1)
    yield* h.age()
    yield* h.prompt.reconcileReports()
    const stamped = yield* h.record(reviewer.id)
    expect(stamped?.escalation).toBe("retry-limit")
    expect(stamped?.continuationAttempts).toBe(MAX_CONTINUATIONS)
    yield* h.recover
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(MAX_CONTINUATIONS + 1)
  }),
)

it.instance("delivery attempts are bounded across restarts, then escalate", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child({ deliveryAttempts: MAX_DELIVERY_ATTEMPTS, deliveryOutcome: "failed" })
    yield* h.recover
    const stamped = yield* h.record(reviewer.id)
    expect(stamped?.escalation).toBe("delivery-failed")
    expect(yield* h.messages(h.reportID)).toHaveLength(0)
  }),
)

it.instance("defect 1: placeholder, error, abort and unlinked answers never consume", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    const assistant = (extra: Partial<SessionLegacy.Assistant>) =>
      h.sessions.updateMessage({
        id: MessageID.ascending(),
        role: "assistant",
        parentID: h.reportID,
        sessionID: h.owner.id,
        mode: "build",
        agent: "build",
        cost: 0,
        path: { cwd: "/tmp", root: "/tmp" },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ref.modelID,
        providerID: ref.providerID,
        time: { created: Date.now() },
        ...extra,
      })
    // A streaming placeholder (no completion stamp).
    yield* assistant({})
    // A completed, finished answer - but its command failed: not consumed.
    yield* assistant({ time: { created: Date.now(), completed: Date.now() }, finish: "stop" })
    expect(yield* reportAnswered(h.db, h.owner.id, h.reportID)).toBe(false)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeUndefined()
    // A human-aborted turn on the report escalates instead of re-running.
    yield* assistant({
      time: { created: Date.now(), completed: Date.now() },
      error: { name: "MessageAbortedError", data: { message: "aborted" } },
    })
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.escalation).toBe("cancelled")
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("defect 2: a newer unrelated owner turn never discards an unconsumed report", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    yield* h.prompt.prompt({ sessionID: h.owner.id, model: ref, parts: [{ type: "text", text: "unrelated question" }] })
    yield* h.prompt.reconcileReports()
    const open = yield* h.record(reviewer.id)
    expect(open?.consumedAt).toBeUndefined()
    expect(open?.escalatedAt).toBeUndefined()
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect((yield* h.settled()).status).toBe("succeeded")
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("reportTo end to end: the worker's final answer reaches the owner as a consumed report", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    yield* h.prompt.prompt({ sessionID: worker.id, model: ref, parts: [{ type: "text", text: "warm up" }] })
    const assignment = MessageID.ascending()
    yield* h.llm.text(h.marker)
    yield* h.prompt.promptAsync({
      sessionID: worker.id,
      messageID: assignment,
      reportTo: h.owner.id,
      parts: [{ type: "text", text: "review G1f and report" }],
    })
    expect((yield* h.settled(assignment, worker.id)).status).toBe("succeeded")
    const reportID = MessageID.make(`msg_delegation_recovery_run_report_${assignment}`)
    const stamped = yield* pollWithTimeout(
      h.record(worker.id).pipe(Effect.map((r) => (r?.deliveryOutcome === "delivered" ? r : undefined))),
      "reportTo never delivered",
      "15 seconds",
    )
    expect(stamped.reportMessageID).toBe(reportID)
    expect((yield* h.settled(reportID)).status).toBe("succeeded")
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(worker.id))?.consumedAt).toBeNumber()
  }),
)

it.instance("defect 6: an outstanding reportTo record is never overwritten; refusal accepts nothing", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    const first = MessageID.ascending()
    yield* h.llm.hang
    yield* h.prompt.promptAsync({ sessionID: worker.id, messageID: first, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "a" }] })
    // Replaying the same assignment is a no-op, not a refusal.
    yield* h.prompt.promptAsync({ sessionID: worker.id, messageID: first, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "a" }] })
    const second = MessageID.ascending()
    const exit = yield* h.prompt
      .promptAsync({ sessionID: worker.id, messageID: second, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "b" }] })
      .pipe(Effect.exit)
    expect(Exit.isFailure(exit) && JSON.stringify(exit.cause)).toContain("outstanding")
    expect(yield* h.messages(second, worker.id)).toHaveLength(0)
    // Another writer (task/swarm) cannot overwrite the owed receipt either.
    expect(
      yield* h.sessions.stampDelegation({
        sessionID: worker.id,
        record: { version: DELEGATION_RECORD_VERSION, runID: "run_other", parentSessionID: h.owner.id, attempt: 1, phase: "running", startedAt: 1 },
      }),
    ).toBe(false)
    expect((yield* h.record(worker.id))?.runID).toBe(`run_report_${first}`)
    yield* h.prompt.cancel(worker.id)
  }),
)

it.instance("defect 7: a missing or retired reportTo owner is refused visibly; a vanished owner escalates", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    const refuse = (reportTo: SessionID) =>
      Effect.gen(function* () {
        const messageID = MessageID.ascending()
        const exit = yield* h.prompt
          .promptAsync({ sessionID: worker.id, messageID, model: ref, reportTo, parts: [{ type: "text", text: "x" }] })
          .pipe(Effect.exit)
        expect(yield* h.messages(messageID, worker.id)).toHaveLength(0)
        return Exit.isFailure(exit) ? JSON.stringify(exit.cause) : "accepted"
      })
    expect(yield* refuse(SessionID.make("ses_does_not_exist"))).toContain("owner-missing")
    const retired = yield* h.sessions.create({ title: "lead (handed off 10/2)" })
    expect(yield* refuse(retired.id)).toContain("owner-retired")
    // Accepted, then the owner vanished before delivery: recorded, not silent.
    const gone = yield* h.sessions.create({ title: "temp owner" })
    const orphan = yield* h.child({}, gone.id, false)
    yield* h.sessions.remove(gone.id)
    yield* h.recover
    const stamped = yield* h.record(orphan.id)
    expect(stamped?.deliveryOutcome).toBe("failed")
    expect(stamped?.escalation).toBe("owner-missing")
  }),
)

/** Real Claude Code mapper output for one turn answering `parentMessageID` in `sessionID`. */
function nativeTurn(sessionID: SessionID, parentMessageID: MessageID, result?: ClaudeEvent) {
  const ctx = {
    sessionID,
    parentMessageID,
    directory: "/tmp",
    providerID: "claude-code",
    modelID: "sonnet",
    nextPartID: () => PartID.ascending(),
    nextMessageID: () => MessageID.ascending(),
    now: () => Date.now(),
  }
  let { writes, state } = startTurn(initialState(), ctx)
  const events: ClaudeEvent[] = [
    { type: "system", subtype: "init", session_id: "cc-k30", model: "claude-sonnet" },
    { type: "assistant", message: { id: "m1", content: [{ type: "text", text: "Read the report; acting on it." }] } },
    ...(result ? [result] : []),
  ]
  for (const event of events) {
    const next = mapEvent(event, state, ctx)
    writes = [...writes, ...next.writes]
    state = next.state
  }
  const info = writes.findLast((w) => w.kind === "message")!.message
  const parts = [...new Map(writes.flatMap((w) => (w.kind === "part" ? [[w.part.id, w.part] as const] : []))).values()]
  return { writes, info, parts }
}

const persist = (sessions: Session.Interface, writes: SessionWrite[]) =>
  Effect.forEach(
    writes,
    (w) => (w.kind === "message" ? sessions.updateMessage(w.message) : w.kind === "part" ? sessions.updatePart(w.part) : Effect.void),
    { discard: true },
  )

it.instance("fix 1: a native Claude Code answer (real mapper output) consumes the report; no repeat report turn", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    yield* h.interaction("pending")
    const reviewer = yield* h.child()
    yield* h.recover
    // Held admission: recorded with noReply, its command already `succeeded`.
    expect((yield* h.settled()).status).toBe("succeeded")
    // Command success alone is not consumption.
    expect(yield* reportAnswered(h.db, h.owner.id, h.reportID)).toBe(false)
    const placeholder = nativeTurn(h.owner.id, h.reportID)
    const failed = nativeTurn(h.owner.id, h.reportID, { type: "result", subtype: "error_during_execution", is_error: true })
    const foreign = nativeTurn(h.owner.id, MessageID.ascending(), { type: "result", subtype: "success" })
    for (const turn of [placeholder, failed, foreign]) {
      expect(answers(turn.info, h.reportID, turn.parts)).toBe(false)
      yield* persist(h.sessions, turn.writes)
    }
    expect(yield* reportAnswered(h.db, h.owner.id, h.reportID)).toBe(false)
    const ok = nativeTurn(h.owner.id, h.reportID, { type: "result", subtype: "success", total_cost_usd: 0.01 })
    expect(ok.info.finish).toBeUndefined()
    expect(answers(ok.info, h.reportID, ok.parts)).toBe(true)
    yield* persist(h.sessions, ok.writes)
    expect(yield* reportAnswered(h.db, h.owner.id, h.reportID)).toBe(true)
    yield* h.interaction("replied")
    yield* h.age()
    yield* h.prompt.reconcileReports()
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
    expect(yield* h.reportTurns).toBe(0)
    expect((yield* h.command())[0]?.status).toBe("succeeded")
  }),
)

const goal = (h: { db: Database.Interface["db"]; runID: string }, ownerID: SessionID, status: "blocked" | "paused", childID?: SessionID) =>
  Effect.gen(function* () {
    const ctx = yield* InstanceState.context
    const id = `goal_${h.runID}_${status}`
    yield* h.db.insert(OpencodeXProjectTable).values({ id: `oxp_${id}`, project_id: ctx.project.id }).run().pipe(Effect.orDie)
    yield* h.db
      .insert(OpencodeXGoalTable)
      .values({ id, opencodex_project_id: `oxp_${id}`, title: "g", statement: "g", success_criteria_json: [], status, source: "test", owner_session_id: ownerID })
      .run()
      .pipe(Effect.orDie)
    if (childID)
      yield* h.db
        .insert(OpencodeXGoalNodeTable)
        .values({ id: `n_${id}`, goal_id: id, kind: "task", title: "n", brief: "n", status: "running", session_id: childID })
        .run()
        .pipe(Effect.orDie)
  })

it.instance("fix 2: an unrelated blocked goal of the owner does not hold the report; the child's own paused goal does", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    yield* goal(h, h.owner.id, "blocked")
    const reviewer = yield* h.child()
    yield* h.recover
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(1)
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
    // The reporting child's own assignment paused: held, no turn.
    const other = yield* h.child({ runID: `${h.runID}_own` })
    yield* goal(h, h.owner.id, "paused", other.id)
    yield* h.recover
    const held = yield* pollWithTimeout(
      h.record(other.id).pipe(Effect.map((r) => (r?.deliveryOutcome === "delivered" ? r : undefined))),
      "own-goal report never recorded",
    )
    expect(held.escalatedAt).toBeUndefined()
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("fix 3: 64 held receipts never starve an actionable one; the scan is fair across passes", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const ctx = yield* InstanceState.context
    const heldOwner = yield* h.sessions.create({ title: "held coordinator" })
    yield* h.db
      .insert(SessionInteractionTable)
      .values({ id: `int_${heldOwner.id}`, kind: "permission", session_id: heldOwner.id, project_id: ctx.project.id, directory: ctx.directory, state: "pending", request_json: {} })
      .run()
      .pipe(Effect.orDie)
    for (let i = 0; i < 64; i++)
      yield* h.child({ runID: `${h.runID}_held_${i}`, deliveryOutcome: "delivered", reportMessageID: `msg_held_${i}` }, heldOwner.id)
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    yield* h.age()
    yield* h.prompt.reconcileReports()
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.continuationAttempts).toBe(1)
    expect((yield* h.settled()).status).toBe("succeeded")
    expect(yield* h.reportTurns).toBe(2)
  }),
)

it.instance("fix 4: no new native/swarm/task run stamp overwrites an open receipt", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    const next = { version: DELEGATION_RECORD_VERSION, runID: `${h.runID}_next`, parentSessionID: h.owner.id, attempt: 2, phase: "running", startedAt: 3 } as const
    expect(yield* h.sessions.stampDelegation({ sessionID: reviewer.id, record: { ...next, mode: "background", background: true } })).toBe(false)
    expect(yield* h.sessions.stampDelegation({ sessionID: reviewer.id, record: next })).toBe(false)
    const kept = yield* h.record(reviewer.id)
    expect(kept?.runID).toBe(h.runID)
    expect(kept?.reportMessageID).toBe(h.reportID)
    // The receipt still drives its continuation.
    yield* h.age()
    yield* h.prompt.reconcileReports()
    expect((yield* h.settled()).status).toBe("succeeded")
    yield* h.prompt.reconcileReports()
    expect((yield* h.record(reviewer.id))?.consumedAt).toBeNumber()
    // Once consumed, a new run is admitted.
    expect(yield* h.sessions.stampDelegation({ sessionID: reviewer.id, record: next })).toBe(true)
  }),
)

it.instance("fix 5: a follow-up reportTo is admitted once the owner's turn settled; refused inside that still-running turn", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    yield* h.prompt.prompt({ sessionID: worker.id, model: ref, parts: [{ type: "text", text: "warm up" }] })
    const gate = Promise.withResolvers<void>()
    yield* h.llm.text(h.marker)
    yield* h.llm.hold("ack", gate.promise)
    const first = MessageID.ascending()
    yield* h.prompt.promptAsync({ sessionID: worker.id, messageID: first, reportTo: h.owner.id, parts: [{ type: "text", text: "task 1" }] })
    expect((yield* h.settled(first, worker.id)).status).toBe("succeeded")
    const reportID = MessageID.make(`msg_delegation_recovery_run_report_${first}`)
    yield* pollWithTimeout(
      h.command(reportID).pipe(Effect.map(([row]) => (row?.status === "running" ? row : undefined))),
      "owner never started the report turn",
      "15 seconds",
    )
    // Same-turn follow-up: the owner's answering turn is unfinished, so refused.
    const during = MessageID.ascending()
    const exit = yield* h.prompt
      .promptAsync({ sessionID: worker.id, messageID: during, reportTo: h.owner.id, parts: [{ type: "text", text: "task 2" }] })
      .pipe(Effect.exit)
    expect(Exit.isFailure(exit) && JSON.stringify(exit.cause)).toContain("outstanding")
    expect(yield* h.messages(during, worker.id)).toHaveLength(0)
    gate.resolve()
    expect((yield* h.settled(reportID)).status).toBe("succeeded")
    // Settled answer: admitted at once, no reconciler pass in between.
    const after = MessageID.ascending()
    yield* h.prompt.promptAsync({ sessionID: worker.id, messageID: after, reportTo: h.owner.id, parts: [{ type: "text", text: "task 2" }] })
    expect((yield* h.record(worker.id))?.runID).toBe(`run_report_${after}`)
    expect((yield* h.settled(after, worker.id)).status).toBe("succeeded")
  }),
)

it.instance("fix 6: a human stop after acceptance escalates the open receipt as cancelled and never auto-clears", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const reviewer = yield* h.child()
    yield* h.llm.error(400, { error: { message: "usage limit reached" } })
    yield* h.recover
    expect((yield* h.settled()).status).toBe("failed")
    const stop = (cancel: number | null) =>
      h.db
        .update(SessionExecutionTable)
        .set({ state: cancel ? "interrupted" : "idle", cancel_requested_at: cancel })
        .where(eq(SessionExecutionTable.session_id, h.owner.id))
        .run()
        .pipe(Effect.orDie)
    yield* stop(Date.now())
    yield* h.age()
    yield* h.prompt.reconcileReports()
    const stamped = yield* h.record(reviewer.id)
    expect(stamped?.escalation).toBe("cancelled")
    expect(stamped?.reportMessageID).toBe(h.reportID)
    expect(yield* h.messages(h.reportID)).toHaveLength(1)
    yield* stop(null)
    yield* h.prompt.reconcileReports()
    expect(yield* h.reportTurns).toBe(1)
  }),
)

it.instance("fix 7: a failed or crashed reportTo acceptance reports nothing and never locks the worker out", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    // Acceptance fails after the reservation: released, no command, nothing reports.
    const broken = MessageID.ascending()
    const exit = yield* h.prompt
      .promptAsync({
        sessionID: worker.id,
        messageID: broken,
        model: ref,
        reportTo: h.owner.id,
        parts: [{ type: "file", mime: "image/png", url: "data:image/png,not-base64" }],
      })
      .pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    expect((yield* h.record(worker.id))?.runID).toBeUndefined()
    // Crash between reservation and acceptance: an old command-less reservation.
    const crashed = yield* h.sessions.create({ title: "worker 2" })
    const reserve = (sessionID: SessionID, messageID: MessageID, startedAt: number) =>
      h.sessions.stampDelegation({
        sessionID,
        record: { version: DELEGATION_RECORD_VERSION, runID: `run_report_${messageID}`, parentSessionID: h.owner.id, mode: "background", background: true, contract: "report-to", role: "report", childMessageID: messageID, attempt: 1, phase: "running", startedAt },
      })
    const lost = MessageID.ascending()
    yield* reserve(crashed.id, lost, 1)
    // A fresh reservation (acceptance possibly in flight) is left alone.
    const pending = yield* h.sessions.create({ title: "worker 3" })
    const inflight = MessageID.ascending()
    yield* reserve(pending.id, inflight, Date.now())
    yield* h.recover
    expect((yield* h.record(crashed.id))?.runID).toBeUndefined()
    expect((yield* h.record(pending.id))?.runID).toBe(`run_report_${inflight}`)
    for (const id of [broken, lost]) expect(yield* h.messages(MessageID.make(`msg_delegation_recovery_run_report_${id}`))).toHaveLength(0)
    // Neither worker is locked out.
    for (const sessionID of [worker.id, crashed.id]) {
      const messageID = MessageID.ascending()
      yield* h.prompt.promptAsync({ sessionID, messageID, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "go" }] })
      expect((yield* h.record(sessionID))?.runID).toBe(`run_report_${messageID}`)
      yield* h.settled(messageID, sessionID)
    }
  }),
)

/** A process identity whose pid is verifiably gone: a scratch process that already exited. */
const deadOwner = Effect.promise(async () => {
  const scratch = Bun.spawn(["true"])
  await scratch.exited
  return (runID: string) => `local:${scratch.pid}:dead:${runID}`
})

const reservation = (h: { owner: { id: SessionID } }, messageID: MessageID, startedAt: number, ownerID?: string) =>
  ({
    version: DELEGATION_RECORD_VERSION,
    runID: `run_report_${messageID}`,
    parentSessionID: h.owner.id,
    mode: "background",
    background: true,
    contract: "report-to",
    role: "report",
    childMessageID: messageID,
    attempt: 1,
    phase: "running",
    startedAt,
    ...(ownerID ? { ownerID } : {}),
  }) as const

it.instance("fence 1: a live owner's reservation stalled past the grace is never replaced; A completes bound to its owner", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    const { directory } = yield* TestInstance
    // A's acceptance blocks reading a FIFO between reservation and command insert.
    const fifo = path.join(directory, "stall.fifo")
    expect(Bun.spawnSync(["mkfifo", fifo]).exitCode).toBe(0)
    yield* h.llm.text(h.marker)
    const a = MessageID.ascending()
    const runA = `run_report_${a}`
    const fiberA = yield* h.prompt
      .promptAsync({
        sessionID: worker.id,
        messageID: a,
        model: ref,
        reportTo: h.owner.id,
        parts: [
          { type: "text", text: "task A" },
          { type: "file", mime: "image/png", filename: "stall.png", url: pathToFileURL(fifo).href },
        ],
      })
      .pipe(Effect.forkScoped)
    const reserved = yield* pollWithTimeout(
      h.record(worker.id).pipe(Effect.map((r) => (r?.runID === runA ? r : undefined))),
      "A never reserved",
      "15 seconds",
    )
    expect(reserved.ownerID).toStartWith(`local:${process.pid}:`)
    // The stall outlives the 60s grace.
    expect(yield* h.sessions.stampDelegation({ sessionID: worker.id, record: { ...reserved, startedAt: 1 }, expectRunID: runA })).toBe(true)
    yield* h.recover
    expect((yield* h.record(worker.id))?.runID).toBe(runA)
    const b = MessageID.ascending()
    const exit = yield* h.prompt
      .promptAsync({ sessionID: worker.id, messageID: b, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "task B" }] })
      .pipe(Effect.exit)
    expect(Exit.isFailure(exit) && JSON.stringify(exit.cause)).toContain("outstanding")
    expect(yield* h.command(b, worker.id)).toHaveLength(0)
    expect((yield* h.record(worker.id))).toMatchObject({ runID: runA, ownerID: reserved.ownerID, parentSessionID: h.owner.id })
    // A resumes: its acceptance completes and it reports to its own owner.
    const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
    yield* Effect.promise(() => Bun.write(fifo, Buffer.from(png, "base64")))
    yield* Fiber.join(fiberA)
    expect((yield* h.settled(a, worker.id)).status).toBe("succeeded")
    const report = MessageID.make(`msg_delegation_recovery_${runA}`)
    expect((yield* h.settled(report)).status).toBe("succeeded")
    expect((yield* h.record(worker.id))).toMatchObject({ runID: runA, deliveryOutcome: "delivered", reportMessageID: report })
    expect(yield* h.messages(MessageID.make(`msg_delegation_recovery_run_report_${b}`))).toHaveLength(0)
  }),
)

it.instance("fence 2: a dead owner's reservation is reclaimed at once and never locks the worker out", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    const lost = MessageID.ascending()
    const owner = (yield* deadOwner)(`run_report_${lost}`)
    yield* h.sessions.stampDelegation({ sessionID: worker.id, record: reservation(h, lost, Date.now(), owner) })
    yield* h.recover
    expect((yield* h.record(worker.id))?.runID).toBeUndefined()
    const next = MessageID.ascending()
    yield* h.prompt.promptAsync({ sessionID: worker.id, messageID: next, model: ref, reportTo: h.owner.id, parts: [{ type: "text", text: "go" }] })
    expect((yield* h.record(worker.id))?.runID).toBe(`run_report_${next}`)
    yield* h.settled(next, worker.id)
  }),
)

it.instance("fence 3: a command that appears before the release transaction keeps the reservation; owner CAS is exact", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const worker = yield* h.sessions.create({ title: "worker" })
    const messageID = MessageID.ascending()
    const runID = `run_report_${messageID}`
    const owner = (yield* deadOwner)(runID)
    yield* h.sessions.stampDelegation({ sessionID: worker.id, record: reservation(h, messageID, 1, owner) })
    // Wrong owner (or ownerless) never releases an owned reservation.
    expect(yield* h.sessions.releaseReservation({ sessionID: worker.id, runID })).toBe(false)
    expect(yield* h.sessions.releaseReservation({ sessionID: worker.id, runID, ownerID: `${owner}x` })).toBe(false)
    // The accepted command landed after the caller's own check.
    const ctx = yield* InstanceState.context
    yield* h.db
      .insert(SessionCommandTable)
      .values({
        id: `sec_fence_${messageID}`,
        session_id: worker.id,
        message_id: messageID,
        project_id: ctx.project.id,
        directory: ctx.directory,
        status: "queued",
        time_created: Date.now(),
        time_updated: Date.now(),
      })
      .run()
      .pipe(Effect.orDie)
    expect(yield* h.sessions.releaseReservation({ sessionID: worker.id, runID, ownerID: owner })).toBe(false)
    expect((yield* h.record(worker.id))).toMatchObject({ runID, ownerID: owner, phase: "running" })
  }),
)

it.instance("fence 4: a legacy ownerless reservation still ages out after the bounded grace only", () =>
  Effect.gen(function* () {
    const h = yield* boot()
    const fresh = yield* h.sessions.create({ title: "fresh" })
    const stale = yield* h.sessions.create({ title: "stale" })
    const kept = MessageID.ascending()
    const lost = MessageID.ascending()
    yield* h.sessions.stampDelegation({ sessionID: fresh.id, record: reservation(h, kept, Date.now() - RESERVATION_GRACE_MS + 10_000) })
    yield* h.sessions.stampDelegation({ sessionID: stale.id, record: reservation(h, lost, Date.now() - RESERVATION_GRACE_MS - 1) })
    yield* h.recover
    expect((yield* h.record(fresh.id))?.runID).toBe(`run_report_${kept}`)
    expect((yield* h.record(stale.id))?.runID).toBeUndefined()
  }),
)
