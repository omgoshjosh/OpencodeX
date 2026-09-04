import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { Permission } from "@/permission"
import { PermissionID } from "@/permission/schema"
import { Question } from "@/question"
import { QuestionID } from "@/question/schema"
import { SessionRunState } from "@/session/run-state"
import { MessageID, SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { SessionInteractionRecovery } from "@/session/interaction-recovery"
import * as PromptClaim from "@/session/prompt-claim"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { ProviderV2 } from "@opencode-ai/core/provider"
import {
  PermissionTable,
  SessionCommandTable,
  SessionExecutionTable,
  SessionInteractionTable,
  SessionStatusTable,
} from "@opencode-ai/core/session/sql"
import { and, eq, sql } from "drizzle-orm"
import { Context, Duration, Effect, Exit, Fiber, Latch, Layer, Ref, Scope } from "effect"
import { pollWithTimeout, testEffect } from "../lib/effect"

const env = Layer.mergeAll(
  Database.defaultLayer,
  EventV2Bridge.defaultLayer,
  BackgroundJob.defaultLayer,
  CrossSpawnSpawner.defaultLayer,
)
const it = testEffect(env)

const sessionID = SessionID.make("ses_durable")
const output: SessionLegacy.WithParts = {
  info: {
    id: MessageID.make("msg_durable_assistant"),
    sessionID,
    role: "assistant",
    parentID: MessageID.make("msg_durable_user"),
    providerID: ProviderV2.ID.make("test"),
    modelID: ProviderV2.ModelID.make("test"),
    mode: "build",
    agent: "build",
    path: { cwd: ".", root: "." },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, completed: 2 },
    finish: "stop",
  },
  parts: [],
}

function eventRequestID(data: unknown) {
  return typeof data === "object" && data !== null && "requestID" in data ? String(data.requestID) : undefined
}

const buildRunGraph = Effect.fn("DurableExecutionTest.buildRunGraph")(function* () {
  const database = yield* Database.Service
  const events = yield* EventV2Bridge.Service
  const background = yield* BackgroundJob.Service
  const databaseLayer = Layer.succeed(Database.Service, database)
  const eventsLayer = Layer.succeed(EventV2Bridge.Service, events)
  const statusLayer = SessionStatus.layer.pipe(Layer.provide(databaseLayer), Layer.provide(eventsLayer))
  const runLayer = SessionRunState.layer.pipe(
    Layer.provide(Layer.succeed(BackgroundJob.Service, background)),
    Layer.provide(databaseLayer),
    Layer.provide(statusLayer),
  )
  const context = yield* Layer.build(Layer.fresh(Layer.mergeAll(runLayer, statusLayer)))
  return {
    run: Context.get(context, SessionRunState.Service),
    status: Context.get(context, SessionStatus.Service),
  }
})

const buildPermissionGraph = Effect.fn("DurableExecutionTest.buildPermissionGraph")(function* () {
  const database = yield* Database.Service
  const events = yield* EventV2Bridge.Service
  const context = yield* Layer.build(
    Layer.fresh(
      Permission.layer.pipe(
        Layer.provide(Layer.succeed(Database.Service, database)),
        Layer.provide(Layer.succeed(EventV2Bridge.Service, events)),
      ),
    ),
  )
  return Context.get(context, Permission.Service)
})

const buildQuestionGraph = Effect.fn("DurableExecutionTest.buildQuestionGraph")(function* () {
  const database = yield* Database.Service
  const events = yield* EventV2Bridge.Service
  const context = yield* Layer.build(
    Layer.fresh(
      Question.layer.pipe(
        Layer.provide(Layer.succeed(Database.Service, database)),
        Layer.provide(Layer.succeed(EventV2Bridge.Service, events)),
      ),
    ),
  )
  return Context.get(context, Question.Service)
})

const buildPromptClaim = Effect.fn("DurableExecutionTest.buildPromptClaim")(function* (
  loop = Effect.succeed(output),
  recoveryInterval?: Duration.Input,
  beforeExecutionAdmission?: PromptClaim.Deps["beforeExecutionAdmission"],
) {
  const database = yield* Database.Service
  const events = yield* EventV2Bridge.Service
  const scope = yield* Scope.Scope
  return yield* PromptClaim.make({
    database,
    events,
    scope,
    loop: () => loop,
    recoveryInterval,
    beforeExecutionAdmission,
  })
})

const insertCommand = Effect.fn("DurableExecutionTest.insertCommand")(function* (input: {
  id: string
  status?: "queued" | "running" | "cancelled"
  owner?: string
  leaseExpiresAt?: number
  generation?: number
  sessionID?: SessionID
  createdAt?: number
}) {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  const now = input.createdAt ?? Date.now()
  yield* db
    .insert(SessionCommandTable)
    .values({
      id: input.id,
      session_id: input.sessionID ?? sessionID,
      message_id: MessageID.make(`msg_${input.id}`),
      project_id: "prj_test",
      directory: ctx.directory,
      status: input.status ?? "queued",
      owner_id: input.owner,
      lease_expires_at: input.leaseExpiresAt,
      claim_generation: input.generation ?? 0,
      time_created: now,
      time_updated: now,
    })
    .run()
    .pipe(Effect.orDie)
})

it.instance("recovers only orphaned interactions and emits one terminal event", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2Bridge.Service
    const ctx = yield* InstanceState.context
    const now = Date.now()
    const crashed = SessionID.make("ses_recovery_crashed")
    const leased = SessionID.make("ses_recovery_leased")
    const deadOwner = SessionID.make("ses_recovery_dead_owner")
    const standalone = SessionID.make("ses_recovery_standalone")
    const rejected = yield* Ref.make(0)
    const unsubscribe = yield* events.listen((event) =>
      event.type === Question.Event.Rejected.type || event.type === Permission.Event.Replied.type
        ? Ref.update(rejected, (count) => count + 1)
        : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* db
      .insert(SessionExecutionTable)
      .values([
        {
          session_id: crashed,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "interrupted",
          generation: 3,
          completed_at: now,
          time_created: now,
          time_updated: now,
        },
        {
          session_id: leased,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "running",
          owner_id: "live:owner",
          generation: 4,
          lease_expires_at: now + 60_000,
          time_created: now,
          time_updated: now,
        },
        {
          session_id: deadOwner,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "running",
          owner_id: "local:999999:dead:recovery",
          generation: 5,
          lease_expires_at: now + 60_000,
          time_created: now,
          time_updated: now,
        },
      ])
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionInteractionTable)
      .values([
        {
          id: "que_recovery_crashed",
          kind: "question",
          session_id: crashed,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending",
          request_json: {
            id: "que_recovery_crashed",
            sessionID: crashed,
            questions: [],
            executionGeneration: 3,
          },
          time_created: now,
          time_updated: now,
        },
        {
          id: "per_recovery_crashed",
          kind: "permission",
          session_id: crashed,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending",
          request_json: {
            id: "per_recovery_crashed",
            sessionID: crashed,
            permission: "bash",
            patterns: [],
            metadata: {},
            always: [],
            executionGeneration: 3,
          },
          time_created: now,
          time_updated: now,
        },
        {
          id: "que_recovery_leased",
          kind: "question",
          session_id: leased,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending",
          request_json: {
            id: "que_recovery_leased",
            sessionID: leased,
            questions: [],
            executionGeneration: 4,
          },
          time_created: now,
          time_updated: now,
        },
        {
          id: "que_recovery_dead_owner",
          kind: "question",
          session_id: deadOwner,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending",
          request_json: {
            id: "que_recovery_dead_owner",
            sessionID: deadOwner,
            questions: [],
            executionGeneration: 5,
          },
          time_created: now,
          time_updated: now,
        },
        {
          id: "que_recovery_legacy",
          kind: "question",
          session_id: standalone,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending",
          request_json: { id: "que_recovery_legacy", sessionID: standalone, questions: [] },
          time_created: now,
          time_updated: now,
        },
        ...["null", "array", "primitive", "malformed"].map((kind) => ({
          id: `que_recovery_${kind}`,
          kind: "question" as const,
          session_id: standalone,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "pending" as const,
          request_json: {},
          time_created: now,
          time_updated: now,
        })),
      ])
      .run()
      .pipe(Effect.orDie)
    yield* Effect.all(
      [
        { id: "que_recovery_null", request: sql`'null'` },
        { id: "que_recovery_array", request: sql`'[]'` },
        { id: "que_recovery_primitive", request: sql`'"legacy"'` },
        { id: "que_recovery_malformed", request: sql`'{"executionGeneration":"old","tool":[]}'` },
      ].map((item) =>
        db
          .update(SessionInteractionTable)
          .set({ request_json: item.request })
          .where(eq(SessionInteractionTable.id, item.id))
          .run()
          .pipe(Effect.orDie),
      ),
      { concurrency: "unbounded" },
    )
    yield* SessionInteractionRecovery.recover()
    const rows = yield* db.select().from(SessionInteractionTable).all().pipe(Effect.orDie)
    expect(rows.filter((row) => row.id.includes("crashed") || row.id.includes("dead_owner")).map((row) => row.state)).toEqual([
      "rejected",
      "rejected",
      "rejected",
    ])
    expect(rows.filter((row) => row.id.includes("leased") || row.id.includes("legacy") || row.id.includes("null") || row.id.includes("array") || row.id.includes("primitive") || row.id.includes("malformed")).every((row) => row.state === "pending")).toBe(true)
    expect(rows.find((row) => row.id === "per_recovery_crashed")?.response_json).toEqual({ reply: "reject" })
    expect(rows.filter((row) => row.id.includes("crashed") || row.id.includes("dead_owner")).every((row) => row.responded_at && row.time_updated >= now)).toBe(true)
    expect(yield* Ref.get(rejected)).toBe(3)
  }),
)

it.instance("allows one lease winner and makes a foreign caller join", () =>
  Effect.gen(function* () {
    const first = yield* buildRunGraph()
    const second = yield* buildRunGraph()
    const started = yield* Ref.make(0)
    const release = yield* Latch.make()
    const work = Ref.update(started, (value) => value + 1).pipe(Effect.andThen(release.await), Effect.as(output))

    const a = yield* first.run.ensureRunning(sessionID, Effect.succeed(output), work).pipe(Effect.forkScoped)
    const b = yield* second.run.ensureRunning(sessionID, Effect.succeed(output), work).pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      Ref.get(started).pipe(Effect.map((count) => (count === 1 ? true : undefined))),
      "multiple session executions started",
    )
    yield* release.open

    expect(yield* Effect.all([Fiber.join(a), Fiber.join(b)])).toHaveLength(2)
    expect(yield* Ref.get(started)).toBe(1)
  }),
)

it.instance("rejects stale status writes after a newer execution generation claims", () =>
  Effect.gen(function* () {
    const first = yield* buildRunGraph()
    const second = yield* buildRunGraph()
    const release = yield* Latch.make()
    const firstFiber = yield* first.run
      .ensureRunning(sessionID, Effect.succeed(output), Effect.never)
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      first.status.get(sessionID).pipe(Effect.map((status) => (status.type === "busy" ? true : undefined))),
      "first execution never became busy",
    )

    const { db } = yield* Database.Service
    yield* db
      .update(SessionExecutionTable)
      .set({ lease_expires_at: Date.now() - 1 })
      .where(eq(SessionExecutionTable.session_id, sessionID))
      .run()
      .pipe(Effect.orDie)
    const secondFiber = yield* second.run
      .ensureRunning(sessionID, Effect.succeed(output), release.await.pipe(Effect.as(output)))
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.generation === 2 ? true : undefined)),
        ),
      "new execution generation never claimed",
    )
    yield* Fiber.join(firstFiber)

    expect(yield* second.status.get(sessionID)).toEqual({ type: "busy" })
    expect(yield* second.status.setForGeneration(sessionID, 1, { type: "idle" })).toBe(false)
    expect(yield* second.status.get(sessionID)).toEqual({ type: "busy" })
    yield* release.open
    yield* Fiber.join(secondFiber)
  }),
)

it.instance("rejects active status writes after their execution generation releases", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    yield* graph.run.ensureRunning(sessionID, Effect.succeed(output), Effect.succeed(output))

    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
    expect(yield* graph.status.setForGeneration(sessionID, 1, { type: "busy" })).toBe(false)
    expect(
      yield* graph.status.setForGeneration(sessionID, 1, {
        type: "retry",
        attempt: 1,
        message: "stale",
        next: Date.now(),
      }),
    ).toBe(false)
    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
  }),
)

it.instance("persists abort across graphs and interrupts the owner", () =>
  Effect.gen(function* () {
    const owner = yield* buildRunGraph()
    const remote = yield* buildRunGraph()
    const fiber = yield* owner.run
      .ensureRunning(sessionID, Effect.succeed(output), Effect.never)
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      owner.status.get(sessionID).pipe(Effect.map((status) => (status.type === "busy" ? true : undefined))),
      "session never became busy",
    )

    yield* remote.run.cancel(sessionID)
    expect(Exit.isSuccess(yield* Fiber.await(fiber))).toBe(true)

    const { db } = yield* Database.Service
    const execution = yield* db
      .select()
      .from(SessionExecutionTable)
      .where(eq(SessionExecutionTable.session_id, sessionID))
      .get()
      .pipe(Effect.orDie)
    expect(execution).toMatchObject({ state: "interrupted", owner_id: null })
    expect(yield* remote.status.get(sessionID)).toEqual({ type: "idle" })
  }),
)

it.instance("interrupt stops a run owned by another graph and keeps queued commands", () =>
  Effect.gen(function* () {
    // A direct ("immediate") prompt pivots the session: the current turn must
    // stop even when this process does not own the run, and the interrupting
    // prompt - already recorded as a queued command - must stay queued so it
    // launches next. `cancel` proves the durable path; `interrupt` must use it
    // too instead of only checking the local in-memory runner.
    const owner = yield* buildRunGraph()
    const remote = yield* buildRunGraph()
    const fiber = yield* owner.run
      .ensureRunning(sessionID, Effect.succeed(output), Effect.never)
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      owner.status.get(sessionID).pipe(Effect.map((status) => (status.type === "busy" ? true : undefined))),
      "session never became busy",
    )

    const { db } = yield* Database.Service
    const now = Date.now()
    yield* db
      .insert(SessionCommandTable)
      .values({
        id: "sec_direct_pivot",
        session_id: sessionID,
        message_id: MessageID.make("msg_direct_pivot"),
        project_id: "prj_test",
        directory: ".",
        status: "queued",
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)

    yield* remote.run.interrupt(sessionID)
    expect(Exit.isSuccess(yield* Fiber.await(fiber))).toBe(true)

    const command = yield* db
      .select({ status: SessionCommandTable.status })
      .from(SessionCommandTable)
      .where(eq(SessionCommandTable.id, "sec_direct_pivot"))
      .get()
      .pipe(Effect.orDie)
    expect(command?.status).toBe("queued")
  }),
)

it.instance("recovers a dead owner and stale busy status", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* db
      .insert(SessionExecutionTable)
      .values({
        session_id: sessionID,
        project_id: "project",
        directory: process.cwd(),
        state: "running",
        owner_id: "local:999999:dead:graph",
        generation: 1,
        lease_expires_at: now + 60_000,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionStatusTable)
      .values({
        session_id: sessionID,
        project_id: "project",
        directory: process.cwd(),
        status: { type: "busy" },
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)

    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
    expect(
      yield* db
        .select({ state: SessionExecutionTable.state })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ state: "interrupted" })
  }),
)

it.instance("does not reclaim a running or unknown owner before its lease expires", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* insertCommand({
      id: "sec_dead_command",
      status: "running",
      owner: "unknown:owner",
      leaseExpiresAt: now + 60_000,
      generation: 1,
    })
    expect(yield* claim.claimCommandTurn("sec_dead_command")).toEqual({ state: "waiting" })
    yield* db
      .insert(SessionExecutionTable)
      .values({
        session_id: sessionID,
        project_id: "prj_test",
        directory: ".",
        state: "running",
        owner_id: "unknown:execution",
        generation: 1,
        lease_expires_at: now + 60_000,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)
    expect(yield* claim.waitForExecutionTurn("sec_dead_command", sessionID)).toBe(false)
    yield* db
      .update(SessionCommandTable)
      .set({ lease_expires_at: now - 1 })
      .where(eq(SessionCommandTable.id, "sec_dead_command"))
      .run()
      .pipe(Effect.orDie)
    expect(yield* claim.claimCommandTurn("sec_dead_command")).toMatchObject({ state: "waiting" })
    yield* db
      .update(SessionExecutionTable)
      .set({ lease_expires_at: now - 1 })
      .where(eq(SessionExecutionTable.session_id, sessionID))
      .run()
      .pipe(Effect.orDie)
    expect(yield* claim.claimCommandTurn("sec_dead_command")).toMatchObject({ state: "ready" })
  }),
)

it.instance("only one recoverer reclaims a dead command lease", () =>
  Effect.gen(function* () {
    const first = yield* buildPromptClaim()
    const second = yield* buildPromptClaim()
    yield* insertCommand({
      id: "sec_dead_race",
      status: "running",
      owner: "unknown:race",
      leaseExpiresAt: Date.now() - 1,
      generation: 1,
    })

    const results = yield* Effect.all(
      [first.claimCommandTurn("sec_dead_race"), second.claimCommandTurn("sec_dead_race")],
      {
        concurrency: "unbounded",
      },
    )
    expect(results.filter((result) => result.state === "ready")).toHaveLength(1)
    expect(results.filter((result) => result.state === "waiting")).toHaveLength(1)
  }),
)

it.instance("sweeps a queued command without status after bootstrap", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_missing_status" })

    yield* claim.recover()
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_missing_status"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "succeeded" ? true : undefined)),
        ),
      "queued command was not recovered",
    )
  }),
)

it.instance("starts one per-instance recovery timer under instance context", () =>
  Effect.gen(function* () {
    const launches = yield* Ref.make(0)
    const claim = yield* buildPromptClaim(
      Ref.update(launches, (count) => count + 1).pipe(Effect.as(output)),
      "1 millis",
    )
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_timer_context" })

    yield* Effect.all([claim.start(), claim.start()], { concurrency: "unbounded" })
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_timer_context"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "succeeded" ? true : undefined)),
        ),
      "per-instance recovery timer did not run",
    )
    expect(yield* Ref.get(launches)).toBe(1)
  }),
)

it.instance("stops the per-instance recovery timer when its scope closes", () =>
  Effect.gen(function* () {
    const recoveryScope = yield* Scope.make()
    const claim = yield* buildPromptClaim(Effect.succeed(output), "1 millis").pipe(
      Effect.provideService(Scope.Scope, recoveryScope),
    )
    const { db } = yield* Database.Service
    yield* claim.start()
    yield* Scope.close(recoveryScope, Exit.void)
    yield* insertCommand({ id: "sec_timer_disposed" })
    yield* Effect.sleep("20 millis")

    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_timer_disposed"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "queued" })
  }),
)

it.instance("does not starve a later session behind a backlog larger than one batch", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    const now = Date.now()
    const commands = Array.from({ length: 33 }, (_, index) => ({
      id: `sec_fifo_${index.toString().padStart(2, "0")}`,
      sessionID: SessionID.make("ses_fifo_backlog"),
      createdAt: now + index,
    }))
    yield* Effect.forEach(commands, insertCommand, { discard: true })
    yield* insertCommand({ id: "sec_fifo_later", sessionID: SessionID.make("ses_fifo_later"), createdAt: now + 34 })

    yield* claim.recover()
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_fifo_later"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "succeeded" ? true : undefined)),
        ),
      "later session was starved by a backlog larger than one batch",
    )
    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_fifo_00"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "succeeded" })
    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_fifo_01"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "queued" })
  }),
)

it.instance("concurrent sweepers launch one durable command exactly once", () =>
  Effect.gen(function* () {
    const launches = yield* Ref.make(0)
    const loop = Ref.update(launches, (count) => count + 1).pipe(Effect.as(output))
    const first = yield* buildPromptClaim(loop)
    const second = yield* buildPromptClaim(loop)
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_sweep_race" })

    yield* Effect.all([first.recover(), second.recover()], { concurrency: "unbounded" })
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_sweep_race"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "succeeded" ? true : undefined)),
        ),
      "concurrent sweepers did not settle command",
    )
    expect(yield* Ref.get(launches)).toBe(1)
  }),
)

it.instance("sweep never revives a cancelled command", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_sweep_cancelled", status: "cancelled" })

    yield* claim.recover()
    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_sweep_cancelled"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "cancelled" })
  }),
)

it.instance("periodic recovery does not reclaim a running command before lease expiry", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    yield* insertCommand({
      id: "sec_periodic_running",
      status: "running",
      owner: "unknown:owner",
      leaseExpiresAt: Date.now() + 60_000,
    })

    yield* claim.recover()
    expect(
      yield* db
        .select({ status: SessionCommandTable.status, owner: SessionCommandTable.owner_id })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_periodic_running"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "running", owner: "unknown:owner" })
  }),
)

it.instance("periodic recovery reclaims an expired running command", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim()
    const { db } = yield* Database.Service
    yield* insertCommand({
      id: "sec_periodic_expired",
      status: "running",
      owner: "unknown:owner",
      leaseExpiresAt: Date.now() - 1,
    })

    yield* claim.recover()
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_periodic_expired"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "succeeded" ? true : undefined)),
        ),
      "expired running command was not recovered",
    )
  }),
)

it.instance("requeues its own command claim when execution admission loses a race", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const ctx = yield* InstanceState.context
    const claim = yield* buildPromptClaim(Effect.succeed(output), undefined, ({ sessionID }) =>
      db
        .insert(SessionExecutionTable)
        .values({
          session_id: sessionID,
          project_id: "prj_test",
          directory: ctx.directory,
          state: "running",
          owner_id: "foreign:execution",
          generation: 1,
          lease_expires_at: Date.now() + 60_000,
          time_created: Date.now(),
          time_updated: Date.now(),
        })
        .run()
        .pipe(Effect.orDie),
    )
    yield* insertCommand({ id: "sec_admission_race" })

    yield* claim.executeCommand("sec_admission_race")
    expect(
      yield* db
        .select({ status: SessionCommandTable.status, owner: SessionCommandTable.owner_id })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_admission_race"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "queued", owner: null })
  }),
)

it.instance("admission requeue never overwrites a newer command claimant", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const claim = yield* buildPromptClaim(Effect.succeed(output), undefined, ({ commandID }) =>
      db
        .update(SessionCommandTable)
        .set({ owner_id: "foreign:newer", claim_generation: 2, time_updated: Date.now() })
        .where(eq(SessionCommandTable.id, commandID))
        .run()
        .pipe(Effect.orDie),
    )
    yield* insertCommand({ id: "sec_admission_newer" })

    yield* claim.executeCommand("sec_admission_newer")
    expect(
      yield* db
        .select({
          status: SessionCommandTable.status,
          owner: SessionCommandTable.owner_id,
          generation: SessionCommandTable.claim_generation,
        })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_admission_newer"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "running", owner: "foreign:newer", generation: 2 })
  }),
)

it.instance("sweep restores an interrupted execution with a new generation", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const claim = yield* buildPromptClaim(
      graph.run.ensureRunning(sessionID, Effect.succeed(output), Effect.succeed(output)),
    )
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* insertCommand({ id: "sec_interrupted_execution" })
    yield* db
      .insert(SessionExecutionTable)
      .values({
        session_id: sessionID,
        project_id: "prj_test",
        directory: (yield* InstanceState.context).directory,
        state: "interrupted",
        generation: 7,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)

    yield* claim.recover()
    yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.generation === 8 ? true : undefined)),
        ),
      "interrupted execution was not reclaimed with a new generation",
    )
  }),
)

it.instance("scope interruption requeues its command but never revives cancellation", () =>
  Effect.gen(function* () {
    const claim = yield* buildPromptClaim(Effect.never)
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_interrupted" })
    const interrupted = yield* claim.executeCommand("sec_interrupted").pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_interrupted"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "running" ? true : undefined)),
        ),
      "command was not claimed",
    )
    yield* Fiber.interrupt(interrupted)
    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_interrupted"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "queued" })
    yield* db
      .update(SessionCommandTable)
      .set({ status: "cancelled", completed_at: Date.now(), time_updated: Date.now() })
      .where(eq(SessionCommandTable.id, "sec_interrupted"))
      .run()
      .pipe(Effect.orDie)

    yield* insertCommand({ id: "sec_cancelled" })
    const cancelled = yield* claim.executeCommand("sec_cancelled").pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_cancelled"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "running" ? true : undefined)),
        ),
      "cancelled command was not claimed",
    )
    yield* db
      .update(SessionCommandTable)
      .set({ status: "cancelled", owner_id: null, lease_expires_at: null, time_updated: Date.now() })
      .where(eq(SessionCommandTable.id, "sec_cancelled"))
      .run()
      .pipe(Effect.orDie)
    yield* Fiber.interrupt(cancelled)
    expect(
      yield* db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_cancelled"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "cancelled" })
  }),
)

it.instance("a stale command owner cannot settle a reclaimed command", () =>
  Effect.gen(function* () {
    const release = yield* Latch.make()
    const claim = yield* buildPromptClaim(release.await.pipe(Effect.as(output)))
    const { db } = yield* Database.Service
    yield* insertCommand({ id: "sec_stale_settle" })
    const fiber = yield* claim.executeCommand("sec_stale_settle").pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      db
        .select({ status: SessionCommandTable.status })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_stale_settle"))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row?.status === "running" ? true : undefined)),
        ),
      "stale command was not claimed",
    )
    yield* db
      .update(SessionCommandTable)
      .set({
        owner_id: "foreign:reclaimed",
        claim_generation: 2,
        lease_expires_at: Date.now() + 60_000,
        time_updated: Date.now(),
      })
      .where(eq(SessionCommandTable.id, "sec_stale_settle"))
      .run()
      .pipe(Effect.orDie)
    yield* release.open
    yield* Fiber.join(fiber)
    expect(
      yield* db
        .select({
          status: SessionCommandTable.status,
          owner: SessionCommandTable.owner_id,
          generation: SessionCommandTable.claim_generation,
        })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, "sec_stale_settle"))
        .get()
        .pipe(Effect.orDie),
    ).toEqual({ status: "running", owner: "foreign:reclaimed", generation: 2 })
  }),
)

it.instance("shares permission requests, replies once, and persists always grants", () =>
  Effect.gen(function* () {
    const first = yield* buildPermissionGraph()
    const second = yield* buildPermissionGraph()
    const events = yield* EventV2Bridge.Service
    const replies = yield* Ref.make(0)
    const requestID = PermissionID.make("per_durable")
    const unsubscribe = yield* events.listen((event) =>
      event.type === Permission.Event.Replied.type && eventRequestID(event.data) === String(requestID)
        ? Ref.update(replies, (count) => count + 1)
        : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    const ask = yield* first
      .ask({
        id: requestID,
        sessionID,
        permission: "bash",
        patterns: ["ls"],
        metadata: {},
        always: ["ls"],
        ruleset: [],
      })
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      second.list().pipe(Effect.map((items) => (items.some((item) => item.id === requestID) ? true : undefined))),
      "permission was not visible across graphs",
    )

    yield* Effect.all([first.reply({ requestID, reply: "always" }), second.reply({ requestID, reply: "reject" })])
    yield* Fiber.await(ask)
    expect(yield* Ref.get(replies)).toBe(1)

    const { db } = yield* Database.Service
    const interaction = yield* db
      .select()
      .from(SessionInteractionTable)
      .where(eq(SessionInteractionTable.id, String(requestID)))
      .get()
      .pipe(Effect.orDie)
    expect(interaction?.state).toMatch(/replied|rejected/)
    expect(yield* second.list()).toHaveLength(0)

    const alwaysID = PermissionID.make("per_durable_always")
    const alwaysAsk = yield* first
      .ask({
        id: alwaysID,
        sessionID,
        permission: "bash",
        patterns: ["pwd"],
        metadata: {},
        always: ["pwd"],
        ruleset: [],
      })
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      second.list().pipe(Effect.map((items) => (items.some((item) => item.id === alwaysID) ? true : undefined))),
      "always permission was not visible across graphs",
    )
    yield* second.reply({ requestID: alwaysID, reply: "always" })
    yield* Fiber.join(alwaysAsk)

    const project = yield* db
      .select({ projectID: SessionInteractionTable.project_id })
      .from(SessionInteractionTable)
      .where(eq(SessionInteractionTable.id, String(alwaysID)))
      .get()
      .pipe(Effect.orDie)
    const grants = project
      ? yield* db
          .select({ data: PermissionTable.data })
          .from(PermissionTable)
          .where(eq(PermissionTable.project_id, project.projectID))
          .get()
          .pipe(Effect.orDie)
      : undefined
    expect(grants?.data).toContainEqual({ permission: "bash", pattern: "pwd", action: "allow" })
    expect(
      yield* second.ask({
        sessionID,
        permission: "bash",
        patterns: ["pwd"],
        metadata: {},
        always: [],
        ruleset: [],
      }),
    ).toBeUndefined()
  }),
)

it.instance("shares questions and settles the owning Deferred from a remote reply", () =>
  Effect.gen(function* () {
    const first = yield* buildQuestionGraph()
    const second = yield* buildQuestionGraph()
    const events = yield* EventV2Bridge.Service
    const replies = yield* Ref.make(0)
    const ask = yield* first
      .ask({
        sessionID,
        questions: [
          {
            question: "Continue?",
            header: "Continue",
            options: [{ label: "Yes", description: "Continue" }],
          },
        ],
      })
      .pipe(Effect.forkScoped)
    const pending = yield* pollWithTimeout(
      second.list().pipe(Effect.map((items) => items[0])),
      "question was not visible across graphs",
    )
    const unsubscribe = yield* events.listen((event) =>
      (event.type === Question.Event.Replied.type || event.type === Question.Event.Rejected.type) &&
      eventRequestID(event.data) === String(pending.id)
        ? Ref.update(replies, (count) => count + 1)
        : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* Effect.all([first.reply({ requestID: pending.id, answers: [["Yes"]] }), second.reject(pending.id)])
    const exit = yield* Fiber.await(ask)
    expect(Exit.isSuccess(exit) ? exit.value : exit.cause).toBeDefined()
    expect(yield* Ref.get(replies)).toBe(1)
    expect(yield* second.list()).toHaveLength(0)

    const { db } = yield* Database.Service
    expect(
      yield* db
        .select({ state: SessionInteractionTable.state })
        .from(SessionInteractionTable)
        .where(
          and(
            eq(SessionInteractionTable.id, String(QuestionID.make(String(pending.id)))),
            eq(SessionInteractionTable.kind, "question"),
          ),
        )
        .get()
        .pipe(Effect.orDie),
    ).toMatchObject({ state: expect.stringMatching(/replied|rejected/) })
  }),
)
