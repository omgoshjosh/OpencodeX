import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { PermissionID } from "@/permission/schema"
import { Question } from "@/question"
import { QuestionID } from "@/question/schema"
import { SessionRunState } from "@/session/run-state"
import { MessageID, SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { OpencodeXJob } from "@/opencodex/job"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
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
import { and, eq } from "drizzle-orm"
import { Context, Effect, Exit, Fiber, Latch, Layer, Ref } from "effect"
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
  const jobsLayer = OpencodeXJob.layer.pipe(Layer.provide(databaseLayer), Layer.provide(eventsLayer))
  const runLayer = SessionRunState.layer.pipe(
    Layer.provide(Layer.succeed(BackgroundJob.Service, background)),
    Layer.provide(databaseLayer),
    Layer.provide(statusLayer),
  )
  const context = yield* Layer.build(Layer.fresh(Layer.mergeAll(runLayer, statusLayer, jobsLayer)))
  return {
    run: Context.get(context, SessionRunState.Service),
    status: Context.get(context, SessionStatus.Service),
    jobs: Context.get(context, OpencodeXJob.Service),
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

    expect(yield* second.status.get(sessionID)).toMatchObject({ type: "busy" })
    expect(yield* second.status.setForGeneration(sessionID, 1, { type: "idle" })).toBe(false)
    expect(yield* second.status.get(sessionID)).toMatchObject({ type: "busy" })
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

it.instance("carries background delegations across status changes and lists an idle parent", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const task = { sessionID: "ses_child", role: "QA", title: "Task QA: run the suite", since: 1 }

    // Published while the parent is idle (the common case: its turn ended
    // right after starting the delegation), so no execution gate applies.
    expect(yield* graph.status.backgroundStart(sessionID, task)).toBe(true)
    expect(yield* graph.status.get(sessionID)).toMatchObject({
      type: "idle",
      background: { running: 1, jobs: [{ ...task, owner: expect.stringMatching(/^local:\d+:/) }] },
    })
    expect(Array.from((yield* graph.status.list()).keys())).toEqual([sessionID])

    // A turn that starts, and one that recovery forces back to idle because
    // its owner died (no execution lease here), both keep the jobs.
    // Re-publishing the same child does not duplicate it.
    yield* graph.status.set(sessionID, { type: "busy" })
    const { db } = yield* Database.Service
    const stored = yield* db
      .select({ status: SessionStatusTable.status })
      .from(SessionStatusTable)
      .where(eq(SessionStatusTable.session_id, sessionID))
      .get()
      .pipe(Effect.orDie)
    expect(stored?.status).toMatchObject({ type: "busy", background: { running: 1 } })
    expect(yield* graph.status.get(sessionID)).toMatchObject({ type: "idle", background: { running: 1 } })
    expect(yield* graph.status.backgroundStart(sessionID, { ...task, title: "Task QA: rerun" })).toBe(true)
    expect(yield* graph.status.get(sessionID)).toMatchObject({
      type: "idle",
      background: { running: 1, jobs: [{ sessionID: "ses_child", title: "Task QA: rerun" }] },
    })

    // Ending an unknown child is a no-op; ending the last one clears the field.
    expect(yield* graph.status.backgroundEnd(sessionID, "ses_other")).toBe(false)
    expect(yield* graph.status.backgroundEnd(sessionID, "ses_child")).toBe(true)
    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
    expect((yield* graph.status.list()).size).toBe(0)

    // A job published by a process that no longer exists is pruned on read:
    // jobs live in memory, so the row it left behind is a phantom.
    const dead = { ...task, owner: "local:2147483000:gone:background:ses_child" }
    yield* db
      .update(SessionStatusTable)
      .set({ status: { type: "idle", background: { running: 1, jobs: [dead] } } })
      .where(eq(SessionStatusTable.session_id, sessionID))
      .run()
      .pipe(Effect.orDie)
    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
    expect((yield* graph.status.list()).size).toBe(0)
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

it.instance("clears a hollow busy status without an execution lease", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* db
      .insert(SessionStatusTable)
      .values({
        session_id: sessionID,
        project_id: "project",
        directory: process.cwd(),
        status: { type: "busy", since: now - 60_000, lastActivityAt: now - 60_000 },
        time_created: now - 60_000,
        time_updated: now - 60_000,
      })
      .run()
      .pipe(Effect.orDie)

    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
  }),
)

it.instance("publishes activity, tools, and pending wakes for the owning generation", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const release = yield* Latch.make()
    const fiber = yield* graph.run
      .ensureRunning(sessionID, Effect.succeed(output), release.await.pipe(Effect.as(output)))
      .pipe(Effect.forkScoped)
    const { db } = yield* Database.Service
    const generation = yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row ? row.generation : undefined)),
        ),
      "execution generation never claimed",
    )
    const generationContext = { sessionID, generation }
    const busy = yield* graph.status.get(sessionID)
    expect(busy).toMatchObject({ type: "busy" })
    if (busy.type !== "busy") throw new Error("session never became busy")
    expect(busy.since).toBeNumber()
    expect(busy.lastActivityAt).toBeNumber()

    yield* graph.status
      .toolStart(sessionID, "bash")
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    expect(
      yield* graph.status
        .setPendingWake(sessionID, { jobID: "missing", reason: "follow-up" })
        .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext)),
    ).toBe(false)
    const job = yield* graph.jobs.create({ kind: "test.wake", sessionID, timeoutAt: Date.now() + 60_000 })
    yield* graph.status
      .setPendingWake(sessionID, { jobID: job.id, reason: "follow-up" })
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    expect(yield* graph.status.get(sessionID)).toMatchObject({
      type: "busy",
      runningTool: { title: "bash" },
      pendingWake: { reason: "follow-up" },
    })
    yield* graph.status
      .toolEnd(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    expect(yield* graph.status.get(sessionID)).toMatchObject({ type: "busy", pendingWake: { reason: "follow-up" } })

    yield* release.open
    yield* Fiber.join(fiber)
    yield* graph.status.setPendingWake(sessionID, { jobID: job.id })
    expect(yield* graph.status.get(sessionID)).toEqual({
      type: "idle",
      pendingWake: { at: expect.any(Number), jobID: job.id },
    })
  }),
)

it.instance("coalesces stream activity into one latest timestamp update", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const release = yield* Latch.make()
    const fiber = yield* graph.run
      .ensureRunning(sessionID, Effect.succeed(output), release.await.pipe(Effect.as(output)))
      .pipe(Effect.forkScoped)
    const { db } = yield* Database.Service
    const generation = yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row ? row.generation : undefined)),
        ),
      "execution generation never claimed",
    )
    const events = yield* EventV2Bridge.Service
    const updates = yield* Ref.make(0)
    const unsubscribe = yield* events.listen((event) =>
      event.type === SessionStatus.Event.Status.type &&
      (event.data as typeof SessionStatus.Event.Status.data.Type).sessionID === sessionID
        ? Ref.update(updates, (count) => count + 1)
        : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    const observedAt = Date.now()
    const generationContext = { sessionID, generation }
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, generationContext))
    yield* Effect.sleep(300)

    expect(yield* Ref.get(updates)).toBe(1)
    const busy = yield* graph.status.get(sessionID)
    if (busy.type !== "busy") throw new Error("session stopped before activity flush")
    expect(busy.lastActivityAt).toBeGreaterThanOrEqual(observedAt)
    yield* release.open
    yield* Fiber.join(fiber)
  }),
)

it.instance("flushes pending activity before terminal idle and cancels its delayed update", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const release = yield* Latch.make()
    const fiber = yield* graph.run
      .ensureRunning(sessionID, Effect.succeed(output), release.await.pipe(Effect.as(output)))
      .pipe(Effect.forkScoped)
    const { db } = yield* Database.Service
    const generation = yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row ? row.generation : undefined)),
        ),
      "execution generation never claimed",
    )
    const updates = yield* Ref.make({ busy: 0, lastActivityAt: 0 })
    const events = yield* EventV2Bridge.Service
    const unsubscribe = yield* events.listen((event) => {
      if (event.type !== SessionStatus.Event.Status.type) return Effect.void
      const data = event.data as typeof SessionStatus.Event.Status.data.Type
      if (data.sessionID !== sessionID || data.status.type !== "busy") return Effect.void
      const lastActivityAt = data.status.lastActivityAt
      return Ref.update(updates, (current) => ({
        busy: current.busy + 1,
        lastActivityAt: lastActivityAt ?? current.lastActivityAt,
      }))
    })
    yield* Effect.addFinalizer(() => unsubscribe)
    const observedAt = Date.now()
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, { sessionID, generation }))
    yield* release.open
    yield* Fiber.join(fiber)
    yield* pollWithTimeout(
      Ref.get(updates).pipe(Effect.map((current) => (current.lastActivityAt >= observedAt ? current : undefined))),
      "pending activity was not flushed before idle",
    )
    yield* Effect.sleep(300)
    expect(yield* Ref.get(updates)).toMatchObject({ busy: 1, lastActivityAt: expect.any(Number) })
    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
  }),
)

it.instance("does not emit a stale busy update when activity timer flushes before idle", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const release = yield* Latch.make()
    const fiber = yield* graph.run
      .ensureRunning(sessionID, Effect.succeed(output), release.await.pipe(Effect.as(output)))
      .pipe(Effect.forkScoped)
    const { db } = yield* Database.Service
    const generation = yield* pollWithTimeout(
      db
        .select({ generation: SessionExecutionTable.generation })
        .from(SessionExecutionTable)
        .where(eq(SessionExecutionTable.session_id, sessionID))
        .get()
        .pipe(
          Effect.orDie,
          Effect.map((row) => (row ? row.generation : undefined)),
        ),
      "execution generation never claimed",
    )
    const observedAt = Date.now()
    yield* graph.status
      .activity(sessionID)
      .pipe(Effect.provideService(SessionStatus.ExecutionGeneration, { sessionID, generation }))
    yield* pollWithTimeout(
      graph.status
        .get(sessionID)
        .pipe(
          Effect.map((status) =>
            status.type === "busy" && (status.lastActivityAt ?? 0) >= observedAt ? true : undefined,
          ),
        ),
      "activity timer never flushed",
    )
    yield* release.open
    yield* Fiber.join(fiber)
    yield* Effect.sleep(300)
    expect(yield* graph.status.get(sessionID)).toEqual({ type: "idle" })
  }),
)

it.instance("does not clear a stale-looking status while its lease owner is live", () =>
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
        owner_id: `local:${process.pid}:${ensureRunID()}:live`,
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
        status: { type: "busy", since: now - 60_000, lastActivityAt: now - 60_000 },
        time_created: now - 60_000,
        time_updated: now - 60_000,
      })
      .run()
      .pipe(Effect.orDie)

    expect(yield* graph.status.get(sessionID)).toMatchObject({ type: "busy", since: now - 60_000 })
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
