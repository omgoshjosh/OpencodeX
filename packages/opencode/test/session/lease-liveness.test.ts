// https://github.com/ecgreen/OpencodeX/issues/51: a lease that lapses because
// its heartbeat was starved by DB contention must not abort the live turn that
// owns it. Only a dead owner is reclaimed.
import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { SessionRunState } from "@/session/run-state"
import { MessageID, SessionID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import * as PromptClaim from "@/session/prompt-claim"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionCommandTable, SessionExecutionTable, SessionTable } from "@opencode-ai/core/session/sql"
import { eq } from "drizzle-orm"
import { Context, Effect, Fiber, Latch, Layer, Ref, Scope } from "effect"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"

const it = testEffect(
  Layer.mergeAll(
    Database.defaultLayer,
    EventV2Bridge.defaultLayer,
    BackgroundJob.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
  ),
)

const sessionID = SessionID.make("ses_lease_liveness")
const assistant = (id: string): SessionLegacy.WithParts => ({
  info: {
    id: MessageID.make(id),
    sessionID,
    role: "assistant",
    parentID: MessageID.make("msg_lease_user"),
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
})
const completed = assistant("msg_lease_completed")
const aborted = assistant("msg_lease_aborted")
// A pid that cannot exist, so its owner is provably dead.
const deadOwner = "local:2147483646:deadrun:owner"

const buildRunGraph = Effect.fn("LeaseLivenessTest.buildRunGraph")(function* () {
  const database = yield* Database.Service
  const databaseLayer = Layer.succeed(Database.Service, database)
  const statusLayer = SessionStatus.layer.pipe(
    Layer.provide(databaseLayer),
    Layer.provide(Layer.succeed(EventV2Bridge.Service, yield* EventV2Bridge.Service)),
  )
  const runLayer = SessionRunState.layer.pipe(
    Layer.provide(Layer.succeed(BackgroundJob.Service, yield* BackgroundJob.Service)),
    Layer.provide(databaseLayer),
    Layer.provide(statusLayer),
  )
  const context = yield* Layer.build(Layer.fresh(Layer.mergeAll(runLayer, statusLayer)))
  return { run: Context.get(context, SessionRunState.Service), status: Context.get(context, SessionStatus.Service) }
})

const execution = Effect.fn("LeaseLivenessTest.execution")(function* () {
  const { db } = yield* Database.Service
  return yield* db
    .select()
    .from(SessionExecutionTable)
    .where(eq(SessionExecutionTable.session_id, sessionID))
    .get()
    .pipe(Effect.orDie)
})

const lapseExecutionLease = Effect.fn("LeaseLivenessTest.lapseExecutionLease")(function* (owner?: string) {
  const { db } = yield* Database.Service
  yield* db
    .update(SessionExecutionTable)
    .set({ lease_expires_at: Date.now() - 60_000, ...(owner ? { owner_id: owner } : {}) })
    .where(eq(SessionExecutionTable.session_id, sessionID))
    .run()
    .pipe(Effect.orDie)
})

it.instance("live turn whose heartbeat was starved past its lease is extended, not interrupted", () =>
  Effect.gen(function* () {
    const graph = yield* buildRunGraph()
    const release = yield* Latch.make()
    const interrupted = yield* Ref.make(false)
    const fiber = yield* graph.run
      .ensureRunning(
        sessionID,
        Ref.set(interrupted, true).pipe(Effect.as(aborted)),
        release.await.pipe(
          Effect.as(completed),
          Effect.onInterrupt(() => Ref.set(interrupted, true)),
        ),
      )
      .pipe(Effect.forkScoped)
    yield* pollWithTimeout(
      graph.status.get(sessionID).pipe(Effect.map((status) => (status.type === "busy" ? true : undefined))),
      "session never became busy",
    )
    const before = yield* execution()
    // The heartbeat lost to the DB for longer than the whole lease.
    yield* lapseExecutionLease()
    const extended = yield* pollWithTimeout(
      execution().pipe(
        Effect.map((row) => (row?.lease_expires_at && row.lease_expires_at > Date.now() ? row : undefined)),
      ),
      "live owner did not extend its lapsed lease",
    )
    expect(extended.generation).toBe(before!.generation)
    expect(extended.owner_id).toBe(before!.owner_id)
    expect(yield* Ref.get(interrupted)).toBe(false)
    yield* release.open
    expect(yield* Fiber.join(fiber)).toEqual(completed)
    expect(yield* Ref.get(interrupted)).toBe(false)
  }),
)

it.instance("control: an execution whose owner process is dead is reclaimed", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const ctx = yield* InstanceState.context
    const now = Date.now()
    yield* db
      .insert(SessionExecutionTable)
      .values({
        session_id: sessionID,
        project_id: ctx.project.id,
        directory: ctx.directory,
        state: "running",
        owner_id: deadOwner,
        generation: 7,
        lease_expires_at: now - 60_000,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)
    const graph = yield* buildRunGraph()
    const result = yield* graph.run.ensureRunning(sessionID, Effect.succeed(aborted), Effect.succeed(completed))
    expect(result).toEqual(completed)
    expect((yield* execution())?.generation).toBe(8)
  }),
)

const buildPromptClaim = Effect.fn("LeaseLivenessTest.buildPromptClaim")(function* (
  deps: Partial<PromptClaim.Deps> = {},
) {
  return yield* PromptClaim.make({
    database: yield* Database.Service,
    events: yield* EventV2Bridge.Service,
    scope: yield* Scope.Scope,
    loop: () => Effect.never,
    ...deps,
  })
})

const insertCommand = Effect.fn("LeaseLivenessTest.insertCommand")(function* (input: {
  id: string
  status: "queued" | "running"
  owner?: string
  leaseExpiresAt?: number
  created: number
}) {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  yield* db
    .insert(SessionCommandTable)
    .values({
      id: input.id,
      session_id: sessionID,
      message_id: MessageID.make(`msg_${input.id}`),
      project_id: ctx.project.id,
      directory: ctx.directory,
      status: input.status,
      owner_id: input.owner ?? null,
      claim_generation: input.status === "running" ? 1 : 0,
      lease_expires_at: input.leaseExpiresAt ?? null,
      started_at: input.status === "running" ? input.created : null,
      time_created: input.created,
      time_updated: input.created,
    })
    .run()
    .pipe(Effect.orDie)
})

const insertSession = Effect.fn("LeaseLivenessTest.insertSession")(function* () {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  const now = Date.now()
  const projectID = ProjectV2.ID.make("prj_lease_liveness")
  yield* db
    .insert(ProjectTable)
    .values({ id: projectID, worktree: ctx.directory, sandboxes: [], time_created: now, time_updated: now })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({
      id: sessionID,
      project_id: projectID,
      slug: "slug-lease-liveness",
      directory: ctx.directory,
      title: "Lease liveness",
      version: "test",
      time_created: now,
      time_updated: now,
    })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
})

const insertExecution = Effect.fn("LeaseLivenessTest.insertExecution")(function* (owner: string, expiresAt: number) {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  const now = Date.now()
  yield* db
    .insert(SessionExecutionTable)
    .values({
      session_id: sessionID,
      project_id: ctx.project.id,
      directory: ctx.directory,
      state: "running",
      owner_id: owner,
      generation: 3,
      lease_expires_at: expiresAt,
      time_created: now,
      time_updated: now,
    })
    .run()
    .pipe(Effect.orDie)
})

it.instance("queued command waits behind a live execution whose lease lapsed; a dead one frees it", () =>
  Effect.gen(function* () {
    const live = yield* Ref.make(true)
    const claim = yield* buildPromptClaim({
      executionOwnerLive: () => Ref.get(live),
    })
    const now = Date.now()
    yield* insertExecution(`local:${process.pid}:${ensureRunID()}:runstate:${sessionID}`, now - 60_000)
    yield* insertCommand({ id: "sec_lease_queued", status: "queued", created: now })
    expect(yield* claim.claimCommandTurn("sec_lease_queued")).toEqual({ state: "waiting" })
    expect(yield* claim.waitForExecutionTurn("sec_lease_queued", sessionID)).toBe(false)
    yield* Ref.set(live, false)
    expect(yield* claim.claimCommandTurn("sec_lease_queued")).toMatchObject({ state: "ready" })
  }),
)

it.instance("a running command whose own turn is live is re-leased, not reclaimed; a dead owner's is reclaimed", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const started = yield* Latch.make()
    const claim = yield* buildPromptClaim({
      commandLeaseMillis: 60_000,
      loop: () => started.open.pipe(Effect.andThen(Effect.never)),
    })
    const now = Date.now()
    yield* insertSession()
    yield* insertCommand({ id: "sec_lease_live", status: "queued", created: now })
    const fiber = yield* claim.executeCommand("sec_lease_live").pipe(Effect.forkScoped)
    yield* awaitWithTimeout(started.await, "command turn never started")
    const command = () =>
      db.select().from(SessionCommandTable).where(eq(SessionCommandTable.id, "sec_lease_live")).get().pipe(Effect.orDie)
    const claimed = yield* command()
    // Heartbeat starved past the lease while the turn is still running here.
    yield* db
      .update(SessionCommandTable)
      .set({ lease_expires_at: Date.now() - 60_000 })
      .where(eq(SessionCommandTable.id, "sec_lease_live"))
      .run()
      .pipe(Effect.orDie)
    expect(yield* claim.claimCommandTurn("sec_lease_live")).toEqual({ state: "waiting" })
    const after = yield* command()
    expect(after?.claim_generation).toBe(claimed!.claim_generation)
    expect(after?.owner_id).toBe(claim.commandOwner)
    expect(after?.lease_expires_at ?? 0).toBeGreaterThan(Date.now())
    yield* Fiber.interrupt(fiber)

    yield* insertCommand({
      id: "sec_lease_dead",
      status: "running",
      owner: deadOwner,
      leaseExpiresAt: now - 60_000,
      created: now + 1,
    })
    yield* db.delete(SessionCommandTable).where(eq(SessionCommandTable.id, "sec_lease_live")).run().pipe(Effect.orDie)
    expect(yield* claim.claimCommandTurn("sec_lease_dead")).toMatchObject({ state: "ready" })
  }),
)
