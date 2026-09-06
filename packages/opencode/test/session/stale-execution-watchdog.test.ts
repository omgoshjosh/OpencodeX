import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { SessionDelegationRecovery } from "@/session/delegation-recovery"
import {
  DELEGATION_RECORD_VERSION,
  delegationRecord,
  settleDelegation,
  type DelegationRecord,
} from "@/session/delegation-outcome"
import * as PromptClaim from "@/session/prompt-claim"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { Storage } from "@/storage/storage"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { InstanceState } from "@/effect/instance-state"
import { and, eq, gt, inArray, or } from "drizzle-orm"
import { Effect, Layer, Ref, Scope } from "effect"
import { testInstanceStoreLayer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const env = Layer.mergeAll(
  Session.layer.pipe(
    Layer.provide(Storage.defaultLayer),
    Layer.provide(Database.defaultLayer),
    Layer.provideMerge(EventV2Bridge.defaultLayer),
    Layer.provide(SessionProjector.defaultLayer),
    Layer.provide(RuntimeFlags.layer({ experimentalWorkspaces: false })),
    Layer.provide(BackgroundJob.defaultLayer),
  ),
  Database.defaultLayer,
  EventV2Bridge.defaultLayer,
  CrossSpawnSpawner.defaultLayer,
  testInstanceStoreLayer,
)
const it = testEffect(env)

const HOUR = 3_600_000
/** Short enough that every fixture's real-clock activity is far older. */
const STALE_AFTER = 60_000

/** The heartbeat that hides the stall: this process, renewed into the future. */
function liveOwner(sessionID: SessionID) {
  return `local:${process.pid}:${ensureRunID()}:watchdog:${sessionID}`
}

/**
 * Mirrors the `session_execution` and `session_command` blockers in
 * `server/routes/instance/httpapi/handlers/global.ts` (the `/global/restart-readiness`
 * handler) so a test can assert readiness without standing up the HTTP api.
 */
const restartBlocked = Effect.fn("StaleExecutionTest.restartBlocked")(function* () {
  const { db } = yield* Database.Service
  const now = Date.now()
  const execution = yield* db
    .select({ id: SessionExecutionTable.session_id })
    .from(SessionExecutionTable)
    .where(
      or(
        eq(SessionExecutionTable.state, "queued"),
        and(eq(SessionExecutionTable.state, "running"), gt(SessionExecutionTable.lease_expires_at, now)),
      ),
    )
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  const command = yield* db
    .select({ id: SessionCommandTable.id })
    .from(SessionCommandTable)
    .where(inArray(SessionCommandTable.status, ["queued", "running"]))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  return execution !== undefined || command !== undefined
})

const insertExecution = Effect.fn("StaleExecutionTest.insertExecution")(function* (input: {
  sessionID: SessionID
  owner?: string
  leaseExpiresAt?: number
  generation?: number
}) {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  const now = Date.now()
  yield* db
    .insert(SessionExecutionTable)
    .values({
      session_id: input.sessionID,
      project_id: ctx.project.id,
      directory: ctx.directory,
      state: "running",
      owner_id: input.owner ?? liveOwner(input.sessionID),
      generation: input.generation ?? 7,
      // Renewed well past the sweep's shifted clock: the lease is alive.
      lease_expires_at: input.leaseExpiresAt ?? now + 4 * HOUR,
      started_at: now,
      time_created: now,
      time_updated: now,
    })
    .run()
    .pipe(Effect.orDie)
})

/**
 * A turn the model has stopped talking in. `completed: false` is the shape of
 * the incident this watchdog exists for: `claude-mapper` wrote the step-finish
 * part and then returned early because background tasks were live, so the
 * message never got its `time.completed`. `stepFinish: false` is the genuinely
 * mid-stream turn, where neither proof exists.
 */
const finishedTurn = Effect.fn("StaleExecutionTest.finishedTurn")(function* (input: {
  sessionID: SessionID
  parentID?: MessageID
  messageID?: MessageID
  completed?: boolean
  stepFinish?: boolean
  text?: string
}) {
  const sessions = yield* Session.Service
  const messageID = input.messageID ?? MessageID.ascending()
  yield* sessions.updateMessage({
    id: messageID,
    sessionID: input.sessionID,
    role: "assistant",
    parentID: input.parentID ?? MessageID.ascending(),
    providerID: ProviderV2.ID.make("test"),
    modelID: ProviderV2.ModelID.make("test"),
    mode: "build",
    agent: "build",
    path: { cwd: ".", root: "." },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: input.completed === false ? { created: 10 } : { created: 10, completed: 11 },
    finish: input.completed === false ? undefined : "stop",
  })
  yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: input.sessionID,
    messageID,
    type: "text",
    text: input.text ?? "the report",
  })
  // Written last, exactly as the mapper writes it when the model stops.
  if (input.stepFinish !== false)
    yield* sessions.updatePart({
      id: PartID.ascending(),
      sessionID: input.sessionID,
      messageID,
      type: "step-finish",
      reason: "stop",
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    })
  return messageID
})

const buildClaim = Effect.fn("StaleExecutionTest.buildClaim")(function* (input?: {
  staleAfter?: number
  clockOffset?: number
  onStaleExecution?: (sessionID: SessionID) => Effect.Effect<void>
}) {
  const database = yield* Database.Service
  const events = yield* EventV2Bridge.Service
  const scope = yield* Scope.Scope
  // Every fixture writes at the real clock; the offset is how the sweep sees
  // an hour-idle session without the test sleeping for one.
  const offset = input?.clockOffset ?? HOUR
  return yield* PromptClaim.make({
    database,
    events,
    scope,
    loop: () => Effect.never,
    clock: () => Date.now() + offset,
    staleExecutionMillis: Effect.succeed(input?.staleAfter ?? STALE_AFTER),
    onStaleExecution: input?.onStaleExecution,
  })
})

const executionRow = Effect.fn("StaleExecutionTest.executionRow")(function* (sessionID: SessionID) {
  const { db } = yield* Database.Service
  return yield* db
    .select()
    .from(SessionExecutionTable)
    .where(eq(SessionExecutionTable.session_id, sessionID))
    .get()
    .pipe(Effect.orDie)
})

it.instance("settles a finished turn whose lease is still renewing and unblocks restart readiness", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service
    const ctx = yield* InstanceState.context
    const session = yield* sessions.create({})
    yield* finishedTurn({ sessionID: session.id })
    yield* insertExecution({ sessionID: session.id })
    const now = Date.now()
    yield* db
      .insert(SessionCommandTable)
      .values([
        {
          id: "sec_stale_running",
          session_id: session.id,
          message_id: MessageID.make("msg_stale_running"),
          project_id: ctx.project.id,
          directory: ctx.directory,
          status: "running",
          owner_id: liveOwner(session.id),
          lease_expires_at: now + 4 * HOUR,
          time_created: now,
          time_updated: now,
        },
        {
          id: "sec_stale_queued",
          session_id: session.id,
          message_id: MessageID.make("msg_stale_queued"),
          project_id: ctx.project.id,
          directory: ctx.directory,
          status: "queued",
          time_created: now + 1,
          time_updated: now + 1,
        },
      ])
      .run()
      .pipe(Effect.orDie)

    const claim = yield* buildClaim()
    expect(yield* restartBlocked()).toBe(true)
    yield* claim.sweepStaleExecutions()

    const execution = yield* executionRow(session.id)
    expect(execution?.state).toBe("idle")
    expect(execution?.owner_id).toBeNull()
    expect(execution?.lease_expires_at).toBeNull()
    expect(execution?.completed_at).not.toBeNull()

    const commands = yield* db
      .select({ id: SessionCommandTable.id, status: SessionCommandTable.status })
      .from(SessionCommandTable)
      .where(eq(SessionCommandTable.session_id, session.id))
      .all()
      .pipe(Effect.orDie)
    // The stuck command is settled; a queued sibling is real work and survives.
    expect(commands.find((row) => row.id === "sec_stale_running")?.status).toBe("cancelled")
    expect(commands.find((row) => row.id === "sec_stale_queued")?.status).toBe("queued")

    // Readiness still sees the queued command, but the execution no longer blocks.
    yield* db
      .update(SessionCommandTable)
      .set({ status: "cancelled" })
      .where(eq(SessionCommandTable.id, "sec_stale_queued"))
      .run()
      .pipe(Effect.orDie)
    expect(yield* restartBlocked()).toBe(false)
  }),
)

it.instance("leaves an execution whose last assistant message still has a running tool part", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    const messageID = yield* finishedTurn({ sessionID: session.id })
    yield* sessions.updatePart({
      id: PartID.ascending(),
      sessionID: session.id,
      messageID,
      type: "tool",
      tool: "bash",
      callID: "call_running",
      state: { status: "running", input: {}, time: { start: 1 } },
    })
    yield* insertExecution({ sessionID: session.id })

    const claim = yield* buildClaim()
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("running")
  }),
)

it.instance("leaves an execution whose session wrote parts more recently than the timeout", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* finishedTurn({ sessionID: session.id })
    yield* insertExecution({ sessionID: session.id })

    // No clock offset: the fixture's parts were written moments ago.
    const claim = yield* buildClaim({ clockOffset: 0 })
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("running")
  }),
)

// The reported incident: `claude-mapper` wrote the step-finish part at 23:57:48
// and returned early on a success result with live background tasks, so
// `time.completed` was never stamped and the row renewed its lease until the
// 00:26:22 abort. Gating on `time.completed` skipped this row forever.
it.instance("settles a step-finished turn that never recorded time.completed", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* finishedTurn({ sessionID: session.id, completed: false })
    yield* insertExecution({ sessionID: session.id })

    const claim = yield* buildClaim()
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("idle")
  }),
)

it.instance("leaves an execution whose last assistant message is still mid-stream", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    // Neither proof: no step-finish part and no `time.completed`.
    yield* finishedTurn({ sessionID: session.id, completed: false, stepFinish: false })
    yield* insertExecution({ sessionID: session.id })

    const claim = yield* buildClaim()
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("running")
  }),
)

it.instance("leaves a step-finished turn that is waiting on a live background delegation", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* finishedTurn({ sessionID: session.id, completed: false })
    yield* insertExecution({ sessionID: session.id })
    // A backgrounded subagent that will report back and re-wake the turn.
    const delegate = yield* sessions.create({ parentID: session.id })
    const record = delegation(session.id, MessageID.make("msg_delegate_boundary"))
    yield* sessions.stampDelegation({ sessionID: delegate.id, record })

    const claim = yield* buildClaim()
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("running")

    // Once the delegation settles, nothing is left to wake the turn.
    yield* sessions.stampDelegation({
      sessionID: delegate.id,
      record: settleDelegation(record, { outcome: "completed", completedAt: Date.now() }),
    })
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("idle")
  }),
)

it.instance("leaves an expired lease to the existing reclaim path", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({})
    yield* finishedTurn({ sessionID: session.id })
    yield* insertExecution({ sessionID: session.id, leaseExpiresAt: Date.now() - 1 })

    const claim = yield* buildClaim()
    yield* claim.sweepStaleExecutions()
    expect((yield* executionRow(session.id))?.state).toBe("running")
  }),
)

function delegation(parentSessionID: SessionID, childMessageID: MessageID): DelegationRecord {
  return {
    version: DELEGATION_RECORD_VERSION,
    runID: "run_stale",
    parentSessionID,
    attempt: 1,
    phase: "running",
    startedAt: 1,
    mode: "background",
    // Alive on purpose: the stall's signature is a live process still
    // heartbeating a run whose turn is already over.
    ownerID: `local:${process.pid}:${ensureRunID()}:stale`,
    childMessageID,
  }
}

const staleChild = Effect.fn("StaleExecutionTest.staleChild")(function* () {
  const sessions = yield* Session.Service
  const parent = yield* sessions.create({})
  const child = yield* sessions.create({ parentID: parent.id })
  const boundary = MessageID.make("msg_stale_boundary")
  yield* sessions.stampDelegation({ sessionID: child.id, record: delegation(parent.id, boundary) })
  yield* sessions.updateMessage({
    id: boundary,
    sessionID: child.id,
    role: "user",
    time: { created: 1 },
    agent: "build",
    model: { providerID: ProviderV2.ID.make("test"), modelID: ProviderV2.ModelID.make("test") },
  })
  yield* finishedTurn({ sessionID: child.id, parentID: boundary, text: "child report" })
  yield* insertExecution({ sessionID: child.id })
  return { parent, child }
})

it.instance("delivers the parent's delegation report exactly once, across repeated sweeps", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const { child } = yield* staleChild()
    const notices = yield* Ref.make<Array<{ sessionID: SessionID; text: string }>>([])
    const settles = yield* Ref.make(0)
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: (input) => Ref.update(notices, (items) => [...items, input]),
      refresh: () => Effect.void,
    })
    const claim = yield* buildClaim({
      onStaleExecution: (sessionID) =>
        Ref.update(settles, (count) => count + 1).pipe(Effect.andThen(recovery.settleFinished(sessionID))),
    })

    yield* claim.sweepStaleExecutions()
    yield* claim.sweepStaleExecutions()

    expect((yield* executionRow(child.id))?.state).toBe("idle")
    expect(yield* Ref.get(settles)).toBe(1)
    expect(yield* Ref.get(notices)).toHaveLength(1)
    expect((yield* Ref.get(notices))[0]).toMatchObject({ text: expect.stringContaining("child report") })
    expect(delegationRecord((yield* sessions.get(child.id)).metadata)).toMatchObject({
      outcome: "completed",
      deliveryOutcome: "delivered",
    })

    // A third pass has nothing left to settle and must not notify again.
    yield* claim.sweepStaleExecutions()
    yield* recovery.settleFinished(child.id)
    expect(yield* Ref.get(notices)).toHaveLength(1)
  }),
)

it.instance("restart recovery still refuses to settle a run whose owner is alive", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const { child } = yield* staleChild()
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: () => Effect.die(new Error("a live owner must not be reported by restart recovery")),
      refresh: () => Effect.void,
    })
    // Force-settle the execution row only, so the owner record is the sole
    // remaining live witness.
    yield* (yield* buildClaim()).sweepStaleExecutions()
    yield* recovery.recover()
    expect(delegationRecord((yield* sessions.get(child.id)).metadata)?.phase).toBe("running")
  }),
)
