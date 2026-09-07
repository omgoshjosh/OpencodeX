import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceState } from "@/effect/instance-state"
import {
  DELEGATION_RECORD_VERSION,
  settleDelegation,
  type DelegationDelivery,
  type DelegationRecord,
} from "@/session/delegation-outcome"
import { SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { Storage } from "@/storage/storage"
import { Database } from "@opencode-ai/core/database/database"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { Effect, Layer } from "effect"
import { testInstanceStoreLayer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

/**
 * `GET /session/status` advertised a background worker that had already
 * finished. The projection asked one question - "is the owner process still
 * alive?" - and the owner is the daemon, which outlives every child it starts.
 * These tests pin the durable evidence that retires a job instead.
 */
const env = Layer.mergeAll(
  Session.layer.pipe(
    Layer.provide(Storage.defaultLayer),
    Layer.provide(Database.defaultLayer),
    Layer.provideMerge(EventV2Bridge.defaultLayer),
    Layer.provide(SessionProjector.defaultLayer),
    Layer.provide(RuntimeFlags.layer({ experimentalWorkspaces: false })),
    Layer.provide(BackgroundJob.defaultLayer),
  ),
  SessionStatus.layer.pipe(Layer.provide(Database.defaultLayer), Layer.provideMerge(EventV2Bridge.defaultLayer)),
  Database.defaultLayer,
  EventV2Bridge.defaultLayer,
  testInstanceStoreLayer,
)
const it = testEffect(env)

/** This process, so `SessionExecutionOwner.alive` reports the owner as live. */
const liveOwner = (runID: string) => `local:${process.pid}:${ensureRunID()}:${runID}`

/** A parent with one background-delegated child, stamped as the task tool does. */
const delegated = Effect.fnUntraced(function* (input?: { record?: (record: DelegationRecord) => DelegationRecord }) {
  const sessions = yield* Session.Service
  const parent = yield* sessions.create({ title: "orchestrator" })
  const child = yield* sessions.create({ title: "child", parentID: parent.id })
  const runID = `run_${child.id}`
  const record: DelegationRecord = {
    version: DELEGATION_RECORD_VERSION,
    runID,
    parentSessionID: parent.id,
    mode: "background",
    background: true,
    ownerID: liveOwner(runID),
    role: "Goomba - Code (Implementer)",
    title: "Fix the phantom worker",
    attempt: 1,
    phase: "running",
    startedAt: Date.now() - 60_000,
  }
  yield* sessions.stampDelegation({ sessionID: child.id, record: input?.record?.(record) ?? record })
  return { parent, child, record }
})

/** The child's own execution row, in whatever state the scenario needs. */
const execution = Effect.fnUntraced(function* (input: {
  sessionID: SessionID
  state: "idle" | "queued" | "running" | "interrupted"
  completedAt?: number
  leaseExpiresAt?: number
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
      state: input.state,
      generation: 1,
      ...(input.state === "running"
        ? { owner_id: liveOwner("execution"), lease_expires_at: input.leaseExpiresAt ?? now + 60_000 }
        : {}),
      ...(input.completedAt !== undefined ? { completed_at: input.completedAt } : {}),
      started_at: now,
      time_created: now,
      time_updated: now,
    })
    .run()
    .pipe(Effect.orDie)
})

/** Task 27's durable question request: the child is parked on a human answer. */
const question = Effect.fnUntraced(function* (sessionID: SessionID) {
  const { db } = yield* Database.Service
  const ctx = yield* InstanceState.context
  const now = Date.now()
  yield* db
    .insert(SessionInteractionTable)
    .values({
      id: `qst_${sessionID}`,
      kind: "question",
      session_id: sessionID,
      project_id: ctx.project.id,
      directory: ctx.directory,
      state: "pending",
      request_json: { question: "which base branch?" },
      time_created: now,
      time_updated: now,
    })
    .run()
    .pipe(Effect.orDie)
})

const backgroundOf = Effect.fnUntraced(function* (sessionID: SessionID) {
  const status = yield* SessionStatus.Service
  const info = yield* status.get(sessionID)
  return info.background
})

it.instance("retires a completed child whose owner process is still alive", () =>
  Effect.gen(function* () {
    const { parent, child } = yield* delegated()
    // The exact incident: the daemon that started the child is still running,
    // so owner liveness says nothing, but the child's turn already returned.
    yield* execution({ sessionID: child.id, state: "idle", completedAt: Date.now() })

    expect(yield* backgroundOf(parent.id)).toBeUndefined()
  }),
)

it.instance("retires a child whose delegation record carries durable terminal evidence", () =>
  Effect.gen(function* () {
    const { parent, child, record } = yield* delegated()
    const sessions = yield* Session.Service
    yield* sessions.stampDelegation({
      sessionID: child.id,
      record: settleDelegation(record, { outcome: "completed", deliveryOutcome: "delivered" }),
      expectRunID: record.runID,
    })
    // Terminal evidence alone retires it: no execution row is consulted.
    expect(yield* backgroundOf(parent.id)).toBeUndefined()
  }),
)

it.instance("keeps a genuinely running child under a live owner", () =>
  Effect.gen(function* () {
    const { parent, child } = yield* delegated()
    yield* execution({ sessionID: child.id, state: "running" })

    const background = yield* backgroundOf(parent.id)
    expect(background?.running).toBe(true)
    expect(background?.jobs).toMatchObject([{ sessionID: child.id, status: "running", role: "Goomba - Code (Implementer)" }])
  }),
)

it.instance("keeps a child that has been stamped but has not started its turn yet", () =>
  Effect.gen(function* () {
    const { parent, child } = yield* delegated()
    // No execution row at all: the run was stamped before the child's turn
    // claimed one. Absence of evidence must never read as termination.
    const background = yield* backgroundOf(parent.id)
    expect(background?.running).toBe(true)
    expect(background?.jobs.map((job) => job.sessionID)).toEqual([child.id])
  }),
)

it.instance("keeps a child whose previous run completed before this run started", () =>
  Effect.gen(function* () {
    const { parent, child, record } = yield* delegated()
    // A reused `task_id` session: the idle row belongs to the run before this
    // one, so it is not evidence about this attempt.
    yield* execution({ sessionID: child.id, state: "idle", completedAt: record.startedAt - 1 })

    expect((yield* backgroundOf(parent.id))?.running).toBe(true)
  }),
)

it.instance("reports a settled run whose report has not reached the parent as completed", () =>
  Effect.gen(function* () {
    const { parent, child, record } = yield* delegated()
    const sessions = yield* Session.Service
    const completedAt = Date.now()
    yield* sessions.stampDelegation({
      sessionID: child.id,
      record: settleDelegation(record, { outcome: "completed", completedAt, deliveryOutcome: "pending" }),
      expectRunID: record.runID,
    })

    const background = yield* backgroundOf(parent.id)
    // Outstanding, but nobody is working: the distinction the client needs.
    expect(background?.running).toBe(false)
    expect(background?.jobs).toMatchObject([
      { sessionID: child.id, status: "completed", completedAt, delivery: "pending" satisfies DelegationDelivery },
    ])
  }),
)

it.instance("reports a child parked on a durable question as blocked, not running", () =>
  Effect.gen(function* () {
    const { parent, child } = yield* delegated()
    yield* execution({ sessionID: child.id, state: "running" })
    yield* question(child.id)

    const background = yield* backgroundOf(parent.id)
    expect(background?.running).toBe(false)
    expect(background?.jobs).toMatchObject([{ sessionID: child.id, status: "blocked" }])
  }),
)

it.instance("still retires a running record whose owner process died", () =>
  Effect.gen(function* () {
    const { parent, child } = yield* delegated({
      record: (record) => ({ ...record, ownerID: "local:999999:dead:delegation" }),
    })
    yield* execution({ sessionID: child.id, state: "running" })

    expect(yield* backgroundOf(parent.id)).toBeUndefined()
  }),
)
