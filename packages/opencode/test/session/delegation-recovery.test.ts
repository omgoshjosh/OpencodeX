import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { SessionDelegationRecovery } from "@/session/delegation-recovery"
import { delegationRecord, DELEGATION_RECORD_VERSION, type DelegationRecord } from "@/session/delegation-outcome"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { Database } from "@opencode-ai/core/database/database"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Context, Effect, Layer, Ref } from "effect"
import { Storage } from "@/storage/storage"
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

function record(
  parentSessionID: string,
  childMessageID: string,
  overrides: Partial<DelegationRecord> = {},
): DelegationRecord {
  return {
    version: DELEGATION_RECORD_VERSION,
    runID: "run_recovery",
    parentSessionID,
    attempt: 1,
    phase: "running",
    startedAt: 1,
    mode: "background",
    ownerID: "local:999999:dead:run_recovery",
    childMessageID,
    ...overrides,
  }
}

const runningParentTask = Effect.fn("DelegationRecoveryTest.runningParentTask")(function* (
  sessions: Context.Service.Shape<typeof Session.Service>,
  parentSessionID: SessionID,
) {
  const message = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    sessionID: parentSessionID,
    role: "assistant",
    parentID: MessageID.ascending(),
    mode: "build",
    agent: "build",
    providerID: ProviderV2.ID.make("test"),
    modelID: ProviderV2.ModelID.make("test"),
    path: { cwd: ".", root: "." },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1 },
  })
  const part = yield* sessions.updatePart({
    id: PartID.ascending(),
    sessionID: parentSessionID,
    messageID: message.id,
    type: "tool",
    tool: "task",
    callID: "call_recovery",
    state: { status: "running", input: {}, time: { start: 1 } },
  })
  return { message, part }
})

it.instance("settles only the exact completed child reply and redelivers idempotently", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const parent = yield* sessions.create({})
    const child = yield* sessions.create({ parentID: parent.id })
    const notices = yield* Ref.make<Array<{ messageID: string; text: string; noReply: boolean }>>([])
    const boundary = "msg_child_boundary"
    yield* sessions.stampDelegation({ sessionID: child.id, record: record(parent.id, boundary) })
    yield* sessions.updateMessage({
      id: MessageID.make(boundary),
      sessionID: child.id,
      role: "user",
      time: { created: 1 },
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ProviderV2.ModelID.make("test") },
    })
    yield* sessions.updateMessage({
      id: MessageID.make("msg_old"),
      sessionID: child.id,
      role: "assistant",
      parentID: MessageID.make("msg_other"),
      providerID: ProviderV2.ID.make("test"),
      modelID: ProviderV2.ModelID.make("test"),
      mode: "build",
      agent: "build",
      path: { cwd: ".", root: "." },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: 1, completed: 2 },
      finish: "stop",
    })
    yield* sessions.updateMessage({
      id: MessageID.make("msg_exact"),
      sessionID: child.id,
      role: "assistant",
      parentID: MessageID.make(boundary),
      providerID: ProviderV2.ID.make("test"),
      modelID: ProviderV2.ModelID.make("test"),
      mode: "build",
      agent: "build",
      path: { cwd: ".", root: "." },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: 1, completed: 2 },
      finish: "stop",
    })
    yield* sessions.updatePart({
      id: PartID.make("prt_exact"),
      sessionID: child.id,
      messageID: MessageID.make("msg_exact"),
      type: "text",
      text: "full exact report",
    })
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: (input) => Ref.update(notices, (items) => [...items, input]),
      refresh: () => Effect.void,
    })
    yield* recovery.recover()
    expect(delegationRecord((yield* sessions.get(child.id)).metadata)).toMatchObject({
      outcome: "completed",
      deliveryOutcome: "delivered",
      summary: "full exact report",
    })
    expect(yield* Ref.get(notices)).toHaveLength(1)
    expect((yield* Ref.get(notices))[0]).toMatchObject({
      text: expect.stringContaining("full exact report"),
      noReply: false,
    })
    yield* recovery.recover()
    expect(yield* Ref.get(notices)).toHaveLength(1)
  }),
)

it.instance("does not reload transcripts for delivered runs", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const parent = yield* sessions.create({})
    const child = yield* sessions.create({ parentID: parent.id })
    yield* sessions.stampDelegation({
      sessionID: child.id,
      record: record(parent.id, "msg_delivered", {
        phase: "settled",
        outcome: "completed",
        completedAt: 2,
        deliveryOutcome: "delivered",
      }),
    })
    let transcriptReads = 0
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions: {
        ...sessions,
        messages: (input) => Effect.sync(() => transcriptReads++).pipe(Effect.andThen(sessions.messages(input))),
      },
      notify: () => Effect.die(new Error("delivered runs must not notify")),
      refresh: () => Effect.void,
    })

    yield* recovery.recover()
    expect(transcriptReads).toBe(0)
  }),
)

it.instance("abandons a dead run without an exact terminal reply and marks failed delivery for retry", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const parent = yield* sessions.create({})
    const child = yield* sessions.create({ parentID: parent.id })
    yield* sessions.stampDelegation({ sessionID: child.id, record: record(parent.id, "msg_missing") })
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: () => Effect.die(new Error("offline")),
      refresh: () => Effect.void,
    })
    yield* recovery.recover()
    expect(delegationRecord((yield* sessions.get(child.id)).metadata)).toMatchObject({
      outcome: "abandoned",
      deliveryOutcome: "failed",
    })
  }),
)

it.instance("repairs a settled foreground task after a deterministic provider reset before delivery short-circuits", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const parent = yield* sessions.create({})
    const child = yield* sessions.create({ parentID: parent.id })
    const parentTask = yield* runningParentTask(sessions, parent.id)
    const boundary = "msg_foreground_boundary"
    yield* sessions.stampDelegation({
      sessionID: child.id,
      record: record(parent.id, boundary, {
        mode: "foreground",
        parentMessageID: parentTask.message.id,
        toolCallID: "call_recovery",
        phase: "settled",
        outcome: "completed",
        completedAt: 2,
        // The child settled before the reset, but the provider never persisted
        // the corresponding parent tool-part terminal frame.
        deliveryOutcome: "delivered",
      }),
    })
    yield* sessions.updateMessage({
      id: MessageID.make(boundary),
      sessionID: child.id,
      role: "user",
      time: { created: 1 },
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ProviderV2.ModelID.make("test") },
    })
    yield* sessions.updateMessage({
      id: MessageID.make("msg_foreground_exact"),
      sessionID: child.id,
      role: "assistant",
      parentID: MessageID.make(boundary),
      providerID: ProviderV2.ID.make("test"),
      modelID: ProviderV2.ModelID.make("test"),
      mode: "build",
      agent: "build",
      path: { cwd: ".", root: "." },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: 1, completed: 2 },
      finish: "stop",
    })
    yield* sessions.updatePart({
      id: PartID.make("prt_foreground_exact"),
      sessionID: child.id,
      messageID: MessageID.make("msg_foreground_exact"),
      type: "text",
      text: "recovered foreground report",
    })
    const notices = yield* Ref.make(0)
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: () => Ref.update(notices, (count) => count + 1),
      refresh: () => Effect.void,
    })
    yield* recovery.recover()
    const repaired = yield* sessions.getPart({
      sessionID: parent.id,
      messageID: parentTask.message.id,
      partID: parentTask.part.id,
    })
    expect(repaired?.type).toBe("tool")
    if (!repaired || repaired.type !== "tool") return
    expect(repaired.state).toMatchObject({ status: "completed", output: expect.stringContaining("recovered foreground report") })
    expect(yield* Ref.get(notices)).toBe(0)
    // A reconnect repeats recovery, not the parent terminal event.
    yield* recovery.recover()
    const repeated = yield* sessions.getPart({ sessionID: parent.id, messageID: parentTask.message.id, partID: parentTask.part.id })
    expect(repeated?.type).toBe("tool")
    if (!repeated || repeated.type !== "tool") return
    expect(repeated.state.status).toBe("completed")
    expect(yield* Ref.get(notices)).toBe(0)
  }),
)

it.instance("bounds a missing foreground child reply as an orphaned parent task", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const database = yield* Database.Service
    const parent = yield* sessions.create({})
    const child = yield* sessions.create({ parentID: parent.id })
    const parentTask = yield* runningParentTask(sessions, parent.id)
    yield* sessions.stampDelegation({
      sessionID: child.id,
      record: record(parent.id, "msg_missing_foreground", {
        mode: "foreground",
        parentMessageID: parentTask.message.id,
        toolCallID: "call_recovery",
      }),
    })
    const recovery = yield* SessionDelegationRecovery.make({
      database,
      sessions,
      notify: () => Effect.die(new Error("foreground runs do not notify")),
      refresh: () => Effect.void,
    })
    yield* recovery.recover()
    expect(delegationRecord((yield* sessions.get(child.id)).metadata)).toMatchObject({ outcome: "abandoned" })
    const orphaned = yield* sessions.getPart({
      sessionID: parent.id,
      messageID: parentTask.message.id,
      partID: parentTask.part.id,
    })
    expect(orphaned?.type).toBe("tool")
    if (!orphaned || orphaned.type !== "tool") return
    expect(orphaned.state).toMatchObject({ status: "error", error: expect.stringContaining("orphaned") })
  }),
)
