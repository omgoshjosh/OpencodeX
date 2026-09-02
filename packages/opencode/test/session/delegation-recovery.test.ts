import { expect } from "bun:test"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { SessionDelegationRecovery } from "@/session/delegation-recovery"
import { delegationRecord, DELEGATION_RECORD_VERSION, type DelegationRecord } from "@/session/delegation-outcome"
import { MessageID, PartID } from "@/session/schema"
import { Session } from "@/session/session"
import { Database } from "@opencode-ai/core/database/database"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Effect, Layer, Ref } from "effect"
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
