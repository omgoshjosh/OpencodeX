import { expect } from "bun:test"
import fs from "fs/promises"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import * as PromptClaim from "@/session/prompt-claim"
import { MessageID } from "@/session/schema"
import { Session } from "@/session/session"
import { Storage } from "@/storage/storage"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import * as EffectLogger from "@opencode-ai/core/effect/logger"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionCommandTable } from "@opencode-ai/core/session/sql"
import { Log } from "@opencode-ai/core/util/log"
import { InstanceState } from "@/effect/instance-state"
import { Effect, Layer, Scope } from "effect"
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

const logText = () => Effect.promise(() => fs.readFile(Log.file(), "utf8").catch(() => ""))

/** The first daemon log line written after `offset` that carries `marker`, via the file logger production uses. */
const renderedLine = Effect.fn("RecoveryLogTest.renderedLine")(function* (marker: string, offset: number) {
  for (let attempt = 0; attempt < 50; attempt++) {
    const line = (yield* logText())
      .slice(offset)
      .split("\n")
      .find((entry) => entry.includes(marker))
    if (line) return line
    yield* Effect.sleep("20 millis")
  }
  return undefined
})

it.instance("recovery diagnostic renders its payload as fields, not [object Object]", () =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    const { db } = yield* Database.Service
    const ctx = yield* InstanceState.context
    const session = yield* sessions.create({})
    const commandID = `sec_recovery_log_${Date.now()}`
    const now = Date.now()
    yield* db
      .insert(SessionCommandTable)
      .values({
        id: commandID,
        session_id: session.id,
        message_id: MessageID.make("msg_recovery_log"),
        project_id: ctx.project.id,
        directory: ctx.directory,
        status: "cancelled",
        completed_at: now,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)

    const claim = yield* PromptClaim.make({
      database: yield* Database.Service,
      events: yield* EventV2Bridge.Service,
      scope: yield* Scope.Scope,
      loop: () => Effect.never,
    })
    const offset = (yield* logText()).length
    // A settled command: claim reports "done" and the diagnostic logs the row.
    yield* claim.executeCommand(commandID).pipe(Effect.provide(EffectLogger.layer))

    const line = yield* renderedLine("session command recovery", offset)
    expect(line).toBeDefined()
    expect(line).toContain(`commandID=${commandID}`)
    expect(line).toContain(`session.id=${session.id}`)
    expect(line).toContain("action=claim")
    expect(line).toContain("cas=done")
    expect(line).not.toContain("[object Object]")
  }),
)
