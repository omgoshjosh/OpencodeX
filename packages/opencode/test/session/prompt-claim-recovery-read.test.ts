import { afterEach, describe, expect } from "bun:test"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import * as PromptClaim from "@/session/prompt-claim"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProviderV2 } from "@opencode-ai/core/provider"
import type { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import {
  MessageTable,
  PartTable,
  SessionCommandTable,
  SessionExecutionTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { eq } from "drizzle-orm"
import { Context, Deferred, Duration, Effect, Fiber, Layer, Ref, Scope } from "effect"
import path from "path"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { testEffect } from "../lib/effect"

/**
 * OpencodeX-dg4: the prompt-claim recovery sweep scans on the read connection
 * (no writer permit held while it walks a large database) and only then goes
 * to the writer, per candidate, inside an immediate transaction that re-reads
 * what the scan judged by. That opens a window between judging a run stuck
 * and settling it, or judging a command launchable and claiming it; the write
 * phase must lose to anything that changed in between. Built on a real file
 * database so the scan really runs on the second connection.
 */

const it = testEffect(Layer.empty)

const HOUR = 3_600_000
const STALE_AFTER = 60_000
const liveOwner = `local:${process.pid}:${ensureRunID()}:recovery-read`
const projectID = ProjectV2.ID.make("prj_recovery_read")
const tokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }

/** The finished assistant turn as `claude-mapper` leaves it in `message.data`. */
function assistantTurn(at: number) {
  return {
    role: "assistant",
    time: { created: at, completed: at },
    parentID: MessageID.make("msg_recovery_read_user"),
    providerID: ProviderV2.ID.make("test"),
    modelID: ProviderV2.ModelID.make("test"),
    mode: "build",
    agent: "build",
    path: { cwd: ".", root: "." },
    cost: 0,
    tokens,
    finish: "stop",
  } satisfies Omit<SessionLegacy.Assistant, "id" | "sessionID">
}
const stepFinish = {
  type: "step-finish",
  reason: "stop",
  cost: 0,
  tokens,
} satisfies Omit<SessionLegacy.StepFinishPart, "id" | "sessionID" | "messageID">
const runningTool = {
  type: "tool",
  tool: "bash",
  callID: "call_woken",
  state: { status: "running", input: {}, time: { start: 1 } },
} satisfies Omit<SessionLegacy.ToolPart, "id" | "sessionID" | "messageID">

/** `alias` is the `OPENCODE_DB_SINGLE_CONNECTION=1` shape: `read` is the writer itself. */
const graph = Effect.fnUntraced(function* (filename: string, alias: "split" | "alias" = "split") {
  const opened = Context.get(yield* Layer.build(Layer.fresh(Database.layerFromPath(filename))), Database.Service)
  expect(opened.read).not.toBe(opened.db)
  const database = alias === "alias" ? { db: opened.db, read: opened.db } : opened
  const databaseLayer = Layer.succeed(Database.Service, database)
  const context = yield* Layer.build(
    Layer.fresh(EventV2Bridge.layer.pipe(Layer.provideMerge(EventV2.layer), Layer.provide(databaseLayer))),
  )
  return { database, events: Context.get(context, EventV2Bridge.Service) }
})

const claimFor = Effect.fnUntraced(function* (
  target: { database: Database.Interface; events: EventV2.Interface },
  beforeRecoveryWrite: Effect.Effect<void>,
) {
  return yield* PromptClaim.make({
    ...target,
    scope: yield* Scope.Scope,
    loop: () => Effect.die(new Error("recovery must not run a command someone else holds")),
    staleExecutionMillis: Effect.succeed(STALE_AFTER),
    beforeRecoveryWrite,
  })
})

const session = Effect.fnUntraced(function* (database: Database.Interface, sessionID: SessionID, now: number) {
  const ctx = yield* InstanceState.context
  yield* database.db
    .insert(ProjectTable)
    .values({ id: projectID, worktree: ctx.directory, sandboxes: [], time_created: now, time_updated: now })
    .onConflictDoNothing()
  yield* database.db.insert(SessionTable).values({
    id: sessionID,
    project_id: projectID,
    slug: sessionID,
    directory: ctx.directory,
    title: "recovery read",
    version: "test",
    time_created: now - HOUR,
    time_updated: now - HOUR,
  })
})

/**
 * A finished turn this process is still heartbeating: an assistant message
 * whose newest part is a step-finish, an hour idle, with the execution and
 * its command both `running` on a renewed lease - stuck on any scan.
 */
const stuckRun = Effect.fnUntraced(function* (database: Database.Interface, sessionID: SessionID, now: number) {
  const ctx = yield* InstanceState.context
  yield* session(database, sessionID, now)
  const messageID = MessageID.make(`msg_${sessionID}`)
  yield* database.db.insert(MessageTable).values({
    id: messageID,
    session_id: sessionID,
    data: assistantTurn(now - HOUR),
    time_created: now - HOUR,
    time_updated: now - HOUR,
  })
  yield* database.db.insert(PartTable).values({
    id: PartID.make(`prt_${sessionID}_finish`),
    message_id: messageID,
    session_id: sessionID,
    data: stepFinish,
    time_created: now - HOUR,
    time_updated: now - HOUR,
  })
  yield* database.db.insert(SessionExecutionTable).values({
    session_id: sessionID,
    project_id: projectID,
    directory: ctx.directory,
    state: "running",
    owner_id: liveOwner,
    generation: 7,
    lease_expires_at: now + 4 * HOUR,
    started_at: now - HOUR,
    time_created: now - HOUR,
    time_updated: now,
  })
  yield* database.db.insert(SessionCommandTable).values({
    id: `sec_${sessionID}`,
    session_id: sessionID,
    message_id: messageID,
    project_id: projectID,
    directory: ctx.directory,
    status: "running",
    owner_id: liveOwner,
    lease_expires_at: now + 4 * HOUR,
    time_created: now - HOUR,
    time_updated: now,
  })
  return messageID
})

const execution = (database: Database.Interface, sessionID: SessionID) =>
  database.db.select().from(SessionExecutionTable).where(eq(SessionExecutionTable.session_id, sessionID)).get()

const command = (database: Database.Interface, id: string) =>
  database.db.select().from(SessionCommandTable).where(eq(SessionCommandTable.id, id)).get()

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("PromptClaim recovery sweep off the writer", () => {
  it.instance("never force-settles a run whose turn woke up between the unlocked scan and the write", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const target = yield* graph(path.join(test.directory, "recovery-read-cas.db"))
      const now = Date.now()
      const woken = SessionID.make("ses_recovery_woken")
      const abandoned = SessionID.make("ses_recovery_abandoned")
      const wokenMessage = yield* stuckRun(target.database, woken, now)
      yield* stuckRun(target.database, abandoned, now)
      const wakes = yield* Ref.make(0)
      const claim = yield* claimFor(
        target,
        // After the scan judged both runs stuck, a backgrounded delegation
        // reports into the woken turn and the model calls another tool. The
        // execution row is untouched (same owner, same generation, same
        // renewing lease) - only the transcript moved.
        Effect.gen(function* () {
          yield* target.database.db.insert(PartTable).values({
            id: PartID.make("prt_recovery_woken_tool"),
            message_id: wokenMessage,
            session_id: woken,
            data: runningTool,
            time_created: Date.now(),
            time_updated: Date.now(),
          })
          yield* Ref.update(wakes, (count) => count + 1)
        }).pipe(Effect.orDie),
      )

      yield* claim.sweepStaleExecutions()

      expect(yield* Ref.get(wakes)).toBe(1)
      expect(yield* execution(target.database, woken)).toMatchObject({ state: "running", owner_id: liveOwner })
      expect((yield* command(target.database, `sec_${woken}`))?.status).toBe("running")
      // The same pass still settled the run nobody woke, so the write phase did run.
      expect(yield* execution(target.database, abandoned)).toMatchObject({ state: "idle", owner_id: null })
      expect((yield* command(target.database, `sec_${abandoned}`))?.status).toBe("cancelled")
    }),
  )

  it.instance("never re-claims a queued command another owner took between the unlocked scan and the launch", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const target = yield* graph(path.join(test.directory, "recovery-read-claim.db"))
      const ctx = yield* InstanceState.context
      const now = Date.now()
      const contested = SessionID.make("ses_recovery_contested")
      yield* session(target.database, contested, now)
      yield* target.database.db.insert(SessionCommandTable).values({
        id: "sec_recovery_contested",
        session_id: contested,
        message_id: MessageID.make("msg_recovery_contested"),
        project_id: projectID,
        directory: ctx.directory,
        status: "queued",
        time_created: now - HOUR,
        time_updated: now - HOUR,
      })
      const claims = yield* Ref.make(0)
      const claim = yield* claimFor(
        target,
        // A second daemon wins the command after this sweep's scan saw it queued.
        Effect.gen(function* () {
          yield* target.database.db
            .update(SessionCommandTable)
            .set({
              status: "running",
              owner_id: "remote:other-daemon",
              claim_generation: 1,
              lease_expires_at: Date.now() + 4 * HOUR,
              started_at: Date.now(),
              time_updated: Date.now(),
            })
            .where(eq(SessionCommandTable.id, "sec_recovery_contested"))
          yield* Ref.update(claims, (count) => count + 1)
        }).pipe(Effect.orDie),
      )

      yield* claim.recover()
      // The launch is forked; give it every chance to misbehave before asserting.
      yield* Effect.sleep(Duration.millis(200))

      expect(yield* Ref.get(claims)).toBe(1)
      expect(yield* command(target.database, "sec_recovery_contested")).toMatchObject({
        status: "running",
        owner_id: "remote:other-daemon",
        claim_generation: 1,
      })
    }),
  )

  it.instance("scans past a held writer on the read connection, but queues behind it under the kill switch", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const split = yield* graph(path.join(test.directory, "recovery-read-split.db"))
      const aliased = yield* graph(path.join(test.directory, "recovery-read-alias.db"), "alias")
      const now = Date.now()
      const abandoned = SessionID.make("ses_recovery_held")
      yield* stuckRun(split.database, abandoned, now)
      yield* stuckRun(aliased.database, abandoned, now)
      const hold = Effect.fnUntraced(function* (target: typeof split) {
        const held = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        // Resolves when the scan phase has run: the write phase is next.
        const scanned = yield* Deferred.make<void>()
        const claim = yield* claimFor(target, Deferred.succeed(scanned, undefined).pipe(Effect.asVoid))
        // An immediate transaction holds the writer's single permit begin to commit.
        const holder = yield* Effect.forkChild(
          target.database.db.transaction(
            () => Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
            { behavior: "immediate" },
          ),
        )
        yield* Deferred.await(held)
        const sweeping = yield* Effect.forkChild(claim.sweepStaleExecutions())
        const outcome = yield* Deferred.await(scanned).pipe(Effect.timeoutOption(Duration.millis(300)))
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(holder)
        yield* Fiber.join(sweeping)
        return outcome._tag
      })
      // The scan runs on the read connection and reaches the write phase while the writer is held...
      expect(yield* hold(split)).toBe("Some")
      // ...whereas under the kill switch the whole pass waits for it, as it always did.
      expect(yield* hold(aliased)).toBe("None")
      // Both settle the abandoned run once the writer is free.
      expect((yield* execution(split.database, abandoned))?.state).toBe("idle")
      expect((yield* execution(aliased.database, abandoned))?.state).toBe("idle")
    }),
  )
})
