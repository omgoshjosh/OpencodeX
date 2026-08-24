import { expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { OpencodeXJobTable, OpencodeXSwarmTable } from "@opencode-ai/core/opencodex/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { eq, inArray } from "drizzle-orm"
import { Context, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import path from "path"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceStore } from "@/project/instance-store"
import { DeploymentDrain } from "@/server/deployment-drain"
import { InstanceRef } from "@/effect/instance-ref"
import { MessageID, SessionID } from "@/session/schema"
import { PromptClaim } from "@/session/prompt-claim"
import { SessionPromptRecovery } from "@/session/prompt-recovery"
import { tmpdir } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const databaseLayer = Database.layerFromPath(":memory:")
const it = testEffect(DeploymentDrain.layer.pipe(Layer.provideMerge(databaseLayer)))

it.live("readiness counts active work but not queued commands", () =>
  Effect.gen(function* () {
    const drain = yield* DeploymentDrain.Service
    const { db } = yield* Database.Service
    const now = Date.now()
    yield* Effect.addFinalizer(() => Effect.sync(DeploymentDrain.resetForTest))

    yield* db
      .insert(SessionCommandTable)
      .values({
        id: "sec_drain_queued",
        session_id: SessionID.make("ses_drain_queued"),
        message_id: MessageID.make("msg_drain_queued"),
        project_id: "prj_drain",
        directory: "/queued",
        status: "queued",
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)

    expect(yield* drain.status()).toMatchObject({ queuedCommands: 1, ready: false })
    expect(yield* drain.begin(drain.runID)).toMatchObject({ queuedCommands: 1, ready: true })
    yield* Effect.addFinalizer(() => drain.cancel(drain.runID).pipe(Effect.ignore))

    yield* db
      .insert(SessionCommandTable)
      .values({
        id: "sec_drain_running",
        session_id: SessionID.make("ses_drain_running"),
        message_id: MessageID.make("msg_drain_running"),
        project_id: "prj_drain",
        directory: "/running",
        status: "running",
        owner_id: `local:${process.pid}:${drain.runID}:prompt:test`,
        lease_expires_at: now + 60_000,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionExecutionTable)
      .values([
        {
          session_id: SessionID.make("ses_drain_live"),
          project_id: "prj_drain",
          directory: "/live",
          state: "running",
          lease_expires_at: now + 60_000,
          time_created: now,
          time_updated: now,
        },
        {
          session_id: SessionID.make("ses_drain_expired"),
          project_id: "prj_drain",
          directory: "/expired",
          state: "running",
          lease_expires_at: now - 1,
          time_created: now,
          time_updated: now,
        },
      ])
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(OpencodeXJobTable)
      .values(
        ["queued", "claimed", "running", "completed"].map((status) => ({
          id: `job_drain_${status}`,
          kind: "test",
          status,
          source: "test",
          lease_expires_at: status === "claimed" || status === "running" ? now + 60_000 : undefined,
          time_created: now,
          time_updated: now,
        })),
      )
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(OpencodeXSwarmTable)
      .values(
        ["queued", "running", "cancelling", "planned"].map((status) => ({
          id: `swm_drain_${status}`,
          title: status,
          prompt: "",
          status,
          source: "test",
          time_created: now,
          time_updated: now,
        })),
      )
      .run()
      .pipe(Effect.orDie)

    expect(yield* drain.status()).toMatchObject({
      queuedCommands: 1,
      runningCommands: 1,
      liveRunningExecutions: 1,
      activeJobs: 2,
      activeSwarms: 3,
      activeGoals: 0,
      ready: false,
    })

    yield* Effect.all(
      [
        db.delete(SessionCommandTable).where(eq(SessionCommandTable.id, "sec_drain_running")).run(),
        db
          .delete(SessionExecutionTable)
          .where(eq(SessionExecutionTable.session_id, SessionID.make("ses_drain_live")))
          .run(),
        db
          .update(OpencodeXJobTable)
          .set({ lease_expires_at: now - 1 })
          .where(inArray(OpencodeXJobTable.status, ["claimed", "running"]))
          .run(),
      ].map((effect) => effect.pipe(Effect.orDie)),
      { discard: true },
    )
    expect(yield* drain.status()).toMatchObject({
      queuedCommands: 1,
      activeJobs: 0,
      activeSwarms: 3,
      ready: true,
    })
  }),
)

it.live("replay loads queued and dead-owner directories while excluding the current run", () =>
  Effect.gen(function* () {
    const drain = yield* DeploymentDrain.Service
    const { db } = yield* Database.Service
    const now = Date.now()
    const loaded: string[] = []
    const recovered: string[] = []
    yield* Effect.addFinalizer(() => Effect.sync(DeploymentDrain.resetForTest))
    const storeContext = yield* Layer.build(
      Layer.mock(InstanceStore.Service)({
        load: (input) =>
          Effect.sync(() => {
            loaded.push(input.directory)
            return {
              directory: input.directory,
              worktree: input.directory,
              project: {
                id: ProjectV2.ID.make("prj_drain_replay"),
                worktree: input.directory,
                time: { created: now, updated: now },
                sandboxes: [],
              },
            }
          }),
        provide: (input, effect) =>
          Effect.gen(function* () {
            loaded.push(input.directory)
            return yield* effect.pipe(
              Effect.provideService(InstanceRef, {
                directory: input.directory,
                worktree: input.directory,
                project: {
                  id: ProjectV2.ID.make("prj_drain_replay"),
                  worktree: input.directory,
                  time: { created: now, updated: now },
                  sandboxes: [],
                },
              }),
            )
          }),
      }),
    )
    const store = Context.get(storeContext, InstanceStore.Service)
    const unregister = SessionPromptRecovery.register(() =>
      Effect.sync(() => recovered.push(loaded.at(-1) ?? "missing")),
    )
    yield* Effect.addFinalizer(() => Effect.sync(unregister))

    yield* db
      .insert(SessionCommandTable)
      .values([
        command("queued-a", "/queued", "queued"),
        command("queued-b", "/queued", "queued"),
        command("dead-owner", "/dead", "running", `local:${process.pid}:dead-run:prompt:test`, now + 60_000),
        command(
          "current-owner",
          "/current",
          "running",
          `local:${process.pid}:${drain.runID}:prompt:test`,
          now + 60_000,
        ),
        command("foreign-live", "/foreign", "running", "remote:live", now + 60_000),
        command("expired", "/expired", "running", "remote:expired", now - 1),
        command("finished", "/finished", "succeeded"),
      ])
      .run()
      .pipe(Effect.orDie)

    yield* drain.begin(drain.runID)
    expect(yield* drain.replay(drain.runID, store).pipe(Effect.flip)).toMatchObject({
      kind: "unavailable",
      message: "replay is only available on an accepting process",
    })
    yield* drain.cancel(drain.runID)

    const first = yield* drain.replay(drain.runID, store)
    const second = yield* drain.replay(drain.runID, store)

    expect(first).toEqual(second)
    expect(first.commandCount).toBe(4)
    expect(new Set(first.directories)).toEqual(new Set(["/queued", "/dead", "/expired"]))
    expect(first.directories).toHaveLength(3)
    expect(loaded).toHaveLength(6)
    expect(new Set(loaded.slice(0, 3))).toEqual(new Set(first.directories))
    expect(new Set(loaded.slice(3))).toEqual(new Set(first.directories))
    expect(recovered).toEqual(loaded)
  }),
)

it.live("reclaims a foreign local command only after its owning process exits", () =>
  Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.sync(DeploymentDrain.resetForTest))
    const temporary = yield* Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    )
    const databaseContext = yield* Layer.build(Database.layerFromPath(path.join(temporary.path, "shared.db")))
    const database = Context.get(databaseContext, Database.Service)
    const drainContext = yield* Layer.build(
      DeploymentDrain.layer.pipe(Layer.provide(Layer.succeed(Database.Service, database))),
    )
    const drain = Context.get(drainContext, DeploymentDrain.Service)
    const eventsContext = yield* Layer.build(EventV2Bridge.defaultLayer)
    const events = Context.get(eventsContext, EventV2Bridge.Service)
    const storeContext = yield* Layer.build(
      Layer.mock(InstanceStore.Service)({
        provide: (input, effect) =>
          effect.pipe(
            Effect.provideService(InstanceRef, {
              directory: input.directory,
              worktree: input.directory,
              project: {
                id: ProjectV2.ID.make("prj_drain_process"),
                worktree: input.directory,
                time: { created: Date.now(), updated: Date.now() },
                sandboxes: [],
              },
            }),
          ),
      }),
    )
    const store = Context.get(storeContext, InstanceStore.Service)
    const child = Bun.spawn(
      [
        process.execPath,
        "-e",
        'process.stdout.write("ready\\n"); process.stdin.once("data", () => process.exit(0)); process.stdin.resume()',
      ],
      { stdin: "pipe", stdout: "pipe", stderr: "inherit" },
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        child.kill()
        await child.exited
      }).pipe(Effect.ignore),
    )
    const reader = child.stdout.getReader()
    const ready = yield* Effect.promise(async () => {
      const decoder = new TextDecoder()
      let output = ""
      while (!output.includes("\n")) {
        const chunk = await reader.read()
        if (chunk.done) throw new Error("child exited before publishing readiness")
        output += decoder.decode(chunk.value, { stream: true })
      }
      return output
    })
    reader.releaseLock()
    expect(ready).toBe("ready\n")

    const now = Date.now()
    const commandID = "sec_foreign_process"
    const owner = `local:${child.pid}:foreign-run:prompt:test`
    yield* database.db
      .insert(SessionCommandTable)
      .values(command(commandID, temporary.path, "running", owner, now + 60_000, 4))
      .run()
      .pipe(Effect.orDie)

    expect(
      yield* Effect.gen(function* () {
        const scope = yield* Effect.scope
        const claim = yield* PromptClaim.make({ database, events, scope, loop: () => Effect.never })
        return yield* claim.claimCommandTurn(`sec_drain_${commandID}`)
      }).pipe(Effect.scoped),
    ).toEqual({ state: "occupied" })
    expect(yield* drain.replay(drain.runID, store)).toEqual({ runID: drain.runID, directories: [], commandCount: 0 })

    child.stdin.write("exit\n")
    yield* Effect.promise(() => Promise.resolve(child.stdin.flush()))
    child.stdin.end()
    expect(yield* Effect.promise(() => child.exited)).toBe(0)

    expect(yield* drain.replay(drain.runID, store)).toEqual({
      runID: drain.runID,
      directories: [temporary.path],
      commandCount: 1,
    })
    const scope = yield* Effect.scope
    const claim = yield* PromptClaim.make({ database, events, scope, loop: () => Effect.never })
    expect(yield* claim.claimCommandTurn(`sec_drain_${commandID}`)).toMatchObject({
      state: "ready",
      command: { claim_generation: 5 },
    })
    expect(
      yield* database.db
        .select({ owner: SessionCommandTable.owner_id, generation: SessionCommandTable.claim_generation })
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, `sec_drain_${commandID}`))
        .get()
        .pipe(Effect.orDie),
    ).toMatchObject({ generation: 5, owner: claim.commandOwner })
  }),
)

it.live("atomically closes admission and waits for admitted effects", () =>
  Effect.gen(function* () {
    const drain = yield* DeploymentDrain.Service
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    yield* Effect.addFinalizer(() => Effect.sync(DeploymentDrain.resetForTest))
    const admitted = yield* drain
      .admit(Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))))
      .pipe(Effect.forkChild)

    yield* Deferred.await(started)
    expect(yield* drain.begin(drain.runID)).toMatchObject({ inFlightAdmissions: 1, ready: false })
    yield* Effect.addFinalizer(() => drain.cancel(drain.runID).pipe(Effect.ignore))
    expect(Exit.isFailure(yield* drain.admit(Effect.void).pipe(Effect.exit))).toBe(true)

    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(admitted)
    expect(yield* drain.status()).toMatchObject({ inFlightAdmissions: 0, ready: true })
  }),
)

it.live("accepts terminal settlement until readiness seals the drain", () =>
  Effect.gen(function* () {
    const drain = yield* DeploymentDrain.Service
    const { db } = yield* Database.Service
    const started = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    yield* Effect.addFinalizer(() => Effect.sync(DeploymentDrain.resetForTest))
    const now = Date.now()
    yield* db
      .insert(SessionExecutionTable)
      .values({
        session_id: SessionID.make("ses_finishing_drain"),
        project_id: "prj_drain",
        directory: "/finishing",
        state: "running",
        lease_expires_at: now + 60_000,
        time_created: now,
        time_updated: now,
      })
      .run()
      .pipe(Effect.orDie)
    yield* drain.begin(drain.runID)

    const finishing = yield* drain
      .finish(Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))))
      .pipe(Effect.forkChild)
    yield* Deferred.await(started)
    yield* db
      .delete(SessionExecutionTable)
      .where(eq(SessionExecutionTable.session_id, SessionID.make("ses_finishing_drain")))
      .run()
      .pipe(Effect.orDie)
    expect(yield* drain.status()).toMatchObject({ inFlightAdmissions: 1, ready: false })
    yield* Deferred.succeed(release, undefined)
    yield* Fiber.join(finishing)
    expect(yield* drain.status()).toMatchObject({ inFlightAdmissions: 0, ready: true })
    expect(Exit.isFailure(yield* drain.finish(Effect.void).pipe(Effect.exit))).toBe(true)
  }),
)

function command(
  id: string,
  directory: string,
  status: "queued" | "running" | "succeeded",
  owner?: string,
  lease?: number,
  generation?: number,
) {
  const now = Date.now()
  return {
    id: `sec_drain_${id}`,
    session_id: SessionID.make(`ses_drain_${id}`),
    message_id: MessageID.make(`msg_drain_${id}`),
    project_id: "prj_drain",
    directory,
    status,
    owner_id: owner,
    claim_generation: generation,
    lease_expires_at: lease,
    time_created: now,
    time_updated: now,
  }
}
