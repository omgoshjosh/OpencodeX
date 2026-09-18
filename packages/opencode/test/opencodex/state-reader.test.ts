import { afterEach, describe, expect } from "bun:test"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { OpencodeXStateEventTable } from "@opencode-ai/core/opencodex/sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { eq, max } from "drizzle-orm"
import { Context, Deferred, Duration, Effect, Fiber, Layer } from "effect"
import path from "path"
import { Permission } from "../../src/permission"
import { Question } from "../../src/question"
import { InstanceBootstrap } from "../../src/project/bootstrap-service"
import { InstanceStore } from "../../src/project/instance-store"
import { SessionID } from "../../src/session/schema"
import { Session } from "../../src/session/session"
import { SessionStatus } from "../../src/session/status"
import { Todo } from "../../src/session/todo"
import { OpencodeXGoal } from "../../src/opencodex/goal"
import { OpencodeXJob } from "../../src/opencodex/job"
import { OpencodeXProject } from "../../src/opencodex/project"
import { OpencodeXSessionState } from "../../src/opencodex/session-state"
import { OpencodeXSwarm } from "../../src/opencodex/swarm"
import { OpencodeXTerminalSession } from "../../src/opencodex/terminal-session"
import { OpencodeXView } from "../../src/opencodex/view"
import { makeStateLog } from "../../src/opencodex/state-log"
import { makeStateReader } from "../../src/opencodex/state-reader"
import { disposeAllInstances, requireInstance, TestInstance } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { testEffect } from "../lib/effect"

/**
 * OpencodeX-fs2 PR2: the state reader no longer freezes every writer behind
 * `events.barrier` while it reads. Its own statements run on `Database.read`,
 * and the revision re-check loop is what keeps a snapshot's cursor honest: a
 * cursor may never advertise a write the payload does not contain. Built on a
 * real file database, because `:memory:` aliases `read` to `db` and takes the
 * old barrier path.
 */

const noopBootstrap = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const it = testEffect(
  Layer.mergeAll(
    AppFileSystem.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    InstanceStore.defaultLayer.pipe(Layer.provide(noopBootstrap)),
    Database.defaultLayer,
  ),
)

const Bump = EventV2.define({ type: "session.fs2_test_bump", schema: { sessionID: SessionID } })
const sessionID = SessionID.make("ses_fs2_reader")
const WRITES = 40

const empty =
  <A>(value: A) =>
  () =>
    Effect.succeed(value)
const stub = <S extends Context.Service<any, any>>(service: S, shape: Partial<Context.Service.Shape<S>>) =>
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- each stub is the subset of its interface the reader calls; the widening is the point.
  Layer.succeed(service, shape as Context.Service.Shape<S>)
/** Only the session-card scan reads the file database; every other producer is inert. */
const producers = Layer.mergeAll(
  stub(OpencodeXProject.Service, { listCatalog: empty([]) }),
  stub(OpencodeXJob.Service, { list: empty([]) }),
  stub(Session.Service, {}),
  stub(OpencodeXGoal.ReadService, { list: empty([]) }),
  stub(OpencodeXSwarm.ReadService, { list: empty([]) }),
  stub(OpencodeXTerminalSession.Service, { list: empty([]) }),
  stub(OpencodeXView.Service, { listCatalog: empty([]) }),
  stub(SessionStatus.Service, { list: empty(new Map()) }),
  stub(Permission.Service, { list: empty([]) }),
  stub(Question.Service, { list: empty([]) }),
  stub(OpencodeXSessionState.Service, { list: empty({}) }),
  stub(Todo.Service, { get: empty([]) }),
)

const graph = Effect.fnUntraced(function* (filename: string) {
  const database = Context.get(yield* Layer.build(Layer.fresh(Database.layerFromPath(filename))), Database.Service)
  expect(database.read).not.toBe(database.db)
  const events = Context.get(
    yield* Layer.build(EventV2.layer.pipe(Layer.provide(Layer.succeed(Database.Service, database)))),
    EventV2.Service,
  )
  const log = yield* makeStateLog(database.db, events, { read: database.read })
  const reader = yield* makeStateReader(database, events, log).pipe(Effect.provide(producers))
  // `read === db` is the kill-switch shape: the reader takes the barrier as it always did.
  const barriered = yield* makeStateReader({ db: database.db, read: database.db }, events, log).pipe(
    Effect.provide(producers),
  )
  const instance = yield* requireInstance
  const now = Date.now()
  yield* database.db
    .insert(ProjectTable)
    .values({ id: ProjectV2.ID.make(instance.project.id), worktree: instance.directory, sandboxes: [] })
    .onConflictDoNothing()
  yield* database.db.insert(SessionTable).values({
    id: sessionID,
    project_id: ProjectV2.ID.make(instance.project.id),
    slug: "fs2-reader",
    directory: instance.directory,
    title: "v0",
    version: "test",
    time_created: now,
    time_updated: now,
  })
  return { database, events, reader, barriered }
})

/** Title `v<k>` of the only card is the payload's version; the cursor carries the log position. */
function observe(snapshot: {
  cursor: string
  payloads: { catalog: { sessionCards: { items: { title: string }[] } } }
}) {
  const card = snapshot.payloads.catalog.sessionCards.items.find((item) => item.title.startsWith("v"))
  const position = Number(JSON.parse(Buffer.from(snapshot.cursor, "base64url").toString()).position)
  return { version: Number(card?.title.slice(1) ?? -1), position }
}

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

describe("OpencodeX state reader off the barrier", () => {
  it.instance("a snapshot cursor never advertises a write its payload lacks under a concurrent writer", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { database, events, reader, barriered } = yield* graph(path.join(test.directory, "state-reader.db"))
      // Log position of write k's event, filled in as the writer commits.
      const positions: number[] = []
      const done = yield* Deferred.make<void>()
      const writer = yield* Effect.forkChild(
        Effect.forEach(
          Array.from({ length: WRITES }, (_, index) => index + 1),
          (version) =>
            // Row and state event commit together, as Session.patch does.
            events
              .barrier(
                database.db.transaction(
                  (tx) =>
                    Effect.gen(function* () {
                      yield* tx
                        .update(SessionTable)
                        .set({ title: `v${version}`, time_updated: Date.now() })
                        .where(eq(SessionTable.id, sessionID))
                        .run()
                      yield* events.commit(Bump, { sessionID })
                      const row = yield* tx
                        .select({ position: max(OpencodeXStateEventTable.position) })
                        .from(OpencodeXStateEventTable)
                        .get()
                      positions[version] = row?.position ?? 0
                    }),
                  { behavior: "immediate" },
                ),
              )
              .pipe(Effect.orDie, Effect.andThen(Effect.sleep(Duration.millis(2)))),
          { discard: true },
        ).pipe(Effect.ensuring(Deferred.succeed(done, undefined))),
      )
      const observed: { version: number; position: number }[] = []
      while (!(yield* Deferred.isDone(done))) observed.push(observe(yield* reader.snapshot()))
      yield* Fiber.join(writer)
      expect(observed.length).toBeGreaterThan(1)
      // The reader really interleaved with the writer rather than running before or after it.
      expect(new Set(observed.map((item) => item.version)).size).toBeGreaterThan(1)
      for (const [index, item] of observed.entries()) {
        expect(item.version).toBeGreaterThanOrEqual(0)
        // Never regresses: the next write's event lies strictly past the cursor.
        if (item.version < WRITES) expect(item.position).toBeLessThan(positions[item.version + 1])
        // Monotone across consecutive snapshots.
        if (index > 0) expect(item.position).toBeGreaterThanOrEqual(observed[index - 1].position)
      }
      // Quiescent: byte-for-byte what a barriered read of the same revision returns.
      const settled = yield* reader.snapshot()
      const reference = yield* barriered.snapshot()
      expect(observe(settled)).toEqual({ version: WRITES, position: positions[WRITES] })
      expect(settled.cursor).toBe(reference.cursor)
      expect(settled.digest).toBe(reference.digest)
      expect(settled.domains).toEqual(reference.domains)
    }),
  )

  it.instance("a snapshot completes while another fiber holds the barrier", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const { events, reader, barriered } = yield* graph(path.join(test.directory, "state-reader-barrier.db"))
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* Effect.forkChild(
        events.barrier(Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))), "test:hold"),
      )
      yield* Deferred.await(held)
      const snapshot = yield* reader.snapshot().pipe(Effect.timeoutOption(Duration.seconds(2)))
      expect(snapshot._tag).toBe("Some")
      // The kill-switch shape still queues behind the holder.
      const blocked = yield* barriered.snapshot().pipe(Effect.timeoutOption(Duration.millis(200)))
      expect(blocked._tag).toBe("None")
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(holder)
    }),
  )
})
