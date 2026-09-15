import { expect } from "bun:test"
import { Effect } from "effect"
import fs from "fs/promises"
import path from "path"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Global } from "@opencode-ai/core/global"
import * as Log from "@opencode-ai/core/util/log"
import { tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(CrossSpawnSpawner.defaultLayer)

function files(dir: string, expected: number) {
  return Effect.gen(function* () {
    let last = ""
    let same = 0

    for (let i = 0; i < 50; i++) {
      const list = yield* Effect.promise(() => fs.readdir(dir).then((files) => files.sort()))
      const next = JSON.stringify(list)
      same = next === last ? same + 1 : 0
      if (same >= 2 && list.length === expected) return list
      last = next
      yield* Effect.sleep("10 millis")
    }

    return yield* Effect.promise(() => fs.readdir(dir).then((files) => files.sort()))
  })
}

const cold = new Date("2000-06-01T00:00:00Z")

// Twelve timestamped logs whose names sort oldest-first, all with cold mtimes
// so none of them looks like a live process's log.
function seed(dir: string) {
  return Effect.gen(function* () {
    const list = Array.from({ length: 12 }, (_, i) => `2000-01-${String(i + 1).padStart(2, "0")}T000000.log`)
    yield* Effect.all(
      list.map((file) =>
        Effect.promise(async () => {
          const full = path.join(dir, file)
          await fs.writeFile(full, file)
          await fs.utimes(full, cold, cold)
        }),
      ),
    )
    return list
  })
}

it.live("serve init cleanup keeps the newest timestamped logs", () =>
  Effect.gen(function* () {
    const log = Global.Path.log
    yield* Effect.addFinalizer(() => Effect.sync(() => (Global.Path.log = log)))
    const dir = yield* tmpdirScoped()
    Global.Path.log = dir

    const list = yield* seed(dir)

    yield* Effect.promise(() => Log.init({ print: false, dev: false, prune: true }))

    const next = yield* files(dir, 11)

    expect(next).not.toContain(list[0])
    expect(next).not.toContain(list[1])
    expect(next).toContain(list[2])
    expect(next).toContain(list.at(-1)!)
  }),
)

it.live("cleanup never removes a recently written log even when it sorts oldest", () =>
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const list = yield* seed(dir)

    // The oldest-named file is the long-lived daemon: still being written.
    const live = path.join(dir, list[0])
    const now = new Date()
    yield* Effect.promise(() => fs.utimes(live, now, now))

    yield* Effect.promise(() => Log.cleanup(dir))

    const next = yield* Effect.promise(() => fs.readdir(dir).then((files) => files.sort()))
    expect(next).toContain(list[0])
    expect(next).not.toContain(list[1])
    expect(next).toContain(list[2])
    expect(next).toHaveLength(11)
  }),
)

it.live("cleanup with only cold logs leaves exactly the newest ten", () =>
  Effect.gen(function* () {
    const dir = yield* tmpdirScoped()
    const list = yield* seed(dir)

    yield* Effect.promise(() => Log.cleanup(dir))

    const next = yield* Effect.promise(() => fs.readdir(dir).then((files) => files.sort()))
    expect(next).toEqual(list.slice(2))
  }),
)

it.live("init without prune removes nothing from the log dir", () =>
  Effect.gen(function* () {
    const log = Global.Path.log
    yield* Effect.addFinalizer(() => Effect.sync(() => (Global.Path.log = log)))
    const dir = yield* tmpdirScoped()
    Global.Path.log = dir

    const list = yield* seed(dir)

    yield* Effect.promise(() => Log.init({ print: false, dev: false }))

    const next = yield* files(dir, 13)
    for (const file of list) expect(next).toContain(file)
  }),
)

it.live("local dev log is not truncated twice for the same run", () =>
  Effect.gen(function* () {
    const log = Global.Path.log
    const runID = process.env.OPENCODE_RUN_ID
    const initialized = process.env.OPENCODE_LOG_INITIALIZED_RUN_ID
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        Global.Path.log = log
        if (runID === undefined) delete process.env.OPENCODE_RUN_ID
        else process.env.OPENCODE_RUN_ID = runID
        if (initialized === undefined) delete process.env.OPENCODE_LOG_INITIALIZED_RUN_ID
        else process.env.OPENCODE_LOG_INITIALIZED_RUN_ID = initialized
      }),
    )

    const dir = yield* tmpdirScoped()
    Global.Path.log = dir
    process.env.OPENCODE_RUN_ID = "run-1"
    delete process.env.OPENCODE_LOG_INITIALIZED_RUN_ID

    yield* Effect.promise(() => Log.init({ print: false, dev: true }))
    yield* Effect.promise(() => fs.writeFile(path.join(dir, "dev.log"), "main startup\n"))
    yield* Effect.promise(() => Log.init({ print: false, dev: true }))

    expect(yield* Effect.promise(() => fs.readFile(path.join(dir, "dev.log"), "utf8"))).toContain("main startup")
  }),
)
