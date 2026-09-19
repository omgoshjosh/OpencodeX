import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import path from "path"
import { open, rename, rm, writeFile, readdir, mkdir, stat } from "node:fs/promises"
import { Auth } from "../../src/auth"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { EventV2 } from "@opencode-ai/core/event"
import { testEffect } from "../lib/effect"
import { TestInstance } from "../fixture/fixture"

const node = CrossSpawnSpawner.defaultLayer

const it = testEffect(node)

const isolated = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const test = yield* TestInstance
    return yield* effect.pipe(
      Effect.provide(
        Auth.layer(path.join(test.directory, "auth.json")).pipe(
          Layer.provide(AppFileSystem.defaultLayer),
          Layer.provide(EventV2.defaultLayer),
        ),
      ),
    )
  })

const isolatedWith = <A, E, R>(hooks: Auth.LayerTestHooks, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const test = yield* TestInstance
    return yield* effect.pipe(
      Effect.provide(
        Auth.layer(path.join(test.directory, "auth.json"), hooks).pipe(
          Layer.provide(AppFileSystem.defaultLayer),
          Layer.provide(EventV2.defaultLayer),
        ),
      ),
    )
  })

const waitFor = async (file: string) => {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (await Bun.file(file).exists()) return
    await Bun.sleep(10)
  }
  throw new Error(`timed out waiting for ${file}`)
}

const childMutation = (authFile: string, started: string, key: string, remove = false) =>
  Bun.spawn(
    [
      "bun",
      "-e",
      `import { Effect, Layer } from "effect"; import { Auth } from ${JSON.stringify(path.join(import.meta.dir, "../../src/auth"))}; import { AppFileSystem } from "@opencode-ai/core/filesystem"; import { EventV2 } from "@opencode-ai/core/event"; await Bun.write(${JSON.stringify(started)}, "started"); const layer = Auth.layer(${JSON.stringify(authFile)}).pipe(Layer.provide(AppFileSystem.defaultLayer), Layer.provide(EventV2.defaultLayer)); await Effect.runPromise(Effect.scoped(Effect.gen(function* () { const auth = yield* Auth.Service; ${remove ? `yield* auth.remove(${JSON.stringify(key)})` : `yield* auth.set(${JSON.stringify(key)}, { type: "api", key: "child" })`} }).pipe(Effect.provide(layer))));`,
    ],
    { cwd: path.join(import.meta.dir, "../.."), stdout: "pipe", stderr: "pipe" },
  )

describe("Auth", () => {
  it.instance("set normalizes trailing slashes in keys", () =>
    isolated(
      Effect.gen(function* () {
        const auth = yield* Auth.Service
        yield* auth.set("https://example.com/", {
          type: "wellknown",
          key: "TOKEN",
          token: "abc",
        })
        const data = yield* auth.all()
        expect(data["https://example.com"]).toBeDefined()
        expect(data["https://example.com/"]).toBeUndefined()
      }),
    ),
  )

  it.instance("set cleans up pre-existing trailing-slash entry", () =>
    isolated(
      Effect.gen(function* () {
        const auth = yield* Auth.Service
        yield* auth.set("https://example.com/", {
          type: "wellknown",
          key: "TOKEN",
          token: "old",
        })
        yield* auth.set("https://example.com", {
          type: "wellknown",
          key: "TOKEN",
          token: "new",
        })
        const data = yield* auth.all()
        const keys = Object.keys(data).filter((key) => key.includes("example.com"))
        expect(keys).toEqual(["https://example.com"])
        const entry = data["https://example.com"]!
        expect(entry.type).toBe("wellknown")
        if (entry.type === "wellknown") expect(entry.token).toBe("new")
      }),
    ),
  )

  it.instance("remove deletes both trailing-slash and normalized keys", () =>
    isolated(
      Effect.gen(function* () {
        const auth = yield* Auth.Service
        yield* auth.set("https://example.com", {
          type: "wellknown",
          key: "TOKEN",
          token: "abc",
        })
        yield* auth.remove("https://example.com/")
        const data = yield* auth.all()
        expect(data["https://example.com"]).toBeUndefined()
        expect(data["https://example.com/"]).toBeUndefined()
      }),
    ),
  )

  it.instance("set and remove are no-ops on keys without trailing slashes", () =>
    isolated(
      Effect.gen(function* () {
        const auth = yield* Auth.Service
        yield* auth.set("anthropic", {
          type: "api",
          key: "sk-test",
        })
        const data = yield* auth.all()
        expect(data["anthropic"]).toBeDefined()
        yield* auth.remove("anthropic")
        const after = yield* auth.all()
        expect(after["anthropic"]).toBeUndefined()
      }),
    ),
  )

  it.instance("retains the last valid snapshot through invalid external writes", () =>
    isolated(
      Effect.gen(function* () {
        const test = yield* TestInstance
        const auth = yield* Auth.Service
        yield* auth.set("anthropic", { type: "api", key: "before" })
        const initial = yield* auth.snapshot()

        const authPath = path.join(test.directory, "auth.json")
        yield* Effect.promise(() => Bun.write(authPath, "{"))
        const malformed = yield* auth.snapshot()
        expect(malformed).toEqual(initial)

        yield* Effect.promise(() => Bun.write(authPath, JSON.stringify({ anthropic: { type: "api" } })))
        const invalid = yield* auth.snapshot()
        expect(invalid).toEqual(initial)

        yield* Effect.promise(() => Bun.write(authPath, JSON.stringify({ anthropic: { type: "api", key: "after" } })))
        const next = yield* auth.snapshot()
        expect(next.revision).not.toBe(initial.revision)
        expect(next.records.anthropic).toMatchObject({ type: "api", key: "after" })
      }),
    ),
  )

  it.instance("merges locked mutations with the latest valid external snapshot", () =>
    isolated(
      Effect.gen(function* () {
        const test = yield* TestInstance
        const auth = yield* Auth.Service
        const authPath = path.join(test.directory, "auth.json")
        yield* auth.set("target", { type: "api", key: "old" })

        yield* Effect.promise(() =>
          Bun.write(
            authPath,
            JSON.stringify({ external: { type: "api", key: "external" }, target: { type: "api", key: "stale" } }),
          ),
        )
        yield* auth.set("target", { type: "api", key: "new" })
        expect((yield* auth.all()).external).toMatchObject({ key: "external" })
        expect((yield* auth.all()).target).toMatchObject({ key: "new" })

        yield* Effect.promise(() => Bun.write(authPath, "{"))
        yield* auth.remove("target")
        expect((yield* auth.all()).external).toMatchObject({ key: "external" })
        expect((yield* auth.all()).target).toBeUndefined()

        yield* Effect.promise(() => Bun.write(authPath, JSON.stringify({ external: { type: "api" } })))
        yield* auth.set("replacement", { type: "api", key: "replacement" })
        expect((yield* auth.all()).external).toMatchObject({ key: "external" })
        expect((yield* auth.all()).replacement).toMatchObject({ key: "replacement" })
      }),
    ),
  )

  it.instance("serializes cooperating child set and remove writers through the file lock", () =>
    isolated(
      Effect.gen(function* () {
        const test = yield* TestInstance
        const authFile = path.join(test.directory, "auth.json")
        const lockFile = `${authFile}.lock`
        const lock = yield* Effect.promise(() => open(lockFile, "wx", 0o600))
        const started = path.join(test.directory, "child.started")
        const child = childMutation(authFile, started, "child")
        yield* Effect.promise(() => waitFor(started))
        expect(yield* Effect.promise(() => Bun.file(authFile).exists())).toBe(false)
        yield* Effect.promise(async () => {
          const temporary = `${authFile}.parent.tmp`
          await writeFile(temporary, JSON.stringify({ parent: { type: "api", key: "parent" } }), { mode: 0o600 })
          await rename(temporary, authFile)
          await lock.close()
          await rm(lockFile)
          await child.exited
        })
        expect(yield* Effect.promise(() => Bun.file(authFile).json())).toEqual({
          parent: { type: "api", key: "parent" },
          child: { type: "api", key: "child" },
        })

        const secondLock = yield* Effect.promise(() => open(lockFile, "wx", 0o600))
        const removeStarted = path.join(test.directory, "remove.started")
        const removeChild = childMutation(authFile, removeStarted, "child", true)
        yield* Effect.promise(() => waitFor(removeStarted))
        yield* Effect.promise(async () => {
          const temporary = `${authFile}.parent-remove.tmp`
          await writeFile(
            temporary,
            JSON.stringify({ parent: { type: "api", key: "parent" }, child: { type: "api", key: "stale" } }),
            { mode: 0o600 },
          )
          await rename(temporary, authFile)
          await secondLock.close()
          await rm(lockFile)
          await removeChild.exited
        })
        expect(yield* Effect.promise(() => Bun.file(authFile).json())).toEqual({
          parent: { type: "api", key: "parent" },
        })
      }),
    ),
  )

  it.instance(
    "times out cleanly on a preexisting auth lock and recovers",
    () =>
      isolated(
        Effect.gen(function* () {
          const test = yield* TestInstance
          const auth = yield* Auth.Service
          const authFile = path.join(test.directory, "auth.json")
          const lock = yield* Effect.promise(() => open(`${authFile}.lock`, "wx", 0o600))
          const exit = yield* auth.set("blocked", { type: "api", key: "sentinel-timeout-secret" }).pipe(Effect.exit)
          expect(exit._tag).toBe("Failure")
          expect(JSON.stringify(exit)).not.toContain("sentinel-timeout-secret")
          expect(yield* Effect.promise(() => readdir(test.directory))).not.toContain("auth.json.tmp")
          yield* Effect.promise(async () => {
            await lock.close()
            await rm(`${authFile}.lock`)
          })
          yield* auth.set("recovered", { type: "api", key: "ok" })
          expect((yield* auth.all()).recovered).toMatchObject({ key: "ok" })
        }),
      ),
    { timeout: 10_000 },
  )

  it.instance("cleans lock and temp after rename failure and creates 0600 files", () => {
    let temporary = ""
    return isolatedWith(
      {
        afterTempOpen: async (file) => {
          temporary = file
          if (process.platform !== "win32") expect((await stat(file)).mode & 0o777).toBe(0o600)
        },
      },
      Effect.gen(function* () {
        const test = yield* TestInstance
        const auth = yield* Auth.Service
        const authFile = path.join(test.directory, "auth.json")
        yield* Effect.promise(() => mkdir(authFile))
        expect((yield* auth.set("fails", { type: "api", key: "secret" }).pipe(Effect.exit))._tag).toBe("Failure")
        expect(yield* Effect.promise(() => Bun.file(`${authFile}.lock`).exists())).toBe(false)
        expect(yield* Effect.promise(() => Bun.file(temporary).exists())).toBe(false)
        yield* Effect.promise(() => rm(authFile, { recursive: true }))
        yield* auth.set("works", { type: "api", key: "ok" })
        if (process.platform !== "win32") expect((yield* Effect.promise(() => stat(authFile))).mode & 0o777).toBe(0o600)
        expect((yield* auth.all()).works).toMatchObject({ key: "ok" })
      }),
    )
  })

  it.instance("treats non-cooperating atomic rename as last-writer-wins whole snapshots", () => {
    const observed: string[] = []
    return isolatedWith(
      {},
      Effect.gen(function* () {
        const test = yield* TestInstance
        const authFile = path.join(test.directory, "auth.json")
        const auth = yield* Auth.Service
        yield* auth.set("initial", { type: "api", key: "initial" })
        observed.push(yield* Effect.promise(() => Bun.file(authFile).text()))
        yield* Effect.promise(() => {
          const temporary = `${authFile}.external.tmp`
          return writeFile(temporary, JSON.stringify({ external: { type: "api", key: "external" } }), {
            mode: 0o600,
          }).then(() => rename(temporary, authFile))
        })
        observed.push(yield* Effect.promise(() => Bun.file(authFile).text()))
        yield* auth.set("internal", { type: "api", key: "internal" })
        observed.push(yield* Effect.promise(() => Bun.file(authFile).text()))
        expect(observed.every((content) => typeof JSON.parse(content) === "object")).toBe(true)
        expect(yield* Effect.promise(() => Bun.file(authFile).json())).toEqual({
          external: { type: "api", key: "external" },
          internal: { type: "api", key: "internal" },
        })
      }),
    )
  })
})
