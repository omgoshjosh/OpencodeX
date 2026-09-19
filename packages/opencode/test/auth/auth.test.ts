import { describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import path from "path"
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
})
