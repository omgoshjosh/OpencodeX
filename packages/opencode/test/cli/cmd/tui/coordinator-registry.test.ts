import { describe, expect, test } from "bun:test"
import {
  COORDINATOR_HANDOFF_VERSION,
  coordinatorHandoffPath,
  readCoordinatorHandoff,
  type CoordinatorHandoffRecord,
} from "@opencode-ai/sdk/coordinator"
import fs from "node:fs/promises"
import path from "node:path"
import { compareAndSwapCoordinatorHandoff } from "../../../../src/cli/cmd/tui/coordinator-registry"
import { tmpdir } from "../../../fixture/fixture"

function record(request: string, sourceToken: string): CoordinatorHandoffRecord {
  return { version: COORDINATOR_HANDOFF_VERSION, request, sourceToken }
}

describe("coordinator handoff compare-and-swap", () => {
  const key = "a".repeat(40)

  test("creates only when absence is expected", async () => {
    await using tmp = await tmpdir()
    const first = record("request-1", "source-1")

    expect(await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)).toBe(true)
    expect(await compareAndSwapCoordinatorHandoff(key, undefined, record("request-2", "source-2"), tmp.path)).toBe(
      false,
    )
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(first)
    expect((await fs.stat(coordinatorHandoffPath(tmp.path, key))).mode & 0o777).toBe(0o600)
  })

  test("replaces and deletes only on an exact request and source-token match", async () => {
    await using tmp = await tmpdir()
    const first = record("request-1", "source-1")
    const second = record("request-2", "source-2")
    await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)

    expect(
      await compareAndSwapCoordinatorHandoff(key, { request: first.request, sourceToken: "wrong" }, second, tmp.path),
    ).toBe(false)
    expect(
      await compareAndSwapCoordinatorHandoff(
        key,
        { request: "wrong", sourceToken: first.sourceToken },
        second,
        tmp.path,
      ),
    ).toBe(false)
    expect(await compareAndSwapCoordinatorHandoff(key, first, second, tmp.path)).toBe(true)
    expect(await compareAndSwapCoordinatorHandoff(key, first, undefined, tmp.path)).toBe(false)
    expect(await compareAndSwapCoordinatorHandoff(key, second, undefined, tmp.path)).toBe(true)
    expect(await Bun.file(coordinatorHandoffPath(tmp.path, key)).exists()).toBe(false)
  })

  test("fails closed without changing malformed state", async () => {
    await using tmp = await tmpdir()
    const file = coordinatorHandoffPath(tmp.path, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, "{malformed")

    await expect(
      compareAndSwapCoordinatorHandoff(key, undefined, record("request", "source"), tmp.path),
    ).rejects.toThrow()
    await expect(
      compareAndSwapCoordinatorHandoff(key, { request: "request", sourceToken: "source" }, undefined, tmp.path),
    ).rejects.toThrow()
    expect(await fs.readFile(file, "utf8")).toBe("{malformed")
  })

  test("allows exactly one concurrent absent-to-create contender", async () => {
    await using tmp = await tmpdir()
    const contenders = Array.from({ length: 12 }, (_, index) => record(`request-${index}`, `source-${index}`))
    const results = await Promise.all(
      contenders.map((replacement) => compareAndSwapCoordinatorHandoff(key, undefined, replacement, tmp.path)),
    )

    expect(results.filter(Boolean)).toHaveLength(1)
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(contenders[results.indexOf(true)])
  })

  test("rejects path aliases and strips additive fields before persistence", async () => {
    await using tmp = await tmpdir()
    expect(() => compareAndSwapCoordinatorHandoff("alias/../key", undefined, record("request", "source"), tmp.path)).toThrow(
      "Invalid coordinator key",
    )

    const replacement = { ...record("request", "source"), password: "must-not-persist" }
    expect(await compareAndSwapCoordinatorHandoff(key, undefined, replacement, tmp.path)).toBe(true)
    expect(JSON.parse(await fs.readFile(coordinatorHandoffPath(tmp.path, key), "utf8"))).toEqual(
      record("request", "source"),
    )
  })
})
