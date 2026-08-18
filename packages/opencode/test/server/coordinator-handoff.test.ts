import { afterEach, beforeEach, describe, expect, setSystemTime, test } from "bun:test"
import {
  COORDINATOR_MANIFEST_VERSION,
  coordinatorHandoffPath,
  coordinatorManifestPath,
  coordinatorHandoffRequestID,
  readCoordinatorHandoff,
} from "@opencode-ai/sdk/coordinator"
import { OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"
import fs from "node:fs/promises"
import path from "node:path"
import { CoordinatorAuthority } from "../../src/server/coordinator-authority"
import { CoordinatorHandoff } from "../../src/server/coordinator-handoff"
import { tmpdir } from "../fixture/fixture"

const key = "b".repeat(40)
const sourceEpoch = "source-epoch-00000000000000000001"
const targetEpoch = "target-epoch-00000000000000000001"
const capability = "capability-00000000000000000000000000000001"
const requestID = (target = targetEpoch) => coordinatorHandoffRequestID(sourceEpoch, target)

async function publishSource(stateRoot: string) {
  const file = coordinatorManifestPath(stateRoot, key)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(
    file,
    JSON.stringify({
      version: COORDINATOR_MANIFEST_VERSION,
      key,
      directory: "/tmp",
      database: "/tmp/authority-handoff-test.db",
      pid: process.pid,
      url: "http://127.0.0.1:4096/",
      username: "user",
      password: "password",
      token: "token",
      createdAt: "2026-08-18T20:00:00.000Z",
      serverVersion: "local",
      authorityEpoch: sourceEpoch,
      admission: true,
      ready: true,
    }),
  )
}

async function waitForHandoff(stateRoot: string) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const value = await readCoordinatorHandoff(stateRoot, key).catch(() => undefined)
    if (value && "phase" in value) return value
    await Bun.sleep(5)
  }
  throw new Error("handoff was not published")
}

describe.serial("coordinator handoff transition", () => {
  beforeEach(() => {
    process.env[OPENCODE_PROCESS_ROLE] = "coordinator"
    process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = sourceEpoch
    process.env.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY = capability
    delete process.env.OPENCODE_COORDINATOR_HANDOFF_DRAIN_TIMEOUT_MS
    setSystemTime()
    CoordinatorAuthority.resetForTest()
  })

  afterEach(() => {
    setSystemTime()
    CoordinatorHandoff.overrideForTest()
    CoordinatorAuthority.resetForTest()
    delete process.env[OPENCODE_PROCESS_ROLE]
    delete process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH
    delete process.env.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY
    delete process.env.OPENCODE_COORDINATOR_HANDOFF_DRAIN_TIMEOUT_MS
  })

  test("publishes requested revision zero before drain and accepts after release", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    const release = CoordinatorAuthority.acquire("/mutation")!
    const pending = CoordinatorHandoff.request({ request: requestID(), targetEpoch })

    expect(await waitForHandoff(tmp.path)).toMatchObject({ phase: "requested", revision: 0 })
    expect(CoordinatorAuthority.acquire("/second-mutation")).toBeUndefined()
    release()

    expect(await pending).toEqual({ phase: "accepted" })
    expect(await readCoordinatorHandoff(tmp.path, key)).toMatchObject({
      phase: "accepted",
      revision: 1,
      targetEpoch,
    })
    expect(await CoordinatorHandoff.request({ request: requestID(), targetEpoch })).toEqual({ phase: "accepted" })
  })

  test("acceptance timestamp advances when the wall clock is frozen", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    setSystemTime(new Date("2026-08-18T21:00:00.000Z"))

    expect(await CoordinatorHandoff.request({ request: requestID(), targetEpoch })).toEqual({ phase: "accepted" })
    expect(await readCoordinatorHandoff(tmp.path, key)).toMatchObject({
      createdAt: "2026-08-18T21:00:00.000Z",
      updatedAt: "2026-08-18T21:00:00.001Z",
    })
    expect(
      CoordinatorHandoff.strictlyLaterTimestamp("2026-08-18T21:00:00.000Z", Date.parse("2026-08-18T20:00:00.000Z")),
    ).toBe("2026-08-18T21:00:00.001Z")
  })

  test("leaves a timed out published request closed and resumes it", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    process.env.OPENCODE_COORDINATOR_HANDOFF_DRAIN_TIMEOUT_MS = "10"
    const release = CoordinatorAuthority.acquire("/mutation")!

    await expect(CoordinatorHandoff.request({ request: requestID(), targetEpoch })).rejects.toThrow("timed out")
    expect(await readCoordinatorHandoff(tmp.path, key)).toMatchObject({ phase: "requested", revision: 0 })
    expect(CoordinatorAuthority.health()?.admission).toBe(false)

    release()
    const changedTarget = `${targetEpoch}-changed`
    await expect(CoordinatorHandoff.request({ request: requestID(), targetEpoch: changedTarget })).rejects.toThrow(
      "does not match",
    )
    expect(await readCoordinatorHandoff(tmp.path, key)).toMatchObject({ request: requestID(), phase: "requested" })

    expect(await CoordinatorHandoff.request({ request: requestID(), targetEpoch })).toEqual({ phase: "accepted" })
  })

  test("cancellation before publication changes neither durable state nor admission", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    let unblock = () => {}
    let started = () => {}
    const ready = new Promise<void>((resolve) => (started = resolve))
    const blocker = CoordinatorAuthority.serialized(
      () =>
        new Promise<void>((resolve) => {
          unblock = resolve
          started()
        }),
    )
    await ready
    const controller = new AbortController()
    const pending = CoordinatorHandoff.request({
      request: requestID(),
      targetEpoch,
      signal: controller.signal,
    })
    controller.abort()
    unblock()
    await blocker

    await expect(pending).rejects.toThrow()
    expect(await Bun.file(coordinatorHandoffPath(tmp.path, key)).exists()).toBe(false)
    expect(CoordinatorAuthority.health()?.admission).toBe(true)
  })

  test("cancellation after publication remains closed and exact abort reopens", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    const release = CoordinatorAuthority.acquire("/mutation")!
    const controller = new AbortController()
    const pending = CoordinatorHandoff.request({
      request: requestID(),
      targetEpoch,
      signal: controller.signal,
    })
    const requested = await waitForHandoff(tmp.path)
    controller.abort()

    await expect(pending).rejects.toThrow()
    expect(CoordinatorAuthority.health()?.admission).toBe(false)
    await CoordinatorHandoff.abort({
      expected: {
        request: requestID(),
        phase: "requested",
        revision: 0,
        sourceEpoch,
        targetEpoch: undefined,
      },
    })
    expect(CoordinatorAuthority.health()?.admission).toBe(true)
    expect(await Bun.file(coordinatorHandoffPath(tmp.path, key)).exists()).toBe(false)
    release()
  })

  test("serializes concurrent winner and loser without reopening the winner gate", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    const release = CoordinatorAuthority.acquire("/mutation")!
    const otherTarget = `${targetEpoch}-other`
    const winner = CoordinatorHandoff.request({ request: requestID(), targetEpoch })
    const loser = CoordinatorHandoff.request({ request: requestID(otherTarget), targetEpoch: otherTarget })
    await waitForHandoff(tmp.path)
    release()

    expect(await winner).toEqual({ phase: "accepted" })
    await expect(loser).rejects.toThrow("already exists")
    expect(CoordinatorAuthority.health()?.admission).toBe(false)
  })

  test("serializes a requested retry ahead of a racing abort", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path)
    CoordinatorHandoff.overrideForTest({ key, stateRoot: tmp.path })
    process.env.OPENCODE_COORDINATOR_HANDOFF_DRAIN_TIMEOUT_MS = "10"
    const release = CoordinatorAuthority.acquire("/mutation")!
    await expect(CoordinatorHandoff.request({ request: requestID(), targetEpoch })).rejects.toThrow("timed out")
    release()

    const retry = CoordinatorHandoff.request({ request: requestID(), targetEpoch })
    const racingAbort = CoordinatorHandoff.abort({
      expected: {
        request: requestID(),
        phase: "requested",
        revision: 0,
        sourceEpoch,
        targetEpoch: undefined,
      },
    })

    expect(await retry).toEqual({ phase: "accepted" })
    await expect(racingAbort).rejects.toThrow("changed")
    expect(await readCoordinatorHandoff(tmp.path, key)).toMatchObject({ phase: "accepted", revision: 1 })
    expect(CoordinatorAuthority.health()?.admission).toBe(false)
  })

  test("checks the dedicated capability with bounded constant-time digests", () => {
    expect(CoordinatorHandoff.available()).toBe(true)
    expect(CoordinatorHandoff.authorized(capability)).toBe(true)
    expect(CoordinatorHandoff.authorized(`${capability}x`)).toBe(false)
    expect(CoordinatorHandoff.authorized(undefined)).toBe(false)
    process.env.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY = "short"
    expect(CoordinatorHandoff.available()).toBe(false)
    expect(CoordinatorHandoff.authorized("short")).toBe(false)
  })
})
