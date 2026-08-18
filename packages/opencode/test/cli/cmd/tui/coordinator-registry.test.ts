import { describe, expect, test } from "bun:test"
import {
  COORDINATOR_MANIFEST_VERSION,
  COORDINATOR_HANDOFF_VERSION,
  coordinatorHandoffPath,
  coordinatorManifestPath,
  readCoordinatorHandoff,
  withCoordinatorAuthorityLock,
  type CoordinatorHandoffRecord,
  type CoordinatorManifest,
} from "@opencode-ai/sdk/coordinator"
import fs from "node:fs/promises"
import path from "node:path"
import {
  compareAndSwapCoordinatorHandoff,
  retireCoordinatorForIdleShutdown,
  readActiveCoordinator,
  readActiveManifest,
} from "../../../../src/cli/cmd/tui/coordinator-registry"
import { tmpdir } from "../../../fixture/fixture"

function record(request: string, sourceEpoch: string): CoordinatorHandoffRecord {
  return {
    version: COORDINATOR_HANDOFF_VERSION,
    request,
    phase: "requested",
    revision: 0,
    sourceEpoch,
    createdAt: "2026-08-18T20:00:00.000Z",
    updatedAt: "2026-08-18T20:00:00.000Z",
  }
}

function accepted(value: CoordinatorHandoffRecord): CoordinatorHandoffRecord {
  return {
    ...value,
    phase: "accepted",
    revision: 1,
    targetEpoch: "target-1",
    updatedAt: "2026-08-18T20:00:01.000Z",
  }
}

function match(value: CoordinatorHandoffRecord) {
  return {
    request: value.request,
    phase: value.phase,
    revision: value.revision,
    sourceEpoch: value.sourceEpoch,
    targetEpoch: value.targetEpoch,
  }
}

async function publishSource(stateRoot: string, key: string, sourceEpoch: string) {
  const file = coordinatorManifestPath(stateRoot, key)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify({
    version: COORDINATOR_MANIFEST_VERSION,
    key,
    directory: "/tmp",
    database: "/tmp/authority-handoff.db",
    pid: process.pid,
    url: "http://127.0.0.1:4096/",
    username: "user",
    password: "password",
    token: "source-token",
    createdAt: "2026-08-18T20:00:00.000Z",
    serverVersion: "local",
    authorityEpoch: sourceEpoch,
    admission: true,
    ready: true,
  }))
}

describe("active coordinator manifest", () => {
  const key = "b".repeat(40)
  const database = "/tmp/authority-handoff.db"
  const invalid = {
    version: COORDINATOR_MANIFEST_VERSION,
    key,
    directory: "/tmp",
    database,
    pid: process.pid,
    url: "http://127.0.0.1:4096/",
    username: "user",
    password: "password",
    token: "live-token",
    createdAt: "2026-08-18T20:00:00.000Z",
    serverVersion: "1.2.3",
    authorityEpoch: "",
  }

  test("readActiveManifest fails closed without deleting an invalid manifest", async () => {
    await using tmp = await tmpdir()
    const file = coordinatorManifestPath(tmp.path, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify(invalid))

    await expect(readActiveManifest(key, tmp.path)).rejects.toThrow("refusing to replace")
    expect(JSON.parse(await fs.readFile(file, "utf8"))).toEqual(invalid)
  })

  test("readActiveCoordinator preserves an invalid live manifest", async () => {
    await using tmp = await tmpdir()
    const file = coordinatorManifestPath(tmp.path, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify(invalid))

    await expect(readActiveCoordinator(key, database, tmp.path)).rejects.toThrow("refusing to replace")
    expect(JSON.parse(await fs.readFile(file, "utf8"))).toEqual(invalid)
  })

  test("attaches to a healthy source before commit and waits for the target once ready", async () => {
    await using tmp = await tmpdir()
    const file = coordinatorManifestPath(tmp.path, key)
    const handoffFile = coordinatorHandoffPath(tmp.path, key)
    let epoch = "source-1"
    const server = Bun.serve({
      port: 0,
      fetch: () =>
        Response.json({ healthy: true, version: "local", authorityEpoch: epoch, admission: false, ready: true }),
    })
    const manifest = {
      ...invalid,
      authorityEpoch: epoch,
      admission: false,
      ready: true,
      url: server.url.href,
      serverVersion: "local",
    }
    const requested = record("request-1", epoch)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify(manifest))
    await fs.writeFile(handoffFile, JSON.stringify(requested))

    try {
      expect(await readActiveCoordinator(key, database, tmp.path)).toMatchObject({ authorityEpoch: "source-1" })

      const ready = {
        ...accepted(requested),
        phase: "ready" as const,
        revision: 2,
        updatedAt: "2026-08-18T20:00:02.000Z",
      }
      await fs.writeFile(handoffFile, JSON.stringify(ready))
      await expect(readActiveCoordinator(key, database, tmp.path)).rejects.toThrow("incompatible_handoff")
      expect(await Bun.file(file).exists()).toBe(true)

      epoch = "target-1"
      await fs.writeFile(file, JSON.stringify({ ...manifest, authorityEpoch: epoch, admission: true }))
      expect(await readActiveCoordinator(key, database, tmp.path)).toMatchObject({ authorityEpoch: "target-1" })
    } finally {
      await server.stop(true)
    }
  })

  test("malformed handoff suppresses dead-manifest cleanup", async () => {
    await using tmp = await tmpdir()
    const file = coordinatorManifestPath(tmp.path, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify({ ...invalid, authorityEpoch: "source-1", pid: 2_147_483_647 }))
    await fs.writeFile(coordinatorHandoffPath(tmp.path, key), "{malformed")

    await expect(readActiveCoordinator(key, database, tmp.path)).rejects.toThrow("malformed_handoff")
    expect(await Bun.file(file).exists()).toBe(true)
  })
})

describe("coordinator handoff compare-and-swap", () => {
  const key = "a".repeat(40)

  test("creates only when absence is expected", async () => {
    await using tmp = await tmpdir()
    const first = record("request-1", "source-1")
    await publishSource(tmp.path, key, first.sourceEpoch)

    expect(await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)).toBe(true)
    expect(await compareAndSwapCoordinatorHandoff(key, undefined, record("request-2", "source-2"), tmp.path)).toBe(
      false,
    )
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(first)
    expect((await fs.stat(coordinatorHandoffPath(tmp.path, key))).mode & 0o777).toBe(0o600)
  })

  test("replaces and deletes only on an exact authority fence after target selection", async () => {
    await using tmp = await tmpdir()
    const first = record("request-1", "source-1")
    const second = accepted(first)
    await publishSource(tmp.path, key, first.sourceEpoch)
    await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)
    expect(await compareAndSwapCoordinatorHandoff(key, first, undefined, tmp.path)).toBe(false)

    expect(
      await compareAndSwapCoordinatorHandoff(key, { ...match(first), sourceEpoch: "wrong" }, second, tmp.path),
    ).toBe(false)
    expect(await compareAndSwapCoordinatorHandoff(key, { ...match(first), request: "wrong" }, second, tmp.path)).toBe(
      false,
    )
    expect(await compareAndSwapCoordinatorHandoff(key, { ...match(first), revision: 1 }, second, tmp.path)).toBe(false)
    expect(await compareAndSwapCoordinatorHandoff(key, first, second, tmp.path)).toBe(true)
    expect(await compareAndSwapCoordinatorHandoff(key, first, undefined, tmp.path)).toBe(false)
    expect(await compareAndSwapCoordinatorHandoff(key, { ...match(second), phase: "ready" }, undefined, tmp.path)).toBe(
      false,
    )
    expect(
      await compareAndSwapCoordinatorHandoff(
        key,
        { ...match(second), targetEpoch: "losing-target" },
        undefined,
        tmp.path,
      ),
    ).toBe(false)
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
      compareAndSwapCoordinatorHandoff(key, match(record("request", "source")), undefined, tmp.path),
    ).rejects.toThrow()
    expect(await fs.readFile(file, "utf8")).toBe("{malformed")
  })

  test("allows exactly one concurrent absent-to-create contender", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path, key, "source")
    const contenders = Array.from({ length: 12 }, (_, index) => record(`request-${index}`, "source"))
    const results = await Promise.all(
      contenders.map((replacement) => compareAndSwapCoordinatorHandoff(key, undefined, replacement, tmp.path)),
    )

    expect(results.filter(Boolean)).toHaveLength(1)
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(contenders[results.indexOf(true)])
  })

  test("allows exactly one concurrent revision transition and rejects replay", async () => {
    await using tmp = await tmpdir()
    const first = record("request", "source")
    await publishSource(tmp.path, key, first.sourceEpoch)
    await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)
    const replacements = Array.from({ length: 12 }, (_, index) => ({
      ...accepted(first),
      targetEpoch: `target-${index}`,
    }))
    const expected = match(first)
    const results = await Promise.all(
      replacements.map((replacement) => compareAndSwapCoordinatorHandoff(key, expected, replacement, tmp.path)),
    )

    expect(results.filter(Boolean)).toHaveLength(1)
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(replacements[results.indexOf(true)])
    expect(await compareAndSwapCoordinatorHandoff(key, expected, replacements[0], tmp.path)).toBe(false)
  })

  test("rejects illegal phase transitions without changing state", async () => {
    await using tmp = await tmpdir()
    const first = record("request", "source")
    await publishSource(tmp.path, key, first.sourceEpoch)
    await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)
    const expected = match(first)

    await expect(
      compareAndSwapCoordinatorHandoff(key, expected, { ...accepted(first), phase: "ready", revision: 2 }, tmp.path),
    ).rejects.toThrow("Illegal coordinator handoff transition")
    expect(await readCoordinatorHandoff(tmp.path, key)).toEqual(first)
  })

  test("rejects path aliases and strips additive fields before persistence", async () => {
    await using tmp = await tmpdir()
    expect(() =>
      compareAndSwapCoordinatorHandoff("alias/../key", undefined, record("request", "source"), tmp.path),
    ).toThrow("Invalid coordinator key")

    const replacement = {
      ...record("request", "source"),
      password: "must-not-persist",
      token: "must-not-persist",
      username: "must-not-persist",
    }
    await publishSource(tmp.path, key, replacement.sourceEpoch)
    expect(await compareAndSwapCoordinatorHandoff(key, undefined, replacement, tmp.path)).toBe(true)
    expect(JSON.parse(await fs.readFile(coordinatorHandoffPath(tmp.path, key), "utf8"))).toEqual(
      record("request", "source"),
    )
  })

  test("serializes target selection across independent processes", async () => {
    await using tmp = await tmpdir()
    const first = record("request", "source")
    await publishSource(tmp.path, key, first.sourceEpoch)
    await compareAndSwapCoordinatorHandoff(key, undefined, first, tmp.path)
    const script = path.join(import.meta.dir, "coordinator-handoff-contender.ts")
    const contenders = Array.from({ length: 8 }, (_, index) => `process-target-${index}`)
    const results = await Promise.all(
      contenders.map(async (target) => {
        const child = Bun.spawn([process.execPath, script, tmp.path, key, target], {
          cwd: path.join(import.meta.dir, "../../../.."),
          stdout: "pipe",
          stderr: "pipe",
        })
        const [exitCode, stdout, stderr] = await Promise.all([
          child.exited,
          new Response(child.stdout).text(),
          new Response(child.stderr).text(),
        ])
        expect(stderr).toBe("")
        expect(exitCode).toBe(0)
        return stdout.trim() === "true"
      }),
    )

    expect(results.filter(Boolean)).toHaveLength(1)
    const winner = await readCoordinatorHandoff(tmp.path, key)
    expect("targetEpoch" in winner ? winner.targetEpoch : undefined).toBe(contenders[results.indexOf(true)])
  })
})

describe("coordinator idle authority", () => {
  const key = "f".repeat(40)

  test("idle shutdown yields when handoff creation wins the authority lock", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path, key, "source-1")
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const creating = withCoordinatorAuthorityLock(tmp.path, key, async () => {
      await fs.writeFile(coordinatorHandoffPath(tmp.path, key), JSON.stringify(record("request-1", "source-1")))
      entered.resolve()
      await release.promise
    })
    await entered.promise

    const shutdown = retireCoordinatorForIdleShutdown(key, {
      ...(JSON.parse(await fs.readFile(coordinatorManifestPath(tmp.path, key), "utf8")) as CoordinatorManifest),
    }, tmp.path)
    release.resolve()
    await creating

    expect(await shutdown).toEqual({ state: "progressing", reason: "handoff_present" })
  })

  test("idle retirement removes the source claim before a handoff can begin", async () => {
    await using tmp = await tmpdir()
    await publishSource(tmp.path, key, "source-1")
    const manifest = JSON.parse(await fs.readFile(coordinatorManifestPath(tmp.path, key), "utf8")) as CoordinatorManifest

    expect(await retireCoordinatorForIdleShutdown(key, manifest, tmp.path)).toEqual({ state: "committed", value: true })
    expect(await compareAndSwapCoordinatorHandoff(key, undefined, record("request-1", "source-1"), tmp.path)).toBe(false)
  })
})
