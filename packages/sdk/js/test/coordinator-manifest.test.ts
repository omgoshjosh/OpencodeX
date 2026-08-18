import { afterEach, describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import {
  COORDINATOR_HANDOFF_LEGACY_VERSION,
  COORDINATOR_HANDOFF_VERSION,
  COORDINATOR_MANIFEST_VERSION,
  checkCoordinatorCompatibility,
  checkCoordinatorHandoffTransition,
  acquireCoordinatorAuthorityLock,
  coordinatorAuthorityLockPath,
  coordinatorClientDir,
  coordinatorDatabaseIdentity,
  coordinatorHandoffPath,
  coordinatorKey,
  coordinatorManifestPath,
  fetchCoordinatorHealth,
  observeCoordinatorAuthority,
  isCoordinatorHandoffRecord,
  isLegacyCoordinatorHandoffRecord,
  isCoordinatorKey,
  isCoordinatorManifest,
  parseCoordinatorHandoffRecord,
  publishCoordinatorManifest,
  readCoordinatorManifest,
  readCoordinatorHandoff,
  removeCoordinatorManifest,
  replaceCoordinatorManifest,
  resolveCoordinatorAuthority,
  startCoordinatorClientLease,
  withCoordinatorAuthorityLock,
  type CoordinatorManifest,
  type CoordinatorHandoffRecord,
} from "../src/coordinator/manifest"

const roots: string[] = []

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => fs.rm(root, { recursive: true, force: true })))
})

async function stateRoot() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencodex-coordinator-"))
  roots.push(root)
  return root
}

function manifest(overrides: Partial<CoordinatorManifest> = {}): CoordinatorManifest {
  return {
    version: COORDINATOR_MANIFEST_VERSION,
    key: "a".repeat(40),
    directory: "/work/project",
    database: "/data/opencode.db",
    pid: process.pid,
    url: "http://127.0.0.1:4096/",
    username: "opencodex-local",
    password: "password",
    token: "token",
    createdAt: new Date(0).toISOString(),
    serverVersion: "1.2.3",
    ...overrides,
  }
}

describe("coordinator manifest validation", () => {
  test("round-trips a manifest through disk", async () => {
    const root = await stateRoot()
    const written = manifest()
    await publishCoordinatorManifest(root, written, undefined)
    expect(await readCoordinatorManifest(root, written.key)).toEqual(written)
    const stat = await fs.stat(coordinatorManifestPath(root, written.key))
    expect(stat.isFile()).toBe(true)
  })

  test("accepts a manifest with no serverVersion so old writers stay readable", () => {
    const { serverVersion: _dropped, ...legacy } = manifest()
    expect(isCoordinatorManifest(legacy)).toBe(true)
  })

  test("accepts unknown extra fields so additive wire changes do not look corrupt", () => {
    expect(isCoordinatorManifest({ ...manifest(), somethingNewer: { nested: true } })).toBe(true)
  })

  test("accepts and validates optional authority classification fields", () => {
    expect(isCoordinatorManifest(manifest({ authorityEpoch: "authority-1", admission: true, ready: false }))).toBe(true)
    expect(isCoordinatorManifest(manifest({ authorityEpoch: "" }))).toBe(false)
    expect(isCoordinatorManifest({ ...manifest(), authorityEpoch: 1 })).toBe(false)
    expect(isCoordinatorManifest({ ...manifest(), admission: "yes" })).toBe(false)
    expect(isCoordinatorManifest({ ...manifest(), ready: "yes" })).toBe(false)
  })

  test("rejects a non-loopback url, a wrong schema number, and a mistyped serverVersion", () => {
    expect(isCoordinatorManifest(manifest({ url: "http://example.com/" }))).toBe(false)
    expect(isCoordinatorManifest({ ...manifest(), version: 3 })).toBe(false)
    expect(isCoordinatorManifest({ ...manifest(), serverVersion: 3 })).toBe(false)
    expect(isCoordinatorManifest(manifest({ pid: "1" as unknown as number }))).toBe(false)
  })

  test("readCoordinatorManifest rejects a corrupt file instead of returning it", async () => {
    const root = await stateRoot()
    const file = coordinatorManifestPath(root, "abc123")
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, "{not json")
    await expect(readCoordinatorManifest(root, "abc123")).rejects.toThrow()
  })
})

describe("coordinator handoff record", () => {
  const current = {
    version: COORDINATOR_HANDOFF_VERSION,
    request: "request-1",
    phase: "requested",
    revision: 0,
    sourceEpoch: "source-generation-1",
    createdAt: "2026-08-18T20:00:00.000Z",
    updatedAt: "2026-08-18T20:00:00.000Z",
  } as const satisfies CoordinatorHandoffRecord

  test("reads a credential-free versioned record from its coordinator-scoped path", async () => {
    const root = await stateRoot()
    const key = "a".repeat(40)
    const file = coordinatorHandoffPath(root, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify(current))

    expect(await readCoordinatorHandoff(root, key)).toEqual(current)
    expect(Object.keys(current)).not.toContain("username")
    expect(Object.keys(current)).not.toContain("password")
  })

  test("validates the version and exact field types while allowing additive fields", () => {
    expect(isCoordinatorHandoffRecord(current)).toBe(true)
    expect(isCoordinatorHandoffRecord({ ...current, future: true })).toBe(true)
    expect(isCoordinatorHandoffRecord({ ...current, version: COORDINATOR_HANDOFF_LEGACY_VERSION })).toBe(false)
    expect(isCoordinatorHandoffRecord({ ...current, sourceEpoch: 1 })).toBe(false)
    expect(isCoordinatorHandoffRecord({ ...current, createdAt: "yesterday" })).toBe(false)
    expect(isCoordinatorHandoffRecord({ ...current, updatedAt: "2026-08-18T19:59:59.000Z" })).toBe(false)
  })

  test("reads the exact baseline v1 fixture without confusing it for v2", async () => {
    const raw = await fs.readFile(path.join(import.meta.dir, "fixtures/coordinator-handoff-v1.json"), "utf8")
    const legacy = {
      version: COORDINATOR_HANDOFF_LEGACY_VERSION,
      request: "baseline-request",
      sourceToken: "baseline-source-token",
    } as const

    expect(parseCoordinatorHandoffRecord(raw)).toEqual(legacy)
    expect(isLegacyCoordinatorHandoffRecord(legacy)).toBe(true)
    expect(isLegacyCoordinatorHandoffRecord({ ...legacy, future: { supported: true } })).toBe(true)
    expect(parseCoordinatorHandoffRecord(JSON.stringify({ ...legacy, future: true }))).toEqual({
      ...legacy,
      future: true,
    })
    expect(isCoordinatorHandoffRecord(legacy)).toBe(false)
    expect(isLegacyCoordinatorHandoffRecord({ ...legacy, phase: "requested" })).toBe(false)
    expect(isCoordinatorHandoffRecord({ ...current, sourceToken: "ambiguous" })).toBe(false)
  })

  test("keeps baseline and state-machine readers from silently sharing a version", () => {
    const baselineReader = (value: unknown) => {
      if (typeof value !== "object" || value === null) return false
      const record = value as { version?: unknown; request?: unknown; sourceToken?: unknown }
      return record.version === 1 && typeof record.request === "string" && typeof record.sourceToken === "string"
    }

    expect(baselineReader(current)).toBe(false)
    expect(isCoordinatorHandoffRecord(current)).toBe(true)
    expect(() =>
      parseCoordinatorHandoffRecord(
        JSON.stringify({ ...current, version: COORDINATOR_HANDOFF_LEGACY_VERSION, sourceToken: "ambiguous" }),
      ),
    ).toThrow("Invalid coordinator handoff record")
  })

  test("permits only revision-fenced legal phase transitions", () => {
    const accepted = {
      ...current,
      phase: "accepted",
      revision: 1,
      targetEpoch: "target-generation-1",
      updatedAt: "2026-08-18T20:00:01.000Z",
    } as const
    const ready = { ...accepted, phase: "ready", revision: 2, updatedAt: "2026-08-18T20:00:02.000Z" } as const
    const committed = { ...ready, phase: "committed", revision: 3, updatedAt: "2026-08-18T20:00:03.000Z" } as const

    expect(checkCoordinatorHandoffTransition(undefined, current)).toBe(true)
    expect(checkCoordinatorHandoffTransition(current, accepted)).toBe(true)
    expect(checkCoordinatorHandoffTransition(accepted, ready)).toBe(true)
    expect(checkCoordinatorHandoffTransition(ready, committed)).toBe(true)
    expect(checkCoordinatorHandoffTransition(current, { ...accepted, revision: 2 })).toBe(false)
    expect(checkCoordinatorHandoffTransition(current, { ...accepted, phase: "ready" })).toBe(false)
    expect(checkCoordinatorHandoffTransition(current, { ...accepted, request: "replay" })).toBe(false)
    expect(checkCoordinatorHandoffTransition(current, { ...accepted, sourceEpoch: "other" })).toBe(false)
    expect(checkCoordinatorHandoffTransition(accepted, { ...ready, targetEpoch: "other" })).toBe(false)
    expect(checkCoordinatorHandoffTransition(accepted, { ...ready, updatedAt: current.updatedAt })).toBe(false)
  })

  test("requires canonical coordinator keys before deriving handoff paths", () => {
    expect(isCoordinatorKey("a".repeat(40))).toBe(true)
    expect(isCoordinatorKey("A".repeat(40))).toBe(false)
    expect(() => coordinatorHandoffPath("/state", "alias/../key")).toThrow("Invalid coordinator key")
    expect(() => coordinatorHandoffPath("/state", "../../outside")).toThrow("Invalid coordinator key")
  })

  test("rejects malformed handoff state", async () => {
    const root = await stateRoot()
    const key = "a".repeat(40)
    const file = coordinatorHandoffPath(root, key)
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, "{not json")

    await expect(readCoordinatorHandoff(root, key)).rejects.toThrow()
  })
})

describe("coordinator authority resolver", () => {
  const activeManifest = manifest({ authorityEpoch: "source-generation-1", admission: true, ready: true })
  const activeHealth = {
    healthy: true,
    version: "1.2.3",
    active: false,
    authorityEpoch: "source-generation-1",
    admission: true,
    ready: true,
  } as const
  const handoff = {
    version: COORDINATOR_HANDOFF_VERSION,
    request: "request-1",
    phase: "requested",
    revision: 0,
    sourceEpoch: "source-generation-1",
    createdAt: "2026-08-18T20:00:00.000Z",
    updatedAt: "2026-08-18T20:00:00.000Z",
  } as const

  test("distinguishes absent, active, and handoff authority", () => {
    expect(resolveCoordinatorAuthority({})).toEqual({ state: "absent" })
    expect(resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth })).toMatchObject({
      state: "active",
      authorityEpoch: "source-generation-1",
    })
    expect(resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff })).toMatchObject({
      state: "handoff",
      authorityEpoch: "source-generation-1",
      handoff,
    })
  })

  test("fails closed on malformed, orphaned, or epoch-incompatible handoff", () => {
    expect(
      resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff: { nope: true } }),
    ).toEqual({
      state: "blocked",
      reason: "malformed_handoff",
    })
    expect(resolveCoordinatorAuthority({ handoff })).toEqual({ state: "blocked", reason: "orphaned_handoff" })
    expect(
      resolveCoordinatorAuthority({
        manifest: activeManifest,
        health: activeHealth,
        handoff: { ...handoff, sourceEpoch: "stale-generation" },
      }),
    ).toEqual({ state: "blocked", reason: "incompatible_handoff" })
    expect(
      resolveCoordinatorAuthority({
        manifest: activeManifest,
        health: activeHealth,
        handoff: { ...handoff, revision: 9 },
      }),
    ).toEqual({ state: "blocked", reason: "malformed_handoff" })
  })

  test("blocks baseline handoffs and empty authority epochs", async () => {
    const legacy = JSON.parse(
      await fs.readFile(path.join(import.meta.dir, "fixtures/coordinator-handoff-v1.json"), "utf8"),
    )
    expect(resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff: legacy })).toEqual({
      state: "blocked",
      reason: "legacy_handoff",
    })
    expect(
      resolveCoordinatorAuthority({ manifest: { ...activeManifest, authorityEpoch: "" }, health: activeHealth }),
    ).toEqual({ state: "blocked", reason: "invalid_authority" })
    expect(
      resolveCoordinatorAuthority({ manifest: activeManifest, health: { ...activeHealth, authorityEpoch: "" } }),
    ).toEqual({ state: "blocked", reason: "invalid_authority" })
  })

  test("recognizes a compatible handoff after source admission closes", () => {
    expect(
      resolveCoordinatorAuthority({
        manifest: { ...activeManifest, admission: false },
        health: { ...activeHealth, admission: false },
        handoff,
      }),
    ).toMatchObject({ state: "handoff", handoff })
  })

  test("keeps pre-commit source authority attachable and waits for the selected target after ready", () => {
    const accepted = {
      ...handoff,
      phase: "accepted" as const,
      revision: 1,
      targetEpoch: "target-generation-1",
      updatedAt: "2026-08-18T20:00:01.000Z",
    }
    expect(
      resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff: accepted }),
    ).toMatchObject({
      state: "handoff",
      authorityEpoch: "source-generation-1",
    })

    const ready = {
      ...accepted,
      phase: "ready" as const,
      revision: 2,
      updatedAt: "2026-08-18T20:00:02.000Z",
    }
    expect(resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff: ready })).toEqual({
      state: "blocked",
      reason: "incompatible_handoff",
    })
    expect(
      resolveCoordinatorAuthority({
        manifest: { ...activeManifest, authorityEpoch: "target-generation-1" },
        health: { ...activeHealth, authorityEpoch: "target-generation-1" },
        handoff: ready,
      }),
    ).toMatchObject({ state: "handoff", authorityEpoch: "target-generation-1" })
  })

  test("requires complete live authority health throughout handoff", () => {
    expect(
      resolveCoordinatorAuthority({
        manifest: activeManifest,
        health: { healthy: true, version: "1.2.3" },
        handoff,
      }),
    ).toEqual({ state: "blocked", reason: "invalid_authority" })
  })

  test("attaches committed handoff only to its exact target epoch", () => {
    const committed = {
      ...handoff,
      phase: "committed" as const,
      revision: 3,
      targetEpoch: "target-generation-1",
      updatedAt: "2026-08-18T20:00:03.000Z",
    }
    expect(
      resolveCoordinatorAuthority({ manifest: activeManifest, health: activeHealth, handoff: committed }),
    ).toEqual({ state: "blocked", reason: "incompatible_handoff" })
    expect(
      resolveCoordinatorAuthority({
        manifest: { ...activeManifest, authorityEpoch: "target-generation-1" },
        health: { ...activeHealth, authorityEpoch: "target-generation-1" },
        handoff: committed,
      }),
    ).toMatchObject({ state: "handoff", authorityEpoch: "target-generation-1" })
  })

  test("blocks incompatible authority observations and explicit admission/readiness refusal", () => {
    expect(
      resolveCoordinatorAuthority({
        manifest: activeManifest,
        health: { ...activeHealth, authorityEpoch: "other-generation" },
      }),
    ).toEqual({ state: "blocked", reason: "incompatible_authority" })
    expect(
      resolveCoordinatorAuthority({ manifest: activeManifest, health: { ...activeHealth, ready: false } }),
    ).toEqual({
      state: "blocked",
      reason: "not_ready",
    })
    expect(
      resolveCoordinatorAuthority({ manifest: { ...activeManifest, admission: false }, health: activeHealth }),
    ).toEqual({ state: "blocked", reason: "admission_closed" })
  })

  test("keeps legacy authority manifests and health responses classifiable", () => {
    expect(
      resolveCoordinatorAuthority({
        manifest: manifest(),
        health: { healthy: true, version: "1.2.3", active: false },
      }),
    ).toEqual({ state: "active", authorityEpoch: undefined })
  })
})

describe("coordinator key derivation", () => {
  test("is the sha1 of the resolved database identity", () => {
    const absolute = path.resolve("/data/opencode.db")
    expect(coordinatorDatabaseIdentity(absolute)).toBe(process.platform === "win32" ? absolute.toLowerCase() : absolute)
    expect(coordinatorKey(absolute)).toMatch(/^[0-9a-f]{40}$/)
    expect(coordinatorKey(absolute)).toBe(coordinatorKey(absolute.toUpperCase() === absolute ? absolute : absolute))
  })

  test("resolves a relative database against the caller's base", () => {
    expect(coordinatorDatabaseIdentity("opencode.db", path.resolve("/data"))).toBe(
      coordinatorDatabaseIdentity(path.resolve("/data", "opencode.db")),
    )
    expect(coordinatorKey("opencode.db", path.resolve("/data"))).toBe(
      coordinatorKey(path.resolve("/data", "opencode.db")),
    )
  })

  test("passes :memory: through untouched", () => {
    expect(coordinatorDatabaseIdentity(":memory:")).toBe(":memory:")
  })
})

describe("token-guarded removal", () => {
  test("keeps the manifest when the token does not match", async () => {
    const root = await stateRoot()
    const written = manifest()
    await publishCoordinatorManifest(root, written, undefined)
    expect(await removeCoordinatorManifest(root, written.key, { token: "other-token", authorityEpoch: undefined })).toEqual({
      state: "progressing",
      reason: "manifest_changed",
    })
    expect(await readCoordinatorManifest(root, written.key)).toEqual(written)
  })

  test("removes the manifest when the token matches", async () => {
    const root = await stateRoot()
    const written = manifest()
    await publishCoordinatorManifest(root, written, undefined)
    expect(await removeCoordinatorManifest(root, written.key, written)).toEqual({ state: "committed", value: true })
    await expect(readCoordinatorManifest(root, written.key)).rejects.toThrow()
  })

  test("preserves the manifest while any handoff observation exists", async () => {
    const root = await stateRoot()
    const written = manifest({ key: "a".repeat(40) })
    await publishCoordinatorManifest(root, written, undefined)
    await fs.writeFile(coordinatorHandoffPath(root, written.key), "{malformed")

    expect(await removeCoordinatorManifest(root, written.key, written)).toEqual({
      state: "progressing",
      reason: "handoff_present",
    })
    expect(await readCoordinatorManifest(root, written.key)).toEqual(written)
  })

  test("is a no-op when there is nothing to remove", async () => {
    const root = await stateRoot()
    expect(
      await removeCoordinatorManifest(root, "f".repeat(40), { token: "token", authorityEpoch: undefined }),
    ).toEqual({ state: "progressing", reason: "manifest_absent" })
  })

  test("serializes manifest removal behind concurrent handoff creation", async () => {
    const root = await stateRoot()
    const written = manifest({ key: "d".repeat(40), authorityEpoch: "source-1" })
    await publishCoordinatorManifest(root, written, undefined)
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const handoff = {
      version: COORDINATOR_HANDOFF_VERSION,
      request: "request-1",
      phase: "requested",
      revision: 0,
      sourceEpoch: "source-1",
      createdAt: "2026-08-18T20:00:00.000Z",
      updatedAt: "2026-08-18T20:00:00.000Z",
    } as const
    const creating = withCoordinatorAuthorityLock(root, written.key, async () => {
      await fs.writeFile(coordinatorHandoffPath(root, written.key), JSON.stringify(handoff))
      entered.resolve()
      await release.promise
    })
    await entered.promise

    const removing = removeCoordinatorManifest(root, written.key, written)
    release.resolve()
    await creating

    expect(await removing).toEqual({ state: "progressing", reason: "handoff_present" })
    expect(await readCoordinatorManifest(root, written.key)).toEqual(written)
  })
})

describe("coordinator manifest transactions", () => {
  test("publishes only into exact manifest and handoff absence", async () => {
    const root = await stateRoot()
    const written = manifest({ key: "6".repeat(40), authorityEpoch: "source-1" })

    expect(await publishCoordinatorManifest(root, written, undefined)).toEqual({ state: "committed", value: written })
    expect(await publishCoordinatorManifest(root, { ...written, token: "other" }, undefined)).toEqual({
      state: "progressing",
      reason: "manifest_changed",
    })
  })

  test("replaces only under exact manifest and handoff fences", async () => {
    const root = await stateRoot()
    const source = manifest({ key: "5".repeat(40), authorityEpoch: "source-1" })
    const target = { ...source, token: "target-token", authorityEpoch: "target-1" }
    const handoff = {
      version: COORDINATOR_HANDOFF_VERSION,
      request: "request-1",
      phase: "accepted" as const,
      revision: 1,
      sourceEpoch: "source-1",
      targetEpoch: "target-1",
      createdAt: "2026-08-18T20:00:00.000Z",
      updatedAt: "2026-08-18T20:00:01.000Z",
    }
    await publishCoordinatorManifest(root, source, undefined)
    await fs.writeFile(coordinatorHandoffPath(root, source.key), JSON.stringify(handoff))

    expect(
      await replaceCoordinatorManifest({
        stateRoot: root,
        manifest: target,
        expectedManifest: { ...source, token: "wrong" },
        expectedHandoff: handoff,
      }),
    ).toEqual({ state: "progressing", reason: "manifest_changed" })
    expect(
      await replaceCoordinatorManifest({
        stateRoot: root,
        manifest: target,
        expectedManifest: source,
        expectedHandoff: { ...handoff, revision: 2 },
      }),
    ).toEqual({ state: "progressing", reason: "handoff_changed" })
    expect(
      await replaceCoordinatorManifest({
        stateRoot: root,
        manifest: target,
        expectedManifest: source,
        expectedHandoff: handoff,
      }),
    ).toEqual({ state: "committed", value: target })
  })
})

describe("coordinator authority observation", () => {
  test("retries when the phase changes during probe and observes the committed target", async () => {
    const root = await stateRoot()
    const key = "e".repeat(40)
    const source = manifest({ key, authorityEpoch: "source-1", admission: false, ready: true })
    const target = { ...source, token: "target-token", authorityEpoch: "target-1", admission: true }
    const requested = {
      version: COORDINATOR_HANDOFF_VERSION,
      request: "request-1",
      phase: "requested",
      revision: 0,
      sourceEpoch: "source-1",
      createdAt: "2026-08-18T20:00:00.000Z",
      updatedAt: "2026-08-18T20:00:00.000Z",
    } as const
    const committed = {
      ...requested,
      phase: "committed" as const,
      revision: 3,
      targetEpoch: "target-1",
      updatedAt: "2026-08-18T20:00:03.000Z",
    }
    await publishCoordinatorManifest(root, source, undefined)
    await fs.writeFile(coordinatorHandoffPath(root, key), JSON.stringify(requested))
    const probing = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    let calls = 0
    const fetch: typeof globalThis.fetch = Object.assign(
      async () => {
        calls += 1
        if (calls === 1) {
          probing.resolve()
          await release.promise
          return Response.json({ healthy: true, authorityEpoch: "source-1", admission: false, ready: true })
        }
        return Response.json({ healthy: true, authorityEpoch: "target-1", admission: true, ready: true })
      },
      { preconnect: globalThis.fetch.preconnect },
    )
    const observing = observeCoordinatorAuthority({ stateRoot: root, key, fetch })
    await probing.promise
    await withCoordinatorAuthorityLock(root, key, async () => {
      await fs.writeFile(coordinatorHandoffPath(root, key), JSON.stringify(committed))
    })
    await replaceCoordinatorManifest({
      stateRoot: root,
      manifest: target,
      expectedManifest: source,
      expectedHandoff: committed,
    })
    release.resolve()

    expect(await observing).toMatchObject({
      state: "observed",
      manifest: { token: "target-token", authorityEpoch: "target-1" },
      authority: { state: "handoff", authorityEpoch: "target-1", handoff: { phase: "committed" } },
    })
    expect(calls).toBe(2)
  })

  test("returns progressing when the authority fence changes on both probes", async () => {
    const root = await stateRoot()
    const key = "f".repeat(40)
    const initial = manifest({ key, authorityEpoch: "source-1" })
    await publishCoordinatorManifest(root, initial, undefined)
    const fetch: typeof globalThis.fetch = Object.assign(
      async () => Response.json({ healthy: true, authorityEpoch: "source-1", admission: true, ready: true }),
      { preconnect: globalThis.fetch.preconnect },
    )

    let current = initial
    expect(
      await observeCoordinatorAuthority({
        stateRoot: root,
        key,
        fetch,
        afterProbe: async (attempt) => {
          const replacement = { ...initial, token: `replacement-${attempt}` }
          await replaceCoordinatorManifest({
            stateRoot: root,
            manifest: replacement,
            expectedManifest: current,
            expectedHandoff: undefined,
          })
          current = replacement
        },
      }),
    ).toEqual({ state: "progressing" })
  })
})

describe("coordinator authority lock recovery", () => {
  test("immediately reclaims a lock whose recorded PID is dead", async () => {
    const root = await stateRoot()
    const key = "9".repeat(40)
    const lockPath = coordinatorAuthorityLockPath(root, key)
    await fs.mkdir(lockPath, { recursive: true })
    await fs.writeFile(path.join(lockPath, "owner.json"), JSON.stringify({ pid: 424242, token: "dead-owner" }))
    let sleeps = 0

    const lock = await acquireCoordinatorAuthorityLock(root, key, {
      timeout: 100,
      now: () => 0,
      sleep: async () => {
        sleeps += 1
      },
      processAlive: () => false,
    })

    expect(lock.token).not.toBe("dead-owner")
    expect(sleeps).toBe(1)
    await lock.release()
  })

  for (const metadata of [undefined, "{malformed"] as const) {
    test(`does not reclaim ${metadata === undefined ? "missing" : "malformed"} owner metadata`, async () => {
      const root = await stateRoot()
      const key = metadata === undefined ? "7".repeat(40) : "8".repeat(40)
      const lockPath = coordinatorAuthorityLockPath(root, key)
      await fs.mkdir(lockPath, { recursive: true })
      if (metadata !== undefined) await fs.writeFile(path.join(lockPath, "owner.json"), metadata)
      let now = 0
      let checkedPID = false

      await expect(
        acquireCoordinatorAuthorityLock(root, key, {
          timeout: 50,
          now: () => now,
          sleep: async (milliseconds) => {
            now += milliseconds
          },
          processAlive: () => {
            checkedPID = true
            return false
          },
        }),
      ).rejects.toThrow("Timed out acquiring coordinator authority lock")
      expect(checkedPID).toBe(false)
      expect((await fs.stat(lockPath)).isDirectory()).toBe(true)
    })
  }
})

describe("compatibility policy", () => {
  test("attaches only on an exact version match", () => {
    expect(
      checkCoordinatorCompatibility({
        manifest: { serverVersion: "1.2.3" },
        clientVersion: "1.2.3",
        skip: false,
      }),
    ).toEqual({ compatible: true, reason: "match" })

    const mismatch = checkCoordinatorCompatibility({
      manifest: { serverVersion: "1.2.4" },
      clientVersion: "1.2.3",
      skip: false,
    })
    expect(mismatch.compatible).toBe(false)
    expect(mismatch.reason).toBe("mismatch")
    expect(mismatch.message).toContain("1.2.4")
    expect(mismatch.message).toContain("1.2.3")
  })

  test("refuses a legacy manifest and says why", () => {
    const result = checkCoordinatorCompatibility({
      manifest: {},
      clientVersion: "1.2.3",
      skip: false,
    })
    expect(result.compatible).toBe(false)
    expect(result.reason).toBe("legacy")
    expect(result.message).toContain("predates the version handshake")
  })

  test("allows a local build on either side with a warning", () => {
    const coordinatorLocal = checkCoordinatorCompatibility({
      manifest: { serverVersion: "local" },
      clientVersion: "1.2.3",
      skip: false,
    })
    expect(coordinatorLocal.compatible).toBe(true)
    expect(coordinatorLocal.reason).toBe("local")
    expect(coordinatorLocal.message).toBeTruthy()

    expect(
      checkCoordinatorCompatibility({
        manifest: { serverVersion: "1.2.3" },
        clientVersion: "local",
        skip: false,
      }).compatible,
    ).toBe(true)
  })

  test("a legacy manifest is not rescued by a local client", () => {
    expect(checkCoordinatorCompatibility({ manifest: {}, clientVersion: "local", skip: false }).reason).toBe("legacy")
  })

  test("cross-checks the manifest against the live health version", () => {
    const stale = checkCoordinatorCompatibility({
      manifest: { serverVersion: "1.2.3" },
      clientVersion: "1.2.3",
      healthVersion: "1.3.0",
      skip: false,
    })
    expect(stale.compatible).toBe(false)
    expect(stale.reason).toBe("health_mismatch")
    expect(stale.message).toContain("stale")

    expect(
      checkCoordinatorCompatibility({
        manifest: { serverVersion: "1.2.3" },
        clientVersion: "1.2.3",
        healthVersion: "1.2.3",
        skip: false,
      }),
    ).toEqual({ compatible: true, reason: "match" })
  })

  test("the skip escape bypasses every refusal", () => {
    expect(checkCoordinatorCompatibility({ manifest: {}, clientVersion: "1.2.3", skip: true })).toEqual({
      compatible: true,
      reason: "skipped",
    })
    expect(
      checkCoordinatorCompatibility({
        manifest: { serverVersion: "9.9.9" },
        clientVersion: "1.2.3",
        healthVersion: "0.0.1",
        skip: true,
      }).compatible,
    ).toBe(true)
  })

  test("reads the skip escape from the environment by default", () => {
    const previous = process.env.OPENCODEX_SKIP_VERSION_CHECK
    try {
      process.env.OPENCODEX_SKIP_VERSION_CHECK = "1"
      expect(checkCoordinatorCompatibility({ manifest: {}, clientVersion: "1.2.3" }).reason).toBe("skipped")
      delete process.env.OPENCODEX_SKIP_VERSION_CHECK
      expect(checkCoordinatorCompatibility({ manifest: {}, clientVersion: "1.2.3" }).reason).toBe("legacy")
    } finally {
      if (previous === undefined) delete process.env.OPENCODEX_SKIP_VERSION_CHECK
      else process.env.OPENCODEX_SKIP_VERSION_CHECK = previous
    }
  })
})

describe("health probe", () => {
  test("returns the parsed version alongside the health flag", async () => {
    const health = await fetchCoordinatorHealth(manifest(), {
      fetch: async () =>
        new Response(JSON.stringify({ healthy: true, version: "1.2.3", active: false }), {
          headers: { "content-type": "application/json" },
        }),
    })
    expect(health).toEqual({ healthy: true, version: "1.2.3", active: false })
  })

  test("returns optional authority classification fields", async () => {
    const health = await fetchCoordinatorHealth(manifest(), {
      fetch: async () =>
        new Response(
          JSON.stringify({
            healthy: true,
            authorityEpoch: "authority-1",
            admission: false,
            ready: true,
          }),
        ),
    })
    expect(health).toEqual({
      healthy: true,
      version: undefined,
      active: undefined,
      authorityEpoch: "authority-1",
      admission: false,
      ready: true,
    })
  })

  test("rejects malformed authority classification fields", async () => {
    for (const field of [
      { authorityEpoch: "" },
      { authorityEpoch: 1 },
      { admission: "yes" },
      { admission: null },
      { ready: "yes" },
      { ready: null },
    ]) {
      expect(
        await fetchCoordinatorHealth(manifest(), {
          fetch: async () => new Response(JSON.stringify({ healthy: true, ...field })),
        }),
      ).toBeUndefined()
    }
  })

  test("returns undefined for an unreachable or failing coordinator", async () => {
    expect(
      await fetchCoordinatorHealth(manifest(), {
        fetch: async () => {
          throw new Error("ECONNREFUSED")
        },
      }),
    ).toBeUndefined()
    expect(
      await fetchCoordinatorHealth(manifest(), { fetch: async () => new Response("nope", { status: 500 }) }),
    ).toBeUndefined()
  })

  test("sends the manifest's basic credentials", async () => {
    let authorization: string | undefined
    await fetchCoordinatorHealth(manifest(), {
      fetch: async (_url, init) => {
        authorization = (init?.headers as Record<string, string>).authorization
        return new Response(JSON.stringify({ healthy: true, version: "1.2.3", active: true }))
      },
    })
    expect(authorization).toBe(`Basic ${Buffer.from("opencodex-local:password").toString("base64")}`)
  })
})

describe("client lease", () => {
  test("writes, then removes, a lease keyed by pid", async () => {
    const root = await stateRoot()
    const lease = startCoordinatorClientLease({ stateRoot: root, key: "abc123", interval: 60_000 })
    await lease.ready
    const dir = coordinatorClientDir(root, "abc123")
    expect(await fs.readdir(dir)).toEqual([`${process.pid}.json`])
    expect(JSON.parse(await fs.readFile(lease.file, "utf8"))).toMatchObject({
      version: 1,
      key: "abc123",
      pid: process.pid,
    })
    await lease.dispose()
    expect(await fs.readdir(dir)).toEqual([])
  })

  test("applies the caller's suffix so client kinds stay distinguishable", async () => {
    const root = await stateRoot()
    const lease = startCoordinatorClientLease({
      stateRoot: root,
      key: "abc123",
      suffix: ".gui",
      pid: 4242,
      interval: 60_000,
    })
    await lease.ready
    expect(await fs.readdir(coordinatorClientDir(root, "abc123"))).toEqual(["4242.gui.json"])
    await lease.dispose()
  })

  test("dispose is idempotent", async () => {
    const root = await stateRoot()
    const lease = startCoordinatorClientLease({ stateRoot: root, key: "abc123", interval: 60_000 })
    await lease.ready
    await lease.dispose()
    await lease.dispose()
    expect(await fs.readdir(coordinatorClientDir(root, "abc123"))).toEqual([])
  })
})
