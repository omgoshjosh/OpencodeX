import { afterEach, describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import {
  COORDINATOR_MANIFEST_VERSION,
  checkCoordinatorCompatibility,
  coordinatorClientDir,
  coordinatorDatabaseIdentity,
  coordinatorKey,
  coordinatorManifestPath,
  fetchCoordinatorHealth,
  isCoordinatorManifest,
  probeCoordinatorHealth,
  probeCoordinatorHealthWithRetry,
  resolveCoordinatorAttachment,
  readCoordinatorManifest,
  removeCoordinatorManifest,
  startCoordinatorClientLease,
  writeCoordinatorManifest,
  type CoordinatorManifest,
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
    key: "abc123",
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
    await writeCoordinatorManifest(root, written)
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

describe("coordinator key derivation", () => {
  test("is the sha1 of the resolved database identity", () => {
    const absolute = path.resolve("/data/opencode.db")
    expect(coordinatorDatabaseIdentity(absolute)).toBe(
      process.platform === "win32" ? absolute.toLowerCase() : absolute,
    )
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
    await writeCoordinatorManifest(root, written)
    expect(await removeCoordinatorManifest(root, written.key, "other-token")).toBe(false)
    expect(await readCoordinatorManifest(root, written.key)).toEqual(written)
  })

  test("removes the manifest when the token matches", async () => {
    const root = await stateRoot()
    const written = manifest()
    await writeCoordinatorManifest(root, written)
    expect(await removeCoordinatorManifest(root, written.key, written.token)).toBe(true)
    await expect(readCoordinatorManifest(root, written.key)).rejects.toThrow()
  })

  test("is a no-op when there is nothing to remove", async () => {
    const root = await stateRoot()
    expect(await removeCoordinatorManifest(root, "missing", "token")).toBe(false)
  })
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
    expect(
      checkCoordinatorCompatibility({ manifest: {}, clientVersion: "local", skip: false }).reason,
    ).toBe("legacy")
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
    expect(
      checkCoordinatorCompatibility({ manifest: {}, clientVersion: "1.2.3", skip: true }),
    ).toEqual({ compatible: true, reason: "skipped" })
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
      expect(
        checkCoordinatorCompatibility({ manifest: {}, clientVersion: "1.2.3" }).reason,
      ).toBe("skipped")
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

/** A fetch that never settles until our own deadline aborts it. */
const stalls: typeof globalThis.fetch = (_url, init) =>
  new Promise((_resolve, reject) => {
    init?.signal?.addEventListener("abort", () => {
      const error = new Error("The operation was aborted")
      error.name = "AbortError"
      reject(error)
    })
  })

/** The shape Node and Bun actually throw when nothing is listening. */
function refusal(code: string) {
  return Object.assign(new TypeError("fetch failed"), {
    cause: Object.assign(new Error(`connect ${code} 127.0.0.1:4096`), { code }),
  })
}

function healthyResponse(body: Record<string, unknown> = { healthy: true, version: "1.2.3" }) {
  return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } })
}

/** Backoff is injected away so retry tests cost no wall-clock time. */
const nowait = async () => {}

describe("probe classification", () => {
  test("a healthy reply carries the version and the active flag", async () => {
    const probe = await probeCoordinatorHealth(manifest(), {
      fetch: async () => healthyResponse({ healthy: true, version: "1.2.3", active: true }),
    })
    expect(probe).toMatchObject({ kind: "healthy", version: "1.2.3", active: true })
  })

  test("a 2xx reply of healthy:false is unhealthy, not unreachable", async () => {
    const probe = await probeCoordinatorHealth(manifest(), {
      fetch: async () => healthyResponse({ healthy: false, version: "1.2.3" }),
    })
    expect(probe).toMatchObject({ kind: "unhealthy", version: "1.2.3" })
  })

  test("our own deadline is a timeout, not a death certificate", async () => {
    const probe = await probeCoordinatorHealth(manifest(), { fetch: stalls, timeout: 10 })
    expect(probe.kind).toBe("timeout")
  })

  test("reads a refusal errno off the error's cause instead of its message", async () => {
    for (const code of ["ECONNREFUSED", "EADDRNOTAVAIL", "EHOSTUNREACH"]) {
      expect(
        await probeCoordinatorHealth(manifest(), {
          fetch: async () => {
            throw refusal(code)
          },
        }),
      ).toEqual({ kind: "refused", code })
    }
  })

  test("a non-2xx status is reported with its status", async () => {
    const probe = await probeCoordinatorHealth(manifest(), {
      fetch: async () => new Response("denied", { status: 401 }),
    })
    expect(probe).toEqual({ kind: "http", status: 401 })
  })

  test("a 2xx body that is not a coordinator response is a body failure", async () => {
    expect(await probeCoordinatorHealth(manifest(), { fetch: async () => new Response("not json") })).toEqual({
      kind: "body",
    })
    expect(await probeCoordinatorHealth(manifest(), { fetch: async () => Response.json("a string") })).toEqual({
      kind: "body",
    })
  })

  test("an unrecognised error is unknown, never a refusal", async () => {
    const probe = await probeCoordinatorHealth(manifest(), {
      fetch: async () => {
        /* The message says ECONNREFUSED but nothing structural does. */
        throw new Error("ECONNREFUSED")
      },
    })
    expect(probe.kind).toBe("unknown")
  })
})

describe("escalating health retry", () => {
  test("retries a coordinator that only responds after N-1 timeouts", async () => {
    let calls = 0
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      fetch: (url, init) => {
        calls += 1
        return calls < 3 ? stalls(url, init) : Promise.resolve(healthyResponse())
      },
    })

    expect(result.probe.kind).toBe("healthy")
    expect(result.attempts).toBe(3)
    expect(calls).toBe(3)
  })

  test("escalates the deadline on every attempt", async () => {
    const deadlines: number[] = []
    await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 20,
      delay: nowait,
      fetch: async (_url, init) => {
        const started = Date.now()
        await new Promise<void>((resolve) => init?.signal?.addEventListener("abort", () => resolve()))
        deadlines.push(Date.now() - started)
        throw Object.assign(new Error("aborted"), { name: "AbortError" })
      },
    })

    expect(deadlines).toHaveLength(3)
    expect(deadlines[1]).toBeGreaterThan(deadlines[0])
    expect(deadlines[2]).toBeGreaterThan(deadlines[1])
  })

  test("does not retry stale credentials — a second ask cannot change the answer", async () => {
    let calls = 0
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      fetch: async () => {
        calls += 1
        return new Response("denied", { status: 401 })
      },
    })

    expect(result.probe).toEqual({ kind: "http", status: 401 })
    expect(calls).toBe(1)
  })

  test("does not retry a body that proves something else owns the port", async () => {
    let calls = 0
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      fetch: async () => {
        calls += 1
        return new Response("<html>nginx</html>")
      },
    })

    expect(result.probe).toEqual({ kind: "body" })
    expect(calls).toBe(1)
  })

  test("retries a 5xx but not a 404", async () => {
    let serverErrors = 0
    await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      fetch: async () => {
        serverErrors += 1
        return new Response("busy", { status: 503 })
      },
    })
    expect(serverErrors).toBe(3)

    let missing = 0
    await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      fetch: async () => {
        missing += 1
        return new Response("gone", { status: 404 })
      },
    })
    expect(missing).toBe(1)
  })

  test("stops before an attempt that would overrun the total wall cap", async () => {
    let calls = 0
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 100,
      totalTimeout: 600,
      delay: nowait,
      fetch: (url, init) => {
        calls += 1
        return stalls(url, init)
      },
    })

    /* Attempt 3 would need 400ms of backoff plus a 300ms deadline on top of
       ~300ms already spent, so it never starts. */
    expect(calls).toBe(2)
    expect(result.attempts).toBe(2)
    expect(result.probe.kind).toBe("timeout")
  })

  test("OPENCODEX_COORDINATOR_HEALTH_ATTEMPTS=1 restores single-shot behaviour", async () => {
    let calls = 0
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      timeout: 10,
      delay: nowait,
      env: { OPENCODEX_COORDINATOR_HEALTH_ATTEMPTS: "1" },
      fetch: (url, init) => {
        calls += 1
        return stalls(url, init)
      },
    })

    expect(calls).toBe(1)
    expect(result.attempts).toBe(1)
    expect(result.probe.kind).toBe("timeout")
  })

  test("the timeout environment override may only make the client more patient", async () => {
    /* Deliberately waits out the 1.5s floor: the point is that a too-small
       value cannot shorten the probe below what the code already guarantees. */
    const result = await probeCoordinatorHealthWithRetry(manifest(), {
      delay: nowait,
      env: { OPENCODEX_COORDINATOR_HEALTH_ATTEMPTS: "1", OPENCODEX_COORDINATOR_HEALTH_TIMEOUT: "10" },
      fetch: stalls,
    })

    expect(result.probe.kind).toBe("timeout")
    expect(result.elapsedMs).toBeGreaterThan(1_000)
  }, 10_000)
})

describe("attachment resolution", () => {
  const prober = (fetch: typeof globalThis.fetch) => (endpoint: Parameters<typeof probeCoordinatorHealth>[0]) =>
    probeCoordinatorHealthWithRetry(endpoint, { fetch, timeout: 10, delay: nowait })

  test("attaches to a coordinator that only answers on a later attempt", async () => {
    let calls = 0
    const resolution = await resolveCoordinatorAttachment({
      manifest: manifest({ serverVersion: "1.2.3" }),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "decide",
      probe: prober((url, init) => {
        calls += 1
        return calls < 3 ? stalls(url, init) : Promise.resolve(healthyResponse())
      }),
    })

    expect(resolution.action).toBe("attach")
  })

  test("refuses without ever offering to kill a live but unverifiable process", async () => {
    const resolution = await resolveCoordinatorAttachment({
      manifest: manifest(),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "decide",
      probe: prober(stalls),
      processAlive: () => true,
    })

    expect(resolution.action).toBe("refuse")
    if (resolution.action !== "refuse") throw new Error("unreachable")
    expect(resolution.code).toBe("unverifiable")
    expect(resolution.message).toContain("will not replace it")
    expect(resolution.message).toContain(`kill ${process.pid}`)
    expect(resolution.probe.attempts).toBe(3)
  })

  test("names the port squatter rather than blaming the coordinator", async () => {
    const resolution = await resolveCoordinatorAttachment({
      manifest: manifest(),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "decide",
      probe: prober(async () => new Response("<html>nginx</html>")),
      processAlive: () => true,
    })

    expect(resolution).toMatchObject({ action: "refuse", code: "foreign" })
    if (resolution.action !== "refuse") throw new Error("unreachable")
    expect(resolution.message).toContain("not an OpencodeX coordinator is listening on that port")
  })

  test("reclaims a manifest whose owner is gone instead of refusing", async () => {
    const resolution = await resolveCoordinatorAttachment({
      manifest: manifest(),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "decide",
      probe: prober(async () => {
        throw refusal("ECONNREFUSED")
      }),
      processAlive: () => false,
    })

    expect(resolution).toMatchObject({ action: "reclaim", reason: "process_dead" })
  })

  test("reclaims a manifest that describes a different key or database", async () => {
    const unreachable = () => {
      throw new Error("the probe must not run for a mismatched manifest")
    }
    expect(
      await resolveCoordinatorAttachment({
        manifest: manifest({ key: "other" }),
        key: "abc123",
        database: "/data/opencode.db",
        clientVersion: "1.2.3",
        mode: "decide",
        probe: unreachable,
      }),
    ).toMatchObject({ action: "reclaim", reason: "key_mismatch" })
    expect(
      await resolveCoordinatorAttachment({
        manifest: manifest({ database: "/data/other.db" }),
        key: "abc123",
        database: "/data/opencode.db",
        clientVersion: "1.2.3",
        mode: "decide",
        probe: unreachable,
      }),
    ).toMatchObject({ action: "reclaim", reason: "database_mismatch" })
  })

  test("quick mode probes once so fan-outs cannot stall on one slow coordinator", async () => {
    let calls = 0
    await resolveCoordinatorAttachment({
      manifest: manifest(),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "quick",
      processAlive: () => true,
      probe: (endpoint, mode) =>
        probeCoordinatorHealthWithRetry(endpoint, {
          fetch: (url, init) => {
            calls += 1
            return stalls(url, init)
          },
          timeout: 10,
          delay: nowait,
          attempts: mode === "quick" ? 1 : 3,
        }),
    })

    expect(calls).toBe(1)
  })

  test("a version mismatch refuses on its own code, not as unreachable", async () => {
    const resolution = await resolveCoordinatorAttachment({
      manifest: manifest({ serverVersion: "9.9.9" }),
      key: "abc123",
      database: "/data/opencode.db",
      clientVersion: "1.2.3",
      mode: "decide",
      skipVersionCheck: false,
      probe: prober(async () => healthyResponse({ healthy: true, version: "9.9.9" })),
    })

    expect(resolution).toMatchObject({ action: "refuse", code: "version" })
  })
})
