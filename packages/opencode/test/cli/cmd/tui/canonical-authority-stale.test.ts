// OpencodeX-1d2 layer 2 hardening: a stale canonical-authority marker (dead
// pid, nothing listening, older than the grace window) is reclaimed instead of
// pinning the embedded worker off :4096 forever. Nothing here binds 4096: the
// only socket is an ephemeral `Bun.listen` on port 0.
import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import * as Log from "@opencode-ai/core/util/log"
import { Global } from "@opencode-ai/core/global"
import { readCanonicalAuthorityGuard } from "@/cli/cmd/tui/canonical-authority"

const marker = path.join(Global.Path.data, "canonical-authority.json")
const HOUR = 60 * 60 * 1000
const MINUTE = 60 * 1000
const ENV = "OPENCODE_CANONICAL_AUTHORITY_GRACE_MS"

async function seed(fields: Record<string, unknown>) {
  await fs.mkdir(path.dirname(marker), { recursive: true })
  await fs.writeFile(
    marker,
    JSON.stringify({
      pid: 0,
      hostname: "127.0.0.1",
      port: 1,
      since: new Date(Date.now() - HOUR).toISOString(),
      ...fields,
    }),
  )
}

const exists = () =>
  fs.stat(marker).then(
    () => true,
    () => false,
  )

/** A port that is certainly not listening: bind, read it back, close. */
async function freePort() {
  const probe = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } })
  const port = probe.port
  probe.stop(true)
  return port
}

describe("canonical authority marker staleness", () => {
  let warn: ReturnType<typeof spyOn>
  let envBefore: string | undefined

  beforeEach(async () => {
    envBefore = process.env[ENV]
    delete process.env[ENV]
    warn = spyOn(Log.Default, "warn").mockImplementation(() => {})
    await fs.rm(marker, { force: true })
  })

  afterEach(async () => {
    warn.mockRestore()
    if (envBefore === undefined) delete process.env[ENV]
    else process.env[ENV] = envBefore
    await fs.rm(marker, { force: true })
  })

  test("dead pid, nothing listening, older than grace: reclaimed, file removed, WARN logged", async () => {
    await seed({ pid: 0, port: await freePort() })

    const guard = await readCanonicalAuthorityGuard()

    expect(guard.engaged).toBe(false)
    expect(await exists()).toBe(false)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][0]).toBe("canonical authority marker stale; reclaimed")
    expect(warn.mock.calls[0][1]).toMatchObject({ path: marker, pid: 0 })
    expect(warn.mock.calls[0][1].ageMs).toBeGreaterThan(HOUR - MINUTE)
  })

  test("dead pid, nothing listening, but within grace: engaged (covers a daemon restart)", async () => {
    await seed({ pid: 0, port: await freePort(), since: new Date(Date.now() - MINUTE).toISOString() })

    const guard = await readCanonicalAuthorityGuard()

    expect(guard.engaged).toBe(true)
    expect(guard.engaged && guard.reason).toBe("within-grace")
    expect(await exists()).toBe(true)
    expect(warn).not.toHaveBeenCalled()
  })

  test("dead pid but the recorded port is listening: engaged", async () => {
    const live = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } })
    try {
      await seed({ pid: 0, port: live.port })

      const guard = await readCanonicalAuthorityGuard()

      expect(guard.engaged).toBe(true)
      expect(guard.engaged && guard.reason).toBe("port-listening")
      expect(guard.engaged && guard.port).toBe(live.port)
      expect(await exists()).toBe(true)
    } finally {
      live.stop(true)
    }
  })

  test("alive pid: engaged regardless of age", async () => {
    await seed({ pid: process.pid, port: await freePort(), since: new Date(Date.now() - 48 * HOUR).toISOString() })

    const guard = await readCanonicalAuthorityGuard()

    expect(guard.engaged).toBe(true)
    expect(guard.engaged && guard.reason).toBe("pid-alive")
    expect(guard.engaged && guard.pid).toBe(process.pid)
    expect(guard.engaged && guard.ageMs).toBeGreaterThan(47 * HOUR)
    expect(await exists()).toBe(true)
  })

  test("corrupt marker older than grace: reclaimed; corrupt and fresh: engaged", async () => {
    await fs.mkdir(path.dirname(marker), { recursive: true })
    await fs.writeFile(marker, "{ not json")
    const old = new Date(Date.now() - HOUR)
    await fs.utimes(marker, old, old)

    expect((await readCanonicalAuthorityGuard()).engaged).toBe(false)
    expect(await exists()).toBe(false)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][1]).toMatchObject({ path: marker, reason: "corrupt-stale" })

    await fs.writeFile(marker, "{ not json")
    const guard = await readCanonicalAuthorityGuard()
    expect(guard.engaged).toBe(true)
    expect(guard.engaged && guard.corrupt).toBe(true)
    expect(await exists()).toBe(true)
  })

  test("graceMs marker field and env override are honoured", async () => {
    const port = await freePort()
    // 1 min old but the marker allows only 10 s of grace => stale.
    await seed({ pid: 0, port, since: new Date(Date.now() - MINUTE).toISOString(), graceMs: 10_000 })
    expect((await readCanonicalAuthorityGuard()).engaged).toBe(false)
    expect(await exists()).toBe(false)

    // Same marker, env widens grace to 2 h => engaged; env wins over the field.
    process.env[ENV] = String(2 * HOUR)
    await seed({ pid: 0, port, since: new Date(Date.now() - MINUTE).toISOString(), graceMs: 10_000 })
    const guard = await readCanonicalAuthorityGuard()
    expect(guard.engaged).toBe(true)
    expect(guard.engaged && guard.reason).toBe("within-grace")
    expect(await exists()).toBe(true)
  })
})
