// OpencodeX-1d2 layer 2: the GUI's embedded worker must never take :4096
// while a canonical launchd daemon is registered via the persistent marker.
// `Server.listenShared` is stubbed (delegating to the real one when no stub is
// set) so no test binds a socket, let alone 4096.
import { afterAll, afterEach, beforeEach, describe, expect, mock, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import * as RealServer from "@/server/server"
import { Global } from "@opencode-ai/core/global"

type ListenShared = typeof RealServer.listenShared
let stub: ListenShared | undefined
const requested: Parameters<ListenShared>[0][] = []

void mock.module("@/server/server", () => ({
  ...RealServer,
  listenShared: (opts: Parameters<ListenShared>[0]) => (stub ?? RealServer.listenShared)(opts),
}))

const fakeListener = (port: number): Awaited<ReturnType<ListenShared>>[number] => ({
  hostname: "127.0.0.1",
  port,
  url: new URL(`http://127.0.0.1:`),
  stop: async () => {},
})

async function failure(work: Promise<unknown>) {
  return work.then(
    () => undefined,
    (error: unknown) => (error instanceof Error ? error.message : String(error)),
  )
}

describe("embedded worker canonical authority guard", () => {
  // Computed here rather than via the helper so the file still loads (and
  // fails on behavior) against a source tree without the guard.
  const marker = path.join(Global.Path.data, "canonical-authority.json")
  const seed = () =>
    fs
      .mkdir(path.dirname(marker), { recursive: true })
      .then(() => fs.writeFile(marker, JSON.stringify({ pid: 4242, hostname: "127.0.0.1", port: 4096, since: "" })))
  let worker: typeof import("@/cli/cmd/tui/worker")

  beforeEach(async () => {
    requested.length = 0
    stub = async (opts) => {
      requested.push(opts)
      return opts.map((_, index) => fakeListener(50_000 + index))
    }
    await fs.rm(marker, { force: true })
    worker ??= await import("@/cli/cmd/tui/worker")
  })

  afterEach(() => fs.rm(marker, { force: true }))

  // Shutdown drains the gate for good, so it runs once; each server() call
  // already replaces the previous owned backend ("reconfigured").
  afterAll(async () => {
    await worker.rpc.shutdown()
    stub = undefined
  })

  test("marker present: port 0 is requested without the :4096 preference", async () => {
    await seed()

    const result = await worker.rpc.server({ port: 0, hostname: "127.0.0.1" })

    expect(requested).toHaveLength(1)
    expect(requested[0]).toEqual([{ hostname: "127.0.0.1", port: 0, cors: [], prefer4096: false }])
    expect(result.url).toBe("http://127.0.0.1:50000/")
  })

  test("marker present: an explicit request for :4096 fails fast naming the marker", async () => {
    await seed()

    const message = await failure(worker.rpc.server({ port: 4096, hostname: "127.0.0.1" }))

    expect(message).toContain("4096")
    expect(message).toContain(marker)
    expect(message).toContain("pid 4242")
    expect(requested).toHaveLength(0)
  })

  test("marker absent: listener options are unchanged", async () => {
    await worker.rpc.server({ port: 0, hostname: "127.0.0.1" })

    expect(requested).toHaveLength(1)
    expect(requested[0]).toEqual([{ hostname: "127.0.0.1", port: 0, cors: [] }])
    expect(requested[0][0]).not.toHaveProperty("prefer4096")
  })

  test("corrupt marker is treated as present (fail safe away from :4096)", async () => {
    await fs.mkdir(path.dirname(marker), { recursive: true })
    await fs.writeFile(marker, "{ not json")

    await worker.rpc.server({ port: 0, hostname: "127.0.0.1" })
    expect(requested[0]).toEqual([{ hostname: "127.0.0.1", port: 0, cors: [], prefer4096: false }])

    const message = await failure(worker.rpc.server({ port: 4096, hostname: "127.0.0.1" }))
    expect(message).toContain(marker)
    expect(message).toContain("unreadable marker")
  })
})
