// OpencodeX-1d2 layer 2: `opencode serve` under OPENCODE_CANONICAL_AUTHORITY=1
// (the launchd hub) records itself in a persistent marker that the GUI's
// embedded worker reads to stay off :4096. The marker survives shutdown on
// purpose - the whole point is covering the daemon's restart window.
import { describe, expect } from "bun:test"
import { Effect } from "effect"
import fs from "node:fs/promises"
import net from "node:net"
import path from "node:path"
import { cliIt } from "../../lib/cli-process"

// Mirrors XDG_DATA_HOME in the child env (test/lib/cli-process.ts).
function markerPath(home: string) {
  return path.join(home, ".local/share/opencode", "canonical-authority.json")
}

// An explicit free port: serve's port 0 would try :4096 first, which tests
// must never bind.
function freePort() {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      server.close(() =>
        typeof address === "object" && address ? resolve(address.port) : reject(new Error("no port")),
      )
    })
  })
}

describe("opencode serve canonical authority marker (subprocess)", () => {
  cliIt.live(
    "writes the marker on listen and leaves it in place after SIGTERM",
    ({ opencode, home }) =>
      Effect.gen(function* () {
        const port = yield* Effect.promise(freePort)
        const serve = yield* opencode.serve({
          port,
          hostname: "127.0.0.1",
          env: { OPENCODE_CANONICAL_AUTHORITY: "1" },
        })
        const marker = JSON.parse(yield* Effect.promise(() => fs.readFile(markerPath(home), "utf8")))

        expect(marker.pid).toBePositive()
        expect(marker.port).toBe(serve.port)
        expect(marker.hostname).toBe("127.0.0.1")
        expect(Date.parse(marker.since)).toBeGreaterThan(0)

        yield* Effect.sync(() => serve.kill())
        yield* Effect.promise(() => serve.exited)
        const survivor = JSON.parse(yield* Effect.promise(() => fs.readFile(markerPath(home), "utf8")))
        expect(survivor.pid).toBe(marker.pid)
      }),
    60_000,
  )

  cliIt.live(
    "does not write the marker without OPENCODE_CANONICAL_AUTHORITY",
    ({ opencode, home }) =>
      Effect.gen(function* () {
        const port = yield* Effect.promise(freePort)
        // Explicit empty: a session started under the launchd hub inherits the flag.
        yield* opencode.serve({ port, hostname: "127.0.0.1", env: { OPENCODE_CANONICAL_AUTHORITY: "" } })
        const exists = yield* Effect.promise(() =>
          fs.access(markerPath(home)).then(
            () => true,
            () => false,
          ),
        )
        expect(exists).toBe(false)
      }),
    60_000,
  )
})
