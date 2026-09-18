// Subprocess test for OpencodeX-fs2.6: the headless daemon holds exactly one
// Database graph — one writer plus one `query_only` reader for the process.
//
// `opencode serve` runs its handler inside AppRuntime (whose graph, Database
// included, lives in the process memo map) and then binds its listeners
// through `Server.listenShared`. Before the fix the listener built the routes
// in a private memo map, so the daemon carried two Database layers: two page
// caches, two independent writer permits with busy_timeout as the only thing
// between them, two readers pinning WAL snapshots. The evidence is the fs2.5
// `db_connection_open` gauge the child logs on every native open.
import { describe, expect } from "bun:test"
import { Effect, Schema } from "effect"
import path from "node:path"
import { cliIt } from "../../lib/cli-process"

const Health = Schema.Struct({ healthy: Schema.Boolean })

type Open = { role: string; path: string; active: number }

// One `db_connection_open` INFO line per native handle, e.g.
//   INFO ... connection_id=1 role=writer path=/x/serve.db active_connections=1 db_connection_open
function opens(stderr: string): Open[] {
  return stderr
    .split("\n")
    .filter((line) => line.includes("db_connection_open"))
    .map((line) => ({
      role: line.match(/\brole=(\w+)/)?.[1] ?? "",
      path: line.match(/\bpath=(\S+)/)?.[1] ?? "",
      active: Number(line.match(/\bactive_connections=(\d+)/)?.[1] ?? NaN),
    }))
}

function request(url: string, directory: string, route: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers)
  headers.set("x-opencode-directory", directory)
  return fetch(new URL(route, url), { ...init, headers })
}

describe("opencode serve single Database graph (subprocess)", () => {
  cliIt.live(
    "the daemon opens exactly one writer and one reader on its file database, and serving requests opens no more",
    ({ home, opencode }) =>
      Effect.gen(function* () {
        const database = path.join(home, "single-graph.db")
        const server = yield* opencode.serve({
          env: { OPENCODE_DB: database, OPENCODE_RUN_ID: "serve-single-database" },
          extraArgs: ["--print-logs"],
        })

        // Health is served by the listener graph; a session list goes through
        // instance bootstrap and the Database service on the request path.
        // Both must run on the connections opened at startup.
        const health = yield* Effect.promise(async () =>
          Schema.decodeUnknownSync(Health)(await (await fetch(new URL("/global/health", server.url))).json()),
        )
        expect(health.healthy).toBe(true)
        const sessions = yield* Effect.promise(() => request(server.url, home, "/session"))
        expect(sessions.status).toBe(200)
        expect(yield* Effect.promise(() => sessions.json())).toEqual([])

        // SIGTERM before reading the gauge so the log is complete, and so the
        // child is gone before the fixture tears the stderr drain down.
        yield* Effect.sync(() => server.kill())
        expect(yield* Effect.promise(() => server.exited)).toBe(0)

        const connections = opens(server.stderr()).filter((open) => open.path === database)
        expect(
          connections.map((open) => open.role).toSorted(),
          `expected one writer and one reader for the process, got:\n${JSON.stringify(connections, null, 2)}`,
        ).toEqual(["reader", "writer"])
        expect(connections.map((open) => open.active)).toEqual([1, 2])
      }),
  )
})
