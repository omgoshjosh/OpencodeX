// Subprocess integration tests for `opencode serve`. Spawns the real CLI in
// headless mode and exercises it over HTTP — this is the only test tier that
// catches bugs spanning argv → server boot → routing → instance loading.
//
// `serve` is long-lived: the harness returns a handle (url/port/kill/exited)
// and kills the process when the test scope closes. The OS-assigned port is
// parsed off the "listening on http://..." line.
import { describe, expect } from "bun:test"
import { Effect, Schedule, Schema } from "effect"
import { HttpClient } from "effect/unstable/http"
import { mkdir, rename, writeFile } from "node:fs/promises"
import path from "node:path"
import { cliIt } from "../../lib/cli-process"

const HealthIdentity = Schema.Struct({
  processRole: Schema.String,
  runID: Schema.String,
  databaseID: Schema.String,
  eventBusID: Schema.String,
})

function writeAuth(home: string, content: string) {
  const file = path.join(home, ".local/share/opencode/auth.json")
  const temporary = `${file}.next`
  return Effect.promise(async () => {
    await mkdir(path.dirname(file), { recursive: true })
    await writeFile(temporary, content)
    await rename(temporary, file)
  })
}

async function openGlobalEvents(url: string) {
  const controller = new AbortController()
  const response = await fetch(`${url}/global/event`, { signal: controller.signal })
  if (!response.ok || !response.body) throw new Error(`global event stream failed: HTTP ${response.status}`)
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  return {
    next: async (): Promise<unknown> => {
      while (true) {
        const newline = buffer.indexOf("\n")
        if (newline >= 0) {
          const line = buffer.slice(0, newline).trimEnd()
          buffer = buffer.slice(newline + 1)
          if (line.startsWith("data:")) return JSON.parse(line.slice(5).trimStart())
          continue
        }
        const chunk = await reader.read()
        if (chunk.done) throw new Error("global event stream closed")
        buffer += decoder.decode(chunk.value, { stream: true })
      }
    },
    close: async () => {
      controller.abort()
      await reader.cancel().catch(() => undefined)
    },
  }
}

async function authChanged(events: Awaited<ReturnType<typeof openGlobalEvents>>) {
  while (true) {
    const event = await events.next()
    if (
      event &&
      typeof event === "object" &&
      "payload" in event &&
      event.payload &&
      typeof event.payload === "object" &&
      "type" in event.payload &&
      event.payload.type === "provider.auth.changed"
    )
      return event
  }
}

describe("opencode serve (subprocess)", () => {
  cliIt.live(
    "fails closed for a passwordless non-loopback listener without a warning",
    ({ opencode }) =>
      Effect.gen(function* () {
        const result = yield* opencode.spawn(["serve", "--port", "0", "--hostname", "0.0.0.0"], {
          env: { OPENCODE_SERVER_PASSWORD: "", OPENCODE_SERVER_ALLOW_INSECURE_LAN: "1" },
        })
        expect(result.exitCode).not.toBe(0)
        expect(result.stderr).toContain("OPENCODE_SERVER_PASSWORD")
        expect(result.stdout).not.toContain("server is unsecured")
      }),
    60_000,
  )

  // Smoke test: server starts, binds a port, and /global/health responds.
  // If this fails, all other serve tests likely will too — debug here first.
  cliIt.live(
    "starts, binds a port, and serves /global/health",
    ({ opencode }) =>
      Effect.gen(function* () {
        const server = yield* opencode.serve()
        expect(server.port).toBeGreaterThan(0)
        expect(server.url).toMatch(/^http:\/\//)

        const client = yield* HttpClient.HttpClient
        const res = yield* client.get(`${server.url}/global/health`)
        expect(res.status).toBe(200)
        // GlobalHealth schema is { success: true, ... } | { success: false, error }.
        // We don't lock in further shape here — any 200 with parseable JSON is
        // enough proof the routing + auth-bypass + instance loading is alive.
        const body = yield* res.json
        expect(body).toBeDefined()
      }),
    60_000,
  )

  cliIt.live(
    "refuses a second serve on the same database",
    ({ opencode }) =>
      Effect.gen(function* () {
        const first = yield* opencode.serve({ env: { OPENCODE_RUN_ID: "serve-first" } })
        const client = yield* HttpClient.HttpClient
        const firstHealth = Schema.decodeUnknownSync(HealthIdentity)(
          yield* (yield* client.get(`${first.url}/global/health`)).json,
        )

        expect(firstHealth).toMatchObject({ processRole: "main", runID: "serve-first" })
        expect(firstHealth).toHaveProperty("databaseID")
        expect(firstHealth).toHaveProperty("eventBusID")

        // Serve claims exclusive per-database backend authority: a second
        // process on the same database must fail clearly, never replace the
        // live authority.
        const second = yield* opencode.spawn(["serve", "--port", "0", "--hostname", "127.0.0.1", "--mdns", "false"], {
          env: { OPENCODE_RUN_ID: "serve-second" },
        })
        expect(second.exitCode).not.toBe(0)
        expect(second.stderr).toContain("A backend authority is already serving this database")

        // The surviving authority is untouched.
        const stillHealthy = Schema.decodeUnknownSync(HealthIdentity)(
          yield* (yield* client.get(`${first.url}/global/health`)).json,
        )
        expect(stillHealthy).toMatchObject({ processRole: "main", runID: "serve-first" })
      }),
    90_000,
  )

  cliIt.live(
    "refreshes provider connectivity after isolated atomic auth replacements without exposing secrets",
    ({ opencode, home }) =>
      Effect.gen(function* () {
        const secret = "sentinel-provider-auth-secret"
        const rotated = "sentinel-provider-auth-secret-rotated"
        const server = yield* opencode.serve({ extraArgs: ["--print-logs"] })
        const client = yield* HttpClient.HttpClient
        const events = yield* Effect.acquireRelease(
          Effect.promise(() => openGlobalEvents(server.url)),
          (stream) => Effect.promise(() => stream.close()),
        )
        const responses: string[] = []
        const providers = () =>
          client.get(`${server.url}/provider`).pipe(
            Effect.flatMap((response) => response.json),
            Effect.tap((body) => Effect.sync(() => responses.push(JSON.stringify(body)))),
          )
        const connected = (value: unknown) => {
          if (!value || typeof value !== "object" || !("connected" in value)) return false
          return Array.isArray(value.connected) && value.connected.includes("anthropic")
        }

        const identity = () =>
          client.get(`${server.url}/global/health`).pipe(
            Effect.flatMap((response) => response.json),
            Effect.map(Schema.decodeUnknownSync(HealthIdentity)),
          )
        const before = yield* identity()
        expect(connected(yield* providers())).toBe(false)
        yield* writeAuth(home, JSON.stringify({ anthropic: { type: "api", key: secret } }))
        yield* providers().pipe(
          Effect.filterOrFail(connected, () => new Error("provider never connected after auth replacement")),
          Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
        )
        const duringConnect = yield* Effect.all(Array.from({ length: 12 }, providers))
        expect(duringConnect.every(connected)).toBe(true)

        const changed = Effect.promise(() => authChanged(events)).pipe(
          Effect.timeoutOrElse({
            duration: "5 seconds",
            orElse: () => Effect.die(new Error("auth rotation event timed out")),
          }),
        )
        yield* writeAuth(home, JSON.stringify({ anthropic: { type: "api", key: rotated } }))
        const duringRotation = yield* Effect.all(Array.from({ length: 12 }, providers))
        expect(duringRotation.every(connected)).toBe(true)
        expect(JSON.stringify(yield* changed)).not.toContain(rotated)

        yield* writeAuth(home, "{}")
        const disconnected = yield* providers().pipe(
          Effect.filterOrFail(
            (value) => !connected(value),
            () => new Error("provider never disconnected after auth replacement"),
          ),
          Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
        )
        const duringDisconnect = yield* Effect.all(Array.from({ length: 12 }, providers))
        expect(duringDisconnect.every((value) => !connected(value))).toBe(true)

        const put = yield* Effect.promise(() =>
          fetch(`${server.url}/auth/anthropic`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ type: "api", key: secret }),
          }),
        )
        expect(put.ok).toBe(true)
        yield* providers().pipe(
          Effect.filterOrFail(connected, () => new Error("provider never connected after auth PUT")),
          Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
        )
        const remove = yield* Effect.promise(() => fetch(`${server.url}/auth/anthropic`, { method: "DELETE" }))
        expect(remove.ok).toBe(true)
        yield* providers().pipe(
          Effect.filterOrFail(
            (value) => !connected(value),
            () => new Error("provider never disconnected after auth DELETE"),
          ),
          Effect.retry({ schedule: Schedule.spaced("250 millis"), times: 20 }),
        )
        expect(yield* identity()).toEqual(before)
        expect(JSON.stringify(disconnected)).not.toContain(secret)
        expect(responses.join("\n")).not.toContain(secret)
        expect(responses.join("\n")).not.toContain(rotated)
        expect(server.stderr()).not.toContain(secret)
        expect(server.stderr()).not.toContain(rotated)
      }),
    90_000,
  )

  // The scope-close finalizer must actually terminate the child. Without this
  // test a regression in the kill path (e.g. a future refactor that forgets
  // to wire the finalizer) would leak processes on every test run.
  cliIt.live(
    "kills the subprocess on scope close",
    ({ opencode }) =>
      Effect.gen(function* () {
        // Inner scope so we can observe `.exited` resolving after it closes.
        const exitedPromise = yield* Effect.scoped(
          Effect.gen(function* () {
            const server = yield* opencode.serve()
            // Capture the Promise, not the resolved value — scope closes after
            // this gen returns, at which point the finalizer kills the child.
            return server.exited
          }),
        )
        // After scope close: finalizer fired, process must have exited.
        const code = yield* Effect.promise(() => exitedPromise)
        // Bun reports the exit code; SIGTERM-killed processes return non-null
        // (typically 143 on POSIX). We just require resolution within a sane
        // window — anything else means the kill didn't take.
        expect(typeof code === "number" || code === null).toBe(true)
      }),
    60_000,
  )
})
