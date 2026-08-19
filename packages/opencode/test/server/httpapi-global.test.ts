import { NodeHttpServer } from "@effect/platform-node"
import { describe, expect, setSystemTime } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import { HttpBody, HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Auth } from "../../src/auth"
import { Config } from "../../src/config/config"
import { Installation } from "../../src/installation"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { GuiBridge } from "../../src/opencodex/gui-bridge"
import { SessionID } from "../../src/session/schema"
import { RootHttpApi } from "../../src/server/routes/instance/httpapi/api"
import { GlobalPaths } from "../../src/server/routes/instance/httpapi/groups/global"
import { controlHandlers } from "../../src/server/routes/instance/httpapi/handlers/control"
import { globalHandlers } from "../../src/server/routes/instance/httpapi/handlers/global"
import { authorizationLayer } from "../../src/server/routes/instance/httpapi/middleware/authorization"
import { schemaErrorLayer } from "../../src/server/routes/instance/httpapi/middleware/schema-error"
import { testEffect } from "../lib/effect"
import { CoordinatorAuthority } from "../../src/server/coordinator-authority"
import { CoordinatorHandoff } from "../../src/server/coordinator-handoff"
import { OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"
import {
  COORDINATOR_MANIFEST_VERSION,
  coordinatorHandoffRequestID,
  coordinatorManifestPath,
  readCoordinatorHandoff,
} from "@opencode-ai/sdk/coordinator"
import { tmpdir } from "../fixture/fixture"
import fs from "node:fs/promises"
import path from "node:path"
import { ServerAuth } from "../../src/server/auth"

const guiBridgeLayer = GuiBridge.layer.pipe(Layer.provideMerge(EventV2Bridge.defaultLayer))

const baseApiLayer = HttpRouter.serve(
  HttpApiBuilder.layer(RootHttpApi).pipe(
    Layer.provide([controlHandlers, globalHandlers]),
    Layer.provide([authorizationLayer, schemaErrorLayer]),
    // Raw HttpApi routes expose an opaque handler context at the request boundary.
    // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
    HttpRouter.provideRequest(Layer.succeedContext(Context.empty() as Context.Context<unknown>)),
  ),
  { disableListenLog: true, disableLogger: true },
).pipe(
  Layer.provideMerge(NodeHttpServer.layerTest),
  Layer.provide(Layer.mock(Auth.Service)({})),
  Layer.provide(Layer.mock(Config.Service)({})),
  Layer.provide(
    Layer.mock(Installation.Service)({
      method: () => Effect.succeed("npm"),
      latest: () => Effect.succeed("9.9.9"),
      upgrade: () => Effect.void,
    }),
  ),
  Layer.provideMerge(guiBridgeLayer),
  Layer.provide(Database.defaultLayer),
)
const apiLayer = baseApiLayer.pipe(
  Layer.provide(ServerAuth.Config.layer({ password: Option.none(), username: "opencode" })),
)
const it = testEffect(apiLayer)
const itSecret = testEffect(
  baseApiLayer.pipe(
    Layer.provide(ServerAuth.Config.layer({ password: Option.some("basic-secret"), username: "opencode" })),
  ),
)

const handoffKey = "c".repeat(40)
const sourceEpoch = "source-epoch-http-0000000000000001"
const targetEpoch = "target-epoch-http-0000000000000001"
const capability = "capability-http-00000000000000000000000001"
const requestID = (target = targetEpoch) => coordinatorHandoffRequestID(sourceEpoch, target)

function basic() {
  return ServerAuth.header({ username: "opencode", password: "basic-secret" }) ?? ""
}

function handoffRequest(payload: unknown, headers: Record<string, string> = {}) {
  return HttpClientRequest.post(GlobalPaths.authorityHandoff).pipe(
    HttpClientRequest.setHeaders({ authorization: basic(), "x-opencode-handoff-capability": capability, ...headers }),
    HttpClientRequest.bodyJsonUnsafe(payload),
    HttpClient.execute,
  )
}

function setupHandoff(stateRoot: string) {
  return Effect.acquireRelease(
    Effect.promise(async () => {
      process.env[OPENCODE_PROCESS_ROLE] = "coordinator"
      CoordinatorAuthority.resetForTest()
      CoordinatorAuthority.initialize(sourceEpoch)
      CoordinatorHandoff.resetForTest()
      CoordinatorHandoff.initialize({ capability })
      CoordinatorHandoff.overrideForTest({ key: handoffKey, stateRoot })
      const file = coordinatorManifestPath(stateRoot, handoffKey)
      await fs.mkdir(path.dirname(file), { recursive: true })
      await fs.writeFile(
        file,
        JSON.stringify({
          version: COORDINATOR_MANIFEST_VERSION,
          key: handoffKey,
          directory: "/tmp",
          database: "/tmp/authority-handoff-http.db",
          pid: process.pid,
          url: "http://127.0.0.1:4096/",
          username: "opencode",
          password: "basic-secret",
          token: "token",
          createdAt: "2026-08-18T20:00:00.000Z",
          serverVersion: "local",
          authorityEpoch: sourceEpoch,
          admission: true,
          ready: true,
        }),
      )
    }),
    () =>
      Effect.sync(() => {
        CoordinatorHandoff.overrideForTest()
        CoordinatorHandoff.resetForTest()
        CoordinatorAuthority.resetForTest()
        delete process.env[OPENCODE_PROCESS_ROLE]
        setSystemTime()
      }),
  )
}

function waitForRequested(stateRoot: string) {
  return Effect.promise(async () => {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const handoff = await readCoordinatorHandoff(stateRoot, handoffKey).catch(() => undefined)
      if (handoff && "phase" in handoff) return handoff
      await Bun.sleep(5)
    }
    throw new Error("handoff was not published")
  })
}

function tempdir() {
  return Effect.acquireRelease(
    Effect.promise(() => tmpdir()),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  )
}

describe.serial("global HttpApi", () => {
  it.live("upgrades to latest when the request body is omitted", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.post(GlobalPaths.upgrade)

      expect(response.status).toBe(200)
      expect(yield* response.json).toEqual({ success: true, version: "9.9.9" })
    }),
  )

  it.live("rejects malformed upgrade payloads", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.post(GlobalPaths.upgrade).pipe(
        HttpClientRequest.setBody(HttpBody.text("{", "application/json")),
        HttpClient.execute,
      )

      expect(response.status).toBe(400)
      expect(yield* response.json).toEqual({ success: false, error: "Invalid request body" })
    }),
  )

  it.live("syncs many GUI bridge scopes and correlates responses without loading an instance", () =>
    Effect.gen(function* () {
      const clientID = "gui-global-http-test"
      const token = "a".repeat(32)
      const scopes = Array.from({ length: 128 }, (_, index) => ({ directory: `C:/repo-${index}` }))
      const response = yield* HttpClientRequest.post(GlobalPaths.guiBridgeSync).pipe(
        HttpClientRequest.bodyJsonUnsafe({ clientID, token, capabilities: ["browser.state"], scopes }),
        HttpClient.execute,
      )
      expect(response.status).toBe(200)
      expect(yield* response.json).toMatchObject({ added: 128, removed: 0, unchanged: 0 })

      const bridge = yield* GuiBridge.Service
      expect(yield* bridge.capabilities(scopes[73])).toEqual(["browser.state"])
      const requestID = yield* Deferred.make<GuiBridge.RequestID>()
      const events = yield* EventV2Bridge.Service
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== GuiBridge.Event.Request.type) return Effect.void
        if (!event.data || typeof event.data !== "object" || !("requestID" in event.data)) return Effect.void
        if (typeof event.data.requestID !== "string") return Effect.void
        return Deferred.succeed(requestID, GuiBridge.RequestID.make(event.data.requestID)).pipe(Effect.asVoid)
      })
      yield* Effect.addFinalizer(() => unsubscribe)
      const pending = yield* bridge
        .request({
          directory: scopes[73].directory,
          sessionID: SessionID.make("ses_gui_global_http_test"),
          operation: "browser.state",
          input: {},
        })
        .pipe(Effect.forkScoped)
      const id = yield* Deferred.await(requestID)

      const responded = yield* HttpClientRequest.post(GlobalPaths.guiBridgeRespond).pipe(
        HttpClientRequest.bodyJsonUnsafe({
          clientID,
          token,
          requestID: id,
          operation: "browser.state",
          result: { status: "ok", output: { url: "https://example.com/" } },
        }),
        HttpClient.execute,
      )
      expect(responded.status).toBe(200)
      expect(yield* Fiber.join(pending)).toEqual({ url: "https://example.com/" })
    }),
  )

  itSecret.live("authenticates and drains coordinator handoff through production middleware", () =>
    Effect.gen(function* () {
      const tmp = yield* tempdir()
      yield* setupHandoff(tmp.path)
      setSystemTime(new Date("2026-08-18T22:00:00.000Z"))
      const release = CoordinatorAuthority.acquire("/held-mutation")!
      const pending = yield* handoffRequest({ action: "request", request: requestID(), targetEpoch }).pipe(
        Effect.forkScoped,
      )
      expect(yield* waitForRequested(tmp.path)).toMatchObject({ phase: "requested", revision: 0 })

      const changedTarget = `${targetEpoch}-changed`
      const changedRetry = yield* handoffRequest({
        action: "request",
        request: requestID(),
        targetEpoch: changedTarget,
      })
      expect(changedRetry.status).toBe(409)
      const unchanged = yield* waitForRequested(tmp.path)
      expect(unchanged).toMatchObject({
        request: requestID(),
        phase: "requested",
      })
      expect("targetEpoch" in unchanged).toBe(false)

      const health = yield* HttpClientRequest.get(GlobalPaths.health).pipe(
        HttpClientRequest.setHeader("authorization", basic()),
        HttpClient.execute,
      )
      expect(yield* health.json).toMatchObject({ admission: false, ready: false, authorityEpoch: sourceEpoch })
      const dispose = yield* HttpClientRequest.post(GlobalPaths.dispose).pipe(
        HttpClientRequest.setHeader("authorization", basic()),
        HttpClient.execute,
      )
      expect(dispose.status).toBe(409)
      release()

      const response = yield* Fiber.join(pending)
      expect(response.status).toBe(200)
      expect(yield* response.json).toEqual({ success: true, phase: "accepted" })
      expect(yield* Effect.promise(() => readCoordinatorHandoff(tmp.path, handoffKey))).toMatchObject({
        createdAt: "2026-08-18T22:00:00.000Z",
        updatedAt: "2026-08-18T22:00:00.001Z",
      })
      const retry = yield* handoffRequest({ action: "request", request: requestID(), targetEpoch })
      expect(retry.status).toBe(200)
      expect(yield* retry.json).toEqual({ success: true, phase: "accepted" })
      const conflict = yield* handoffRequest({
        action: "request",
        request: requestID(`${targetEpoch}-other`),
        targetEpoch,
      })
      expect(conflict.status).toBe(409)
      expect(CoordinatorAuthority.health()?.admission).toBe(false)
      const aborted = yield* handoffRequest({
        action: "abort",
        expected: {
          request: requestID(),
          phase: "accepted",
          revision: 1,
          sourceEpoch,
          targetEpoch,
        },
      })
      expect(aborted.status).toBe(409)
      expect(CoordinatorAuthority.health()?.admission).toBe(false)
    }),
  )

  itSecret.live("requires Basic auth and the dedicated handoff capability", () =>
    Effect.gen(function* () {
      const tmp = yield* tempdir()
      yield* setupHandoff(tmp.path)
      const payload = { action: "request", request: requestID(), targetEpoch }
      const missingBasic = yield* HttpClientRequest.post(GlobalPaths.authorityHandoff).pipe(
        HttpClientRequest.setHeader("x-opencode-handoff-capability", capability),
        HttpClientRequest.bodyJsonUnsafe(payload),
        HttpClient.execute,
      )
      const missingCapability = yield* handoffRequest(payload, { "x-opencode-handoff-capability": "" })
      const wrongCapability = yield* handoffRequest({}, { "x-opencode-handoff-capability": "wrong" })

      expect(missingBasic.status).toBe(401)
      expect(missingCapability.status).toBe(403)
      expect(wrongCapability.status).toBe(403)
      expect(CoordinatorAuthority.health()?.admission).toBe(true)
    }),
  )

  itSecret.live("keeps handoff control disabled without explicit capability initialization", () =>
    Effect.gen(function* () {
      const tmp = yield* tempdir()
      yield* setupHandoff(tmp.path)
      CoordinatorHandoff.initialize()

      const response = yield* handoffRequest({ action: "request", request: requestID(), targetEpoch })

      expect(response.status).toBe(403)
      expect(CoordinatorHandoff.available()).toBe(false)
      expect(CoordinatorAuthority.health()).toMatchObject({ admission: true, ready: true })
      expect(
        yield* Effect.promise(() => readCoordinatorHandoff(tmp.path, handoffKey).catch(() => undefined)),
      ).toBeUndefined()
    }),
  )

  itSecret.live("rejects oversized handoff bodies before parsing", () =>
    Effect.gen(function* () {
      const tmp = yield* tempdir()
      yield* setupHandoff(tmp.path)
      const response = yield* HttpClientRequest.post(GlobalPaths.authorityHandoff).pipe(
        HttpClientRequest.setHeaders({
          authorization: basic(),
          "x-opencode-handoff-capability": capability,
        }),
        HttpClientRequest.setBody(HttpBody.text(" ".repeat(4_097), "application/json")),
        HttpClient.execute,
      )
      expect(response.status).toBe(413)
    }),
  )

  itSecret.live("rejects unbounded handoff identifiers and epochs", () =>
    Effect.gen(function* () {
      const tmp = yield* tempdir()
      yield* setupHandoff(tmp.path)
      const empty = yield* handoffRequest({ action: "request", request: "", targetEpoch })
      const long = yield* handoffRequest({ action: "request", request: "x".repeat(129), targetEpoch })
      const shortEpoch = yield* handoffRequest({
        action: "request",
        request: coordinatorHandoffRequestID(sourceEpoch, "short"),
        targetEpoch: "short",
      })

      expect(empty.status).toBe(400)
      expect(long.status).toBe(400)
      expect(shortEpoch.status).toBe(400)
      expect(CoordinatorAuthority.health()?.admission).toBe(true)
    }),
  )
})
