import { NodeHttpServer } from "@effect/platform-node"
import { describe, expect, test } from "bun:test"
import { Context, Deferred, Effect, Fiber, Layer, Option } from "effect"
import { HttpBody, HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Auth } from "../../src/auth"
import { Config } from "../../src/config/config"
import { Installation } from "../../src/installation"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { OpencodeXSwarmTable } from "@opencode-ai/core/opencodex/sql"
import { GuiBridge } from "../../src/opencodex/gui-bridge"
import { SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { ServerAuth } from "../../src/server/auth"
import { RootHttpApi } from "../../src/server/routes/instance/httpapi/api"
import { GlobalPaths } from "../../src/server/routes/instance/httpapi/groups/global"
import { controlHandlers } from "../../src/server/routes/instance/httpapi/handlers/control"
import { globalHandlers } from "../../src/server/routes/instance/httpapi/handlers/global"
import { authorizationLayer } from "../../src/server/routes/instance/httpapi/middleware/authorization"
import { schemaErrorLayer } from "../../src/server/routes/instance/httpapi/middleware/schema-error"
import { testEffect } from "../lib/effect"
import { eq } from "drizzle-orm"

const guiBridgeLayer = GuiBridge.layer.pipe(Layer.provideMerge(EventV2Bridge.defaultLayer))

const apiLayer = HttpRouter.serve(
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
  Layer.provide(ServerAuth.Config.layer({ password: Option.none(), username: "opencode" })),
  Layer.provideMerge(SessionStatus.layer),
  Layer.provideMerge(guiBridgeLayer),
  Layer.provideMerge(Database.defaultLayer),
)
const it = testEffect(apiLayer)

describe("global HttpApi", () => {
  it.live("only executing swarm rows block idle-gated redeploy", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const id = "swm_health_activity_test"
      const now = Date.now()
      yield* database.db
        .insert(OpencodeXSwarmTable)
        .values({
          id,
          title: "health",
          prompt: "",
          status: "planned",
          source: "manual",
          time_created: now,
          time_updated: now,
        })
        .run()
      yield* Effect.addFinalizer(() =>
        database.db.delete(OpencodeXSwarmTable).where(eq(OpencodeXSwarmTable.id, id)).run().pipe(Effect.orDie),
      )

      const health = () => HttpClient.get(GlobalPaths.health).pipe(Effect.flatMap((response) => response.json))
      expect(yield* health()).toMatchObject({ active: false })

      yield* database.db
        .update(OpencodeXSwarmTable)
        .set({ status: "running" })
        .where(eq(OpencodeXSwarmTable.id, id))
        .run()
      expect(yield* health()).toMatchObject({ active: true })

      yield* database.db
        .update(OpencodeXSwarmTable)
        .set({ status: "blocked" })
        .where(eq(OpencodeXSwarmTable.id, id))
        .run()
      expect(yield* health()).toMatchObject({ active: false })
    }),
  )

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
})
