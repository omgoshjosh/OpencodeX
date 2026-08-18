import { NodeHttpServer } from "@effect/platform-node"
import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber, Layer, Option, Schema } from "effect"
import { HttpClient, HttpClientRequest, HttpRouter } from "effect/unstable/http"
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiError, HttpApiGroup } from "effect/unstable/httpapi"
import { ServerAuth } from "../../src/server/auth"
import { Authorization, authorizationLayer } from "../../src/server/routes/instance/httpapi/middleware/authorization"
import { testEffect } from "../lib/effect"
import { CoordinatorAuthority } from "../../src/server/coordinator-authority"
import { OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"

let held: Deferred.Deferred<void> | undefined
let heldStarted: Deferred.Deferred<void> | undefined

const Api = HttpApi.make("test-authorization").add(
  HttpApiGroup.make("test")
    .add(
      HttpApiEndpoint.get("probe", "/probe", {
        success: Schema.String,
      }),
      HttpApiEndpoint.get("missing", "/missing", {
        success: Schema.String,
        error: HttpApiError.NotFound,
      }),
      HttpApiEndpoint.post("hold", "/hold", { success: Schema.String }),
      HttpApiEndpoint.post("interrupt", "/interrupt", { success: Schema.String }),
    )
    .middleware(Authorization),
)

const handlers = HttpApiBuilder.group(Api, "test", (handlers) =>
  handlers
    .handle("probe", () => Effect.succeed("ok"))
    .handle("missing", () => Effect.fail(new HttpApiError.NotFound({})))
    .handle("hold", () =>
      Effect.gen(function* () {
        if (!held || !heldStarted) return "unused"
        yield* Deferred.succeed(heldStarted, undefined)
        yield* Deferred.await(held)
        return "released"
      }),
    )
    .handle("interrupt", () => Effect.interrupt),
)

const apiLayer = HttpRouter.serve(
  HttpApiBuilder.layer(Api).pipe(Layer.provide(handlers), Layer.provide(authorizationLayer)),
  { disableListenLog: true, disableLogger: true },
).pipe(Layer.provideMerge(NodeHttpServer.layerTest))

const noAuthLayer = ServerAuth.Config.layer({ password: Option.none(), username: "opencode" })
const secretLayer = ServerAuth.Config.layer({ password: Option.some("secret"), username: "opencode" })
const kitSecretLayer = ServerAuth.Config.layer({ password: Option.some("secret"), username: "kit" })

const it = testEffect(apiLayer.pipe(Layer.provide(noAuthLayer)))
const itSecret = testEffect(apiLayer.pipe(Layer.provide(secretLayer)))
const itKitSecret = testEffect(apiLayer.pipe(Layer.provide(kitSecretLayer)))

const basic = (username: string, password: string) => ServerAuth.header({ username, password }) ?? ""

const token = (username: string, password: string) => Buffer.from(`${username}:${password}`).toString("base64")

const getProbe = (headers?: Record<string, string>) =>
  HttpClientRequest.get("/probe").pipe(
    headers ? HttpClientRequest.setHeaders(headers) : (request) => request,
    HttpClient.execute,
  )

describe.serial("HttpApi authorization middleware", () => {
  it.live("allows requests when server password is not configured", () =>
    Effect.gen(function* () {
      const response = yield* getProbe()

      expect(response.status).toBe(200)
      expect(yield* response.json).toBe("ok")
    }),
  )

  itSecret.live("requires configured password for basic auth", () =>
    Effect.gen(function* () {
      const [missing, badPassword, good] = yield* Effect.all(
        [
          getProbe(),
          getProbe({ authorization: basic("opencode", "wrong") }),
          getProbe({ authorization: basic("opencode", "secret") }),
        ],
        { concurrency: "unbounded" },
      )

      expect(missing.status).toBe(401)
      expect(missing.headers["www-authenticate"] ?? "").toContain("Basic")
      expect(badPassword.status).toBe(401)
      expect(badPassword.headers["www-authenticate"] ?? "").toContain("Basic")
      expect(good.status).toBe(200)
    }),
  )

  itKitSecret.live("respects configured basic auth username", () =>
    Effect.gen(function* () {
      const [defaultUser, configuredUser] = yield* Effect.all(
        [getProbe({ authorization: basic("opencode", "secret") }), getProbe({ authorization: basic("kit", "secret") })],
        { concurrency: "unbounded" },
      )

      expect(defaultUser.status).toBe(401)
      expect(configuredUser.status).toBe(200)
    }),
  )

  itSecret.live("accepts auth token query credentials", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.get(`/probe?auth_token=${encodeURIComponent(token("opencode", "secret"))}`)

      expect(response.status).toBe(200)
    }),
  )

  itSecret.live("prefers auth token query credentials over basic auth", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get(
        `/probe?auth_token=${encodeURIComponent(token("opencode", "secret"))}`,
      ).pipe(HttpClientRequest.setHeader("authorization", basic("opencode", "wrong")), HttpClient.execute)

      expect(response.status).toBe(200)
    }),
  )

  itSecret.live("preserves handler errors when basic auth succeeds", () =>
    Effect.gen(function* () {
      const response = yield* HttpClientRequest.get("/missing").pipe(
        HttpClientRequest.setHeader("authorization", basic("opencode", "secret")),
        HttpClient.execute,
      )

      expect(response.status).toBe(404)
    }),
  )

  itSecret.live("preserves handler errors when auth token query succeeds", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.get(`/missing?auth_token=${encodeURIComponent(token("opencode", "secret"))}`)

      expect(response.status).toBe(404)
    }),
  )

  itSecret.live("rejects malformed auth token query credentials", () =>
    Effect.gen(function* () {
      const response = yield* HttpClient.get("/probe?auth_token=not-base64")

      expect(response.status).toBe(401)
    }),
  )

  itSecret.live("acquires admission only after authentication and releases on completion and error", () =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        process.env[OPENCODE_PROCESS_ROLE] = "coordinator"
        process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = "authorization-test-epoch"
        CoordinatorAuthority.resetForTest()
      }),
      () =>
        Effect.gen(function* () {
          const unauthorized = yield* getProbe()
          expect(unauthorized.status).toBe(401)
          yield* Effect.promise(() => CoordinatorAuthority.close())
          CoordinatorAuthority.reopen()

          held = yield* Deferred.make<void>()
          heldStarted = yield* Deferred.make<void>()
          const pending = yield* HttpClientRequest.post("/hold").pipe(
            HttpClientRequest.setHeader("authorization", basic("opencode", "secret")),
            HttpClient.execute,
            Effect.forkScoped,
          )
          yield* Deferred.await(heldStarted)
          const drain = CoordinatorAuthority.close()
          expect(yield* Effect.promise(() => Promise.race([drain.then(() => true), Promise.resolve(false)]))).toBe(
            false,
          )
          yield* Deferred.succeed(held, undefined)
          expect((yield* Fiber.join(pending)).status).toBe(200)
          yield* Effect.promise(() => drain)

          CoordinatorAuthority.reopen()
          const failed = yield* HttpClientRequest.get("/missing").pipe(
            HttpClientRequest.setHeader("authorization", basic("opencode", "secret")),
            HttpClient.execute,
          )
          expect(failed.status).toBe(404)
          yield* Effect.promise(() => CoordinatorAuthority.close())

          CoordinatorAuthority.reopen()
          yield* HttpClientRequest.post("/interrupt").pipe(
            HttpClientRequest.setHeader("authorization", basic("opencode", "secret")),
            HttpClient.execute,
            Effect.exit,
          )
          yield* Effect.promise(() => CoordinatorAuthority.close())
        }),
      () =>
        Effect.sync(() => {
          held = undefined
          heldStarted = undefined
          CoordinatorAuthority.resetForTest()
          delete process.env[OPENCODE_PROCESS_ROLE]
          delete process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH
        }),
    ),
  )
})
