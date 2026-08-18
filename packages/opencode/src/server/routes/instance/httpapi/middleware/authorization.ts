import { ServerAuth } from "@/server/auth"
import { Effect, Encoding, Layer, Redacted } from "effect"
import { HttpEffect, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpApiError, HttpApiMiddleware } from "effect/unstable/httpapi"
import { CoordinatorAuthority } from "@/server/coordinator-authority"
import { CoordinatorHandoff } from "@/server/coordinator-handoff"

const AUTH_TOKEN_QUERY = "auth_token"
const UNAUTHORIZED = 401
const WWW_AUTHENTICATE = 'Basic realm="Secure Area"'

// Avoid HttpApiSecurity alternatives here: Effect security middleware wraps the
// full handler, so a downstream failure can make the next auth alternative run
// and remap an authorized NotFound into Unauthorized.
export class Authorization extends HttpApiMiddleware.Service<Authorization>()(
  "@opencode/ExperimentalHttpApiAuthorization",
  {
    error: HttpApiError.UnauthorizedNoContent,
  },
) {}

function emptyCredential() {
  return {
    username: "",
    password: Redacted.make(""),
  }
}

function validateCredential<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  credential: ServerAuth.DecodedCredentials,
  config: ServerAuth.Info,
) {
  return Effect.gen(function* () {
    if (ServerAuth.required(config) && !ServerAuth.authorized(credential, config)) {
      yield* HttpEffect.appendPreResponseHandler((_request, response) =>
        Effect.succeed(HttpServerResponse.setHeader(response, "www-authenticate", WWW_AUTHENTICATE)),
      )
      return yield* new HttpApiError.Unauthorized({})
    }
    const request = yield* HttpServerRequest.HttpServerRequest
    if (new URL(request.url, "http://localhost").pathname === "/global/authority-handoff") {
      const length = Number(request.headers["content-length"])
      if (!Number.isInteger(length) || length < 1 || length > 4_096)
        return yield* Effect.succeed(
          HttpServerResponse.jsonUnsafe({ error: "request_body_too_large" }, { status: 413 }),
        )
      if (
        !CoordinatorHandoff.available() ||
        !CoordinatorHandoff.authorized(request.headers[CoordinatorHandoff.CAPABILITY_HEADER])
      )
        return yield* Effect.succeed(
          HttpServerResponse.jsonUnsafe({ error: "handoff_control_unavailable" }, { status: 403 }),
        )
    }
    if (!CoordinatorAuthority.enabled()) return yield* effect
    const release = CoordinatorAuthority.acquire(request.url)
    if (!release) {
      return yield* Effect.succeed(
        HttpServerResponse.jsonUnsafe(
          { error: "authority_transition", code: "coordinator_admission_closed" },
          { status: 409 },
        ),
      )
    }
    return yield* effect.pipe(Effect.ensuring(Effect.sync(release)))
  })
}

function decodeCredential(input: string) {
  return Effect.fromResult(Encoding.decodeBase64String(input)).pipe(
    Effect.match({
      onFailure: emptyCredential,
      onSuccess: (header) => {
        const separator = header.indexOf(":")
        if (separator === -1) return emptyCredential()
        return {
          username: header.slice(0, separator),
          password: Redacted.make(header.slice(separator + 1)),
        }
      },
    }),
  )
}

function credentialFromRequest(request: HttpServerRequest.HttpServerRequest) {
  return credentialFromURL(new URL(request.url, "http://localhost"), request)
}

function credentialFromURL(url: URL, request: HttpServerRequest.HttpServerRequest) {
  const token = url.searchParams.get(AUTH_TOKEN_QUERY)
  if (token) return decodeCredential(token)
  const match = /^Basic\s+(.+)$/i.exec(request.headers.authorization ?? "")
  if (match) return decodeCredential(match[1])
  return Effect.succeed(emptyCredential())
}

function validateRawCredential<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  credential: ServerAuth.DecodedCredentials,
  config: ServerAuth.Info,
) {
  if (ServerAuth.required(config) && !ServerAuth.authorized(credential, config))
    return Effect.succeed(
      HttpServerResponse.empty({
        status: UNAUTHORIZED,
        headers: { "www-authenticate": WWW_AUTHENTICATE },
      }),
    )
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    if (!CoordinatorAuthority.enabled()) return yield* effect
    const release = CoordinatorAuthority.acquire(request.url)
    if (!release)
      return HttpServerResponse.jsonUnsafe(
        { error: "authority_transition", code: "coordinator_admission_closed" },
        { status: 409 },
      )
    return yield* effect.pipe(Effect.ensuring(Effect.sync(release)))
  })
}

export const authorizationRouterMiddleware = HttpRouter.middleware()(
  Effect.gen(function* () {
    const config = yield* ServerAuth.Config
    return (effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const url = new URL(request.url, "http://localhost")
        return yield* credentialFromURL(url, request).pipe(
          Effect.flatMap((credential) => validateRawCredential(effect, credential, config)),
        )
      })
  }),
)

export const authorizationLayer = Layer.effect(
  Authorization,
  Effect.gen(function* () {
    const config = yield* ServerAuth.Config
    return Authorization.of((effect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        return yield* credentialFromRequest(request).pipe(
          Effect.flatMap((credential) => validateCredential(effect, credential, config)),
        )
      }),
    )
  }),
)
