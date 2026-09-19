import { Effect, Layer } from "effect"
import { Auth } from "../../src/auth"

export const empty = Layer.mock(Auth.Service)({
  all: () => Effect.succeed({}),
  snapshot: () => Effect.succeed({ records: {}, revision: "empty" }),
})

export * as AuthTest from "./auth"
