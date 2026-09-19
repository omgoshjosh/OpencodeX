import path from "path"
import { Effect, Layer, Record, Result, Schema, Context, Semaphore, Schedule } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { NonNegativeInt } from "@opencode-ai/core/schema"
import { Global } from "@opencode-ai/core/global"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Hash } from "@opencode-ai/core/util/hash"

export const OAUTH_DUMMY_KEY = "opencode-oauth-dummy-key"

export const Event = {
  Changed: EventV2.define({ type: "provider.auth.changed", schema: {} }),
}

const fail = (message: string) => (cause: unknown) => new AuthError({ message, cause })

export class Oauth extends Schema.Class<Oauth>("OAuth")({
  type: Schema.Literal("oauth"),
  refresh: Schema.String,
  access: Schema.String,
  expires: NonNegativeInt,
  accountId: Schema.optional(Schema.String),
  enterpriseUrl: Schema.optional(Schema.String),
}) {}

export class Api extends Schema.Class<Api>("ApiAuth")({
  type: Schema.Literal("api"),
  key: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.String)),
}) {}

export class WellKnown extends Schema.Class<WellKnown>("WellKnownAuth")({
  type: Schema.Literal("wellknown"),
  key: Schema.String,
  token: Schema.String,
}) {}

export const Info = Schema.Union([Oauth, Api, WellKnown]).annotate({ discriminator: "type", identifier: "Auth" })
export type Info = Schema.Schema.Type<typeof Info>

export class AuthError extends Schema.TaggedErrorClass<AuthError>()("AuthError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect),
}) {}

export interface Snapshot {
  readonly records: Record<string, Info>
  readonly revision: string
}

export interface Interface {
  readonly get: (providerID: string) => Effect.Effect<Info | undefined, AuthError>
  readonly all: () => Effect.Effect<Record<string, Info>, AuthError>
  readonly snapshot: () => Effect.Effect<Snapshot, AuthError>
  readonly set: (key: string, info: Info) => Effect.Effect<void, AuthError>
  readonly remove: (key: string) => Effect.Effect<void, AuthError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Auth") {}

export const layer = (file = path.join(Global.Path.data, "auth.json")) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const fsys = yield* AppFileSystem.Service
      const events = yield* EventV2.Service
      const decode = Schema.decodeUnknownOption(Info)
      const lock = Semaphore.makeUnsafe(1)
      let last: Snapshot | undefined

      const snapshotUnlocked = Effect.fnUntraced(function* () {
        const parse = (content: string) => {
          const raw = JSON.parse(content)
          if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("Invalid auth snapshot")
          const records = Record.filterMap(raw as Record<string, unknown>, (value) =>
            Result.fromOption(decode(value), () => undefined),
          )
          if (Object.keys(records).length !== Object.keys(raw).length) throw new Error("Invalid auth snapshot")
          return { records, revision: Hash.fast(content) }
        }
        if (process.env.OPENCODE_AUTH_CONTENT) {
          try {
            const next = parse(process.env.OPENCODE_AUTH_CONTENT)
            last = next
            return next
          } catch {}
        }
        const content = new TextDecoder().decode(
          yield* fsys.readFile(file).pipe(Effect.orElseSucceed(() => new Uint8Array())),
        )
        try {
          const next = parse(content)
          last = next
          return next
        } catch {
          return last ?? { records: {}, revision: Hash.fast("") }
        }
      })

      const snapshot = Effect.fn("Auth.snapshot")(() => lock.withPermits(1)(snapshotUnlocked()))

      const all = Effect.fn("Auth.all")(function* () {
        return (yield* snapshot()).records
      })

      const get = Effect.fn("Auth.get")(function* (providerID: string) {
        return (yield* all())[providerID]
      })

      const set = Effect.fn("Auth.set")(function* (key: string, info: Info) {
        yield* lock.withPermits(1)(
          Effect.gen(function* () {
            const norm = key.replace(/\/+$/, "")
            const data = last?.records ?? (yield* snapshotUnlocked()).records
            const next = { ...data, [norm]: info }
            if (norm !== key) delete next[key]
            delete next[norm + "/"]
            yield* fsys.writeJson(file, next, 0o600).pipe(Effect.mapError(fail("Failed to write auth data")))
            last = { records: next, revision: Hash.fast(JSON.stringify(next, null, 2)) }
          }),
        )
      })

      const remove = Effect.fn("Auth.remove")(function* (key: string) {
        yield* lock.withPermits(1)(
          Effect.gen(function* () {
            const norm = key.replace(/\/+$/, "")
            const next = { ...(last?.records ?? (yield* snapshotUnlocked()).records) }
            delete next[key]
            delete next[norm]
            yield* fsys.writeJson(file, next, 0o600).pipe(Effect.mapError(fail("Failed to write auth data")))
            last = { records: next, revision: Hash.fast(JSON.stringify(next, null, 2)) }
          }),
        )
      })

      let observedRevision = (yield* snapshot()).revision
      yield* Effect.forkScoped(
        Effect.gen(function* () {
          const next = yield* snapshot()
          if (next.revision === observedRevision) return
          observedRevision = next.revision
          yield* events.publish(Event.Changed, {})
        }).pipe(
          Effect.catch(() => Effect.void),
          Effect.repeat(Schedule.spaced("1 second")),
        ),
      )

      return Service.of({ get, all, snapshot, set, remove })
    }),
  )

export const defaultLayer = layer().pipe(Layer.provide(AppFileSystem.defaultLayer), Layer.provide(EventV2.defaultLayer))

export * as Auth from "."
