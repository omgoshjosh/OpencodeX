import path from "path"
import { open, rename, rm, mkdir } from "node:fs/promises"
import { randomUUID } from "node:crypto"
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

export interface LayerTestHooks {
  afterLock?: () => Promise<void>
  afterRead?: () => Promise<void>
  afterTempOpen?: (temporary: string) => Promise<void>
}

export const layer = (file = path.join(Global.Path.data, "auth.json"), hooks?: LayerTestHooks) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const fsys = yield* AppFileSystem.Service
      const events = yield* EventV2.Service
      const decode = Schema.decodeUnknownOption(Info)
      const lock = Semaphore.makeUnsafe(1)
      let last: Snapshot | undefined

      const parse = (content: string) => {
        const raw = JSON.parse(content)
        if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("Invalid auth snapshot")
        const records = Record.filterMap(raw as Record<string, unknown>, (value) =>
          Result.fromOption(decode(value), () => undefined),
        )
        if (Object.keys(records).length !== Object.keys(raw).length) throw new Error("Invalid auth snapshot")
        return { records, revision: Hash.fast(content) }
      }

      const snapshotFromFile = Effect.fnUntraced(function* () {
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

      const snapshotUnlocked = Effect.fnUntraced(function* () {
        if (process.env.OPENCODE_AUTH_CONTENT) {
          try {
            const next = parse(process.env.OPENCODE_AUTH_CONTENT)
            last = next
            return next
          } catch {}
        }
        return yield* snapshotFromFile()
      })

      const snapshot = Effect.fn("Auth.snapshot")(() => lock.withPermits(1)(snapshotUnlocked()))

      const mutate = <A>(fn: (records: Record<string, Info>) => Record<string, Info>) =>
        lock.withPermits(1)(
          Effect.gen(function* () {
            const lockFile = `${file}.lock`
            const handle = yield* Effect.tryPromise({
              try: async () => {
                const deadline = Date.now() + 2_000
                while (true) {
                  try {
                    return await open(lockFile, "wx", 0o600)
                  } catch (error) {
                    if ((error as NodeJS.ErrnoException).code !== "EEXIST" || Date.now() >= deadline) {
                      throw new Error("Timed out acquiring auth mutation lock")
                    }
                    await Bun.sleep(25)
                  }
                }
              },
              catch: fail("Failed to acquire auth mutation lock"),
            })
            return yield* Effect.gen(function* () {
              // Cooperating writers serialize through this lock; arbitrary external renames remain last-writer-wins.
              if (hooks?.afterLock) yield* Effect.promise(hooks.afterLock)
              const next = fn((yield* snapshotFromFile()).records)
              if (hooks?.afterRead) yield* Effect.promise(hooks.afterRead)
              const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`
              yield* Effect.tryPromise({
                try: async () => {
                  await mkdir(path.dirname(file), { recursive: true })
                  const output = await open(temporary, "wx", 0o600)
                  try {
                    if (hooks?.afterTempOpen) await hooks.afterTempOpen(temporary)
                    await output.writeFile(JSON.stringify(next, null, 2))
                    await output.sync()
                  } finally {
                    await output.close()
                  }
                  await rename(temporary, file)
                },
                catch: fail("Failed to write auth data"),
              }).pipe(Effect.ensuring(Effect.promise(() => rm(temporary, { force: true }))))
              last = { records: next, revision: Hash.fast(JSON.stringify(next, null, 2)) }
            }).pipe(
              Effect.ensuring(
                Effect.promise(async () => {
                  await handle.close()
                  await rm(lockFile, { force: true })
                }),
              ),
            )
          }),
        )

      const all = Effect.fn("Auth.all")(function* () {
        return (yield* snapshot()).records
      })

      const get = Effect.fn("Auth.get")(function* (providerID: string) {
        return (yield* all())[providerID]
      })

      const set = Effect.fn("Auth.set")(function* (key: string, info: Info) {
        yield* mutate((data) => {
          const norm = key.replace(/\/+$/, "")
          const next = { ...data, [norm]: info }
          if (norm !== key) delete next[key]
          delete next[norm + "/"]
          return next
        })
      })

      const remove = Effect.fn("Auth.remove")(function* (key: string) {
        yield* mutate((data) => {
          const norm = key.replace(/\/+$/, "")
          const next = { ...data }
          delete next[key]
          delete next[norm]
          return next
        })
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
