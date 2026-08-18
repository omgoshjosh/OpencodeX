import { ensureRunID, OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { fetchCoordinatorHealth } from "@opencode-ai/sdk/coordinator"
import { randomBytes } from "node:crypto"
import * as Log from "@opencode-ai/core/util/log"
import { Effect } from "effect"
import { ServerAuth } from "@/server/auth"
import { Server } from "@/server/server"
import { errorMessage } from "@/util/error"
import { Filesystem } from "@/util/filesystem"
import {
  acquireCoordinatorOwnerLock,
  coordinatorStartupLock,
  coordinatorDatabaseIdentity,
  coordinatorKey,
  readActiveCoordinator,
  readActiveCoordinatorClientLeases,
  removeCoordinatorManifest,
  retireCoordinatorForIdleShutdown,
  publishCoordinatorManifest,
  type TuiCoordinatorManifest,
} from "./coordinator-registry"

const FIRST_CLIENT_TIMEOUT = 60_000
const IDLE_SHUTDOWN_TIMEOUT = 5_000
const CLIENT_MONITOR_INTERVAL = 2_000

type OwnedCoordinator = {
  owned: true
  manifest: TuiCoordinatorManifest
  ownerLock: { release: () => Promise<void> }
  server: Awaited<ReturnType<typeof Server.listen>>
  reason?: string
  stopped: boolean
  retired: boolean
}

export type CoordinatorRunnerOptions = {
  directory: string
  key?: string
  startupLock?: boolean
  signal?: AbortSignal
  beforeStart?: Effect.Effect<void, unknown>
}

export const runCoordinator = Effect.fn("TuiCoordinator.run")(function* (input: CoordinatorRunnerOptions) {
  const directory = Filesystem.resolve(input.directory)
  const database = coordinatorDatabaseIdentity()
  const key = coordinatorKey(database)
  if (input.key && input.key !== key) throw new Error("TUI coordinator key does not match its database")
  const username = requiredEnv("OPENCODE_TUI_COORDINATOR_USERNAME")
  const password = requiredEnv("OPENCODE_TUI_COORDINATOR_PASSWORD")
  const token = requiredEnv("OPENCODE_TUI_COORDINATOR_TOKEN")

  process.env[OPENCODE_PROCESS_ROLE] ??= "coordinator"
  ensureRunID()
  process.env.OPENCODE_SERVER_USERNAME ??= username
  process.env.OPENCODE_SERVER_PASSWORD ??= password

  yield* Effect.try({
    try: () => process.chdir(directory),
    catch: () => new Error(`Failed to change directory to ${directory}`),
  })

  return yield* Effect.acquireUseRelease(
    startCoordinator({ ...input, directory, database, key, username, password, token }),
    (resource) => {
      if (!resource.owned) return Effect.succeed(resource.manifest)
      return waitForShutdown(resource, input.signal).pipe(
        Effect.tap((reason) =>
          Effect.sync(() => {
            resource.reason = reason
          }),
        ),
        Effect.as(resource.manifest),
      )
    },
    (resource) => {
      if (!resource.owned) return Effect.void
      return stopCoordinator(resource, resource.reason ?? "interrupted")
    },
  )
})

function startCoordinator(
  input: CoordinatorRunnerOptions & {
    directory: string
    database: string
    key: string
    username: string
    password: string
    token: string
  },
) {
  const start = Effect.fnUntraced(function* () {
    input.signal?.throwIfAborted()
    const existing = yield* Effect.promise(() => readActiveCoordinator(input.key, input.database))
    if (existing) return { owned: false as const, manifest: existing }
    const ownerLock = yield* Effect.tryPromise({
      try: () => acquireCoordinatorOwnerLock(input.key),
      catch: (error) => new Error(`Failed to acquire TUI coordinator ownership: ${errorMessage(error)}`),
    })
    const launch = Effect.gen(function* () {
      const claimed = yield* Effect.promise(() => readActiveCoordinator(input.key, input.database))
      if (claimed) {
        yield* Effect.promise(() => ownerLock.release())
        return { owned: false as const, manifest: claimed }
      }
      if (input.beforeStart) yield* input.beforeStart
      input.signal?.throwIfAborted()

      const authorityEpoch = randomBytes(32).toString("base64url")
      process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = authorityEpoch
      const server = yield* Effect.promise(() =>
        Server.listen({
          hostname: "127.0.0.1",
          port: 0,
          mdns: false,
          cors: [],
        }),
      )
      const manifest = {
        version: 2 as const,
        key: input.key,
        directory: input.directory,
        database: input.database,
        pid: process.pid,
        url: server.url.toString(),
        username: input.username,
        password: input.password,
        token: input.token,
        createdAt: new Date().toISOString(),
        /* Published so an attaching client can refuse SDK/server skew without
           having to reach the process first. Additive on purpose: the schema
           number stays at 2 so older readers keep accepting this manifest
           instead of deleting a live coordinator's claim. */
        serverVersion: InstallationVersion,
        authorityEpoch,
        admission: true,
        ready: true,
      }
      return yield* Effect.promise(async () => {
        const published = await publishCoordinatorManifest(manifest)
        if (published.state === "progressing") throw new Error(`Coordinator manifest publish ${published.reason}`)
      }).pipe(
        Effect.as({
          owned: true as const,
          manifest,
          ownerLock,
          server,
          stopped: false,
          retired: false,
          reason: undefined as string | undefined,
        }),
        Effect.onError(() => Effect.promise(() => server.stop(true)).pipe(Effect.ignore)),
      )
    })
    return yield* launch.pipe(Effect.onError(() => Effect.promise(() => ownerLock.release()).pipe(Effect.ignore)))
  })

  if (input.startupLock === false) return start()
  return Effect.scoped(
    Effect.gen(function* () {
      yield* coordinatorStartupLock(input.key)
      return yield* start()
    }),
  )
}

function waitForShutdown(resource: OwnedCoordinator, signal?: AbortSignal) {
  return Effect.callback<string>((resume) => {
    let completed = false
    const finish = (reason: string, retired = false) => {
      if (completed) return
      completed = true
      resource.retired = retired
      resume(Effect.succeed(reason))
    }
    const onSignal = () => finish("signal")
    const onAbort = () => finish("abort")
    const onBeforeExit = () => finish("beforeExit")
    const monitor = createClientMonitor(resource.manifest, finish)

    process.on("SIGINT", onSignal)
    process.on("SIGTERM", onSignal)
    process.on("SIGHUP", onSignal)
    process.on("beforeExit", onBeforeExit)
    signal?.addEventListener("abort", onAbort, { once: true })
    if (signal?.aborted) onAbort()

    return Effect.sync(() => {
      monitor.dispose()
      signal?.removeEventListener("abort", onAbort)
      process.off("SIGINT", onSignal)
      process.off("SIGTERM", onSignal)
      process.off("SIGHUP", onSignal)
      process.off("beforeExit", onBeforeExit)
    })
  })
}

function stopCoordinator(resource: OwnedCoordinator, reason: string) {
  return Effect.gen(function* () {
    if (resource.stopped) return
    resource.stopped = true
    Log.Default.info("tui coordinator stopping", { reason })
    const shutdown = yield* Effect.promise(() =>
      stopCoordinatorServices({
        dispose: () =>
          fetch(new URL("/global/dispose", resource.server.url), {
            method: "POST",
            headers: ServerAuth.headers({
              username: resource.manifest.username,
              password: resource.manifest.password,
            }),
          }).then(async (response) => {
            if (!response.ok) throw new Error(await response.text())
          }),
        stop: () => resource.server.stop(true),
        onError: (step, error) => Log.Default.warn(`tui coordinator ${step} failed`, { error: errorMessage(error) }),
      }),
    )
    if (!resource.retired) {
      yield* Effect.tryPromise(() => removeCoordinatorManifest(resource.manifest.key, resource.manifest)).pipe(
        Effect.ignore,
      )
    }
    if (shutdown.stop) {
      yield* Effect.tryPromise(() => resource.ownerLock.release()).pipe(
        Effect.catch((error) =>
          Effect.sync(() =>
            Log.Default.warn("tui coordinator owner lock release failed", { error: errorMessage(error) }),
          ),
        ),
      )
    }
  })
}

function createClientMonitor(
  manifest: TuiCoordinatorManifest,
  stop: (reason: string, retired?: boolean) => void,
) {
  let sawClient = false
  let disposed = false
  let lastClientCount = -1
  let firstClientTimer: Timer | undefined
  let shutdownTimer: Timer | undefined

  const cancelShutdown = () => {
    if (!shutdownTimer) return
    clearTimeout(shutdownTimer)
    shutdownTimer = undefined
  }

  const cancelFirstClientTimeout = () => {
    if (!firstClientTimer) return
    clearTimeout(firstClientTimer)
    firstClientTimer = undefined
  }

  const scheduleShutdown = (reason: string, timeout: number) => {
    cancelShutdown()
    shutdownTimer = setTimeout(() => {
      shutdownTimer = undefined
      void readActiveCoordinatorClientLeases(manifest.key)
        .then(async (leases) => {
          if (disposed) return
          if (leases.length === 0) {
            if (await coordinatorHasDurableActivity(manifest)) {
              sawClient = true
              return
            }
            const retired = await retireCoordinatorForIdleShutdown(manifest.key, manifest)
            if (retired.state === "progressing") return
            stop(reason, true)
            return
          }
          sawClient = true
          cancelFirstClientTimeout()
          cancelShutdown()
          if (lastClientCount === leases.length) return
          Log.Default.info("tui coordinator clients active", { clients: leases.length })
          lastClientCount = leases.length
        })
        .catch((error) => {
          Log.Default.warn("tui coordinator client monitor failed", { error: errorMessage(error) })
        })
    }, timeout)
    shutdownTimer.unref?.()
  }

  const check = async () => {
    if (disposed) return
    const leases = await readActiveCoordinatorClientLeases(manifest.key)
    if (leases.length > 0) {
      sawClient = true
      cancelFirstClientTimeout()
      cancelShutdown()
      if (lastClientCount !== leases.length) {
        Log.Default.info("tui coordinator clients active", { clients: leases.length })
        lastClientCount = leases.length
      }
      return
    }
    if (lastClientCount !== 0) {
      Log.Default.info("tui coordinator clients active", { clients: 0 })
      lastClientCount = 0
    }
    if (!sawClient || shutdownTimer) return
    scheduleShutdown("all coordinator clients closed", IDLE_SHUTDOWN_TIMEOUT)
  }

  firstClientTimer = setTimeout(() => {
    firstClientTimer = undefined
    void coordinatorHasDurableActivity(manifest).then((active) => {
      if (disposed) return
      if (!active) {
        void retireCoordinatorForIdleShutdown(manifest.key, manifest).then((retired) => {
          if (retired.state === "committed") stop("no coordinator clients connected", true)
        })
        return
      }
      sawClient = true
    })
  }, FIRST_CLIENT_TIMEOUT)
  firstClientTimer.unref?.()
  const interval = setInterval(() => {
    void check().catch((error) => {
      Log.Default.warn("tui coordinator client monitor failed", { error: errorMessage(error) })
    })
  }, CLIENT_MONITOR_INTERVAL)
  interval.unref?.()
  void check()

  return {
    dispose() {
      if (disposed) return
      disposed = true
      cancelFirstClientTimeout()
      cancelShutdown()
      clearInterval(interval)
    },
  }
}

async function coordinatorHasDurableActivity(manifest: TuiCoordinatorManifest) {
  const health = await fetchCoordinatorHealth(manifest, { timeout: 5_000 })
  if (health) return health.active === true
  /* An unreachable coordinator is not evidence of idleness, and shutting down
     on a failed probe would drop work the server may still be doing. */
  Log.Default.warn("tui coordinator activity check failed")
  return true
}

export async function stopCoordinatorServices(input: {
  dispose: () => Promise<unknown>
  stop: () => Promise<unknown>
  timeout?: number
  onError?: (step: "dispose" | "server stop", error: unknown) => void
}) {
  const result = { dispose: false, stop: false }
  for (const [step, action] of [["dispose", input.dispose], ["server stop", input.stop]] as const) {
    await boundedCoordinatorShutdownStep(action, input.timeout ?? 5_000).then(
      () => {
        result[step === "dispose" ? "dispose" : "stop"] = true
      },
      (error) => input.onError?.(step, error),
    )
  }
  return result
}

async function boundedCoordinatorShutdownStep(action: () => Promise<unknown>, timeout: number) {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      action(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Coordinator shutdown step timed out")), timeout)
        timer.unref?.()
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function requiredEnv(name: string) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}
