import { spawn, type ChildProcess } from "node:child_process"
import { randomBytes } from "node:crypto"
import { rememberBackendAuthority } from "./backend-authority.js"
import fs from "node:fs"
import {
  checkCoordinatorCompatibility,
  isCoordinatorProcessAlive,
  observeCoordinatorAuthority,
  removeCoordinatorManifest as removeCoordinatorManifestIn,
  startCoordinatorClientLease as startCoordinatorClientLeaseIn,
} from "@opencode-ai/sdk/coordinator"
import {
  COORDINATOR_STATE_ROOT,
  COORDINATOR_USERNAME,
  coordinatorDatabaseIdentity,
  coordinatorKey,
  coordinatorStartupLogPath,
  createSidecarLaunch,
  createStartupLog,
  selectedDatabaseEnv,
  sidecarDatabase,
  sidecarVersion,
  startError,
  startupLogDetails,
  workingDirectory,
  type CoordinatorManifest,
  type SidecarLaunch,
} from "./sidecar-launch.js"
import { stopOwnedCoordinatorUnderAuthority } from "./sidecar-lifecycle.js"
import { sidecarChildEnvironment, writeSidecarBootstrap } from "./sidecar-bootstrap.js"

export type SidecarConnection = {
  url: string
  username: string
  password: string
  directory: string
  key: string
  database: string
  token: string
  authorityEpoch?: string
}

type SidecarState = {
  child?: { process: ChildProcess; key: string; token: string }
  connection?: SidecarConnection
  startup?: Promise<SidecarConnection>
  lease?: { dispose: () => Promise<void> }
  controller?: AbortController
  generation: number
}

/**
 * A coordinator whose server version this GUI cannot verify is refused, never
 * replaced: the manifest stays, the process keeps running, and any client that
 * does match it stays attached. Killing it would put a second writer on the
 * same database, which is the failure the whole attach protocol prevents.
 */
class CoordinatorVersionMismatchError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "CoordinatorVersionMismatchError"
  }
}

/* Generous on purpose: the coordinator is `bun run` over the full opencode
   source graph, and a dev machine under load (builds, test suites) can push a
   cold start well past 15s. The wait loop still returns the moment the
   manifest appears, so the ceiling only matters on slow starts. */
const START_TIMEOUT = 45_000
const CLIENT_HEARTBEAT_INTERVAL = 2_000
const state: SidecarState = { generation: 0 }

export function startSidecar(signal?: AbortSignal) {
  if (signal?.aborted) return Promise.reject(startupStoppedError())
  if (state.connection) return Promise.resolve(state.connection)
  if (state.startup) return state.startup

  const generation = state.generation
  const controller = new AbortController()
  const abort = () => controller.abort()
  signal?.addEventListener("abort", abort, { once: true })
  const directory = workingDirectory()
  const startup = coordinatorConnection(directory, controller.signal)
    .then(async (manifest) => {
      if (generation !== state.generation || controller.signal.aborted) throw startupStoppedError()
      const lease = startCoordinatorClientLease(manifest.key)
      try {
        await lease.ready
      } catch (error) {
        await lease.dispose()
        throw error
      }
      if (generation !== state.generation || controller.signal.aborted) {
        await lease.dispose()
        throw startupStoppedError()
      }
      if (state.lease) await state.lease.dispose()
      if (generation !== state.generation || controller.signal.aborted) {
        await lease.dispose()
        throw startupStoppedError()
      }
      state.lease = lease
      if (state.child?.process.pid === manifest.pid && process.env.OPENCODEX_GUI_SMOKE !== "1") state.child = undefined
      const connection = connectionFromManifest(manifest, directory)
      state.connection = connection
      return connection
    })
    .finally(() => {
      signal?.removeEventListener("abort", abort)
      if (state.startup !== startup) return
      state.startup = undefined
      state.controller = undefined
    })

  state.controller = controller
  state.startup = startup
  return startup
}

export async function stopSidecar() {
  state.generation += 1
  const startup = state.startup
  const lease = state.lease
  const child = state.child
  state.controller?.abort()
  state.controller = undefined
  state.lease = undefined
  state.child = undefined
  state.connection = undefined
  state.startup = undefined
  await Promise.all([
    lease?.dispose(),
    child ? stopOwnedCoordinator(child) : undefined,
    startup?.catch(() => undefined),
  ])
}

async function coordinatorConnection(directory: string, signal: AbortSignal) {
  const database = await sidecarDatabase(directory)
  await rememberBackendAuthority(database)
  const key = coordinatorKey(database)
  throwIfStartupStopped(signal)
  const existing = await activeCoordinator(key, database)
  throwIfStartupStopped(signal)
  if (existing) return existing
  throwIfStartupStopped(signal)
  return spawnCoordinator(directory, key, database, signal)
}

function connectionFromManifest(manifest: CoordinatorManifest, directory: string) {
  return {
    url: manifest.url,
    username: manifest.username,
    password: manifest.password,
    directory,
    key: manifest.key,
    database: manifest.database,
    token: manifest.token,
    authorityEpoch: manifest.authorityEpoch,
  }
}

export async function isSidecarConnectionActive(connection: SidecarConnection) {
  const manifest = await activeCoordinator(connection.key, connection.database)
  return manifest?.token === connection.token
}

async function activeCoordinator(key: string, database: string) {
  const observation = await observeCoordinatorAuthority({ stateRoot: COORDINATOR_STATE_ROOT, key })
  if (observation.state === "progressing") throw coordinatorAuthorityError("progressing")
  const manifest = observation.manifest
  if (!manifest) {
    if (observation.authority.state === "absent") return undefined
    throw coordinatorAuthorityError(observation.authority)
  }
  if (
    manifest.key !== key ||
    coordinatorDatabaseIdentity(manifest.database) !== coordinatorDatabaseIdentity(database)
  ) {
    if (observation.handoff !== undefined) throw coordinatorAuthorityError(observation.authority)
    const removed = await removeCoordinatorManifest(key, manifest)
    if (removed.state === "progressing") throw coordinatorAuthorityError("progressing")
    return undefined
  }
  const health = observation.health
  const authority = observation.authority
  if (authority.state === "active" || authority.state === "handoff") {
    const compatibility = checkCoordinatorCompatibility({
      manifest,
      clientVersion: sidecarVersion(),
      healthVersion: health?.version,
    })
    if (!compatibility.compatible) {
      throw new CoordinatorVersionMismatchError(compatibility.message ?? "Coordinator version mismatch")
    }
    if (compatibility.reason === "local" && compatibility.message) console.warn(compatibility.message)
    return manifest
  }
  if (observation.handoff !== undefined || authority.state !== "blocked" || authority.reason !== "unhealthy") {
    throw coordinatorAuthorityError(authority)
  }
  if (isCoordinatorProcessAlive(manifest.pid)) {
    throw new Error(`OpencodeX coordinator process ${manifest.pid} is alive but unhealthy; refusing to replace it`)
  }
  const removed = await removeCoordinatorManifest(key, manifest)
  if (removed.state === "progressing") throw coordinatorAuthorityError("progressing")
  return undefined
}

function coordinatorAuthorityError(authority: { state: string; reason?: string } | "progressing") {
  const reason =
    authority === "progressing" ? authority : authority.state === "blocked" ? authority.reason : authority.state
  return new Error(`OpencodeX coordinator authority is ${reason}; refusing election or replacement`)
}

function removeCoordinatorManifest(key: string, manifest: CoordinatorManifest) {
  return removeCoordinatorManifestIn(COORDINATOR_STATE_ROOT, key, manifest)
}

function startCoordinatorClientLease(key: string) {
  /* The `.gui` suffix is load-bearing: the TUI scans for `*.gui.json` leases to
     decide which database an active GUI already owns. */
  return startCoordinatorClientLeaseIn({
    stateRoot: COORDINATOR_STATE_ROOT,
    key,
    suffix: ".gui",
    interval: CLIENT_HEARTBEAT_INTERVAL,
  })
}

async function spawnCoordinator(directory: string, key: string, database: string, signal: AbortSignal) {
  throwIfStartupStopped(signal)
  const password = randomBytes(32).toString("base64url")
  const token = randomBytes(32).toString("base64url")
  const started = { ...createSidecarLaunch(directory, key, database), startupLog: coordinatorStartupLogPath(key) }
  const startupLog = createStartupLog(started)
  const child = await (async () => {
    try {
      const spawned = spawn(started.command, started.args, {
        cwd: started.cwd,
        /* Detached everywhere, including Windows: a non-detached child shares
           the dev terminal's console process group, so a Ctrl-C aimed at the
           GUI dev process also signals the coordinator - which then shuts down
           gracefully even while an attached TUI still holds a client lease.
           `windowsHide` keeps the detached child from opening a console
           window. */
        detached: true,
        stdio: ["ignore", startupLog, startupLog, "pipe"],
        env: sidecarChildEnvironment(process.env, {
          ...selectedDatabaseEnv(started.database),
          OPENCODE_CLI_NAME: "opencodex",
          OPENCODE_COORDINATOR_BOOTSTRAP_FD: "3",
        }),
        windowsHide: true,
      })
      fs.closeSync(startupLog)
      await writeSidecarBootstrap(spawned, { version: 1, username: COORDINATOR_USERNAME, password, token })
      return spawned
    } catch (error) {
      try {
        fs.closeSync(startupLog)
      } catch {}
      throw startError(error, started)
    }
  })()
  child.unref()
  const owned = { process: child, key, token }
  state.child = owned
  try {
    return await waitForCoordinator(directory, child, started, signal)
  } catch (error) {
    await stopOwnedCoordinator(owned)
    if (state.child === owned) state.child = undefined
    throw error
  }
}

async function waitForCoordinator(directory: string, child: ChildProcess, started: SidecarLaunch, signal: AbortSignal) {
  const startedAt = Date.now()
  let failure: Error | undefined
  child.once("error", (error) => {
    failure = startError(error, started)
  })
  child.once("exit", (code, signal) => {
    failure = new Error(
      `OpencodeX coordinator exited before startup (${signal ?? code ?? "unknown"})${startupLogDetails(started)}`,
    )
  })
  while (Date.now() - startedAt < START_TIMEOUT) {
    throwIfStartupStopped(signal)
    const manifest = await activeCoordinator(coordinatorKey(started.database), started.database)
    throwIfStartupStopped(signal)
    if (manifest) return manifest
    if (failure) throw failure
    await startupDelay(signal)
  }
  throw new Error(`Timed out waiting for OpencodeX coordinator to start${startupLogDetails(started)}`)
}

function throwIfStartupStopped(signal: AbortSignal) {
  if (signal.aborted) throw startupStoppedError()
}

function startupStoppedError() {
  const error = new Error("Sidecar startup was stopped")
  error.name = "AbortError"
  return error
}

function startupDelay(signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(done, 150)
    const abort = () => {
      clearTimeout(timer)
      signal.removeEventListener("abort", abort)
      reject(startupStoppedError())
    }
    function done() {
      signal.removeEventListener("abort", abort)
      resolve()
    }
    signal.addEventListener("abort", abort, { once: true })
    if (signal.aborted) abort()
  })
}

async function stopOwnedCoordinator(owned: NonNullable<SidecarState["child"]>) {
  await stopOwnedCoordinatorUnderAuthority({
    stateRoot: COORDINATOR_STATE_ROOT,
    key: owned.key,
    token: owned.token,
    child: owned.process,
  })
}
