import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import * as Log from "@opencode-ai/core/util/log"
import { Flock } from "@opencode-ai/core/util/flock"
import { LockProtocol } from "@opencode-ai/core/util/lock-protocol"
import { ensureRunID, OPENCODE_PROCESS_ROLE, OPENCODE_RUN_ID } from "@opencode-ai/core/util/opencode-process"
import {
  checkCoordinatorCompatibility,
  checkCoordinatorHandoffTransition,
  coordinatorClientDir as coordinatorClientDirIn,
  coordinatorDatabaseIdentity as coordinatorDatabaseIdentityOf,
  coordinatorHandoffPath,
  coordinatorHeaders as coordinatorHeadersFor,
  coordinatorKey as coordinatorKeyOf,
  coordinatorManifestPath as coordinatorManifestPathIn,
  coordinatorRoot,
  isCoordinatorClientLease,
  isCoordinatorHandoffRecord,
  isCoordinatorKey,
  isCoordinatorProcessAlive,
  isMissingCoordinatorFile,
  observeCoordinatorAuthority,
  readCoordinatorManifestFile,
  readCoordinatorHandoff,
  removeCoordinatorManifest as removeCoordinatorManifestIn,
  withCoordinatorAuthorityLock,
  startCoordinatorClientLease as startCoordinatorClientLeaseIn,
  publishCoordinatorManifest as publishCoordinatorManifestIn,
  type CoordinatorClientLease,
  type CoordinatorHandoffRecord,
  type CoordinatorManifest,
} from "@opencode-ai/sdk/coordinator"
import { errorMessage } from "@/util/error"
import { randomBytes } from "crypto"
import { spawn } from "child_process"
import fs from "fs/promises"
import path from "path"
import { discoverBackendDatabase } from "./database-discovery"

export type TuiCoordinatorManifest = CoordinatorManifest
export type TuiCoordinatorClientLease = CoordinatorClientLease
export type TuiCoordinatorHandoffRecord = CoordinatorHandoffRecord
export type TuiCoordinatorHandoffMatch = Pick<
  TuiCoordinatorHandoffRecord,
  "request" | "phase" | "revision" | "sourceEpoch" | "targetEpoch"
>

const STATE_ROOT = Global.Path.state
const ROOT = coordinatorRoot(STATE_ROOT)
const BACKEND_AUTHORITY = path.join(STATE_ROOT, "backend-authority.json")
const USERNAME = "opencodex-local"
const START_TIMEOUT = 15_000
const CLIENT_STALE_MS = 10_000

export const COORDINATOR_STARTUP_LOCK_HELD = "OPENCODE_TUI_COORDINATOR_STARTUP_LOCK_HELD"

export function coordinatorDatabaseIdentity(database = Database.path()) {
  return coordinatorDatabaseIdentityOf(database)
}

export function coordinatorKey(database = coordinatorDatabaseIdentity()) {
  return coordinatorKeyOf(database)
}

export function coordinatorManifestPath(key: string) {
  return coordinatorManifestPathIn(STATE_ROOT, key)
}

export function coordinatorClientDir(key: string) {
  return coordinatorClientDirIn(STATE_ROOT, key)
}

export function coordinatorHeaders(manifest: TuiCoordinatorManifest) {
  return coordinatorHeadersFor(manifest)
}

export function publishCoordinatorManifest(manifest: TuiCoordinatorManifest) {
  return publishCoordinatorManifestIn(STATE_ROOT, manifest, undefined)
}

export async function removeCoordinatorManifest(key: string, manifest: TuiCoordinatorManifest) {
  return removeCoordinatorManifestIn(STATE_ROOT, key, manifest)
}

export function startCoordinatorClientLease(key: string) {
  const lease = startCoordinatorClientLeaseIn({ stateRoot: STATE_ROOT, key })
  return {
    ready: lease.ready,
    dispose() {
      void lease.dispose().catch(() => {})
    },
  }
}

export function retireCoordinatorForIdleShutdown(
  key: string,
  manifest: TuiCoordinatorManifest,
  stateRoot = STATE_ROOT,
) {
  return removeCoordinatorManifestIn(stateRoot, key, manifest)
}

export async function readActiveCoordinatorClientLeases(key: string) {
  const dir = coordinatorClientDir(key)
  const files = await fs.readdir(dir).catch(() => [])
  const leases = await Promise.all(
    files
      .filter((file) => file.endsWith(".json"))
      .map(async (name) => {
        const file = path.join(dir, name)
        const lease = await readClientLease(file).catch(() => undefined)
        const recovered = lease ?? (await recoverReplacingClientLease(file, name, key))
        const active =
          recovered !== undefined &&
          recovered.key === key &&
          Date.now() - recovered.updatedAt <= CLIENT_STALE_MS &&
          isCoordinatorProcessAlive(recovered.pid)
        if (active) return recovered
        await fs.rm(file, { force: true }).catch(() => {})
        return undefined
      }),
  )
  return leases.filter((lease): lease is TuiCoordinatorClientLease => lease !== undefined)
}

async function recoverReplacingClientLease(file: string, name: string, key: string) {
  const pid = Number(name.split(".")[0])
  if (!Number.isInteger(pid) || pid <= 0 || !isCoordinatorProcessAlive(pid)) return undefined
  const stat = await fs.stat(file).catch(() => undefined)
  if (!stat || Date.now() - stat.mtimeMs > CLIENT_STALE_MS) return undefined
  return {
    version: 1,
    key,
    pid,
    updatedAt: stat.mtimeMs,
  } satisfies TuiCoordinatorClientLease
}

async function readClientLease(file: string) {
  const parsed = JSON.parse(await fs.readFile(file, "utf8")) as unknown
  if (!isCoordinatorClientLease(parsed)) throw new Error("Invalid TUI coordinator client lease")
  return parsed
}

/**
 * A live coordinator whose server version this client cannot verify is left
 * strictly alone: no manifest deletion, no process kill. Another client may be
 * attached and happy with it, and replacing it would put two writers on the
 * same database.
 */
export class CoordinatorVersionMismatchError extends Error {
  constructor(
    readonly manifest: TuiCoordinatorManifest,
    message: string,
  ) {
    super(message)
    this.name = "CoordinatorVersionMismatchError"
  }
}

export async function readActiveCoordinator(
  key = coordinatorKey(),
  database = coordinatorDatabaseIdentity(),
  stateRoot = STATE_ROOT,
) {
  const observation = await observeCoordinatorAuthority({ stateRoot, key }).catch((error) => {
    throw new Error("Invalid TUI coordinator manifest; refusing to replace it", { cause: error })
  })
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
    const removed = await removeCoordinatorManifestIn(stateRoot, key, manifest)
    if (removed.state === "progressing") throw coordinatorAuthorityError("progressing")
    return undefined
  }
  const health = observation.health
  const authority = observation.authority
  if (authority.state === "active" || authority.state === "handoff") {
    const compatibility = checkCoordinatorCompatibility({
      manifest,
      clientVersion: InstallationVersion,
      healthVersion: health?.version,
    })
    if (!compatibility.compatible) {
      throw new CoordinatorVersionMismatchError(manifest, compatibility.message ?? "Coordinator version mismatch")
    }
    if (compatibility.reason === "local" && compatibility.message) {
      Log.Default.warn("tui coordinator version unverified", { detail: compatibility.message })
    }
    return manifest
  }
  if (observation.handoff !== undefined || authority.state !== "blocked" || authority.reason !== "unhealthy") {
    throw coordinatorAuthorityError(authority)
  }
  if (isCoordinatorProcessAlive(manifest.pid)) {
    throw new Error(`TUI coordinator process ${manifest.pid} is alive but unhealthy; refusing to replace it`)
  }
  const removed = await removeCoordinatorManifestIn(stateRoot, key, manifest)
  if (removed.state === "progressing") throw coordinatorAuthorityError("progressing")
  return undefined
}

function coordinatorAuthorityError(authority: { state: string; reason?: string } | "progressing") {
  const reason =
    authority === "progressing" ? authority : authority.state === "blocked" ? authority.reason : authority.state
  return new Error(`TUI coordinator authority is ${reason}; refusing election or replacement`)
}

export async function readPreferredCoordinator() {
  const database = await preferredCoordinatorDatabase()
  return readActiveCoordinator(coordinatorKey(database), database)
}

export async function readActiveManifest(key: string, stateRoot = STATE_ROOT) {
  const file = coordinatorManifestPathIn(stateRoot, key)
  try {
    return await readCoordinatorManifestFile(file)
  } catch (error) {
    if (isMissingCoordinatorFile(error)) return undefined
    throw new Error("Invalid TUI coordinator manifest; refusing to replace it", { cause: error })
  }
}

/**
 * Longer than `START_TIMEOUT` on purpose: a coordinator whose heartbeat has
 * merely gone quiet may still be running and serving the database, and stealing
 * its lock would start a second writer. A coordinator that has actually died is
 * reclaimed immediately by pid — see `LockProtocol.ownerGone` — so a contender
 * timing out before this window elapses is the correct, conservative outcome
 * rather than a stuck launch.
 */
const LOCK_STALE_TIMEOUT = 30_000

export function withCoordinatorStartupLock<T>(key: string, fn: () => Promise<T>) {
  return Flock.withLock(`tui-coordinator:${key}`, fn, { timeoutMs: START_TIMEOUT, staleMs: LOCK_STALE_TIMEOUT })
}

export function coordinatorStartupLock(key: string) {
  return Flock.effect(`tui-coordinator:${key}`, { timeoutMs: START_TIMEOUT, staleMs: LOCK_STALE_TIMEOUT })
}

/**
 * Atomically changes handoff state only when its full authority fence is
 * exactly expected. Malformed and legacy state is never treated as absence, so
 * it cannot be overwritten or deleted through this API.
 */
export function compareAndSwapCoordinatorHandoff(
  key: string,
  expected: TuiCoordinatorHandoffMatch | undefined,
  replacement: TuiCoordinatorHandoffRecord | undefined,
  stateRoot = STATE_ROOT,
) {
  if (!isCoordinatorKey(key)) throw new Error("Invalid coordinator key")
  return withCoordinatorAuthorityLock(stateRoot, key, async () => {
    const current = await readCoordinatorHandoff(stateRoot, key).catch((error) => {
      if (isMissingCoordinatorFile(error)) return undefined
      throw error
    })
    const matches = expected
      ? isCoordinatorHandoffRecord(current) &&
        current.request === expected.request &&
        current.phase === expected.phase &&
        current.sourceEpoch === expected.sourceEpoch &&
        current.revision === expected.revision &&
        current.targetEpoch === expected.targetEpoch
      : current === undefined
    if (!matches) return false

    const file = coordinatorHandoffPath(stateRoot, key)
    if (!replacement) {
      if (!isCoordinatorHandoffRecord(current) || (current.phase !== "requested" && current.phase !== "accepted"))
        return false
      const manifest = await readCoordinatorManifestFile(coordinatorManifestPathIn(stateRoot, key)).catch(
        () => undefined,
      )
      if (!manifest || manifest.authorityEpoch !== current.sourceEpoch || manifest.pid !== process.pid) return false
      await fs.rm(file)
      return true
    }
    if (!isCoordinatorHandoffRecord(replacement)) throw new Error("Invalid replacement coordinator handoff record")
    if (!current) {
      const manifest = await readCoordinatorManifestFile(coordinatorManifestPathIn(stateRoot, key)).catch(
        () => undefined,
      )
      if (!manifest || manifest.authorityEpoch !== replacement.sourceEpoch || manifest.pid !== process.pid) return false
    }
    if (!checkCoordinatorHandoffTransition(isCoordinatorHandoffRecord(current) ? current : undefined, replacement)) {
      throw new Error("Illegal coordinator handoff transition")
    }

    await fs.mkdir(path.dirname(file), { recursive: true })
    const temporary = `${file}.${process.pid}.${randomBytes(8).toString("hex")}.tmp`
    try {
      await fs.writeFile(
        temporary,
        JSON.stringify(
          {
            version: replacement.version,
            request: replacement.request,
            phase: replacement.phase,
            revision: replacement.revision,
            sourceEpoch: replacement.sourceEpoch,
            ...(replacement.targetEpoch === undefined ? {} : { targetEpoch: replacement.targetEpoch }),
            createdAt: replacement.createdAt,
            updatedAt: replacement.updatedAt,
          },
          null,
          2,
        ),
        { mode: 0o600 },
      )
      await fs.rename(temporary, file)
    } finally {
      await fs.rm(temporary, { force: true }).catch(() => {})
    }
    return true
  })
}

export function inspectCoordinatorHandoff(key: string, stateRoot = STATE_ROOT) {
  if (!isCoordinatorKey(key)) throw new Error("Invalid coordinator key")
  return withCoordinatorAuthorityLock(stateRoot, key, async () => {
    const handoff = await readCoordinatorHandoff(stateRoot, key).catch((error) => {
      if (isMissingCoordinatorFile(error)) return undefined
      throw error
    })
    if (handoff !== undefined && !isCoordinatorHandoffRecord(handoff)) throw new Error("Invalid coordinator handoff")
    return handoff
  })
}

export function proveCoordinatorSourceWithoutHandoff(key: string, sourceEpoch: string, stateRoot = STATE_ROOT) {
  if (!isCoordinatorKey(key)) throw new Error("Invalid coordinator key")
  return withCoordinatorAuthorityLock(stateRoot, key, async () => {
    const handoff = await readCoordinatorHandoff(stateRoot, key).catch((error) => {
      if (isMissingCoordinatorFile(error)) return undefined
      throw error
    })
    if (handoff !== undefined) return false
    const manifest = await readCoordinatorManifestFile(coordinatorManifestPathIn(stateRoot, key)).catch(() => undefined)
    return manifest?.authorityEpoch === sourceEpoch && manifest.pid === process.pid
  })
}

export function acquireCoordinatorOwnerLock(key: string) {
  return Flock.acquire(`tui-coordinator-owner:${key}`, { timeoutMs: START_TIMEOUT, staleMs: LOCK_STALE_TIMEOUT })
}

function cliCommand() {
  if (process.argv[1]?.endsWith(".ts")) return [process.execPath, "--conditions=browser", process.argv[1]]
  return [process.execPath]
}

function createSecret() {
  return randomBytes(32).toString("base64url")
}

function spawnCoordinator(directory: string, key: string, database: string) {
  const password = createSecret()
  const token = createSecret()
  const command = cliCommand()
  const child = spawn(command[0], [...command.slice(1), "internal-tui-coordinator", directory, "--key", key], {
    cwd: directory,
    /* Detached everywhere, including Windows: a non-detached child shares the
       spawning terminal's console process group, so a Ctrl-C meant for the TUI
       or GUI dev process also signals the coordinator - which then shuts down
       gracefully even while other clients still hold leases. `windowsHide`
       keeps the detached child from opening a console window. */
    detached: true,
    stdio: "ignore",
    windowsHide: true,
    env: {
      ...process.env,
      [OPENCODE_PROCESS_ROLE]: "coordinator",
      [OPENCODE_RUN_ID]: ensureRunID(),
      [COORDINATOR_STARTUP_LOCK_HELD]: "1",
      OPENCODE_TUI_COORDINATOR_USERNAME: USERNAME,
      OPENCODE_TUI_COORDINATOR_PASSWORD: password,
      OPENCODE_TUI_COORDINATOR_TOKEN: token,
      OPENCODE_SERVER_USERNAME: USERNAME,
      OPENCODE_SERVER_PASSWORD: password,
      OPENCODE_DB: database,
    },
  })
  child.unref()
}

async function waitForCoordinator(key: string, database: string) {
  const started = Date.now()
  let lastError = "coordinator did not publish a manifest"
  while (Date.now() - started < START_TIMEOUT) {
    const manifest = await readActiveCoordinator(key, database).catch((error) => {
      if (error instanceof CoordinatorVersionMismatchError) throw error
      lastError = errorMessage(error)
      return undefined
    })
    if (manifest) return manifest
    await new Promise((resolve) => setTimeout(resolve, 150))
  }
  throw new Error(`Timed out waiting for TUI coordinator: ${lastError}`)
}

export async function resolveLocalCoordinator(directory: string) {
  const database = await preferredCoordinatorDatabase()
  const key = coordinatorKey(database)
  return await withCoordinatorStartupLock(key, async () => {
    const existing = await readActiveCoordinator(key, database)
    if (existing) return existing
    spawnCoordinator(directory, key, database)
    return await waitForCoordinator(key, database)
  })
}

export async function readBackendAuthority(file = BACKEND_AUTHORITY) {
  return fs
    .readFile(file, "utf8")
    .then((raw) => JSON.parse(raw) as Partial<{ version: number; database: string; updatedAt: number }>)
    .then(async (selection) => {
      if (
        selection.version !== 1 ||
        typeof selection.database !== "string" ||
        typeof selection.updatedAt !== "number" ||
        selection.database === ":memory:"
      )
        return undefined
      const database = coordinatorDatabaseIdentity(selection.database)
      return (await fs.stat(database)).isFile() ? database : undefined
    })
    .catch(() => undefined)
}

async function preferredCoordinatorDatabase() {
  const fallback = coordinatorDatabaseIdentity()
  if (process.env.OPENCODE_DB) return fallback
  const persisted = await readBackendAuthority()
  const active = await discoverActiveGuiCoordinatorDatabase()
  const discovered =
    active || persisted ? undefined : coordinatorDatabaseIdentity((await discoverBackendDatabase()) ?? fallback)
  const database = selectBackendAuthority(active, persisted, discovered, fallback)
  if (database !== fallback && database !== persisted) await rememberBackendAuthority(database).catch(() => undefined)
  return database
}

export function selectBackendAuthority(
  active: string | undefined,
  persisted: string | undefined,
  discovered: string | undefined,
  fallback: string,
) {
  return active ?? persisted ?? discovered ?? fallback
}

export async function discoverActiveGuiCoordinatorDatabase(root = ROOT) {
  const manifests = await Promise.all(
    (await fs.readdir(root).catch(() => []))
      .filter((file) => file.endsWith(".json"))
      .map((file) => readCoordinatorManifestFile(path.join(root, file)).catch(() => undefined)),
  )
  const databases = await Promise.all(
    manifests.flatMap((manifest) =>
      manifest
        ? [
            hasActiveGuiClient(manifest.key, root).then(async (active) => {
              if (!active) return undefined
              const observation = await observeCoordinatorAuthority({
                stateRoot: path.dirname(root),
                key: manifest.key,
              })
              if (observation.state === "progressing") return coordinatorDatabaseIdentity(manifest.database)
              if (observation.authority.state === "absent") return undefined
              if (observation.authority.state === "blocked" && observation.handoff === undefined) return undefined
              return coordinatorDatabaseIdentity(manifest.database)
            }),
          ]
        : [],
    ),
  )
  const active = [...new Set(databases.filter((database): database is string => database !== undefined))]
  return active.length === 1 ? active[0] : undefined
}

async function hasActiveGuiClient(key: string, root: string) {
  const dir = path.join(root, `${key}.clients`)
  const clients = await Promise.all(
    (await fs.readdir(dir).catch(() => []))
      .filter((file) => file.endsWith(".gui.json"))
      .map(async (name) => {
        const file = path.join(dir, name)
        const lease = await readClientLease(file).catch(() => undefined)
        const active =
          lease !== undefined &&
          lease.key === key &&
          Date.now() - lease.updatedAt <= CLIENT_STALE_MS &&
          isCoordinatorProcessAlive(lease.pid)
        if (!active) await fs.rm(file, { force: true }).catch(() => {})
        return active
      }),
  )
  return clients.some(Boolean)
}

async function rememberBackendAuthority(database: string, file = BACKEND_AUTHORITY) {
  await fs.mkdir(path.dirname(file), { recursive: true })
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`
  await fs.writeFile(tmp, JSON.stringify({ version: 1, database, updatedAt: Date.now() }, null, 2), { mode: 0o600 })
  await fs.rename(tmp, file)
}
