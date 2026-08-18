/**
 * The single implementation of the coordinator attach protocol.
 *
 * One backend coordinator owns each SQLite database. Whichever client starts
 * first publishes a manifest; every other client reads it and attaches. Both
 * the TUI (`cli/cmd/tui/coordinator-registry.ts`) and the Electron main process
 * (`src/main/sidecar.ts`) used to carry their own copy of the manifest schema,
 * the lease files, and the health probe, which is exactly the kind of pair that
 * drifts. This module owns all three so the two agree by construction.
 *
 * Only node builtins are imported: Electron main bundles this file with esbuild
 * and rejects any non-builtin external, and the TUI imports it from Bun.
 */
import { createHash, randomBytes } from "node:crypto"
import fs from "node:fs/promises"
import path from "node:path"

/**
 * Schema number of the manifest file. This intentionally did NOT move when
 * `serverVersion` was added. Readers fail closed on validation errors, while
 * additive optional fields let older binaries retain compatibility with newer
 * coordinators. New information must arrive that way instead of through a
 * version bump.
 */
export const COORDINATOR_MANIFEST_VERSION = 2
export const COORDINATOR_CLIENT_LEASE_VERSION = 1
export const COORDINATOR_HANDOFF_LEGACY_VERSION = 1
export const COORDINATOR_HANDOFF_VERSION = 2
/** Directory under the state root that holds manifests, leases, and startup logs. */
export const COORDINATOR_DIRECTORY = "tui-coordinators"
/** Version string used by builds that were not stamped by the release pipeline. */
export const COORDINATOR_LOCAL_VERSION = "local"
export const COORDINATOR_SKIP_VERSION_CHECK_ENV = "OPENCODEX_SKIP_VERSION_CHECK"

const CLIENT_HEARTBEAT_INTERVAL = 2_000
const HEALTH_TIMEOUT = 1_500
const AUTHORITY_LOCK_TIMEOUT = 15_000
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]", "::1"])

export type CoordinatorManifest = {
  version: typeof COORDINATOR_MANIFEST_VERSION
  key: string
  directory: string
  database: string
  pid: number
  url: string
  username: string
  password: string
  token: string
  createdAt: string
  /**
   * Server version of the process that published the manifest. Optional on the
   * wire: a coordinator started by a binary older than the handshake has none,
   * and that absence is what identifies it as legacy.
   */
  serverVersion?: string
  /** Opaque generation of the process currently holding database authority. */
  authorityEpoch?: string
  /** Whether this authority currently accepts new work. */
  admission?: boolean
  /** Whether this authority is ready to serve attached clients. */
  ready?: boolean
}

export type CoordinatorClientLease = {
  version: typeof COORDINATOR_CLIENT_LEASE_VERSION
  key: string
  pid: number
  updatedAt: number
}

/**
 * Credential-free coordination state for a future authority handoff. The
 * opaque pair identifies one request against one generation of the source
 * coordinator; attach credentials never belong in this file.
 */
export type CoordinatorHandoffRecord = {
  version: typeof COORDINATOR_HANDOFF_VERSION
  request: string
  phase: CoordinatorHandoffPhase
  revision: number
  sourceEpoch: string
  targetEpoch?: string
  createdAt: string
  updatedAt: string
}

export type LegacyCoordinatorHandoffRecord = {
  version: typeof COORDINATOR_HANDOFF_LEGACY_VERSION
  request: string
  sourceToken: string
}

export type CoordinatorHandoffWireRecord = LegacyCoordinatorHandoffRecord | CoordinatorHandoffRecord

export type CoordinatorHandoffPhase = "requested" | "accepted" | "ready" | "committed"

export type CoordinatorCredentials = Pick<CoordinatorManifest, "username" | "password">

export type CoordinatorHealth = {
  healthy: boolean
  version?: string
  active?: boolean
  authorityEpoch?: string
  admission?: boolean
  ready?: boolean
}

export type CoordinatorAuthorityResolution =
  | { state: "absent" }
  | { state: "active"; authorityEpoch: string | undefined }
  | { state: "handoff"; authorityEpoch: string; handoff: CoordinatorHandoffRecord }
  | {
      state: "blocked"
      reason:
        | "malformed_handoff"
        | "legacy_handoff"
        | "orphaned_handoff"
        | "incompatible_handoff"
        | "incompatible_authority"
        | "invalid_authority"
        | "unhealthy"
        | "not_ready"
        | "admission_closed"
    }

export type CoordinatorAuthorityTransactionResult<T> =
  | { state: "committed"; value: T }
  | {
      state: "progressing"
      reason: "manifest_changed" | "handoff_changed" | "handoff_present" | "manifest_absent" | "invalid_manifest"
    }

export type CoordinatorAuthorityObservation =
  | {
      state: "observed"
      manifest?: CoordinatorManifest
      health?: CoordinatorHealth
      handoff?: unknown
      authority: CoordinatorAuthorityResolution
    }
  | { state: "progressing" }

export type CoordinatorManifestFence = Pick<CoordinatorManifest, "token" | "authorityEpoch">
export type CoordinatorHandoffFence = Pick<
  CoordinatorHandoffRecord,
  "request" | "phase" | "revision" | "sourceEpoch" | "targetEpoch"
>

export type CoordinatorAuthorityLock = {
  stateRoot: string
  key: string
  token: string
  release: () => Promise<void>
}

export type CoordinatorCompatibilityReason =
  | "match"
  | "skipped"
  | "local"
  | "legacy"
  | "mismatch"
  | "health_mismatch"

export type CoordinatorCompatibility = {
  compatible: boolean
  reason: CoordinatorCompatibilityReason
  /** Human-readable detail: a warning when compatible, the refusal when not. */
  message?: string
}

export function coordinatorRoot(stateRoot: string) {
  return path.join(stateRoot, COORDINATOR_DIRECTORY)
}

export function coordinatorManifestPath(stateRoot: string, key: string) {
  return path.join(coordinatorRoot(stateRoot), `${key}.json`)
}

export function coordinatorClientDir(stateRoot: string, key: string) {
  return path.join(coordinatorRoot(stateRoot), `${key}.clients`)
}

export function coordinatorStartupLogPath(stateRoot: string, key: string) {
  return path.join(coordinatorRoot(stateRoot), `${key}.startup.log`)
}

export function isCoordinatorKey(key: string) {
  return /^[0-9a-f]{40}$/.test(key)
}

export function coordinatorHandoffPath(stateRoot: string, key: string) {
  if (!isCoordinatorKey(key)) throw new Error("Invalid coordinator key")
  return path.join(coordinatorRoot(stateRoot), `${key}.handoff.json`)
}

export function coordinatorAuthorityLockPath(stateRoot: string, key: string) {
  if (!isCoordinatorKey(key)) throw new Error("Invalid coordinator key")
  return path.join(coordinatorRoot(stateRoot), `${key}.authority.lock`)
}

/**
 * Canonical spelling of a database path, used as the hash input for the
 * coordinator key. `base` resolves relative paths; callers that mean "relative
 * to the process cwd" leave it unset, and the GUI passes its data root because
 * a bare filename there names a database in that directory.
 */
export function coordinatorDatabaseIdentity(database: string, base?: string) {
  if (database === ":memory:") return database
  const resolved = base && !path.isAbsolute(database) ? path.resolve(base, database) : path.resolve(database)
  return process.platform === "win32" ? resolved.toLowerCase() : resolved
}

export function coordinatorKey(database: string, base?: string) {
  return createHash("sha1").update(coordinatorDatabaseIdentity(database, base)).digest("hex")
}

export function coordinatorHeaders(credentials: CoordinatorCredentials) {
  const encoded = Buffer.from(`${credentials.username}:${credentials.password}`).toString("base64")
  return { authorization: `Basic ${encoded}` }
}

export function isLoopbackCoordinatorURL(value: string) {
  return coordinatorURL(value) !== undefined
}

export function coordinatorURL(value: string) {
  try {
    const url = new URL(value)
    if (url.protocol === "http:" && LOOPBACK_HOSTS.has(url.hostname)) return url
  } catch {
    // Fall through: an unparseable URL is not a loopback URL.
  }
  return undefined
}

/**
 * Field-typed validation. Unknown extra fields pass on purpose so a newer
 * coordinator can add information without an older reader treating the manifest
 * as corrupt (see `COORDINATOR_MANIFEST_VERSION`).
 */
export function isCoordinatorManifest(value: unknown): value is CoordinatorManifest {
  if (typeof value !== "object" || value === null) return false
  const manifest = value as Partial<CoordinatorManifest>
  return (
    manifest.version === COORDINATOR_MANIFEST_VERSION &&
    typeof manifest.key === "string" &&
    typeof manifest.directory === "string" &&
    typeof manifest.database === "string" &&
    typeof manifest.pid === "number" &&
    typeof manifest.url === "string" &&
    isLoopbackCoordinatorURL(manifest.url) &&
    typeof manifest.username === "string" &&
    typeof manifest.password === "string" &&
    typeof manifest.token === "string" &&
    typeof manifest.createdAt === "string" &&
    (manifest.serverVersion === undefined || typeof manifest.serverVersion === "string") &&
    (manifest.authorityEpoch === undefined ||
      (typeof manifest.authorityEpoch === "string" && manifest.authorityEpoch.length > 0)) &&
    (manifest.admission === undefined || typeof manifest.admission === "boolean") &&
    (manifest.ready === undefined || typeof manifest.ready === "boolean")
  )
}

export function parseCoordinatorManifest(raw: string): CoordinatorManifest {
  const parsed = JSON.parse(raw) as unknown
  if (!isCoordinatorManifest(parsed)) throw new Error("Invalid coordinator manifest")
  return parsed
}

export function isCoordinatorClientLease(value: unknown): value is CoordinatorClientLease {
  if (typeof value !== "object" || value === null) return false
  const lease = value as Partial<CoordinatorClientLease>
  return (
    lease.version === COORDINATOR_CLIENT_LEASE_VERSION &&
    typeof lease.key === "string" &&
    typeof lease.pid === "number" &&
    typeof lease.updatedAt === "number"
  )
}

export function isCoordinatorHandoffRecord(value: unknown): value is CoordinatorHandoffRecord {
  return normalizeCoordinatorHandoffRecord(value) !== undefined
}

/** Returns a canonical v2 record. Legacy v1 records remain distinct on purpose. */
export function normalizeCoordinatorHandoffRecord(value: unknown): CoordinatorHandoffRecord | undefined {
  if (typeof value !== "object" || value === null) return undefined
  if ("sourceToken" in value) return undefined
  const record = value as Partial<CoordinatorHandoffRecord>
  if (record.version !== COORDINATOR_HANDOFF_VERSION) return undefined
  if (typeof record.request !== "string" || record.request.length === 0) return undefined
  if (!isCoordinatorHandoffPhase(record.phase)) return undefined
  if (typeof record.revision !== "number" || !Number.isSafeInteger(record.revision) || record.revision < 0) {
    return undefined
  }
  if (typeof record.sourceEpoch !== "string" || record.sourceEpoch.length === 0) return undefined
  if (record.targetEpoch !== undefined && (typeof record.targetEpoch !== "string" || record.targetEpoch.length === 0)) {
    return undefined
  }
  if (!isCoordinatorTimestamp(record.createdAt) || !isCoordinatorTimestamp(record.updatedAt)) return undefined
  if (Date.parse(record.updatedAt) < Date.parse(record.createdAt)) return undefined
  if (record.phase === "requested" && record.targetEpoch !== undefined) return undefined
  if (record.phase !== "requested" && record.targetEpoch === undefined) return undefined
  if (record.revision !== coordinatorHandoffPhaseRevision(record.phase)) return undefined
  return {
    version: COORDINATOR_HANDOFF_VERSION,
    request: record.request,
    phase: record.phase,
    revision: record.revision,
    sourceEpoch: record.sourceEpoch,
    ...(record.targetEpoch === undefined ? {} : { targetEpoch: record.targetEpoch }),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  }
}

/** Recognizes additive v1 wire records, but never a v1/v2 hybrid. */
export function isLegacyCoordinatorHandoffRecord(value: unknown): value is LegacyCoordinatorHandoffRecord {
  if (typeof value !== "object" || value === null) return false
  if (
    "phase" in value ||
    "revision" in value ||
    "sourceEpoch" in value ||
    "targetEpoch" in value ||
    "createdAt" in value ||
    "updatedAt" in value
  ) {
    return false
  }
  const record = value as Partial<LegacyCoordinatorHandoffRecord>
  return (
    record.version === COORDINATOR_HANDOFF_LEGACY_VERSION &&
    typeof record.request === "string" &&
    record.request.length > 0 &&
    typeof record.sourceToken === "string" &&
    record.sourceToken.length > 0
  )
}

export function parseCoordinatorHandoffRecord(raw: string): CoordinatorHandoffWireRecord {
  const parsed = JSON.parse(raw) as unknown
  const record = normalizeCoordinatorHandoffRecord(parsed)
  if (record) return record
  if (isLegacyCoordinatorHandoffRecord(parsed)) return parsed
  throw new Error("Invalid coordinator handoff record")
}

/** Enforces the monotonic handoff state machine independently of file locking. */
export function checkCoordinatorHandoffTransition(
  current: CoordinatorHandoffRecord | undefined,
  next: CoordinatorHandoffRecord,
) {
  if (!isCoordinatorHandoffRecord(next)) return false
  if (!current) return next.phase === "requested" && next.revision === 0
  if (next.request !== current.request || next.sourceEpoch !== current.sourceEpoch) return false
  if (next.createdAt !== current.createdAt || next.revision !== current.revision + 1) return false
  if (Date.parse(next.updatedAt) <= Date.parse(current.updatedAt)) return false
  if (current.targetEpoch !== undefined && next.targetEpoch !== current.targetEpoch) return false
  if (current.phase === "requested") return next.phase === "accepted" && next.targetEpoch !== undefined
  if (current.phase === "accepted") return next.phase === "ready"
  if (current.phase === "ready") return next.phase === "committed"
  return false
}

function isCoordinatorHandoffPhase(value: unknown): value is CoordinatorHandoffPhase {
  return value === "requested" || value === "accepted" || value === "ready" || value === "committed"
}

function coordinatorHandoffPhaseRevision(phase: CoordinatorHandoffPhase) {
  if (phase === "requested") return 0
  if (phase === "accepted") return 1
  if (phase === "ready") return 2
  return 3
}

function isCoordinatorTimestamp(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value))
}

/** Reads and validates a manifest file, propagating filesystem errors. */
export async function readCoordinatorManifestFile(file: string) {
  return parseCoordinatorManifest(await fs.readFile(file, "utf8"))
}

export async function readCoordinatorManifest(stateRoot: string, key: string) {
  return readCoordinatorManifestFile(coordinatorManifestPath(stateRoot, key))
}

/** Reads and validates a handoff file, propagating missing and malformed state. */
export async function readCoordinatorHandoffFile(file: string) {
  return parseCoordinatorHandoffRecord(await fs.readFile(file, "utf8"))
}

export async function readCoordinatorHandoff(stateRoot: string, key: string) {
  return readCoordinatorHandoffFile(coordinatorHandoffPath(stateRoot, key))
}

/**
 * Reads the handoff as an authority observation. Invalid JSON is returned as a
 * present, malformed value so election callers cannot mistake it for absence.
 */
export async function readCoordinatorHandoffObservation(stateRoot: string, key: string): Promise<unknown> {
  return readCoordinatorHandoffFileObservation(coordinatorHandoffPath(stateRoot, key))
}

export async function readCoordinatorHandoffFileObservation(file: string): Promise<unknown> {
  try {
    const raw = await fs.readFile(file, "utf8")
    try {
      return JSON.parse(raw) as unknown
    } catch {
      return raw
    }
  } catch (error) {
    if (isMissingCoordinatorFile(error)) return undefined
    throw error
  }
}

export async function acquireCoordinatorAuthorityLock(
  stateRoot: string,
  key: string,
  options?: {
    timeout?: number
    now?: () => number
    sleep?: (milliseconds: number) => Promise<void>
    processAlive?: (pid: number) => boolean
  },
): Promise<CoordinatorAuthorityLock> {
  const root = coordinatorRoot(stateRoot)
  const lock = coordinatorAuthorityLockPath(stateRoot, key)
  const token = randomBytes(16).toString("hex")
  const now = options?.now ?? Date.now
  const sleep = options?.sleep ?? ((milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds)))
  const started = now()
  await fs.mkdir(root, { recursive: true })
  while (true) {
    try {
      await fs.mkdir(lock)
      try {
        await fs.writeFile(
          path.join(lock, "owner.json"),
          JSON.stringify({ pid: process.pid, token, createdAt: Date.now() }),
          { mode: 0o600 },
        )
      } catch (error) {
        await fs.rm(lock, { recursive: true, force: true }).catch(() => undefined)
        throw error
      }
      return {
        stateRoot,
        key,
        token,
        release: async () => {
          const owner = await readAuthorityLockOwner(lock)
          if (owner?.token !== token) return
          await fs.rm(lock, { recursive: true, force: true })
        },
      }
    } catch (error) {
      if (!isAlreadyExists(error)) throw error
    }
    await reclaimCoordinatorAuthorityLock(lock, options?.processAlive ?? isCoordinatorProcessAlive)
    if (now() - started >= (options?.timeout ?? AUTHORITY_LOCK_TIMEOUT)) {
      throw new Error(`Timed out acquiring coordinator authority lock for ${key}`)
    }
    await sleep(25)
  }
}

export async function withCoordinatorAuthorityLock<T>(
  stateRoot: string,
  key: string,
  fn: (lock: CoordinatorAuthorityLock) => Promise<T>,
) {
  const lock = await acquireCoordinatorAuthorityLock(stateRoot, key)
  try {
    return await fn(lock)
  } finally {
    await lock.release()
  }
}

export async function runCoordinatorAuthorityTransaction<T>(input: {
  stateRoot: string
  key: string
  manifest?: CoordinatorManifestFence
  requireHandoffAbsent?: boolean
  action: (lock: CoordinatorAuthorityLock, manifest: CoordinatorManifest | undefined) => Promise<T>
}): Promise<CoordinatorAuthorityTransactionResult<T>> {
  return withCoordinatorAuthorityLock(input.stateRoot, input.key, async (lock) => {
    const snapshot = await readCoordinatorAuthoritySnapshot(input.stateRoot, input.key).catch(() => undefined)
    if (!snapshot) return { state: "progressing", reason: "invalid_manifest" }
    if (input.manifest && !snapshot.manifest) return { state: "progressing", reason: "manifest_absent" }
    if (
      input.manifest &&
      (snapshot.manifest?.token !== input.manifest.token ||
        snapshot.manifest.authorityEpoch !== input.manifest.authorityEpoch)
    ) {
      return { state: "progressing", reason: "manifest_changed" }
    }
    if (input.requireHandoffAbsent && snapshot.handoffRaw !== undefined) {
      return { state: "progressing", reason: "handoff_present" }
    }
    return { state: "committed", value: await input.action(lock, snapshot.manifest) }
  })
}

export async function observeCoordinatorAuthority(input: {
  stateRoot: string
  key: string
  fetch?: typeof globalThis.fetch
  timeout?: number
  afterProbe?: (attempt: number) => Promise<void>
}): Promise<CoordinatorAuthorityObservation> {
  for (const attempt of [0, 1]) {
    const before = await withCoordinatorAuthorityLock(input.stateRoot, input.key, () =>
      readCoordinatorAuthoritySnapshot(input.stateRoot, input.key),
    )
    const health = before.manifest
      ? await fetchCoordinatorHealth(before.manifest, { fetch: input.fetch, timeout: input.timeout })
      : undefined
    await input.afterProbe?.(attempt)
    const observed = await withCoordinatorAuthorityLock(input.stateRoot, input.key, async () => {
      const after = await readCoordinatorAuthoritySnapshot(input.stateRoot, input.key)
      if (!sameCoordinatorAuthorityFence(before, after)) return undefined
      return {
        state: "observed" as const,
        manifest: before.manifest,
        health,
        handoff: before.handoff,
        authority: resolveCoordinatorAuthority({ manifest: before.manifest, health, handoff: before.handoff }),
      }
    })
    if (observed) return observed
  }
  return { state: "progressing" }
}

async function readCoordinatorAuthoritySnapshot(stateRoot: string, key: string) {
  const manifestRaw = await readOptionalCoordinatorFile(coordinatorManifestPath(stateRoot, key))
  const handoffRaw = await readOptionalCoordinatorFile(coordinatorHandoffPath(stateRoot, key))
  const manifest = manifestRaw === undefined ? undefined : parseCoordinatorManifest(manifestRaw)
  const handoff = handoffRaw === undefined ? undefined : parseAuthorityHandoffObservation(handoffRaw)
  return { manifest, handoff, handoffRaw }
}

function sameCoordinatorAuthorityFence(
  before: Awaited<ReturnType<typeof readCoordinatorAuthoritySnapshot>>,
  after: Awaited<ReturnType<typeof readCoordinatorAuthoritySnapshot>>,
) {
  return (
    before.manifest?.token === after.manifest?.token &&
    before.manifest?.authorityEpoch === after.manifest?.authorityEpoch &&
    before.handoffRaw === after.handoffRaw
  )
}

function parseAuthorityHandoffObservation(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown
  } catch {
    return raw
  }
}

async function readOptionalCoordinatorFile(file: string) {
  return fs.readFile(file, "utf8").catch((error) => {
    if (isMissingCoordinatorFile(error)) return undefined
    throw error
  })
}

async function readAuthorityLockOwner(lock: string) {
  return fs
    .readFile(path.join(lock, "owner.json"), "utf8")
    .then((raw): unknown => JSON.parse(raw))
    .then((value) => {
      if (typeof value !== "object" || value === null) return undefined
      if (!("pid" in value) || typeof value.pid !== "number") return undefined
      if (!("token" in value) || typeof value.token !== "string") return undefined
      return { pid: value.pid, token: value.token }
    })
    .catch(() => undefined)
}

async function reclaimCoordinatorAuthorityLock(lock: string, processAlive: (pid: number) => boolean) {
  const owner = await readAuthorityLockOwner(lock)
  // Node has no portable process-start identity lookup, so PID reuse remains a
  // residual risk. Ambiguous owner metadata is never reclaimed.
  if (!owner || processAlive(owner.pid)) return
  const reclaimed = `${lock}.stale.${process.pid}.${randomBytes(4).toString("hex")}`
  await fs.rename(lock, reclaimed).catch(() => undefined)
  await fs.rm(reclaimed, { recursive: true, force: true }).catch(() => undefined)
}

function isAlreadyExists(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error && error.code === "EEXIST"
}

/**
 * Reads only the token, without validating the rest. Removal is token-guarded,
 * so a manifest that fails validation still has to prove ownership before it
 * can be deleted.
 */
export async function readCoordinatorManifestToken(file: string) {
  return fs
    .readFile(file, "utf8")
    .then((raw): unknown => JSON.parse(raw))
    .then((manifest) => {
      if (typeof manifest !== "object" || manifest === null || !("token" in manifest)) return undefined
      return typeof manifest.token === "string" ? manifest.token : undefined
    })
    .catch(() => undefined)
}

export function publishCoordinatorManifest(
  stateRoot: string,
  manifest: CoordinatorManifest,
  expectedHandoff: CoordinatorHandoffFence | undefined,
) {
  return withCoordinatorAuthorityLock(stateRoot, manifest.key, async () => {
    const snapshot = await readCoordinatorAuthoritySnapshot(stateRoot, manifest.key)
    if (snapshot.manifest) return { state: "progressing" as const, reason: "manifest_changed" as const }
    if (!matchesCoordinatorHandoffFence(snapshot.handoff, expectedHandoff)) {
      return { state: "progressing" as const, reason: "handoff_changed" as const }
    }
    await writeCoordinatorManifestLocked(stateRoot, manifest)
    return { state: "committed" as const, value: manifest }
  })
}

export function replaceCoordinatorManifest(input: {
  stateRoot: string
  manifest: CoordinatorManifest
  expectedManifest: CoordinatorManifestFence
  expectedHandoff: CoordinatorHandoffFence | undefined
}) {
  return withCoordinatorAuthorityLock(input.stateRoot, input.manifest.key, async () => {
    const snapshot = await readCoordinatorAuthoritySnapshot(input.stateRoot, input.manifest.key)
    if (
      snapshot.manifest?.token !== input.expectedManifest.token ||
      snapshot.manifest.authorityEpoch !== input.expectedManifest.authorityEpoch
    ) {
      return { state: "progressing" as const, reason: "manifest_changed" as const }
    }
    if (!matchesCoordinatorHandoffFence(snapshot.handoff, input.expectedHandoff)) {
      return { state: "progressing" as const, reason: "handoff_changed" as const }
    }
    await writeCoordinatorManifestLocked(input.stateRoot, input.manifest)
    return { state: "committed" as const, value: input.manifest }
  })
}

async function writeCoordinatorManifestLocked(stateRoot: string, manifest: CoordinatorManifest) {
  const root = coordinatorRoot(stateRoot)
  await fs.mkdir(root, { recursive: true })
  const file = coordinatorManifestPath(stateRoot, manifest.key)
  const temporary = `${file}.${process.pid}.${Date.now()}.tmp`
  try {
    await fs.writeFile(temporary, JSON.stringify(manifest, null, 2), { mode: 0o600 })
    await fs.rename(temporary, file)
  } finally {
    await fs.rm(temporary, { force: true }).catch(() => undefined)
  }
}

function matchesCoordinatorHandoffFence(handoff: unknown, expected: CoordinatorHandoffFence | undefined) {
  if (!expected) return handoff === undefined
  if (!isCoordinatorHandoffRecord(handoff)) return false
  return (
    handoff.request === expected.request &&
    handoff.phase === expected.phase &&
    handoff.revision === expected.revision &&
    handoff.sourceEpoch === expected.sourceEpoch &&
    handoff.targetEpoch === expected.targetEpoch
  )
}

/** Deletes only under an exact manifest fence while the handoff is absent. */
export function removeCoordinatorManifest(stateRoot: string, key: string, manifest: CoordinatorManifestFence) {
  return withCoordinatorAuthorityLock(stateRoot, key, (lock) => removeCoordinatorManifestLocked(lock, manifest))
}

export async function removeCoordinatorManifestLocked(
  lock: CoordinatorAuthorityLock,
  manifest: CoordinatorManifestFence,
): Promise<CoordinatorAuthorityTransactionResult<boolean>> {
  const snapshot = await readCoordinatorAuthoritySnapshot(lock.stateRoot, lock.key).catch(() => undefined)
  if (!snapshot) return { state: "progressing", reason: "invalid_manifest" }
  if (!snapshot.manifest) return { state: "progressing", reason: "manifest_absent" }
  if (
    snapshot.manifest.token !== manifest.token ||
    snapshot.manifest.authorityEpoch !== manifest.authorityEpoch
  ) {
    return { state: "progressing", reason: "manifest_changed" }
  }
  if (snapshot.handoffRaw !== undefined) return { state: "progressing", reason: "handoff_present" }
  await fs.rm(coordinatorManifestPath(lock.stateRoot, lock.key), { force: true })
  return { state: "committed", value: true }
}

export function isCoordinatorProcessAlive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return typeof error === "object" && error !== null && "code" in error && error.code === "EPERM"
  }
}

export function isMissingCoordinatorFile(error: unknown) {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error.code === "ENOENT" || error.code === "ENOTDIR")
  )
}

/**
 * Probes `/global/health`. Returns `undefined` when the coordinator is
 * unreachable or answers with something unusable, which is the only signal a
 * caller needs to treat the manifest as dead. The parsed body carries the
 * server version, so this is also the handshake's second source of truth.
 */
export async function fetchCoordinatorHealth(
  manifest: Pick<CoordinatorManifest, "url"> & CoordinatorCredentials,
  options?: { timeout?: number; fetch?: typeof globalThis.fetch },
): Promise<CoordinatorHealth | undefined> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), options?.timeout ?? HEALTH_TIMEOUT)
  try {
    const request = options?.fetch ?? globalThis.fetch
    const response = await request(new URL("/global/health", manifest.url), {
      headers: coordinatorHeaders(manifest),
      signal: controller.signal,
    })
    if (!response.ok) return undefined
    const body: unknown = await response.json()
    if (typeof body !== "object" || body === null) return undefined
    if ("authorityEpoch" in body && (typeof body.authorityEpoch !== "string" || body.authorityEpoch.length === 0)) {
      return undefined
    }
    if ("admission" in body && typeof body.admission !== "boolean") return undefined
    if ("ready" in body && typeof body.ready !== "boolean") return undefined
    return {
      healthy: "healthy" in body && body.healthy === true,
      version: "version" in body && typeof body.version === "string" ? body.version : undefined,
      active: "active" in body && typeof body.active === "boolean" ? body.active : undefined,
      ...("authorityEpoch" in body && typeof body.authorityEpoch === "string"
        ? { authorityEpoch: body.authorityEpoch }
        : {}),
      ...("admission" in body && typeof body.admission === "boolean" ? { admission: body.admission } : {}),
      ...("ready" in body && typeof body.ready === "boolean" ? { ready: body.ready } : {}),
    }
  } catch {
    return undefined
  } finally {
    clearTimeout(timer)
  }
}

export async function isCoordinatorHealthy(
  manifest: Pick<CoordinatorManifest, "url"> & CoordinatorCredentials,
  options?: { timeout?: number; fetch?: typeof globalThis.fetch },
) {
  return (await fetchCoordinatorHealth(manifest, options))?.healthy === true
}

/**
 * Classifies observed authority state without performing I/O or changing election behavior.
 * Any handoff that cannot be proven to belong to the observed authority fails closed.
 */
export function resolveCoordinatorAuthority(input: {
  manifest?: CoordinatorManifest
  health?: CoordinatorHealth
  handoff?: unknown
}): CoordinatorAuthorityResolution {
  if (isLegacyCoordinatorHandoffRecord(input.handoff)) return { state: "blocked", reason: "legacy_handoff" }
  const handoff = input.handoff === undefined ? undefined : normalizeCoordinatorHandoffRecord(input.handoff)
  if (input.handoff !== undefined && !handoff) return { state: "blocked", reason: "malformed_handoff" }
  if (!input.manifest) {
    if (handoff) return { state: "blocked", reason: "orphaned_handoff" }
    return { state: "absent" }
  }
  if (input.manifest.authorityEpoch === "" || input.health?.authorityEpoch === "") {
    return { state: "blocked", reason: "invalid_authority" }
  }
  if (!input.health?.healthy) return { state: "blocked", reason: "unhealthy" }
  if (
    handoff &&
    (typeof input.health.authorityEpoch !== "string" ||
      input.health.authorityEpoch.length === 0 ||
      typeof input.health.admission !== "boolean" ||
      typeof input.health.ready !== "boolean")
  ) {
    return { state: "blocked", reason: "invalid_authority" }
  }
  if (
    input.manifest.authorityEpoch !== undefined &&
    input.health.authorityEpoch !== undefined &&
    input.manifest.authorityEpoch !== input.health.authorityEpoch
  ) {
    return { state: "blocked", reason: "incompatible_authority" }
  }
  const authorityEpoch = handoff
    ? input.health.authorityEpoch
    : input.health.authorityEpoch ?? input.manifest.authorityEpoch
  if (handoff) {
    const expectedEpoch =
      handoff.phase === "requested" || handoff.phase === "accepted" ? handoff.sourceEpoch : handoff.targetEpoch
    if (!authorityEpoch || expectedEpoch !== authorityEpoch) {
      return { state: "blocked", reason: "incompatible_handoff" }
    }
    return { state: "handoff", authorityEpoch, handoff }
  }
  if (input.manifest.ready === false || input.health.ready === false) return { state: "blocked", reason: "not_ready" }
  if (input.manifest.admission === false || input.health.admission === false) {
    return { state: "blocked", reason: "admission_closed" }
  }
  return { state: "active", authorityEpoch }
}

/**
 * Decides whether a client of `clientVersion` may attach to the coordinator the
 * manifest describes.
 *
 * Exact match is the rule; the escapes are deliberate and narrow:
 * - either side reporting `"local"` is a dev build, allowed with a warning;
 * - a manifest with no `serverVersion` predates the handshake and is refused,
 *   because nothing about its wire contract can be verified;
 * - `OPENCODEX_SKIP_VERSION_CHECK=1` bypasses everything.
 *
 * On refusal the caller must NOT delete the manifest or kill the coordinator.
 * The other client may be perfectly happy with it, and destroying it would
 * break the single-writer guarantee this whole protocol exists to hold.
 */
export function checkCoordinatorCompatibility(input: {
  manifest: Pick<CoordinatorManifest, "serverVersion">
  clientVersion: string
  /** Version reported by a live `/global/health`, when one was probed. */
  healthVersion?: string
  /** Defaults to the `OPENCODEX_SKIP_VERSION_CHECK` environment variable. */
  skip?: boolean
}): CoordinatorCompatibility {
  const skip = input.skip ?? coordinatorVersionCheckSkipped()
  if (skip) return { compatible: true, reason: "skipped" }

  const serverVersion = input.manifest.serverVersion
  if (serverVersion === undefined) {
    return {
      compatible: false,
      reason: "legacy",
      message:
        "The running coordinator predates the version handshake and does not publish its server version. Restart it so this client can verify the two agree.",
    }
  }

  if (
    serverVersion === COORDINATOR_LOCAL_VERSION ||
    input.clientVersion === COORDINATOR_LOCAL_VERSION ||
    input.healthVersion === COORDINATOR_LOCAL_VERSION
  ) {
    return {
      compatible: true,
      reason: "local",
      message: `Attaching across a local development build (coordinator ${serverVersion}, client ${input.clientVersion}); server and SDK skew is not validated.`,
    }
  }

  if (input.healthVersion !== undefined && input.healthVersion !== serverVersion) {
    return {
      compatible: false,
      reason: "health_mismatch",
      message: `The coordinator manifest claims version ${serverVersion} but the running process reports ${input.healthVersion}. The manifest is stale; restart the coordinator.`,
    }
  }

  if (serverVersion !== input.clientVersion) {
    return {
      compatible: false,
      reason: "mismatch",
      message: `The running coordinator is version ${serverVersion} but this client is ${input.clientVersion}. Close the other OpencodeX client so a matching coordinator can start, or set ${COORDINATOR_SKIP_VERSION_CHECK_ENV}=1 to attach anyway.`,
    }
  }

  return { compatible: true, reason: "match" }
}

export function coordinatorVersionCheckSkipped(env: NodeJS.ProcessEnv = process.env) {
  const value = env[COORDINATOR_SKIP_VERSION_CHECK_ENV]
  return value === "1" || value === "true"
}

export type CoordinatorClientLeaseHandle = {
  /** Resolves once the first lease file has been written. */
  ready: Promise<void>
  file: string
  dispose: () => Promise<void>
}

/**
 * Publishes a heartbeat file that tells the coordinator this client is still
 * attached. The coordinator shuts itself down once no lease remains, so the
 * file has to disappear on dispose even if a write is still in flight.
 *
 * `suffix` distinguishes client kinds inside the same directory (the GUI writes
 * `<pid>.gui.json` so the TUI can detect an active GUI without opening files).
 */
export function startCoordinatorClientLease(input: {
  stateRoot: string
  key: string
  suffix?: string
  pid?: number
  interval?: number
}): CoordinatorClientLeaseHandle {
  const pid = input.pid ?? process.pid
  const dir = coordinatorClientDir(input.stateRoot, input.key)
  const file = path.join(dir, `${pid}${input.suffix ?? ""}.json`)
  let disposed = false
  let writing = Promise.resolve()

  const write = () => {
    const next = writing.catch(() => {}).then(async () => {
      if (disposed) return
      await fs.mkdir(dir, { recursive: true })
      if (disposed) return
      const temporary = `${file}.${randomBytes(4).toString("hex")}.tmp`
      try {
        await fs.writeFile(
          temporary,
          JSON.stringify({
            version: COORDINATOR_CLIENT_LEASE_VERSION,
            key: input.key,
            pid,
            updatedAt: Date.now(),
          } satisfies CoordinatorClientLease),
          { mode: 0o600 },
        )
        if (disposed) return
        await fs.rename(temporary, file)
        if (disposed) await fs.rm(file, { force: true })
      } finally {
        await fs.rm(temporary, { force: true }).catch(() => {})
      }
    })
    writing = next
    return next
  }

  const timer = setInterval(() => {
    void write().catch(() => {})
  }, input.interval ?? CLIENT_HEARTBEAT_INTERVAL)
  timer.unref?.()
  const ready = write()

  return {
    ready,
    file,
    async dispose() {
      if (disposed) return
      disposed = true
      clearInterval(timer)
      await Promise.all([ready.catch(() => undefined), writing.catch(() => undefined)])
      await fs.rm(file, { force: true }).catch(() => undefined)
    },
  }
}
