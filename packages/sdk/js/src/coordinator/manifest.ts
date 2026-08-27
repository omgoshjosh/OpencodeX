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
 * `serverVersion` was added: both readers hard-delete a manifest that fails
 * validation, so an older binary meeting a newer schema number would delete the
 * manifest of a live, healthy coordinator and start a second writer on the same
 * database. Additive optional fields pass every field-typed validator, so new
 * information must arrive that way instead of through a version bump.
 */
export const COORDINATOR_MANIFEST_VERSION = 2
export const COORDINATOR_CLIENT_LEASE_VERSION = 1
/** Directory under the state root that holds manifests, leases, and startup logs. */
export const COORDINATOR_DIRECTORY = "tui-coordinators"
/** Version string used by builds that were not stamped by the release pipeline. */
export const COORDINATOR_LOCAL_VERSION = "local"
export const COORDINATOR_SKIP_VERSION_CHECK_ENV = "OPENCODEX_SKIP_VERSION_CHECK"
/** Per-attempt health timeout in milliseconds. May only raise the built-in floor. */
export const COORDINATOR_HEALTH_TIMEOUT_ENV = "OPENCODEX_COORDINATOR_HEALTH_TIMEOUT"
/** Health probe attempt count. Set to 1 to restore the pre-retry single-shot behaviour. */
export const COORDINATOR_HEALTH_ATTEMPTS_ENV = "OPENCODEX_COORDINATOR_HEALTH_ATTEMPTS"

const CLIENT_HEARTBEAT_INTERVAL = 2_000
const HEALTH_TIMEOUT = 1_500
const HEALTH_ATTEMPTS = 3
/** Backoff before attempt N (index 0 is the first attempt, which never waits). */
const HEALTH_RETRY_DELAYS = [0, 200, 400]
/**
 * Hard ceiling on a whole escalating probe, checked before each attempt starts
 * so the sequence can never overrun it. The default schedule
 * (1500 → +200 → 3000 → +400 → 4500) tops out at 9.6s and fits inside it.
 */
const HEALTH_TOTAL_TIMEOUT = 10_000
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]", "::1"])
/**
 * Errno values that mean "nothing is listening there". Node and Bun surface
 * these as a `TypeError` with the real error hanging off `cause`, so they are
 * read structurally — never by matching the message text, which is not a
 * stable contract across runtimes or locales.
 */
const CONNECTION_REFUSED_CODES = new Set(["ECONNREFUSED", "EADDRNOTAVAIL", "EHOSTUNREACH"])

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
}

export type CoordinatorClientLease = {
  version: typeof COORDINATOR_CLIENT_LEASE_VERSION
  key: string
  pid: number
  updatedAt: number
}

export type CoordinatorCredentials = Pick<CoordinatorManifest, "username" | "password">

export type CoordinatorHealth = {
  healthy: boolean
  version?: string
  active?: boolean
  coordinatorKey?: string
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
    (manifest.serverVersion === undefined || typeof manifest.serverVersion === "string")
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

/** Reads and validates a manifest file, propagating filesystem errors. */
export async function readCoordinatorManifestFile(file: string) {
  return parseCoordinatorManifest(await fs.readFile(file, "utf8"))
}

export async function readCoordinatorManifest(stateRoot: string, key: string) {
  return readCoordinatorManifestFile(coordinatorManifestPath(stateRoot, key))
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

export async function writeCoordinatorManifest(stateRoot: string, manifest: CoordinatorManifest) {
  const root = coordinatorRoot(stateRoot)
  await fs.mkdir(root, { recursive: true })
  const file = coordinatorManifestPath(stateRoot, manifest.key)
  const temporary = `${file}.${process.pid}.${Date.now()}.tmp`
  await fs.writeFile(temporary, JSON.stringify(manifest, null, 2), { mode: 0o600 })
  await fs.rename(temporary, file)
}

/** Deletes the manifest only when `token` matches the one on disk. */
export async function removeCoordinatorManifest(stateRoot: string, key: string, token: string) {
  const file = coordinatorManifestPath(stateRoot, key)
  if ((await readCoordinatorManifestToken(file)) !== token) return false
  await fs.rm(file, { force: true })
  return true
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

/** Everything a caller needs to identify and reach a coordinator's health endpoint. */
export type CoordinatorEndpoint = Pick<CoordinatorManifest, "url"> & CoordinatorCredentials

/**
 * The outcome of one health request, kept as a discriminated union rather than
 * `CoordinatorHealth | undefined` because the collapse to `undefined` is the
 * bug this type exists to prevent: "the coordinator said it is unhealthy",
 * "the request timed out", and "nothing is listening on that port" are three
 * different facts, and only the last of them means the coordinator is gone.
 */
export type CoordinatorProbe =
  /**
   * 2xx, parsed body, `healthy: true`.
   *
   * `coordinatorKey` is the database identity the answering process claims. It
   * is optional because a coordinator predating the identity field does not
   * send one; see `isCoordinatorProbeForManifest` for what that absence means.
   */
  | { kind: "healthy"; version?: string; active?: boolean; coordinatorKey?: string; ms: number }
  /** 2xx, parsed body, `healthy` was not `true`. The coordinator is up but says no. */
  | { kind: "unhealthy"; version?: string; active?: boolean; coordinatorKey?: string; ms: number }
  /** The request was aborted by our own deadline. Says nothing about liveness. */
  | { kind: "timeout"; ms: number }
  /** The connection could not be established at all. */
  | { kind: "refused"; code: string }
  /** A non-2xx status. */
  | { kind: "http"; status: number }
  /** 2xx whose body is not a JSON object — so whatever answered is not a coordinator. */
  | { kind: "body" }
  /** Anything unrecognised. Always treated as ambiguous, never as dead. */
  | { kind: "unknown"; detail: string }

export type CoordinatorProbeResult = {
  probe: CoordinatorProbe
  attempts: number
  elapsedMs: number
}

/** How hard a caller is willing to work before believing a coordinator is gone. */
export type CoordinatorProbeMode =
  /** One attempt, no escalation: for polls and fan-outs where the caller retries. */
  | "quick"
  /** The escalating retry: for the one-shot decision to attach, reclaim, or refuse. */
  | "decide"

export type CoordinatorProbeOptions = {
  timeout?: number
  fetch?: typeof globalThis.fetch
}

export type CoordinatorProbeRetryOptions = CoordinatorProbeOptions & {
  attempts?: number
  /** Wall-clock ceiling for the whole sequence. */
  totalTimeout?: number
  /** Injected for tests so backoff does not cost real time. */
  delay?: (ms: number) => Promise<void>
  env?: NodeJS.ProcessEnv
}

/**
 * Probes `/global/health` once and classifies the outcome.
 *
 * Classification is structural throughout: an abort is recognised by
 * `error.name`, a refusal by an errno on the error or its `cause`. Anything
 * that does not match a known shape becomes `unknown`, which callers must
 * treat as ambiguous — guessing "dead" from an unrecognised error is how a
 * healthy coordinator gets replaced by a second writer on its own database.
 */
export async function probeCoordinatorHealth(
  manifest: CoordinatorEndpoint,
  options?: CoordinatorProbeOptions,
): Promise<CoordinatorProbe> {
  const started = Date.now()
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), options?.timeout ?? HEALTH_TIMEOUT)
  try {
    const request = options?.fetch ?? globalThis.fetch
    const response = await request(new URL("/global/health", manifest.url), {
      headers: coordinatorHeaders(manifest),
      signal: controller.signal,
    })
    if (!response.ok) return { kind: "http", status: response.status }
    const body = await response
      .json()
      .then((value: unknown) => value)
      .catch(() => undefined)
    if (typeof body !== "object" || body === null) return { kind: "body" }
    const healthy = "healthy" in body && body.healthy === true
    return {
      kind: healthy ? "healthy" : "unhealthy",
      version: "version" in body && typeof body.version === "string" ? body.version : undefined,
      active: "active" in body && typeof body.active === "boolean" ? body.active : undefined,
      coordinatorKey:
        "coordinatorKey" in body && typeof body.coordinatorKey === "string" ? body.coordinatorKey : undefined,
      ms: Date.now() - started,
    }
  } catch (error) {
    return classifyCoordinatorProbeError(error, Date.now() - started)
  } finally {
    clearTimeout(timer)
  }
}

function classifyCoordinatorProbeError(error: unknown, ms: number): CoordinatorProbe {
  if (typeof error === "object" && error !== null) {
    if ("name" in error && error.name === "AbortError") return { kind: "timeout", ms }
    const code = errnoOf(error) ?? errnoOf("cause" in error ? error.cause : undefined)
    if (code !== undefined && CONNECTION_REFUSED_CODES.has(code)) return { kind: "refused", code }
  }
  return { kind: "unknown", detail: describeCoordinatorProbeError(error) }
}

function errnoOf(value: unknown) {
  if (typeof value !== "object" || value === null || !("code" in value)) return undefined
  return typeof value.code === "string" ? value.code : undefined
}

function describeCoordinatorProbeError(error: unknown) {
  if (typeof error === "object" && error !== null && "message" in error && typeof error.message === "string") {
    return `${"name" in error && typeof error.name === "string" ? error.name : "Error"}: ${error.message}`
  }
  return String(error)
}

/**
 * Whether an outcome leaves the coordinator's state genuinely unknown, and so
 * is worth asking again.
 *
 * The terminal cases are the ones a second request cannot change: `healthy` is
 * the answer, 401/403 means the manifest's credentials no longer match the live
 * process, and `body` means something that is not a coordinator owns the port.
 * Every other outcome — including `unknown` — is ambiguous by default.
 */
export function isAmbiguousCoordinatorProbe(probe: CoordinatorProbe) {
  switch (probe.kind) {
    case "healthy":
    case "body":
      return false
    case "http":
      return probe.status >= 500
    default:
      return true
  }
}

/**
 * Probes with escalating patience, retrying only on an ambiguous outcome.
 *
 * The reported bug was a single 1.5s probe aborting while the coordinator was
 * mid-stall and then being read as death. Each retry gets a longer deadline
 * than the last, because the thing being waited out is a busy event loop, not
 * a slow network.
 */
export async function probeCoordinatorHealthWithRetry(
  manifest: CoordinatorEndpoint,
  options?: CoordinatorProbeRetryOptions,
): Promise<CoordinatorProbeResult> {
  const env = options?.env ?? process.env
  const timeout = options?.timeout ?? Math.max(HEALTH_TIMEOUT, positiveInteger(env[COORDINATOR_HEALTH_TIMEOUT_ENV]) ?? 0)
  const attempts = options?.attempts ?? positiveInteger(env[COORDINATOR_HEALTH_ATTEMPTS_ENV]) ?? HEALTH_ATTEMPTS
  const total = options?.totalTimeout ?? HEALTH_TOTAL_TIMEOUT
  const delay = options?.delay ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)))
  const started = Date.now()

  let attempt = 0
  let probe: CoordinatorProbe = { kind: "unknown", detail: "health probe was never attempted" }
  while (attempt < attempts) {
    const backoff = HEALTH_RETRY_DELAYS[Math.min(attempt, HEALTH_RETRY_DELAYS.length - 1)] ?? 0
    /* Each attempt is more patient than the last: 1x, 2x, 3x the base timeout. */
    const budget = timeout * (attempt + 1)
    /* The first attempt always runs — a caller who raised the per-attempt
       timeout past the wall cap still deserves one probe. Later attempts must
       fit entirely inside the cap, deadline included, or they do not start. */
    if (attempt > 0 && Date.now() - started + backoff + budget > total) break
    if (backoff > 0) await delay(backoff)
    probe = await probeCoordinatorHealth(manifest, { timeout: budget, fetch: options?.fetch })
    attempt += 1
    if (!isAmbiguousCoordinatorProbe(probe)) break
  }

  return { probe, attempts: attempt, elapsedMs: Date.now() - started }
}

function positiveInteger(value: string | undefined) {
  if (value === undefined) return undefined
  const parsed = Number(value)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined
}

/**
 * Probes `/global/health`. Returns `undefined` when the coordinator is
 * unreachable or answers with something unusable, which is the only signal a
 * caller needs to treat the manifest as dead. The parsed body carries the
 * server version, so this is also the handshake's second source of truth.
 *
 * Kept as-is for callers that only want the boolean; it discards the
 * classification, so anything deciding whether to replace a coordinator should
 * use `probeCoordinatorHealthWithRetry` instead.
 */
export async function fetchCoordinatorHealth(
  manifest: CoordinatorEndpoint,
  options?: CoordinatorProbeOptions,
): Promise<CoordinatorHealth | undefined> {
  const probe = await probeCoordinatorHealth(manifest, options)
  if (probe.kind !== "healthy" && probe.kind !== "unhealthy") return undefined
  return {
    healthy: probe.kind === "healthy",
    version: probe.version,
    active: probe.active,
    coordinatorKey: probe.coordinatorKey,
  }
}

export async function isCoordinatorHealthy(manifest: CoordinatorEndpoint, options?: CoordinatorProbeOptions) {
  return (await fetchCoordinatorHealth(manifest, options))?.healthy === true
}

export function isCoordinatorHealthForManifest(manifest: Pick<CoordinatorManifest, "key">, health: CoordinatorHealth) {
  return health.healthy && health.coordinatorKey === manifest.key
}

/**
 * Whether a healthy probe came from the coordinator this manifest describes.
 *
 * The identity check and the retry classification have to be read together: a
 * probe that answers promptly and cheerfully still must not be attached to when
 * it is speaking for a *different* database, because that is exactly how a
 * second writer arrives on someone else's file.
 *
 * A coordinator predating the identity field reports no `coordinatorKey` at
 * all. That absence is tolerated deliberately — rejecting it would route this
 * client to a different database while the old one keeps its own, splitting
 * sessions across two invisible halves. Only a key that actively disagrees is
 * evidence of another database.
 */
export function isCoordinatorProbeForManifest(manifest: Pick<CoordinatorManifest, "key">, probe: CoordinatorProbe) {
  if (probe.kind !== "healthy") return false
  return probe.coordinatorKey === undefined || probe.coordinatorKey === manifest.key
}

/**
 * The retrying counterpart of `isCoordinatorHealthy`, for callers that act
 * destructively on a `false` — notably the GUI's cached-connection health
 * check, where one flaky probe used to silently stop a healthy coordinator and
 * respawn it mid-session.
 */
export async function isCoordinatorHealthyWithRetry(
  manifest: CoordinatorEndpoint,
  options?: CoordinatorProbeRetryOptions,
) {
  return (await probeCoordinatorHealthWithRetry(manifest, options)).probe.kind === "healthy"
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

/**
 * What a client should do about the manifest it just read.
 *
 * There is deliberately no `kill` variant, and there never should be: a client
 * cannot tell a wedged coordinator from a busy one, so the only safe outcomes
 * are attach, reclaim a manifest whose owner is provably gone, or refuse and
 * tell the human. Making that a property of the type means a future caller
 * cannot quietly grow a fourth behaviour.
 */
export type CoordinatorAttachment =
  | { action: "attach"; manifest: CoordinatorManifest; warning?: string }
  /** The manifest is stale and its owner cannot be harmed by deleting it. */
  | {
      action: "reclaim"
      reason: "key_mismatch" | "database_mismatch" | "identity_mismatch" | "process_dead"
      manifest: CoordinatorManifest
    }
  /**
   * The live process is a coordinator this client cannot verify it may use.
   * `reason` is carried through because the GUI offers a human override for a
   * plain version mismatch and for nothing else.
   */
  | {
      action: "refuse"
      code: "version"
      reason: CoordinatorCompatibilityReason
      message: string
      manifest: CoordinatorManifest
      probe: CoordinatorProbeResult
    }
  /** A live process holds the database and this client must not touch it. */
  | {
      action: "refuse"
      code: "identity" | "unverifiable" | "foreign"
      message: string
      manifest: CoordinatorManifest
      probe: CoordinatorProbeResult
    }

export type CoordinatorProber = (
  manifest: CoordinatorEndpoint,
  mode: CoordinatorProbeMode,
) => Promise<CoordinatorProbeResult>

/**
 * The one implementation of the attach decision, shared by the TUI registry and
 * the Electron main process so the two cannot drift apart again.
 *
 * Deliberately free of side effects: no filesystem writes, no signals, no
 * logging. `clientVersion` is injected rather than imported because this module
 * is bundled into Electron main by esbuild, which rejects any non-builtin
 * import. Callers apply whatever the returned action implies.
 */
export async function resolveCoordinatorAttachment(input: {
  manifest: CoordinatorManifest
  key: string
  database: string
  clientVersion: string
  mode: CoordinatorProbeMode
  probe: CoordinatorProber
  /** Canonicalises a database path; the GUI supplies one bound to its data root. */
  identity?: (database: string) => string
  /** Liveness check, injected so this stays testable and signal-free by default. */
  processAlive?: (pid: number) => boolean
  skipVersionCheck?: boolean
  /** How this client names a coordinator process in a refusal ("TUI coordinator"). */
  label?: string
}): Promise<CoordinatorAttachment> {
  const { manifest } = input
  const identity = input.identity ?? ((database: string) => coordinatorDatabaseIdentity(database))
  const label = input.label ?? "OpencodeX coordinator"
  if (manifest.key !== input.key) return { action: "reclaim", reason: "key_mismatch", manifest }
  if (identity(manifest.database) !== identity(input.database))
    return { action: "reclaim", reason: "database_mismatch", manifest }

  const probe = await input.probe(manifest, input.mode)
  if (probe.probe.kind === "healthy") {
    /* Retrying makes this client *more* willing to believe a slow coordinator
       is alive, so the identity gate has to sit in front of the version check:
       a prompt, cheerful answer from the wrong database is still the wrong
       database, and patience must never be what lets a second writer in. */
    if (!isCoordinatorProbeForManifest(manifest, probe.probe)) {
      if (!(input.processAlive ?? isCoordinatorProcessAlive)(manifest.pid))
        return { action: "reclaim", reason: "identity_mismatch", manifest }
      return {
        action: "refuse",
        code: "identity",
        message: `${label} process ${manifest.pid} answered for a different database; refusing to attach`,
        manifest,
        probe,
      }
    }
    const compatibility = checkCoordinatorCompatibility({
      manifest,
      clientVersion: input.clientVersion,
      healthVersion: probe.probe.version,
      skip: input.skipVersionCheck,
    })
    if (!compatibility.compatible)
      return {
        action: "refuse",
        code: "version",
        reason: compatibility.reason,
        message: compatibility.message ?? "Coordinator version mismatch",
        manifest,
        probe,
      }
    return {
      action: "attach",
      manifest,
      warning: compatibility.reason === "local" ? compatibility.message : undefined,
    }
  }

  const alive = (input.processAlive ?? isCoordinatorProcessAlive)(manifest.pid)
  if (!alive) return { action: "reclaim", reason: "process_dead", manifest }
  const foreign = probe.probe.kind === "body"
  return {
    action: "refuse",
    code: foreign ? "foreign" : "unverifiable",
    message: coordinatorUnreachableMessage(manifest, probe),
    manifest,
    probe,
  }
}

/**
 * Raised when a live process holds the database but will not confirm it is a
 * healthy coordinator this client can use. Carries the probe so callers can
 * report *why* rather than restating the refusal.
 */
export class CoordinatorUnreachableError extends Error {
  readonly manifest: CoordinatorManifest
  readonly probe: CoordinatorProbe
  readonly attempts: number
  readonly elapsedMs: number

  constructor(manifest: CoordinatorManifest, result: CoordinatorProbeResult, message?: string) {
    super(message ?? coordinatorUnreachableMessage(manifest, result))
    this.name = "CoordinatorUnreachableError"
    this.manifest = manifest
    this.probe = result.probe
    this.attempts = result.attempts
    this.elapsedMs = result.elapsedMs
  }
}

/** One clause naming what actually happened, for the first line of the refusal. */
export function describeCoordinatorProbe(probe: CoordinatorProbe) {
  if (probe.kind === "healthy") return "healthy"
  if (probe.kind === "unhealthy") return `a reply of healthy:false after ${probe.ms}ms`
  if (probe.kind === "timeout") return `timeout after ${probe.ms}ms`
  if (probe.kind === "refused") return `connection refused (${probe.code})`
  if (probe.kind === "http") return `HTTP ${probe.status}`
  if (probe.kind === "body") return "a reply that was not a coordinator health response"
  return `an unrecognised failure (${probe.detail})`
}

/**
 * The message a human sees when OpencodeX refuses to start. It has to answer
 * three questions the old one-line throw did not: what was actually tried, why
 * we will not simply replace the process, and what the human can do next.
 */
export function coordinatorUnreachableMessage(manifest: CoordinatorManifest, result: CoordinatorProbeResult) {
  const { probe, attempts, elapsedMs } = result
  const where = `Coordinator (pid ${manifest.pid}) at ${manifest.url}`
  const tried = `${attempts} attempt${attempts === 1 ? "" : "s"} over ${(elapsedMs / 1000).toFixed(1)}s, last outcome ${describeCoordinatorProbe(probe)}`
  const kill = `  • If you are certain it is wedged, stop it yourself, then re-run:\n      kill ${manifest.pid}`

  if (probe.kind === "body") {
    return [
      `${where} answered its health check with something that is not an OpencodeX`,
      `coordinator: ${tried}.`,
      "",
      "Something that is not an OpencodeX coordinator is listening on that port, so",
      "OpencodeX cannot attach to it — and it will not replace the process that the",
      "manifest points at.",
      "",
      "What to try:",
      "  • Check what owns that port and stop it, or move OpencodeX to another port.",
      kill,
    ].join("\n")
  }

  if (probe.kind === "http" && (probe.status === 401 || probe.status === 403)) {
    return [
      `${where} rejected this client's credentials: ${tried}.`,
      "",
      "The manifest's credentials no longer match the live process, so OpencodeX",
      "cannot verify it. The process is still running, so OpencodeX will not replace",
      "it — two processes writing one database is how that database gets corrupted.",
      "",
      "What to try:",
      kill,
    ].join("\n")
  }

  return [
    `${where} did not answer its health`,
    `check: ${tried}.`,
    "",
    "The process is still running, so OpencodeX will not replace it — two processes",
    "writing one database is how that database gets corrupted.",
    "",
    "It is most likely busy (large session, long model stream). What to try:",
    "  • Wait a few seconds and re-run.",
    "  • Raise the patience threshold:",
    `      ${COORDINATOR_HEALTH_TIMEOUT_ENV}=8000 opencodex -c`,
    kill,
  ].join("\n")
}

/** The prober both decide-mode call sites use: `quick` polls, `decide` escalates. */
export function createCoordinatorProber(options?: CoordinatorProbeRetryOptions): CoordinatorProber {
  return (manifest, mode) =>
    probeCoordinatorHealthWithRetry(manifest, mode === "quick" ? { ...options, attempts: 1 } : options)
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
