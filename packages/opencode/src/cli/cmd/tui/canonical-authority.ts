import { Global } from "@opencode-ai/core/global"
import * as Log from "@opencode-ai/core/util/log"
import fs from "fs/promises"
import net from "net"
import path from "path"

/*
 * Canonical authority marker (OpencodeX-1d2 layer 2).
 *
 * The launchd daemon (`opencodex serve` under OPENCODE_CANONICAL_AUTHORITY=1)
 * owns 127.0.0.1:4096. During a daemon cutover the coordinator manifest is
 * momentarily absent, and the GUI's embedded worker (port 0 => "prefer 4096")
 * would otherwise grab the socket, after which the new daemon refuses to start.
 *
 * The marker persists on purpose: it is written on every canonical start and
 * never removed on shutdown, so the restart window is covered too. Its only
 * reader is the embedded worker, which stops preferring :4096 while it exists.
 *
 * Staleness: a marker whose daemon is gone for good must not pin the worker
 * off :4096 forever, so the reader reclaims (deletes) it when ALL of these hold:
 *   (a) the recorded pid is not alive,
 *   (b) nothing is listening on the recorded hostname:port, and
 *   (c) the marker is older than the grace window (default 15 min).
 * The grace window is what keeps the guard engaged during a restart/cutover,
 * when the pid is dead and the port is free for seconds to a couple of minutes
 * — exactly the race the marker exists to close. A zero-grace rule would re-open
 * it. Corrupt markers stay fail-safe (engaged) but get the same grace test on
 * file mtime, so a wedged file cannot hold the guard indefinitely either.
 */

export const CANONICAL_AUTHORITY_ENV = "OPENCODE_CANONICAL_AUTHORITY"
export const CANONICAL_AUTHORITY_GRACE_ENV = "OPENCODE_CANONICAL_AUTHORITY_GRACE_MS"
export const CANONICAL_PORT = 4096
export const DEFAULT_GRACE_MS = 15 * 60 * 1000
const PORT_PROBE_TIMEOUT_MS = 300

export type CanonicalAuthorityMarker = {
  pid: number
  hostname: string
  port: number
  since: string
  /** Optional per-marker override of the stale grace window. */
  graceMs?: number
}

export type CanonicalAuthorityGuardReason = "pid-alive" | "port-listening" | "within-grace" | "corrupt-fresh"

export type CanonicalAuthorityGuard =
  | { engaged: false }
  | {
      engaged: true
      path: string
      pid?: number
      port?: number
      since?: string
      /** Age of the marker (from `since`, falling back to file mtime). */
      ageMs: number
      corrupt: boolean
      reason: CanonicalAuthorityGuardReason
    }

/** Where the marker lives; the cutover script pre-seeds this exact file. */
export function canonicalAuthorityMarkerPath() {
  return path.join(Global.Path.data, "canonical-authority.json")
}

export function isCanonicalAuthority(env: NodeJS.ProcessEnv = process.env) {
  const value = env[CANONICAL_AUTHORITY_ENV]?.trim().toLowerCase()
  return value === "1" || value === "true"
}

export async function writeCanonicalAuthorityMarker(input: { pid: number; hostname: string; port: number }) {
  const file = canonicalAuthorityMarkerPath()
  const marker: CanonicalAuthorityMarker = { ...input, since: new Date().toISOString() }
  await fs.mkdir(path.dirname(file), { recursive: true })
  const tmp = `${file}.${input.pid}.tmp`
  await fs.writeFile(tmp, JSON.stringify(marker, null, 2) + "\n", { mode: 0o600 })
  await fs.rename(tmp, file)
  return marker
}

/**
 * Missing marker => guard off. Stale marker (see header) => reclaimed with a
 * WARN, guard off. Anything else — live, in grace, corrupt, or unreadable for
 * any reason other than ENOENT — => guard on: the safe failure is to leave
 * :4096 alone.
 */
export async function readCanonicalAuthorityGuard(
  env: NodeJS.ProcessEnv = process.env,
): Promise<CanonicalAuthorityGuard> {
  const file = canonicalAuthorityMarkerPath()
  const raw = await fs.readFile(file, "utf8").then(
    (text) => ({ text }),
    (error: NodeJS.ErrnoException) => ({ missing: error.code === "ENOENT" }),
  )
  if ("missing" in raw) {
    if (raw.missing) return { engaged: false }
    return { engaged: true, path: file, ageMs: 0, corrupt: true, reason: "corrupt-fresh" }
  }
  const mtimeAge = await fs.stat(file).then(
    (stat) => Date.now() - stat.mtimeMs,
    () => 0,
  )
  const parsed = parseMarker(raw.text)
  if (!parsed) {
    if (mtimeAge > graceWindow(undefined, env)) {
      await reclaim({ path: file, ageMs: mtimeAge, reason: "corrupt-stale" })
      return { engaged: false }
    }
    return { engaged: true, path: file, ageMs: mtimeAge, corrupt: true, reason: "corrupt-fresh" }
  }
  const sinceMs = Date.parse(parsed.since ?? "")
  const ageMs = Number.isFinite(sinceMs) ? Date.now() - sinceMs : mtimeAge
  const base = { path: file, pid: parsed.pid, port: parsed.port, since: parsed.since, ageMs, corrupt: false } as const
  if (pidAlive(parsed.pid)) return { engaged: true, ...base, reason: "pid-alive" }
  if (await portListening(parsed.hostname ?? "127.0.0.1", parsed.port)) {
    return { engaged: true, ...base, reason: "port-listening" }
  }
  if (ageMs <= graceWindow(parsed.graceMs, env)) return { engaged: true, ...base, reason: "within-grace" }
  await reclaim({ path: file, pid: parsed.pid, port: parsed.port, ageMs, reason: "stale" })
  return { engaged: false }
}

type ParsedMarker = { pid: number; hostname?: string; port?: number; since?: string; graceMs?: number }

function parseMarker(text: string): ParsedMarker | undefined {
  try {
    const record: unknown = JSON.parse(text)
    if (!isRecord(record) || typeof record.pid !== "number") return undefined
    return {
      pid: record.pid,
      hostname: typeof record.hostname === "string" ? record.hostname : undefined,
      port: typeof record.port === "number" ? record.port : undefined,
      since: typeof record.since === "string" ? record.since : undefined,
      graceMs: typeof record.graceMs === "number" ? record.graceMs : undefined,
    }
  } catch {
    return undefined
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

/** Env overrides the marker field, which overrides the default. */
function graceWindow(markerGraceMs: number | undefined, env: NodeJS.ProcessEnv) {
  const fromEnv = Number(env[CANONICAL_AUTHORITY_GRACE_ENV])
  if (env[CANONICAL_AUTHORITY_GRACE_ENV]?.trim() && Number.isFinite(fromEnv) && fromEnv >= 0) return fromEnv
  if (markerGraceMs !== undefined && Number.isFinite(markerGraceMs) && markerGraceMs >= 0) return markerGraceMs
  return DEFAULT_GRACE_MS
}

/** `kill(pid, 0)` semantics: EPERM means the process exists but is not ours. */
function pidAlive(pid: number) {
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return isRecord(error) && error.code === "EPERM"
  }
}

/** Quick TCP connect; refused, unreachable or timed out all count as "not listening". */
function portListening(hostname: string, port: number | undefined) {
  if (port === undefined || !Number.isInteger(port) || port <= 0 || port > 65535) return Promise.resolve(false)
  return new Promise<boolean>((resolve) => {
    const socket = net.connect({ host: hostname, port })
    const done = (listening: boolean) => {
      socket.destroy()
      resolve(listening)
    }
    socket.setTimeout(PORT_PROBE_TIMEOUT_MS, () => done(false))
    socket.once("connect", () => done(true))
    socket.once("error", () => done(false))
  })
}

async function reclaim(info: {
  path: string
  pid?: number
  port?: number
  ageMs: number
  reason: "stale" | "corrupt-stale"
}) {
  await fs.rm(info.path, { force: true })
  Log.Default.warn("canonical authority marker stale; reclaimed", info)
}
