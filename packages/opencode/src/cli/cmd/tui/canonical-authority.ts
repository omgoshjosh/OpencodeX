import { Global } from "@opencode-ai/core/global"
import fs from "fs/promises"
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
 */

export const CANONICAL_AUTHORITY_ENV = "OPENCODE_CANONICAL_AUTHORITY"
export const CANONICAL_PORT = 4096

export type CanonicalAuthorityMarker = {
  pid: number
  hostname: string
  port: number
  since: string
}

export type CanonicalAuthorityGuard =
  | { engaged: false }
  | { engaged: true; path: string; pid?: number; corrupt: boolean }

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
 * Missing marker => guard off. Anything else (readable, corrupt, or unreadable
 * for any reason other than ENOENT) => guard on: the safe failure is to leave
 * :4096 alone.
 */
export async function readCanonicalAuthorityGuard(): Promise<CanonicalAuthorityGuard> {
  const file = canonicalAuthorityMarkerPath()
  const raw = await fs.readFile(file, "utf8").then(
    (text) => ({ text }),
    (error: NodeJS.ErrnoException) => ({ missing: error.code === "ENOENT" }),
  )
  if ("missing" in raw) return raw.missing ? { engaged: false } : { engaged: true, path: file, corrupt: true }
  try {
    const parsed: unknown = JSON.parse(raw.text)
    const pid = typeof parsed === "object" && parsed !== null ? (parsed as { pid?: unknown }).pid : undefined
    if (typeof pid !== "number") return { engaged: true, path: file, corrupt: true }
    return { engaged: true, path: file, pid, corrupt: false }
  } catch {
    return { engaged: true, path: file, corrupt: true }
  }
}
