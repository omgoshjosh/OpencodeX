import {
  COORDINATOR_MANIFEST_VERSION,
  coordinatorHandoffRequestID,
  isCoordinatorKey,
  type CoordinatorSourceManifestFence,
} from "./manifest.js"

export const COORDINATOR_SUPERVISOR_INTENT_VERSION = 1
const MAX_INTENT_BYTES = 16_384
const SOURCE_KEYS = ["version", "key", "pid", "createdAt", "serverVersion", "authorityEpoch"] as const
const INTENT_KEYS = [
  "version",
  "key",
  "source",
  "targetEpoch",
  "request",
  "targetBuildID",
  "revision",
  "createdAt",
  "updatedAt",
] as const

export type CoordinatorSupervisorSourceFence = CoordinatorSourceManifestFence & {
  serverVersion: string
  authorityEpoch: string
}

export type CoordinatorSupervisorIntent = {
  version: typeof COORDINATOR_SUPERVISOR_INTENT_VERSION
  key: string
  source: CoordinatorSupervisorSourceFence
  targetEpoch: string
  request: string
  targetBuildID: string
  revision: number
  createdAt: string
  updatedAt: string
}

export function isCoordinatorSupervisorBuildID(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value !== "." &&
    value !== ".." &&
    !value.includes("/") &&
    !value.includes("\\") &&
    /^[A-Za-z0-9][A-Za-z0-9._:+@-]{0,255}$/.test(value)
  )
}

export function isCoordinatorSupervisorIntent(value: unknown): value is CoordinatorSupervisorIntent {
  if (!strictObject(value, INTENT_KEYS)) return false
  const intent = value as Partial<CoordinatorSupervisorIntent>
  return (
    intent.version === COORDINATOR_SUPERVISOR_INTENT_VERSION &&
    isCoordinatorKey(intent.key ?? "") &&
    isSourceFence(intent.source) &&
    intent.source.key === intent.key &&
    safeIdentifier(intent.targetEpoch, 16, 256) &&
    intent.targetEpoch !== intent.source.authorityEpoch &&
    /^[0-9a-f]{64}$/.test(intent.request ?? "") &&
    intent.request === coordinatorHandoffRequestID(intent.source.authorityEpoch, intent.targetEpoch) &&
    isCoordinatorSupervisorBuildID(intent.targetBuildID) &&
    Number.isSafeInteger(intent.revision) &&
    (intent.revision ?? -1) >= 0 &&
    timestamp(intent.createdAt) &&
    timestamp(intent.updatedAt) &&
    Date.parse(intent.updatedAt) >= Date.parse(intent.createdAt)
  )
}

export function parseCoordinatorSupervisorIntent(raw: string) {
  if (Buffer.byteLength(raw) > MAX_INTENT_BYTES) throw new Error("Coordinator supervisor intent exceeds maximum size")
  const value = JSON.parse(raw) as unknown
  if (!isCoordinatorSupervisorIntent(value)) throw new Error("Invalid coordinator supervisor intent")
  return value
}

export function serializeCoordinatorSupervisorIntent(value: CoordinatorSupervisorIntent) {
  if (!isCoordinatorSupervisorIntent(value)) throw new Error("Invalid coordinator supervisor intent")
  const serialized = JSON.stringify(value)
  if (Buffer.byteLength(serialized) > MAX_INTENT_BYTES) {
    throw new Error("Coordinator supervisor intent exceeds maximum size")
  }
  return serialized
}

function isSourceFence(value: unknown): value is CoordinatorSupervisorSourceFence {
  if (!strictObject(value, SOURCE_KEYS)) return false
  const source = value as Partial<CoordinatorSupervisorSourceFence>
  return (
    source.version === COORDINATOR_MANIFEST_VERSION &&
    isCoordinatorKey(source.key ?? "") &&
    Number.isSafeInteger(source.pid) &&
    (source.pid ?? 0) > 0 &&
    timestamp(source.createdAt) &&
    isCoordinatorSupervisorBuildID(source.serverVersion) &&
    safeIdentifier(source.authorityEpoch, 1, 256)
  )
}

function strictObject<const Keys extends readonly string[]>(value: unknown, keys: Keys) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const actual = Object.keys(value)
  return actual.length === keys.length && actual.every((key) => keys.includes(key))
}

function bounded(value: unknown, minimum: number, maximum: number): value is string {
  return typeof value === "string" && value.length >= minimum && value.length <= maximum && !value.includes("\0")
}

function safeIdentifier(value: unknown, minimum: number, maximum: number): value is string {
  return (
    bounded(value, minimum, maximum) && value !== "." && value !== ".." && /^[A-Za-z0-9][A-Za-z0-9._:+@-]*$/.test(value)
  )
}

function timestamp(value: unknown): value is string {
  if (typeof value !== "string") return false
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value
}
