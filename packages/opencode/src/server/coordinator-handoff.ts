import { CoordinatorAuthority } from "@/server/coordinator-authority"
import {
  compareAndSwapCoordinatorHandoff,
  coordinatorKey,
  inspectCoordinatorHandoff,
  proveCoordinatorSourceWithoutHandoff,
  type TuiCoordinatorHandoffRecord,
  type TuiCoordinatorHandoffMatch,
} from "@/cli/cmd/tui/coordinator-registry"
import { createHash, timingSafeEqual } from "node:crypto"
import { coordinatorHandoffRequestID } from "@opencode-ai/sdk/coordinator"

export const CAPABILITY_HEADER = "x-opencode-handoff-capability"
const CAPABILITY_ENV = "OPENCODE_COORDINATOR_HANDOFF_CAPABILITY"
const TIMEOUT_ENV = "OPENCODE_COORDINATOR_HANDOFF_DRAIN_TIMEOUT_MS"
const DEFAULT_TIMEOUT_MS = 5_000
const testOverride: { key?: string; stateRoot?: string } = {}

export class TransitionConflict extends Error {}
export class TransitionUnavailable extends Error {}

export function authorized(capability: string | undefined) {
  const secret = process.env[CAPABILITY_ENV]
  if (!secret || secret.length < 32 || secret.length > 256 || !capability || capability.length > 256) return false
  return timingSafeEqual(digest(secret), digest(capability))
}

export function available() {
  const secret = process.env[CAPABILITY_ENV]
  return secret !== undefined && secret.length >= 32 && secret.length <= 256
}

export function request(input: { request: string; targetEpoch: string; signal?: AbortSignal }) {
  const sourceEpoch = requireSourceEpoch()
  if (input.request !== coordinatorHandoffRequestID(sourceEpoch, input.targetEpoch))
    return Promise.reject(new TransitionConflict("Coordinator handoff request does not match its authority epochs"))
  return CoordinatorAuthority.serialized(async () => {
    input.signal?.throwIfAborted()
    if (requireSourceEpoch() !== sourceEpoch) throw new TransitionConflict("Coordinator source authority changed")
    const key = testOverride.key ?? coordinatorKey()
    const current = await inspectCoordinatorHandoff(key, testOverride.stateRoot)
    if (current) {
      if (current.request !== input.request || current.sourceEpoch !== sourceEpoch)
        throw new TransitionConflict("Coordinator handoff already exists")
      if (current.phase === "accepted") {
        if (current.targetEpoch !== input.targetEpoch)
          throw new TransitionConflict("Coordinator handoff target changed")
        void CoordinatorAuthority.close()
        return { phase: "accepted" as const }
      }
      if (current.phase !== "requested") throw new TransitionConflict("Coordinator handoff cannot be resumed")
      void CoordinatorAuthority.close()
      await waitForDrain(input.signal)
      return accept(key, current, input.targetEpoch)
    }

    CoordinatorAuthority.close()
    const now = new Date().toISOString()
    const requested = {
      version: 2 as const,
      request: input.request,
      phase: "requested" as const,
      revision: 0,
      sourceEpoch,
      createdAt: now,
      updatedAt: now,
    }
    const published = await compareAndSwapCoordinatorHandoff(key, undefined, requested, testOverride.stateRoot).catch(
      async (error) => {
        if (await proveCoordinatorSourceWithoutHandoff(key, sourceEpoch, testOverride.stateRoot))
          CoordinatorAuthority.reopen()
        throw error
      },
    )
    if (!published) {
      if (await proveCoordinatorSourceWithoutHandoff(key, sourceEpoch, testOverride.stateRoot))
        CoordinatorAuthority.reopen()
      throw new TransitionConflict("Coordinator handoff changed")
    }
    await waitForDrain(input.signal)
    return accept(key, requested, input.targetEpoch)
  })
}

export function abort(input: { expected: TuiCoordinatorHandoffMatch; signal?: AbortSignal }) {
  return CoordinatorAuthority.serialized(async () => {
    input.signal?.throwIfAborted()
    const sourceEpoch = requireSourceEpoch()
    const key = testOverride.key ?? coordinatorKey()
    const current = await inspectCoordinatorHandoff(key, testOverride.stateRoot)
    if (!current || !matches(current, input.expected) || current.sourceEpoch !== sourceEpoch)
      throw new TransitionConflict("Coordinator handoff changed")
    if (current.phase !== "requested" && current.phase !== "accepted")
      throw new TransitionConflict("Coordinator handoff cannot be aborted")
    void CoordinatorAuthority.close()
    const deleted = await compareAndSwapCoordinatorHandoff(key, input.expected, undefined, testOverride.stateRoot)
    if (!deleted) throw new TransitionConflict("Coordinator handoff changed")
    if (!(await proveCoordinatorSourceWithoutHandoff(key, sourceEpoch, testOverride.stateRoot)))
      throw new TransitionConflict("Coordinator source authority changed")
    CoordinatorAuthority.reopen()
    return { phase: "aborted" as const }
  })
}

async function accept(key: string, requested: TuiCoordinatorHandoffRecord, targetEpoch: string) {
  const accepted = {
    version: 2 as const,
    request: requested.request,
    phase: "accepted" as const,
    revision: requested.revision + 1,
    sourceEpoch: requested.sourceEpoch,
    targetEpoch,
    createdAt: requested.createdAt,
    updatedAt: strictlyLaterTimestamp(requested.updatedAt),
  }
  if (!(await compareAndSwapCoordinatorHandoff(key, requested, accepted, testOverride.stateRoot)))
    throw new TransitionConflict("Coordinator handoff changed before acceptance")
  return { phase: "accepted" as const }
}

function matches(current: TuiCoordinatorHandoffMatch, expected: TuiCoordinatorHandoffMatch) {
  return (
    current.request === expected.request &&
    current.phase === expected.phase &&
    current.revision === expected.revision &&
    current.sourceEpoch === expected.sourceEpoch &&
    current.targetEpoch === expected.targetEpoch
  )
}

function waitForDrain(signal?: AbortSignal) {
  const configured = Number(process.env[TIMEOUT_ENV])
  const timeoutMs =
    Number.isInteger(configured) && configured >= 10 && configured <= 30_000 ? configured : DEFAULT_TIMEOUT_MS
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason instanceof Error ? signal.reason : new Error("Coordinator handoff interrupted"))
      return
    }
    const timeout = setTimeout(() => finish(new TransitionConflict("Coordinator handoff drain timed out")), timeoutMs)
    const aborted = () =>
      finish(signal?.reason instanceof Error ? signal.reason : new Error("Coordinator handoff interrupted"))
    const finish = (error?: Error) => {
      clearTimeout(timeout)
      signal?.removeEventListener("abort", aborted)
      if (error) reject(error)
      else resolve()
    }
    signal?.addEventListener("abort", aborted, { once: true })
    CoordinatorAuthority.drained().then(
      () => finish(),
      (error) => finish(error instanceof Error ? error : new Error(String(error))),
    )
  })
}

function requireSourceEpoch() {
  const epoch = process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH
  if (!CoordinatorAuthority.enabled() || !epoch) throw new TransitionUnavailable("Coordinator handoff is unavailable")
  return epoch
}

function digest(value: string) {
  return createHash("sha256").update(value).digest()
}

export function strictlyLaterTimestamp(previous: string, now = Date.now()) {
  return new Date(Math.max(now, Date.parse(previous) + 1)).toISOString()
}

export function overrideForTest(input?: { key: string; stateRoot: string }) {
  testOverride.key = input?.key
  testOverride.stateRoot = input?.stateRoot
}

export * as CoordinatorHandoff from "./coordinator-handoff"
