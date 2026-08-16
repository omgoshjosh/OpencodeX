import type { GlobalRestartReadinessResponse } from "./gen/types.gen.js"

export const restartReadinessDefaults = {
  consecutiveSamples: 10,
  intervalMs: 6_000,
} as const

export type RestartReadinessClient = {
  global: {
    restartReadiness(options?: { signal?: AbortSignal }): Promise<{
      data?: GlobalRestartReadinessResponse
      error?: unknown
    }>
  }
}

export type RestartReadinessProgress = {
  sample: GlobalRestartReadinessResponse
  consecutiveIdleSamples: number
  requiredIdleSamples: number
}

export async function waitForRestartReadiness(
  client: RestartReadinessClient,
  options: {
    consecutiveSamples?: number
    intervalMs?: number
    signal?: AbortSignal
    onSample?: (progress: RestartReadinessProgress) => void
  } = {},
) {
  const required = options.consecutiveSamples ?? restartReadinessDefaults.consecutiveSamples
  const interval = options.intervalMs ?? restartReadinessDefaults.intervalMs
  if (!Number.isInteger(required) || required < 1) throw new Error("consecutiveSamples must be a positive integer")
  if (!Number.isFinite(interval) || interval < 0) throw new Error("intervalMs must be a non-negative number")

  let consecutive = 0
  while (true) {
    options.signal?.throwIfAborted()
    const result = await client.global.restartReadiness({ signal: options.signal })
    if (!result.data) throw result.error instanceof Error ? result.error : new Error("Restart readiness check failed")

    consecutive = result.data.ready ? consecutive + 1 : 0
    options.onSample?.({
      sample: result.data,
      consecutiveIdleSamples: consecutive,
      requiredIdleSamples: required,
    })
    if (consecutive >= required) return result.data
    await delay(interval, options.signal)
  }
}

function delay(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const done = () => {
      signal?.removeEventListener("abort", abort)
      resolve()
    }
    const abort = () => {
      clearTimeout(timer)
      reject(signal?.reason ?? new DOMException("The operation was aborted", "AbortError"))
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener("abort", abort, { once: true })
    if (signal?.aborted) abort()
  })
}
