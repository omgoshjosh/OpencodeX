import { describe, expect, test } from "bun:test"
import {
  restartReadinessDefaults,
  waitForRestartReadiness,
  type RestartReadinessClient,
  type RestartReadinessProgress,
} from "../src/v2/restart-readiness"

describe("restart readiness", () => {
  test("defaults to ten samples at six-second intervals", () => {
    expect(restartReadinessDefaults).toEqual({ consecutiveSamples: 10, intervalMs: 6_000 })
  })

  test("resets the idle streak whenever authoritative work becomes active", async () => {
    const samples = [true, true, false, ...Array.from({ length: 10 }, () => true)]
    const progress: RestartReadinessProgress[] = []
    const result = await waitForRestartReadiness(client(samples), {
      intervalMs: 0,
      onSample: (sample) => progress.push(sample),
    })

    expect(result.ready).toBe(true)
    expect(progress.map((sample) => sample.consecutiveIdleSamples)).toEqual([1, 2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  })

  test("stops waiting when aborted between samples", async () => {
    const controller = new AbortController()
    const waiting = waitForRestartReadiness(client([true]), {
      consecutiveSamples: 2,
      intervalMs: 60_000,
      signal: controller.signal,
      onSample: () => controller.abort(new Error("cancelled")),
    })

    await expect(waiting).rejects.toThrow("cancelled")
  })

  test("fails closed when a readiness request has no data", async () => {
    const failure = new Error("unavailable")
    const client: RestartReadinessClient = {
      global: { restartReadiness: async () => ({ error: failure }) },
    }

    await expect(waitForRestartReadiness(client, { intervalMs: 0 })).rejects.toBe(failure)
  })
})

function client(samples: boolean[]): RestartReadinessClient {
  let index = 0
  return {
    global: {
      restartReadiness: async () => {
        const ready = samples[Math.min(index++, samples.length - 1)] ?? false
        return {
          data: {
            ready,
            checkedAt: index,
            blockers: {
              sessionExecutions: !ready,
              sessionCommands: false,
              sessionInteractions: false,
              jobs: false,
              swarms: false,
            },
          },
        }
      },
    },
  }
}
