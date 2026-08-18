import { describe, expect, test } from "bun:test"
import {
  releaseCoordinatorOwnerAfterStop,
  stopCoordinatorServices,
} from "../../../../src/cli/cmd/tui/coordinator-runner"

describe("coordinator shutdown", () => {
  test("bounds stalled dispose and server stop independently", async () => {
    const calls: string[] = []
    const errors: string[] = []
    const stalled = new Promise<never>(() => {})

    const result = await stopCoordinatorServices({
      dispose: () => {
        calls.push("dispose")
        return stalled
      },
      stop: () => {
        calls.push("server stop")
        return stalled
      },
      timeout: 5,
      onError: (step) => errors.push(step),
    })

    expect(result).toEqual({ dispose: false, stop: false })
    expect(
      await releaseCoordinatorOwnerAfterStop(result.stop, async () => {
        calls.push("owner release")
      }),
    ).toBe(false)
    expect(calls).toEqual(["dispose", "server stop"])
    expect(errors).toEqual(["dispose", "server stop"])
  })

  test("releases owner lock only after confirmed server stop", async () => {
    const calls: string[] = []
    expect(
      await releaseCoordinatorOwnerAfterStop(false, async () => {
        calls.push("release")
      }),
    ).toBe(false)
    expect(calls).toEqual([])

    expect(
      await releaseCoordinatorOwnerAfterStop(true, async () => {
        calls.push("release")
      }),
    ).toBe(true)
    expect(calls).toEqual(["release"])
  })
})
