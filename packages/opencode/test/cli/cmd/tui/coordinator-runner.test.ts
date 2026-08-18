import { describe, expect, test } from "bun:test"
import { stopCoordinatorServices } from "../../../../src/cli/cmd/tui/coordinator-runner"

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
    expect(calls).toEqual(["dispose", "server stop"])
    expect(errors).toEqual(["dispose", "server stop"])
  })
})
