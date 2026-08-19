import { describe, expect, test } from "bun:test"
import {
  initializeCoordinatorProcess,
  releaseCoordinatorOwnerAfterStop,
  stopCoordinatorServices,
} from "../../../../src/cli/cmd/tui/coordinator-runner"
import { CoordinatorHandoff } from "../../../../src/server/coordinator-handoff"
import { CoordinatorAuthority } from "../../../../src/server/coordinator-authority"
import { ServerAuth } from "../../../../src/server/auth"
import { spawnSync } from "node:child_process"

describe("coordinator shutdown", () => {
  test("initializes ordinary bootstrap credentials and disables inherited handoff control", () => {
    process.env.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY = "inherited-capability-that-must-not-be-used"
    process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = "inherited-epoch-that-must-not-be-used"
    const bootstrap = {
      version: 1 as const,
      username: "coordinator",
      password: "runner-password-0000000000000000000001",
      token: "runner-token-0000000000000000000000001",
    }

    try {
      expect(initializeCoordinatorProcess(bootstrap)).toEqual(bootstrap)
      expect(ServerAuth.headers()).toEqual({
        Authorization: `Basic ${Buffer.from(`${bootstrap.username}:${bootstrap.password}`).toString("base64")}`,
      })
      expect(CoordinatorHandoff.available()).toBe(false)
      expect(process.env.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY).toBeUndefined()
      expect(process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH).toBeUndefined()
      expect(process.env.OPENCODE_SERVER_PASSWORD).toBeUndefined()
      expect(process.env.OPENCODE_TUI_COORDINATOR_TOKEN).toBeUndefined()
      const child = spawnSync(process.execPath, ["-e", "console.log(JSON.stringify(process.env))"], {
        env: process.env,
        encoding: "utf8",
      })
      const inherited = JSON.parse(child.stdout) as Record<string, string>
      expect(inherited.OPENCODE_COORDINATOR_HANDOFF_CAPABILITY).toBeUndefined()
      expect(inherited.OPENCODE_COORDINATOR_AUTHORITY_EPOCH).toBeUndefined()
      expect(inherited.OPENCODE_SERVER_PASSWORD).toBeUndefined()
      expect(inherited.OPENCODE_TUI_COORDINATOR_TOKEN).toBeUndefined()
    } finally {
      ServerAuth.resetForTest()
      CoordinatorAuthority.resetForTest()
      CoordinatorHandoff.resetForTest()
    }
  })

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
