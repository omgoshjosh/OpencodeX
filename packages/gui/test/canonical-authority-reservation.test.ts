import { describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { startFallbackCoordinator } from "@opencode-ai/sdk/coordinator"

describe("GUI canonical authority reservation", () => {
  test("waits instead of spawning a sidecar while canonical serve is reserved", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencodex-gui-canonical-"))
    const key = "canonical-authority"
    try {
      await Bun.write(path.join(root, "tui-coordinators", `${key}.canonical.json`), "{")
      let spawned = false
      expect(
        await startFallbackCoordinator({
          stateRoot: root,
          key,
          spawn: async () => {
            spawned = true
            return "spawned"
          },
          wait: async () => "attached",
        }),
      ).toBe("attached")
      expect(spawned).toBe(false)
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  })

  test("keeps sidecar fallback behavior when no canonical serve is reserved", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencodex-gui-canonical-"))
    try {
      expect(
        await startFallbackCoordinator({
          stateRoot: root,
          key: "unreserved",
          spawn: async () => "spawned",
          wait: async () => "attached",
        }),
      ).toBe("spawned")
    } finally {
      await fs.rm(root, { recursive: true, force: true })
    }
  })
})
