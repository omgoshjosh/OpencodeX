import { describe, expect, test } from "bun:test"
import { isLive } from "../../src/session/activity"

const running = {
  version: 2 as const,
  runID: "run",
  parentSessionID: "ses_parent",
  attempt: 1,
  phase: "running" as const,
  startedAt: 0,
}

describe("session activity", () => {
  test("recognizes only durable live work", () => {
    expect(isLive({ execution: "running", runID: "run" })).toBe(true)
    expect(isLive({ execution: "queued", runID: "run" })).toBe(true)
    expect(isLive({ interaction: "pending", runID: "run" })).toBe(true)
    expect(isLive({ delegation: { ...running, ownerID: "remote-owner" }, runID: "run" })).toBe(true)
    expect(isLive({ delegation: { ...running, phase: "monitoring", ownerID: "remote-owner" }, runID: "run" })).toBe(true)
    expect(isLive({ delegation: { ...running, ownerID: "local:999999:other:child" }, runID: "run" })).toBe(false)
    expect(isLive({ delegation: running, runID: "run" })).toBe(false)
    expect(isLive({ execution: "idle", interaction: "replied", runID: "run" })).toBe(false)
  })
})
