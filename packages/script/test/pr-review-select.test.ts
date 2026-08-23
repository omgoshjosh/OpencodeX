import { describe, expect, test } from "bun:test"
import { existsSync, readFileSync } from "node:fs"
import path from "node:path"
import {
  decidePullRequest,
  flattenPages,
  mapConcurrent,
  normalizeCheckStatus,
  NO_CI_GRACE_MS,
  type PullRequestSnapshot,
} from "../src/pr-review-select.js"

const snapshot = (overrides: Partial<PullRequestSnapshot> = {}): PullRequestSnapshot => ({
  number: 25,
  title: "test PR",
  authorLogin: "author",
  isDraft: false,
  headRefOid: "efa8c2ad2cc604ee64195c4acb5091d24ead7342",
  headCommittedAt: "2026-08-19T10:00:00Z",
  checks: [{ name: "unit", status: "COMPLETED" }],
  ...overrides,
})

describe("read-only PR selection", () => {
  test("selects an eligible PR for reporting", () => {
    expect(decidePullRequest(snapshot(), new Date("2026-08-20T00:00:00Z")).action).toBe("review")
  })

  test("returns an empty report set when every PR is deferred or draft", () => {
    expect(decidePullRequest(snapshot({ isDraft: true }), new Date()).action).toBe("skip")
    expect(decidePullRequest(snapshot({ checks: [{ name: "unit", status: "IN_PROGRESS" }] }), new Date()).action).toBe(
      "defer",
    )
  })

  test("retains every paginated record", () => {
    expect(flattenPages(Array.from({ length: 101 }, () => [1]))).toHaveLength(101)
  })

  test("normalizes classic pending and terminal status contexts", () => {
    expect(normalizeCheckStatus(undefined, "PENDING")).toBe("IN_PROGRESS")
    expect(normalizeCheckStatus(undefined, "SUCCESS")).toBe("COMPLETED")
    expect(normalizeCheckStatus("QUEUED", undefined)).toBe("QUEUED")
  })

  test("defers invalid and fresh no-CI head timestamps", () => {
    const now = new Date("2026-08-20T00:00:00Z")
    expect(decidePullRequest(snapshot({ checks: [], headCommittedAt: "invalid" }), now).reason).toBe(
      "invalid head commit timestamp",
    )
    expect(
      decidePullRequest(
        snapshot({ checks: [], headCommittedAt: new Date(now.getTime() - NO_CI_GRACE_MS + 1).toISOString() }),
        now,
      ).action,
    ).toBe("defer")
    expect(
      decidePullRequest(
        snapshot({ checks: [], headCommittedAt: new Date(now.getTime() - NO_CI_GRACE_MS).toISOString() }),
        now,
      ).action,
    ).toBe("review")
  })

  test("bounds API fan-out", async () => {
    let active = 0
    let peak = 0
    await mapConcurrent(Array.from({ length: 9 }), 4, async () => {
      active += 1
      peak = Math.max(peak, active)
      await new Promise((resolve) => setTimeout(resolve, 1))
      active -= 1
    })
    expect(peak).toBe(4)
  })

  test("keeps the shipped reporting surface free of GitHub writes and workflows", () => {
    const root = path.join(import.meta.dir, "../../..")
    expect(existsSync(path.join(root, ".github/workflows/review-open-prs.yml"))).toBe(false)
    const selector = readFileSync(path.join(root, "packages/script/src/pr-review-select-cli.ts"), "utf8")
    expect(selector).not.toContain("gh pr review")
    expect(selector).not.toContain('method: "POST"')
  })
})
