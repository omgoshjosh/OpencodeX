import { describe, expect, test } from "bun:test"
import { decidePullRequest, flattenPages, mapConcurrent, type PullRequestSnapshot } from "../src/pr-review-select.js"

const snapshot = (overrides: Partial<PullRequestSnapshot> = {}): PullRequestSnapshot => ({
  number: 25,
  title: "test PR",
  authorLogin: "author",
  isDraft: false,
  headRefOid: "efa8c2ad2cc604ee64195c4acb5091d24ead7342",
  headCommittedAt: "2026-08-19T10:00:00Z",
  reviews: [],
  comments: [],
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
})
