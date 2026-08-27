import { describe, expect, test } from "bun:test"
import { existsSync, readFileSync } from "node:fs"
import path from "node:path"
import {
  decidePullRequest,
  flattenPages,
  mapConcurrent,
  normalizeCheckStatus,
  readRollupPage,
  reviewRepoParts,
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

  test("reads a null rollup as no checks rather than bad data", () => {
    // GitHub returns a null statusCheckRollup whenever the head commit has no
    // checks or statuses. Throwing here aborted the whole selection run
    // (mapConcurrent is Promise.all) whenever any one PR had no CI yet, and
    // made the no-CI grace window unreachable.
    expect(readRollupPage({ data: { repository: { pullRequest: { statusCheckRollup: null } } } })).toBeUndefined()
  })

  test("reads a populated rollup page with its cursor", () => {
    const page = readRollupPage({
      data: {
        repository: {
          pullRequest: {
            statusCheckRollup: {
              contexts: {
                nodes: [{ name: "unit", status: "COMPLETED" }],
                pageInfo: { hasNextPage: true, endCursor: "cursor-1" },
              },
            },
          },
        },
      },
    })
    expect(page).toEqual({ nodes: [{ name: "unit", status: "COMPLETED" }], hasNextPage: true, endCursor: "cursor-1" })
  })

  test("still rejects a genuinely malformed rollup", () => {
    expect(() => readRollupPage({ data: { repository: {} } })).toThrow()
    expect(() =>
      readRollupPage({ data: { repository: { pullRequest: { statusCheckRollup: { contexts: { nodes: "no" } } } } } }),
    ).toThrow()
  })

  test("drives both REST and GraphQL calls from one repository constant", () => {
    expect(reviewRepoParts("ecgreen/OpencodeX")).toEqual({ owner: "ecgreen", name: "OpencodeX" })
    expect(() => reviewRepoParts("OpencodeX")).toThrow()
  })

  test("holds the no-CI window open for a freshly opened old branch", () => {
    // A branch committed days ago and opened as a PR just now: keying the
    // window off the commit date alone reports it as reviewable while Actions
    // is still queuing its first job.
    const decision = decidePullRequest(
      snapshot({ checks: [], headCommittedAt: "2026-08-20T12:00:00Z", updatedAt: "2026-08-26T11:59:00Z" }),
      new Date("2026-08-26T12:00:00Z"),
    )
    expect(decision.action).toBe("defer")
    expect(decision.reason).toContain("not yet registered")
  })

  test("reports a draft's real CI state", () => {
    const decision = decidePullRequest(
      snapshot({ isDraft: true, checks: [{ name: "unit", status: "COMPLETED" }] }),
      new Date(),
    )
    expect(decision.action).toBe("skip")
    expect(decision.ci).toBe("present")
  })

  test("normalizes classic pending and terminal status contexts", () => {
    expect(normalizeCheckStatus(undefined, "PENDING")).toBe("IN_PROGRESS")
    expect(normalizeCheckStatus(undefined, "SUCCESS")).toBe("COMPLETED")
    expect(normalizeCheckStatus("QUEUED", undefined)).toBe("QUEUED")
  })

  test("treats every unsettled or unrecognized rollup node as in progress", () => {
    // Failing open here is the one direction that defeats the defer gate: an
    // unsettled check read as settled means the PR is reviewed mid-CI.
    // EXPECTED = a required context declared but not yet posted.
    expect(normalizeCheckStatus(undefined, "EXPECTED")).toBe("IN_PROGRESS")
    // A rollup node of a type neither inline fragment covers deserializes with
    // every field undefined.
    expect(normalizeCheckStatus(undefined, undefined)).toBe("IN_PROGRESS")
    // A StatusState added by GitHub after this code was written.
    expect(normalizeCheckStatus(undefined, "SOMETHING_NEW")).toBe("IN_PROGRESS")
    expect(normalizeCheckStatus(undefined, "FAILURE")).toBe("COMPLETED")
    expect(normalizeCheckStatus(undefined, "ERROR")).toBe("COMPLETED")
  })

  test("a PR whose checks are all unrecognized defers instead of being reviewed", () => {
    const decision = decidePullRequest(
      snapshot({ checks: [{ name: "mystery", status: normalizeCheckStatus(undefined, undefined) }] }),
      new Date(),
    )
    expect(decision.action).toBe("defer")
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
