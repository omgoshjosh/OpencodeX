#!/usr/bin/env bun
import { REVIEW_REPO, REVIEWER_LOGIN } from "./pr-review-select.js"

export type ReviewDraft = {
  number: number
  headRefOid: string
  ci: "present" | "absent"
  pass: number
  body: string
  blocking: number
  nonBlocking: number
  nits: number
}

const marker = /<!--\s*opencodex-pr-review(?:-v2)?\b/i

export function validateDraft(value: unknown): ReviewDraft {
  if (!record(value)) throw new Error("review draft must be an object")
  if (typeof value.number !== "number" || !Number.isInteger(value.number) || value.number < 1)
    throw new Error("review draft has invalid PR number")
  if (typeof value.headRefOid !== "string" || !/^[0-9a-f]{40}$/.test(value.headRefOid))
    throw new Error("review draft must carry a full head SHA")
  if (value.ci !== "present" && value.ci !== "absent") throw new Error("review draft has invalid CI state")
  if (typeof value.pass !== "number" || !Number.isInteger(value.pass) || value.pass < 1)
    throw new Error("review draft has invalid pass")
  if (typeof value.body !== "string" || !value.body.trim()) throw new Error("review draft has an empty body")
  if (marker.test(value.body)) throw new Error("review draft must not contain a marker")
  for (const field of ["blocking", "nonBlocking", "nits"] as const)
    if (typeof value[field] !== "number" || !Number.isInteger(value[field]) || value[field] < 0)
      throw new Error(`review draft has invalid ${field} count`)
  return value as ReviewDraft
}

export function markerFor(draft: ReviewDraft): string {
  return `<!-- opencodex-pr-review-v2 sha=${draft.headRefOid} ci=${draft.ci} pass=${draft.pass} -->`
}

export function postBody(draft: ReviewDraft): string {
  return `${markerFor(draft)}\n${draft.body}`
}

export function verifyPostedReview(value: unknown, draft: ReviewDraft, reviewID: number, currentHead: string): void {
  if (!record(value)) throw new Error("GitHub returned an invalid review")
  if (value.id !== reviewID || value.user === null || !record(value.user) || value.user.login !== REVIEWER_LOGIN)
    throw new Error("GitHub returned a review from the wrong actor or ID")
  if (value.event !== "COMMENT" || value.commit_id !== draft.headRefOid || value.body !== postBody(draft))
    throw new Error("GitHub returned a mismatched review")
  if (currentHead !== draft.headRefOid) throw new Error("pull request head changed after posting")
}

if (import.meta.main) await main()

async function main() {
  const file = process.argv[2] === "--validate" ? process.argv[3] : process.argv[2]
  const value: unknown = await Bun.file(file ?? "").json()
  const drafts = Array.isArray(value) ? value.map(validateDraft) : [validateDraft(value)]
  if (process.argv[2] === "--validate") return
  const token = process.env.GITHUB_TOKEN
  if (!token) throw new Error("GITHUB_TOKEN is required")
  for (const draft of drafts) {
    const fresh = await selectFresh(draft.number)
    if (
      fresh.action !== "review" ||
      fresh.headRefOid !== draft.headRefOid ||
      fresh.nextPass !== draft.pass ||
      fresh.ci !== draft.ci
    )
      throw new Error("review draft is stale; refusing to post")
    const created = await github(`pulls/${draft.number}/reviews`, token, {
      method: "POST",
      body: JSON.stringify({ body: postBody(draft), commit_id: draft.headRefOid, event: "COMMENT" }),
    })
    if (!record(created) || typeof created.id !== "number") throw new Error("GitHub did not return a review ID")
    const current = await github(`pulls/${draft.number}`, token)
    const currentHead =
      record(current) && record(current.head) && typeof current.head.sha === "string" ? current.head.sha : ""
    verifyPostedReview(created, draft, created.id, currentHead)
    console.log(JSON.stringify({ number: draft.number, reviewID: created.id, posted: true }))
  }
}

async function selectFresh(number: number) {
  const process = Bun.spawn(["bun", "run", "src/pr-review-select-cli.ts", "--pr", String(number)], {
    cwd: import.meta.dir,
    stdout: "pipe",
    stderr: "inherit",
  })
  const output = await new Response(process.stdout).json()
  if ((await process.exited) !== 0 || !Array.isArray(output) || output.length !== 1 || !record(output[0]))
    throw new Error("fresh selection failed")
  return output[0]
}

async function github(path: string, token: string, init?: RequestInit): Promise<unknown> {
  const response = await fetch(`https://api.github.com/repos/${REVIEW_REPO}/${path}`, {
    ...init,
    headers: { Accept: "application/vnd.github+json", Authorization: `Bearer ${token}`, ...init?.headers },
  })
  if (!response.ok) throw new Error(`GitHub API failed: ${response.status}`)
  return response.json()
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
