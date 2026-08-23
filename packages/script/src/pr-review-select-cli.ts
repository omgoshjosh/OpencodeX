#!/usr/bin/env bun
import { $ } from "bun"
import {
  decidePullRequest,
  flattenPages,
  mapConcurrent,
  REVIEW_REPO,
  normalizeCheckStatus,
  type CheckRun,
  type PullRequestSnapshot,
} from "./pr-review-select.js"

// `statusCheckRollup` mixes CheckRun nodes (name/status) with older
// StatusContext nodes (context/state), so both shapes are optional here.
type GhRollupEntry = {
  name?: string
  context?: string
  status?: string
  state?: string
}

type GhPullRequest = {
  number: number
  title: string
  author: { login: string } | null
  isDraft: boolean
  headRefOid: string
  statusCheckRollup: GhRollupEntry[] | null
  headCommittedAt: string
}

type GhListPullRequest = { number: number }

const PR_QUERY = `query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      number title isDraft headRefOid author { login }
      commits(last: 1) { nodes { commit { committedDate } } }
    }
  }
}`

const CHECK_QUERY = `query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      statusCheckRollup { contexts(first: 100, after: $after) {
        nodes { ... on CheckRun { name status } ... on StatusContext { context state } }
        pageInfo { hasNextPage endCursor }
      } }
    }
  }
}`

const PR_CONCURRENCY = 4
const requestedPr = process.argv[2] === "--pr" ? Number(process.argv[3]) : undefined
if (process.argv.length > 2 && (requestedPr === undefined || !Number.isInteger(requestedPr) || requestedPr < 1))
  throw new Error("usage: pr-review:select [--pr <positive-number>]")

const listed = await paginated<GhListPullRequest>(
  `repos/${REVIEW_REPO}/pulls?state=open&per_page=100`,
  isListPullRequest,
)
const selected = requestedPr === undefined ? listed : listed.filter((pull) => pull.number === requestedPr!)
if (requestedPr !== undefined && selected.length !== 1)
  throw new Error(`open pull request #${requestedPr} was not found`)
const pulls = await mapConcurrent(selected, PR_CONCURRENCY, (pull) => loadPullRequest(pull.number))

const now = new Date()
const decisions = pulls.map((pull) => {
  const rollup = pull.statusCheckRollup ?? []
  const checks: CheckRun[] = rollup.map((entry) => ({
    name: entry.name ?? entry.context ?? "unnamed check",
    status: normalizeCheckStatus(entry.status, entry.state),
  }))

  const snapshot: PullRequestSnapshot = {
    number: pull.number,
    title: pull.title,
    authorLogin: pull.author?.login ?? "",
    isDraft: pull.isDraft,
    headRefOid: pull.headRefOid,
    headCommittedAt: pull.headCommittedAt,
    checks,
  }

  return decidePullRequest(snapshot, now)
})

console.log(JSON.stringify(decisions, null, 2))

function isGhPullRequest(value: unknown): value is GhPullRequest {
  if (!record(value)) return false
  if (typeof value.number !== "number" || typeof value.title !== "string") return false
  if (typeof value.isDraft !== "boolean" || typeof value.headRefOid !== "string") return false
  if (value.author !== null && (!record(value.author) || typeof value.author.login !== "string")) return false
  if (typeof value.headCommittedAt !== "string") return false
  return (
    value.statusCheckRollup === null ||
    (Array.isArray(value.statusCheckRollup) && value.statusCheckRollup.every(isCheck))
  )
}

async function loadPullRequest(number: number): Promise<GhPullRequest> {
  const raw: unknown =
    await $`gh api graphql -f query=${PR_QUERY} -F owner=ecgreen -F name=OpencodeX -F number=${number}`.json()
  const pull =
    record(raw) && record(raw.data) && record(raw.data.repository) ? raw.data.repository.pullRequest : undefined
  if (!record(pull) || !record(pull.commits) || !Array.isArray(pull.commits.nodes))
    throw new Error(`gh api graphql returned an invalid pull request for #${number}`)
  const commit = pull.commits.nodes.at(-1)
  if (!record(commit) || !record(commit.commit) || typeof commit.commit.committedDate !== "string")
    throw new Error(`gh api graphql returned no head commit timestamp for #${number}`)
  const rollup = await loadCheckContexts(number)
  const value = {
    ...pull,
    headCommittedAt: commit.commit.committedDate,
    statusCheckRollup: rollup,
  }
  if (!isGhPullRequest(value)) throw new Error(`gh api returned an invalid pull request for #${number}`)
  return value
}

async function loadCheckContexts(number: number, after?: string): Promise<GhRollupEntry[]> {
  const raw: unknown = after
    ? await $`gh api graphql -f query=${CHECK_QUERY} -F owner=ecgreen -F name=OpencodeX -F number=${number} -F after=${after}`.json()
    : await $`gh api graphql -f query=${CHECK_QUERY} -F owner=ecgreen -F name=OpencodeX -F number=${number}`.json()
  const contexts =
    record(raw) &&
    record(raw.data) &&
    record(raw.data.repository) &&
    record(raw.data.repository.pullRequest) &&
    record(raw.data.repository.pullRequest.statusCheckRollup) &&
    record(raw.data.repository.pullRequest.statusCheckRollup.contexts)
      ? raw.data.repository.pullRequest.statusCheckRollup.contexts
      : undefined
  if (
    !record(contexts) ||
    !Array.isArray(contexts.nodes) ||
    !contexts.nodes.every(isCheck) ||
    !record(contexts.pageInfo) ||
    typeof contexts.pageInfo.hasNextPage !== "boolean" ||
    (contexts.pageInfo.endCursor !== null && typeof contexts.pageInfo.endCursor !== "string")
  )
    throw new Error(`gh api graphql returned invalid check contexts for #${number}`)
  if (!contexts.pageInfo.hasNextPage) return contexts.nodes
  if (typeof contexts.pageInfo.endCursor !== "string")
    throw new Error(`gh api graphql returned no next check cursor for #${number}`)
  return [...contexts.nodes, ...(await loadCheckContexts(number, contexts.pageInfo.endCursor))]
}

async function paginated<T>(path: string, validate: (value: unknown) => value is T): Promise<T[]> {
  const pages: unknown = await $`gh api --paginate --slurp ${path}`.json()
  if (!Array.isArray(pages) || !pages.every((page) => Array.isArray(page) && page.every(validate)))
    throw new Error(`gh api returned invalid paginated data for ${path}`)
  return flattenPages(pages)
}

function isListPullRequest(value: unknown): value is GhListPullRequest {
  return record(value) && typeof value.number === "number"
}

function isCheck(value: unknown) {
  if (!record(value)) return false
  return [value.name, value.context, value.status, value.state].every(
    (entry) => entry === undefined || typeof entry === "string",
  )
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
