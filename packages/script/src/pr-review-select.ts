export const REVIEW_REPO = "ecgreen/OpencodeX"
export const NO_CI_GRACE_MS = 20 * 60 * 1000

export type CheckRun = { name: string; status: string }
export type PullRequestSnapshot = {
  number: number
  title: string
  authorLogin: string
  isDraft: boolean
  headRefOid: string
  headCommittedAt: string
  /** When GitHub last touched the PR; absent on older callers. */
  updatedAt?: string
  checks: CheckRun[]
}
export type Decision = {
  number: number
  title: string
  headRefOid: string
  action: "review" | "skip" | "defer"
  reason: string
  ci: "present" | "absent"
}

// Read-only v1 reports each eligible current PR. It never writes a marker or
// treats a previous automated report as permission to suppress a later report.
export function decidePullRequest(pr: PullRequestSnapshot, now: Date): Decision {
  const base = { number: pr.number, title: pr.title, headRefOid: pr.headRefOid }
  // `ci` describes the PR, not the decision, so a draft reports what it
  // actually has rather than a hardcoded "absent".
  const draftCi = pr.checks.length ? ("present" as const) : ("absent" as const)
  if (pr.isDraft) return { ...base, action: "skip" as const, reason: "draft", ci: draftCi }
  const pending = pr.checks.filter((check) => check.status !== "COMPLETED")
  if (pending.length)
    return {
      ...base,
      action: "defer" as const,
      reason: `CI running (${pending.map((check) => check.name).join(", ")})`,
      ci: "present" as const,
    }
  const ci = pr.checks.length ? "present" : "absent"
  const committedAt = new Date(pr.headCommittedAt).getTime()
  if (!Number.isFinite(committedAt))
    return { ...base, action: "defer" as const, reason: "invalid head commit timestamp", ci }
  // A branch can be committed days before it is pushed or opened, and the
  // window needs to cover the moment CI could first have started - otherwise
  // the exact case it exists for (a just-opened PR whose jobs are still
  // queuing) skips it. The later of the two timestamps is that moment.
  const updatedAt = pr.updatedAt === undefined ? Number.NaN : new Date(pr.updatedAt).getTime()
  const startedAt = Number.isFinite(updatedAt) ? Math.max(committedAt, updatedAt) : committedAt
  if (ci === "absent" && now.getTime() - startedAt < NO_CI_GRACE_MS)
    return { ...base, action: "defer" as const, reason: "CI not yet registered", ci }
  return { ...base, action: "review" as const, reason: "eligible", ci }
}

/**
 * Terminal commit-status states. `EXPECTED` is absent on purpose: it means a
 * required context has been declared but not yet posted, which is unsettled.
 */
const TERMINAL_STATUS_STATES = new Set(["SUCCESS", "FAILURE", "ERROR"])

/**
 * Normalizes a rollup node to a CheckRun status. Deliberately fails *closed*:
 * anything unrecognized - a node type neither inline fragment covers (whose
 * fields all deserialize to `undefined`), a `StatusState` added later, or
 * `EXPECTED` - reads as in-progress. Treating an unsettled check as settled is
 * the one direction that defeats the defer gate this module exists to
 * implement; the cost of the other direction is one more polling round.
 */
/**
 * Pulls the check-context page out of a `statusCheckRollup` GraphQL response.
 *
 * Returns `undefined` for the legitimate "this head commit has no checks or
 * statuses at all" case, which GitHub reports as a null rollup - that is the
 * no-CI case the grace window exists for, not malformed data. Throws only when
 * the response really is unusable, because the caller aborts the whole
 * selection run on a throw.
 */
export function readRollupPage(
  raw: unknown,
): { nodes: unknown[]; hasNextPage: boolean; endCursor?: string } | undefined {
  const pullRequest =
    isRecord(raw) && isRecord(raw.data) && isRecord(raw.data.repository) ? raw.data.repository.pullRequest : undefined
  if (!isRecord(pullRequest)) throw new Error("gh api graphql returned no pull request")
  if (pullRequest.statusCheckRollup === null) return undefined
  const rollup = pullRequest.statusCheckRollup
  const contexts = isRecord(rollup) ? rollup.contexts : undefined
  if (
    !isRecord(contexts) ||
    !Array.isArray(contexts.nodes) ||
    !isRecord(contexts.pageInfo) ||
    typeof contexts.pageInfo.hasNextPage !== "boolean" ||
    (contexts.pageInfo.endCursor !== null && typeof contexts.pageInfo.endCursor !== "string")
  )
    throw new Error("gh api graphql returned invalid check contexts")
  const endCursor = contexts.pageInfo.endCursor
  if (contexts.pageInfo.hasNextPage && typeof endCursor !== "string")
    throw new Error("gh api graphql returned no next check cursor")
  return {
    nodes: contexts.nodes,
    hasNextPage: contexts.pageInfo.hasNextPage,
    ...(typeof endCursor === "string" ? { endCursor } : {}),
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

/** Splits `owner/name`, so one constant drives both REST and GraphQL calls. */
export function reviewRepoParts(repo: string = REVIEW_REPO) {
  const [owner, name, ...rest] = repo.split("/")
  if (!owner || !name || rest.length) throw new Error(`invalid repository "${repo}"; expected "owner/name"`)
  return { owner, name }
}

export function normalizeCheckStatus(status?: string, state?: string): string {
  if (status) return status
  if (state === undefined) return "IN_PROGRESS"
  return TERMINAL_STATUS_STATES.has(state) ? "COMPLETED" : "IN_PROGRESS"
}

export function flattenPages<T>(pages: readonly (readonly T[])[]): T[] {
  return pages.flatMap((page) => page)
}
export async function mapConcurrent<T, U>(
  values: readonly T[],
  limit: number,
  action: (value: T) => Promise<U>,
): Promise<U[]> {
  if (!Number.isInteger(limit) || limit < 1) throw new Error("concurrency limit must be a positive integer")
  const output = Array<U>(values.length)
  await Promise.all(
    Array.from({ length: Math.min(values.length, limit) }, async (_, worker) => {
      for (let index = worker; index < values.length; index += limit) output[index] = await action(values[index])
    }),
  )
  return output
}
