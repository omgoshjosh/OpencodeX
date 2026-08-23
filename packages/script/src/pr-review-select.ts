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
  if (pr.isDraft) return { ...base, action: "skip" as const, reason: "draft", ci: "absent" as const }
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
  if (ci === "absent" && now.getTime() - committedAt < NO_CI_GRACE_MS)
    return { ...base, action: "defer" as const, reason: "CI not yet registered", ci }
  return { ...base, action: "review" as const, reason: "eligible", ci }
}

export function normalizeCheckStatus(status?: string, state?: string): string {
  if (status) return status
  return state === "PENDING" ? "IN_PROGRESS" : "COMPLETED"
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
