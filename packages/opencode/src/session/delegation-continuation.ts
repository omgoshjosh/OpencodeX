import { Database } from "@opencode-ai/core/database/database"
import { MessageTable, SessionCommandTable, SessionTable } from "@opencode-ai/core/session/sql"
import { and, eq, gt, isNotNull, isNull, sql } from "drizzle-orm"
import { Cause, Context, Effect } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { delegationRecord, receiptOpen, type DelegationEscalation } from "./delegation-outcome"
import { ownerGate, reportAborted, reportAnswered, type OwnerGate } from "./report-receipt"
import { Session } from "./session"
import { MessageID, SessionID } from "./schema"

/**
 * Accepted-but-unconsumed report receipts (OpencodeX-k30).
 *
 * A delegation's `deliveryOutcome: "delivered"` says the owner accepted the
 * report message; this reconciler closes the gap to "an owner turn answered
 * it". It only ever acts on a receipt - a tagged report whose delivery
 * recorded `reportMessageID` - so a prompt without an explicit report
 * contract (an informational notice, a human prompt) is never touched, and
 * records written before receipts existed are never re-prompted.
 *
 * Per open receipt, re-read on every pass:
 * - a finished assistant answered the report AND
 *   the report's command succeeded                -> stamp consumed, done
 * - a human aborted the owner's turn on it        -> escalate, never prompt
 * - owner missing/retired, goal terminal, or the
 *   report command was cancelled by a human      -> escalate, never prompt
 * - the human cancelled the owner's execution     -> escalate, never prompt
 * - owner busy/queued, or held                     -> wait, no prompt
 * - otherwise, idle: re-run the report command once per pass, at most
 *   `MAX_CONTINUATIONS` times with backoff, then escalate.
 * A newer unrelated owner turn never closes a receipt. The same gates are
 * re-checked inside `requeueReport`'s write transaction.
 */

export const MAX_CONTINUATIONS = 3
/** Wait after the last report turn ended before continuation n (0-based). */
export const CONTINUATION_BACKOFF_MS = [30_000, 120_000, 600_000] as const
/** The scan reads every session's metadata, so it runs at most this often. */
export const RECONCILE_INTERVAL_MS = 60_000
const BATCH = 64

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly sessions: Context.Service.Shape<typeof Session.Service>
  readonly requeueReport: (input: {
    sessionID: SessionID
    messageID: MessageID
    childID?: SessionID
  }) => Effect.Effect<"requeued" | "busy" | "in-flight" | "cancelled" | "missing" | OwnerGate>
  readonly clock?: () => number
  /** Test seams. */
  readonly backoffMs?: readonly number[]
  readonly intervalMs?: number
}

export type Decision = "consumed" | "escalated" | "waiting" | "continued" | "skipped"

const receiptPath = (key: string) => sql`json_extract(${SessionTable.metadata}, ${`$.opencodex.delegation.${key}`})`

export function make(deps: Deps) {
  const { db, read } = deps.database
  const clock = deps.clock ?? Date.now
  const backoff = deps.backoffMs ?? CONTINUATION_BACKOFF_MS
  const interval = deps.intervalMs ?? RECONCILE_INTERVAL_MS
  let lastRun = Number.NEGATIVE_INFINITY
  /** Fair bounded scan: each pass resumes after the last id read, wrapping once exhausted. */
  let cursor: string | undefined

  const escalate = (
    childID: SessionID,
    record: { runID: string; reportMessageID: string },
    escalation: DelegationEscalation,
  ) =>
    deps.sessions
      .updateDelegationReceipt({
        sessionID: childID,
        runID: record.runID,
        reportMessageID: record.reportMessageID,
        patch: { escalatedAt: clock(), escalation },
      })
      .pipe(
        Effect.tap((written) =>
          written
            ? Effect.logWarning("report receipt escalated; no further automatic prompts").pipe(
                Effect.annotateLogs({ childSessionID: childID, runID: record.runID, escalation }),
              )
            : Effect.void,
        ),
        Effect.as("escalated" as const),
      )

  /** Only a held owner waits; every other gate, a human stop included, escalates (as admission does). */
  const gated = (childID: SessionID, record: { runID: string; reportMessageID: string }, gate: OwnerGate) =>
    gate === "held"
      ? Effect.succeed("waiting" as const)
      : escalate(childID, record, gate === "human-cancelled" ? "cancelled" : gate)

  /** One receipt, decided from a fresh read of every fact it depends on. */
  const reconcileChild = Effect.fn("DelegationContinuation.reconcileChild")(function* (childID: SessionID) {
    const child = yield* deps.sessions.get(childID).pipe(Effect.option)
    if (child._tag === "None") return "skipped" as const
    const record = delegationRecord(child.value.metadata)
    if (!receiptOpen(record)) return "skipped" as const
    const ownerID = SessionID.make(record.parentSessionID)
    const reportID = MessageID.make(record.reportMessageID)
    if (yield* reportAnswered(db, ownerID, reportID)) {
      yield* deps.sessions.updateDelegationReceipt({
        sessionID: childID,
        runID: record.runID,
        reportMessageID: reportID,
        patch: { consumedAt: clock() },
      })
      return "consumed" as const
    }
    // A human stopped the owner's turn on this report: theirs to restart.
    if (yield* reportAborted(db, ownerID, reportID)) return yield* escalate(childID, record, "cancelled")
    const gate = yield* ownerGate(db, ownerID, childID)
    if (gate) return yield* gated(childID, record, gate)
    const report = yield* db
      .select({ id: MessageTable.id })
      .from(MessageTable)
      .where(and(eq(MessageTable.id, reportID), eq(MessageTable.session_id, ownerID)))
      .get()
      .pipe(Effect.orDie)
    const command = yield* db
      .select({ status: SessionCommandTable.status, completedAt: SessionCommandTable.completed_at })
      .from(SessionCommandTable)
      .where(and(eq(SessionCommandTable.session_id, ownerID), eq(SessionCommandTable.message_id, reportID)))
      .get()
      .pipe(Effect.orDie)
    if (!report || !command) return yield* escalate(childID, record, "report-missing")
    if (command.status === "cancelled") return yield* escalate(childID, record, "cancelled")
    if (command.status === "queued" || command.status === "running") return "waiting" as const

    const attempts = record.continuationAttempts ?? 0
    if (attempts >= MAX_CONTINUATIONS) return yield* escalate(childID, record, "retry-limit")
    const wait = backoff[Math.min(attempts, backoff.length - 1)] ?? 0
    if (clock() < (command.completedAt ?? 0) + wait) return "waiting" as const
    // Count the attempt first (compare-and-set), so a crash between the two
    // writes can only under-run, never exceed, the bound.
    const counted = yield* deps.sessions.updateDelegationReceipt({
      sessionID: childID,
      runID: record.runID,
      reportMessageID: reportID,
      expectAttempts: attempts,
      patch: { continuationAttempts: attempts + 1 },
    })
    if (!counted) return "skipped" as const
    const started = yield* deps.requeueReport({ sessionID: ownerID, messageID: reportID, childID })
    if (started !== "requeued") {
      // Lost the idle window or a gate closed: hand the attempt back.
      yield* deps.sessions.updateDelegationReceipt({
        sessionID: childID,
        runID: record.runID,
        reportMessageID: reportID,
        expectAttempts: attempts + 1,
        patch: { continuationAttempts: attempts },
      })
      if (started === "cancelled" || started === "missing")
        return yield* escalate(childID, record, started === "missing" ? "report-missing" : "cancelled")
      if (started === "busy" || started === "in-flight") return "waiting" as const
      return yield* gated(childID, record, started)
    }
    yield* Effect.logWarning("continuing an accepted but unconsumed report").pipe(
      Effect.annotateLogs({ childSessionID: childID, ownerSessionID: ownerID, runID: record.runID, attempt: attempts + 1 }),
    )
    return "continued" as const
  })

  /** Every open receipt in this instance's directory, throttled unless forced. */
  const reconcile = Effect.fn("DelegationContinuation.reconcile")(function* (options?: { force?: boolean }) {
    const now = clock()
    if (!options?.force && now - lastRun < interval) return
    lastRun = now
    const ctx = yield* InstanceState.context
    const rows = yield* read
      .select({ id: SessionTable.id })
      .from(SessionTable)
      .where(
        and(
          eq(SessionTable.directory, ctx.directory),
          eq(receiptPath("deliveryOutcome"), "delivered"),
          isNotNull(receiptPath("reportMessageID")),
          isNull(receiptPath("consumedAt")),
          isNull(receiptPath("escalatedAt")),
          cursor ? gt(SessionTable.id, SessionID.make(cursor)) : undefined,
        ),
      )
      .orderBy(SessionTable.id)
      .limit(BATCH)
      .all()
      .pipe(Effect.orDie)
    cursor = rows.length < BATCH ? undefined : rows.at(-1)?.id
    yield* Effect.forEach(
      rows,
      (row) =>
        reconcileChild(row.id).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("report receipt reconciliation failed").pipe(
              Effect.annotateLogs({ childSessionID: row.id, cause: Cause.pretty(cause) }),
            ),
          ),
        ),
      { discard: true },
    )
  })

  return { reconcile, reconcileChild }
}

export * as DelegationContinuation from "./delegation-continuation"
