import { OpencodeXGoalNodeTable, OpencodeXGoalTable } from "@opencode-ai/core/opencodex/sql"
import {
  MessageTable,
  PartTable,
  SessionCommandTable,
  SessionExecutionTable,
  SessionInteractionTable,
  SessionStatusTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { and, eq, inArray, isNotNull, isNull, or, sql } from "drizzle-orm"
import { Effect } from "effect"
import type { Database } from "@opencode-ai/core/database/database"
import type { MessageID, SessionID } from "./schema"

/**
 * Durable evidence for report consumption, and the owner gates every report
 * admission and continuation must pass (OpencodeX-k30).
 *
 * A report is a synthetic user message tagged `task_report`. Accepting it
 * (its message and command row are durable) is NOT the owner acting on it;
 * the only proof of consumption is a FINISHED assistant answer whose
 * `parentID` is the report message: completed, no error, not aborted. A
 * streaming placeholder, an errored or aborted turn, or a turn that returned
 * a prior final (a joined runner, a foreign-owner wait) is not consumption.
 */

type Reader = Pick<Database.Interface["db"], "select">

const json = (path: string) => sql`json_extract(${MessageTable.data}, ${path})`
const FINISHED = ["stop", "length"]
const NATIVE = "success"
/** SQL form of {@link finished}'s native branch: the message's last step-finish part succeeded. */
const nativeSuccess = sql`(SELECT json_extract(${PartTable.data}, '$.reason') FROM ${PartTable}
  WHERE ${PartTable.message_id} = ${MessageTable.id} AND json_extract(${PartTable.data}, '$.type') = 'step-finish'
  ORDER BY ${PartTable.id} DESC LIMIT 1) = ${NATIVE}`
const TERMINAL = ["completed", "failed", "cancelled"]

/**
 * Verified finality of a completed, error-free assistant message: a provider
 * `finish` of stop/length, or the native Claude Code shape, which never sets
 * `finish` and instead ends a successful turn with a step-finish part whose
 * reason is "success" (claude-mapper.ts `finishTurn`). Tool-call steps,
 * failed results and placeholders match neither.
 */
export function finished(info: { finish?: string }, parts: readonly { type: string; reason?: unknown }[] = []) {
  if (info.finish !== undefined) return FINISHED.includes(info.finish)
  return parts.findLast((part) => part.type === "step-finish")?.reason === NATIVE
}

/** A live assistant message that answers `messageID` (the in-memory form of {@link reportAnswered}). */
export function answers(
  info: { role: string; parentID?: string; error?: unknown; finish?: string; time?: { created?: number; completed?: number } },
  messageID: string,
  parts?: readonly { type: string }[],
) {
  return (
    info.role === "assistant" && info.parentID === messageID && !info.error && !!info.time?.completed && finished(info, parts)
  )
}

/** Whether the message carries a `task_report` part: the explicit report contract. */
export const isReportMessage = Effect.fnUntraced(function* (reader: Reader, messageID: MessageID) {
  const parts = yield* reader
    .select({ data: PartTable.data })
    .from(PartTable)
    .where(eq(PartTable.message_id, messageID))
    .all()
    .pipe(Effect.orDie)
  return parts.some((part) => {
    const data = part.data as { type?: unknown; metadata?: { task_report?: unknown } | null }
    return data.type === "text" && data.metadata?.task_report === true
  })
})

const reply = (sessionID: SessionID, messageID: string) =>
  and(eq(MessageTable.session_id, sessionID), eq(json("$.role"), "assistant"), eq(json("$.parentID"), messageID))

/**
 * A finished, successful assistant answer to the report exists in the owner
 * session AND the report's own command succeeded. `settling` is the command's
 * own settlement (it is still `running`), so only the answer is checked.
 */
export const reportAnswered = Effect.fnUntraced(function* (
  reader: Reader,
  sessionID: SessionID,
  messageID: string,
  settling = false,
) {
  const row = yield* reader
    .select({ id: MessageTable.id })
    .from(MessageTable)
    .where(
      and(
        reply(sessionID, messageID),
        isNotNull(json("$.time.completed")),
        isNull(json("$.error")),
        or(inArray(json("$.finish"), FINISHED), and(isNull(json("$.finish")), nativeSuccess)),
      ),
    )
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  if (!row || settling) return !!row
  const command = yield* reader
    .select({ id: SessionCommandTable.id })
    .from(SessionCommandTable)
    .where(
      and(
        eq(SessionCommandTable.session_id, sessionID),
        sql`${SessionCommandTable.message_id} = ${messageID}`,
        eq(SessionCommandTable.status, "succeeded"),
      ),
    )
    .get()
    .pipe(Effect.orDie)
  return !!command
})

/** The owner's turn on the report was aborted (a human stop): never re-run it. */
export const reportAborted = Effect.fnUntraced(function* (reader: Reader, sessionID: SessionID, messageID: string) {
  const row = yield* reader
    .select({ id: MessageTable.id })
    .from(MessageTable)
    .where(and(reply(sessionID, messageID), eq(json("$.error.name"), "MessageAbortedError")))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  return !!row
})

/** Retired by archive or by the handoff naming rule ("(handed off M/D)"). */
export function ownerRetired(owner: { title: string; time_archived?: number | null }) {
  return !!owner.time_archived || owner.title.includes("(handed off")
}

/**
 * Why a report may not run an owner turn right now:
 * - `owner-missing` / `owner-retired` / `goal-terminal`: never, escalate
 * - `human-cancelled`: the human cancelled the owner's execution; escalated
 *   (result kept, never auto-run) by admission and reconciler alike
 * - `held`: interrupted, a pending permission or question, blocked, or the
 *   reporting child's goal paused/blocked; may run once the gate clears
 */
export type OwnerGate = "owner-missing" | "owner-retired" | "goal-terminal" | "human-cancelled" | "held"

/** Reads every gate from `reader` (pass a transaction to decide atomically with a write). */
export const ownerGate = Effect.fnUntraced(function* (reader: Reader, ownerID: SessionID, childID?: SessionID) {
  const owner = yield* reader
    .select({ title: SessionTable.title, time_archived: SessionTable.time_archived })
    .from(SessionTable)
    .where(eq(SessionTable.id, ownerID))
    .get()
    .pipe(Effect.orDie)
  if (!owner) return "owner-missing" as const
  if (ownerRetired(owner)) return "owner-retired" as const
  // Goal gates apply only to the goal the reporting child's node belongs to:
  // an owner's other goals (finished, paused, or awaiting approval) say
  // nothing about this report.
  const goals = childID
    ? yield* reader
        .select({ status: OpencodeXGoalTable.status })
        .from(OpencodeXGoalNodeTable)
        .innerJoin(OpencodeXGoalTable, eq(OpencodeXGoalNodeTable.goal_id, OpencodeXGoalTable.id))
        .where(eq(OpencodeXGoalNodeTable.session_id, childID))
        .all()
        .pipe(Effect.orDie)
    : []
  if (goals.some((goal) => TERMINAL.includes(goal.status))) return "goal-terminal" as const
  if (goals.some((goal) => goal.status === "paused" || goal.status === "blocked")) return "held" as const
  const execution = yield* reader
    .select({ state: SessionExecutionTable.state, cancelRequestedAt: SessionExecutionTable.cancel_requested_at })
    .from(SessionExecutionTable)
    .where(eq(SessionExecutionTable.session_id, ownerID))
    .get()
    .pipe(Effect.orDie)
  if (execution?.state === "interrupted") return execution.cancelRequestedAt ? ("human-cancelled" as const) : ("held" as const)
  const interaction = yield* reader
    .select({ id: SessionInteractionTable.id })
    .from(SessionInteractionTable)
    .where(and(eq(SessionInteractionTable.session_id, ownerID), eq(SessionInteractionTable.state, "pending")))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  if (interaction) return "held" as const
  const status = yield* reader
    .select({ status: SessionStatusTable.status })
    .from(SessionStatusTable)
    .where(eq(SessionStatusTable.session_id, ownerID))
    .get()
    .pipe(Effect.orDie)
  // Blocked on THIS child: its report is what resolves the block.
  if (status?.status.type === "blocked" && status.status.childSessionID !== childID) return "held" as const
  return undefined
})

/**
 * What a gated admission does: any gate records the report without a turn
 * (noReply). Held owners keep a receipt the reconciler continues once the
 * gate clears; every other gate, a human stop included, escalates, so it is
 * never auto-run and waits for explicit human action.
 */
export function admission(gate: OwnerGate | undefined) {
  return {
    noReply: gate !== undefined,
    escalation: gate === "human-cancelled" ? ("cancelled" as const) : gate === "held" ? undefined : gate,
  }
}

export * as ReportReceipt from "./report-receipt"
