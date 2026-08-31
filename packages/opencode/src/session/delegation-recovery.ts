import { Database } from "@opencode-ai/core/database/database"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { SessionExecutionTable, SessionTable } from "@opencode-ai/core/session/sql"
import { and, eq } from "drizzle-orm"
import { Context, Effect } from "effect"
import { InstanceState } from "@/effect/instance-state"
import { SessionExecutionOwner } from "./execution-owner"
import { delegationRecord, settleDelegation } from "./delegation-outcome"
import { Session } from "./session"
import { MessageID, SessionID } from "./schema"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly sessions: Context.Service.Shape<typeof Session.Service>
  readonly notify: (input: {
    sessionID: SessionID
    messageID: MessageID
    text: string
    noReply: boolean
  }) => Effect.Effect<void>
}

export function make(deps: Deps) {
  return Effect.gen(function* () {
    const { db } = deps.database
    const processRunID = ensureRunID()

    const recover = Effect.fn("SessionDelegationRecovery.recover")(function* () {
      const ctx = yield* InstanceState.context
      const children = yield* db
        .select({ id: SessionTable.id })
        .from(SessionTable)
        .where(and(eq(SessionTable.project_id, ctx.project.id), eq(SessionTable.directory, ctx.directory)))
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(
        children,
        (row) =>
          Effect.gen(function* () {
            const found = yield* deps.sessions.get(row.id).pipe(Effect.option)
            if (found._tag === "None") return
            const child = found.value
            const record = delegationRecord(child.metadata)
            if (!record) return
            const parent = yield* deps.sessions.get(SessionID.make(record.parentSessionID)).pipe(Effect.option)
            const parentPart =
              parent._tag === "Some" ? yield* taskPart(deps.sessions, record, parent.value.id) : undefined
            // Version-2 records written before `mode` are recoverable only when
            // their durable parent task metadata proves they were background.
            const legacyBackground =
              parentPart?.metadata?.background === true ||
              (parentPart !== undefined &&
                "metadata" in parentPart.state &&
                parentPart.state.metadata?.background === true)
            if (record.mode !== "background" && !legacyBackground) return
            const message = record.childMessageID
              ? (yield* deps.sessions.messages({ sessionID: child.id })).find(
                  (item) => item.info.role === "assistant" && item.info.parentID === record.childMessageID,
                )
              : undefined
            const reportText = message?.parts
              .flatMap((part) =>
                part.type === "text" && !part.synthetic && part.text.trim() ? [part.text.trim()] : [],
              )
              .join("\n")
            const report =
              message?.info.role === "assistant" && message.info.error
                ? [JSON.stringify(message.info.error), reportText].filter(Boolean).join("\n")
                : reportText
            if (record.phase === "running") {
              const execution = yield* db
                .select()
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, child.id))
                .get()
                .pipe(Effect.orDie)
              const ownerAlive = !!record.ownerID && SessionExecutionOwner.alive(record.ownerID, processRunID)
              const executionAlive =
                execution?.state === "running" &&
                !!execution.owner_id &&
                !!execution.lease_expires_at &&
                execution.lease_expires_at > Date.now() &&
                SessionExecutionOwner.alive(execution.owner_id, processRunID)
              // Either live witness is enough to leave the run untouched. A
              // restart may settle only once both process identities are dead.
              if (ownerAlive || executionAlive) return

              const outcome =
                message?.info.role === "assistant" && message.info.time.completed
                  ? message.info.error
                    ? "errored"
                    : message.info.finish === "abort"
                      ? "cancelled"
                      : ["stop", "length"].includes(message.info.finish ?? "")
                        ? "completed"
                        : "abandoned"
                  : "abandoned"
              const summary =
                outcome === "abandoned"
                  ? "Daemon restarted. Runtime execution cannot safely resume; do not assume completion or automatically repeat work/side effects. Inspect the child transcript and decide whether to verify, continue, or start a new attempt."
                  : report
              yield* deps.sessions.stampDelegation({
                sessionID: child.id,
                record: settleDelegation(record, { outcome, summary, deliveryOutcome: "pending" }),
                expectRunID: record.runID,
              })
              if (parent._tag === "Some") {
                if (parentPart) yield* finalizeDanglingPart(deps.sessions, parentPart)
              }
            }

            const settled = delegationRecord((yield* deps.sessions.get(child.id)).metadata)
            // A reused child session may have started a newer run while this
            // recovery pass was inspecting the old one. Never deliver the old
            // transcript as the newer run's result when the settle CAS loses.
            if (
              !settled ||
              settled.runID !== record.runID ||
              settled.phase !== "settled" ||
              settled.deliveryOutcome === "delivered" ||
              parent._tag !== "Some"
            )
              return
            const text = [
              `Background delegation recovery for child ${child.id}, run ${settled.runID}.`,
              report ?? settled.summary ?? `The child run is recorded as ${settled.outcome}.`,
              settled.outcome === "abandoned"
                ? "Daemon restarted, so runtime execution cannot safely resume. Do not assume completion or automatically repeat work/side effects; inspect the child transcript and decide whether to verify, continue, or start a new attempt."
                : undefined,
            ]
              .filter(Boolean)
              .join("\n\n")
            const execution = yield* db
              .select({
                state: SessionExecutionTable.state,
                cancelRequestedAt: SessionExecutionTable.cancel_requested_at,
              })
              .from(SessionExecutionTable)
              .where(eq(SessionExecutionTable.session_id, parent.value.id))
              .get()
              .pipe(Effect.orDie)
            const delivered = yield* deps
              .notify({
                sessionID: parent.value.id,
                messageID: MessageID.make(`msg_delegation_recovery_${settled.runID}`),
                text,
                noReply: execution?.state === "interrupted" && !!execution.cancelRequestedAt,
              })
              .pipe(
                Effect.as(true),
                Effect.catchCause((cause) =>
                  Effect.logWarning("delegation recovery delivery failed", {
                    child: child.id,
                    runID: settled.runID,
                    cause,
                  }).pipe(Effect.as(false)),
                ),
              )
            yield* deps.sessions.stampDelegationDelivery({
              sessionID: child.id,
              runID: settled.runID,
              outcome: delivered ? "delivered" : "failed",
            })
          }).pipe(
            Effect.catchCause((cause) => Effect.logWarning("delegation recovery failed", { child: row.id, cause })),
          ),
        { concurrency: "unbounded", discard: true },
      )
    })
    return { recover }
  })
}

const taskPart = Effect.fn("SessionDelegationRecovery.taskPart")(function* (
  sessions: Context.Service.Shape<typeof Session.Service>,
  record: ReturnType<typeof delegationRecord> extends infer T ? Exclude<T, undefined> : never,
  parentSessionID: SessionID,
) {
  if (!record.parentMessageID || !record.toolCallID) return undefined
  const message = (yield* sessions.messages({ sessionID: parentSessionID })).find(
    (item) => item.info.id === record.parentMessageID,
  )
  const part = message?.parts.find(
    (item): item is SessionLegacy.ToolPart =>
      item.type === "tool" && item.tool === "task" && item.callID === record.toolCallID,
  )
  return part
})

const finalizeDanglingPart = Effect.fn("SessionDelegationRecovery.finalizeDanglingPart")(function* (
  sessions: Context.Service.Shape<typeof Session.Service>,
  part: SessionLegacy.ToolPart,
) {
  if (part.state.status !== "pending" && part.state.status !== "running") return
  yield* sessions.updatePart({
    ...part,
    state: {
      ...part.state,
      status: "error",
      error: "Interrupted by daemon restart; inspect the child transcript before continuing.",
      time: { start: part.state.status === "running" ? part.state.time.start : Date.now(), end: Date.now() },
    },
  })
})

export * as SessionDelegationRecovery from "./delegation-recovery"
