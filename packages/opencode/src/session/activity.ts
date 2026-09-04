import { Database } from "@opencode-ai/core/database/database"
import { SessionExecutionTable, SessionInteractionTable } from "@opencode-ai/core/session/sql"
import { inArray } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import { delegationRecord, isLiveDelegation } from "./delegation-outcome"
import { Session } from "./session"
import { SessionID } from "./schema"
import { SessionStatus } from "./status"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"

export type Projected = Session.Info & { status: SessionStatus.Info }

export function isLive(input: {
  execution?: "idle" | "queued" | "running" | "interrupted"
  interaction?: "pending" | "replied" | "rejected"
  delegation?: ReturnType<typeof delegationRecord>
  runID: string
}) {
  if (input.execution === "running" || input.execution === "queued") return true
  if (input.interaction === "pending") return true
  return isLiveDelegation(input.delegation, input.runID)
}

export interface Interface {
  readonly project: (sessions: readonly Session.Info[]) => Effect.Effect<ReadonlyArray<{ session: Projected; live: boolean }>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionActivity") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const status = yield* SessionStatus.Service
    const runID = ensureRunID()

    const project = Effect.fn("SessionActivity.project")(function* (sessions: readonly Session.Info[]) {
      const ids = sessions.map((session) => session.id)
      if (ids.length === 0) return []
      const [executions, interactions, statuses] = yield* Effect.all([
        db
          .select({ sessionID: SessionExecutionTable.session_id, state: SessionExecutionTable.state })
          .from(SessionExecutionTable)
          .where(inArray(SessionExecutionTable.session_id, ids))
          .all()
          .pipe(Effect.orDie),
        db
          .select({ sessionID: SessionInteractionTable.session_id, state: SessionInteractionTable.state })
          .from(SessionInteractionTable)
          .where(inArray(SessionInteractionTable.session_id, ids))
          .all()
          .pipe(Effect.orDie),
        status.list(),
      ])
      const execution = new Map(executions.map((row) => [row.sessionID, row.state]))
      const interaction = new Map(
        interactions.filter((row) => row.state === "pending").map((row) => [row.sessionID, row.state]),
      )
      return sessions.map((session) => ({
        session: { ...session, status: statuses.get(session.id) ?? { type: "idle" } },
        live: isLive({
          execution: execution.get(session.id),
          interaction: interaction.get(session.id),
          delegation: delegationRecord(session.metadata),
          runID,
        }),
      }))
    })

    return Service.of({ project })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(SessionStatus.defaultLayer), Layer.provide(Database.defaultLayer))

export * as SessionActivity from "./activity"
