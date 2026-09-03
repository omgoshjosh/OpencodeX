import { OpencodeXSwarmRoleTable } from "@opencode-ai/core/opencodex/sql"
import { Database } from "@opencode-ai/core/database/database"
import { Identifier } from "@opencode-ai/core/util/identifier"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Effect, Schema } from "effect"
import { eq, and } from "drizzle-orm"
import { BackgroundJob } from "@/background/job"
import { Session } from "./session"
import { SessionPrompt } from "./prompt"
import { SessionStatus } from "./status"
import { SessionID } from "./schema"
import { DELEGATION_RECORD_VERSION, delegationAttempts, delegationRecord } from "./delegation-outcome"
import { hydrateFallbackModels } from "@/opencodex/swarm-model"

export type Result = {
  childSessionID: SessionID
  attempt: number
  providerID: ProviderV2.ID
  modelID: ProviderV2.ModelID
  status: "busy"
}

export class RetryError extends Schema.TaggedErrorClass<RetryError>()("DelegationRetry.RetryError", {
  message: Schema.String,
}) {}

export const retryBlockedChild = Effect.fn("DelegationRetry.retryBlockedChild")(function* (input: {
  parentSessionID: SessionID
  childSessionID: SessionID
}) {
  const sessions = yield* Session.Service
  const status = yield* SessionStatus.Service
  const prompt = yield* SessionPrompt.Service
  const { db } = yield* Database.Service
  const [parent, child] = yield* Effect.all([sessions.get(input.parentSessionID), sessions.get(input.childSessionID)])
  if (child.parentID !== parent.id || delegationRecord(child.metadata)?.parentSessionID !== parent.id) {
    return yield* new RetryError({ message: "Child does not belong to this parent." })
  }
  const blocked = yield* status.get(parent.id)
  if (blocked.type !== "blocked" || blocked.childSessionID !== child.id) {
    return yield* new RetryError({ message: "Child is not blocked by a provider failure." })
  }
  const messages = yield* sessions.messages({ sessionID: child.id })
  if (hasUnsafeRetryOutput(messages)) {
    return yield* new RetryError({ message: "Child produced output or tool activity and cannot be safely retried." })
  }
  const user = messages.findLast((message) => message.info.role === "user")
  const parts = user?.parts.filter((part) => part.type === "text" || part.type === "file")
  if (!user || !parts?.length) return yield* new RetryError({ message: "Child has no durable prompt to retry." })
  const opencodex = child.metadata?.opencodex
  if (!isRecord(opencodex) || typeof opencodex.swarmID !== "string" || typeof opencodex.swarmRole !== "string") {
    return yield* new RetryError({ message: "Child has no swarm role to retry." })
  }
  const role = yield* db
    .select({
      providerID: OpencodeXSwarmRoleTable.provider_id,
      modelID: OpencodeXSwarmRoleTable.model_id,
      fallbackModels: OpencodeXSwarmRoleTable.fallback_models,
      agent: OpencodeXSwarmRoleTable.agent,
      variant: OpencodeXSwarmRoleTable.variant,
    })
    .from(OpencodeXSwarmRoleTable)
    .where(and(eq(OpencodeXSwarmRoleTable.swarm_id, opencodex.swarmID), eq(OpencodeXSwarmRoleTable.name, opencodex.swarmRole)))
    .get()
    .pipe(Effect.orDie)
  if (!role?.providerID || !role.modelID)
    return yield* new RetryError({ message: "Swarm role has no current primary model." })
  const routes = [
    { providerID: role.providerID, modelID: role.modelID, ...(role.variant ? { variant: role.variant } : {}) },
    ...hydrateFallbackModels(role.fallbackModels, { providerID: role.providerID, modelID: role.modelID }),
  ]
  const selected = selectUntriedRoute(routes, blocked.attemptedModels)
  if (!selected)
    return yield* new RetryError({
      message: "No untried model route remains. Update the role or add a fallback model.",
    })
  const background = yield* BackgroundJob.Service
  if ((yield* background.get(child.id))?.status === "running") {
    return yield* new RetryError({ message: "Child is already running." })
  }
  if (!(yield* status.claimBlockedRetry({ sessionID: parent.id, childSessionID: child.id }))) {
    return yield* new RetryError({ message: "Child retry is already in progress." })
  }
  const attempt = delegationAttempts(child.metadata) + 1
  const runID = Identifier.ascending()
  yield* sessions.stampDelegation({
    sessionID: child.id,
    record: {
      version: DELEGATION_RECORD_VERSION,
      runID,
      parentSessionID: parent.id,
      attempt,
      phase: "running",
      startedAt: Date.now(),
    },
  })
  yield* prompt
    .promptAsync({
      sessionID: child.id,
      model: { providerID: ProviderV2.ID.make(selected.providerID), modelID: ProviderV2.ModelID.make(selected.modelID) },
      ...(role.agent ? { agent: role.agent } : {}),
      ...(selected.variant && selected.variant !== "default" ? { variant: selected.variant } : {}),
      parts,
    })
    .pipe(
      Effect.catch((error) =>
        status.set(parent.id, {
          type: "blocked",
          childSessionID: child.id,
          attemptedModels: [...blocked.attemptedModels, `${selected.providerID}/${selected.modelID}`],
          error: error.message || "Retry could not be started.",
        }),
      ),
      Effect.forkScoped,
    )
  return {
    childSessionID: child.id,
    attempt,
    providerID: ProviderV2.ID.make(selected.providerID),
    modelID: ProviderV2.ModelID.make(selected.modelID),
    status: "busy" as const,
  } satisfies Result
})

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

export function selectUntriedRoute<T extends { providerID: string; modelID: string }>(
  routes: readonly T[],
  attemptedModels: readonly string[],
) {
  return routes.find((route) => !attemptedModels.includes(`${route.providerID}/${route.modelID}`))
}

export function hasUnsafeRetryOutput(messages: SessionLegacy.WithParts[]) {
  return messages.some(
    (message) =>
      message.info.role === "assistant" &&
      message.parts.some((part) => part.type === "tool" || (part.type === "text" && !part.synthetic && part.text.trim())),
  )
}

export * as DelegationRetry from "./delegation-retry"
