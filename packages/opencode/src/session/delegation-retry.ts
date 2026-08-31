import { OpencodeXSwarmRoleTable } from "@opencode-ai/core/opencodex/sql"
import { Database } from "@opencode-ai/core/database/database"
import { Identifier } from "@opencode-ai/core/util/identifier"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Effect, Scope } from "effect"
import { eq, and } from "drizzle-orm"
import { BackgroundJob } from "@/background/job"
import { Session } from "./session"
import { SessionPrompt } from "./prompt"
import { SessionStatus } from "./status"
import { SessionID } from "./schema"
import { DELEGATION_RECORD_VERSION, delegationAttempts, delegationRecord } from "./delegation-outcome"

export type Result = {
  childSessionID: SessionID
  attempt: number
  providerID: ProviderV2.ID
  modelID: ProviderV2.ModelID
  status: "busy"
}

export class RetryError extends Error {}

export const retryBlockedChild = Effect.fn("DelegationRetry.retryBlockedChild")(function* (input: {
  parentSessionID: SessionID
  childSessionID: SessionID
}) {
  const sessions = yield* Session.Service
  const status = yield* SessionStatus.Service
  const prompt = yield* SessionPrompt.Service
  const background = yield* BackgroundJob.Service
  const { db } = yield* Database.Service
  const scope = yield* Scope.Scope
  const [parent, child] = yield* Effect.all([sessions.get(input.parentSessionID), sessions.get(input.childSessionID)])
  if (child.parentID !== parent.id || delegationRecord(child.metadata)?.parentSessionID !== parent.id) {
    return yield* Effect.fail(new RetryError("Child does not belong to this parent."))
  }
  const blocked = yield* status.get(parent.id)
  if (blocked.type !== "blocked" || blocked.childSessionID !== child.id) {
    return yield* Effect.fail(new RetryError("Child is not blocked by a provider failure."))
  }
  if ((yield* background.get(child.id))?.status === "running") {
    return yield* Effect.fail(new RetryError("Child is already running."))
  }
  const messages = yield* sessions.messages({ sessionID: child.id })
  if (
    messages.some(
      (message) =>
        message.info.role === "assistant" &&
        message.parts.some((part) => part.type === "tool" || (part.type === "text" && !part.synthetic && part.text.trim())),
    )
  ) {
    return yield* Effect.fail(new RetryError("Child produced output or tool activity and cannot be safely retried."))
  }
  const user = messages.findLast((message) => message.info.role === "user")
  const parts = user?.parts.filter((part) => part.type === "text" || part.type === "file")
  if (!user || !parts?.length) return yield* Effect.fail(new RetryError("Child has no durable prompt to retry."))
  const opencodex = child.metadata?.opencodex
  if (!isRecord(opencodex) || typeof opencodex.swarmID !== "string" || typeof opencodex.swarmRole !== "string") {
    return yield* Effect.fail(new RetryError("Child has no swarm role to retry."))
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
  if (!role?.providerID || !role.modelID) return yield* Effect.fail(new RetryError("Swarm role has no current primary model."))
  const routes = [{ providerID: role.providerID, modelID: role.modelID }, ...(role.fallbackModels ?? [])]
  const selected = routes.find((route) => !blocked.attemptedModels.includes(`${route.providerID}/${route.modelID}`))
  if (!selected) return yield* Effect.fail(new RetryError("No untried model route remains. Update the role or add a fallback model."))
  if (!(yield* status.claimBlockedRetry({ sessionID: parent.id, childSessionID: child.id }))) {
    return yield* Effect.fail(new RetryError("Child retry is already in progress."))
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
      ...(role.variant && role.variant !== "default" ? { variant: role.variant } : {}),
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
      Effect.forkIn(scope),
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

export * as DelegationRetry from "./delegation-retry"
