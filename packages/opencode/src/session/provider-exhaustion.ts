import { OpencodeXSwarmRoleTable } from "@opencode-ai/core/opencodex/sql"
import { Database } from "@opencode-ai/core/database/database"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Effect } from "effect"
import { and, eq } from "drizzle-orm"
import { hydrateFallbackModels } from "@/opencodex/swarm-model"
import { isRecord } from "@/util/record"

export type Route = { providerID: string; modelID: string; variant?: string }

/** `provider/model`, the key both this module and the blocked-retry path use. */
export function routeKey(route: { providerID: string; modelID: string }) {
  return `${route.providerID}/${route.modelID}`
}

/**
 * The swarm role a session was started under, stamped by the delegation path as
 * `metadata.opencodex.{swarmID,swarmRole}`. A session without one (a plain user
 * session) has no operator-configured fallback chain and must not be moved off
 * its model.
 */
export function swarmIdentity(metadata: unknown) {
  const opencodex = isRecord(metadata) ? metadata.opencodex : undefined
  if (!isRecord(opencodex)) return undefined
  const { swarmID, swarmRole } = opencodex
  if (typeof swarmID !== "string" || typeof swarmRole !== "string") return undefined
  return { swarmID, swarmRole }
}

/**
 * The role's ordered routes: configured primary first, then the operator's
 * fallbacks in order. Deliberately sourced from the role row rather than from
 * the message's current model, so a turn that has already been moved once
 * cannot re-derive a different chain and wander off the configured set.
 */
export const roleRoutes = Effect.fn("SessionProviderExhaustion.roleRoutes")(function* (input: {
  swarmID: string
  swarmRole: string
}) {
  const { db } = yield* Database.Service
  const role = yield* db
    .select({
      providerID: OpencodeXSwarmRoleTable.provider_id,
      modelID: OpencodeXSwarmRoleTable.model_id,
      fallbackModels: OpencodeXSwarmRoleTable.fallback_models,
      variant: OpencodeXSwarmRoleTable.variant,
    })
    .from(OpencodeXSwarmRoleTable)
    .where(and(eq(OpencodeXSwarmRoleTable.swarm_id, input.swarmID), eq(OpencodeXSwarmRoleTable.name, input.swarmRole)))
    .get()
    .pipe(Effect.orDie)
  const { providerID, modelID } = role ?? {}
  if (!role || !providerID || !modelID) return []
  return orderedRoutes({ ...role, providerID, modelID })
})

/** Split out from the query so the ordering rule is testable without a database. */
export function orderedRoutes(role: {
  providerID: string
  modelID: string
  fallbackModels: string
  variant?: string | null
}): Route[] {
  return [
    { providerID: role.providerID, modelID: role.modelID, ...(role.variant ? { variant: role.variant } : {}) },
    ...hydrateFallbackModels(role.fallbackModels, { providerID: role.providerID, modelID: role.modelID }),
  ]
}

/**
 * The first route the turn has not already been answered on. Lives here rather
 * than beside the blocked-child retry so the ordinary provider path can reach
 * it without importing the delegation module, which depends on the prompt loop.
 */
export function selectUntriedRoute<T extends { providerID: string; modelID: string }>(
  routes: readonly T[],
  attemptedModels: readonly string[],
) {
  return routes.find((route) => !attemptedModels.includes(routeKey(route)))
}

/**
 * Every model this user turn has already been answered on. Read off the durable
 * assistant rows rather than tracked in memory so a turn resumed after a daemon
 * restart cannot retry a route it already burned.
 */
export function attemptedRoutes(turn: readonly SessionLegacy.WithParts[], userMessageID: string) {
  return turn.flatMap((message) =>
    message.info.role === "assistant" && message.info.parentID === userMessageID
      ? [routeKey({ providerID: message.info.providerID, modelID: message.info.modelID })]
      : [],
  )
}

/**
 * The reason line the session states when the chain runs out. The incident this
 * guards against left a durable assistant row with zero parts, which a caller
 * cannot tell apart from a dropped write - so the terminal case has to say the
 * provider, the classification, and what was tried.
 */
export function exhaustionNotice(input: { providerID: string; modelID: string; attempted: readonly string[] }) {
  return [
    `Provider usage limit reached on ${input.providerID}/${input.modelID}, and no untried fallback model remains for this role.`,
    `Routes attempted: ${input.attempted.join(", ") || routeKey(input)}.`,
    "Update the role's model or add a fallback model, then retry this turn.",
  ].join(" ")
}

export * as SessionProviderExhaustion from "./provider-exhaustion"
