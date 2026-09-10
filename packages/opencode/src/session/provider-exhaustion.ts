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
 * The reason line for a turn that ends without the model saying anything.
 *
 * This is the invariant the whole feature rests on, and it deliberately does
 * NOT depend on having classified the provider's prose correctly. Chasing every
 * provider's wording is unbounded and we will always be one message behind; a
 * durable assistant row with zero parts, however, is indistinguishable from a
 * dropped write to every caller downstream, which is the actual user-visible
 * defect. So the notice states what is known either way: the provider, the
 * model, the provider's own words, how those words were classified (including
 * "unclassified"), and whether a fallback route was attempted.
 */
export function unfinishedTurnNotice(input: {
  providerID: string
  modelID: string
  message?: string
  classification?: string
  attempted?: readonly string[]
  fallbackAttempted: boolean
}) {
  const reported = input.message?.trim()
  return [
    input.fallbackAttempted
      ? `Provider usage limit reached on ${routeKey(input)}, and no untried fallback model remains for this role.`
      : `This turn stopped on ${routeKey(input)} before the model produced a response.`,
    `Provider reported: ${reported || "no message"} (classified as ${input.classification ?? "unclassified"}).`,
    `Routes attempted: ${input.attempted?.length ? input.attempted.join(", ") : routeKey(input)}.`,
    input.fallbackAttempted
      ? "Update the role's model or add a fallback model, then retry this turn."
      : "Retry this turn, or configure a fallback model for this role.",
  ].join(" ")
}

/** The chain-spent case: the same notice, with the fallback attempt stated. */
export function exhaustionNotice(input: {
  providerID: string
  modelID: string
  attempted: readonly string[]
  message?: string
  classification?: string
}) {
  return unfinishedTurnNotice({ ...input, fallbackAttempted: true })
}

/**
 * The provider's own words, whatever error shape carried them. Read
 * structurally rather than from a known error class because the point of the
 * notice is to survive error shapes this module has never seen.
 */
export function failureMessage(error: unknown) {
  if (!isRecord(error)) return undefined
  const data = isRecord(error.data) ? error.data : undefined
  const message = data?.message
  const name = typeof error.name === "string" ? error.name : undefined
  if (typeof message !== "string" || !message.trim()) return name
  return name ? `${name}: ${message}` : message
}

export * as SessionProviderExhaustion from "./provider-exhaustion"
