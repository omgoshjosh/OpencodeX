import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Effect } from "effect"

type ModelFallbackRoute = {
  providerID: string
  modelID: string
  variant?: string
}

export function availableModelAttempts<T extends ModelFallbackRoute, E, R>(
  models: readonly T[],
  resolve: (model: T) => Effect.Effect<{ variants?: Record<string, unknown> } | undefined, E, R>,
) {
  if (models.length < 2) return Effect.succeed(models.slice())
  return Effect.forEach(models, (model) =>
    resolve(model).pipe(
      Effect.map((info) =>
        info && (!model.variant || model.variant === "default" || info.variants?.[model.variant]) ? model : undefined,
      ),
    ),
  ).pipe(
    Effect.map((resolved) => {
      const available = resolved.filter((model): model is T => model !== undefined)
      // Preserve the primary route when the entire chain is stale so the
      // ordinary prompt path still reports its actionable model error.
      return available.length > 0 ? available : models.slice(0, 1)
    }),
  )
}

const exhaustionCodes = new Set([
  "insufficient_quota",
  "quota_exceeded",
  "usage_limit_reached",
  "usage_not_included",
  "billing_hard_limit_reached",
  // Model-unavailability codes advance the chain too: a fallback entry that
  // names a model the provider no longer serves would otherwise dead-end the
  // whole chain on an error the next entry could sidestep entirely.
  "model_not_found",
  "model_not_available",
  "model_decommissioned",
  "unknown_model",
  "invalid_model",
])

/** Only explicit structured usage/quota-exhaustion or model-unavailability codes may advance a role fallback. */
export function isModelFallbackError(error: SessionLegacy.Assistant["error"] | undefined) {
  if (!error || !SessionLegacy.APIError.isInstance(error) || !error.data.responseBody) return false
  const parsed = parseResponse(error.data.responseBody)
  return parsed !== undefined && hasExhaustionCode(parsed)
}

export function shouldAdvanceModelFallback(turn: readonly SessionLegacy.WithParts[], userMessageID: string) {
  const assistants = turn.filter(
    (message): message is SessionLegacy.WithParts & { info: SessionLegacy.Assistant } =>
      message.info.role === "assistant" && message.info.parentID === userMessageID,
  )
  const latest = assistants.at(-1)
  if (!latest || !isModelFallbackError(latest.info.error)) return false
  return !assistants.some((message) =>
    message.parts.some((part) => part.type !== "step-start" && part.type !== "step-finish"),
  )
}

function parseResponse(value: string) {
  if (value.length > 65_536) return undefined
  try {
    return JSON.parse(value) as unknown
  } catch {
    return undefined
  }
}

function hasExhaustionCode(value: unknown): boolean {
  const pending = [{ value, depth: 0 }]
  for (let visited = 0; pending.length > 0 && visited < 256; visited++) {
    const current = pending.pop()!
    if (typeof current.value !== "object" || current.value === null) continue
    const entries = Array.isArray(current.value)
      ? current.value.map((child) => ["", child] as const)
      : Object.entries(current.value)
    for (const [key, child] of entries) {
      if ((key === "code" || key === "type") && typeof child === "string" && exhaustionCodes.has(child.toLowerCase())) {
        return true
      }
      if (current.depth < 8) pending.push({ value: child, depth: current.depth + 1 })
    }
  }
  return false
}
