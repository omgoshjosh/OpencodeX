import { SessionLegacy } from "@opencode-ai/core/session/legacy"

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

/**
 * Provider prose for an account-level condition the SAME model will keep
 * returning until a human tops up or the window rolls over. Matched only
 * against the provider's own `message` field, and only as whole phrases: a
 * bare machine token like `insufficient_quota` can appear anywhere (including
 * in echoed user content), so it still has to arrive as a structured code.
 *
 * This list exists because the classification signal is genuinely absent
 * everywhere else. The 2026-09-10 incident arrived as
 * `{"name":"AI_APICallError","isRetryable":true,"statusCode":429}` with the
 * message "The usage limit has been reached" - identical in status and
 * retryability to an ordinary rate limit, and carrying no structured code.
 */
const EXHAUSTION_MESSAGE =
  /usage limits? (?:has |have )?(?:been )?reached|reached your usage limit|exceeded your current quota|insufficient quota|quota (?:has been )?exceeded|billing hard limit|credit balance is too low|out of credits|no credits remaining/i

/**
 * Transient pressure: the same model is expected to succeed after a wait, so
 * this must NOT advance the route. Kept explicit rather than implied by "not
 * exhaustion" so the two stay legible as separate decisions - conflating them
 * either burns a fallback on a limit that would have cleared, or parks a turn
 * on a model that will never answer.
 *
 * Note "Rate limit reached for <model> ... on requests per min" (OpenAI's real
 * 429) shares the word "reached" with the exhaustion set but not "usage limit".
 */
const RATE_LIMIT_MESSAGE = /rate limits? reached|rate limit|too many requests|rate increased too quickly/i

export type ProviderFailure = "exhausted" | "rate-limited"

/**
 * Which of the two provider-pressure conditions this error is, if either.
 *
 * Exhaustion is tested first and wins outright: it is the more specific claim,
 * and a message carrying both ("usage limit reached" on a 429 whose body also
 * says "rate_limit") describes an account that is out, not a window that is
 * full. Neither HTTP status nor `isRetryable` participates - both are 429/true
 * for each condition, which is precisely why this function reads prose.
 */
export function classifyProviderFailure(error: unknown): ProviderFailure | undefined {
  if (!error || !SessionLegacy.APIError.isInstance(error)) return undefined
  const body = error.data.responseBody ? parseResponse(error.data.responseBody) : undefined
  if (body !== undefined && hasExhaustionCode(body)) return "exhausted"
  const message = error.data.message
  if (typeof message !== "string" || !message) return undefined
  if (EXHAUSTION_MESSAGE.test(message)) return "exhausted"
  if (RATE_LIMIT_MESSAGE.test(message)) return "rate-limited"
  return undefined
}

/** Only usage/quota-exhaustion or model-unavailability may advance a role fallback. */
export function isModelFallbackError(error: SessionLegacy.Assistant["error"] | undefined) {
  return classifyProviderFailure(error) === "exhausted"
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
