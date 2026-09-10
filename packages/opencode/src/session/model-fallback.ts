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
 * Google/Vertex report both a spent daily allowance and a per-minute burst as
 * `RESOURCE_EXHAUSTED`, so the enum alone cannot decide whether the same model
 * will answer in a minute. It is classified rather than ignored: unclassified
 * is what stalled the turn on 2026-09-10.
 */
const ambiguousExhaustionCodes = new Set(["resource_exhausted"])

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

/**
 * Google's per-MINUTE rate limit borrows the vocabulary of account exhaustion:
 * "Quota exceeded for quota metric 'Generate Content API requests per minute'
 * and limit '... per minute ...' of service 'generativelanguage.googleapis.com'".
 * It matches `quota exceeded` word for word, yet it clears on its own in about
 * a minute - reading it as exhaustion burns a fallback route on a blip.
 *
 * The discriminator is the windowing wording (`quota metric`, `per minute`): a
 * genuine account-level "You exceeded your current quota" never names a window.
 * Tested BEFORE the exhaustion set precisely because the two overlap.
 */
const QUOTA_RATE_LIMIT_MESSAGE = /quota metric|per[- ]minute|per min\b|per day\b|per[- ]hour\b/i

/**
 * Google's canonical RESOURCE_EXHAUSTED prose - "Resource has been exhausted
 * (e.g. check quota)." - and the bare status enum. It is genuinely ambiguous:
 * the same string covers a per-minute burst and a spent daily allowance, and
 * the provider offers nothing else to separate them. Guessing either way is
 * wrong half the time, so this gets its own class: back off on the same model
 * first, and advance the route only once that budget is spent.
 */
const RESOURCE_EXHAUSTED_MESSAGE = /resource (?:has been |was |is )?exhausted|resource_exhausted/i

export type ProviderFailure = "exhausted" | "rate-limited" | "resource-exhausted"

/**
 * Which provider-pressure condition this error is, if any.
 *
 * Three outcomes, because two were not enough:
 * - `exhausted`: the account is out. Do not retry the model, advance the route.
 * - `rate-limited`: a window is full. Retry the same model, never advance.
 * - `resource-exhausted`: the provider said one of those two and did not say
 *   which. Retry the same model first, then advance rather than stall.
 *
 * Definite exhaustion wins over the ambiguous class, and a message that names a
 * time window wins over both - that ordering is what keeps Google's per-minute
 * 429 (which literally reads "Quota exceeded") from burning a fallback route.
 * Neither HTTP status nor `isRetryable` participates: both are 429/true for
 * every condition here, which is precisely why this function reads prose.
 */
export function classifyProviderFailure(error: unknown): ProviderFailure | undefined {
  if (!error || !SessionLegacy.APIError.isInstance(error)) return undefined
  const body = error.data.responseBody ? parseResponse(error.data.responseBody) : undefined
  const structured = body !== undefined ? structuredFailure(body) : undefined
  if (structured === "exhausted") return "exhausted"
  const message = error.data.message
  if (typeof message !== "string" || !message) return structured
  if (QUOTA_RATE_LIMIT_MESSAGE.test(message)) return "rate-limited"
  if (EXHAUSTION_MESSAGE.test(message)) return "exhausted"
  if (RESOURCE_EXHAUSTED_MESSAGE.test(message)) return "resource-exhausted"
  if (RATE_LIMIT_MESSAGE.test(message)) return "rate-limited"
  return structured
}

/**
 * Only usage/quota exhaustion, model unavailability, or an ambiguous
 * RESOURCE_EXHAUSTED may advance a role fallback. The ambiguous case reaches
 * here only after `SessionRetry` has already spent its same-model attempts, so
 * advancing at this point costs a route no transient blip would have needed.
 */
export function isModelFallbackError(error: SessionLegacy.Assistant["error"] | undefined) {
  const classification = classifyProviderFailure(error)
  return classification === "exhausted" || classification === "resource-exhausted"
}

/**
 * Whether an assistant row says anything a caller can read. Step bookkeeping is
 * not an answer: a row carrying only step-start/step-finish is exactly the
 * zero-part row the 2026-09-10 incident left behind, which is indistinguishable
 * from a dropped write.
 */
export function hasStatedOutcome(parts: readonly SessionLegacy.Part[]) {
  return parts.some((part) => part.type !== "step-start" && part.type !== "step-finish")
}

export function shouldAdvanceModelFallback(turn: readonly SessionLegacy.WithParts[], userMessageID: string) {
  const assistants = turn.filter(
    (message): message is SessionLegacy.WithParts & { info: SessionLegacy.Assistant } =>
      message.info.role === "assistant" && message.info.parentID === userMessageID,
  )
  const latest = assistants.at(-1)
  if (!latest || !isModelFallbackError(latest.info.error)) return false
  return !assistants.some((message) => hasStatedOutcome(message.parts))
}

function parseResponse(value: string) {
  if (value.length > 65_536) return undefined
  try {
    return JSON.parse(value) as unknown
  } catch {
    return undefined
  }
}

/**
 * The classification carried by a structured error body, if any.
 *
 * `status` joins `code`/`type` because that is where Google puts the only
 * useful field in its envelope - `{"error":{"code":429,"message":"...",
 * "status":"RESOURCE_EXHAUSTED"}}` - and numbers join strings because the same
 * envelope puts a number in `code`, so a string-only check reads the whole body
 * as empty. Depth and visit caps are unchanged: a response body is untrusted
 * input and this walk must stay bounded.
 *
 * A definite exhaustion code anywhere in the body outranks an ambiguous one.
 */
function structuredFailure(value: unknown): ProviderFailure | undefined {
  const pending = [{ value, depth: 0 }]
  let ambiguous: ProviderFailure | undefined
  for (let visited = 0; pending.length > 0 && visited < 256; visited++) {
    const current = pending.pop()!
    if (typeof current.value !== "object" || current.value === null) continue
    const entries = Array.isArray(current.value)
      ? current.value.map((child) => ["", child] as const)
      : Object.entries(current.value)
    for (const [key, child] of entries) {
      if (key === "code" || key === "type" || key === "status") {
        const token = typeof child === "string" ? child : typeof child === "number" ? String(child) : undefined
        const lower = token?.toLowerCase()
        if (lower !== undefined) {
          if (exhaustionCodes.has(lower)) return "exhausted"
          if (ambiguousExhaustionCodes.has(lower)) ambiguous = "resource-exhausted"
        }
      }
      if (current.depth < 8) pending.push({ value: child, depth: current.depth + 1 })
    }
  }
  return ambiguous
}
