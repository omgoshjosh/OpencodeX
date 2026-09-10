import { describe, expect, test } from "bun:test"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import {
  classifyProviderFailure,
  isModelFallbackError,
  shouldAdvanceModelFallback,
} from "../../src/session/model-fallback"
import { SessionRetry } from "../../src/session/retry"
import { SessionProviderExhaustion } from "../../src/session/provider-exhaustion"
import { assistantMessage, userMessage } from "./message-fixture"

/**
 * The exact wire shape of the 2026-09-10 15:44 UTC stall, taken from the daemon
 * log: `{"name":"AI_APICallError","isRetryable":true,"statusCode":429}` with the
 * message "The usage limit has been reached" and no structured error code. Every
 * field a classifier might lean on - status, retryability - is identical to an
 * ordinary rate limit, which is why the turn burned three same-model attempts
 * and then broke the loop with an empty assistant row.
 */
function incidentError() {
  return new SessionLegacy.APIError({
    message: "The usage limit has been reached",
    isRetryable: true,
    statusCode: 429,
  })
}

/** A real rate limit: same status, same retryability, different prose. */
function rateLimitError() {
  return new SessionLegacy.APIError({
    message:
      "Rate limit reached for gpt-5.6-sol in organization org-abc on requests per min (RPM): Limit 500, Used 500",
    isRetryable: true,
    statusCode: 429,
  })
}

describe("provider usage exhaustion classification", () => {
  test("classifies the incident's plain-text usage limit as exhaustion", () => {
    expect(classifyProviderFailure(incidentError())).toBe("exhausted")
  })

  test("does not spend same-model retry attempts on exhaustion", () => {
    // The provider marks it retryable; backing off only burns the budget the
    // route fallback needs, which is what stalled four sessions.
    expect(SessionRetry.retryable(incidentError().toObject())).toBeUndefined()
  })

  test("keeps a genuine rate limit transient and retryable on the same model", () => {
    // The anti-conflation guard: identical status and retryability, so only the
    // prose separates them. A rate limit must still back off in place.
    expect(classifyProviderFailure(rateLimitError())).toBe("rate-limited")
    expect(isModelFallbackError(rateLimitError())).toBe(false)
    expect(SessionRetry.retryable(rateLimitError().toObject())).toEqual({
      message: rateLimitError().data.message,
    })
  })

  test("a bare machine token in provider prose is not enough to advance a route", () => {
    // `insufficient_quota` can arrive inside echoed content; only a structured
    // code or a whole natural-language phrase may move the role off its model.
    const token = new SessionLegacy.APIError({ message: "insufficient_quota", isRetryable: false })
    expect(classifyProviderFailure(token)).toBeUndefined()
  })

  test("still classifies structured exhaustion codes with no telling prose", () => {
    const structured = new SessionLegacy.APIError({
      message: "request failed",
      isRetryable: true,
      responseBody: JSON.stringify({ error: { code: "insufficient_quota" } }),
    })
    expect(classifyProviderFailure(structured)).toBe("exhausted")
    expect(SessionRetry.retryable(structured.toObject())).toBeUndefined()
  })

  test("leaves unrelated provider failures unclassified", () => {
    const overloaded = new SessionLegacy.APIError({ message: "Overloaded", isRetryable: true, statusCode: 503 })
    expect(classifyProviderFailure(overloaded)).toBeUndefined()
    expect(SessionRetry.retryable(overloaded.toObject())).toBeDefined()
  })
})

/**
 * Google and Vertex are real providers here (`@ai-sdk/google`,
 * `@ai-sdk/google-vertex` in provider/transform.ts), and they are the hard case:
 * one enum, RESOURCE_EXHAUSTED, covers both a 60-second burst limit and a spent
 * daily allowance, and the per-minute variant is worded as "Quota exceeded".
 *
 * Every string below is the provider's real wire prose, asserted in BOTH
 * directions, so a future regex edit cannot silently re-break either half.
 */
describe("Google RESOURCE_EXHAUSTED is not one condition", () => {
  /** Google's per-MINUTE 429. Clears on its own; must not burn a route. */
  const PER_MINUTE =
    "Quota exceeded for quota metric 'Generate Content API requests per minute' and limit 'GenerateContent request limit per minute for a region' of service 'generativelanguage.googleapis.com' for consumer 'project_number:123456789'."
  /** Google's canonical RESOURCE_EXHAUSTED prose. Genuinely ambiguous. */
  const CANONICAL = "Resource has been exhausted (e.g. check quota)."

  function googleError(message: string, body?: unknown) {
    return new SessionLegacy.APIError({
      message,
      isRetryable: true,
      statusCode: 429,
      ...(body === undefined ? {} : { responseBody: JSON.stringify(body) }),
    })
  }

  test("a per-minute quota-metric limit is a rate limit, not exhaustion", () => {
    // It matches `quota exceeded` word for word, which is why the windowing
    // wording has to be read first: treating it as exhaustion spends one of the
    // role's fallback routes on a blip that clears in about a minute.
    const error = googleError(PER_MINUTE)
    expect(classifyProviderFailure(error)).toBe("rate-limited")
    expect(isModelFallbackError(error)).toBe(false)
    expect(SessionRetry.retryable(error.toObject())).toEqual({ message: PER_MINUTE })

    // The other direction of the same discriminator: an ACCOUNT-level quota
    // message names no window, so narrowing "quota exceeded" must not demote it
    // to a rate limit that retries forever on a model that will never answer.
    for (const message of [
      "You exceeded your current quota, please check your plan and billing details.",
      "The usage limit has been reached",
      "Your credit balance is too low to access the Anthropic API.",
    ]) {
      const account = googleError(message)
      expect(classifyProviderFailure(account)).toBe("exhausted")
      expect(SessionRetry.retryable(account.toObject())).toBeUndefined()
    }
  })

  test("the per-minute limit stays a rate limit inside Google's envelope", () => {
    // The envelope's own status says RESOURCE_EXHAUSTED, so the message has to
    // outrank the ambiguous enum or defect (a) comes back through the body.
    const error = googleError(PER_MINUTE, {
      error: { code: 429, message: PER_MINUTE, status: "RESOURCE_EXHAUSTED" },
    })
    expect(classifyProviderFailure(error)).toBe("rate-limited")
    expect(isModelFallbackError(error)).toBe(false)
  })

  test("bare RESOURCE_EXHAUSTED prose is classified, and retries the same model first", () => {
    // Unclassified is what broke the loop on 2026-09-10: three attempts on one
    // model and then a durable assistant row with zero parts.
    const error = googleError(CANONICAL)
    expect(classifyProviderFailure(error)).toBe("resource-exhausted")
    // Cheaper bet first: back off in place rather than burn a route...
    expect(SessionRetry.retryable(error.toObject())).toEqual({ message: CANONICAL })
    // ...but once that budget is spent, advance instead of stalling.
    expect(isModelFallbackError(error)).toBe(true)
    expect(shouldAdvanceModelFallback([userMessage(), assistantMessage({ error: error.toObject() })], "msg_user")).toBe(
      true,
    )
  })

  test("reads Google's structured envelope without trusting its numeric code", () => {
    // `status` carries the classification; `code` is the NUMBER 429, which a
    // string-only check read as an empty body.
    const envelope = googleError("request failed", {
      error: { code: 429, message: "Resource has been exhausted", status: "RESOURCE_EXHAUSTED" },
    })
    expect(classifyProviderFailure(envelope)).toBe("resource-exhausted")
    // A numeric status code on its own says nothing about the account, so
    // accepting numbers must not turn every 429 body into a fallback trigger.
    expect(classifyProviderFailure(googleError("request failed", { error: { code: 429 } }))).toBeUndefined()
    // And a definite code anywhere in the body still outranks the ambiguous
    // status, so a gateway that sends both does not lose the certain reading.
    const definite = googleError("request failed", {
      error: { code: 429, status: "RESOURCE_EXHAUSTED", type: "insufficient_quota" },
    })
    expect(classifyProviderFailure(definite)).toBe("exhausted")
    expect(SessionRetry.retryable(definite.toObject())).toBeUndefined()
  })
})

describe("provider exhaustion route advancement", () => {
  const role = {
    providerID: "openai",
    modelID: "gpt-5.6-sol",
    variant: null,
    fallbackModels: JSON.stringify([
      { providerID: "anthropic", modelID: "claude-sonnet-4-5" },
      { providerID: "anthropic", modelID: "claude-opus-4-1" },
    ]),
  }

  test("orders the configured primary ahead of the operator's fallbacks", () => {
    expect(SessionProviderExhaustion.orderedRoutes(role)).toEqual([
      { providerID: "openai", modelID: "gpt-5.6-sol" },
      { providerID: "anthropic", modelID: "claude-sonnet-4-5" },
      { providerID: "anthropic", modelID: "claude-opus-4-1" },
    ])
  })

  test("takes the role's next fallback after the primary is exhausted", () => {
    const routes = SessionProviderExhaustion.orderedRoutes(role)
    expect(SessionProviderExhaustion.selectUntriedRoute(routes, ["openai/gpt-5.6-sol"])).toEqual({
      providerID: "anthropic",
      modelID: "claude-sonnet-4-5",
    })
  })

  test("never returns a model the operator did not configure for the role", () => {
    const routes = SessionProviderExhaustion.orderedRoutes(role)
    const every = routes.map((route) => `${route.providerID}/${route.modelID}`)
    expect(every).toEqual(["openai/gpt-5.6-sol", "anthropic/claude-sonnet-4-5", "anthropic/claude-opus-4-1"])
    // Chain spent: the turn stops and states why rather than picking a default.
    expect(SessionProviderExhaustion.selectUntriedRoute(routes, every)).toBeUndefined()
  })

  test("a role with no configured fallbacks yields only its primary", () => {
    expect(SessionProviderExhaustion.orderedRoutes({ ...role, fallbackModels: "[]" })).toEqual([
      { providerID: "openai", modelID: "gpt-5.6-sol" },
    ])
  })

  test("reads attempted routes off the durable assistant rows of the turn", () => {
    // Durable rather than in-memory so a turn resumed after a daemon restart
    // cannot retry a route it already burned.
    const turn = [
      userMessage(),
      assistantMessage({ error: incidentError().toObject(), id: "msg_a", providerID: "openai", modelID: "gpt-5.6-sol" }),
      assistantMessage({
        error: incidentError().toObject(),
        id: "msg_b",
        providerID: "anthropic",
        modelID: "claude-sonnet-4-5",
      }),
      assistantMessage({
        id: "msg_c",
        parentID: "msg_other",
        providerID: "anthropic",
        modelID: "claude-opus-4-1",
      }),
    ]
    expect(SessionProviderExhaustion.attemptedRoutes(turn, "msg_user")).toEqual([
      "openai/gpt-5.6-sol",
      "anthropic/claude-sonnet-4-5",
    ])
  })
})

describe("provider exhaustion session statement", () => {
  test("states the provider, the model, and every route tried", () => {
    const notice = SessionProviderExhaustion.exhaustionNotice({
      providerID: "openai",
      modelID: "gpt-5.6-sol",
      attempted: ["openai/gpt-5.6-sol", "anthropic/claude-sonnet-4-5"],
    })
    expect(notice).toContain("openai/gpt-5.6-sol")
    expect(notice).toContain("anthropic/claude-sonnet-4-5")
    expect(notice).toMatch(/usage limit/i)
    expect(notice).toMatch(/no untried fallback/i)
  })

  test("states an UNCLASSIFIED provider failure rather than saying nothing", () => {
    // The invariant: what the turn says must not depend on having matched the
    // provider's prose. Chasing every provider's wording is unbounded; a
    // zero-part assistant row is indistinguishable from a dropped write.
    const notice = SessionProviderExhaustion.unfinishedTurnNotice({
      providerID: "google",
      modelID: "gemini-3-pro",
      message: "APIError: Developer instruction is not enabled for models/gemini-3-pro",
      classification: undefined,
      attempted: ["google/gemini-3-pro"],
      fallbackAttempted: false,
    })
    expect(notice).toContain("google/gemini-3-pro")
    expect(notice).toContain("Developer instruction is not enabled")
    expect(notice).toContain("unclassified")
    expect(notice.trim().length).toBeGreaterThan(0)
  })

  test("reads the provider's own words off an error shape it has never seen", () => {
    expect(SessionProviderExhaustion.failureMessage({ name: "APIError", data: { message: "boom" } })).toBe(
      "APIError: boom",
    )
    // No message at all still names something; "" would fail the invariant.
    expect(SessionProviderExhaustion.failureMessage({ name: "UnknownError", data: {} })).toBe("UnknownError")
    expect(SessionProviderExhaustion.failureMessage(undefined)).toBeUndefined()
  })
})

describe("provider exhaustion role identity", () => {
  test("finds the swarm role a session was started under", () => {
    expect(SessionProviderExhaustion.swarmIdentity({ opencodex: { swarmID: "swm_1", swarmRole: "Engineer" } })).toEqual(
      {
        swarmID: "swm_1",
        swarmRole: "Engineer",
      },
    )
  })

  test("a plain session has no operator-configured chain and must not be moved", () => {
    expect(SessionProviderExhaustion.swarmIdentity(undefined)).toBeUndefined()
    expect(SessionProviderExhaustion.swarmIdentity({})).toBeUndefined()
    expect(SessionProviderExhaustion.swarmIdentity({ opencodex: { swarmID: "swm_1" } })).toBeUndefined()
  })
})
