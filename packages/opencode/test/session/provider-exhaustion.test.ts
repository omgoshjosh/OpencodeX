import { describe, expect, test } from "bun:test"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { classifyProviderFailure, isModelFallbackError } from "../../src/session/model-fallback"
import { SessionRetry } from "../../src/session/retry"
import { SessionProviderExhaustion } from "../../src/session/provider-exhaustion"

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
      user(),
      assistant([], incidentError().toObject(), "msg_a", "msg_user", "openai", "gpt-5.6-sol"),
      assistant([], incidentError().toObject(), "msg_b", "msg_user", "anthropic", "claude-sonnet-4-5"),
      assistant([], undefined, "msg_c", "msg_other", "anthropic", "claude-opus-4-1"),
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

function user(): SessionLegacy.WithParts {
  return { info: { id: "msg_user", role: "user" }, parts: [] } as unknown as SessionLegacy.WithParts
}

function assistant(
  parts: Array<Record<string, unknown>>,
  error: SessionLegacy.Assistant["error"],
  id = "msg_assistant",
  parentID = "msg_user",
  providerID = "openai",
  modelID = "gpt-5.6-sol",
): SessionLegacy.WithParts {
  return {
    info: { id, role: "assistant", parentID, error, providerID, modelID },
    parts,
  } as unknown as SessionLegacy.WithParts
}
