import { describe, expect, test } from "bun:test"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { NamedError } from "@opencode-ai/core/util/error"
import { isModelFallbackError, shouldAdvanceModelFallback } from "../../src/session/model-fallback"
import {
  assistantMessage,
  completedToolState,
  filePart,
  reasoningPart,
  stepFinishPart,
  stepStartPart,
  textPart,
  toolPart,
  userMessage,
} from "./message-fixture"

describe("model fallback error classification", () => {
  test.each([
    "insufficient_quota",
    "quota_exceeded",
    "usage_limit_reached",
    "usage_not_included",
    "billing_hard_limit_reached",
  ])("accepts recursive structured exhaustion code: %s", (code) => {
    expect(isModelFallbackError(apiError({ responseBody: JSON.stringify({ outer: [{ error: { code } }] }) }))).toBe(
      true,
    )
    expect(isModelFallbackError(apiError({ responseBody: JSON.stringify({ error: { type: code } }) }))).toBe(true)
  })

  test.each(["model_not_found", "model_not_available", "model_decommissioned", "unknown_model", "invalid_model"])(
    // A fallback entry naming a model the provider no longer serves must not
    // dead-end the chain: unavailability advances to the next entry.
    "accepts structured model-unavailability code: %s",
    (code) => {
      expect(isModelFallbackError(apiError({ responseBody: JSON.stringify({ error: { code } }) }))).toBe(true)
    },
  )

  test.each([
    apiError({ message: "insufficient_quota" }),
    apiError({ responseBody: "insufficient_quota" }),
    apiError({ responseBody: '{"message":"insufficient_quota"}' }),
    apiError({ responseBody: '{"code":"rate_limit_exceeded"}', statusCode: 429, isRetryable: true }),
    apiError({ responseBody: '{"type":"server_overloaded"}', statusCode: 503, isRetryable: true }),
    apiError({ metadata: { code: "quota_exceeded" } }),
    new SessionLegacy.AuthError({ providerID: "test", message: "quota_exceeded" }),
    new SessionLegacy.ContextOverflowError({ message: "usage_limit_reached" }),
    new SessionLegacy.AbortedError({ message: "cancelled" }),
    // An error the provider path did not classify as an API failure: only an
    // APIError may advance a route, whatever the surrounding error carries.
    new NamedError.Unknown({ message: "unknown" }),
    // ...including a structured exhaustion code sitting in a non-APIError's own
    // response body, which is the reading path a bare code would otherwise hit.
    new SessionLegacy.ContextOverflowError({ message: "unknown", responseBody: '{"code":"quota_exceeded"}' }),
  ])("rejects non-structured or non-usage errors", (error) => {
    expect(isModelFallbackError(error)).toBe(false)
  })
})

describe("provider usage exhaustion in plain provider prose", () => {
  /**
   * The wire shape of the 2026-09-10 15:44 UTC stall, from the daemon log:
   * `{"name":"AI_APICallError","isRetryable":true,"statusCode":429}` with the
   * message "The usage limit has been reached" and no structured code. Status
   * and retryability are identical to an ordinary rate limit, so prose is the
   * only signal that separates them.
   */
  const incident = apiError({ message: "The usage limit has been reached", isRetryable: true, statusCode: 429 })
  const rateLimit = apiError({
    message: "Rate limit reached for gpt-5.6-sol in organization org-abc on requests per min (RPM): Limit 500, Used 500",
    isRetryable: true,
    statusCode: 429,
  })

  test("advances the role's route on an unstructured usage limit", () => {
    expect(isModelFallbackError(incident)).toBe(true)
    expect(shouldAdvanceModelFallback([userMessage(), assistantMessage({ error: incident })], "msg_user")).toBe(true)
  })

  test("a rate limit on the same status and retryability does not advance", () => {
    // Conflating the two either burns a fallback on a limit that would have
    // cleared, or parks a turn on a model that will never answer.
    expect(isModelFallbackError(rateLimit)).toBe(false)
    expect(shouldAdvanceModelFallback([userMessage(), assistantMessage({ error: rateLimit })], "msg_user")).toBe(false)
  })

  test.each([
    "You exceeded your current quota, please check your plan and billing details.",
    "Your credit balance is too low to access the Anthropic API.",
    "Monthly usage limit reached.",
  ])("accepts other provider exhaustion prose: %s", (message) => {
    expect(isModelFallbackError(apiError({ message, isRetryable: true, statusCode: 429 }))).toBe(true)
  })
})

describe("model fallback turn safety", () => {
  test("advances only for the latest eligible empty assistant result", () => {
    expect(shouldAdvanceModelFallback([userMessage(), assistantMessage({ error: exhaustion() })], "msg_user")).toBe(
      true,
    )
    expect(
      shouldAdvanceModelFallback(
        [
          userMessage(),
          assistantMessage({ error: exhaustion() }),
          assistantMessage({ error: apiError({ responseBody: '{"code":"rate_limit_exceeded"}' }) }),
        ],
        "msg_user",
      ),
    ).toBe(false)
  })

  test("prior visible or side-effecting assistant parts permanently block advancement", () => {
    const latest = assistantMessage({ error: exhaustion(), id: "msg_latest" })
    const prior = (parts: SessionLegacy.Part[]) =>
      assistantMessage({ parts, error: exhaustion(), id: "msg_prior" })
    expect(
      shouldAdvanceModelFallback([userMessage(), prior([textPart("partial", { synthetic: false })]), latest], "msg_user"),
    ).toBe(false)
    expect(
      shouldAdvanceModelFallback([userMessage(), prior([toolPart(completedToolState())]), latest], "msg_user"),
    ).toBe(false)
    expect(
      shouldAdvanceModelFallback([userMessage(), prior([reasoningPart("partial reasoning")]), latest], "msg_user"),
    ).toBe(false)
    expect(
      shouldAdvanceModelFallback(
        [userMessage(), prior([filePart({ mime: "text/plain", url: "data:text/plain,output" })]), latest],
        "msg_user",
      ),
    ).toBe(false)
  })

  test("allows internal step bookkeeping before an exhaustion failure", () => {
    expect(
      shouldAdvanceModelFallback(
        [userMessage(), assistantMessage({ parts: [stepStartPart(), stepFinishPart()], error: exhaustion() })],
        "msg_user",
      ),
    ).toBe(true)
  })

  test("ignores unrelated assistant messages from another user turn", () => {
    expect(
      shouldAdvanceModelFallback(
        [
          userMessage(),
          assistantMessage({ parts: [toolPart()], error: exhaustion(), id: "msg_other", parentID: "msg_other_user" }),
          assistantMessage({ error: exhaustion() }),
        ],
        "msg_user",
      ),
    ).toBe(true)
  })
})

function exhaustion() {
  return apiError({ responseBody: '{"error":{"code":"insufficient_quota"}}' })
}

function apiError(input: Partial<SessionLegacy.APIError["data"]>) {
  return new SessionLegacy.APIError({ message: "request failed", isRetryable: false, ...input })
}
