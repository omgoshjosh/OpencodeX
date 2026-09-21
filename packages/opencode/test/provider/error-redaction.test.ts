import { expect, test } from "bun:test"
import { APICallError } from "ai"
import { ProviderError } from "@/provider/error"

const sentinel = "credential-echo-abc123"
const overlapping = "credential-echo-abc123-extra"

function redact(error: APICallError): APICallError {
  return ProviderError.redactor({
    auth: { type: "oauth", access: sentinel, refresh: overlapping },
    providerKey: sentinel,
    providerOptions: { customSecret: overlapping, headers: { "x-api-key": sentinel } },
    headers: { authorization: `Bearer ${sentinel}` },
  }).error(error) as APICallError
}

test("redacts APICallError echoes without changing its classification", () => {
  const error = redact(
    new APICallError({
      message: `unauthorized ${encodeURIComponent(overlapping)}`,
      url: `https://user:${sentinel}@example.test/v1?credential=${sentinel}`,
      requestBodyValues: { token: sentinel, safe: "near-credential-echo-abc12" },
      statusCode: 401,
      responseHeaders: { "x-upstream-debug": sentinel, "retry-after": "30", "x-request-id": "req-1" },
      responseBody: JSON.stringify({ error: { message: overlapping }, token: sentinel }),
      isRetryable: false,
    }),
  )

  expect(APICallError.isInstance(error)).toBe(true)
  expect(error.statusCode).toBe(401)
  expect(error.isRetryable).toBe(false)
  expect(error.responseHeaders?.["retry-after"]).toBe("30")
  expect(error.responseHeaders?.["x-request-id"]).toBe("req-1")
  expect(JSON.stringify(error)).not.toContain(sentinel)
  expect(JSON.stringify(error)).not.toContain(encodeURIComponent(overlapping))
  expect(JSON.stringify(error)).toContain("near-credential-echo-abc12")
  expect(error.cause).toBeUndefined()
})

test("redacts response stream wrappers without retaining their cause", () => {
  const error = ProviderError.redactor({ providerKey: sentinel }).error(
    new ProviderError.ResponseStreamError(`failed ${sentinel}`, {
      cause: new Error(sentinel),
      statusCode: 503,
      responseHeaders: { authorization: sentinel },
      responseBody: sentinel,
      isRetryable: true,
    }),
  )

  expect(error).toBeInstanceOf(ProviderError.ResponseStreamError)
  expect(error.message).toBe("failed [REDACTED]")
  expect(error.cause).toBeUndefined()
  expect(JSON.stringify(error)).not.toContain(sentinel)
})

test("removes configured credentials from public provider projections", () => {
  const value = ProviderError.publicValue({
    options: {
      apiKey: sentinel,
      baseURL: "https://example.test",
      headers: { authorization: sentinel, accept: "json" },
    },
  })
  expect(value).toEqual({ options: { baseURL: "https://example.test", headers: { accept: "json" } } })
})

test("does not redact noncredential auth metadata", () => {
  const value = ProviderError.redactor({ auth: { type: "oauth", accountId: "account-123" } }).value({
    type: "oauth",
    accountId: "account-123",
  })
  expect(value).toEqual({ type: "oauth", accountId: "account-123" })
})
