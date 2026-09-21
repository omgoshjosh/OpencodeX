import { expect, test } from "bun:test"
import { APICallError } from "ai"
import { ProviderError } from "@/provider/error"
import { MessageV2 } from "@/session/message-v2"
import { ProviderV2 } from "@opencode-ai/core/provider"

const sentinel = "credential-echo-abc123"
const overlapping = "credential-echo-abc123-extra"

function redact(error: APICallError): APICallError {
  const result = ProviderError.redactor({
    auth: { type: "oauth", access: sentinel, refresh: overlapping },
    providerKey: sentinel,
    providerOptions: { customSecret: overlapping, headers: { "x-api-key": sentinel } },
    headers: { "x-custom-license": `Bearer ${sentinel}` },
  }).error(error)
  if (!APICallError.isInstance(result)) throw new Error("Expected APICallError")
  return result
}

test("redacts APICallError echoes without changing its classification", () => {
  const error = redact(
    new APICallError({
      message: `unauthorized ${encodeURIComponent(overlapping)}`,
      url: `https://user:${sentinel}@example.test/v1?credential=${sentinel}`,
      requestBodyValues: { token: sentinel },
      statusCode: 401,
      responseHeaders: { "x-upstream-debug": sentinel, "retry-after": "30", "x-request-id": "req-1" },
      responseBody: JSON.stringify({
        error: { message: overlapping },
        token: sentinel,
        safe: "near-credential-echo-abc12",
      }),
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
  expect(error.requestBodyValues).toEqual({})
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

test("does not redact noncredential auth metadata", () => {
  const value = ProviderError.redactor({ auth: { type: "oauth", accountId: "account-123" } }).value({
    type: "oauth",
    accountId: "account-123",
  })
  expect(value).toEqual({ type: "oauth", accountId: "account-123" })
})

test("redacts short and encoded credentials from arbitrary upstream fields", () => {
  const secret = 'x y+/%"'
  const short = "xy"
  const uri = encodeURIComponent(secret)
  const lower = uri.replace(/%[0-9A-F]{2}/g, (part) => part.toLowerCase())
  const form = new URLSearchParams([["value", secret]]).toString().slice("value=".length)
  const json = JSON.stringify(secret).slice(1, -1)
  const double = encodeURIComponent(uri)
  const mixed = uri.replace("%2B", "%2b")
  const result = ProviderError.redactor({
    auth: { type: "oauth", access: secret, refresh: short, accountId: "safe-account" },
    headers: { "x-license": secret },
  }).error(
    new APICallError({
      message: `failed ${uri} ${lower} ${mixed} ${form} ${double}`,
      url: `https://${encodeURIComponent(secret)}@example.test/v1/${uri}?token=${uri}&safe=visible`,
      requestBodyValues: { secret },
      statusCode: 429,
      responseHeaders: {
        "x-arbitrary-echo": secret,
        "x-short-echo": short,
        "x-request-id": "req-safe",
        "retry-after": "15",
      },
      responseBody: `{"echo":"${json}","safe":"diagnostic"}`,
      isRetryable: true,
    }),
  )
  if (!APICallError.isInstance(result)) throw new Error("Expected APICallError")

  const serialized = inspect(result)
  for (const variant of [secret, short, uri, lower, mixed, form, json, double])
    expect(serialized).not.toContain(variant)
  expect(JSON.parse(result.responseBody ?? "{}").safe).toBe("diagnostic")
  expect(result.responseHeaders?.["x-request-id"]).toBe("req-safe")
  expect(result.responseHeaders?.["retry-after"]).toBe("15")
  expect(result.url).toContain("example.test/v1/")
  expect(result.url).toContain("safe=%5BREDACTED%5D")
})

test("redacts bare scheme payloads and decoded configured credentials", () => {
  const headerSecret = "header-only-secret-abc123"
  const basicSecret = "basic-only-secret-xyz789"
  const encodedSecret = "credential%2Fsegment"
  const decodedSecret = "credential/segment"
  const redact = ProviderError.redactor({
    providerKey: encodedSecret,
    headers: {
      authorization: `Bearer ${headerSecret}`,
      "x-basic-auth": `Basic ${basicSecret}`,
    },
  })
  const error = redact.error(
    new APICallError({
      message: `denied ${headerSecret} ${basicSecret} ${decodedSecret}`,
      url: "https://example.test/v1",
      requestBodyValues: {},
      statusCode: 401,
      responseHeaders: { "x-debug": `${headerSecret} ${basicSecret} ${decodedSecret}`, "x-request-id": "req-safe" },
      responseBody: JSON.stringify({ error: { message: `${headerSecret} ${basicSecret} ${decodedSecret}` } }),
      isRetryable: false,
    }),
  )
  const surfaced = MessageV2.fromError(error, { providerID: ProviderV2.ID.make("test") })

  expect(inspect(error)).not.toContain(headerSecret)
  expect(inspect(error)).not.toContain(basicSecret)
  expect(inspect(error)).not.toContain(decodedSecret)
  expect(JSON.stringify(surfaced)).not.toContain(headerSecret)
  expect(JSON.stringify(surfaced)).not.toContain(basicSecret)
  expect(JSON.stringify(surfaced)).not.toContain(decodedSecret)
  expect(JSON.stringify(surfaced)).toContain("req-safe")
})

test("keeps the redaction marker stable across repeated sanitization", () => {
  const redact = ProviderError.redactor({ providerKey: "RE" })
  const once = redact.error(new Error("RE prompt is too long"))
  const twice = redact.error(once)

  expect(once.message).toBe("[REDACTED] prompt is too long")
  expect(twice.message).toBe(once.message)
})

test("flattens wrapped API errors without losing safe retry diagnostics", () => {
  const cause = new APICallError({
    message: `rate limited ${sentinel}`,
    url: "https://example.test/v1",
    requestBodyValues: { credential: sentinel },
    statusCode: 429,
    responseHeaders: { "retry-after": "30", "x-request-id": "req-2", "x-debug": sentinel },
    responseBody: `retry ${sentinel}`,
    isRetryable: true,
  })
  const result = ProviderError.redactor({ providerKey: sentinel }).error(
    new ProviderError.ResponseStreamError("stream failed", { cause }),
  )
  if (!APICallError.isInstance(result)) throw new Error("Expected APICallError")

  expect(APICallError.isInstance(result)).toBe(true)
  expect(result.statusCode).toBe(429)
  expect(result.isRetryable).toBe(true)
  expect(result.responseHeaders?.["retry-after"]).toBe("30")
  expect(result.responseHeaders?.["x-request-id"]).toBe("req-2")
  expect(inspect(result)).not.toContain(sentinel)
  expect(result.cause).toBeUndefined()
})

test("fails closed for hostile error objects", () => {
  const hostile = new Proxy(new Error(sentinel), {
    get() {
      throw new Error(sentinel)
    },
    getPrototypeOf() {
      throw new Error(sentinel)
    },
  })
  const result = ProviderError.redactor({ providerKey: sentinel }).error(hostile)
  expect(result.message).toBe("Provider request failed")
  expect(inspect(result)).not.toContain(sentinel)
  expect(result.cause).toBeUndefined()
})

test("does not retain an untrusted generic error name", () => {
  const error = new Error("")
  error.name = sentinel
  const result = ProviderError.redactor({ providerKey: sentinel }).error(error)
  const surfaced = MessageV2.fromError(result, { providerID: ProviderV2.ID.make("test") })
  expect(inspect(result)).not.toContain(sentinel)
  expect(JSON.stringify(surfaced)).not.toContain(sentinel)
})

test("prioritizes authoritative header credentials over bounded option traversal", () => {
  const options = Object.fromEntries(
    Array.from({ length: 256 }, (_, index) => [`custom${index}Secret`, `option-secret-${index}`]),
  )
  const result = ProviderError.redactor({
    providerOptions: options,
    headers: { "x-custom-license": sentinel },
  }).error(
    new APICallError({
      message: sentinel,
      url: "https://example.test/v1",
      requestBodyValues: {},
      statusCode: 401,
      responseHeaders: { "x-debug": sentinel },
      responseBody: sentinel,
      isRetryable: false,
    }),
  )
  expect(inspect(result)).not.toContain(sentinel)
})

test("keeps provider keys and URLs outside bounded bulk-source collection", () => {
  const providerKey = "PROVIDER_KEY_SHOULD_NOT_LEAK"
  const pathSecret = "URL_PATH_SHOULD_NOT_LEAK"
  const auth = Object.fromEntries(
    Array.from({ length: 256 }, (_, index) => [`custom${index}Secret`, `auth-secret-${index}`]),
  )
  const headers = Object.fromEntries(
    Array.from({ length: 256 }, (_, index) => [`x-custom-${index}`, `header-secret-${index}`]),
  )
  const redact = ProviderError.redactor({
    auth,
    providerKey,
    headers,
    urls: [`https://example.test/${pathSecret}`],
  })
  for (const secret of [providerKey, pathSecret]) {
    const result = redact.error(
      new APICallError({
        message: secret,
        url: `https://example.test/${secret}`,
        requestBodyValues: {},
        statusCode: 401,
        responseHeaders: { "x-debug": secret },
        responseBody: secret,
        isRetryable: false,
      }),
    )
    expect(inspect(result)).not.toContain(secret)
  }
})

test("removes configured URL query and fragment credentials without relying on key names", () => {
  const signed = "signed-secret-123"
  const result = ProviderError.redactor({
    urls: [`https://example.test/v1?sig=${signed}#${signed}`],
  }).error(
    new APICallError({
      message: `rejected ${signed}`,
      url: `https://example.test/v1?sig=${signed}#${signed}`,
      requestBodyValues: {},
      statusCode: 403,
      responseHeaders: { "x-debug": signed },
      responseBody: signed,
      isRetryable: false,
    }),
  )
  expect(inspect(result)).not.toContain(signed)
  expect(APICallError.isInstance(result) && result.url).toBe("https://example.test/[REDACTED]?sig=%5BREDACTED%5D")
})

test("scrubs URL values structurally before redacting short secrets", () => {
  const generated = "generated-query-credential"
  const result = ProviderError.redactor({ providerKey: "ht" }).error(
    new APICallError({
      message: "request failed",
      url: `https://example.test/v1?sig=${generated}#${generated}`,
      requestBodyValues: {},
      statusCode: 403,
      responseHeaders: {},
      responseBody: "request failed",
      isRetryable: false,
    }),
  )
  expect(APICallError.isInstance(result) && result.url).toBe("https://example.test/v1?sig=%5BREDACTED%5D")
})

test("treats configured URL path segments as request secrets", () => {
  const pathSecret = "pathCredential123"
  const result = ProviderError.redactor({ urls: [`https://example.test/api/${pathSecret}`] }).error(
    new APICallError({
      message: pathSecret,
      url: `https://example.test/api/${pathSecret}`,
      requestBodyValues: {},
      statusCode: 403,
      responseHeaders: { "x-debug": pathSecret },
      responseBody: pathSecret,
      isRetryable: false,
    }),
  )
  expect(inspect(result)).not.toContain(pathSecret)
})

test("redacts decoded echoes of encoded URL path and fragment credentials", () => {
  const pathSecret = "credential/segment"
  const fragmentSecret = "credential/fragment"
  const result = ProviderError.redactor({
    urls: [`https://example.test/${encodeURIComponent(pathSecret)}#${encodeURIComponent(fragmentSecret)}`],
  }).error(
    new APICallError({
      message: `${pathSecret} ${fragmentSecret}`,
      url: "https://example.test/v1",
      requestBodyValues: {},
      statusCode: 403,
      responseHeaders: { "x-path": pathSecret, "x-fragment": fragmentSecret },
      responseBody: `${pathSecret} ${fragmentSecret}`,
      isRetryable: false,
    }),
  )
  expect(inspect(result)).not.toContain(pathSecret)
  expect(inspect(result)).not.toContain(fragmentSecret)
})

test("preserves context overflow classification when a short credential overlaps its code", () => {
  const result = ProviderError.redactor({ providerKey: "ex" }).error(
    new APICallError({
      message: "Bad Request",
      url: "https://example.test/v1",
      requestBodyValues: {},
      statusCode: 400,
      responseHeaders: {},
      responseBody: JSON.stringify({ type: "error", error: { code: "context_length_exceeded" } }),
      isRetryable: false,
    }),
  )
  const redactor = ProviderError.redactor({ providerKey: "ex" })
  const surfaced = MessageV2.fromError(redactor.error(result), { providerID: ProviderV2.ID.make("test") })
  expect(surfaced.name).toBe("ContextOverflowError")
})

function inspect(input: unknown) {
  const strings: string[] = []
  const seen = new WeakSet<object>()
  const visit = (value: unknown) => {
    if (typeof value === "string") strings.push(value)
    if (!value || typeof value !== "object" || seen.has(value)) return
    seen.add(value)
    Object.values(Object.getOwnPropertyDescriptors(value)).forEach((descriptor) => {
      if ("value" in descriptor) visit(descriptor.value)
    })
  }
  visit(input)
  return strings.join("\n")
}
