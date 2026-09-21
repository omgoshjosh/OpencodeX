import { APICallError } from "ai"
import { STATUS_CODES } from "http"
import { iife } from "@/util/iife"
import type { ProviderV2 } from "@opencode-ai/core/provider"

const REDACTED = "[REDACTED]"
const SECRET_HEADER =
  /^(?:authorization|proxy-authorization|.*(?:api[-_]?key|access[-_]?token|auth(?:entication)?|credential|secret|token).*)$/i
const MAX_DEPTH = 12
const MAX_VALUES = 256
const overflowErrors = new WeakSet<APICallError>()

export type Redactor = {
  readonly value: (value: unknown) => unknown
  readonly error: (error: unknown) => Error
}

/** Builds a per-request boundary; the secret list never leaves this closure. */
export function redactor(input: {
  auth?: unknown
  providerKey?: unknown
  providerOptions?: unknown
  requestOptions?: unknown
  headers?: Record<string, string>
  urls?: ReadonlyArray<string>
}): Redactor {
  const values = new Set<string>()
  collectAuth(input.auth, values)
  add(values, input.providerKey)
  collectHeaders(input.headers, values)
  input.urls?.forEach((url) => collectURL(url, values))
  collectSource(input.providerOptions, values)
  collectSource(input.requestOptions, values)
  const secrets = [...new Set([...values].flatMap(variants).filter(Boolean))].toSorted((a, b) => b.length - a.length)
  const protectedSecrets = secrets.filter((secret) => secret.includes(REDACTED))
  const unprotectedSecrets = secrets.filter((secret) => !secret.includes(REDACTED))
  const replacer = (source: string[]) => {
    const encoded = source.filter((secret) => /%[0-9a-f]{2}/i.test(secret)).map(percentPattern)
    return (value: string) =>
      encoded.reduce(
        (result, pattern) => result.replace(pattern, REDACTED),
        source.reduce((result, secret) => result.split(secret).join(REDACTED), value),
      )
  }
  const replaceProtected = replacer(protectedSecrets)
  const replaceUnprotected = replacer(unprotectedSecrets)
  const string = (value: string) => replaceProtected(value).split(REDACTED).map(replaceUnprotected).join(REDACTED)
  const value = (item: unknown) => {
    try {
      return sanitizeValue(item, string)
    } catch {
      return REDACTED
    }
  }
  const error = (item: unknown): Error => {
    try {
      if (APICallError.isInstance(item)) return apiCallError(item, string)
      if (item instanceof ResponseStreamError) {
        if (APICallError.isInstance(item.cause)) return apiCallError(item.cause, string)
        return new ResponseStreamError(string(item.message), {
          statusCode: validStatus(item.statusCode),
          responseHeaders: sanitizeHeaders(item.responseHeaders, string),
          responseBody: typeof item.responseBody === "string" ? string(item.responseBody) : undefined,
          isRetryable: typeof item.isRetryable === "boolean" ? item.isRetryable : undefined,
        })
      }
      if (item instanceof ToolInflightTimeoutError) {
        return new ToolInflightTimeoutError(string(item.tool), string(item.callID), item.ms)
      }
      if (item instanceof HeaderTimeoutError) return new HeaderTimeoutError(item.ms)
      if (item instanceof StreamIdleTimeoutError) return new StreamIdleTimeoutError(item.ms, string(item.message))
      if (item instanceof DOMException && item.name === "AbortError") {
        return new DOMException(string(item.message), "AbortError")
      }
      if (item instanceof Error) {
        return new Error(string(item.message))
      }
      if (typeof item === "string") return new Error(string(item))
      return new Error("Provider request failed")
    } catch {
      return new Error("Provider request failed")
    }
  }
  return { value, error }
}

function apiCallError(input: APICallError, redact: (value: string) => string) {
  const overflow = overflowErrors.has(input) || isAPICallOverflow(input)
  const result = new APICallError({
    message: redact(input.message),
    url: safeURL(input.url, redact),
    requestBodyValues: {},
    statusCode: validStatus(input.statusCode),
    responseHeaders: sanitizeHeaders(input.responseHeaders, redact) ?? {},
    responseBody: typeof input.responseBody === "string" ? redact(input.responseBody) : undefined,
    isRetryable: input.isRetryable,
  })
  if (overflow) overflowErrors.add(result)
  return result
}

function isAPICallOverflow(input: APICallError) {
  const body = json(input.responseBody)
  return isOverflow(input.message) || input.statusCode === 413 || body?.error?.code === "context_length_exceeded"
}

function collect(
  value: unknown,
  output: Set<string>,
  allStrings = false,
  depth = 0,
  seen = new WeakSet<object>(),
): void {
  if (depth > MAX_DEPTH || output.size >= MAX_VALUES) return
  if (typeof value === "string") {
    if (allStrings) add(output, value)
    return
  }
  if (!value || typeof value !== "object" || seen.has(value)) return
  seen.add(value)
  try {
    for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
      if (!("value" in descriptor)) continue
      if (isURLKey(key) && typeof descriptor.value === "string") collectURL(descriptor.value, output)
      collect(
        descriptor.value,
        output,
        allStrings || key.toLowerCase() === "headers" || isSecretKey(key),
        depth + 1,
        seen,
      )
    }
  } catch {}
}

function collectSource(value: unknown, output: Set<string>) {
  const source = new Set<string>()
  collect(value, source)
  source.forEach((item) => output.add(item))
}

function collectAuth(value: unknown, output: Set<string>) {
  if (!value || typeof value !== "object") return
  try {
    for (const key of ["key", "access", "refresh", "token"]) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor && "value" in descriptor) add(output, descriptor.value)
    }
  } catch {}
}

function collectHeaders(value: unknown, output: Set<string>) {
  if (!value || typeof value !== "object") return
  try {
    Object.values(Object.getOwnPropertyDescriptors(value)).forEach((descriptor) => {
      if (!("value" in descriptor) || typeof descriptor.value !== "string") return
      add(output, descriptor.value)
      add(output, /^\S+\s+(.+)$/.exec(descriptor.value)?.[1])
    })
  } catch {}
}

function add(output: Set<string>, value: unknown) {
  if (typeof value === "string" && value) output.add(value)
}

function isSecretKey(key: string) {
  const normalized = key.replace(/[-_]/g, "").toLowerCase()
  if (["key", "access", "refresh", "token", "secret", "credential", "password", "authorization"].includes(normalized)) {
    return true
  }
  if (/(?:apikey|accesskeyid|privatekey|secretaccesskey)$/.test(normalized)) return true
  return /(?:token|secret|credential|password|authorization)$/.test(normalized) && !normalized.endsWith("tokens")
}

function isURLKey(key: string) {
  return /^(?:base)?url$/i.test(key)
}

function collectURL(value: string, output: Set<string>) {
  try {
    const url = new URL(value)
    addURLValue(output, url.username)
    addURLValue(output, url.password)
    url.pathname.split("/").forEach((item) => addURLValue(output, item))
    url.searchParams.forEach((item) => addURLValue(output, item))
    addURLValue(output, url.hash.slice(1))
  } catch {}
}

function addURLValue(output: Set<string>, value: string) {
  add(output, value)
  try {
    add(output, decodeURIComponent(value))
  } catch {}
}

function variants(value: string) {
  const initial = [value, decode(value)].flatMap((item) => {
    const uri = encode(item)
    const form = new URLSearchParams([["value", item]]).toString().slice("value=".length)
    const json = JSON.stringify(item).slice(1, -1)
    return [item, uri, uri.replace(/%[0-9A-F]{2}/g, (part) => part.toLowerCase()), form, json]
  })
  return [...new Set([...initial, ...initial.map(encode)])]
}

function encode(value: string) {
  try {
    return encodeURIComponent(value)
  } catch {
    return value
  }
}

function decode(value: string) {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

function percentPattern(value: string) {
  const pattern: string[] = []
  for (let index = 0; index < value.length; index++) {
    const triplet = value.slice(index, index + 3)
    if (/^%[0-9a-f]{2}$/i.test(triplet)) {
      pattern.push(
        `%${triplet
          .slice(1)
          .split("")
          .map((char) => (/[a-f]/i.test(char) ? `[${char.toLowerCase()}${char.toUpperCase()}]` : char))
          .join("")}`,
      )
      index += 2
      continue
    }
    pattern.push(value[index].replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
  }
  return new RegExp(pattern.join(""), "g")
}

function sanitizeValue(
  input: unknown,
  redact: (value: string) => string,
  depth = 0,
  seen = new WeakSet<object>(),
): unknown {
  if (typeof input === "string") return redact(input)
  if (!input || typeof input !== "object") return input
  if (depth > MAX_DEPTH || seen.has(input)) return REDACTED
  seen.add(input)
  const output: Record<string, unknown> = {}
  for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(input))) {
    if (!("value" in descriptor)) continue
    output[key] =
      SECRET_HEADER.test(key) || isSecretKey(key) ? REDACTED : sanitizeValue(descriptor.value, redact, depth + 1, seen)
  }
  return Array.isArray(input) ? Object.values(output) : output
}

function sanitizeHeaders(input: unknown, redact: (value: string) => string) {
  if (!input || typeof input !== "object") return undefined
  return Object.fromEntries(
    Object.entries(Object.getOwnPropertyDescriptors(input)).flatMap(([key, descriptor]) => {
      if (!("value" in descriptor) || typeof descriptor.value !== "string") return []
      return [[key, SECRET_HEADER.test(key) ? REDACTED : redact(descriptor.value)]]
    }),
  )
}

function validStatus(value: unknown) {
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599 ? value : undefined
}

function safeURL(value: unknown, redact: (value: string) => string) {
  if (typeof value !== "string") return ""
  try {
    const url = new URL(value)
    url.username = ""
    url.password = ""
    url.pathname = redact(url.pathname)
    url.searchParams.forEach((_, key) => url.searchParams.set(key, REDACTED))
    url.hash = ""
    return url.toString()
  } catch {
    return ""
  }
}

export class HeaderTimeoutError extends Error {
  public override readonly name = "ProviderHeaderTimeoutError"

  constructor(public readonly ms: number) {
    super(`Provider response headers timed out after ${ms}ms`)
  }
}

export class StreamIdleTimeoutError extends Error {
  public override readonly name: string = "ProviderStreamIdleTimeoutError"

  constructor(
    public readonly ms: number,
    message = `Provider stream produced no events for ${ms}ms`,
  ) {
    super(message)
  }
}

/**
 * The idle watchdog's ceiling for a locally executed tool: the stream was
 * silent because a tool was in flight, and that tool never settled within
 * the in-flight bound. Same family (and failure path) as the idle timeout,
 * but the stated reason names the tool so the operator knows what hung.
 */
export class ToolInflightTimeoutError extends StreamIdleTimeoutError {
  public override readonly name: string = "ProviderToolInflightTimeoutError"

  constructor(
    public readonly tool: string,
    public readonly callID: string,
    ms: number,
  ) {
    super(ms, `Tool ${tool} (${callID}) produced no result for ${ms}ms`)
  }
}

export class ResponseStreamError extends Error {
  public override readonly name = "ProviderResponseStreamError"

  constructor(
    message: string,
    options?: ErrorOptions & {
      statusCode?: number
      responseHeaders?: Record<string, string>
      responseBody?: string
      isRetryable?: boolean
    },
  ) {
    super(message, options)
    this.statusCode = options?.statusCode
    this.responseHeaders = options?.responseHeaders
    this.responseBody = options?.responseBody
    this.isRetryable = options?.isRetryable
  }

  public readonly statusCode?: number
  public readonly responseHeaders?: Record<string, string>
  public readonly responseBody?: string
  public readonly isRetryable?: boolean
}

// Adapted from overflow detection patterns in:
// https://github.com/badlogic/pi-mono/blob/main/packages/ai/src/utils/overflow.ts
const OVERFLOW_PATTERNS = [
  /prompt is too long/i, // Anthropic
  /input is too long for requested model/i, // Amazon Bedrock
  /exceeds the context window/i, // OpenAI (Completions + Responses API message text)
  /input token count.*exceeds the maximum/i, // Google (Gemini)
  /maximum prompt length is \d+/i, // xAI (Grok)
  /reduce the length of the messages/i, // Groq
  /maximum context length is \d+ tokens/i, // OpenRouter, DeepSeek, vLLM
  /exceeds the limit of \d+/i, // GitHub Copilot
  /exceeds the available context size/i, // llama.cpp server
  /greater than the context length/i, // LM Studio
  /context window exceeds limit/i, // MiniMax
  /exceeded model token limit/i, // Kimi For Coding, Moonshot
  /context[_ ]length[_ ]exceeded/i, // Generic fallback
  /request entity too large/i, // HTTP 413
  /context length is only \d+ tokens/i, // vLLM
  /input length.*exceeds.*context length/i, // vLLM
  /prompt too long; exceeded (?:max )?context length/i, // Ollama explicit overflow error
  /too large for model with \d+ maximum context length/i, // Mistral
  /model_context_window_exceeded/i, // z.ai non-standard finish_reason surfaced as error text
]

// Providers not reliably handled in this function:
// - z.ai: can accept overflow silently (needs token-count/context-window checks)
function isOverflow(message: string) {
  if (OVERFLOW_PATTERNS.some((p) => p.test(message))) return true

  // Providers/status patterns handled outside of regex list:
  // - Cerebras: often returns "400 (no body)" / "413 (no body)"
  // - Mistral: often returns "400 (no body)" / "413 (no body)"
  return /^4(00|13)\s*(status code)?\s*\(no body\)/i.test(message)
}

function message(providerID: ProviderV2.ID, e: APICallError) {
  return iife(() => {
    const msg = e.message
    if (msg === "") {
      if (e.responseBody) return e.responseBody
      if (e.statusCode) {
        const err = STATUS_CODES[e.statusCode]
        if (err) return err
      }
      return "Unknown error"
    }

    if (!e.responseBody || (e.statusCode && msg !== STATUS_CODES[e.statusCode])) {
      return msg
    }

    try {
      const body = JSON.parse(e.responseBody)
      // try to extract common error message fields
      const errMsg = body.message || body.error || body.error?.message
      if (errMsg && typeof errMsg === "string") {
        return `${msg}: ${errMsg}`
      }
    } catch {}

    // If responseBody is HTML (e.g. from a gateway or proxy error page),
    // provide a human-readable message instead of dumping raw markup
    if (/^\s*<!doctype|^\s*<html/i.test(e.responseBody)) {
      if (e.statusCode === 401) {
        return "Unauthorized: request was blocked by a gateway or proxy. Your authentication token may be missing or expired — try running `opencode auth login <your provider URL>` to re-authenticate."
      }
      if (e.statusCode === 403) {
        return "Forbidden: request was blocked by a gateway or proxy. You may not have permission to access this resource — check your account and provider settings."
      }
      return msg
    }

    return `${msg}: ${e.responseBody}`
  }).trim()
}

function json(input: unknown) {
  if (typeof input === "string") {
    try {
      const result = JSON.parse(input)
      if (result && typeof result === "object") return result
      return undefined
    } catch {
      return undefined
    }
  }
  if (typeof input === "object" && input !== null) {
    return input
  }
  return undefined
}

export type ParsedStreamError =
  | {
      type: "context_overflow"
      message: string
      responseBody: string
    }
  | {
      type: "api_error"
      message: string
      isRetryable: boolean
      responseBody: string
    }

export function parseStreamError(input: unknown): ParsedStreamError | undefined {
  const raw = json(input)
  if (!raw) {
    if (typeof input !== "string" || !isOverflow(input)) return undefined
    return {
      type: "context_overflow",
      message: input,
      responseBody: input,
    }
  }
  const body = typeof raw?.message === "string" ? (json(raw.message) ?? raw) : raw

  const responseBody = JSON.stringify(body)
  if (body.type !== "error") {
    if (typeof body.message !== "string" || !isOverflow(body.message)) return undefined
    return {
      type: "context_overflow",
      message: body.message,
      responseBody,
    }
  }

  switch (body?.error?.code) {
    case "context_length_exceeded":
      return {
        type: "context_overflow",
        message: "Input exceeds context window of this model",
        responseBody,
      }
    case "insufficient_quota":
      return {
        type: "api_error",
        message: "Quota exceeded. Check your plan and billing details.",
        isRetryable: false,
        responseBody,
      }
    case "usage_not_included":
      return {
        type: "api_error",
        message: "To use Codex with your ChatGPT plan, upgrade to Plus: https://chatgpt.com/explore/plus.",
        isRetryable: false,
        responseBody,
      }
    case "invalid_prompt":
      return {
        type: "api_error",
        message: typeof body?.error?.message === "string" ? body?.error?.message : "Invalid prompt.",
        isRetryable: false,
        responseBody,
      }
    case "server_is_overloaded":
    case "server_error":
      return {
        type: "api_error",
        message: typeof body?.error?.message === "string" ? body?.error?.message : "Server error.",
        isRetryable: true,
        responseBody,
      }
  }
  return undefined
}

export type ParsedAPICallError =
  | {
      type: "context_overflow"
      message: string
      responseBody?: string
    }
  | {
      type: "api_error"
      message: string
      statusCode?: number
      isRetryable: boolean
      responseHeaders?: Record<string, string>
      responseBody?: string
      metadata?: Record<string, string>
    }

export function parseAPICallError(input: { providerID: ProviderV2.ID; error: APICallError }): ParsedAPICallError {
  const m = message(input.providerID, input.error)
  const body = json(input.error.responseBody)
  if (
    overflowErrors.has(input.error) ||
    isOverflow(m) ||
    input.error.statusCode === 413 ||
    body?.error?.code === "context_length_exceeded"
  ) {
    return {
      type: "context_overflow",
      message: m,
      responseBody: input.error.responseBody,
    }
  }

  const metadata = input.error.url ? { url: input.error.url } : undefined
  return {
    type: "api_error",
    message: m,
    statusCode: input.error.statusCode,
    isRetryable: [408, 409, 429].includes(input.error.statusCode ?? 0)
      ? true
      : input.error.statusCode !== undefined && input.error.statusCode >= 400 && input.error.statusCode < 500
        ? false
        : input.error.isRetryable,
    responseHeaders: input.error.responseHeaders,
    responseBody: input.error.responseBody,
    metadata,
  }
}

export * as ProviderError from "./error"
