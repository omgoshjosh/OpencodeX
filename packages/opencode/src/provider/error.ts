import { APICallError } from "ai"
import { STATUS_CODES } from "http"
import { iife } from "@/util/iife"
import type { ProviderV2 } from "@opencode-ai/core/provider"

const REDACTED = "[REDACTED]"
const SECRET_KEY = /(?:api[-_]?key|authorization|credential|password|secret|token)/i
const SECRET_HEADER =
  /^(?:authorization|proxy-authorization|.*(?:api[-_]?key|access[-_]?token|auth(?:entication)?|credential|secret|token).*)$/i

export type Redactor = {
  readonly value: (value: unknown) => unknown
  readonly error: (error: unknown) => Error
}

/** Builds a per-request boundary; the secret list never leaves this closure. */
export function redactor(input: {
  auth?: unknown
  providerKey?: unknown
  providerOptions?: unknown
  headers?: Record<string, string>
}): Redactor {
  const values = new Set<string>()
  collectAuth(input.auth, values)
  if (typeof input.providerKey === "string") values.add(input.providerKey)
  collect(input.providerOptions, values, false)
  for (const [key, value] of Object.entries(input.headers ?? {})) {
    if (SECRET_HEADER.test(key)) values.add(value)
  }
  const secrets = [...values]
    .filter((value) => value.length >= 4 && !/^(?:local|public|true|false|null|undefined)$/i.test(value))
    .flatMap((value) => [value, encodeURIComponent(value)])
    .sort((a, b) => b.length - a.length)

  const string = (value: string) => secrets.reduce((result, secret) => result.split(secret).join(REDACTED), value)
  const value = (input: unknown, key?: string): unknown => {
    if (key && SECRET_KEY.test(key)) return REDACTED
    if (typeof input === "string") return string(input)
    if (Array.isArray(input)) return input.map((item) => value(item))
    if (!input || typeof input !== "object") return input
    return Object.fromEntries(
      Object.entries(input).map(([key, item]) => [key, SECRET_HEADER.test(key) ? REDACTED : value(item, key)]),
    )
  }
  const error = (input: unknown): Error => {
    if (APICallError.isInstance(input)) {
      return new APICallError({
        message: string(input.message),
        url: safeURL(input.url, string),
        requestBodyValues: value(input.requestBodyValues) as Record<string, unknown>,
        statusCode: input.statusCode,
        responseHeaders: value(input.responseHeaders) as Record<string, string>,
        responseBody: typeof input.responseBody === "string" ? string(input.responseBody) : input.responseBody,
        isRetryable: input.isRetryable,
      })
    }
    if (input instanceof ResponseStreamError) {
      return new ResponseStreamError(string(input.message), {
        statusCode: input.statusCode,
        responseHeaders: value(input.responseHeaders) as Record<string, string> | undefined,
        responseBody: typeof input.responseBody === "string" ? string(input.responseBody) : input.responseBody,
        isRetryable: input.isRetryable,
      })
    }
    const result = new Error(input instanceof Error ? string(input.message) : string(String(input)))
    if (input instanceof Error && input.stack) result.stack = string(input.stack)
    return result
  }
  return { value, error }
}

export function publicValue(value: unknown): unknown {
  if (typeof value === "string") return value
  if (Array.isArray(value)) return value.map(publicValue)
  if (!value || typeof value !== "object") return value
  return Object.fromEntries(
    Object.entries(value).flatMap(([key, item]) => {
      if (SECRET_KEY.test(key) || SECRET_HEADER.test(key)) return []
      if (key === "headers" && item && typeof item === "object") {
        return [[key, Object.fromEntries(Object.entries(item).filter(([name]) => !SECRET_HEADER.test(name)))]]
      }
      return [[key, publicValue(item)]]
    }),
  )
}

function collect(value: unknown, output: Set<string>, allStrings: boolean, key?: string): void {
  if (typeof value === "string") {
    if (allStrings || (key && (SECRET_KEY.test(key) || SECRET_HEADER.test(key)))) output.add(value)
    return
  }
  if (Array.isArray(value)) return value.forEach((item) => collect(item, output, allStrings))
  if (!value || typeof value !== "object") return
  Object.entries(value).forEach(([key, item]) => collect(item, output, allStrings, key))
}

function collectAuth(value: unknown, output: Set<string>): void {
  if (!value || typeof value !== "object") return
  Object.entries(value).forEach(([key, item]) => {
    if (key === "key" || key === "access" || key === "refresh" || key === "token") collect(item, output, true)
    if (SECRET_KEY.test(key) || SECRET_HEADER.test(key)) collect(item, output, true)
  })
}

function safeURL(value: string, redact: (value: string) => string) {
  try {
    const url = new URL(value)
    const query = [...url.searchParams.keys()].map((key) => `${key}=${REDACTED}`).join("&")
    return redact(`${url.origin}${url.pathname}${query ? `?${query}` : ""}${url.hash}`)
  } catch {
    return redact(value)
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
  if (isOverflow(m) || input.error.statusCode === 413 || body?.error?.code === "context_length_exceeded") {
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
