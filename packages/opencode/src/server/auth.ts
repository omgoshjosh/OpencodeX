export * as ServerAuth from "./auth"

import { Flag } from "@opencode-ai/core/flag/flag"
import { Config as EffectConfig, Context, Effect, Layer, Option, Redacted } from "effect"

export type Credentials = {
  password?: string
  username?: string
}

export type DecodedCredentials = {
  readonly username: string
  readonly password: Redacted.Redacted
}

export type Info = {
  readonly password: Option.Option<string>
  readonly username: string
}

let configured: Info | undefined

export class Config extends Context.Service<Config, Info>()("@opencode/ServerAuthConfig") {
  static layer(input: Info) {
    return Layer.succeed(this, this.of(input))
  }

  static get defaultLayer() {
    const tag = this
    return Layer.effect(
      tag,
      Effect.gen(function* () {
        if (configured) return tag.of(configured)
        const config = yield* EffectConfig.all({
          password: EffectConfig.string("OPENCODE_SERVER_PASSWORD").pipe(EffectConfig.option),
          username: EffectConfig.string("OPENCODE_SERVER_USERNAME").pipe(EffectConfig.withDefault("opencode")),
        })
        return tag.of(config)
      }),
    )
  }
}

export function initialize(credentials: Credentials) {
  configured = {
    username: credentials.username ?? "opencode",
    password: credentials.password ? Option.some(credentials.password) : Option.none(),
  }
  scrubEnvironment()
  return configured!
}

export function capture() {
  const inherited =
    process.env.OPENCODE_SERVER_PASSWORD !== undefined || process.env.OPENCODE_SERVER_USERNAME !== undefined
  if (!configured || inherited) {
    initialize({
      username: process.env.OPENCODE_SERVER_USERNAME ?? Flag.OPENCODE_SERVER_USERNAME,
      password: process.env.OPENCODE_SERVER_PASSWORD ?? Flag.OPENCODE_SERVER_PASSWORD,
    })
  } else {
    scrubEnvironment()
  }
  return configured
}

export function scrubEnvironment() {
  delete process.env.OPENCODE_SERVER_USERNAME
  delete process.env.OPENCODE_SERVER_PASSWORD
  delete process.env.OPENCODE_TUI_COORDINATOR_USERNAME
  delete process.env.OPENCODE_TUI_COORDINATOR_PASSWORD
  delete process.env.OPENCODE_TUI_COORDINATOR_TOKEN
}

export function resetForTest() {
  configured = undefined
}

export function required(config: Info) {
  return Option.isSome(config.password) && config.password.value !== ""
}

export function authorized(credentials: DecodedCredentials, config: Info) {
  return (
    Option.isSome(config.password) &&
    credentials.username === config.username &&
    Redacted.value(credentials.password) === config.password.value
  )
}

export function header(credentials?: Credentials) {
  const password =
    credentials?.password ??
    Option.getOrUndefined(configured?.password ?? Option.none()) ??
    Flag.OPENCODE_SERVER_PASSWORD
  if (!password) return undefined

  const username = credentials?.username ?? configured?.username ?? Flag.OPENCODE_SERVER_USERNAME ?? "opencode"
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`
}

export function headers(credentials?: Credentials) {
  const authorization = header(credentials)
  if (!authorization) return undefined
  return { Authorization: authorization }
}
