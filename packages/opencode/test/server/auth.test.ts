import { afterEach, describe, expect, test } from "bun:test"
import { Option, Redacted } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { ServerAuth } from "../../src/server/auth"
import { spawnSync } from "node:child_process"

const original = {
  OPENCODE_SERVER_PASSWORD: Flag.OPENCODE_SERVER_PASSWORD,
  OPENCODE_SERVER_USERNAME: Flag.OPENCODE_SERVER_USERNAME,
}

afterEach(() => {
  ServerAuth.resetForTest()
  Flag.OPENCODE_SERVER_PASSWORD = original.OPENCODE_SERVER_PASSWORD
  Flag.OPENCODE_SERVER_USERNAME = original.OPENCODE_SERVER_USERNAME
})

describe("ServerAuth", () => {
  test("does not emit auth headers without a password", () => {
    Flag.OPENCODE_SERVER_PASSWORD = undefined
    Flag.OPENCODE_SERVER_USERNAME = "alice"

    expect(ServerAuth.header()).toBeUndefined()
    expect(ServerAuth.headers()).toBeUndefined()
  })

  test("defaults to the opencode username", () => {
    Flag.OPENCODE_SERVER_PASSWORD = "secret"
    Flag.OPENCODE_SERVER_USERNAME = undefined

    expect(ServerAuth.headers()).toEqual({
      Authorization: `Basic ${Buffer.from("opencode:secret").toString("base64")}`,
    })
  })

  test("uses the configured username", () => {
    Flag.OPENCODE_SERVER_PASSWORD = "secret"
    Flag.OPENCODE_SERVER_USERNAME = "alice"

    expect(ServerAuth.headers()).toEqual({
      Authorization: `Basic ${Buffer.from("alice:secret").toString("base64")}`,
    })
  })

  test("prefers explicit credentials", () => {
    Flag.OPENCODE_SERVER_PASSWORD = "secret"
    Flag.OPENCODE_SERVER_USERNAME = "alice"

    expect(ServerAuth.headers({ password: "cli-secret", username: "bob" })).toEqual({
      Authorization: `Basic ${Buffer.from("bob:cli-secret").toString("base64")}`,
    })
  })

  test("validates decoded credentials against effect config", () => {
    const config = { password: Option.some("secret"), username: "alice" }

    expect(ServerAuth.required(config)).toBe(true)
    expect(ServerAuth.authorized({ username: "alice", password: Redacted.make("secret") }, config)).toBe(true)
    expect(ServerAuth.authorized({ username: "opencode", password: Redacted.make("secret") }, config)).toBe(false)
  })

  test("stores credentials in memory and scrubs child inheritance", () => {
    process.env.OPENCODE_SERVER_USERNAME = "inherited-user"
    process.env.OPENCODE_SERVER_PASSWORD = "inherited-password"
    process.env.OPENCODE_TUI_COORDINATOR_USERNAME = "coordinator-user"
    process.env.OPENCODE_TUI_COORDINATOR_PASSWORD = "coordinator-password"
    process.env.OPENCODE_TUI_COORDINATOR_TOKEN = "coordinator-token"

    ServerAuth.initialize({ username: "memory-user", password: "memory-password" })
    const child = spawnSync(process.execPath, ["-e", "console.log(JSON.stringify(process.env))"], {
      env: process.env,
      encoding: "utf8",
    })
    const inherited = JSON.parse(child.stdout) as Record<string, string>
    expect(inherited.OPENCODE_SERVER_USERNAME).toBeUndefined()
    expect(inherited.OPENCODE_SERVER_PASSWORD).toBeUndefined()
    expect(inherited.OPENCODE_TUI_COORDINATOR_USERNAME).toBeUndefined()
    expect(inherited.OPENCODE_TUI_COORDINATOR_PASSWORD).toBeUndefined()
    expect(inherited.OPENCODE_TUI_COORDINATOR_TOKEN).toBeUndefined()
    expect(ServerAuth.headers()).toEqual({
      Authorization: `Basic ${Buffer.from("memory-user:memory-password").toString("base64")}`,
    })
  })

  test("captures manual environment configuration once and scrubs it", () => {
    process.env.OPENCODE_SERVER_USERNAME = "manual-user"
    process.env.OPENCODE_SERVER_PASSWORD = "manual-password"

    expect(ServerAuth.capture()).toEqual({ username: "manual-user", password: Option.some("manual-password") })
    expect(process.env.OPENCODE_SERVER_USERNAME).toBeUndefined()
    expect(process.env.OPENCODE_SERVER_PASSWORD).toBeUndefined()
    expect(ServerAuth.headers()).toEqual({
      Authorization: `Basic ${Buffer.from("manual-user:manual-password").toString("base64")}`,
    })
  })
})
