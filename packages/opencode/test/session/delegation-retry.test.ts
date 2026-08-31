import { describe, expect, test } from "bun:test"
import { DelegationRetry } from "../../src/session/delegation-retry"

describe("session.delegation-retry", () => {
  test("uses the current primary then ordered untried fallbacks", () => {
    const routes = [
      { providerID: "current", modelID: "primary" },
      { providerID: "fallback", modelID: "first" },
      { providerID: "fallback", modelID: "second" },
    ]
    expect(DelegationRetry.selectUntriedRoute(routes, ["old/primary"])).toEqual(routes[0])
    expect(DelegationRetry.selectUntriedRoute(routes, ["current/primary"])).toEqual(routes[1])
    expect(DelegationRetry.selectUntriedRoute(routes, ["current/primary", "fallback/first"])).toEqual(routes[2])
    expect(DelegationRetry.selectUntriedRoute(routes, routes.map((route) => `${route.providerID}/${route.modelID}`))).toBeUndefined()
  })

  test("accepts legacy roles without fallbacks and preserves attempted route metadata", () => {
    const routes = [{ providerID: "current", modelID: "primary" }]
    const attempted = ["retired/primary"]
    expect(DelegationRetry.selectUntriedRoute(routes, attempted)).toEqual(routes[0])
    expect(DelegationRetry.selectUntriedRoute(routes, [...attempted, "current/primary"])).toBeUndefined()
  })

  test("rejects partial assistant text, tool activity, and external side effects before replay", () => {
    const message = (parts: unknown[]) => ({ info: { role: "assistant" }, parts })
    expect(DelegationRetry.hasUnsafeRetryOutput(([message([{ type: "text", synthetic: false, text: "partial" }])] as never))).toBe(true)
    expect(DelegationRetry.hasUnsafeRetryOutput(([message([{ type: "tool" }])] as never))).toBe(true)
    expect(
      DelegationRetry.hasUnsafeRetryOutput(
        ([message([{ type: "tool", state: { status: "completed", externalSideEffect: true } }])] as never),
      ),
    ).toBe(true)
    expect(DelegationRetry.hasUnsafeRetryOutput(([message([{ type: "text", synthetic: true, text: "briefing" }])] as never))).toBe(false)
  })
})
