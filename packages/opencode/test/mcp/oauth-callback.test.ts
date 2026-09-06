import { test, expect, describe, afterEach } from "bun:test"
import { createConnection, createServer as createNetServer } from "net"
import { McpOAuthCallback } from "../../src/mcp/oauth-callback"
import { parseRedirectUri } from "../../src/mcp/oauth-provider"

async function getFreeLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createNetServer()
    probe.once("error", reject)
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address()
      probe.close(() => {
        if (typeof address === "object" && address) {
          resolve(address.port)
          return
        }
        reject(new Error("Could not allocate a loopback port"))
      })
    })
  })
}

async function canConnect(host: string, port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ host, port })
    const done = (ok: boolean) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(ok)
    }

    socket.setTimeout(500)
    socket.once("connect", () => done(true))
    socket.once("error", () => done(false))
    socket.once("timeout", () => done(false))
  })
}

describe("parseRedirectUri", () => {
  test("returns defaults when no URI provided", () => {
    const result = parseRedirectUri()
    expect(result.port).toBe(19876)
    expect(result.path).toBe("/mcp/oauth/callback")
  })

  test("parses port and path from URI", () => {
    const result = parseRedirectUri("http://127.0.0.1:8080/oauth/callback")
    expect(result.port).toBe(8080)
    expect(result.path).toBe("/oauth/callback")
  })

  test("returns defaults for invalid URI", () => {
    const result = parseRedirectUri("not-a-valid-url")
    expect(result.port).toBe(19876)
    expect(result.path).toBe("/mcp/oauth/callback")
  })
})

describe("McpOAuthCallback.ensureRunning", () => {
  afterEach(async () => {
    await McpOAuthCallback.stop()
  })

  test("starts server with custom redirectUri port and path", async () => {
    await McpOAuthCallback.ensureRunning("http://127.0.0.1:18000/custom/callback")
    expect(McpOAuthCallback.isRunning()).toBe(true)
  })

  test("stops after the callback completes", async () => {
    const redirectUri = "http://127.0.0.1:18003/custom/callback"
    await McpOAuthCallback.ensureRunning(redirectUri)
    const callback = McpOAuthCallback.waitForCallback("success")

    const response = await fetch(`${redirectUri}?code=code&state=success`)

    expect(response.status).toBe(200)
    expect(await callback).toBe("code")
    expect(McpOAuthCallback.isRunning()).toBe(false)
  })

  test("escapes provider error markup in callback HTML", async () => {
    const redirectUri = "http://127.0.0.1:18001/custom/callback"
    await McpOAuthCallback.ensureRunning(redirectUri)

    const error = `<script>alert("xss" & 'more')</script>`
    const response = await fetch(
      `${redirectUri}?state=test&error=access_denied&error_description=${encodeURIComponent(error)}`,
    )
    const body = await response.text()

    expect(response.headers.get("content-type")).toBe("text/html; charset=utf-8")
    expect(body).toContain("&lt;script&gt;alert(&quot;xss&quot; &amp; &#39;more&#39;)&lt;/script&gt;")
    expect(body).not.toContain(error)
  })

  test("keeps normal provider errors readable", async () => {
    const redirectUri = "http://127.0.0.1:18002/custom/callback"
    await McpOAuthCallback.ensureRunning(redirectUri)

    const response = await fetch(
      `${redirectUri}?state=test&error=access_denied&error_description=${encodeURIComponent("The user denied access")}`,
    )

    expect(await response.text()).toContain('<div class="error">The user denied access</div>')
  })

  test("binds the callback server to IPv4 loopback", async () => {
    const port = await getFreeLoopbackPort()
    await McpOAuthCallback.ensureRunning(`http://127.0.0.1:${port}/custom/callback`)

    expect(await canConnect("127.0.0.1", port)).toBe(true)
    expect(await canConnect("::1", port)).toBe(false)
  })
})

// Resolves to the rejection reason, or `undefined` if the promise fulfilled.
async function settle(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => undefined,
    (error: unknown) => error,
  )
}

describe("McpOAuthCallback.waitForCallback", () => {
  afterEach(async () => {
    await McpOAuthCallback.stop()
  })

  // `MCP.startAuth` creates the callback promise, opens a browser, and only then awaits
  // it, so a `cancelPending` from a concurrent `MCP.removeAuth` can reject it while no
  // handler is attached. An unhandled rejection is fatal by default under Bun and Node,
  // so the rejection has to be observed by `waitForCallback` itself.
  test("a cancellation before the caller awaits is not an unhandled rejection", async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on("unhandledRejection", onUnhandled)

    try {
      const callback = McpOAuthCallback.waitForCallback("state-cancelled-early", "cancel-me")
      McpOAuthCallback.cancelPending("cancel-me")

      // An unhandled rejection is only reported once the microtask queue has drained.
      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(unhandled).toEqual([])

      // The caller that awaits later still sees the cancellation.
      expect(await settle(callback)).toEqual(new Error("Authorization cancelled"))
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("a server stop before the caller awaits is not an unhandled rejection", async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => unhandled.push(reason)
    process.on("unhandledRejection", onUnhandled)

    try {
      const callback = McpOAuthCallback.waitForCallback("state-stopped-early", "stop-me")
      await McpOAuthCallback.stop()

      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(unhandled).toEqual([])

      expect(await settle(callback)).toEqual(new Error("OAuth callback server stopped"))
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })
})
