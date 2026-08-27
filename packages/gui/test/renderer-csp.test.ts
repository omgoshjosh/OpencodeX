import { expect, test } from "bun:test"
import path from "node:path"

test("renderer HTML carries no broad static policy", async () => {
  const html = await Bun.file(path.join(import.meta.dir, "../src/renderer/index.html")).text()

  expect(html).not.toContain("http: https: ws: wss:")
  // Deliberately NOT asserting the absence of a meta CSP. The policy is
  // delivered as a response header (proven enforced on the packaged app's
  // file:// renderer by renderer-csp-file-protocol.test.ts), but a meta tag
  // would be a legitimate belt-and-braces addition - and an assertion that
  // forbids one would turn adding a second layer of defence into a test
  // failure. Only the over-broad policy this fork removed is pinned.
})

test("Electron and Vite policies add only the configured exact origin", async () => {
  const main = await Bun.file(path.join(import.meta.dir, "../src/main/index.ts")).text()
  const vite = await Bun.file(path.join(import.meta.dir, "../vite.config.mjs")).text()

  expect(main).toContain("configuredBackendConnectSource(loadConfiguredBackend())")
  expect(main.lastIndexOf("registerContentSecurityPolicy()")).toBeLessThan(main.indexOf("return openWindow()"))
  expect(main).toContain('"Content-Security-Policy": [rendererContentSecurityPolicy()]')
  expect(main).not.toContain("const configuredBackend = configuredBackendConnection()")
  expect(main).toContain("return failedGuiConnection(error)")
  expect(vite).toContain("new URL(process.env.VITE_OPENCODEX_SERVER_URL).origin")
  expect(main).not.toContain('"http:", "https:", "ws:", "wss:"')
  expect(vite).not.toContain('"http:", "https:", "ws:", "wss:"')
})
