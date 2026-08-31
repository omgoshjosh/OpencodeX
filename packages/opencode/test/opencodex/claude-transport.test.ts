import { describe, expect, test } from "bun:test"
import { DELEGATE_SERVER, DELEGATE_TOOL, claudePrompt, delegateServer } from "../../src/opencodex/claude-transport"

function fakeSdk() {
  const calls: { tool?: { name: string; description: string; extras?: Record<string, unknown> }; server?: Record<string, unknown> } = {}
  return {
    calls,
    sdk: {
      tool: (name: string, description: string, _schema: unknown, _handler: unknown, extras?: Record<string, unknown>) => {
        calls.tool = { name, description, extras }
        return { name, description }
      },
      createSdkMcpServer: (input: Record<string, unknown>) => {
        calls.server = input
        return input
      },
    } as unknown as typeof import("@anthropic-ai/claude-agent-sdk"),
  }
}

describe("delegateServer", () => {
  test("registers the delegate tool on the swarm server", () => {
    const { sdk, calls } = fakeSdk()
    delegateServer(sdk, { roles: [{ name: "Researcher 1" }], run: async () => "ok" })
    expect(calls.server?.name).toBe(DELEGATE_SERVER)
    expect(calls.tool?.name).toBe(DELEGATE_TOOL)
    expect(calls.tool?.description).toContain("Researcher 1")
  })

  test("marks the delegate tool concurrency-safe so parallel role calls actually run in parallel", () => {
    // The CLI executes in-process MCP tools serially unless the tool's
    // annotations mark it read-only: `isConcurrencySafe()` is
    // `annotations?.readOnlyHint ?? false`. Without this, an orchestrator
    // fanning two ten-minute roles out "in parallel" runs them back to back -
    // the second role never starts until the first returns.
    const { sdk, calls } = fakeSdk()
    delegateServer(sdk, { roles: [{ name: "Researcher 1" }], run: async () => "ok" })
    expect(calls.tool?.extras).toMatchObject({ annotations: { readOnlyHint: true } })
  })
})

describe("claudePrompt", () => {
  test("keeps text-only prompts as strings", () => {
    expect(claudePrompt("Describe this.", [])).toBe("Describe this.")
  })

  test("sends mixed, image-only, and multiple persisted images as native blocks", async () => {
    const mixed = claudePrompt("Describe these.", [
      { mime: "image/png", url: "data:image/png;base64,aGVsbG8=" },
      { mime: "image/gif", url: "data:image/gif;base64,d29ybGQ=" },
    ])
    const imageOnly = claudePrompt("", [{ mime: "image/webp", url: "data:image/webp;base64,aGVsbG8=" }])
    expect(await first(mixed)).toMatchObject({
      message: { content: [{ type: "text", text: "Describe these." }, { type: "image" }, { type: "image" }] },
    })
    expect(await first(imageOnly)).toMatchObject({ message: { content: [{ type: "image" }] } })
  })

  test("makes malformed and unsupported attachments visible text instead of failing", async () => {
    const prompt = claudePrompt("", [
      { mime: "image/svg+xml", url: "data:image/svg+xml;base64,aGVsbG8=" },
      { mime: "image/png", url: "data:image/png;base64," },
    ])
    expect(await first(prompt)).toMatchObject({
      message: {
        content: [
          { type: "text", text: "[Unsupported image attachment: image/svg+xml]" },
          { type: "text", text: "[Unsupported image attachment: image/png]" },
        ],
      },
    })
  })
})

async function first(prompt: ReturnType<typeof claudePrompt>) {
  if (typeof prompt === "string") throw new Error("expected image prompt")
  return (await prompt[Symbol.asyncIterator]().next()).value
}
