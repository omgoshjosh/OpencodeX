import { describe, expect, test } from "bun:test"
import { MessageV2 } from "../../src/session/message-v2"
import type { SessionLegacy } from "@opencode-ai/core/session/legacy"

function toolMessage(output: string): SessionLegacy.WithParts {
  return {
    info: { id: "msg_1", role: "assistant" },
    parts: [
      { id: "prt_text", type: "text", text: "hello" },
      {
        id: "prt_tool",
        type: "tool",
        tool: "bash",
        state: {
          status: "completed",
          output,
          metadata: { exit: 0 },
          time: { start: 1, end: 2 },
          input: {},
          title: "t",
        },
      },
    ],
  } as unknown as SessionLegacy.WithParts
}

describe("MessageV2.truncateToolOutputs", () => {
  const toolState = (item: SessionLegacy.WithParts | undefined) => {
    const part = item?.parts[1]
    if (part?.type !== "tool" || part.state.status !== "completed") throw new Error("expected a completed tool part")
    return part.state
  }

  test("caps oversized tool output, marks it, and leaves everything else alone", () => {
    const items = [toolMessage("x".repeat(5_000))]
    const capped = MessageV2.truncateToolOutputs(items, 1_000)
    const tool = toolState(capped[0])
    expect(tool.output.length).toBeLessThan(1_200)
    expect(tool.output).toContain("output truncated: 5000 chars")
    expect(tool.metadata).toMatchObject({ exit: 0, outputTruncated: true, outputLength: 5_000 })
    expect(capped[0]?.parts[0]).toBe(items[0]?.parts[0])
    // The source objects are untouched - they may serve an uncapped request.
    expect(toolState(items[0]).output.length).toBe(5_000)
  })

  test("returns items unchanged when nothing exceeds the budget", () => {
    const items = [toolMessage("short")]
    expect(MessageV2.truncateToolOutputs(items, 1_000)[0]).toBe(items[0])
  })
})
