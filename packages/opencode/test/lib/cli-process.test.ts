import { expect, test } from "bun:test"
import { cliProcessDefaultTimeout } from "./cli-process"

test("CLI child default timeout gives hosted Windows startup headroom", () => {
  expect(cliProcessDefaultTimeout("win32", undefined)).toBe(90_000)
  expect(cliProcessDefaultTimeout("darwin", undefined)).toBe(45_000)
  expect(cliProcessDefaultTimeout("win32", "15000")).toBe(15_000)
})
