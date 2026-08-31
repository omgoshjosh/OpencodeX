import { test } from "bun:test"
import { checkLintBaseline } from "./check-lint-baseline"

test("parses an Oxlint report larger than Bun's pipe capture limit", async () => {
  const command = [
    process.execPath,
    "-e",
    'process.stdout.write(JSON.stringify({ diagnostics: [{ severity: "warning", message: "x".repeat(1024 * 1024) }] }))',
  ]

  await checkLintBaseline(command, 1)
})
