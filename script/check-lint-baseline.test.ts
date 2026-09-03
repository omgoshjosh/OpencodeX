import { expect, test } from "bun:test"
import { checkLintBaseline, lintBaselineWarnings } from "./check-lint-baseline"

test("parses an Oxlint report larger than Bun's pipe capture limit", async () => {
  const command = [
    process.execPath,
    "-e",
    'process.stdout.write(JSON.stringify({ diagnostics: [{ severity: "warning", message: "x".repeat(1024 * 1024) }] }))',
  ]

  await checkLintBaseline(command, 1)
})

test("rejects warning regressions", () =>
  expect(
    checkLintBaseline([process.execPath, "-e", 'process.stdout.write(JSON.stringify({ diagnostics: [{ severity: "warning" }] }))'], 0),
  ).rejects.toThrow("Oxlint warnings increased from 0 to 1."),
)

test("resolves numeric and per-platform warning baselines", () => {
  expect(lintBaselineWarnings({ warnings: 4 }, "linux")).toBe(4)
  expect(lintBaselineWarnings({ warnings: { darwin: 3, default: 5 } }, "darwin")).toBe(3)
  expect(lintBaselineWarnings({ warnings: { darwin: 3, default: 5 } }, "linux")).toBe(5)
})
