import { expect, test } from "bun:test"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { checkLintBaseline, lintBaselineStale, lintBaselineWarnings } from "./check-lint-baseline"

function repoWithCommits(commits: { file: string; at: string }[]) {
  const root = mkdtempSync(join(tmpdir(), "oxlint-baseline-"))
  const git = (args: string[], at: string) =>
    Bun.spawnSync(["git", ...args], {
      cwd: root,
      env: {
        ...process.env,
        GIT_AUTHOR_DATE: at,
        GIT_COMMITTER_DATE: at,
        GIT_AUTHOR_NAME: "t",
        GIT_AUTHOR_EMAIL: "t@t",
        GIT_COMMITTER_NAME: "t",
        GIT_COMMITTER_EMAIL: "t@t",
      },
    })
  git(["init", "-q"], "2000-01-01T00:00:00Z")
  for (const { file, at } of commits) {
    writeFileSync(join(root, file), at)
    git(["add", file], at)
    git(["commit", "-q", "-m", file], at)
  }
  return root
}

test("flags a baseline committed before the lint config last changed", async () => {
  const root = repoWithCommits([
    { file: ".oxlint-baseline.json", at: "2026-01-01T00:00:00Z" },
    { file: ".oxlintrc.json", at: "2026-01-02T00:00:00Z" },
  ])
  expect(await lintBaselineStale(root)).toBe(true)
})

test("accepts a baseline committed with or after the lint config", async () => {
  const root = repoWithCommits([
    { file: ".oxlintrc.json", at: "2026-01-01T00:00:00Z" },
    { file: ".oxlint-baseline.json", at: "2026-01-02T00:00:00Z" },
  ])
  expect(await lintBaselineStale(root)).toBe(false)
  expect(await lintBaselineStale(import.meta.dir + "/..")).toBe(false)
})

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
    checkLintBaseline(
      [process.execPath, "-e", 'process.stdout.write(JSON.stringify({ diagnostics: [{ severity: "warning" }] }))'],
      0,
    ),
  ).rejects.toThrow("Oxlint warnings increased from 0 to 1."))

test("resolves numeric and per-platform warning baselines", () => {
  expect(lintBaselineWarnings({ warnings: 4 }, "linux")).toBe(4)
  expect(lintBaselineWarnings({ warnings: { darwin: 3, default: 5 } }, "darwin")).toBe(3)
  expect(lintBaselineWarnings({ warnings: { darwin: 3, default: 5 } }, "linux")).toBe(5)
})
