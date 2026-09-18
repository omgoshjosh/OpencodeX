export async function checkLintBaseline(command: string[], baselineWarnings: number) {
  const stdoutFile = Bun.file(`${Bun.env.TMPDIR ?? "/tmp"}/oxlint-${crypto.randomUUID()}.stdout`)
  const stderrFile = Bun.file(`${Bun.env.TMPDIR ?? "/tmp"}/oxlint-${crypto.randomUUID()}.stderr`)
  const process = Bun.spawn(command, {
    cwd: import.meta.dir + "/..",
    stdout: stdoutFile,
    stderr: stderrFile,
  })
  try {
    const exitCode = await process.exited
    const [stdout, stderr] = await Promise.all([stdoutFile.text(), stderrFile.text()])
    const result = JSON.parse(stdout)
    if (!record(result) || !Array.isArray(result.diagnostics)) {
      throw new Error(`Oxlint returned an invalid report.\n${stderr}`)
    }
    const diagnostics = result.diagnostics.filter(record)
    const errors = diagnostics.filter((diagnostic) => diagnostic.severity === "error")
    const warnings = diagnostics.filter((diagnostic) => diagnostic.severity === "warning")

    if (errors.length > 0 || exitCode !== 0) {
      errors.slice(0, 20).forEach((diagnostic) => console.error(format(diagnostic)))
      throw new Error(`Oxlint reported ${errors.length} correctness error(s).`)
    }
    if (warnings.length > baselineWarnings) {
      throw new Error(`Oxlint warnings increased from ${baselineWarnings} to ${warnings.length}.`)
    }
    console.log(`Oxlint: ${warnings.length} warning(s), baseline ${baselineWarnings}.`)
  } finally {
    await Promise.all([stdoutFile.delete(), stderrFile.delete()])
  }
}

if (import.meta.main) {
  const root = import.meta.dir + "/.."
  if (await lintBaselineStale(root)) {
    throw new Error(
      ".oxlint-baseline.json was committed before .oxlintrc.json changed; re-measure and update its counts.",
    )
  }
  await checkLintBaseline(
    ["bunx", "oxlint", "--format", "json"],
    lintBaselineWarnings(JSON.parse(await Bun.file(root + "/.oxlint-baseline.json").text())),
  )
}

// Uses commit times, not mtimes: a fresh checkout gives every file the same mtime.
export async function lintBaselineStale(root: string, baseline = ".oxlint-baseline.json", config = ".oxlintrc.json") {
  const committedAt = async (file: string) => {
    const proc = Bun.spawn(["git", "log", "-1", "--format=%ct", "--", file], { cwd: root, stderr: "ignore" })
    return Number((await new Response(proc.stdout).text()).trim())
  }
  const [baselineAt, configAt] = await Promise.all([committedAt(baseline), committedAt(config)])
  return baselineAt > 0 && configAt > baselineAt
}

export function lintBaselineWarnings(baseline: unknown, platform = globalThis.process.platform) {
  if (!record(baseline)) throw new Error("Invalid .oxlint-baseline.json")
  if (typeof baseline.warnings === "number") return baseline.warnings
  if (!record(baseline.warnings)) throw new Error("Invalid .oxlint-baseline.json")
  const warnings = baseline.warnings[platform] ?? baseline.warnings.default
  if (typeof warnings !== "number") throw new Error("Invalid .oxlint-baseline.json")
  return warnings
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function format(diagnostic: Record<string, unknown>) {
  return [diagnostic.filename, diagnostic.code, diagnostic.message]
    .filter((value) => typeof value === "string")
    .join(": ")
}
