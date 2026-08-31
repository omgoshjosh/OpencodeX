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
  const baseline = JSON.parse(await Bun.file(import.meta.dir + "/../.oxlint-baseline.json").text())
  if (!record(baseline) || typeof baseline.warnings !== "number") throw new Error("Invalid .oxlint-baseline.json")
  await checkLintBaseline(["bunx", "oxlint", "--format", "json"], baseline.warnings)
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function format(diagnostic: Record<string, unknown>) {
  return [diagnostic.filename, diagnostic.code, diagnostic.message]
    .filter((value) => typeof value === "string")
    .join(": ")
}
