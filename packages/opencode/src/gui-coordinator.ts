import { ensureRunID, OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"
import { Effect } from "effect"
import { CoordinatorBootstrap } from "@/cli/cmd/tui/coordinator-bootstrap"

const usage = "Usage: opencode-gui-coordinator <directory> --key <coordinator-key>"

function parseArgs(args: string[]) {
  if (args.length !== 3 || args[1] !== "--key" || args[0]?.startsWith("-") || !args[2] || args[2].startsWith("-")) {
    throw new Error(usage)
  }
  return { directory: args[0], key: args[2] }
}

try {
  const args = parseArgs(process.argv.slice(2))
  process.env[OPENCODE_PROCESS_ROLE] ??= "coordinator"
  ensureRunID()
  process.env.AGENT = "1"
  process.env.OPENCODE = "1"
  process.env.OPENCODEX = "1"
  process.env.OPENCODE_PID = String(process.pid)

  const { initializeCoordinatorProcess, runCoordinator } = await import("@/cli/cmd/tui/coordinator-runner")
  const bootstrap = initializeCoordinatorProcess(CoordinatorBootstrap.readCoordinatorBootstrap(true))
  const runtime = await import("@/gui-coordinator-runtime")
  await Effect.runPromise(runtime.initializeGuiCoordinator())
  await Effect.runPromise(
    runCoordinator({
      ...args,
      bootstrap,
      beforeStart: runtime.migrateGuiCoordinatorDatabase(),
    }),
  )
} catch (error) {
  const message =
    error instanceof Error
      ? error.stack?.includes(error.message)
        ? error.stack
        : [error.message, error.stack].filter(Boolean).join("\n")
      : String(error)
  process.stderr.write(message + "\n")
  process.exitCode = 1
}
