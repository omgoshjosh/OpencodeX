import type { ChildProcess } from "node:child_process"
import { once } from "node:events"
import { finished } from "node:stream/promises"

export type SidecarBootstrap = {
  version: 1
  username: string
  password: string
  token: string
}

const PRIVILEGED_ENV = [
  /^OPENCODE(?:X)?_(?:TUI_)?COORDINATOR_/,
  /^(?:VITE_)?OPENCODE(?:X)?_(?:TUI_)?SERVER_(?:USERNAME|PASSWORD)$/,
]

export function sidecarChildEnvironment(environment: NodeJS.ProcessEnv, additions: NodeJS.ProcessEnv = {}) {
  return Object.fromEntries(
    Object.entries({ ...environment, ...additions }).filter(
      ([key]) => key in additions || !PRIVILEGED_ENV.some((pattern) => pattern.test(key)),
    ),
  )
}

export async function writeSidecarBootstrap(child: ChildProcess, bootstrap: SidecarBootstrap) {
  try {
    const stream = child.stdio[3]
    if (!stream || typeof stream === "number" || !("end" in stream)) {
      throw new Error("Coordinator bootstrap pipe was not created")
    }
    stream.end(JSON.stringify(bootstrap))
    await finished(stream)
  } catch (error) {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = once(child, "close")
      child.kill()
      await exited
    }
    throw error
  }
}
