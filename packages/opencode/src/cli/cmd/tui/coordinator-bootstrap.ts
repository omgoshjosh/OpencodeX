import fs from "node:fs"
import type { ChildProcess } from "node:child_process"
import { once } from "node:events"
import { finished } from "node:stream/promises"

export const COORDINATOR_BOOTSTRAP_FD_ENV = "OPENCODE_COORDINATOR_BOOTSTRAP_FD"
const MAX_BOOTSTRAP_BYTES = 16_384
const PRIVILEGED_ENV = [
  /^OPENCODE(?:X)?_(?:TUI_)?COORDINATOR_/,
  /^(?:VITE_)?OPENCODE(?:X)?_(?:TUI_)?SERVER_(?:USERNAME|PASSWORD)$/,
]

export type CoordinatorBootstrap = {
  version: 1
  username: string
  password: string
  token: string
}

export function coordinatorChildEnvironment(environment: NodeJS.ProcessEnv, additions: NodeJS.ProcessEnv = {}) {
  return Object.fromEntries(
    Object.entries({ ...environment, ...additions }).filter(
      ([key]) => key in additions || !PRIVILEGED_ENV.some((pattern) => pattern.test(key)),
    ),
  )
}

export function readCoordinatorBootstrap(required: true): CoordinatorBootstrap
export function readCoordinatorBootstrap(required: false): CoordinatorBootstrap | undefined
export function readCoordinatorBootstrap(required: boolean): CoordinatorBootstrap | undefined {
  const descriptor = process.env[COORDINATOR_BOOTSTRAP_FD_ENV]
  if (!descriptor) {
    if (required) throw new Error("Coordinator bootstrap is required")
    return undefined
  }
  if (descriptor !== "3") throw new Error("Coordinator bootstrap must use inherited descriptor 3")
  const chunks: Buffer[] = []
  let size = 0
  try {
    while (true) {
      const chunk = Buffer.allocUnsafe(Math.min(4_096, MAX_BOOTSTRAP_BYTES + 1 - size))
      const read = fs.readSync(3, chunk, 0, chunk.length, null)
      if (read === 0) break
      size += read
      if (size > MAX_BOOTSTRAP_BYTES) throw new Error("Coordinator bootstrap exceeds maximum size")
      chunks.push(chunk.subarray(0, read))
    }
  } finally {
    fs.closeSync(3)
    delete process.env[COORDINATOR_BOOTSTRAP_FD_ENV]
  }
  const value = (() => {
    try {
      return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown
    } catch (error) {
      if (size === MAX_BOOTSTRAP_BYTES) throw new Error("Coordinator bootstrap exceeds maximum size")
      throw error
    }
  })()
  if (!isBootstrap(value)) throw new Error("Invalid coordinator bootstrap")
  return value
}

function isBootstrap(value: unknown): value is CoordinatorBootstrap {
  if (typeof value !== "object" || value === null) return false
  if (Object.keys(value).some((key) => !["version", "username", "password", "token"].includes(key))) return false
  const input = value as Partial<CoordinatorBootstrap>
  return (
    input.version === 1 &&
    bounded(input.username, 1, 128) &&
    bounded(input.password, 32, 256) &&
    bounded(input.token, 32, 256)
  )
}

function bounded(value: unknown, minimum: number, maximum: number): value is string {
  return typeof value === "string" && value.length >= minimum && value.length <= maximum
}

export function writeCoordinatorBootstrap(stream: NodeJS.WritableStream, bootstrap: CoordinatorBootstrap) {
  stream.end(JSON.stringify(bootstrap))
  return finished(stream)
}

export async function writeCoordinatorBootstrapToChild(child: ChildProcess, bootstrap: CoordinatorBootstrap) {
  try {
    const stream = child.stdio[3]
    if (!stream || typeof stream === "number" || !("end" in stream)) {
      throw new Error("Coordinator bootstrap pipe was not created")
    }
    await writeCoordinatorBootstrap(stream, bootstrap)
  } catch (error) {
    await terminateChild(child)
    throw error
  }
}

async function terminateChild(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return
  const exited = once(child, "close")
  child.kill()
  await exited
}

export * as CoordinatorBootstrap from "./coordinator-bootstrap"
