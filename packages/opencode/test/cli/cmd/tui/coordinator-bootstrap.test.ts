import { describe, expect, test } from "bun:test"
import { spawn } from "node:child_process"
import { Writable } from "node:stream"
import {
  COORDINATOR_BOOTSTRAP_FD_ENV,
  coordinatorChildEnvironment,
  writeCoordinatorBootstrap,
  writeCoordinatorBootstrapToChild,
} from "../../../../src/cli/cmd/tui/coordinator-bootstrap"

const modulePath = new URL("../../../../src/cli/cmd/tui/coordinator-bootstrap.ts", import.meta.url).pathname
const bootstrap = {
  version: 1 as const,
  username: "coordinator",
  password: "bootstrap-password-0000000000000001",
  token: "bootstrap-token-0000000000000000000001",
}

async function readStream(stream: NodeJS.ReadableStream) {
  const chunks: Buffer[] = []
  for await (const chunk of stream) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks).toString("utf8")
}

async function runBootstrap(script: string, payload?: unknown) {
  const command = [process.execPath, "--conditions=browser", "-e", script]
  const child = spawn(command[0], command.slice(1), {
    env: payload === undefined ? { ...process.env } : { ...process.env, [COORDINATOR_BOOTSTRAP_FD_ENV]: "3" },
    stdio: ["ignore", "pipe", "pipe", payload === undefined ? "ignore" : "pipe"],
  })
  if (payload !== undefined) {
    const pipe = child.stdio[3]
    if (!pipe || typeof pipe === "number" || !("end" in pipe)) throw new Error("bootstrap pipe was not created")
    pipe.end(JSON.stringify(payload))
  }
  const [stdout, stderr, exit] = await Promise.all([
    readStream(child.stdout!),
    readStream(child.stderr!),
    new Promise<number | null>((resolve) => child.once("exit", resolve)),
  ])
  return { stdout, stderr, exit, argv: command }
}

const readScript = `
  import fs from "node:fs"
  import { readCoordinatorBootstrap, COORDINATOR_BOOTSTRAP_FD_ENV } from ${JSON.stringify(modulePath)}
  try {
    const value = readCoordinatorBootstrap(true)
    let closed = false
    try { fs.fstatSync(3) } catch { closed = true }
    let oneShot = false
    try { readCoordinatorBootstrap(true) } catch { oneShot = true }
    console.log(JSON.stringify({ ok: true, username: value.username, closed, cleared: process.env[COORDINATOR_BOOTSTRAP_FD_ENV] === undefined, oneShot }))
  } catch (error) {
    console.log(JSON.stringify({ ok: false, message: error instanceof Error ? error.message : String(error) }))
  }
`

describe("coordinator bootstrap", () => {
  test("reads descriptor 3 once, closes it, and clears its locator", async () => {
    const result = await runBootstrap(readScript, bootstrap)

    expect(result.exit).toBe(0)
    expect(result.stderr).toBe("")
    expect(JSON.parse(result.stdout)).toEqual({
      ok: true,
      username: bootstrap.username,
      closed: true,
      cleared: true,
      oneShot: true,
    })
    expect(result.argv.join(" ")).not.toContain(bootstrap.password)
  })

  test("rejects unknown fields and values outside protocol bounds", async () => {
    const unknown = await runBootstrap(readScript, { ...bootstrap, extraSecret: "not-allowed" })
    const short = await runBootstrap(readScript, { ...bootstrap, password: "short" })
    const oversized = await runBootstrap(readScript, { ...bootstrap, token: "x".repeat(17_000) })

    expect(JSON.parse(unknown.stdout)).toEqual({ ok: false, message: "Invalid coordinator bootstrap" })
    expect(JSON.parse(short.stdout)).toEqual({ ok: false, message: "Invalid coordinator bootstrap" })
    expect(JSON.parse(oversized.stdout)).toEqual({
      ok: false,
      message: "Coordinator bootstrap exceeds maximum size",
    })
  })

  test("requires the bootstrap and rejects descriptors other than 3", async () => {
    const missing = await runBootstrap(readScript)
    const wrong = await runBootstrap(
      `
        import { readCoordinatorBootstrap } from ${JSON.stringify(modulePath)}
        process.env.${COORDINATOR_BOOTSTRAP_FD_ENV} = "4"
        try { readCoordinatorBootstrap(true) } catch (error) { console.log(error instanceof Error ? error.message : String(error)) }
      `,
    )

    expect(JSON.parse(missing.stdout)).toEqual({ ok: false, message: "Coordinator bootstrap is required" })
    expect(wrong.stdout.trim()).toBe("Coordinator bootstrap must use inherited descriptor 3")
  })

  test("rejects write errors", async () => {
    const stream = new Writable({
      write(_chunk, _encoding, callback) {
        callback(new Error("injected pipe failure"))
      },
    })

    await expect(writeCoordinatorBootstrap(stream, bootstrap)).rejects.toThrow("injected pipe failure")
  })

  test("terminates and reaps a spawned coordinator when bootstrap setup fails", async () => {
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: ["ignore", "ignore", "ignore", "ignore"],
    })

    await expect(writeCoordinatorBootstrapToChild(child, bootstrap)).rejects.toThrow("pipe was not created")
    expect(child.exitCode !== null || child.signalCode !== null).toBe(true)
  })

  test("sanitizes privileged parent variables before child application imports", async () => {
    const environment = coordinatorChildEnvironment(
      {
        ...process.env,
        OPENCODE_COORDINATOR_HANDOFF_CAPABILITY: "sentinel-capability",
        OPENCODE_COORDINATOR_AUTHORITY_EPOCH: "sentinel-epoch",
        OPENCODE_COORDINATOR_READY: "sentinel-ready",
        OPENCODE_COORDINATOR_BOOTSTRAP_FD: "sentinel-fd",
        OPENCODE_TUI_COORDINATOR_USERNAME: "sentinel-username",
        OPENCODE_TUI_COORDINATOR_PASSWORD: "sentinel-password",
        OPENCODE_TUI_COORDINATOR_TOKEN: "sentinel-token",
        OPENCODE_SERVER_USERNAME: "sentinel-server-username",
        OPENCODE_SERVER_PASSWORD: "sentinel-server-password",
        OPENCODEX_SERVER_PASSWORD: "sentinel-server-alias",
      },
      { [COORDINATOR_BOOTSTRAP_FD_ENV]: "3" },
    )
    const child = spawn(
      process.execPath,
      ["-e", "console.log(JSON.stringify(process.env)); setInterval(() => {}, 1000)"],
      {
        env: environment,
        stdio: ["ignore", "pipe", "ignore", "pipe"],
      },
    )
    const output = readLine(child.stdout!)

    await writeCoordinatorBootstrapToChild(child, bootstrap)
    const inherited = JSON.parse(await output) as Record<string, string>
    const closed = new Promise((resolve) => child.once("close", resolve))
    child.kill()
    await closed
    expect(inherited[COORDINATOR_BOOTSTRAP_FD_ENV]).toBe("3")
    expect(Object.values(inherited)).not.toContain("sentinel-capability")
    expect(Object.values(inherited)).not.toContain("sentinel-epoch")
    expect(Object.values(inherited)).not.toContain("sentinel-ready")
    expect(Object.values(inherited)).not.toContain("sentinel-username")
    expect(Object.values(inherited)).not.toContain("sentinel-password")
    expect(Object.values(inherited)).not.toContain("sentinel-token")
    expect(Object.values(inherited)).not.toContain("sentinel-server-password")
    expect(Object.values(inherited)).not.toContain("sentinel-server-alias")
  })
})

function readLine(stream: NodeJS.ReadableStream) {
  return new Promise<string>((resolve) => {
    let output = ""
    stream.on("data", (chunk) => {
      output += String(chunk)
      const newline = output.indexOf("\n")
      if (newline !== -1) resolve(output.slice(0, newline))
    })
  })
}
