import { describe, expect, test } from "bun:test"
import { spawn } from "node:child_process"
import { sidecarChildEnvironment, writeSidecarBootstrap } from "../src/main/sidecar-bootstrap"

const bootstrap = {
  version: 1 as const,
  username: "opencodex-local",
  password: "sidecar-password-00000000000000000001",
  token: "sidecar-token-00000000000000000000001",
}

describe("sidecar bootstrap", () => {
  test("writes ordinary attach credentials through descriptor 3", async () => {
    const child = spawn(
      process.execPath,
      [
        "-e",
        `
          import fs from "node:fs"
          const chunks = []
          const buffer = Buffer.alloc(4096)
          for (;;) {
            const size = fs.readSync(3, buffer, 0, buffer.length, null)
            if (!size) break
            chunks.push(buffer.subarray(0, size))
          }
          const value = JSON.parse(Buffer.concat(chunks).toString("utf8"))
          console.log(JSON.stringify({ username: value.username, hasPassword: !!value.password, hasToken: !!value.token }))
        `,
      ],
      { stdio: ["ignore", "pipe", "pipe", "pipe"] },
    )

    await writeSidecarBootstrap(child, bootstrap)
    const [output, exit] = await Promise.all([
      new Response(child.stdout as ReadableStream).text(),
      new Promise<number | null>((resolve) => child.once("exit", resolve)),
    ])
    expect(exit).toBe(0)
    expect(JSON.parse(output)).toEqual({ username: bootstrap.username, hasPassword: true, hasToken: true })
  })

  test("terminates and reaps the child when descriptor setup fails", async () => {
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: ["ignore", "ignore", "ignore", "ignore"],
    })

    await expect(writeSidecarBootstrap(child, bootstrap)).rejects.toThrow("pipe was not created")
    expect(child.exitCode !== null || child.signalCode !== null).toBe(true)
  })

  test("sanitizes privileged parent variables before child application imports", async () => {
    const environment = sidecarChildEnvironment(
      {
        ...process.env,
        OPENCODE_COORDINATOR_HANDOFF_CAPABILITY: "sentinel-capability",
        OPENCODE_COORDINATOR_AUTHORITY_EPOCH: "sentinel-epoch",
        OPENCODE_COORDINATOR_READINESS: "sentinel-readiness",
        OPENCODE_COORDINATOR_BOOTSTRAP_FD: "sentinel-fd",
        OPENCODE_TUI_COORDINATOR_USERNAME: "sentinel-username",
        OPENCODE_TUI_COORDINATOR_PASSWORD: "sentinel-password",
        OPENCODE_TUI_COORDINATOR_TOKEN: "sentinel-token",
        OPENCODE_SERVER_USERNAME: "sentinel-server-username",
        OPENCODE_SERVER_PASSWORD: "sentinel-server-password",
        VITE_OPENCODEX_SERVER_PASSWORD: "sentinel-renderer-alias",
      },
      { OPENCODE_COORDINATOR_BOOTSTRAP_FD: "3" },
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

    await writeSidecarBootstrap(child, bootstrap)
    const inherited = JSON.parse(await output) as Record<string, string>
    const closed = new Promise((resolve) => child.once("close", resolve))
    child.kill()
    await closed
    expect(inherited.OPENCODE_COORDINATOR_BOOTSTRAP_FD).toBe("3")
    expect(Object.values(inherited)).not.toContain("sentinel-capability")
    expect(Object.values(inherited)).not.toContain("sentinel-epoch")
    expect(Object.values(inherited)).not.toContain("sentinel-readiness")
    expect(Object.values(inherited)).not.toContain("sentinel-username")
    expect(Object.values(inherited)).not.toContain("sentinel-password")
    expect(Object.values(inherited)).not.toContain("sentinel-token")
    expect(Object.values(inherited)).not.toContain("sentinel-server-password")
    expect(Object.values(inherited)).not.toContain("sentinel-renderer-alias")
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
