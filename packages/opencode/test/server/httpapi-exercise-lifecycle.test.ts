import { expect, test } from "bun:test"
import { mkdtemp, rm } from "fs/promises"
import os from "os"
import path from "path"

test("resets the HTTP exerciser database after sequential OpencodeX project scenarios", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "opencode-httpapi-lifecycle-"))
  try {
    const child = Bun.spawn(
      [
        process.execPath,
        "run",
        "script/httpapi-exercise.ts",
        "--mode",
        "effect",
        "--start-at",
        "opencodex.project.list",
        "--stop-at",
        "opencodex.project.create",
      ],
      {
        cwd: path.resolve(import.meta.dir, "../.."),
        env: { ...process.env, TMPDIR: directory },
      },
    )
    const output = await new Response(child.stdout).text()
    expect(await child.exited).toBe(0)
    expect(output).toContain("summary pass=2 fail=0 skip=0")
    expect(await Bun.file(path.join(directory, `opencode-httpapi-exercise-${child.pid}.db`)).exists()).toBe(false)
    expect(await Bun.file(path.join(directory, `opencode-httpapi-exercise-${child.pid}.db-wal`)).exists()).toBe(false)
    expect(await Bun.file(path.join(directory, `opencode-httpapi-exercise-${child.pid}.db-shm`)).exists()).toBe(false)
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})
