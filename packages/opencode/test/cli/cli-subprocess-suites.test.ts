import { expect, test } from "bun:test"
import path from "node:path"
import { cliSubprocessCommand, cliSubprocessSuites } from "../../script/cli-subprocess-suites"

test("CLI subprocess manifest covers every tracked direct cli-process test", async () => {
  const root = path.resolve(import.meta.dir, "../../../..")
  const process = Bun.spawn(["git", "ls-files", "packages/opencode/test"], { cwd: root, stdout: "pipe" })
  const [files, code] = await Promise.all([new Response(process.stdout).text(), process.exited])
  expect(code).toBe(0)

  const subprocessTests = (
    await Promise.all(
      files
        .trim()
        .split("\n")
        .filter((file) => file.endsWith(".test.ts") || file.endsWith(".test.tsx"))
        .map(async (file) => ({ file, source: await Bun.file(path.join(root, file)).text() })),
    )
  )
    .filter(({ source }) => /from ["'][^"']*lib\/cli-process["']/.test(source))
    .map(({ file }) => file.replace("packages/opencode/", ""))
    .toSorted()

  expect(cliSubprocessSuites.toSorted()).toEqual(subprocessTests)
})

test("CLI subprocess command serializes Windows without throttling other runners", () => {
  const concurrency = (command: string[]) => command[command.indexOf("--max-concurrency") + 1]
  expect(concurrency(cliSubprocessCommand("test/cli/run/run-process.test.ts", "win32"))).toBe("1")
  expect(concurrency(cliSubprocessCommand("test/cli/run/run-process.test.ts", "darwin"))).toBe("4")
})
