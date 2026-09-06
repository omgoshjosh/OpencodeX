import { mkdir, rm } from "node:fs/promises"
import { closeSync, openSync } from "node:fs"
import path from "node:path"
import { cliSubprocessSuites } from "./cli-subprocess-suites"

const output = path.join(import.meta.dir, "../.artifacts/unit")
await rm(output, { recursive: true, force: true })
await mkdir(output, { recursive: true })

// Handled by test:ci:cli, in its own job. See cli-subprocess-suites.ts.
const excluded = new Set(cliSubprocessSuites)
console.log(`[test:ci] ${excluded.size} CLI subprocess suites run separately via test:ci:cli`)

const parallel = [...new Bun.Glob("**/*.test.{ts,tsx}").scanSync({ cwd: path.join(import.meta.dir, "../test") })]
  .map((file) => `test/${file.replaceAll("\\", "/")}`)
  .filter(
    (file) =>
      !file.startsWith("test/cli/tui/") &&
      !file.startsWith("test/cli/cmd/tui/") &&
      !file.startsWith("test/cli/run/") &&
      !file.startsWith("test/cli/acp/") &&
      !file.startsWith("test/cli/serve/") &&
      !file.startsWith("test/cli/smokes/") &&
      !file.startsWith("test/file/") &&
      !file.startsWith("test/project/") &&
      !file.startsWith("test/snapshot/") &&
      file !== "test/plugin/openai-ws.test.ts" &&
      file !== "test/session/compaction.test.ts" &&
      file !== "test/session/prompt.test.ts" &&
      file !== "test/server/httpapi-listen.test.ts" &&
      !excluded.has(file),
  )
  .toSorted()

/*
 * bun's per-test timeout is a hang detector here, not a work budget. Measured on
 * windows-latest across the two heaviest shards (2077 and 1867 cases): p50 1ms,
 * p95 573ms/188ms, p99 1609ms/801ms, slowest *passing* case 19.2s. Nothing
 * legitimate is within 10s of the 30s default.
 *
 * What does blow 30s there is a cold subprocess spawn. `tool.shell > basic
 * [pwsh]` runs `echo test`; its `[powershell]` and `[cmd]` siblings take 1051ms
 * and 371ms, yet the pwsh case -- the first pwsh.exe start in that process --
 * has died at exactly 30000ms on four separate runs, reaped with a dangling
 * child. Same shape as the ~3.3x CLI-startup stretch documented in
 * cli-subprocess-suites.ts. A 30s cap on that runner fails healthy tests without
 * catching anything; the 15-minute per-command watchdog in run() below is the
 * hang detector that actually works.
 */
const testTimeout = Bun.env.OPENCODE_TEST_TIMEOUT_MS ?? (process.platform === "win32" ? "120000" : "30000")
const shards = shardByArea(parallel)
const selectedAreas = new Set((Bun.env.OPENCODE_TEST_ONLY_AREAS ?? "").split(",").filter(Boolean))
const selected = selectedAreas.size === 0 ? shards : shards.filter((shard) => selectedAreas.has(shard.area))
const concurrency = Number(Bun.env.OPENCODE_TEST_SHARD_CONCURRENCY ?? 3)
console.log(
  `[test:ci] DISCOVERED ${selected.flatMap((shard) => shard.files).length} parallel files in ${selected.length} process-isolated shards (concurrency ${concurrency})`,
)
if (selectedAreas.size === 0) {
  await serial("test/snapshot", "snapshot.xml")
  await serial("test/project", "project.xml")
  await serial("test/file", "file.xml")
}
await pooled(selected, concurrency, async (shard, index) => {
  await run(`parallel shard ${index + 1}/${selected.length} (${shard.area} ${shard.part}/${shard.parts})`, [
    "bun",
    "test",
    ...shard.files,
    "--timeout",
    testTimeout,
    "--max-concurrency",
    Bun.env.OPENCODE_TEST_FILE_CONCURRENCY ?? "1",
    "--reporter",
    "junit",
    "--reporter-outfile",
    path.join(output, `junit-${index + 1}.xml`),
  ])
})

if (selectedAreas.size === 0) {
  await serial("test/plugin/openai-ws.test.ts", "openai-ws.xml")
  await serial("test/session/compaction.test.ts", "session-compaction.xml")
  await serial("test/session/prompt.test.ts", "session-prompt.xml")
  await serial("test/server/httpapi-listen.test.ts", "httpapi-listen.xml")
  await serialFiles("test/cli/tui", "tui", ["--conditions=browser"])
  await serialFiles("test/cli/cmd/tui", "tui-sync", ["--conditions=browser"])
  await serialFiles("test/cli/run", "run-ui")
  await serialFiles("test/cli/acp", "acp")
  await serialFiles("test/cli/serve", "serve")
  await serialFiles("test/cli/smokes", "smokes")
}

async function serialFiles(directory: string, report: string, options: string[] = []) {
  const root = path.join(import.meta.dir, "..", directory)
  const files = [...new Bun.Glob("**/*.test.*").scanSync({ cwd: root })]
    .filter((file) => file.endsWith(".ts") || file.endsWith(".tsx"))
    .filter((file) => !excluded.has(`${directory}/${file.replaceAll("\\", "/")}`))
    .toSorted()
  for (const file of files) {
    const target = path.join(directory, file).replaceAll("\\", "/")
    await serial(target, `${report}-${file.replace(/[^a-zA-Z0-9.-]/g, "-")}.xml`, options)
  }
}

function serial(target: string, report: string, options: string[] = []) {
  return run(target, [
    "bun",
    "test",
    ...options,
    target,
    "--timeout",
    testTimeout,
    "--max-concurrency",
    "1",
    "--reporter",
    "junit",
    "--reporter-outfile",
    path.join(output, report),
  ])
}

function shardByArea(files: string[]) {
  // A file count alone does not bound a shard's cost: `tool` is 24 files, which
  // cut into two 12-file parts measuring 202-214s and 69-71s on windows-latest.
  // The long half was the pool's critical path at 1.7x the next-biggest shard,
  // spawning pwsh/git/LSP children for three and a half unbroken minutes. Cut an
  // area into enough parts to also bound its estimated seconds.
  const maxShardSeconds = Number(Bun.env.OPENCODE_TEST_SHARD_SECONDS ?? 90)
  const byArea = Map.groupBy(files, (file) => file.slice("test/".length).split("/")[0] ?? "root")
  return [...byArea.entries()]
    .toSorted((left, right) => {
      const size = right[1].length - left[1].length
      return size === 0 ? left[0].localeCompare(right[0]) : size
    })
    .flatMap(([area, areaFiles]) => {
      const parts = Math.min(
        areaFiles.length,
        Math.max(
          Math.ceil(areaFiles.length / Number(Bun.env.OPENCODE_TEST_SHARD_SIZE ?? 12)),
          Math.ceil(estimatedSeconds(area) / maxShardSeconds),
        ),
      )
      return Array.from({ length: parts }, (_, index) => ({
        area,
        part: index + 1,
        parts,
        // The pool schedules shards, not areas, so order on what a single shard
        // is expected to cost. Ordering on the area total put every `tool` shard
        // at the front of the queue, co-scheduling the spawn-heavy suites.
        seconds: estimatedSeconds(area) / parts,
        files: areaFiles.filter((_, fileIndex) => fileIndex % parts === index),
      }))
    })
    .toSorted((left, right) => right.seconds - left.seconds)
}

// Wall-clock seconds for the whole area on the Windows runner, which is the
// slower of the two platforms and the one that sets the critical path. These
// were guesses and had drifted far enough to mis-schedule the pool -- `server`
// was 70 against a measured 405 and `tool` 65 against 285. Re-read them off the
// `[test:ci] PASS parallel shard` lines of a green `unit (windows)` job when
// they drift again; the values below come from job 101450360346.
function estimatedSeconds(area: string) {
  return (
    {
      server: 405,
      tool: 285,
      session: 130,
      snapshot: 118,
      provider: 89,
      "control-plane": 81,
      project: 78,
      opencodex: 74,
      cli: 72,
      config: 71,
      permission: 53,
      plugin: 50,
      file: 39,
      skill: 35,
      agent: 26,
      lsp: 22,
    }[area] ?? 10
  )
}

async function pooled<T>(items: T[], concurrency: number, task: (item: T, index: number) => Promise<void>) {
  const queue = items.map((item, index) => ({ item, index }))
  let failure: unknown
  await Promise.all(
    Array.from({ length: Math.min(Math.max(1, concurrency), queue.length) }, async () => {
      while (queue.length > 0) {
        if (failure) return
        const next = queue.shift()
        if (!next) return
        await task(next.item, next.index).catch((error) => {
          failure ??= error
        })
      }
    }),
  )
  if (failure) throw failure
}

async function run(label: string, command: string[]) {
  const started = performance.now()
  const timeoutMs = Number(Bun.env.OPENCODE_TEST_COMMAND_TIMEOUT_MS ?? 15 * 60 * 1_000)
  const log = path.join(output, label.replace(/[^a-zA-Z0-9.-]/g, "-").replace(/-+/g, "-"))
  const stdout = openSync(`${log}.stdout.log`, "w")
  const stderr = openSync(`${log}.stderr.log`, "w")
  console.log(`[test:ci] START ${label}`)
  const child = Bun.spawn(command, {
    cwd: path.join(import.meta.dir, ".."),
    stdin: "inherit",
    stdout,
    stderr,
  })
  const heartbeat = setInterval(() => {
    console.log(`[test:ci] RUNNING ${label} ${seconds(started)}s`)
  }, 30_000)
  const timedOut = Symbol("timed-out")
  const deadline = Promise.withResolvers<typeof timedOut>()
  const timeout = setTimeout(() => deadline.resolve(timedOut), timeoutMs)
  const code = await Promise.race([child.exited, deadline.promise]).finally(() => {
    clearInterval(heartbeat)
    clearTimeout(timeout)
  })
  if (code === timedOut) {
    child.kill()
    await child.exited
    closeSync(stdout)
    closeSync(stderr)
    await printFailureLogs(log)
    throw new Error(`Test command timed out after ${seconds(started)}s: ${label}`)
  }
  closeSync(stdout)
  closeSync(stderr)
  if (code !== 0) {
    await printFailureLogs(log)
    throw new Error(`Test command failed with exit code ${code}: ${command.join(" ")}`)
  }
  console.log(`[test:ci] PASS ${label} ${seconds(started)}s`)
}

async function printFailureLogs(log: string) {
  const [stdout, stderr] = await Promise.all([
    Bun.file(`${log}.stdout.log`).text(),
    Bun.file(`${log}.stderr.log`).text(),
  ])
  if (stdout) console.error(stdout)
  if (stderr) console.error(stderr)
}

function seconds(started: number) {
  return ((performance.now() - started) / 1_000).toFixed(1)
}
