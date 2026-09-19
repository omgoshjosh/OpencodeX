import { describe, expect, test } from "bun:test"
import path from "node:path"

// Effect renders every extra `Effect.log*` argument with String(), so a
// payload object becomes `message [object Object]` (OpencodeX-2kg,
// OpencodeX-fow). Fields go through `Effect.annotateLogs({...})`; causes
// through `Cause.pretty(cause)`. This guard fails on any second argument
// after a literal message, single- or multi-line, anywhere under packages/*/src.
const pattern =
  /\bEffect\.log(?:Info|Warning|Error|Debug|Fatal|Trace)?\(\s*(?:"[^"\n]*"|'[^'\n]*'|`[^`]*`)\s*,\s*[^\s)]/g

const root = path.resolve(import.meta.dir, "../../../..")
const trees = "packages/*/src/**/*.{ts,tsx}"

function logShapeOffenders(source: string): number[] {
  return [...source.matchAll(pattern)].map((match) => source.slice(0, match.index).split("\n").length)
}

describe("Effect.log* message shape", () => {
  test("flags a second argument after the message, on one line or several", () => {
    const source = [
      'Effect.logWarning("sweep failed", { cause })',
      'Effect.logError("delivery failed", {',
      "  runID,",
      "  cause,",
      "})",
      'Effect.logDebug("tick", context)',
      'Effect.log("tick", context)',
    ].join("\n")
    expect(logShapeOffenders(source)).toEqual([1, 2, 6, 7])
  })

  test("accepts annotateLogs and message-only calls", () => {
    const source = [
      'Effect.logWarning("sweep failed").pipe(Effect.annotateLogs({ cause: Cause.pretty(cause) }))',
      "Effect.logInfo(`retention interval_ms=${interval}`)",
      'Effect.logWarning(\n  "unwind failed",\n)',
    ].join("\n")
    expect(logShapeOffenders(source)).toEqual([])
  })

  test("no source under packages/*/src passes a payload as a log argument", async () => {
    const files = (await Array.fromAsync(new Bun.Glob(trees).scan({ cwd: root }))).toSorted()
    expect(files.length).toBeGreaterThan(100)
    const offenders: string[] = []
    for (const file of files) {
      const source = await Bun.file(path.join(root, file)).text()
      for (const line of logShapeOffenders(source)) offenders.push(`${file.replaceAll("\\", "/")}:${line}`)
    }
    expect(offenders, "use Effect.log*(msg).pipe(Effect.annotateLogs({...})) with Cause.pretty(cause)").toEqual([])
  })
})
