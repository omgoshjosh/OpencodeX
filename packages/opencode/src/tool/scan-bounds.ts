import path from "path"
import { realpath } from "fs/promises"
import { Effect } from "effect"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { InstanceState } from "@/effect/instance-state"

export const SCAN_TIMEOUT = "15 seconds"
export const SCAN_TIMEOUT_MS = 15_000

export const assertScanRoot = Effect.fn("Tool.assertScanRoot")(function* (
  target: string,
  options?: { allowExternal?: boolean },
) {
  const root = path.parse(target).root
  if (path.resolve(target) === root)
    return yield* Effect.fail(
      new Error(`Refusing to scan filesystem root: ${target}. Specify a project subdirectory instead.`),
    )

  const resolved = yield* Effect.tryPromise({
    try: () => realpath(target),
    catch: (cause) =>
      new Error(`Cannot resolve scan path ${target}: ${cause instanceof Error ? cause.message : String(cause)}`),
  })
  const ins = yield* InstanceState.context
  const roots = [ins.directory, ins.worktree, ...(ins.opencodex?.folders ?? [])].filter(
    (candidate) => path.resolve(candidate) !== path.parse(candidate).root,
  )
  const allowed = yield* Effect.forEach(roots, (candidate) =>
    Effect.tryPromise({
      try: () => realpath(candidate),
      // A stale optional workspace folder must not make valid scans fail.
      catch: () => candidate,
    }),
  )

  const lexicalWorkspacePath = roots.some((candidate) => AppFileSystem.contains(candidate, path.resolve(target)))
  if (allowed.some((candidate) => AppFileSystem.contains(candidate, resolved))) return resolved
  if (options?.allowExternal && !lexicalWorkspacePath) return resolved
  return yield* Effect.fail(
    new Error(
      `Refusing to scan outside the active workspace: ${target}. Use a project subdirectory or an approved reference.`,
    ),
  )
})
