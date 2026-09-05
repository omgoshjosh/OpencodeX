import { Effect } from "effect"

/** Default ceiling for filesystem-backed file tools; approvals are intentionally excluded. */
export const FILE_TOOL_DEADLINE_MS = 10_000

export class FileToolTimeoutError extends Error {
  constructor(tool: string) {
    super(`${tool} timed out after ${FILE_TOOL_DEADLINE_MS / 1000} seconds; narrow the path or try again`)
    this.name = "FileToolTimeoutError"
  }
}

function abortReason(signal: AbortSignal) {
  if (signal.reason instanceof Error) return signal.reason
  const error = new Error("Aborted")
  error.name = "AbortError"
  return error
}

function waitForAbort(signal: AbortSignal) {
  if (signal.aborted) return Effect.fail(abortReason(signal))
  return Effect.callback<never, Error>((resume) => {
    const abort = () => resume(Effect.fail(abortReason(signal)))
    signal.addEventListener("abort", abort, { once: true })
    return Effect.sync(() => signal.removeEventListener("abort", abort))
  })
}

/**
 * Bounds filesystem and process work without putting time spent in an approval
 * prompt on the clock. The caller's cancellation remains the cancellation reason.
 */
export function withFileToolDeadline<A, E, R>(
  tool: string,
  caller: AbortSignal,
  run: (signal: AbortSignal) => Effect.Effect<A, E, R>,
) {
  if (caller.aborted) return Effect.fail(abortReason(caller))

  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const controller = new AbortController()
      const abort = () => controller.abort(caller.reason)
      caller.addEventListener("abort", abort, { once: true })
      return { controller, abort }
    }),
    ({ controller }) => {
      const timeout = new FileToolTimeoutError(tool)
      return Effect.raceFirst(
        Effect.raceFirst(run(controller.signal), waitForAbort(caller)),
        Effect.sleep(FILE_TOOL_DEADLINE_MS).pipe(
          Effect.tap(() => Effect.sync(() => controller.abort(timeout))),
          Effect.andThen(Effect.fail(timeout)),
        ),
      )
    },
    ({ controller, abort }) =>
      Effect.sync(() => {
        caller.removeEventListener("abort", abort)
        if (!controller.signal.aborted) controller.abort()
      }),
  )
}
