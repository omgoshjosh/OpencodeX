import { Effect } from "effect"

/** Default ceiling for filesystem-backed file tools; approvals are intentionally excluded. */
export const FILE_TOOL_DEADLINE_MS = 10_000

/**
 * Default ceiling for the streaming search tools (glob, grep). It is looser than
 * FILE_TOOL_DEADLINE_MS because these tools return the partial results gathered
 * so far instead of failing the turn, so spending the budget still produces work
 * the model can use. Override with `experimental.search_timeout`.
 */
export const SEARCH_TOOL_DEADLINE_MS = 60_000

export class FileToolTimeoutError extends Error {
  constructor(tool: string) {
    super(`${tool} timed out after ${FILE_TOOL_DEADLINE_MS / 1000} seconds; narrow the path or try again`)
    this.name = "FileToolTimeoutError"
  }
}

/** Signals that a search tool hit its wall clock; recovered into partial output, never surfaced as a turn failure. */
export class SearchTimeoutError extends Error {
  constructor(
    readonly tool: string,
    readonly timeoutMs: number,
  ) {
    super(`${tool} search stopped after ${timeoutMs / 1000} seconds`)
    this.name = "SearchTimeoutError"
  }
}

/** The truncation notice appended to partial search output. */
export function searchTimeoutNotice(timeoutMs: number, count: number, label: string) {
  return `(Search stopped after ${timeoutMs / 1000}s; ${count} ${label} so far; narrow the path or pattern.)`
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
 * Races `run` against the deadline on a child signal. Aborting that signal is what
 * tears down the work — for the search tools it kills the ripgrep child process,
 * because the spawn is scoped to the effect being interrupted.
 */
function withDeadline<A, E, R>(
  caller: AbortSignal,
  timeoutMs: number,
  timeout: Error,
  run: (signal: AbortSignal) => Effect.Effect<A, E, R>,
) {
  return Effect.acquireUseRelease(
    Effect.sync(() => {
      const controller = new AbortController()
      const abort = () => controller.abort(caller.reason)
      caller.addEventListener("abort", abort, { once: true })
      return { controller, abort }
    }),
    ({ controller }) =>
      Effect.raceFirst(
        Effect.raceFirst(run(controller.signal), waitForAbort(caller)),
        Effect.sleep(timeoutMs).pipe(
          Effect.tap(() => Effect.sync(() => controller.abort(timeout))),
          Effect.andThen(Effect.fail(timeout)),
        ),
      ),
    ({ controller, abort }) =>
      Effect.sync(() => {
        caller.removeEventListener("abort", abort)
        if (!controller.signal.aborted) controller.abort()
      }),
  )
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
  return withDeadline(caller, FILE_TOOL_DEADLINE_MS, new FileToolTimeoutError(tool), run)
}

/**
 * Same bound for the search tools, except expiry is not a failure: `partial` renders
 * whatever the tool accumulated before the clock ran out. Caller cancellation still
 * fails, so an interrupted turn stays interrupted.
 */
export function withSearchDeadline<A, E, R>(
  tool: string,
  caller: AbortSignal,
  timeoutMs: number,
  run: (signal: AbortSignal) => Effect.Effect<A, E, R>,
  partial: (error: SearchTimeoutError) => A,
): Effect.Effect<A, E | Error, R> {
  if (caller.aborted) return Effect.fail(abortReason(caller))
  const timeout = new SearchTimeoutError(tool, timeoutMs)
  return withDeadline(caller, timeoutMs, timeout, run).pipe(
    Effect.catch((error) => (error === timeout ? Effect.succeed(partial(timeout)) : Effect.fail(error))),
  )
}
