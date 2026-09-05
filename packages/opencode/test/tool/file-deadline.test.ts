import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { FileToolTimeoutError, withFileToolDeadline, withSearchDeadline } from "@/tool/file-deadline"
import { it } from "../lib/effect"

describe("tool.file-deadline", () => {
  it.effect("fails stalled glob work at the shared deadline", () =>
    Effect.gen(function* () {
      const fiber = yield* withFileToolDeadline("glob", AbortSignal.any([]), () => Effect.never).pipe(Effect.forkChild)
      yield* TestClock.adjust(10_000)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(FileToolTimeoutError)
    }),
  )

  it.effect("preserves caller cancellation before the deadline", () =>
    Effect.gen(function* () {
      const caller = new AbortController()
      const reason = new Error("session cancelled")
      caller.abort(reason)
      const exit = yield* withFileToolDeadline("read", caller.signal, () => Effect.never).pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(reason)
    }),
  )

  it.effect("uses the same deadline for grep work", () =>
    Effect.gen(function* () {
      const fiber = yield* withFileToolDeadline("grep", AbortSignal.any([]), () => Effect.never).pipe(Effect.forkChild)
      yield* TestClock.adjust(10_000)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(FileToolTimeoutError)
    }),
  )

  it.effect("uses the same deadline for read work", () =>
    Effect.gen(function* () {
      const fiber = yield* withFileToolDeadline("read", AbortSignal.any([]), () => Effect.never).pipe(Effect.forkChild)
      yield* TestClock.adjust(10_000)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(FileToolTimeoutError)
    }),
  )

  it.effect("returns partial search output at the deadline instead of failing", () =>
    Effect.gen(function* () {
      const fiber = yield* withSearchDeadline(
        "grep",
        AbortSignal.any([]),
        60_000,
        () => Effect.never,
        () => "partial",
      ).pipe(Effect.forkChild)
      yield* TestClock.adjust(60_000)
      const exit = yield* Fiber.await(fiber)
      expect(exit).toStrictEqual(Exit.succeed("partial"))
    }),
  )

  it.effect("aborts the search signal at the deadline so the child process is torn down", () =>
    Effect.gen(function* () {
      let observed: AbortSignal | undefined
      const fiber = yield* withSearchDeadline(
        "glob",
        AbortSignal.any([]),
        5_000,
        (signal) => {
          observed = signal
          return Effect.never
        },
        () => "partial",
      ).pipe(Effect.forkChild)
      yield* TestClock.adjust(5_000)
      yield* Fiber.await(fiber)
      expect(observed?.aborted).toBe(true)
    }),
  )

  it.effect("does not convert caller cancellation into partial search output", () =>
    Effect.gen(function* () {
      const caller = new AbortController()
      const reason = new Error("session cancelled")
      const fiber = yield* withSearchDeadline(
        "grep",
        caller.signal,
        60_000,
        () => Effect.never,
        () => "partial",
      ).pipe(Effect.forkChild)
      yield* Effect.sync(() => caller.abort(reason))
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(reason)
    }),
  )

  it.effect("leaves a search that finishes inside its bound untouched", () =>
    Effect.gen(function* () {
      const result = yield* withSearchDeadline(
        "glob",
        AbortSignal.any([]),
        60_000,
        () => Effect.succeed("complete"),
        () => "partial",
      )
      expect(result).toBe("complete")
    }),
  )
})
