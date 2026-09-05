import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { FileToolTimeoutError, withFileToolDeadline } from "@/tool/file-deadline"
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
})
