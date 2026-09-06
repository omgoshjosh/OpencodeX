import { Context, Effect, Exit, Fiber } from "effect"
import { WorkspaceContext } from "@/control-plane/workspace-context"
import { EventV2 } from "@opencode-ai/core/event"
import type { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { InstanceRef, WorkspaceRef } from "./instance-ref"
import { attachWith } from "./run-service"

export interface Shape {
  readonly promise: <A, E, R>(effect: Effect.Effect<A, E, R>, options?: Effect.RunOptions) => Promise<A>
  readonly promiseExit: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    options?: Effect.RunOptions,
  ) => Promise<Exit.Exit<A, E>>
  readonly fork: <A, E, R>(effect: Effect.Effect<A, E, R>) => Fiber.Fiber<A, E>
  readonly run: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E>
  readonly bind: <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => (...args: Args) => Result
}

function restoreWorkspace<R>(workspace: WorkspaceV2.ID | undefined, fn: () => R): R {
  if (workspace !== undefined) return WorkspaceContext.restore(workspace, fn)
  return fn()
}

function captureSync() {
  const fiber = Fiber.getCurrent()
  const instance = fiber ? Context.getReferenceUnsafe(fiber.context, InstanceRef) : undefined
  const workspace =
    (fiber ? Context.getReferenceUnsafe(fiber.context, WorkspaceRef) : undefined) ?? WorkspaceContext.workspaceID
  return { instance, workspace }
}

export const bind = <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) => {
  const captured = captureSync()
  return (...args: Args) =>
    restoreWorkspace(captured.workspace, () =>
      Effect.runSync(
        attachWith(
          Effect.sync(() => fn(...args)),
          captured,
        ),
      ),
    )
}

/**
 * Bridge from Effect into a Promise-returning JS callback while preserving
 * `WorkspaceContext` AsyncLocalStorage for callback code that still reads it.
 * `InstanceRef` is captured for effects run through the returned bridge APIs;
 * plain JS callbacks that need it should receive the ref explicitly.
 *
 * Mirrors `Effect.promise` but restores workspace ALS first.
 */
export const fromPromise = <T>(fn: () => Promise<T> | T): Effect.Effect<T> =>
  Effect.gen(function* () {
    const workspace = yield* WorkspaceRef
    return yield* Effect.promise(() => Promise.resolve(restoreWorkspace(workspace, () => fn())))
  })

export function make(): Effect.Effect<Shape> {
  return Effect.gen(function* () {
    const ctx = yield* Effect.context()
    const captured = captureSync()
    const instance = (yield* InstanceRef) ?? captured.instance
    const workspace = (yield* WorkspaceRef) ?? captured.workspace
    const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      attachWith(effect.pipe(Effect.provide(ctx)) as Effect.Effect<A, E>, { instance, workspace })
    /**
     * Same as `wrap`, plus the caller's application-barrier reentrancy
     * reference — and nothing else from the caller.
     *
     * Every bridge entry point starts a fresh root fiber, so it inherits none
     * of the caller's fiber-scoped state. `ctx` is captured once inside
     * `make()` — outside any application barrier — so a bridged call made from
     * inside `EventV2.barrier` loses that region's reentrancy reference and
     * queues on the single process-wide permit its own caller is still
     * holding. That wait is unbounded: the caller cannot release until the
     * bridged call returns. Reading the reference off the LIVE caller fiber at
     * CALL time (not make() time) restores it.
     *
     * Only that one reference crosses. The rest of the caller's live context
     * deliberately does NOT — above all `Scope` and `Tracer.ParentSpan`. A
     * bridged awaited call is typically a tool execution; inheriting the
     * caller's `Scope` would attach any resource it acquires to the caller's
     * scope instead of releasing when the bridged call returns, and in exactly
     * the case this fix targets that scope is the barrier region — holding
     * resources for the life of the region, the opposite of the intent.
     *
     * Deliberately only used by the AWAITED entry points (`promise`,
     * `promiseExit`, `run`). `fork` must not get it: a concurrent bridged
     * fiber inheriting barrier reentrancy would write outside the barrier and
     * break the commit-then-broadcast ordering the permit exists to protect.
     */
    const wrapLive = <A, E, R>(effect: Effect.Effect<A, E, R>) => {
      const fiber = Fiber.getCurrent()
      if (!fiber) return wrap(effect)
      if (!Context.getReferenceUnsafe(fiber.context, EventV2.InApplicationBarrier)) return wrap(effect)
      return wrap(Effect.provideService(effect, EventV2.InApplicationBarrier, true))
    }

    return {
      promise: <A, E, R>(effect: Effect.Effect<A, E, R>, options?: Effect.RunOptions) =>
        restoreWorkspace(workspace, () => Effect.runPromise(wrapLive(effect), options)),
      promiseExit: <A, E, R>(effect: Effect.Effect<A, E, R>, options?: Effect.RunOptions) =>
        restoreWorkspace(workspace, () => Effect.runPromiseExit(wrapLive(effect), options)),
      fork: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        restoreWorkspace(workspace, () => Effect.runFork(wrap(effect))),
      run: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        Effect.callback<A, E>((resume) => {
          const wrapped = wrapLive(effect)
          restoreWorkspace(workspace, () =>
            Effect.runPromiseExit(wrapped).then((exit) =>
              resume(Exit.isSuccess(exit) ? Effect.succeed(exit.value) : Effect.failCause(exit.cause)),
            ),
          )
        }),
      bind:
        <Args extends readonly unknown[], Result>(fn: (...args: Args) => Result) =>
        (...args: Args) =>
          restoreWorkspace(workspace, () => Effect.runSync(wrap(Effect.sync(() => fn(...args))))),
    } satisfies Shape
  })
}

export * as EffectBridge from "./bridge"
