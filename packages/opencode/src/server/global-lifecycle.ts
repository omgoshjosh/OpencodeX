import { GlobalBus } from "@/bus/global"
import { InstanceStore } from "@/project/instance-store"
import { closeAllPersistentChannels } from "@/opencodex/claude-transport"
import { Effect } from "effect"
import { Event } from "./event"

export const emitGlobalDisposed = Effect.sync(() =>
  GlobalBus.emit("event", {
    directory: "global",
    payload: {
      type: Event.Disposed.type,
      properties: {},
    },
  }),
)

export const disposeAllInstancesAndEmitGlobalDisposed = Effect.fn("Server.disposeAllInstancesAndEmitGlobalDisposed")(
  function* (options?: { swallowErrors?: boolean }) {
    const store = yield* InstanceStore.Service
    yield* Effect.gen(function* () {
      // Each live Claude channel holds a CLI child process that outlives the
      // instance that started it, so disposal has to reclaim them too.
      yield* Effect.promise(() => closeAllPersistentChannels()).pipe(
        Effect.catchCause((cause) => Effect.logWarning("claude channel disposal failed", { cause })),
      )
      yield* options?.swallowErrors
        ? store.disposeAll().pipe(Effect.catchCause((cause) => Effect.logWarning("global disposal failed", { cause })))
        : store.disposeAll()
      yield* emitGlobalDisposed
    }).pipe(Effect.uninterruptible)
  },
)

export * as GlobalLifecycle from "./global-lifecycle"
