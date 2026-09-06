import { expect } from "bun:test"
import { Duration, Effect, Fiber, Layer, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EffectBridge } from "@/effect/bridge"
import { testEffect } from "../lib/effect"

// Task 23 Step 1 (https://github.com/ecgreen/OpencodeX/pull/38).
//
// `EventV2.barrier` is a single process-wide permit. Reentrancy rides a
// fiber-scoped reference, which `Effect.fork` and scoped children inherit but a
// fresh root fiber does not. Every `EffectBridge` entry point starts a fresh
// root fiber, so before the fix a bridged call made from inside the barrier
// queued on the permit its own caller was still holding — an unbounded wait,
// because the caller cannot release until the bridged call returns.
//
// These tests build the bridge OUTSIDE any barrier, exactly as production does
// (the app runtime makes one bridge at startup and reuses it), so they exercise
// the live-context capture in `EffectBridge.make` rather than a lucky capture.
const it = testEffect(Layer.mergeAll(EventV2.defaultLayer, Database.defaultLayer))

const Message = EventV2.define({
  type: "test.bridge.message",
  schema: { text: Schema.String },
})

// Real clock: a deadlock has to fail on a timeout, not hang the run.
const TIMEOUT = Duration.seconds(10)

it.live("an awaited bridged call from inside an inline listener acquires the barrier", () =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const bridge = yield* EffectBridge.make()
    const entered: string[] = []

    // Listeners run INLINE inside the barrier, so this listener holds the
    // permit while it awaits the bridged acquisition below.
    yield* events.listen(() =>
      Effect.promise(() =>
        bridge.promise(
          events.barrier(
            Effect.sync(() => {
              entered.push("bridged")
            }),
            "test:bridged",
          ),
        ),
      ),
    )

    yield* events.publish(Message, { text: "inline listener" }).pipe(Effect.timeout(TIMEOUT))

    expect(entered).toEqual(["bridged"])
  }),
)

it.live("bridge.run from inside the barrier acquires the barrier", () =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const bridge = yield* EffectBridge.make()
    const entered: string[] = []

    yield* events
      .barrier(
        bridge.run(
          events.barrier(
            Effect.sync(() => {
              entered.push("inner")
            }),
            "test:inner",
          ),
        ),
        "test:outer",
      )
      .pipe(Effect.timeout(TIMEOUT))

    expect(entered).toEqual(["inner"])
  }),
)

it.live("bridge.fork does NOT inherit barrier reentrancy", () =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const bridge = yield* EffectBridge.make()
    const entered: string[] = []

    // A concurrent bridged fiber that inherited the flag would write outside
    // the barrier and break the commit-then-broadcast ordering the permit
    // exists to protect. It must queue instead.
    const forked = yield* events.barrier(
      Effect.gen(function* () {
        const fiber = bridge.fork(
          events.barrier(
            Effect.sync(() => {
              entered.push("forked")
            }),
            "test:forked",
          ),
        )
        // A real event-loop turn. A fiber that had inherited reentrancy would
        // have pushed "forked" by now instead of queueing on the permit.
        yield* Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)))
        expect(entered).toEqual([])
        entered.push("outer")
        return fiber
      }),
      "test:outer",
    )

    yield* Fiber.join(forked).pipe(Effect.timeout(TIMEOUT))
    expect(entered).toEqual(["outer", "forked"])
  }),
)
