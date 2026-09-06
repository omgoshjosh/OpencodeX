import { describe, expect } from "bun:test"
import { Deferred, Duration, Effect, Fiber, Layer, Schema } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

/**
 * Task 22 (https://github.com/ecgreen/OpencodeX/pull/38): can the application
 * barrier strand, or unboundedly delay, a delegation settle under realistic
 * contention?
 *
 * `applicationBarrier` is one process-wide `Semaphore.makeUnsafe(1)` per Event
 * layer (packages/core/src/event.ts:164, acquired in `barrier` at :310-365).
 * The realistic concurrent holders in a single daemon process are:
 *
 *   - every session write: `Session.patch` and `Session.mutate` wrap their
 *     transaction *and* `events.broadcast`
 *     (packages/opencode/src/session/session.ts:1033, :1065)
 *   - the `state-reader` snapshot/list scans, which take the barrier to freeze
 *     the log while they read several statements
 *     (packages/opencode/src/opencodex/state-reader.ts:156, :179, :201, :217)
 *   - the retention/compaction delete pass
 *     (packages/core/src/event/retention.ts:267)
 *   - every inline event listener, which `broadcastEvent` runs *inside* the
 *     permit before publishing (packages/core/src/event.ts:379-399)
 *
 * These tests model that shape against the real `events.barrier` and answer,
 * deterministically and with virtual time only, whether a settle-shaped writer
 * can be starved. They deliberately do not change barrier behaviour.
 */

const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory: AbsolutePath.make("project"), workspaceID: "workspace" })),
)
const eventLayer = Layer.mergeAll(EventV2.defaultLayer, Database.defaultLayer)
const it = testEffect(eventLayer.pipe(Layer.provideMerge(locationLayer)))

const Message = EventV2.define({
  type: "test.contention",
  schema: {
    text: Schema.String,
  },
})

/** Number of acquisitions a runaway contender is allowed before we call it starvation. */
const RUNAWAY_CAP = 2000

/**
 * Contenders acquire the barrier in a loop until the settle gets in or the cap
 * trips. `gap` is the scheduling boundary *between* acquisitions — the async
 * work every real caller does outside the barrier (a query, an HTTP hop, an
 * LLM turn). It is the single variable that decides the answer, so it is the
 * knob rather than a constant.
 */
const contend = (events: EventV2.Interface, options: { readonly contenders: number; readonly gap: "none" | "yield" }) =>
  Effect.gen(function* () {
    const state = { acquisitions: 0, settleAcquiredAfter: -1 }
    const contender = Effect.gen(function* () {
      while (state.settleAcquiredAfter < 0 && state.acquisitions < RUNAWAY_CAP) {
        yield* events.barrier(
          Effect.gen(function* () {
            state.acquisitions++
            // A holder that awaits anything (every real holder awaits SQLite).
            yield* Effect.yieldNow
          }),
          "test:contender",
        )
        if (options.gap === "yield") yield* Effect.yieldNow
      }
    })
    const fibers = new Array<Fiber.Fiber<void>>()
    // Forked one at a time from this fiber: `Effect.forEach` with concurrency
    // would make each contender a child of a wrapper that finishes immediately
    // and interrupts it.
    for (let index = 0; index < options.contenders; index++) fibers.push(yield* Effect.forkChild(contender))
    yield* Effect.yieldNow
    const settle = yield* Effect.forkChild(
      events.barrier(
        Effect.sync(() => {
          state.settleAcquiredAfter = state.acquisitions
        }),
        "test:settle",
      ),
    )
    yield* Fiber.join(settle)
    yield* Effect.forEach(fibers, Fiber.join, { discard: true })
    return state
  })

describe("EventV2 barrier contention", () => {
  // (a), part one: waiters already queued on the permit are woken in arrival
  // order. Effect's SemaphoreImpl keeps them in an insertion-ordered Set and
  // drains it front-to-back, so nothing reorders an existing queue.
  it.effect("barrier waiters already queued are served first-in-first-out", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const release = yield* Deferred.make<void>()
      const order = new Array<string>()

      const holder = yield* Effect.forkChild(events.barrier(Deferred.await(release), "test:holder"))
      yield* Effect.yieldNow

      const waiters = new Array<Fiber.Fiber<void>>()
      for (const name of ["mutate", "snapshot", "retention", "settle", "listener"]) {
        waiters.push(
          yield* Effect.forkChild(
            events.barrier(
              Effect.sync(() => {
                order.push(name)
              }),
              `test:${name}`,
            ),
          ),
        )
        yield* Effect.yieldNow
      }
      expect(order).toEqual([])

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(holder)
      yield* Effect.forEach(waiters, Fiber.join, { discard: true })

      expect(order).toEqual(["mutate", "snapshot", "retention", "settle", "listener"])
    }),
  )

  // (a), part two — the crux. FIFO among *queued* waiters is not fairness: the
  // permit is not handed to the head of the queue, it is only released, and
  // waking the queue is deferred to a scheduler task. A fiber that releases and
  // re-acquires without yielding in between takes the permit again inside that
  // window, ahead of a waiter that has been queued the whole time.
  it.effect("a fiber that re-acquires without yielding barges ahead of a queued waiter", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const release = yield* Deferred.make<void>()
      const order = new Array<string>()

      const barger = yield* Effect.forkChild(
        Effect.gen(function* () {
          yield* events.barrier(
            Effect.gen(function* () {
              order.push("barger:first")
              yield* Deferred.await(release)
            }),
            "test:barger",
          )
          yield* events.barrier(
            Effect.sync(() => {
              order.push("barger:second")
            }),
            "test:barger",
          )
        }),
      )
      yield* Effect.yieldNow

      const settle = yield* Effect.forkChild(
        events.barrier(
          Effect.sync(() => {
            order.push("settle")
          }),
          "test:settle",
        ),
      )
      yield* Effect.yieldNow

      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(barger)
      yield* Fiber.join(settle)

      // If acquisition were fair, the settle — queued before the second
      // acquisition was even requested — would run second.
      expect(order).toEqual(["barger:first", "barger:second", "settle"])
    }),
  )

  // The production answer. Every real holder yields to the scheduler between
  // acquisitions (`Session.mutate` returns to a request handler, `state-reader`
  // returns to an HTTP response, retention sleeps between passes). With that
  // gap present the barge window closes and a settle gets in within about one
  // round of the contenders, no matter how long they keep churning.
  it.effect("a settle behind realistic contenders acquires within one round", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const state = yield* contend(events, { contenders: 8, gap: "yield" })

      expect(state.settleAcquiredAfter).toBeGreaterThanOrEqual(0)
      expect(state.settleAcquiredAfter).toBeLessThan(RUNAWAY_CAP)
      // Bounded by the contenders already queued ahead of it, twice over for
      // the in-flight round. Not by how long contention lasts.
      expect(state.settleAcquiredAfter).toBeLessThanOrEqual(2 * 8 + 2)
    }),
  )

  // The pathological shape, recorded so we know what it takes. Remove the
  // scheduling gap — contenders that re-acquire synchronously on release — and
  // the barge window never closes. This is genuine starvation, not slowness:
  // the settle is still queued after RUNAWAY_CAP acquisitions and only lands
  // when the loop stops on its own.
  it.effect("contenders that re-acquire with no scheduling gap starve a queued settle", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const state = yield* contend(events, { contenders: 8, gap: "none" })

      expect(state.settleAcquiredAfter).toBeGreaterThanOrEqual(RUNAWAY_CAP)
    }),
  )

  // (b) A single holder blocks for exactly as long as it holds — the shape that
  // actually explains a multi-minute stall. Modelled as an inline listener
  // awaiting something slow, because `broadcastEvent` runs listeners inside the
  // permit (packages/core/src/event.ts:379-399), so a slow listener holds the
  // whole process's barrier for its full duration.
  it.effect("a slow inline listener holds the barrier for its entire duration", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const listenerEntered = yield* Deferred.make<void>()
      const releaseListener = yield* Deferred.make<void>()
      const settled = { acquired: false }

      yield* events.listen(() =>
        Effect.gen(function* () {
          yield* Deferred.succeed(listenerEntered, undefined)
          yield* Deferred.await(releaseListener)
        }),
      )

      const publish = yield* Effect.forkChild(events.publish(Message, { text: "slow listener" }))
      yield* Deferred.await(listenerEntered)

      const settle = yield* Effect.forkChild(
        events.barrier(
          Effect.sync(() => {
            settled.acquired = true
          }),
          "test:settle",
        ),
      )
      yield* Effect.yieldNow

      // Virtual time only: no wall-clock sleep, so this is 27 minutes of stall
      // in microseconds of test.
      yield* TestClock.adjust(Duration.minutes(27))
      yield* Effect.yieldNow
      expect(settled.acquired).toBe(false)

      yield* Deferred.succeed(releaseListener, undefined)
      yield* Fiber.join(publish)
      yield* Fiber.join(settle)
      expect(settled.acquired).toBe(true)
    }),
  )

  // (c) Reentrancy rides `Effect.provideService(effect, InApplicationBarrier,
  // true)` (packages/core/src/event.ts:355), which a child fiber inherits and a
  // fresh root fiber does not. `EffectBridge.make` replays a context captured
  // outside any barrier onto a new root fiber for every `bridge.promise`
  // (packages/opencode/src/effect/bridge.ts:55,60-70), so a bridged acquirer
  // queues on the permit its own caller is holding. Nothing releases it while
  // the holder is waiting on it: that is a stall with no upper bound, not a
  // slow one. This measures the divergence; unskipping the deadlock
  // reproduction in event.test.ts is Task 21 Step 1's acceptance criterion and
  // is deliberately left alone here.
  it.effect("a bridged acquirer stays queued for as long as the holder holds; a child fiber does not", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const captured = yield* Effect.context<EventV2.Service>()
      const entered = new Array<string>()
      const release = yield* Deferred.make<void>()

      let bridged: Fiber.Fiber<void> | undefined
      let inherited: Fiber.Fiber<void> | undefined

      const holder = yield* Effect.forkChild(
        events.barrier(
          Effect.gen(function* () {
            // Mimics `bridge.promise`: a brand new root fiber replaying a
            // context captured outside the barrier.
            bridged = Effect.runFork(
              events
                .barrier(
                  Effect.sync(() => {
                    entered.push("bridged")
                  }),
                  "test:bridged",
                )
                .pipe(Effect.provide(captured)),
            )
            // A child fiber of the holder, which inherits the reentrancy flag.
            inherited = yield* Effect.forkChild(
              events.barrier(
                Effect.sync(() => {
                  entered.push("inherited")
                }),
                "test:inherited",
              ),
            )
            yield* Fiber.join(inherited)
            yield* TestClock.adjust(Duration.minutes(27))
            yield* Effect.yieldNow
            // 27 virtual minutes in, the bridged acquirer has still not run.
            expect(entered).toEqual(["inherited"])
            yield* Deferred.await(release)
          }),
          "test:holder",
        ),
      )

      yield* Effect.yieldNow
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(holder)
      yield* Effect.promise(() => Effect.runPromise(Fiber.join(bridged!)))

      expect(entered).toEqual(["inherited", "bridged"])
    }),
  )
})
