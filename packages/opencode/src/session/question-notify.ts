import { Effect } from "effect"
import type { Question } from "@/question"

/**
 * The hook `Question.ask` fires the moment a request becomes durable.
 *
 * A child session that asks a question blocks until somebody answers it. Until
 * this hook existed the only thing that ever noticed was a human watching the
 * GUI, or the operator's 30-minute sweep: on 2026-09-06 a child sat on one
 * question for 43 minutes while its parent idled, because the parent was never
 * told. The listener that turns a question into a durable parent notification
 * lives in the SessionPrompt layer, and this registry is how it gets called
 * without `Question` importing `SessionPrompt` -- `prompt.ts` already depends
 * on `Question.Service`, so a direct import is a layer cycle.
 *
 * The shape mirrors `SessionPromptRecovery.register` on purpose: module-level,
 * unregistered by the registering layer's finalizer, and awaited by nobody who
 * matters. `notify` isolates each handler's failures so a broken listener can
 * never take down the question that triggered it, and `Question.ask` forks the
 * call so a slow listener can never delay one either.
 */
type Handler = (request: Question.Request) => Effect.Effect<void>

const handlers = new Set<Handler>()

export function register(handler: Handler) {
  handlers.add(handler)
  return () => handlers.delete(handler)
}

export const notify = Effect.fn("SessionQuestionNotify.notify")(function* (request: Question.Request) {
  yield* Effect.forEach(
    handlers,
    (handler) =>
      handler(request).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("question notification handler failed", { requestID: request.id, cause }),
        ),
      ),
    { discard: true },
  )
})

export * as SessionQuestionNotify from "./question-notify"
