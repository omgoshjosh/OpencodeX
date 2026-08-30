import { and, eq, inArray, isNull, lt, or } from "drizzle-orm"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Schedule, Scope } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { MessageID, SessionID } from "./schema"
import type { LoopInput } from "./prompt-schema"
import { SessionPromptRecovery } from "./prompt-recovery"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { SessionExecutionOwner } from "./execution-owner"
import * as Session from "./session"
import type { DeploymentDrainError } from "@/server/deployment-drain"

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly events: Context.Service.Shape<typeof EventV2Bridge.Service>
  readonly scope: Scope.Scope
  readonly loop: (input: LoopInput) => Effect.Effect<SessionLegacy.WithParts>
  readonly reconcileToolParts?: (input: {
    sessionID: SessionID
    messageID: MessageID
    commandID: string
    generation: number
    reason: string
    owner?: string
  }) => Effect.Effect<number>
  readonly commandLeaseMillis?: number
  readonly clock?: () => number
  readonly admit?: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | DeploymentDrainError, R>
}

/**
 * Durable admission for queued prompts. A prompt is a row in
 * `session_command`; exactly one process may run it at a time, which it proves
 * by holding a lease it heartbeats. Everything here is about winning, holding
 * and settling that lease — the turn itself is `loop`.
 */
export function make(deps: Deps) {
  return Effect.gen(function* () {
    const { database, events, scope, loop } = deps
    const { db } = database
    const processRunID = ensureRunID()
    const commandOwner = `local:${process.pid}:${processRunID}:prompt:${crypto.randomUUID()}`
    const commandLeaseMillis = deps.commandLeaseMillis ?? 30_000
    const clock = deps.clock ?? Date.now

    const claimCommandTurn = Effect.fn("SessionPrompt.claimCommandTurn")(function* (commandID: string) {
      const now = clock()
      const claim = db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionCommandTable)
                .where(eq(SessionCommandTable.id, commandID))
                .get()
              if (!current || ["succeeded", "failed", "cancelled"].includes(current.status)) {
                return { state: "done" as const }
              }
              const parent = current.adopted_by
                ? yield* transaction
                    .select()
                    .from(SessionCommandTable)
                    .where(eq(SessionCommandTable.id, current.adopted_by))
                    .get()
                : undefined
              const parentLive =
                parent?.status === "running" &&
                parent.claim_generation === current.adopted_generation &&
                !!parent.owner_id &&
                parent.lease_expires_at !== null &&
                parent.lease_expires_at > now &&
                SessionExecutionOwner.alive(parent.owner_id, processRunID)
              // An adopted command belongs to a specific live parent claim.
              // Never let recovery clear or replay it while that claim holds
              // a valid lease, whether Claude has received the offer or not.
              if (parentLive) return { state: "occupied" as const }
              // A process can die after reserving an ordinary human command
              // but before writing it to Claude's streaming input. Once its
              // parent claim is stale, clear the fence and recover FIFO.
              if (current.status === "queued" && current.adopted_by && current.offered_at === null) {
                yield* transaction
                  .update(SessionCommandTable)
                  .set({ adopted_by: null, adopted_generation: null, offer_ordinal: null, time_updated: now })
                  .where(
                    and(
                      eq(SessionCommandTable.id, commandID),
                      eq(SessionCommandTable.status, "queued"),
                      eq(SessionCommandTable.adopted_by, current.adopted_by),
                      current.adopted_generation === null
                        ? isNull(SessionCommandTable.adopted_generation)
                        : eq(SessionCommandTable.adopted_generation, current.adopted_generation),
                      isNull(SessionCommandTable.offered_at),
                    ),
                  )
                  .run()
                return { state: "waiting" as const }
              }
              // An offered input may have reached Claude even if this process
              // died before observing its result. Never replay it on recovery.
              if (current.status === "queued" && current.offered_at !== null) {
                yield* transaction
                  .update(SessionCommandTable)
                  .set({
                    status: "failed",
                    error: "Live Claude offer outcome is unknown after recovery.",
                    completed_at: now,
                    time_updated: now,
                  })
                  .where(
                    and(
                      eq(SessionCommandTable.id, commandID),
                      eq(SessionCommandTable.status, "queued"),
                      current.adopted_by
                        ? eq(SessionCommandTable.adopted_by, current.adopted_by)
                        : isNull(SessionCommandTable.adopted_by),
                      current.adopted_generation === null
                        ? isNull(SessionCommandTable.adopted_generation)
                        : eq(SessionCommandTable.adopted_generation, current.adopted_generation),
                      eq(SessionCommandTable.offered_at, current.offered_at),
                    ),
                  )
                  .run()
                return { state: "done" as const }
              }
              const reclaimDeadOwner =
                current.status === "running" &&
                !!current.owner_id &&
                /^local:\d+:[^:]+:prompt:/.test(current.owner_id) &&
                !SessionExecutionOwner.alive(current.owner_id, processRunID)
              if (
                current.status === "running" &&
                current.lease_expires_at &&
                current.lease_expires_at > now &&
                !reclaimDeadOwner
              ) {
                return { state: "occupied" as const }
              }
              const active = yield* transaction
                .select({
                  id: SessionCommandTable.id,
                  created: SessionCommandTable.time_created,
                  status: SessionCommandTable.status,
                  leaseExpiresAt: SessionCommandTable.lease_expires_at,
                })
                .from(SessionCommandTable)
                .where(
                  and(
                    eq(SessionCommandTable.session_id, current.session_id),
                    inArray(SessionCommandTable.status, ["queued", "running"]),
                  ),
                )
                .all()
              const blocker = active.find(
                (item) =>
                  item.id !== current.id &&
                  (item.created < current.time_created ||
                    (item.created === current.time_created && item.id.localeCompare(current.id) < 0)),
              )
              if (blocker) {
                if (blocker.status === "running" && (!blocker.leaseExpiresAt || blocker.leaseExpiresAt <= now)) {
                  return { state: "blocked" as const, commandID: blocker.id }
                }
                return { state: "waiting" as const }
              }
              const claimed = yield* transaction
                .update(SessionCommandTable)
                .set({
                  status: "running",
                  owner_id: commandOwner,
                  claim_generation: current.claim_generation + 1,
                  lease_expires_at: now + commandLeaseMillis,
                  started_at: current.started_at ?? now,
                  time_updated: now,
                })
                .where(
                  and(
                    eq(SessionCommandTable.id, commandID),
                    eq(SessionCommandTable.status, current.status),
                    eq(SessionCommandTable.claim_generation, current.claim_generation),
                    current.status === "running"
                      ? reclaimDeadOwner
                        ? undefined
                        : or(
                            isNull(SessionCommandTable.lease_expires_at),
                            lt(SessionCommandTable.lease_expires_at, now),
                          )
                      : undefined,
                  ),
                )
                .returning()
                .get()
              if (!claimed) return { state: "occupied" as const }
              return { state: "ready" as const, command: claimed }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
      if (!deps.admit) return yield* claim
      return yield* deps.admit(claim).pipe(
        Effect.catchIf(
          (error): error is DeploymentDrainError =>
            error instanceof Error && "_tag" in error && error._tag === "DeploymentDrainError",
          () => Effect.succeed({ state: "draining" as const }),
        ),
      )
    })

    const waitForExecutionTurn = Effect.fn("SessionPrompt.waitForExecutionTurn")(function* (
      commandID: string,
      sessionID: SessionID,
    ) {
      while (true) {
        const [command, execution] = yield* Effect.all(
          [
            db
              .select({ status: SessionCommandTable.status })
              .from(SessionCommandTable)
              .where(eq(SessionCommandTable.id, commandID))
              .get()
              .pipe(Effect.orDie),
            db
              .select({ state: SessionExecutionTable.state, leaseExpiresAt: SessionExecutionTable.lease_expires_at })
              .from(SessionExecutionTable)
              .where(eq(SessionExecutionTable.session_id, sessionID))
              .get()
              .pipe(Effect.orDie),
          ],
          { concurrency: "unbounded" },
        )
        if (!command || ["succeeded", "failed", "cancelled"].includes(command.status)) return false
        if (execution?.state !== "running" || !execution.leaseExpiresAt || execution.leaseExpiresAt <= clock()) {
          return true
        }
        yield* Effect.sleep("200 millis")
      }
    })

    type ExecuteCommand = (commandID: string) => Effect.Effect<void>
    const executeCommand: ExecuteCommand = Effect.fn("SessionPrompt.executeCommand")(function* (commandID: string) {
      let claimed = yield* claimCommandTurn(commandID)
      while (
        claimed.state === "waiting" ||
        claimed.state === "occupied" ||
        claimed.state === "blocked" ||
        claimed.state === "draining"
      ) {
        if (claimed.state === "blocked") yield* executeCommand(claimed.commandID)
        yield* Effect.sleep("200 millis")
        claimed = yield* claimCommandTurn(commandID)
      }
      if (claimed.state !== "ready") return
      const command = claimed.command
      const ownershipLost = yield* Deferred.make<void>()
      const heartbeat = yield* Effect.sleep(Math.floor(commandLeaseMillis / 3)).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            const now = clock()
            const updated = yield* db
              .update(SessionCommandTable)
              .set({ lease_expires_at: now + commandLeaseMillis, time_updated: now })
              .where(
                and(
                  eq(SessionCommandTable.id, commandID),
                  eq(SessionCommandTable.status, "running"),
                  eq(SessionCommandTable.owner_id, commandOwner),
                  eq(SessionCommandTable.claim_generation, command.claim_generation),
                ),
              )
              .returning({ id: SessionCommandTable.id })
              .get()
              .pipe(Effect.orDie)
            if (updated) return
            yield* Deferred.succeed(ownershipLost, undefined)
            return yield* Effect.interrupt
          }),
        ),
        Effect.repeat(Schedule.forever),
        Effect.forkIn(scope),
      )
      const exit = yield* Effect.raceFirst(
        Effect.gen(function* () {
          if (!(yield* waitForExecutionTurn(commandID, command.session_id))) return
          const admitted = yield* db
            .select({ id: SessionCommandTable.id })
            .from(SessionCommandTable)
            .where(
              and(
                eq(SessionCommandTable.id, commandID),
                eq(SessionCommandTable.status, "running"),
                eq(SessionCommandTable.owner_id, commandOwner),
                eq(SessionCommandTable.claim_generation, command.claim_generation),
              ),
            )
            .get()
            .pipe(Effect.orDie)
          if (!admitted) return
          // A recovered command starts a new claim generation. Before it can
          // execute, durably close any unfinished parts from the old owner.
          // The owner/generation admission above prevents stale runners from
          // reconciling a newer generation's live execution.
          if (deps.reconcileToolParts) {
            yield* deps.reconcileToolParts({
              sessionID: command.session_id,
              messageID: command.message_id,
              commandID,
              generation: command.claim_generation,
              reason: "command claim resumed or settled before tool completion",
              owner: commandOwner,
            })
          }
          return yield* loop({
            sessionID: command.session_id,
            messageID: command.message_id,
            commandID,
            claimGeneration: command.claim_generation,
          })
        }),
        Deferred.await(ownershipLost).pipe(Effect.andThen(Effect.interrupt)),
      ).pipe(Effect.exit, Effect.ensuring(Fiber.interrupt(heartbeat)))
      if (yield* Deferred.isDone(ownershipLost)) return
      const completedAt = clock()
      const error = Exit.isFailure(exit)
        ? Cause.pretty(exit.cause)
        : exit.value?.info.role === "assistant" &&
            exit.value.info.error &&
            exit.value.info.error.name !== "MessageAbortedError"
          ? JSON.stringify(exit.value.info.error)
          : undefined
      const owned = yield* db
        .select({ id: SessionCommandTable.id })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.id, commandID),
            eq(SessionCommandTable.status, "running"),
            eq(SessionCommandTable.owner_id, commandOwner),
            eq(SessionCommandTable.claim_generation, command.claim_generation),
          ),
        )
        .get()
        .pipe(Effect.orDie)
      if (!owned) return
      if (!error) {
        yield* db
          .update(SessionCommandTable)
          .set({
            status: "succeeded",
            owner_id: null,
            lease_expires_at: null,
            completed_at: completedAt,
            time_updated: completedAt,
          })
          .where(
            and(
              eq(SessionCommandTable.id, commandID),
              eq(SessionCommandTable.status, "running"),
              eq(SessionCommandTable.owner_id, commandOwner),
              eq(SessionCommandTable.claim_generation, command.claim_generation),
            ),
          )
          .run()
          .pipe(Effect.orDie)
        if (deps.reconcileToolParts) {
          yield* deps.reconcileToolParts({
            sessionID: command.session_id,
            messageID: command.message_id,
            commandID,
            generation: command.claim_generation,
            reason: "command settled before tool completion",
          })
        }
        return
      }

      yield* db
        .update(SessionCommandTable)
        .set({
          status: "failed",
          owner_id: null,
          lease_expires_at: null,
          error,
          completed_at: completedAt,
          time_updated: completedAt,
        })
        .where(
          and(
            eq(SessionCommandTable.id, commandID),
            eq(SessionCommandTable.status, "running"),
            eq(SessionCommandTable.owner_id, commandOwner),
            eq(SessionCommandTable.claim_generation, command.claim_generation),
          ),
        )
        .run()
        .pipe(Effect.orDie)
      if (deps.reconcileToolParts) {
        yield* deps.reconcileToolParts({
          sessionID: command.session_id,
          messageID: command.message_id,
          commandID,
          generation: command.claim_generation,
          reason: "command failed before tool completion",
        })
      }
      if (Exit.isSuccess(exit)) return
      yield* Effect.logError("prompt_async failed").pipe(
        Effect.annotateLogs({ sessionID: command.session_id, cause: exit.cause }),
      )
      yield* events.publish(Session.Event.Error, {
        sessionID: command.session_id,
        error: new NamedError.Unknown({ message: error }).toObject(),
      })
    })

    const launchCommand = Effect.fn("SessionPrompt.launchCommand")(function* (commandID: string) {
      yield* executeCommand(commandID).pipe(
        Effect.catchCause((cause) => Effect.logError("prompt_async recovery failed", { commandID, cause })),
        Effect.forkIn(scope, { startImmediately: true }),
      )
    })

    const wakeSession = Effect.fn("SessionPrompt.wakeSession")(function* (sessionID: SessionID) {
      const now = clock()
      const commands = yield* db
        .select({ id: SessionCommandTable.id })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.session_id, sessionID),
            or(
              eq(SessionCommandTable.status, "queued"),
              and(
                eq(SessionCommandTable.status, "running"),
                or(isNull(SessionCommandTable.lease_expires_at), lt(SessionCommandTable.lease_expires_at, now)),
              ),
            ),
          ),
        )
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(commands, (command) => launchCommand(command.id), { discard: true })
    })

    const recover = Effect.fn("SessionPrompt.recover")(function* () {
      const ctx = yield* InstanceState.context
      const commands = yield* db
        .select({ id: SessionCommandTable.id })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.directory, ctx.directory),
            inArray(SessionCommandTable.status, ["queued", "running"]),
          ),
        )
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(commands, (command) => launchCommand(command.id), { discard: true })
    })
    const unregisterRecovery = SessionPromptRecovery.register(() => recover())
    yield* Effect.addFinalizer(() => Effect.sync(unregisterRecovery))

    /**
     * Cancels a message that is still waiting in the queue (OpencodeX-8vq).
     * A human who queued a prompt behind a long turn could not take it back:
     * the only lever was abort, which kills the turn that is actually
     * working. Only a `queued` row can be withdrawn - once a command is
     * running its turn owns it, and stopping that is what abort is for.
     */
    const cancelCommand = Effect.fn("SessionPrompt.cancelCommand")(function* (input: {
      sessionID: SessionID
      messageID: MessageID
    }) {
      const now = clock()
      return yield* db
        .transaction(
          (transaction) =>
            Effect.gen(function* () {
              const current = yield* transaction
                .select()
                .from(SessionCommandTable)
                .where(
                  and(
                    eq(SessionCommandTable.session_id, input.sessionID),
                    eq(SessionCommandTable.message_id, input.messageID),
                  ),
                )
                .get()
              if (!current) return "missing" as const
              if (current.status !== "queued") return "settled" as const
              if (current.offered_at !== null) return "running" as const
              yield* transaction
                .update(SessionCommandTable)
                .set({ status: "cancelled", completed_at: now, time_updated: now })
                .where(and(eq(SessionCommandTable.id, current.id), eq(SessionCommandTable.status, "queued")))
                .run()
              return "cancelled" as const
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    return {
      cancelCommand,
      commandOwner,
      commandLeaseMillis,
      claimCommandTurn,
      waitForExecutionTurn,
      executeCommand,
      launchCommand,
      wakeSession,
      recover,
    }
  })
}

export * as PromptClaim from "./prompt-claim"
