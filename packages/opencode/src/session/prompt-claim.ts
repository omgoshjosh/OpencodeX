import { and, asc, desc, eq, inArray, isNull, lt, min, or } from "drizzle-orm"
import { Cause, Context, Duration, Effect, Exit, Fiber, Schedule, Scope } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import {
  MessageTable,
  SessionCommandTable,
  SessionExecutionTable,
  SessionStatusTable,
  SessionTable,
} from "@opencode-ai/core/session/sql"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { MessageID, SessionID } from "./schema"
import type { LoopInput } from "./prompt-schema"
import { SessionExecutionOwner } from "./execution-owner"
import * as Session from "./session"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly events: Context.Service.Shape<typeof EventV2Bridge.Service>
  readonly scope: Scope.Scope
  readonly loop: (input: LoopInput) => Effect.Effect<SessionLegacy.WithParts>
  /** Test seam: how long a claimed command's lease lasts. */
  readonly commandLeaseMillis?: number
  /** Test seam: the wall clock every lease decision reads. */
  readonly clock?: () => number
  readonly recoveryInterval?: Duration.Input
  readonly beforeExecutionAdmission?: (input: { sessionID: SessionID; commandID: string }) => Effect.Effect<void>
}

/**
 * Durable admission for queued prompts. A prompt is a row in
 * `session_command`; exactly one process may run it at a time, which it proves
 * by holding a lease it heartbeats. Everything here is about winning, holding
 * and settling that lease — the turn itself is `loop`.
 */
export function make(deps: Deps) {
  return Effect.gen(function* () {
    const { database, events, scope, loop, beforeExecutionAdmission } = deps
    const { db } = database
    const processRunID = ensureRunID()
    const commandOwner = `local:${process.pid}:${processRunID}:${crypto.randomUUID()}`
    const commandLeaseMillis = deps.commandLeaseMillis ?? 30_000
    const clock = deps.clock ?? Date.now
    const recoveryBatchSize = 32
    const recoveryInterval = deps.recoveryInterval ?? "20 seconds"
    const launching = new Set<string>()

    const diagnostic = Effect.fnUntraced(function* (
      commandID: string,
      action: "launch" | "claim",
      cas: "not-attempted" | "ready" | "waiting" | "done",
    ) {
      const command = yield* db
        .select()
        .from(SessionCommandTable)
        .where(eq(SessionCommandTable.id, commandID))
        .get()
        .pipe(Effect.orDie)
      if (!command) {
        yield* Effect.logInfo("session command recovery", {
          commandID,
          statusPresent: false,
          action,
          cas,
        })
        return
      }
      const [execution, status] = yield* Effect.all(
        [
          db
            .select()
            .from(SessionExecutionTable)
            .where(eq(SessionExecutionTable.session_id, command.session_id))
            .get()
            .pipe(Effect.orDie),
          db
            .select({ sessionID: SessionStatusTable.session_id })
            .from(SessionStatusTable)
            .where(eq(SessionStatusTable.session_id, command.session_id))
            .get()
            .pipe(Effect.orDie),
        ],
        { concurrency: "unbounded" },
      )
      yield* Effect.logInfo("session command recovery", {
        commandID,
        commandAgeMillis: clock() - command.time_created,
        executionGeneration: execution?.generation,
        executionOwner: execution?.owner_id,
        executionLeaseExpiresAt: execution?.lease_expires_at,
        statusPresent: !!status,
        action,
        cas,
      })
    })

    const claimCommandTurn = Effect.fn("SessionPrompt.claimCommandTurn")(function* (commandID: string) {
      const now = clock()
      return yield* db
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
              const execution = yield* transaction
                .select({
                  state: SessionExecutionTable.state,
                  owner: SessionExecutionTable.owner_id,
                  leaseExpiresAt: SessionExecutionTable.lease_expires_at,
                })
                .from(SessionExecutionTable)
                .where(eq(SessionExecutionTable.session_id, current.session_id))
                .get()
              if (execution?.state === "running" && execution.leaseExpiresAt && execution.leaseExpiresAt > now) {
                return { state: "waiting" as const }
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
              if (parentLive) return { state: "waiting" as const }
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
                /^local:\d+:[^:]+:/.test(current.owner_id) &&
                !SessionExecutionOwner.alive(current.owner_id, processRunID)
              if (
                current.status === "running" &&
                current.lease_expires_at &&
                current.lease_expires_at > now &&
                !reclaimDeadOwner
              ) {
                return { state: "waiting" as const }
              }
              const active = yield* transaction
                .select({
                  id: SessionCommandTable.id,
                  created: SessionCommandTable.time_created,
                })
                .from(SessionCommandTable)
                .where(
                  and(
                    eq(SessionCommandTable.session_id, current.session_id),
                    inArray(SessionCommandTable.status, ["queued", "running"]),
                  ),
                )
                .all()
              const blocked = active.some(
                (item) =>
                  item.id !== current.id &&
                  (item.created < current.time_created ||
                    (item.created === current.time_created && item.id.localeCompare(current.id) < 0)),
              )
              if (blocked) return { state: "waiting" as const }
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
                      ? or(isNull(SessionCommandTable.lease_expires_at), lt(SessionCommandTable.lease_expires_at, now))
                      : undefined,
                  ),
                )
                .returning()
                .get()
              if (!claimed) return { state: "waiting" as const }
              return { state: "ready" as const, command: claimed }
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const waitForExecutionTurn = Effect.fn("SessionPrompt.waitForExecutionTurn")(function* (
      commandID: string,
      sessionID: SessionID,
    ) {
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
      return execution?.state !== "running" || !execution.leaseExpiresAt || execution.leaseExpiresAt <= clock()
    })

    const executeCommand = Effect.fn("SessionPrompt.executeCommand")(function* (commandID: string) {
      const claimed = yield* claimCommandTurn(commandID)
      yield* diagnostic(commandID, "claim", claimed.state).pipe(Effect.catchCause(() => Effect.void))
      if (claimed.state !== "ready") return
      const command = claimed.command
      const requeue = Effect.fnUntraced(function* () {
        const completedAt = clock()
        yield* db
          .update(SessionCommandTable)
          .set({
            status: "queued",
            owner_id: null,
            lease_expires_at: null,
            error: null,
            completed_at: null,
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
      })
      if (beforeExecutionAdmission) yield* beforeExecutionAdmission({ sessionID: command.session_id, commandID })
      if (!(yield* waitForExecutionTurn(commandID, command.session_id))) {
        yield* requeue()
        return
      }
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

      const heartbeat = yield* Effect.sleep(Math.floor(commandLeaseMillis / 3)).pipe(
        Effect.andThen(
          // Suspended so the clock is read on EVERY beat. Built eagerly, drizzle
          // bakes the first timestamp into the statement and every later beat
          // rewrites the same already-expiring lease.
          Effect.suspend(() => {
            const now = clock()
            return db
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
              .run()
              .pipe(Effect.orDie)
          }),
        ),
        Effect.repeat(Schedule.forever),
        Effect.forkIn(scope),
      )
      const exit = yield* Effect.uninterruptibleMask((restore) =>
        restore(
          loop({
            sessionID: command.session_id,
            messageID: command.message_id,
            commandID,
            claimGeneration: command.claim_generation,
            claimOwner: commandOwner,
          }).pipe(Effect.onInterrupt(() => requeue())),
        ).pipe(Effect.exit, Effect.ensuring(Fiber.interrupt(heartbeat))),
      )
      const completedAt = clock()
      // A turn that returns an errored assistant message is a FAILED command,
      // not a succeeded one: the effect succeeded, but the work did not. The
      // error itself was already published by the loop, so this only records
      // the durable outcome (an abort is a cancellation, not a failure).
      const assistantError =
        Exit.isSuccess(exit) &&
        exit.value?.info.role === "assistant" &&
        exit.value.info.error &&
        exit.value.info.error.name !== "MessageAbortedError"
          ? JSON.stringify(exit.value.info.error)
          : undefined
      if (Exit.isSuccess(exit)) {
        yield* db
          .update(SessionCommandTable)
          .set({
            status: assistantError ? "failed" : "succeeded",
            owner_id: null,
            lease_expires_at: null,
            ...(assistantError ? { error: assistantError } : {}),
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
        yield* Effect.suspend(() => wakeSession(command.session_id))
        return
      }

      if (Cause.hasInterruptsOnly(exit.cause)) {
        yield* requeue()
        return
      }

      const error = Cause.pretty(exit.cause)
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
      yield* Effect.suspend(() => wakeSession(command.session_id))
      yield* Effect.logError("prompt_async failed").pipe(
        Effect.annotateLogs({ sessionID: command.session_id, cause: exit.cause }),
      )
      yield* events.publish(Session.Event.Error, {
        sessionID: command.session_id,
        error: new NamedError.Unknown({ message: error }).toObject(),
      })
    })

    const launchCommand = Effect.fn("SessionPrompt.launchCommand")(function* (commandID: string) {
      if (launching.has(commandID)) return
      launching.add(commandID)
      yield* executeCommand(commandID).pipe(
        Effect.catchCause((cause) => Effect.logError("prompt_async recovery failed", { commandID, cause })),
        Effect.ensuring(Effect.sync(() => launching.delete(commandID))),
        Effect.forkIn(scope, { startImmediately: true }),
      )
    })

    /**
     * Launches every command this session still owes work on: anything queued,
     * plus a `running` row whose lease has lapsed (its owner died). Pin-only
     * entry point used by `promptAsync`, so a newly queued or re-queued prompt
     * starts without waiting for the next recovery sweep.
     */
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
      const sessions = yield* db
        .select({ sessionID: SessionCommandTable.session_id, oldest: min(SessionCommandTable.time_created) })
        .from(SessionCommandTable)
        .where(
          and(
            eq(SessionCommandTable.directory, ctx.directory),
            or(
              and(eq(SessionCommandTable.status, "queued"), isNull(SessionCommandTable.owner_id)),
              and(eq(SessionCommandTable.status, "running"), lt(SessionCommandTable.lease_expires_at, clock())),
            ),
          ),
        )
        .groupBy(SessionCommandTable.session_id)
        .orderBy(asc(min(SessionCommandTable.time_created)), asc(SessionCommandTable.session_id))
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      yield* Effect.forEach(
        sessions,
        (session) =>
          db
            .select({ id: SessionCommandTable.id })
            .from(SessionCommandTable)
            .where(
              and(
                eq(SessionCommandTable.session_id, session.sessionID),
                eq(SessionCommandTable.directory, ctx.directory),
                or(
                  and(eq(SessionCommandTable.status, "queued"), isNull(SessionCommandTable.owner_id)),
                  and(eq(SessionCommandTable.status, "running"), lt(SessionCommandTable.lease_expires_at, clock())),
                ),
              ),
            )
            .orderBy(asc(SessionCommandTable.time_created), asc(SessionCommandTable.id))
            .limit(1)
            .get()
            .pipe(
              Effect.orDie,
              Effect.flatMap((command) =>
                command
                  ? diagnostic(command.id, "launch", "not-attempted").pipe(
                      Effect.catchCause(() => Effect.void),
                      Effect.andThen(launchCommand(command.id)),
                    )
                  : Effect.void,
              ),
            ),
        { discard: true },
      )
      const messages = yield* db
        .select({
          id: MessageTable.id,
          sessionID: MessageTable.session_id,
          data: MessageTable.data,
          created: MessageTable.time_created,
          commandID: SessionCommandTable.id,
        })
        .from(MessageTable)
        .innerJoin(SessionTable, eq(SessionTable.id, MessageTable.session_id))
        .leftJoin(
          SessionCommandTable,
          and(
            eq(SessionCommandTable.session_id, MessageTable.session_id),
            eq(SessionCommandTable.message_id, MessageTable.id),
          ),
        )
        .where(eq(SessionTable.directory, ctx.directory))
        .orderBy(desc(MessageTable.time_created))
        .limit(recoveryBatchSize)
        .all()
        .pipe(Effect.orDie)
      for (const message of messages) {
        if (message.data.role !== "user" || message.commandID) continue
        if (
          messages.some(
            (other) =>
              other.sessionID === message.sessionID &&
              other.created > message.created &&
              other.data.role === "assistant",
          )
        )
          continue
        yield* Effect.logWarning("session transcript has pending message without durable command", {
          messageID: message.id,
          sessionID: message.sessionID,
          messageAgeMillis: clock() - message.created,
          action: "transcript-only",
          cas: "not-attempted",
        }).pipe(Effect.catchCause(() => Effect.void))
      }
    })
    const recovery = yield* InstanceState.make(() =>
      Effect.gen(function* () {
        yield* Effect.sleep(recoveryInterval).pipe(
          Effect.andThen(recover()),
          Effect.catchCause((cause) => Effect.logWarning("session command recovery sweep failed", { cause })),
          Effect.repeat(Schedule.forever),
          Effect.forkScoped,
        )
      }),
    )
    const start = Effect.fn("SessionPrompt.startRecovery")(function* () {
      yield* InstanceState.get(recovery)
    })
    yield* Effect.addFinalizer(() =>
      db
        .update(SessionCommandTable)
        .set({
          status: "queued",
          owner_id: null,
          lease_expires_at: null,
          error: null,
          completed_at: null,
          time_updated: clock(),
        })
        .where(and(eq(SessionCommandTable.status, "running"), eq(SessionCommandTable.owner_id, commandOwner)))
        .run()
        .pipe(Effect.orDie),
    )

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
      start,
    }
  })
}

export * as PromptClaim from "./prompt-claim"
