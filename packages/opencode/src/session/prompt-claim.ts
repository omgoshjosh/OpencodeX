import { and, eq, inArray } from "drizzle-orm"
import { Cause, Context, Effect, Exit, Fiber, Schedule, Scope } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Database } from "@opencode-ai/core/database/database"
import { NamedError } from "@opencode-ai/core/util/error"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "./schema"
import type { LoopInput } from "./prompt-schema"
import * as Session from "./session"
import { SessionExecutionOwner } from "./execution-owner"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"

export interface Deps {
  readonly database: Context.Service.Shape<typeof Database.Service>
  readonly events: Context.Service.Shape<typeof EventV2Bridge.Service>
  readonly scope: Scope.Scope
  readonly loop: (input: LoopInput) => Effect.Effect<SessionLegacy.WithParts>
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
    const commandOwner = `local:${process.pid}:${processRunID}:${crypto.randomUUID()}`
    const commandLeaseMillis = 30_000

    const claimCommandTurn = Effect.fn("SessionPrompt.claimCommandTurn")(function* (commandID: string) {
      const now = Date.now()
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
              if (
                current.status === "running" &&
                current.owner_id &&
                current.lease_expires_at &&
                current.lease_expires_at > now &&
                SessionExecutionOwner.alive(current.owner_id, processRunID)
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
              const runningOwner = current.status === "running" ? current.owner_id : undefined
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
                    runningOwner ? eq(SessionCommandTable.owner_id, runningOwner) : undefined,
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
              .select({
                state: SessionExecutionTable.state,
                owner: SessionExecutionTable.owner_id,
                leaseExpiresAt: SessionExecutionTable.lease_expires_at,
              })
              .from(SessionExecutionTable)
              .where(eq(SessionExecutionTable.session_id, sessionID))
              .get()
              .pipe(Effect.orDie),
          ],
          { concurrency: "unbounded" },
        )
        if (!command || ["succeeded", "failed", "cancelled"].includes(command.status)) return false
        if (
          execution?.state !== "running" ||
          !execution.owner ||
          !execution.leaseExpiresAt ||
          execution.leaseExpiresAt <= Date.now() ||
          !SessionExecutionOwner.alive(execution.owner, processRunID)
        ) {
          return true
        }
        yield* Effect.sleep("200 millis")
      }
    })

    const executeCommand = Effect.fn("SessionPrompt.executeCommand")(function* (commandID: string) {
      let claimed = yield* claimCommandTurn(commandID)
      while (claimed.state === "waiting") {
        yield* Effect.sleep("200 millis")
        claimed = yield* claimCommandTurn(commandID)
      }
      if (claimed.state !== "ready") return
      const command = claimed.command
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

      const heartbeat = yield* Effect.sleep(Math.floor(commandLeaseMillis / 3)).pipe(
        Effect.andThen(
          db
            .update(SessionCommandTable)
            .set({ lease_expires_at: Date.now() + commandLeaseMillis, time_updated: Date.now() })
            .where(
              and(
                eq(SessionCommandTable.id, commandID),
                eq(SessionCommandTable.status, "running"),
                eq(SessionCommandTable.owner_id, commandOwner),
                eq(SessionCommandTable.claim_generation, command.claim_generation),
              ),
            )
            .run()
            .pipe(Effect.orDie),
        ),
        Effect.repeat(Schedule.forever),
        Effect.forkIn(scope),
      )
      const requeue = Effect.fnUntraced(function* () {
        const completedAt = Date.now()
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
      const exit = yield* Effect.uninterruptibleMask((restore) =>
        restore(
          loop({ sessionID: command.session_id, messageID: command.message_id }).pipe(
            Effect.onInterrupt(() => requeue()),
          ),
        ).pipe(Effect.exit, Effect.ensuring(Fiber.interrupt(heartbeat))),
      )
      const completedAt = Date.now()
      if (Exit.isSuccess(exit)) {
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
    yield* Effect.addFinalizer(() =>
      db
        .update(SessionCommandTable)
        .set({
          status: "queued",
          owner_id: null,
          lease_expires_at: null,
          error: null,
          completed_at: null,
          time_updated: Date.now(),
        })
        .where(and(eq(SessionCommandTable.status, "running"), eq(SessionCommandTable.owner_id, commandOwner)))
        .run()
        .pipe(Effect.orDie),
    )

    return {
      commandOwner,
      commandLeaseMillis,
      claimCommandTurn,
      waitForExecutionTurn,
      executeCommand,
      launchCommand,
      recover,
    }
  })
}
