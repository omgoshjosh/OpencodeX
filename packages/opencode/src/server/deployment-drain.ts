import { Database } from "@opencode-ai/core/database/database"
import { OpencodeXGoalTable, OpencodeXJobTable, OpencodeXSwarmTable } from "@opencode-ai/core/opencodex/sql"
import { SessionCommandTable, SessionExecutionTable } from "@opencode-ai/core/session/sql"
import { ensureRunID } from "@opencode-ai/core/util/opencode-process"
import { and, count, eq, gt, inArray } from "drizzle-orm"
import { Context, Effect, Layer } from "effect"
import type { InstanceStore } from "@/project/instance-store"
import { SessionExecutionOwner } from "@/session/execution-owner"
import { SessionPromptRecovery } from "@/session/prompt-recovery"

const ACTIVE_SWARM_STATUSES = ["queued", "running", "cancelling"] as const
const gate = { draining: false, sealed: false, inFlight: 0, generation: 0 }

export function resetForTest() {
  gate.draining = false
  gate.sealed = false
  gate.inFlight = 0
  gate.generation++
}

export interface Status {
  readonly runID: string
  readonly accepting: boolean
  readonly draining: boolean
  readonly inFlightAdmissions: number
  readonly queuedCommands: number
  readonly runningCommands: number
  readonly liveRunningExecutions: number
  readonly activeJobs: number
  readonly activeSwarms: number
  readonly activeGoals: number
  readonly ready: boolean
}

export interface ReplayReceipt {
  readonly runID: string
  readonly directories: readonly string[]
  readonly commandCount: number
}

export class DeploymentDrainError extends Error {
  readonly _tag = "DeploymentDrainError"
  constructor(
    readonly kind: "conflict" | "unavailable",
    message: string,
  ) {
    super(message)
  }
}

export function isDraining() {
  return gate.draining
}

function admit<A, E, R>(effect: Effect.Effect<A, E, R>, completion: boolean) {
  return Effect.acquireUseRelease(
    Effect.suspend(() => {
      if (gate.draining && (!completion || gate.sealed))
        return Effect.fail(new DeploymentDrainError("unavailable", "server is draining and not accepting execution"))
      gate.inFlight++
      gate.generation++
      return Effect.void
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        gate.inFlight--
        gate.generation++
      }),
  )
}

export function admitExecution<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return admit(effect, false)
}

export function admitCompletion<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return admit(effect, true)
}

export interface Interface {
  readonly runID: string
  readonly isDraining: () => boolean
  readonly admit: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | DeploymentDrainError, R>
  readonly finish: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E | DeploymentDrainError, R>
  readonly begin: (expectedRunID: string) => Effect.Effect<Status, DeploymentDrainError>
  readonly cancel: (expectedRunID: string) => Effect.Effect<Status, DeploymentDrainError>
  readonly status: () => Effect.Effect<Status>
  readonly replay: (
    expectedRunID: string,
    store: InstanceStore.Interface,
  ) => Effect.Effect<ReplayReceipt, DeploymentDrainError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/DeploymentDrain") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const runID = ensureRunID()
    const status = Effect.fn("DeploymentDrain.status")(function* () {
      const now = Date.now()
      const drainingAtStart = gate.draining
      const generationAtStart = gate.generation
      const inFlightAdmissions = gate.inFlight
      const [queuedCommands, runningCommands, liveRunningExecutions, activeJobs, activeSwarms, activeGoals] = yield* db
        .transaction((transaction) =>
          Effect.all([
            transaction
              .select({ value: count() })
              .from(SessionCommandTable)
              .where(eq(SessionCommandTable.status, "queued"))
              .get(),
            transaction
              .select({ owner: SessionCommandTable.owner_id, leaseExpiresAt: SessionCommandTable.lease_expires_at })
              .from(SessionCommandTable)
              .where(eq(SessionCommandTable.status, "running"))
              .all(),
            transaction
              .select({ value: count() })
              .from(SessionExecutionTable)
              .where(and(eq(SessionExecutionTable.state, "running"), gt(SessionExecutionTable.lease_expires_at, now)))
              .get(),
            transaction
              .select({ value: count() })
              .from(OpencodeXJobTable)
              .where(
                and(
                  inArray(OpencodeXJobTable.status, ["claimed", "running"]),
                  gt(OpencodeXJobTable.lease_expires_at, now),
                ),
              )
              .get(),
            transaction
              .select({ value: count() })
              .from(OpencodeXSwarmTable)
              .where(inArray(OpencodeXSwarmTable.status, ACTIVE_SWARM_STATUSES))
              .get(),
            transaction
              .select({ value: count() })
              .from(OpencodeXGoalTable)
              .where(eq(OpencodeXGoalTable.status, "running"))
              .get(),
          ]),
        )
        .pipe(Effect.orDie)
      const liveRunningCommands = runningCommands.filter(
        (command) =>
          !!command.owner &&
          !!command.leaseExpiresAt &&
          command.leaseExpiresAt > now &&
          SessionExecutionOwner.alive(command.owner, runID),
      ).length
      const result = {
        runID,
        accepting: !gate.draining,
        draining: gate.draining,
        inFlightAdmissions,
        queuedCommands: queuedCommands?.value ?? 0,
        runningCommands: liveRunningCommands,
        liveRunningExecutions: liveRunningExecutions?.value ?? 0,
        activeJobs: activeJobs?.value ?? 0,
        activeSwarms: activeSwarms?.value ?? 0,
        activeGoals: activeGoals?.value ?? 0,
      }
      const ready =
        drainingAtStart &&
        generationAtStart === gate.generation &&
        result.draining &&
        // Queued jobs and durable workflow containers are restartable state,
        // like queued commands. Only process-owned execution blocks handoff.
        result.inFlightAdmissions + result.runningCommands + result.liveRunningExecutions + result.activeJobs === 0
      if (ready) gate.sealed = true
      return {
        ...result,
        ready,
      }
    })
    const verify = (expectedRunID: string) =>
      expectedRunID === runID
        ? Effect.void
        : Effect.fail(new DeploymentDrainError("conflict", "expectedRunID does not match this process"))
    return Service.of({
      runID,
      isDraining: () => gate.draining,
      admit: admitExecution,
      finish: admitCompletion,
      begin: (expectedRunID) =>
        Effect.gen(function* () {
          yield* verify(expectedRunID)
          yield* Effect.sync(() => {
            gate.draining = true
            gate.sealed = false
            gate.generation++
          })
          return yield* status()
        }),
      cancel: (expectedRunID) =>
        Effect.gen(function* () {
          yield* verify(expectedRunID)
          yield* Effect.sync(() => {
            gate.draining = false
            gate.sealed = false
            gate.generation++
          })
          return yield* status()
        }),
      status,
      replay: (expectedRunID, store) =>
        Effect.gen(function* () {
          yield* verify(expectedRunID)
          if (gate.draining)
            return yield* Effect.fail(
              new DeploymentDrainError("unavailable", "replay is only available on an accepting process"),
            )
          const now = Date.now()
          const candidates = yield* db
            .select({
              directory: SessionCommandTable.directory,
              owner: SessionCommandTable.owner_id,
              status: SessionCommandTable.status,
              leaseExpiresAt: SessionCommandTable.lease_expires_at,
            })
            .from(SessionCommandTable)
            .where(inArray(SessionCommandTable.status, ["queued", "running"]))
            .all()
            .pipe(Effect.orDie)
          const reclaimable = candidates.filter(
            (row) =>
              row.status === "queued" ||
              !row.owner ||
              (/^local:\d+:[^:]+:prompt:/.test(row.owner) && !SessionExecutionOwner.alive(row.owner, runID)) ||
              !row.leaseExpiresAt ||
              row.leaseExpiresAt <= now,
          )
          const directories = [...new Set(reclaimable.map((row) => row.directory))]
          yield* Effect.forEach(directories, (directory) => store.provide({ directory }, SessionPromptRecovery.run()), {
            concurrency: "unbounded",
            discard: true,
          })
          return { runID, directories, commandCount: reclaimable.length }
        }),
    })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Database.defaultLayer))

export * as DeploymentDrain from "./deployment-drain"
