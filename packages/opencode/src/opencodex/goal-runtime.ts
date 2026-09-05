import { Database } from "@opencode-ai/core/database/database"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceBootstrap } from "@/project/bootstrap"
import { InstanceStore } from "@/project/instance-store"
import { OpencodeXJob } from "@/opencodex/job"
import { OpencodeXJobDispatcher } from "@/opencodex/job-dispatcher"
import { OpencodeXProject } from "@/opencodex/project"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { MessageID, SessionID } from "@/session/schema"
import { SessionPrompt } from "@/session/prompt"
import { Skill } from "@/skill"
import { Context, Effect, Layer, Schedule, Scope } from "effect"
import { GOAL_NODE_JOB_KIND, GoalDispatch } from "./goal-dispatch"
import { GoalExecution, goalExecutionLayer } from "./goal-execution"
import { OpencodeXGoal, StateEvent } from "./goal"
import { GoalStoreService } from "./goal-store"

/**
 * Where planning meets running. The goal service can be built anywhere - it
 * only talks to the store and the job queue - but executing a node needs the
 * prompt loop, which builds the tool registry, which imports the goal service
 * for its graph tools. So the executing half lives here, outside that cycle,
 * and is provided once by the process that runs jobs.
 */

/** How often standing goals are checked. Cadences are minutes-and-up. */
const SCHEDULE_TICK_MS = 30_000
const CHILD_ABORT_CLAIM_MS = 60_000
const CHILD_ABORT_RETRY_MS = 30_000

export class GoalRuntime extends Context.Service<GoalRuntime, { readonly kind: string }>()(
  "@opencode/OpencodeXGoalRuntime",
) {}

export const layer = Layer.effect(
  GoalRuntime,
  Effect.gen(function* () {
    const dispatcher = yield* OpencodeXJobDispatcher.Service
    const dispatch = yield* GoalDispatch
    const execution = yield* GoalExecution
    const store = yield* GoalStoreService
    const prompt = yield* SessionPrompt.Service
    const jobs = yield* OpencodeXJob.Service
    const events = yield* EventV2Bridge.Service
    const unregister = yield* dispatcher.register(GOAL_NODE_JOB_KIND, execution.executeNode, dispatch.settleNode)
    yield* Effect.addFinalizer(() => unregister)
    const deliverChildAbortReports = Effect.fn("OpencodeXGoal.deliverChildAbortReports")(function* () {
      const now = Date.now()
      const goals = yield* store.list()
      yield* Effect.forEach(
        goals.flatMap((goal) =>
          goal.ownerSessionID
            ? goal.nodes
                .filter(
                  (node) =>
                    node.status === "cancelled" &&
                    isChildAbort(node.metadata) &&
                    childAbortClaimable(node.metadata.childAbort, now) &&
                    node.sessionID,
                )
                .map((node) => ({ goal, node }))
            : [],
        ),
        ({ goal, node }) =>
          Effect.gen(function* () {
            const childAbort = node.metadata!.childAbort
            const claimToken = crypto.randomUUID()
            const claimedMetadata = {
              ...node.metadata,
              childAbort: {
                ...childAbort,
                delivery: "claimed",
                claimToken,
                claimExpiresAt: now + CHILD_ABORT_CLAIM_MS,
                retryAt: undefined,
              },
            }
            const claimed = yield* store.compareAndSetNodeMetadata({
              goalID: goal.id,
              nodeID: node.id,
              expected: node.metadata!,
              next: claimedMetadata,
            })
            if (!claimed) return
            const delivered = yield* prompt
              .promptAsync({
                sessionID: SessionID.make(goal.ownerSessionID!),
                messageID: MessageID.make(childAbort.messageID),
                parts: [
                  {
                    type: "text",
                    synthetic: true,
                    metadata: { task_report: true },
                    text: [
                      `<task id="${node.sessionID}" state="error">`,
                      `<summary>Goal node aborted: ${node.title}</summary>`,
                      `<task_error>${childAbort.error}</task_error>`,
                      "</task>",
                    ].join("\n"),
                  },
                ],
              })
              .pipe(
                Effect.as(true),
                Effect.catchCause(() => Effect.succeed(false)),
              )
            const nextMetadata = {
              ...node.metadata,
              childAbort: {
                ...childAbort,
                delivery: delivered ? "delivered" : "pending",
                claimToken: undefined,
                claimExpiresAt: undefined,
                retryAt: delivered ? undefined : Date.now() + CHILD_ABORT_RETRY_MS,
              },
            }
            yield* store.compareAndSetNodeMetadata({
              goalID: goal.id,
              nodeID: node.id,
              expected: claimedMetadata,
              next: nextMetadata,
            })
          }),
        { concurrency: 1, discard: true },
      )
    })
    const unregisterCancel = yield* prompt.onCancel((sessionID) =>
      Effect.gen(function* () {
        const goals = yield* store.list()
        yield* Effect.forEach(
          goals.flatMap((goal) =>
            goal.nodes
              .filter(
                (node) => node.sessionID === sessionID && node.jobID && ["dispatched", "running"].includes(node.status),
              )
              .map((node) => ({ goal, node })),
          ),
          ({ goal, node }) =>
            Effect.gen(function* () {
              const metadata = {
                ...node.metadata,
                childAbort: {
                  source: "child_session",
                  delivery: "pending",
                  messageID: `msg_goal_child_abort_${node.sessionID}`,
                  error: "Child session was aborted.",
                },
              }
              yield* store.patchNodes(goal.id, [{ nodeID: node.id, patch: { metadata } }])
              yield* jobs.cancel(node.jobID!, dispatch.settleNode).pipe(Effect.ignore)
            }),
          { concurrency: 1, discard: true },
        )
        yield* deliverChildAbortReports().pipe(Effect.ignore)
      }).pipe(Effect.ignore),
    )
    yield* Effect.addFinalizer(() => unregisterCancel)
    const unsubscribeReports = yield* events.listen((event) =>
      event.type === StateEvent.Updated.type ? deliverChildAbortReports().pipe(Effect.ignore) : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribeReports)
    // A restart leaves goals mid-graph. One sweep re-dispatches whatever the
    // crash interrupted, because readiness is recomputed from state rather
    // than remembered - a node whose job vanished simply looks ready again.
    yield* dispatch.reconcileAll().pipe(Effect.andThen(deliverChildAbortReports()), Effect.ignore)
    // Standing goals have no session to wake them, so a slow tick is what
    // starts them. The cadence itself lives on each goal.
    const scope = yield* Scope.Scope
    yield* Effect.sleep(SCHEDULE_TICK_MS).pipe(
      Effect.andThen(
        Effect.all([dispatch.sweepSchedules(), deliverChildAbortReports()], { discard: true }).pipe(Effect.ignore),
      ),
      Effect.repeat(Schedule.forever),
      Effect.forkIn(scope),
    )
    return GoalRuntime.of({ kind: GOAL_NODE_JOB_KIND })
  }),
)

const executionLayer = goalExecutionLayer.pipe(
  Layer.provide(
    Layer.mergeAll(
      Database.defaultLayer,
      InstanceStore.defaultLayer.pipe(Layer.provide(InstanceBootstrap.defaultLayer)),
      OpencodeXJob.defaultLayer,
      OpencodeXProject.defaultLayer,
      Provider.defaultLayer,
      Session.defaultLayer,
      SessionPrompt.defaultLayer,
      Skill.defaultLayer,
      OpencodeXGoal.storeOnlyLayer,
    ),
  ),
)

export const defaultLayer = layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      executionLayer,
      OpencodeXGoal.defaultLayer,
      OpencodeXGoal.storeOnlyLayer,
      OpencodeXJobDispatcher.defaultLayer,
      SessionPrompt.defaultLayer,
    ),
  ),
  Layer.provide(Layer.mergeAll(Database.defaultLayer, EventV2Bridge.defaultLayer, OpencodeXJob.defaultLayer)),
)

function isChildAbort(metadata: Record<string, unknown> | undefined): metadata is {
  childAbort: ChildAbort
} {
  const childAbort = metadata?.childAbort
  return (
    typeof childAbort === "object" &&
    childAbort !== null &&
    "source" in childAbort &&
    childAbort.source === "child_session" &&
    "delivery" in childAbort &&
    typeof childAbort.delivery === "string" &&
    "messageID" in childAbort &&
    typeof childAbort.messageID === "string" &&
    "error" in childAbort &&
    typeof childAbort.error === "string"
  )
}

type ChildAbort = {
  source: "child_session"
  delivery: string
  messageID: string
  error: string
  claimToken?: string
  claimExpiresAt?: number
  retryAt?: number
}

function childAbortClaimable(childAbort: ChildAbort, now: number) {
  if (childAbort.delivery === "pending") return (childAbort.retryAt ?? 0) <= now
  return childAbort.delivery === "claimed" && (childAbort.claimExpiresAt ?? 0) <= now
}

export * as OpencodeXGoalRuntime from "./goal-runtime"
