import { beforeEach, describe, expect } from "bun:test"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import { Agent } from "@/agent/agent"
import { OpencodeXGoal } from "@/opencodex/goal"
import { OpencodeXProject } from "@/opencodex/project"
import { Project } from "@/project/project"
import { MessageID, SessionID } from "@/session/schema"
import { NotFoundError } from "@/storage/storage"
import { GraphPlanTool } from "@/tool/graph"
import { Tool } from "@/tool/tool"
import { Truncate } from "@/tool/truncate"
import { Effect, Layer } from "effect"
import { testEffect } from "../lib/effect"

const sessionID = SessionID.make("ses_graph_plan_tool")
const directory = "/tmp/graph-plan-tool"
const created: OpencodeXGoal.CreateInput[] = []
const planned: OpencodeXGoal.PlanInput[] = []
const contexts: ({ swarmID?: string | null; directory?: string } | undefined)[] = []
const started: string[] = []
const moved: OpencodeXProject.MoveSessionInput[] = []
let listed: OpencodeXGoal.Info[] = []
let projectList: OpencodeXProject.Info[] = []
let assignmentWinner: string | undefined
let assignmentFailure: Project.NotFoundError | NotFoundError | undefined

const goal = (
  status: OpencodeXGoal.Status = "draft",
  context?: { swarmID?: string; directory?: string },
): OpencodeXGoal.Info => ({
  id: "goal-1",
  projectID: "project-1",
  title: "Dispatch context",
  statement: "Verify graph dispatch context",
  successCriteria: [],
  status,
  source: "manual",
  ownerSessionID: sessionID,
  swarmID: context?.swarmID,
  directory: context?.directory,
  spend: { nodeRuns: 0, costUsd: 0 },
  nodes: [],
  edges: [],
  timeCreated: 1,
  timeUpdated: 1,
})

const goals = Layer.mock(OpencodeXGoal.Service)({
  list: () => Effect.sync(() => listed),
  create: (input) => Effect.sync(() => (created.push(input), goal())),
  plan: (_goalID, input, context) =>
    Effect.sync(() => (planned.push(input), contexts.push(context), goal("planned"))),
  start: (goalID) => Effect.sync(() => (started.push(goalID), goal("running"))),
})
const projectInfo = (input?: { id?: string; folders?: string[]; assigned?: boolean }) =>
  // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion -- projectForSession only reads these fields
  ({
    id: input?.id ?? "project-1",
    project: { id: input?.id ?? "project-1", worktree: directory, vcs: "git", time: { created: 1, updated: 1 } },
    folders: (input?.folders ?? []).map((path) => ({ path })),
    sessions: input?.assigned === false ? [] : [{ id: sessionID }],
    terminalSessions: [],
  }) as unknown as OpencodeXProject.Info

const projects = Layer.mock(OpencodeXProject.Service)({
  list: () => Effect.sync(() => projectList),
  assignSessionIfUnassigned: (input) =>
    Effect.gen(function* () {
      moved.push(input)
      if (assignmentFailure) return yield* assignmentFailure
      return assignmentWinner ?? input.projectID
    }),
})
const agents = Layer.mock(Agent.Service)({
  get: () => Effect.succeed({ name: "build", mode: "primary", permission: [], options: {} }),
})
const truncate = Layer.mock(Truncate.Service)({
  output: (text) => Effect.succeed({ content: text, truncated: false }),
})
const it = testEffect(Layer.mergeAll(goals, projects, agents, truncate))

beforeEach(() => {
  assignmentWinner = undefined
  assignmentFailure = undefined
})

function userMessage(id: string, providerID: string, modelID: string): SessionLegacy.WithParts {
  return {
    info: {
      id: MessageID.make(id),
      sessionID,
      role: "user",
      time: { created: 1 },
      agent: "build",
      model: { providerID: ProviderV2.ID.make(providerID), modelID: ProviderV2.ModelID.make(modelID) },
    },
    parts: [],
  }
}

function context(messages: SessionLegacy.WithParts[], model?: { providerID: string; modelID: string }): Tool.Context {
  return {
    sessionID,
    directory,
    messageID: MessageID.make("msg_graph_plan_tool"),
    agent: "build",
    abort: new AbortController().signal,
    messages,
    extra: model ? { model } : undefined,
    metadata: () => Effect.void,
    ask: () => Effect.void,
  }
}

const params = {
  goal: "Verify graph dispatch context",
  wait: false,
  nodes: [
    {
      id: "root",
      title: "Root",
      brief: "Run at the root",
      kind: "task" as const,
      parentNodeID: "  ",
      loop: { exitCheckNodeID: "ignored", maxIterations: 9 },
    },
    {
      id: "repeat",
      title: "Repeat",
      brief: "Repeat until verified",
      kind: "loop" as const,
      loop: { exitCheckNodeID: "check", maxIterations: 4 },
    },
  ],
}

describe("graph_plan", () => {
  it.effect("passes normalized dispatch context through execute", () =>
    Effect.gen(function* () {
      created.length = 0
      planned.length = 0
      contexts.length = 0
      started.length = 0
      moved.length = 0
      listed = []
      projectList = [projectInfo()]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(
        params,
        context([userMessage("msg_001", "swarm", "older-swarm"), userMessage("msg_002", "openai", "gpt-5")], {
          providerID: "swarm",
          modelID: "direct-swarm",
        }),
      )

      expect(created).toHaveLength(1)
      expect(created[0]).toMatchObject({ directory, swarmID: "direct-swarm", ownerSessionID: sessionID })
      expect(planned).toHaveLength(1)
      expect(planned[0].nodes).toEqual([
        expect.objectContaining({ id: "root", parentNodeID: undefined, loop: undefined }),
        expect.objectContaining({ id: "repeat", loop: { exitCheckNodeID: "check", maxIterations: 4 } }),
      ])
      expect(started).toEqual(["goal-1"])
      expect(contexts).toEqual([{ swarmID: "direct-swarm", directory }])
      expect(moved).toHaveLength(0)
    }),
  )

  it.effect("uses only the latest persisted user model for swarm fallback", () =>
    Effect.gen(function* () {
      created.length = 0
      listed = []
      projectList = [projectInfo()]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(params, context([userMessage("msg_002", "swarm", "persisted-swarm")]))
      yield* tool.execute(
        params,
        context([userMessage("msg_002", "anthropic", "claude"), userMessage("msg_001", "swarm", "stale-swarm")]),
      )

      expect(created.map((input) => input.swarmID)).toEqual(["persisted-swarm", undefined])
    }),
  )

  it.effect("updates stale dispatch context while preserving a reusable goal", () =>
    Effect.gen(function* () {
      created.length = 0
      contexts.length = 0
      listed = [goal("planned", { swarmID: "stale-swarm", directory: "/tmp/stale" })]
      projectList = [projectInfo()]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(params, context([userMessage("msg_002", "swarm", "current-swarm")]))

      expect(created).toHaveLength(0)
      expect(contexts).toEqual([{ swarmID: "current-swarm", directory }])
    }),
  )

  it.effect("attaches an unassigned session to its single unambiguous project", () =>
    Effect.gen(function* () {
      created.length = 0
      moved.length = 0
      listed = []
      projectList = [projectInfo({ assigned: false })]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(params, context([]))

      expect(moved).toEqual([{ projectID: "project-1", sessionID }])
      expect(created).toHaveLength(1)
    }),
  )

  it.effect("attaches by the longest unique matching project folder", () =>
    Effect.gen(function* () {
      created.length = 0
      moved.length = 0
      listed = []
      projectList = [
        projectInfo({ id: "project-parent", folders: ["/tmp"], assigned: false }),
        projectInfo({ id: "project-1", folders: [directory], assigned: false }),
      ]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(params, context([]))

      expect(moved).toEqual([{ projectID: "project-1", sessionID }])
      expect(created).toHaveLength(1)
    }),
  )

  it.effect("leaves an unassigned session unattached when project routing is ambiguous", () =>
    Effect.gen(function* () {
      created.length = 0
      moved.length = 0
      listed = []
      projectList = [
        projectInfo({ id: "project-1", assigned: false }),
        projectInfo({ id: "project-2", assigned: false }),
      ]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      const result = yield* tool.execute(params, context([]))

      expect(result.title).toBe("No project")
      expect(moved).toHaveLength(0)
      expect(created).toHaveLength(0)
    }),
  )

  it.effect("uses a concurrent durable assignment winner instead of stale inference", () =>
    Effect.gen(function* () {
      created.length = 0
      moved.length = 0
      listed = []
      assignmentWinner = "project-2"
      projectList = [
        projectInfo({ id: "project-1", folders: [directory], assigned: false }),
        projectInfo({ id: "project-2", folders: ["/tmp/other"], assigned: false }),
      ]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      yield* tool.execute(params, context([]))

      expect(moved).toEqual([{ projectID: "project-1", sessionID }])
      expect(created[0]?.projectID).toBe("project-2")
    }),
  )

  it.effect("propagates project and session assignment failures", () =>
    Effect.gen(function* () {
      created.length = 0
      listed = []
      projectList = [projectInfo({ assigned: false })]
      const tool = yield* Tool.init(yield* GraphPlanTool)

      assignmentFailure = new Project.NotFoundError({ projectID: ProjectV2.ID.make("missing") })
      const projectError = yield* tool.execute(params, context([])).pipe(Effect.flip)
      expect(projectError).toMatchObject({ _tag: "Project.NotFoundError" })

      assignmentFailure = new NotFoundError({ message: "Session not found" })
      const sessionError = yield* tool.execute(params, context([])).pipe(Effect.flip)
      expect(sessionError).toMatchObject({ _tag: "NotFoundError" })
      expect(created).toHaveLength(0)
    }),
  )
})
