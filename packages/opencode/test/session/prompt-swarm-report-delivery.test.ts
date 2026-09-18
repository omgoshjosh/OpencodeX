import { beforeEach, describe, expect, test } from "bun:test"
import { Effect, Exit, Logger, Option } from "effect"
import { SessionLegacy } from "@opencode-ai/core/session/legacy"
import * as Log from "@opencode-ai/core/util/log"
import * as PromptSwarm from "../../src/session/prompt-swarm"
import { resetSessionAgentWarnings, resolveSessionAgent, WARNED_AGENT_PAIRS_CAP } from "../../src/session/session-agent"
import type { DelegationRecord } from "../../src/session/delegation-outcome"
import { SessionID } from "../../src/session/schema"

/**
 * OpencodeX-557: a parent whose stored `agent` is not in the registry (e.g.
 * "claude-code", written through PATCH /session) made the report prompt throw
 * "Agent not found"; deliverReport stamped `failed` with no log line and the
 * stamp is terminal, so 21 reports were owed to one root session.
 */

const REGISTERED = ["build", "plan"]

function registry(names = REGISTERED) {
  return {
    get: (name: string) => Effect.succeed(names.includes(name) ? { name } : undefined),
    list: () => Effect.succeed(names.map((name) => ({ name }))),
  }
}

/** The resolver's logger is cached per service, so patching it here observes the module's calls. */
const warned: Array<Record<string, unknown>> = []
const agentLog = Log.create({ service: "session.agent" })
const originalWarn = agentLog.warn.bind(agentLog)
agentLog.warn = (message, extra) => {
  warned.push({ message, ...extra })
  originalWarn(message, extra)
}

beforeEach(() => {
  resetSessionAgentWarnings()
  warned.length = 0
})

describe("resolveSessionAgent", () => {
  test("returns a registered name, drops an unregistered one, and warns once per (session, agent)", async () => {
    const agents = registry()
    expect(await Effect.runPromise(resolveSessionAgent(agents, { sessionID: "ses_a", agent: "build" }))).toBe("build")
    expect(await Effect.runPromise(resolveSessionAgent(agents, { sessionID: "ses_a", agent: null }))).toBeUndefined()
    for (let i = 0; i < 3; i++) {
      expect(
        await Effect.runPromise(resolveSessionAgent(agents, { sessionID: "ses_a", agent: "claude-code" })),
      ).toBeUndefined()
    }
    expect(
      await Effect.runPromise(resolveSessionAgent(agents, { sessionID: "ses_b", agent: "claude-code" })),
    ).toBeUndefined()
    expect(warned).toEqual([
      expect.objectContaining({ sessionID: "ses_a", agent: "claude-code", registered: REGISTERED }),
      expect.objectContaining({ sessionID: "ses_b", agent: "claude-code", registered: REGISTERED }),
    ])
  })

  test("registry failures count as unregistered rather than propagating", async () => {
    const agents = {
      get: () => Effect.fail(new Error("registry unavailable")),
      list: () => Effect.fail(new Error("registry unavailable")),
    }
    expect(
      await Effect.runPromise(resolveSessionAgent(agents, { sessionID: "ses_a", agent: "claude-code" })),
    ).toBeUndefined()
    expect(warned).toEqual([expect.objectContaining({ agent: "claude-code", registered: [] })])
  })

  test("the warned set is bounded: the oldest pair is forgotten past the cap, the newest stays deduped", async () => {
    const agents = registry()
    const resolve = (sessionID: string) =>
      Effect.runPromise(resolveSessionAgent(agents, { sessionID, agent: "claude-code" }))
    for (let i = 0; i < WARNED_AGENT_PAIRS_CAP; i++) await resolve(`ses_${i}`)
    expect(warned).toHaveLength(WARNED_AGENT_PAIRS_CAP)
    // Still inside the cap: every pair is remembered.
    await resolve("ses_0")
    expect(warned).toHaveLength(WARNED_AGENT_PAIRS_CAP)
    // One past the cap evicts the oldest pair only.
    await resolve("ses_overflow")
    expect(warned).toHaveLength(WARNED_AGENT_PAIRS_CAP + 1)
    await resolve("ses_overflow")
    await resolve(`ses_${WARNED_AGENT_PAIRS_CAP - 1}`)
    expect(warned).toHaveLength(WARNED_AGENT_PAIRS_CAP + 1)
    await resolve("ses_0")
    expect(warned).toHaveLength(WARNED_AGENT_PAIRS_CAP + 2)
    expect(warned.at(-1)).toMatchObject({ sessionID: "ses_0", agent: "claude-code" })
  })
})

describe("background report delivery", () => {
  test("a parent with an unregistered agent is woken without one and the delivery is stamped delivered", async () => {
    const h = harness({ parentAgent: "claude-code" })
    await Effect.runPromise(h.deliver())
    expect(h.asyncPrompts).toHaveLength(1)
    expect(h.asyncPrompts[0]).not.toHaveProperty("agent")
    expect(h.deliveries).toEqual(["delivered"])
    expect(h.warnings()).toEqual([
      expect.objectContaining({ sessionID: "ses_parent", agent: "claude-code", registered: REGISTERED }),
    ])
  })

  test("a parent with a registered agent keeps it on the report prompt", async () => {
    const h = harness({ parentAgent: "plan" })
    await Effect.runPromise(h.deliver())
    expect(h.asyncPrompts[0]?.agent).toBe("plan")
    expect(h.deliveries).toEqual(["delivered"])
    expect(h.warnings()).toHaveLength(0)
  })

  test("a prompt failure is logged, retried once, and stamped failed only after the second failure", async () => {
    const h = harness({ promptFailures: 2 })
    const exit = await Effect.runPromiseExit(h.deliver())
    expect(Exit.isFailure(exit)).toBe(true)
    expect(h.asyncPrompts).toHaveLength(2)
    expect(h.deliveries).toEqual(["failed"])
    const errors = h.errors()
    expect(errors).toHaveLength(2)
    expect(errors[0]).toMatchObject({ attempt: 1, parentSessionID: "ses_parent", childSessionID: "ses_child" })
    expect(errors[1]).toMatchObject({ attempt: 2 })
    expect(String(errors[0]?.cause)).toContain("Agent not found")
  })

  test("a prompt that fails once and then succeeds is delivered", async () => {
    const h = harness({ promptFailures: 1 })
    await Effect.runPromise(h.deliver())
    expect(h.asyncPrompts).toHaveLength(2)
    expect(h.deliveries).toEqual(["delivered"])
    expect(h.errors()).toHaveLength(1)
  })
})

function harness(input: { parentAgent?: string; promptFailures?: number }) {
  const asyncPrompts: Array<Record<string, unknown>> = []
  const deliveries: string[] = []
  const logged: Array<{ level: string; message: unknown; data: unknown }> = []
  let failures = input.promptFailures ?? 0
  let delegation: DelegationRecord | undefined
  let claimed = false
  const deps = {
    claudeDriver: {},
    database: {},
    agents: {
      get: (name: string) => Effect.succeed(REGISTERED.includes(name) ? { name } : undefined),
      list: () => Effect.succeed(REGISTERED.map((name) => ({ name }))),
    },
    sessions: {
      get: (sessionID: string) =>
        Effect.succeed(
          sessionID === "ses_parent"
            ? { id: "ses_parent", agent: input.parentAgent, metadata: {} }
            : { id: "ses_child", metadata: { opencodex: { swarmID: "swm_1", delegation } } },
        ),
      create: () => Effect.succeed({ id: "ses_child" }),
      messages: () => Effect.succeed([]),
      messageWithChildren: () => Effect.succeed([]),
      updateMessage: (message: SessionLegacy.Info) => Effect.succeed(message),
      stampDelegation: (write: { record: DelegationRecord }) =>
        Effect.sync(() => {
          delegation = write.record
          return true
        }),
      stampDelegationDelivery: (write: { outcome: string }) =>
        Effect.sync(() => {
          deliveries.push(write.outcome)
        }),
      claimDelegationDelivery: () =>
        Effect.sync(() => {
          if (claimed) return undefined
          claimed = true
          return "claim"
        }),
      findMessage: () => Effect.succeed(Option.none()),
      updatePart: (part: Record<string, unknown>) => Effect.succeed(part),
    },
    skills: { get: () => Effect.succeed(undefined) },
    background: {
      start: (job: { run: Effect.Effect<string, unknown> }) =>
        Effect.sync(() => {
          started = job.run
          return { id: "ses_child", type: "swarm-delegate", status: "running", started_at: 0 }
        }),
    },
    prompt: () =>
      Effect.succeed({
        info: { role: "assistant", error: undefined, time: { created: 0, completed: 1 } },
        parts: [{ type: "text", text: `done\n${PromptSwarm.DELEGATION_COMPLETE_MARKER}`, synthetic: false }],
      }),
    promptAsync: (promptInput: Record<string, unknown>) =>
      Effect.suspend(() => {
        asyncPrompts.push(promptInput)
        if (failures > 0) {
          failures--
          return Effect.die(new Error('Agent not found: "claude-code"'))
        }
        return Effect.void
      }),
    loop: () => Effect.die("unused"),
    backgroundCompletionGraceMs: 0,
    deliveryRetryDelayMs: 1,
  }
  let started: Effect.Effect<string, unknown> | undefined
  const { runSwarmRole } = PromptSwarm.make(deps as unknown as PromptSwarm.Deps)
  const logger = Logger.make<unknown, void>(({ logLevel, message }) => {
    const [first, data] = Array.isArray(message) ? message : [message, {}]
    logged.push({ level: logLevel, message: first, data })
  })
  const withLogger = <A, E>(effect: Effect.Effect<A, E>) => effect.pipe(Effect.provide(Logger.layer([logger])))
  const deliver = () =>
    withLogger(
      Effect.gen(function* () {
        const result = yield* runSwarmRole({
          sessionID: SessionID.make("ses_parent"),
          swarmID: "swm_1",
          roles: [
            {
              name: "Specialist",
              agent: null,
              skill: null,
              instructions: "",
              provider_id: "anthropic",
              model_id: "claude-sonnet-5",
              variant: null,
              fallback_models: "[]",
            },
          ],
          role: "Specialist",
          prompt: "Do the task.",
          background: true,
        })
        if (!result.ok) throw new Error(`delegation rejected: ${JSON.stringify(result)}`)
        if (!started) throw new Error("background job never started")
        yield* started
      }),
    )
  return {
    asyncPrompts,
    deliveries,
    deliver,
    errors: () =>
      logged
        .filter((entry) => /error/i.test(entry.level) && entry.message === "background report delivery failed")
        .map((entry) => entry.data)
        .filter(isRecord),
    warnings: () => warned,
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}
