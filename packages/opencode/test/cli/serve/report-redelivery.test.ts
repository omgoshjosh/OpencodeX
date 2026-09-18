// Subprocess e2e for OpencodeX-557: a background delegation whose report
// delivery was stamped `failed` because the parent's stored `agent` is not a
// registered name ("claude-code", written through PATCH /session before that
// route validated it) is redelivered by the next daemon start. Pre-fix the
// report prompt threw "Agent not found" on every attempt, so `failed` was
// terminal and the parent never heard back.
//
// The stranded state is written straight into the database between two
// `opencode serve` runs, the same way it reached production: no supported
// route writes an unregistered agent any more.
import { describe, expect } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import path from "node:path"
import { cliIt } from "../../lib/cli-process"
import { DELEGATION_RECORD_VERSION } from "../../../src/session/delegation-outcome"

const Session = Schema.Struct({ id: Schema.String, directory: Schema.String })
const Messages = Schema.Array(
  Schema.Struct({
    info: Schema.Struct({
      id: Schema.String,
      role: Schema.String,
      time: Schema.Struct({ completed: Schema.optional(Schema.Number) }),
    }),
    parts: Schema.Array(Schema.Record(Schema.String, Schema.Any)),
  }),
)
const Delegation = Schema.Struct({
  metadata: Schema.Struct({
    opencodex: Schema.Struct({ delegation: Schema.Struct({ deliveryOutcome: Schema.optional(Schema.String) }) }),
  }),
})

const RUN_ID = "run_e2e_557"
const REPORT_MESSAGE_ID = `msg_delegation_recovery_${RUN_ID}`
const CHILD_REPORT = "e2e child report: the work is done"

function request(url: string, directory: string, path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers)
  headers.set("x-opencode-directory", directory)
  return fetch(new URL(path, url), { ...init, headers })
}

async function messages(url: string, directory: string, sessionID: string) {
  const response = await request(url, directory, `/session/${sessionID}/message`)
  if (response.status !== 200) throw new Error(`messages: HTTP ${response.status} ${await response.text()}`)
  return Schema.decodeUnknownSync(Messages)(await response.json())
}

async function poll<T>(label: string, read: () => Promise<T | undefined>, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = await read()
    if (value !== undefined) return value
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
  throw new Error(`${label} did not happen within ${timeoutMs}ms`)
}

describe("background report redelivery across restart (subprocess)", () => {
  cliIt.live(
    "redelivers a report stamped failed under an unregistered parent agent",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.text(CHILD_REPORT)
        const dbPath = path.join(home, "redelivery.db")
        const env = { OPENCODE_DB: dbPath }
        const first = yield* opencode.serve({ env })
        const state = yield* Effect.tryPromise(async () => {
          const create = async (body: Record<string, unknown>) => {
            const created = await request(first.url, home, "/session", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(body),
            })
            expect(created.status).toBe(200)
            return Schema.decodeUnknownSync(Session)(await created.json())
          }
          const parent = await create({ title: "stranded orchestrator" })
          const child = await create({ title: "delegate", parentID: parent.id })
          // A real child turn, so the recovered report is read off its transcript.
          const prompt = await request(first.url, home, `/session/${child.id}/prompt_async`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              messageID: "msg_e2e_557_child_turn",
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              parts: [{ type: "text", text: "do the work" }],
            }),
          })
          expect(prompt.status).toBe(204)
          await poll("child turn", async () =>
            (await messages(first.url, home, child.id)).find(
              (message) => message.info.role === "assistant" && message.info.time.completed !== undefined,
            ),
          )
          return { parentID: parent.id, childID: child.id, directory: parent.directory }
        })
        first.kill()
        yield* Effect.promise(() => first.exited)

        // The state the daemon left behind pre-fix: an unregistered parent
        // agent and a settled background record whose delivery already failed.
        yield* Effect.sync(() => {
          const db = new Database(dbPath)
          try {
            db.run(`update session set agent = ? where id = ?`, ["claude-code", state.parentID])
            const now = Date.now()
            db.run(`update session set metadata = ? where id = ?`, [
              JSON.stringify({
                opencodex: {
                  delegation: {
                    version: DELEGATION_RECORD_VERSION,
                    runID: RUN_ID,
                    parentSessionID: state.parentID,
                    attempt: 1,
                    phase: "settled",
                    outcome: "completed",
                    startedAt: now - 2_000,
                    completedAt: now - 1_000,
                    mode: "background",
                    background: true,
                    role: "delegate",
                    ownerID: "local:999999:dead:run_e2e_557",
                    deliveryOutcome: "failed",
                  },
                },
              }),
              state.childID,
            ])
          } finally {
            db.close()
          }
        })

        const second = yield* opencode.serve({ env })
        yield* Effect.tryPromise(async () => {
          const report = await poll("report redelivery", async () =>
            (await messages(second.url, state.directory, state.parentID)).find(
              (message) => message.info.id === REPORT_MESSAGE_ID,
            ),
          )
          expect(report.info.role).toBe("user")
          const part = report.parts.find((part) => part.type === "text")
          expect(part).toMatchObject({ synthetic: true, metadata: { task_report: true } })
          expect(String(part?.text)).toContain(CHILD_REPORT)
          expect(String(part?.text)).toContain(state.childID)

          const delivered = await poll("delivered stamp", async () => {
            const response = await request(second.url, state.directory, `/session/${state.childID}`)
            if (response.status !== 200) return undefined
            const outcome = Schema.decodeUnknownSync(Delegation)(await response.json()).metadata.opencodex.delegation
              .deliveryOutcome
            return outcome === "delivered" ? outcome : undefined
          })
          expect(delivered).toBe("delivered")
        })
      }),
    180_000,
  )
})
