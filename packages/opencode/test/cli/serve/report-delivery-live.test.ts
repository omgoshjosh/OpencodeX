// Subprocess e2e for OpencodeX-557, in-process half: a background delegation
// finishes while its parent's stored `agent` is not a registered name. The
// report has to reach the parent anyway - exactly once - and wake it for its
// follow-up turn; the child's record has to say `delivered`. Pre-fix the
// forwarded name made the report prompt throw "Agent not found", the record
// was stamped `failed` (terminal for the in-process path), and the parent
// never heard back. The restart half of the same defect is covered by
// report-redelivery.test.ts.
//
// The delegation is driven through the native task tool: it is the background
// path the test LLM can reach end to end (a swarm role runs behind the local
// Claude driver). The stranded column is written straight into the database
// while the daemon runs, the way it reached production - the supported route
// refuses it now, which the test asserts first.
import { describe, expect } from "bun:test"
import { Database } from "bun:sqlite"
import { Effect, Schema } from "effect"
import path from "node:path"
import { cliIt } from "../../lib/cli-process"

const Session = Schema.Struct({ id: Schema.String, directory: Schema.String })
const Messages = Schema.Array(
  Schema.Struct({
    info: Schema.Struct({
      id: Schema.String,
      role: Schema.String,
      parentID: Schema.optional(Schema.String),
      time: Schema.Struct({ completed: Schema.optional(Schema.Number) }),
    }),
    parts: Schema.Array(
      Schema.Struct({
        type: Schema.String,
        text: Schema.optional(Schema.String),
        synthetic: Schema.optional(Schema.Boolean),
        metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
      }),
    ),
  }),
)
const Delegation = Schema.Struct({
  metadata: Schema.Struct({
    opencodex: Schema.Struct({ delegation: Schema.Struct({ deliveryOutcome: Schema.optional(Schema.String) }) }),
  }),
})

const PARENT_PROMPT = "e2e 557: delegate the work in the background"
const CHILD_PROMPT = "e2e 557: do the child work"
const CHILD_REPORT = "e2e 557 child report: the work is done"

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

async function deliveryOutcome(url: string, directory: string, sessionID: string) {
  const response = await request(url, directory, `/session/${sessionID}`)
  if (response.status !== 200) return undefined
  return Schema.decodeUnknownSync(Delegation)(await response.json()).metadata.opencodex.delegation.deliveryOutcome
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

const isReport = (message: (typeof Messages)["Type"][number]) =>
  message.info.role === "user" &&
  message.parts.some((part) => part.type === "text" && part.synthetic === true && part.metadata?.task_report === true)

describe("background report delivery under an unregistered parent agent (subprocess)", () => {
  cliIt.live(
    "delivers the report once, wakes the parent, and stamps the child delivered",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        // The request bodies: the parent's opening turn carries its prompt and
        // no tool call yet; the child's turn carries the delegated prompt and
        // no tool call. Everything else (the parent's continuation after the
        // tool returns, its follow-up on the report, titles) takes the
        // server's automatic "ok".
        const body = (hit: { body: unknown }) => JSON.stringify(hit.body)
        yield* llm.toolMatch((hit) => body(hit).includes(PARENT_PROMPT) && !body(hit).includes("tool_calls"), "task", {
          description: "e2e delegate",
          prompt: CHILD_PROMPT,
          subagent_type: "general",
          background: true,
        })
        yield* llm.textMatch(
          (hit) => body(hit).includes(CHILD_PROMPT) && !body(hit).includes("tool_calls"),
          CHILD_REPORT,
        )
        const dbPath = path.join(home, "delivery.db")
        const serve = yield* opencode.serve({ env: { OPENCODE_DB: dbPath } })
        yield* Effect.tryPromise(async () => {
          const created = await request(serve.url, home, "/session", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              title: "orchestrator",
              permission: [{ permission: "*", pattern: "*", action: "allow" }],
            }),
          })
          expect(created.status).toBe(200)
          const parent = Schema.decodeUnknownSync(Session)(await created.json())

          // The supported route refuses the name the production roots carried...
          const patched = await request(serve.url, parent.directory, `/session/${parent.id}`, {
            method: "PATCH",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ agent: "claude-code" }),
          })
          expect(patched.status).toBe(400)
          // ...so it is written the way it reached them, under the live daemon.
          const db = new Database(dbPath)
          try {
            db.run(`update session set agent = ? where id = ?`, ["claude-code", parent.id])
          } finally {
            db.close()
          }

          const prompt = await request(serve.url, parent.directory, `/session/${parent.id}/prompt_async`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              messageID: "msg_e2e_557_parent_turn",
              agent: "build",
              model: { providerID: "test", modelID: "test-model" },
              parts: [{ type: "text", text: PARENT_PROMPT }],
            }),
          })
          expect(prompt.status).toBe(204)

          const report = await poll("task report", async () =>
            (await messages(serve.url, parent.directory, parent.id)).find(isReport),
          )
          const text = report.parts.find((part) => part.type === "text")?.text ?? ""
          expect(text).toContain(CHILD_REPORT)
          const childID = /<task id="([^"]+)"/.exec(text)?.[1]
          expect(childID).toBeDefined()

          // The parent's follow-up turn on the report.
          await poll("parent follow-up turn", async () =>
            (await messages(serve.url, parent.directory, parent.id)).find(
              (message) =>
                message.info.role === "assistant" &&
                message.info.parentID === report.info.id &&
                message.info.time.completed !== undefined,
            ),
          )
          const delivered = await poll("delivered stamp", async () => {
            const outcome = await deliveryOutcome(serve.url, parent.directory, childID!)
            return outcome === "delivered" ? outcome : undefined
          })
          expect(delivered).toBe("delivered")
          // Exactly once: the follow-up turn finished with nothing else queued.
          expect((await messages(serve.url, parent.directory, parent.id)).filter(isReport)).toHaveLength(1)
        })
      }),
    180_000,
  )
})
