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
    opencodex: Schema.Struct({
      delegation: Schema.Struct({ runID: Schema.String, deliveryOutcome: Schema.optional(Schema.String) }),
    }),
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

async function delegation(url: string, directory: string, sessionID: string) {
  const response = await request(url, directory, `/session/${sessionID}`)
  if (response.status !== 200) return undefined
  return Schema.decodeUnknownSync(Delegation)(await response.json()).metadata.opencodex.delegation
}

async function deliveryOutcome(url: string, directory: string, sessionID: string) {
  return (await delegation(url, directory, sessionID))?.deliveryOutcome
}

async function createParent(url: string, home: string) {
  const created = await request(url, home, "/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      title: "orchestrator",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    }),
  })
  expect(created.status).toBe(200)
  return Schema.decodeUnknownSync(Session)(await created.json())
}

async function promptParent(url: string, parent: { id: string; directory: string }) {
  const prompt = await request(url, parent.directory, `/session/${parent.id}/prompt_async`, {
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

// The request bodies: the parent's opening turn carries its prompt and no
// tool call yet; the child's turn carries the delegated prompt and no tool
// call. Everything else (the parent's continuation after the tool returns, its
// follow-up on the report, titles) takes the server's automatic "ok".
const body = (hit: { body: unknown }) => JSON.stringify(hit.body)
const parentTurn = (hit: { body: unknown }) => body(hit).includes(PARENT_PROMPT) && !body(hit).includes("tool_calls")
const childTurn = (hit: { body: unknown }) => body(hit).includes(CHILD_PROMPT) && !body(hit).includes("tool_calls")

const isReport = (message: (typeof Messages)["Type"][number]) =>
  message.info.role === "user" &&
  message.parts.some((part) => part.type === "text" && part.synthetic === true && part.metadata?.task_report === true)

describe("background report delivery under an unregistered parent agent (subprocess)", () => {
  cliIt.live(
    "delivers the report once, wakes the parent, and stamps the child delivered",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.toolMatch(parentTurn, "task", {
          description: "e2e delegate",
          prompt: CHILD_PROMPT,
          subagent_type: "general",
          background: true,
        })
        yield* llm.textMatch(childTurn, CHILD_REPORT)
        const dbPath = path.join(home, "delivery.db")
        const serve = yield* opencode.serve({
          env: { OPENCODE_DB: dbPath, OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS: "true" },
        })
        yield* Effect.tryPromise(async () => {
          const parent = await createParent(serve.url, home)

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

          await promptParent(serve.url, parent)

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

  // A delivery that does fail has to leave a greppable trace: the daemon log
  // line must carry the run and both session ids as fields. Passing the
  // payload as a log argument rendered it `[object Object]` (OpencodeX-2kg),
  // which is unsearchable by runID. The failure is injected at the database:
  // a trigger refuses the report's session_command row, so the prompt's
  // durable write dies inside the delivery handler and nowhere else.
  cliIt.live(
    "logs a failed delivery with runID and cause as fields, not [object Object]",
    ({ home, llm, opencode }) =>
      Effect.gen(function* () {
        yield* llm.toolMatch(parentTurn, "task", {
          description: "e2e delegate",
          prompt: CHILD_PROMPT,
          subagent_type: "general",
          background: true,
        })
        yield* llm.textMatch(childTurn, CHILD_REPORT)
        const dbPath = path.join(home, "delivery-failed.db")
        const serve = yield* opencode.serve({
          env: { OPENCODE_DB: dbPath, OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS: "true" },
          extraArgs: ["--print-logs"],
        })
        yield* Effect.tryPromise(async () => {
          const parent = await createParent(serve.url, home)
          const db = new Database(dbPath)
          try {
            db.run(
              `create trigger e2e_557_refuse_report before insert on session_command
                 when new.message_id like 'msg_task_report_%'
                 begin select raise(abort, 'e2e 557: report refused'); end`,
            )
            await promptParent(serve.url, parent)
            const childID = await poll("child session", async () => {
              const row = db.query(`select id, directory from session where parent_id = ?`).get(parent.id)
              return row === null ? undefined : Schema.decodeUnknownSync(Session)(row).id
            })
            const failed = await poll("failed stamp", async () => {
              const outcome = await deliveryOutcome(serve.url, parent.directory, childID)
              return outcome === "failed" ? outcome : undefined
            })
            expect(failed).toBe("failed")
            const runID = (await delegation(serve.url, parent.directory, childID))?.runID
            expect(runID).toBeDefined()

            // One log entry: fields render before the message, and a pretty
            // cause spans lines, so the entry runs from its level marker
            // through the message.
            const line = await poll("delivery failure log entry", async () => {
              const text = serve.stderr()
              const end = text.indexOf("background report delivery failed")
              if (end === -1) return undefined
              const start = text.lastIndexOf("\nERROR ", end)
              return text.slice(start === -1 ? 0 : start + 1, text.indexOf("\n", end))
            })
            expect(line).not.toContain("[object Object]")
            expect(line).toContain(`runID=${runID}`)
            expect(line).toContain(`childSessionID=${childID}`)
            expect(line).toContain(`parentSessionID=${parent.id}`)
            expect(line).toContain("cause=")
            expect(line).toContain("e2e 557: report refused")
          } finally {
            db.close()
          }
        })
      }),
    180_000,
  )
})
