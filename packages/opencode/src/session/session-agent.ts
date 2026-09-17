import { Effect } from "effect"
import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "session.agent" })

/** The slice of Agent.Service a forwarding site needs to vet a stored agent name. */
export interface AgentRegistry {
  readonly get: (agent: string) => Effect.Effect<{ name: string } | undefined, unknown>
  readonly list: () => Effect.Effect<ReadonlyArray<{ name: string }>, unknown>
}

/** One warning per (sessionID, agent): a stranded root logs once, not per report. */
const warned = new Set<string>()

/**
 * A session or swarm role row can carry an `agent` that no registry knows
 * (`claude-code`, written through POST/PATCH /session before those routes
 * validated it). Forwarding such a name onto a prompt makes createUserMessage
 * throw "Agent not found", which turned every background report, question
 * notification, and retry for those sessions into a silent failure
 * (OpencodeX-557). This returns the name only when the registry resolves it;
 * callers omit `agent` otherwise so the prompt falls back to the default agent.
 */
export const resolveSessionAgent = Effect.fnUntraced(function* (
  agents: AgentRegistry,
  input: { sessionID: string; agent: string | null | undefined },
) {
  if (!input.agent) return undefined
  const found = yield* agents.get(input.agent).pipe(Effect.orElseSucceed(() => undefined))
  if (found) return found.name
  const key = `${input.sessionID} ${input.agent}`
  if (!warned.has(key)) {
    warned.add(key)
    const registered = yield* agents.list().pipe(
      Effect.map((list) => list.map((agent) => agent.name)),
      Effect.orElseSucceed((): string[] => []),
    )
    log.warn("session agent not registered; prompting with the default agent", {
      sessionID: input.sessionID,
      agent: input.agent,
      registered,
    })
  }
  return undefined
})

/** Test seam: forget which (session, agent) pairs have already been warned about. */
export function resetSessionAgentWarnings() {
  warned.clear()
}
