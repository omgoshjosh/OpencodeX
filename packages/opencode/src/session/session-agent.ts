import { Effect } from "effect"
import * as Log from "@opencode-ai/core/util/log"

const log = Log.create({ service: "session.agent" })

/** The slice of Agent.Service a forwarding site needs to vet a stored agent name. */
export interface AgentRegistry {
  readonly get: (agent: string) => Effect.Effect<{ name: string } | undefined, unknown>
  readonly list: () => Effect.Effect<ReadonlyArray<{ name: string }>, unknown>
}

/**
 * One warning per (sessionID, agent): a stranded root logs once, not per
 * report. Bounded so the set of pairs a long-lived daemon has seen cannot grow
 * without limit; once full, the oldest pair is forgotten (and may warn again),
 * which is a bounded repeat rather than a flood.
 */
export const WARNED_AGENT_PAIRS_CAP = 256
const warned = new Set<string>()

function rememberWarned(key: string) {
  if (warned.has(key)) return false
  if (warned.size >= WARNED_AGENT_PAIRS_CAP) {
    const oldest = warned.values().next().value
    if (oldest !== undefined) warned.delete(oldest)
  }
  warned.add(key)
  return true
}

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
  if (rememberWarned(`${input.sessionID} ${input.agent}`)) {
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
