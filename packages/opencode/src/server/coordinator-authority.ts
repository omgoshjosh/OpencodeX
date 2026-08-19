import { OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"

type Waiter = () => void

const state = {
  authorityEpoch: undefined as string | undefined,
  admission: true,
  ready: true,
  inflight: 0,
  waiters: new Set<Waiter>(),
  transition: Promise.resolve() as Promise<unknown>,
}

export function enabled() {
  return process.env[OPENCODE_PROCESS_ROLE] === "coordinator" && state.authorityEpoch !== undefined
}

export function health() {
  const authorityEpoch = state.authorityEpoch
  return authorityEpoch && enabled() ? { authorityEpoch, admission: state.admission, ready: state.ready } : undefined
}

export function initialize(authorityEpoch?: string) {
  state.authorityEpoch = authorityEpoch
  state.admission = true
  state.ready = true
  delete process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH
}

export function epoch() {
  return state.authorityEpoch
}

export function acquire(url: string) {
  if (!enabled()) return () => {}
  const pathname = new URL(url, "http://localhost").pathname
  if (
    pathname === "/global/health" ||
    pathname === "/global/event" ||
    pathname === "/event" ||
    pathname === "/global/authority-handoff"
  )
    return () => {}
  if (!state.admission) return undefined
  state.inflight += 1
  let released = false
  return () => {
    if (released) return
    released = true
    state.inflight -= 1
    if (state.inflight !== 0) return
    const waiters = [...state.waiters]
    state.waiters.clear()
    waiters.forEach((resolve) => resolve())
  }
}

export function close() {
  state.admission = false
  state.ready = false
  return drained()
}

export function reopen() {
  state.admission = true
  state.ready = true
}

export function drained() {
  if (state.inflight === 0) return Promise.resolve()
  return new Promise<void>((resolve) => state.waiters.add(resolve))
}

export function serialized<A>(transition: () => Promise<A>) {
  const result = state.transition.then(transition, transition)
  state.transition = result.catch(() => undefined)
  return result
}

export function resetForTest() {
  state.authorityEpoch = undefined
  state.admission = true
  state.ready = true
  state.inflight = 0
  state.waiters.clear()
  state.transition = Promise.resolve()
}

export * as CoordinatorAuthority from "./coordinator-authority"
