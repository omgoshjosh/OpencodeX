type Definition = {
  [method: string]: (input: any) => any
}

export function listen(rpc: Definition) {
  onmessage = async (evt) => {
    const parsed = JSON.parse(evt.data)
    if (parsed.type === "rpc.request") {
      const id = parsed.id
      try {
        const result = await rpc[parsed.method](parsed.input)
        postMessage(JSON.stringify({ type: "rpc.result", result, id }))
      } catch (error) {
        // A rejected RPC handler must reach the caller: without this the
        // requesting side's `call` never resolves and hangs forever.
        const message = error instanceof Error ? error.message : String(error)
        postMessage(JSON.stringify({ type: "rpc.error", error: message, id }))
      }
    }
  }
}

export function emit(event: string, data: unknown) {
  postMessage(JSON.stringify({ type: "rpc.event", event, data }))
}

export function client<T extends Definition>(target: {
  postMessage: (data: string) => void | null
  onmessage: ((this: Worker, ev: MessageEvent) => any) | null
}) {
  const pending = new Map<number, { resolve: (result: any) => void; reject: (error: Error) => void }>()
  const listeners = new Map<string, Set<(data: any) => void>>()
  let id = 0
  target.onmessage = async (evt) => {
    const parsed = JSON.parse(evt.data)
    if (parsed.type === "rpc.result") {
      const entry = pending.get(parsed.id)
      if (entry) {
        entry.resolve(parsed.result)
        pending.delete(parsed.id)
      }
    }
    if (parsed.type === "rpc.error") {
      const entry = pending.get(parsed.id)
      if (entry) {
        entry.reject(new Error(String(parsed.error)))
        pending.delete(parsed.id)
      }
    }
    if (parsed.type === "rpc.event") {
      const handlers = listeners.get(parsed.event)
      if (handlers) {
        for (const handler of handlers) {
          handler(parsed.data)
        }
      }
    }
  }
  let failure: Error | undefined
  return {
    call<Method extends keyof T>(method: Method, input: Parameters<T[Method]>[0]): Promise<ReturnType<T[Method]>> {
      if (failure) return Promise.reject(failure)
      const requestId = id++
      return new Promise((resolve, reject) => {
        pending.set(requestId, { resolve, reject })
        target.postMessage(JSON.stringify({ type: "rpc.request", method, input, id: requestId }))
      })
    },
    /**
     * Marks the peer dead: rejects every pending call and every future one.
     * The rpc.error path only covers a handler that rejects - a worker that
     * throws during module init (before Rpc.listen installs onmessage) or is
     * terminated answers nothing, and an un-timed call would hang forever
     * with the TUI silent. The worker's error/exit hooks call this.
     */
    fail(error: Error) {
      if (failure) return
      failure = error
      const entries = [...pending.values()]
      pending.clear()
      for (const entry of entries) entry.reject(error)
    },
    on<Data>(event: string, handler: (data: Data) => void) {
      let handlers = listeners.get(event)
      if (!handlers) {
        handlers = new Set()
        listeners.set(event, handlers)
      }
      handlers.add(handler)
      return () => {
        handlers.delete(handler)
      }
    },
  }
}

export * as Rpc from "./rpc"
