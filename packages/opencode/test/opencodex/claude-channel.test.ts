import { describe, expect, spyOn, test } from "bun:test"
import type { SDKUserMessage } from "@anthropic-ai/claude-agent-sdk"
import { create } from "@opencode-ai/core/util/log"
import { Channel, createChannelRegistry, createPushable, type CreateQuery } from "../../src/opencodex/claude-channel"
import type { ClaudeEvent } from "../../src/opencodex/claude-mapper"

type Handlers = { name: string }

/** A scriptable stand-in for the SDK query a channel wraps. */
function fakeQuery() {
  const stream = createPushable<ClaudeEvent>()
  const state = {
    prompts: [] as SDKUserMessage[],
    resumeID: undefined as string | undefined,
    interrupts: 0,
    aborted: false,
    aborts: 0,
    promptEnded: false,
    handlers: undefined as (() => Handlers | undefined) | undefined,
  }
  const create: CreateQuery<Handlers> = async (input) => {
    state.resumeID = input.resumeID
    state.handlers = input.handlers
    void (async () => {
      for await (const message of input.prompt) state.prompts.push(message)
      state.promptEnded = true
    })()
    return {
      events: stream.iterable,
      interrupt: async () => {
        state.interrupts += 1
      },
      abort: () => {
        state.aborted = true
        state.aborts += 1
        stream.end()
      },
    }
  }
  return { create, emit: stream.push.bind(stream), endStream: stream.end.bind(stream), state }
}

function user(text: string): SDKUserMessage {
  return { type: "user", message: { role: "user", content: text }, parent_tool_use_id: null } as SDKUserMessage
}

/** Test fixtures cross the same untyped stream-json boundary the SDK does. */
function event(value: Record<string, unknown>): ClaudeEvent {
  return value as unknown as ClaudeEvent
}

function typeOf(value: ClaudeEvent) {
  return (value as unknown as { type?: string }).type
}

const assistant = event({ type: "assistant", message: { content: [] } })
const result = event({ type: "result", subtype: "success" })
const init = event({ type: "system", subtype: "init", session_id: "conv-1" })

async function collect(events: AsyncIterable<ClaudeEvent>, count: number) {
  const seen: ClaudeEvent[] = []
  for await (const event of events) {
    seen.push(event)
    if (seen.length >= count) break
  }
  return seen
}

async function waitFor(predicate: () => boolean, timeoutMs = 1_000) {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("condition was never met")
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

async function settled() {
  await new Promise((resolve) => setTimeout(resolve, 10))
}

describe("claude channel", () => {
  test("drops the post-resume close-out result instead of ending the live turn", async () => {
    // The live capture (2026-08-22 16:35 UTC): a resume spawn emitted a stale
    // result +1s after start, before the new turn produced anything; the
    // daemon read it as the turn's completion and orphaned the real turn.
    const { create, emit, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { resumeID: "conv-1" })
    const turn = channel.turn([user("wake")], { name: "t1" })

    await settled()
    expect(state.resumeID).toBe("conv-1")
    emit(init) // the resume was accepted
    emit(result) // stale close-out of the dangling previous turn
    emit(assistant)
    emit(result) // the real completion

    const seen = await collect(turn.events, 3)
    expect(seen.map(typeOf)).toEqual(["system", "assistant", "result"])
  })

  test("forwards a pre-init result so a rejected resume still reaches the mapper", async () => {
    // A resume the CLI refuses (or an auth failure) reports itself as a failed
    // result with no system.init; the mapper's resumeRejected/needs-login
    // recovery depends on seeing it. Only post-init results can be stale.
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { resumeID: "conv-gone" })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "result", subtype: "error_during_execution", is_error: true }))
    const seen = await collect(turn.events, 1)
    expect(typeOf(seen[0])).toBe("result")
  })

  test("forwards an error result that lands in the close-out window", async () => {
    // A rate limit, usage limit, upstream 5xx, or invalid model is reported as
    // an error result after system.init and before any assistant output -
    // exactly where the close-out guard looks. Dropping one stalls the turn
    // forever, because the streaming input keeps the child alive so the query
    // stream never ends.
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { resumeID: "conv-1" })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(init)
    emit(event({ type: "result", subtype: "error_during_execution", is_error: true }))
    const seen = await collect(turn.events, 2)
    expect(seen.map(typeOf)).toEqual(["system", "result"])
  })

  test("forwards an is_error result even when its subtype reads as success", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { resumeID: "conv-1" })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(init)
    emit(event({ type: "result", subtype: "success", is_error: true }))
    const seen = await collect(turn.events, 2)
    expect(seen.map(typeOf)).toEqual(["system", "result"])
  })

  test("fails the turn when a dropped close-out is followed by nothing", async () => {
    // The guard's blind spot must be bounded: without this the driver parks in
    // nextClaudeEvent holding the session's execution lease indefinitely.
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { resumeID: "conv-1", closeOutGraceMs: 20 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(init)
    emit(result)

    // The init is forwarded; the dropped close-out leaves nothing after it.
    const outcome = await collect(turn.events, 2).then(
      () => "completed",
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    )
    expect(outcome).toContain("no output")
    // The whole channel goes down with the turn: the child may still be alive
    // and mid-answer, and detaching alone would let that output land in the
    // next turn's sink - the previous prompt's answer on the new prompt.
    await waitFor(() => channel.dead)
    expect(channel.dead).toBe(true)
  })

  test("keeps a live background task beyond the close-out grace", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 20, backgroundTaskGraceMs: 100 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(channel.dead).toBe(false)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(assistant)
    emit(result)

    const seen = await collect(turn.events, 5)
    expect(seen.map(typeOf)).toEqual(["system", "result", "system", "assistant", "result"])
  })

  test("fails when background tasks settle without final output", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 20, backgroundTaskGraceMs: 100 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))

    const outcome = await collect(turn.events, 4).then(
      () => "completed",
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    )
    expect(outcome).toContain("no final output")
    await waitFor(() => channel.dead)
  })

  test("refreshes final-output inactivity after background tasks settle", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 80, backgroundTaskGraceMs: 300 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    await new Promise((resolve) => setTimeout(resolve, 50))
    emit(assistant)
    await new Promise((resolve) => setTimeout(resolve, 50))
    emit(event({ type: "stream_event", event: { type: "content_block_delta" } }))
    await new Promise((resolve) => setTimeout(resolve, 50))
    emit(result)

    const seen = await collect(turn.events, 6)
    expect(seen.map(typeOf)).toEqual(["system", "result", "system", "assistant", "stream_event", "result"])
    expect(channel.dead).toBe(false)
  })

  test("returns to background drain when tasks reappear after settling", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 20, backgroundTaskGraceMs: 120 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "b1" }] }))
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(channel.dead).toBe(false)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(assistant)
    emit(result)

    const seen = await collect(turn.events, 7)
    expect(seen.map(typeOf)).toEqual(["system", "result", "system", "system", "system", "assistant", "result"])
  })

  test("does not extend the drain cap across task-list flaps", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 100, backgroundTaskGraceMs: 50 })
    channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    await new Promise((resolve) => setTimeout(resolve, 30))
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "b1" }] }))

    await waitFor(() => channel.dead)
  })

  test("does not grant reappearing tasks a new grace after the drain deadline passes", async () => {
    const { create, emit } = fakeQuery()
    let now = 0
    const channel = new Channel<Handlers>("s1", create, {
      closeOutGraceMs: 100,
      backgroundTaskGraceMs: 20,
      now: () => now,
    })
    channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    // The original drain timer has fired while final output was pending.
    await new Promise((resolve) => setTimeout(resolve, 30))
    now = 21
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "b1" }] }))

    await waitFor(() => channel.dead)
  })

  test("fails synchronously when expired background work reappears", async () => {
    const { create, emit, state } = fakeQuery()
    let now = 0
    const channel = new Channel<Handlers>("s1", create, {
      closeOutGraceMs: 100,
      backgroundTaskGraceMs: 20,
      now: () => now,
    })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    await settled()
    now = 21
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "b1" }] }))
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(assistant)
    emit(result)

    const outcome = await collect(turn.events, 4).then(
      () => "completed",
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    )
    expect(outcome).toContain("background tasks did not settle")
    await waitFor(() => state.aborts === 1)
    expect(channel.dead).toBe(true)
  })

  test("allows final output after the background deadline when no tasks reappear", async () => {
    const { create, emit } = fakeQuery()
    let now = 0
    const channel = new Channel<Handlers>("s1", create, {
      closeOutGraceMs: 100,
      backgroundTaskGraceMs: 20,
      now: () => now,
    })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(result)
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    now = 21
    emit(assistant)
    emit(result)

    const seen = await collect(turn.events, 5)
    expect(seen.map(typeOf)).toEqual(["system", "result", "system", "assistant", "result"])
  })

  test("completes normally when background tasks already drained", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 20, backgroundTaskGraceMs: 100 })
    const turn = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [] }))
    emit(result)

    const seen = await collect(turn.events, 2)
    expect(seen.map(typeOf)).toEqual(["system", "result"])
  })

  test("does not carry background state into a turn after a failed result", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { closeOutGraceMs: 20, backgroundTaskGraceMs: 30 })
    const first = channel.turn([user("wake")], { name: "t1" })
    emit(event({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "a1" }] }))
    emit(event({ type: "result", subtype: "error_during_execution", is_error: true }))
    await collect(first.events, 2)

    const second = channel.turn([user("retry")], { name: "t2" })
    emit(result)
    await collect(second.events, 1)
    await new Promise((resolve) => setTimeout(resolve, 40))
    expect(channel.dead).toBe(false)
  })

  test("forwards results normally on a channel that did not resume", async () => {
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const turn = channel.turn([user("hi")], { name: "t1" })
    emit(result)
    const seen = await collect(turn.events, 1)
    expect(typeOf(seen[0])).toBe("result")
  })

  test("drops every event type that arrives between turns", async () => {
    const query = fakeQuery()
    const channel = new Channel<Handlers>("s1", query.create)
    const dropped = spyOn(create({ service: "claude-channel" }), "info")
    const first = channel.turn([user("one")], { name: "t1" })
    query.emit(assistant)
    query.emit(result)
    await collect(first.events, 2)

    // Nobody is consuming: background chatter and stray results must not leak
    // into the next turn.
    query.emit(result)
    query.emit(event({ type: "user", message: { content: [{ type: "tool_result", tool_use_id: "call_1", content: "secret" }] } }))
    query.emit(event({ type: "assistant", message: { content: [{ type: "text", text: "secret" }] } }))
    await settled()

    const calls = dropped.mock.calls.filter(([message]) => message === "dropped out-of-turn event")
    expect(calls.map(([, fields]) => fields)).toEqual([
      { channel: "s1", type: "result" },
      { channel: "s1", type: "user" },
      { channel: "s1", type: "assistant" },
    ])
    expect(JSON.stringify(calls)).not.toContain("secret")
    dropped.mockRestore()

    const second = channel.turn([user("two")], { name: "t2" })
    query.emit(assistant)
    const seen = await collect(second.events, 1)
    expect(typeOf(seen[0])).toBe("assistant")
  })

  test("runs consecutive turns over one query and swaps handlers per turn", async () => {
    const { create, emit, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)

    const first = channel.turn([user("one")], { name: "t1" })
    await settled()
    expect(state.handlers?.()).toEqual({ name: "t1" })
    emit(assistant)
    emit(result)
    await collect(first.events, 2)
    // Between turns there is no handler to answer control requests.
    expect(state.handlers?.()).toBeUndefined()

    const second = channel.turn([user("two")], { name: "t2" })
    await settled()
    expect(state.handlers?.()).toEqual({ name: "t2" })
    emit(assistant)
    await collect(second.events, 1)
    expect(state.prompts.map((message) => message.message.content)).toEqual(["one", "two"])
  })

  test("offers follow-ups FIFO into an attached turn without replacing its handlers or sink", async () => {
    const { create, emit, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const turn = channel.turn([user("one")], { name: "t1" })

    expect(channel.offer([user("two"), user("three")])).toBe(true)
    await settled()
    expect(state.prompts.map((message) => message.message.content)).toEqual(["one", "two", "three"])
    expect(state.handlers?.()).toEqual({ name: "t1" })
    expect(state.interrupts).toBe(0)

    emit(assistant)
    const iterator = turn.events[Symbol.asyncIterator]()
    expect(typeOf((await iterator.next()).value)).toBe("assistant")
    expect(state.handlers?.()).toEqual({ name: "t1" })
    await iterator.return?.(undefined)
  })

  test("rejects offers when the channel is idle, dead, or retiring", async () => {
    const { create, endStream } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    expect(channel.offer([user("idle")])).toBe(false)

    channel.turn([user("image")], { name: "image" }, { closeInput: true })
    expect(channel.offer([user("retiring")])).toBe(false)

    endStream()
    await waitFor(() => channel.dead)
    expect(channel.offer([user("dead")])).toBe(false)
  })

  test("an image turn without closeInput keeps the channel open for tool approvals", async () => {
    // Regression for the 2026-08-28 capture: closing input after an image
    // message (the CLI 2.1.228 EOF workaround) also closed stdin, which is the
    // canUseTool response path - every approval-gated tool call in the turn
    // died with "Stream closed". The transport no longer passes closeInput;
    // this pins that an image turn leaves the input open and handlers attached.
    const { create, state, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const message = {
      type: "user",
      message: {
        role: "user",
        content: [
          { type: "text", text: "describe it" },
          { type: "image", source: { type: "base64", media_type: "image/png", data: "aGVsbG8=" } },
        ],
      },
      parent_tool_use_id: null,
    } as SDKUserMessage

    const turn = channel.turn([message], { name: "image" })
    await settled()
    expect(state.promptEnded).toBe(false)
    expect(channel.retiring).toBe(false)
    // Mid-turn, the SDK can still resolve approvals through the turn's handlers.
    expect(state.handlers?.()).toEqual({ name: "image" })
    emit(assistant)
    await collect(turn.events, 1)
  })

  test("closes streaming input after a native image message", async () => {
    const { create, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const message = {
      type: "user",
      message: {
        role: "user",
        content: [
          { type: "text", text: "describe it" },
          {
            type: "image",
            source: { type: "base64", media_type: "image/png", data: "aGVsbG8=" },
          },
        ],
      },
      parent_tool_use_id: null,
    } as SDKUserMessage

    channel.turn([message], { name: "image" }, { closeInput: true })
    await settled()

    expect(state.prompts).toEqual([message])
    expect(state.promptEnded).toBe(true)
    expect(channel.retiring).toBe(true)
    expect(() => channel.turn([user("too soon")], { name: "next" })).toThrow("retiring")
  })

  test("ends the turn's events when the underlying query dies", async () => {
    const { create, emit, endStream } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const turn = channel.turn([user("hi")], { name: "t1" })
    emit(assistant)
    const seen: ClaudeEvent[] = []
    for await (const event of turn.events) {
      seen.push(event)
      if (seen.length === 1) endStream()
    }
    expect(seen).toHaveLength(1)
    expect(channel.dead).toBe(true)
    expect(() => channel.turn([user("again")], { name: "t2" })).toThrow("closed")
  })

  test("supports the driver's manual-iterator pattern across turns", async () => {
    // The driver consumes via iterator.next() and breaks on the mapper's
    // finished flag, then calls iterator.return() explicitly. A turn released
    // that way must leave the channel ready for the session's next turn.
    const { create, emit } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)

    const first = channel.turn([user("one")], { name: "t1" })
    const iterator = first.events[Symbol.asyncIterator]()
    emit(assistant)
    emit(result)
    await iterator.next()
    await iterator.next()
    // The driver saw finished and breaks - manual iteration performs no
    // implicit cleanup, so it must release explicitly.
    await iterator.return?.(undefined)

    const second = channel.turn([user("two")], { name: "t2" })
    emit(assistant)
    const seen = await collect(second.events, 1)
    expect(typeOf(seen[0])).toBe("assistant")
  })

  test("closes a channel whose child ignores an interrupt", async () => {
    const { create, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create, { interruptGraceMs: 20 })
    const turn = channel.turn([user("one")], { name: "t1" })
    await turn.interrupt()
    expect(state.interrupts).toBe(1)
    // The child never acknowledges: no events, no stream end. After the grace
    // window the channel must tear itself down rather than wedge the session.
    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(channel.dead).toBe(true)
    expect(state.aborted).toBe(true)
  })

  test("closes and ends the turn when query creation never resolves", async () => {
    const channel = new Channel<Handlers>("s1", () => new Promise<never>(() => {}), { interruptGraceMs: 20 })
    const turn = channel.turn([user("one")], { name: "t1" })
    const iterator = turn.events[Symbol.asyncIterator]()

    void turn.interrupt()
    const next = await Promise.race([
      iterator.next(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("turn did not end")), 200)),
    ])

    expect(next.done).toBe(true)
    expect(channel.dead).toBe(true)
  })

  test("rejects a second concurrent turn", async () => {
    const { create } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    channel.turn([user("one")], { name: "t1" })
    expect(() => channel.turn([user("two")], { name: "t2" })).toThrow("already active")
  })

  test("interrupt reaches the query without killing the channel", async () => {
    const { create, emit, state } = fakeQuery()
    const channel = new Channel<Handlers>("s1", create)
    const turn = channel.turn([user("one")], { name: "t1" })
    await turn.interrupt()
    expect(state.interrupts).toBe(1)
    expect(channel.dead).toBe(false)
    emit(assistant)
    await collect(turn.events, 1)
  })
})

describe("channel registry", () => {
  test("offers only to a live attached session", async () => {
    const registry = createChannelRegistry<Handlers>()
    const query = fakeQuery()
    const channel = await registry.acquire("s1", "c", query.create)
    expect(registry.offer("s1", [user("idle")])).toBe(false)

    channel.turn([user("one")], { name: "t1" })
    expect(registry.offer("s1", [user("two")])).toBe(true)
    await settled()
    expect(query.state.prompts.map((message) => message.message.content)).toEqual(["one", "two"])
    await registry.closeAll()
  })

  test("reuses a live channel with the same config and recycles on change", async () => {
    const registry = createChannelRegistry<Handlers>()
    const first = fakeQuery()
    const channel1 = await registry.acquire("session", "config-a", first.create)
    const channel2 = await registry.acquire("session", "config-a", fakeQuery().create)
    expect(channel2).toBe(channel1)

    const second = fakeQuery()
    const channel3 = await registry.acquire("session", "config-b", second.create, { resumeID: "conv-9" })
    expect(channel3).not.toBe(channel1)
    expect(channel1.dead).toBe(true)
    expect(first.state.aborted).toBe(true)
    await settled()
    expect(second.state.resumeID).toBe("conv-9")
    await registry.closeAll()
  })

  test("replaces a dead channel even with an unchanged config", async () => {
    const registry = createChannelRegistry<Handlers>()
    const first = fakeQuery()
    const channel1 = await registry.acquire("session", "config-a", first.create)
    first.endStream()
    await settled()
    expect(channel1.dead).toBe(true)
    const channel2 = await registry.acquire("session", "config-a", fakeQuery().create, { resumeID: "conv-2" })
    expect(channel2).not.toBe(channel1)
  })

  test("replaces a retiring image channel even before its query exits", async () => {
    const registry = createChannelRegistry<Handlers>()
    const first = fakeQuery()
    const channel1 = await registry.acquire("session", "config-a", first.create)
    channel1.turn([user("image")], { name: "image" }, { closeInput: true })

    const channel2 = await registry.acquire("session", "config-a", fakeQuery().create, { resumeID: "conv-3" })

    expect(channel2).not.toBe(channel1)
    expect(channel1.dead).toBe(true)
    expect(first.state.aborted).toBe(true)
  })

  test("closeAll tears every channel down", async () => {
    const registry = createChannelRegistry<Handlers>()
    const one = fakeQuery()
    const two = fakeQuery()
    const a = await registry.acquire("s1", "c", one.create)
    const b = await registry.acquire("s2", "c", two.create)
    await registry.closeAll()
    expect(a.dead).toBe(true)
    expect(b.dead).toBe(true)
    expect(one.state.aborted).toBe(true)
    expect(two.state.aborted).toBe(true)
  })

  test("reaps a channel left idle past its ttl", async () => {
    // Every live channel owns a CLI child, so a session that goes quiet must
    // not keep one for the backend's lifetime.
    let clock = 0
    const registry = createChannelRegistry<Handlers>({ idleTtlMs: 100, sweepMs: 5, now: () => clock })
    const query = fakeQuery()
    const channel = await registry.acquire("s1", "c", query.create)
    expect(registry.size()).toBe(1)

    clock = 1_000
    await registry.sweepNow()
    expect(registry.size()).toBe(0)
    expect(channel.dead).toBe(true)
    expect(query.state.aborted).toBe(true)
  })

  test("never reaps a channel that is mid-turn", async () => {
    let clock = 0
    const registry = createChannelRegistry<Handlers>({ idleTtlMs: 100, sweepMs: 5, now: () => clock })
    const query = fakeQuery()
    const channel = await registry.acquire("s1", "c", query.create)
    channel.turn([user("one")], { name: "t1" })

    clock = 1_000
    await registry.sweepNow()
    expect(registry.size()).toBe(1)
    expect(channel.dead).toBe(false)
    await registry.closeAll()
  })

  test("closeAll leaves a mid-turn channel running", async () => {
    // Instance disposal also runs on a global config change and a plugin
    // install, so closing a busy channel would truncate a live turn into a
    // silent clean completion.
    const registry = createChannelRegistry<Handlers>()
    const busy = fakeQuery()
    const idle = fakeQuery()
    const a = await registry.acquire("s1", "c", busy.create)
    const b = await registry.acquire("s2", "c", idle.create)
    a.turn([user("one")], { name: "t1" })

    await registry.closeAll()
    expect(a.dead).toBe(false)
    expect(busy.state.aborted).toBe(false)
    expect(b.dead).toBe(true)
    expect(registry.get("s1")).toBe(a)
    await registry.closeAll()
  })

  test("close drops a single session's channel", async () => {
    const registry = createChannelRegistry<Handlers>()
    const query = fakeQuery()
    const channel = await registry.acquire("s1", "c", query.create)
    await registry.close("s1")
    expect(channel.dead).toBe(true)
    expect(registry.size()).toBe(0)
    expect(registry.get("s1")).toBeUndefined()
  })
})
