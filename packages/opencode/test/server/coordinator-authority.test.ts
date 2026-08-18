import { OPENCODE_PROCESS_ROLE } from "@opencode-ai/core/util/opencode-process"
import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { CoordinatorAuthority } from "../../src/server/coordinator-authority"

const role = process.env[OPENCODE_PROCESS_ROLE]
const epoch = process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH

describe.serial("coordinator authority admission", () => {
  beforeEach(() => {
    process.env[OPENCODE_PROCESS_ROLE] = "coordinator"
    process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = "epoch-test"
    CoordinatorAuthority.resetForTest()
  })

  afterEach(() => {
    if (role === undefined) delete process.env[OPENCODE_PROCESS_ROLE]
    else process.env[OPENCODE_PROCESS_ROLE] = role
    if (epoch === undefined) delete process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH
    else process.env.OPENCODE_COORDINATOR_AUTHORITY_EPOCH = epoch
    CoordinatorAuthority.resetForTest()
  })

  test("closes before waiting for deterministic drain", async () => {
    const release = CoordinatorAuthority.acquire("/session")
    expect(release).toBeFunction()

    let drained = false
    const wait = CoordinatorAuthority.close().then(() => (drained = true))
    expect(CoordinatorAuthority.health()).toEqual({ authorityEpoch: "epoch-test", admission: false, ready: false })
    expect(CoordinatorAuthority.acquire("/session/one")).toBeUndefined()
    await Promise.resolve()
    expect(drained).toBe(false)

    release?.()
    release?.()
    await wait
    expect(drained).toBe(true)
  })

  test("reopens mutation admission", async () => {
    await CoordinatorAuthority.close()
    CoordinatorAuthority.reopen()

    expect(CoordinatorAuthority.health()).toEqual({ authorityEpoch: "epoch-test", admission: true, ready: true })
    expect(CoordinatorAuthority.acquire("/session/one")).toBeFunction()
  })

  test("allows only audited observation and control paths while closed", async () => {
    await CoordinatorAuthority.close()

    expect(CoordinatorAuthority.acquire("/global/health")).toBeFunction()
    expect(CoordinatorAuthority.acquire("/global/event")).toBeFunction()
    expect(CoordinatorAuthority.acquire("/global/authority-handoff")).toBeFunction()
    expect(CoordinatorAuthority.acquire("/session")).toBeUndefined()
    expect(CoordinatorAuthority.acquire("/global/dispose")).toBeUndefined()
  })

  test("does not expose authority state outside coordinator mode", () => {
    process.env[OPENCODE_PROCESS_ROLE] = "worker"
    expect(CoordinatorAuthority.health()).toBeUndefined()
    expect(CoordinatorAuthority.acquire("/session")).toBeFunction()
  })
})
