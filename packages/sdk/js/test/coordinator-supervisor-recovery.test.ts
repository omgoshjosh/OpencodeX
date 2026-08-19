import { describe, expect, test } from "bun:test"
import {
  COORDINATOR_HANDOFF_VERSION,
  COORDINATOR_MANIFEST_VERSION,
  coordinatorHandoffRequestID,
  coordinatorSourceManifestFence,
  reduceCoordinatorSupervisorRecovery,
  type CoordinatorHandoffPhase,
  type CoordinatorHandoffRecord,
  type CoordinatorManifest,
  type CoordinatorOwnerLockProof,
  type CoordinatorStartIdentityProof,
  type CoordinatorSupervisorHealthyProof,
  type CoordinatorSupervisorIntent,
  type CoordinatorSupervisorRecoveryAction,
  type CoordinatorSupervisorRecoveryInput,
} from "../src/coordinator/manifest"

const key = "b".repeat(40)
const sourceEpoch = "source-epoch-0001"
const targetEpoch = "target-epoch-0001"
const targetBuildID = "target-build-1"
const request = coordinatorHandoffRequestID(sourceEpoch, targetEpoch)
const sourcePID = 1001
const targetPID = 2002
const supervisorPID = 9009

function identity(pid: number): CoordinatorStartIdentityProof {
  return { state: "available", value: `start-${pid}` }
}

function sourceManifest(overrides: Partial<CoordinatorManifest> = {}): CoordinatorManifest {
  return {
    version: COORDINATOR_MANIFEST_VERSION,
    key,
    directory: "/work/source",
    database: "/private/source.sqlite",
    pid: sourcePID,
    url: "http://127.0.0.1:4101/",
    username: "source-user",
    password: "source-password",
    token: "source-token",
    createdAt: "2026-08-18T20:00:00.000Z",
    serverVersion: "source-build-1",
    authorityEpoch: sourceEpoch,
    admission: true,
    ready: true,
    ...overrides,
  }
}

function targetManifest(overrides: Partial<CoordinatorManifest> = {}): CoordinatorManifest {
  return sourceManifest({
    directory: "/work/target",
    database: "/private/target.sqlite",
    pid: targetPID,
    url: "http://127.0.0.1:4202/",
    username: "target-user",
    password: "target-password",
    token: "target-token",
    createdAt: "2026-08-18T20:00:03.000Z",
    serverVersion: targetBuildID,
    authorityEpoch: targetEpoch,
    admission: false,
    ready: false,
    ...overrides,
  })
}

function intent(): CoordinatorSupervisorIntent {
  const source = coordinatorSourceManifestFence(sourceManifest())
  return {
    version: 1,
    key,
    source: {
      ...source,
      serverVersion: source.serverVersion!,
      authorityEpoch: source.authorityEpoch!,
    },
    targetEpoch,
    request,
    targetBuildID,
    revision: 7,
    createdAt: "2026-08-18T20:00:01.000Z",
    updatedAt: "2026-08-18T20:00:08.000Z",
  }
}

function handoff(phase: CoordinatorHandoffPhase): CoordinatorHandoffRecord {
  const revision = { requested: 0, accepted: 1, ready: 2, committed: 3 }[phase]
  return {
    version: COORDINATOR_HANDOFF_VERSION,
    request,
    phase,
    revision,
    sourceEpoch,
    ...(phase === "requested" ? {} : { targetEpoch }),
    createdAt: "2026-08-18T20:00:01.000Z",
    updatedAt: new Date(Date.parse("2026-08-18T20:00:01.000Z") + revision * 1_000).toISOString(),
  }
}

function owner(overrides: Partial<CoordinatorOwnerLockProof> = {}): CoordinatorOwnerLockProof {
  return {
    state: "owned",
    key,
    token: "supervisor-owner-token",
    supervisorPID,
    supervisorStartIdentity: identity(supervisorPID),
    ...overrides,
  }
}

function healthy(
  pid: number,
  authorityEpoch: string,
  admission: boolean,
  ready: boolean,
  status: CoordinatorSupervisorHealthyProof["status"] = "running",
  startIdentity = identity(pid),
): CoordinatorSupervisorHealthyProof {
  return {
    state: "healthy",
    key,
    pid,
    startIdentity,
    authorityEpoch,
    status,
    admission,
    ready,
  }
}

function recovery(overrides: Partial<CoordinatorSupervisorRecoveryInput> = {}): CoordinatorSupervisorRecoveryInput {
  return {
    key,
    database: { derivedKey: key },
    manifest: { state: "valid", manifest: sourceManifest(), database: { derivedKey: key } },
    handoff: { state: "valid", handoff: handoff("accepted") },
    intent: intent(),
    owner: owner(),
    process: { state: "alive", key, pid: sourcePID, startIdentity: identity(sourcePID) },
    health: healthy(sourcePID, sourceEpoch, false, false),
    targetBuild: { state: "available", buildID: targetBuildID },
    ...overrides,
  }
}

function targetLive(
  phase: "absent" | CoordinatorHandoffPhase,
  admission: boolean,
  ready: boolean,
  status: CoordinatorSupervisorHealthyProof["status"] = "running",
) {
  return recovery({
    manifest: {
      state: "valid",
      manifest: targetManifest({ admission, ready }),
      database: { derivedKey: key },
    },
    handoff: phase === "absent" ? { state: "absent" } : { state: "valid", handoff: handoff(phase) },
    process: { state: "alive", key, pid: targetPID, startIdentity: identity(targetPID) },
    health: healthy(targetPID, targetEpoch, admission, ready, status),
  })
}

function deadSource(phase: "absent" | CoordinatorHandoffPhase, ownerState: "owned" | "available" | "unavailable") {
  return recovery({
    handoff: phase === "absent" ? { state: "absent" } : { state: "valid", handoff: handoff(phase) },
    owner: ownerState === "owned" ? owner() : { state: ownerState, key },
    process: { state: "dead", key, pid: sourcePID, startIdentity: identity(sourcePID) },
    health: { state: "unavailable", key, pid: sourcePID, startIdentity: identity(sourcePID) },
  })
}

function absentDead(phase: "absent" | CoordinatorHandoffPhase, ownerState: "owned" | "available" | "unavailable") {
  return recovery({
    manifest: { state: "absent" },
    handoff: phase === "absent" ? { state: "absent" } : { state: "valid", handoff: handoff(phase) },
    owner: ownerState === "owned" ? owner() : { state: ownerState, key },
    process: { state: "dead", key, pid: sourcePID, startIdentity: identity(sourcePID) },
    health: { state: "unavailable", key, pid: sourcePID, startIdentity: identity(sourcePID) },
  })
}

function deadTarget(phase: "accepted" | "ready" | "committed", ownerState: "owned" | "available" | "unavailable") {
  const input = targetLive(phase, phase !== "accepted", phase !== "accepted", "verified")
  input.owner = ownerState === "owned" ? owner() : { state: ownerState, key }
  input.process = { state: "dead", key, pid: targetPID, startIdentity: identity(targetPID) }
  input.health = { state: "unavailable", key, pid: targetPID, startIdentity: identity(targetPID) }
  return input
}

function action(input: CoordinatorSupervisorRecoveryInput) {
  return reduceCoordinatorSupervisorRecovery(input)
}

function expectAction(
  input: CoordinatorSupervisorRecoveryInput,
  expected: CoordinatorSupervisorRecoveryAction["action"],
) {
  const result = action(input)
  expect(result.action).toBe(expected)
  return result
}

describe("coordinator supervisor pure crash table", () => {
  test("ordinary attach, stale removal, acquisition, and start are separate", () => {
    expectAction(
      recovery({
        intent: undefined,
        handoff: { state: "absent" },
        health: healthy(sourcePID, sourceEpoch, true, true),
        owner: { state: "unavailable", key },
      }),
      "attach_source",
    )
    expectAction({ ...deadSource("absent", "owned"), intent: undefined }, "remove_exact_dead_manifest")
    expectAction({ ...absentDead("absent", "available"), intent: undefined }, "acquire_owner")
    expectAction({ ...absentDead("absent", "owned"), intent: undefined }, "ordinary_start")
  })

  test("request, stop, requested abort, and abandon do not require owner lock", () => {
    const results = [
      expectAction(recovery({ handoff: { state: "absent" }, owner: { state: "unknown" } }), "request_continue_drain"),
      expectAction(recovery({ owner: { state: "unknown" } }), "stop_exact_source"),
      expectAction(
        recovery({
          handoff: { state: "valid", handoff: handoff("requested") },
          owner: { state: "unknown" },
          targetBuild: { state: "missing", buildID: targetBuildID },
        }),
        "abort_exact_requested",
      ),
      expectAction(
        recovery({
          handoff: { state: "absent" },
          owner: { state: "unknown" },
          targetBuild: { state: "missing", buildID: targetBuildID },
        }),
        "abandon_intent",
      ),
    ]
    for (const result of results) expect("owner" in result).toBe(false)
  })

  test("preserves accepted source manifest and starts target after exact dead proof", () => {
    expectAction(deadSource("accepted", "available"), "acquire_owner")
    const result = expectAction(deadSource("accepted", "owned"), "start_retry_target")
    expect(result).toMatchObject({
      credentialSource: "generate_before_publication",
      owner: { supervisorPID },
      process: { state: "dead", pid: sourcePID },
      build: { state: "available", buildID: targetBuildID },
      fence: { manifest: { state: "valid", manifest: sourceManifest() } },
    })
    expect(result.action).not.toBe("remove_exact_dead_manifest")
  })

  test("recovers dead requested without removing the source manifest", () => {
    expectAction(deadSource("requested", "available"), "acquire_owner")
    const result = expectAction(deadSource("requested", "owned"), "recover_requested")
    expect(result).toMatchObject({
      process: { state: "dead", pid: sourcePID },
      build: { state: "available", buildID: targetBuildID },
      fence: { manifest: { state: "valid", manifest: sourceManifest() } },
    })
  })

  test("uses exact target credentials after any target publication", () => {
    expect(expectAction(targetLive("accepted", false, false), "open_verify_target")).toMatchObject({
      credentialSource: "exact_target_manifest",
    })
    expect(expectAction(targetLive("accepted", false, false, "verified"), "finish_activation")).toMatchObject({
      credentialSource: "exact_target_manifest",
    })
    expectAction(targetLive("ready", true, true, "verified"), "commit")
    expectAction(targetLive("committed", true, true, "verified"), "cleanup")
    expectAction(targetLive("absent", true, true, "verified"), "attach_target")
    for (const phase of ["accepted", "ready", "committed"] as const) {
      expect(expectAction(deadTarget(phase, "owned"), "start_retry_target")).toMatchObject({
        credentialSource: "exact_target_manifest",
        process: { state: "dead", pid: targetPID },
        build: { state: "available", buildID: targetBuildID },
      })
    }
  })

  test("missing build abandons before handoff, aborts requested, and fails accepted or later", () => {
    expectAction(
      recovery({ handoff: { state: "absent" }, targetBuild: { state: "missing", buildID: targetBuildID } }),
      "abandon_intent",
    )
    expectAction(
      recovery({
        handoff: { state: "valid", handoff: handoff("requested") },
        targetBuild: { state: "missing", buildID: targetBuildID },
      }),
      "abort_exact_requested",
    )
    for (const phase of ["accepted", "ready", "committed"] as const) {
      const input = phase === "accepted" ? recovery() : targetLive(phase, true, true, "verified")
      input.targetBuild = { state: "missing", buildID: targetBuildID }
      expect(action(input)).toEqual({ action: "fail_closed", reason: "target_build_missing_after_acceptance" })
    }
  })
})

describe("coordinator supervisor ownership and identity proofs", () => {
  test("supervisor owner PID and start identity are independent from child process", () => {
    const input = targetLive("ready", true, true, "verified")
    input.owner = owner({ supervisorPID, supervisorStartIdentity: identity(supervisorPID) })
    const result = expectAction(input, "commit")
    expect(result).toMatchObject({ owner: { supervisorPID }, process: { pid: targetPID } })
    expect(supervisorPID).not.toBe(targetPID)
  })

  test("accepts both unavailable start identities but rejects one-sided downgrade and mismatch", () => {
    const unavailable = { state: "unavailable" as const }
    expectAction(
      recovery({
        process: { state: "alive", key, pid: sourcePID, startIdentity: unavailable },
        health: healthy(sourcePID, sourceEpoch, false, false, "running", unavailable),
      }),
      "stop_exact_source",
    )
    for (const [processIdentity, healthIdentity] of [
      [identity(sourcePID), unavailable],
      [unavailable, identity(sourcePID)],
      [identity(sourcePID), { state: "available" as const, value: "different-start" }],
    ] as const) {
      expect(
        action(
          recovery({
            process: { state: "alive", key, pid: sourcePID, startIdentity: processIdentity },
            health: healthy(sourcePID, sourceEpoch, false, false, "running", healthIdentity),
          }),
        ),
      ).toEqual({ action: "fail_closed", reason: "proof_start_identity_mismatch" })
    }
    expect(
      action(
        recovery({
          process: { state: "alive", key, pid: sourcePID } as CoordinatorSupervisorRecoveryInput["process"],
          health: {
            ...healthy(sourcePID, sourceEpoch, false, false),
            startIdentity: undefined,
          } as unknown as CoordinatorSupervisorRecoveryInput["health"],
        }),
      ),
    ).toEqual({ action: "fail_closed", reason: "malformed_process" })
  })

  test("rejects key, PID, contradictory liveness, unknown, and build proof mismatches", () => {
    const failures: Array<[CoordinatorSupervisorRecoveryInput, string]> = [
      [recovery({ owner: { ...owner(), key: "c".repeat(40) } }), "proof_key_mismatch"],
      [recovery({ health: healthy(9999, sourceEpoch, false, false) }), "proof_pid_mismatch"],
      [
        recovery({ process: { state: "dead", key, pid: sourcePID, startIdentity: identity(sourcePID) } }),
        "contradictory_process_health",
      ],
      [recovery({ process: { state: "unknown", key } }), "process_unknown"],
      [recovery({ targetBuild: { state: "unknown" } }), "target_build_unknown"],
      [recovery({ targetBuild: { state: "available", buildID: "other-build" } }), "target_build_proof_mismatch"],
    ]
    for (const [input, reason] of failures) expect(action(input)).toEqual({ action: "fail_closed", reason })
  })

  test("proves manifest database identity only through derived coordinator key", () => {
    expect(action(recovery())).not.toEqual({ action: "fail_closed", reason: "database_key_mismatch" })
    expect(
      action(
        recovery({
          manifest: {
            state: "valid",
            manifest: sourceManifest(),
            database: { derivedKey: "c".repeat(40) },
          },
        }),
      ),
    ).toEqual({ action: "fail_closed", reason: "database_key_mismatch" })
    expect(action({ ...absentDead("accepted", "owned"), database: { derivedKey: "c".repeat(40) } })).toEqual({
      action: "fail_closed",
      reason: "database_key_mismatch",
    })
  })

  test("binds absent-manifest dead proof to the persisted source PID", () => {
    expect(
      action(
        recovery({
          manifest: { state: "absent" },
          handoff: { state: "valid", handoff: handoff("requested") },
          process: { state: "dead", key, pid: 9999, startIdentity: identity(9999) },
          health: { state: "unavailable", key, pid: 9999, startIdentity: identity(9999) },
        }),
      ),
    ).toEqual({ action: "fail_closed", reason: "proof_pid_mismatch" })
  })
})

describe("coordinator supervisor real manifest and health policy", () => {
  test("ordinary source requires manifest and health true/true", () => {
    for (const [manifestAdmission, manifestReady, healthAdmission, healthReady, expected] of [
      [true, true, true, true, "attach_source"],
      [false, false, true, true, "fail_closed"],
      [true, true, false, false, "fail_closed"],
    ] as const) {
      expectAction(
        recovery({
          intent: undefined,
          handoff: { state: "absent" },
          manifest: {
            state: "valid",
            manifest: sourceManifest({ admission: manifestAdmission, ready: manifestReady }),
            database: { derivedKey: key },
          },
          health: healthy(sourcePID, sourceEpoch, healthAdmission, healthReady),
        }),
        expected,
      )
    }
  })

  test("requested and accepted source keep manifest true/true while health is false/false", () => {
    for (const phase of ["requested", "accepted"] as const) {
      const result = expectAction(
        recovery({ handoff: { state: "valid", handoff: handoff(phase) } }),
        phase === "requested" ? "request_continue_drain" : "stop_exact_source",
      )
      expect(result.fence.manifest).toMatchObject({
        state: "valid",
        manifest: { admission: true, ready: true },
      })
      expect(result).toMatchObject({ health: { admission: false, ready: false } })
      expectAction(
        recovery({
          handoff: { state: "valid", handoff: handoff(phase) },
          health: healthy(sourcePID, sourceEpoch, true, true),
        }),
        "fail_closed",
      )
    }
  })

  test("pre-handoff source health rejects mixed admission and readiness", () => {
    for (const [admission, ready] of [
      [true, false],
      [false, true],
    ] as const) {
      expectAction(
        recovery({
          handoff: { state: "absent" },
          health: healthy(sourcePID, sourceEpoch, admission, ready),
        }),
        "fail_closed",
      )
    }
    expectAction(
      recovery({ handoff: { state: "absent" }, health: healthy(sourcePID, sourceEpoch, true, true) }),
      "request_continue_drain",
    )
    expectAction(
      recovery({ handoff: { state: "absent" }, health: healthy(sourcePID, sourceEpoch, false, false) }),
      "request_continue_drain",
    )
  })

  test("target manifest and health flags independently match every durable phase", () => {
    const acceptedManifestMismatch = targetLive("accepted", false, false)
    acceptedManifestMismatch.manifest = {
      state: "valid",
      manifest: targetManifest({ admission: true, ready: true }),
      database: { derivedKey: key },
    }
    expectAction(acceptedManifestMismatch, "fail_closed")

    const acceptedHealthMismatch = targetLive("accepted", false, false)
    acceptedHealthMismatch.health = healthy(targetPID, targetEpoch, true, true)
    expectAction(acceptedHealthMismatch, "fail_closed")

    for (const phase of ["ready", "committed", "absent"] as const) {
      const manifestMismatch = targetLive(phase, true, true, "verified")
      manifestMismatch.manifest = {
        state: "valid",
        manifest: targetManifest({ admission: false, ready: false }),
        database: { derivedKey: key },
      }
      expectAction(manifestMismatch, "fail_closed")

      const healthMismatch = targetLive(phase, true, true, "verified")
      healthMismatch.health = healthy(targetPID, targetEpoch, false, false, "verified")
      expectAction(healthMismatch, "fail_closed")
    }
  })

  test("target manifest phase flags are enforced before dead-process recovery", () => {
    const accepted = deadTarget("accepted", "owned")
    accepted.manifest = {
      state: "valid",
      manifest: targetManifest({ admission: true, ready: true }),
      database: { derivedKey: key },
    }
    expectAction(accepted, "fail_closed")

    for (const phase of ["ready", "committed"] as const) {
      const input = deadTarget(phase, "owned")
      input.manifest = {
        state: "valid",
        manifest: targetManifest({ admission: false, ready: false }),
        database: { derivedKey: key },
      }
      expectAction(input, "fail_closed")
    }
  })
})

describe("coordinator supervisor action fences and properties", () => {
  test("every mutating action carries complete private in-memory observations", () => {
    const inputs = [
      recovery({ handoff: { state: "absent" } }),
      recovery(),
      deadSource("accepted", "available"),
      deadSource("accepted", "owned"),
      targetLive("accepted", false, false),
      targetLive("accepted", false, false, "verified"),
      targetLive("ready", true, true, "verified"),
      targetLive("committed", true, true, "verified"),
      recovery({
        handoff: { state: "valid", handoff: handoff("requested") },
        targetBuild: { state: "missing", buildID: targetBuildID },
      }),
      deadSource("requested", "owned"),
      recovery({ handoff: { state: "absent" }, targetBuild: { state: "missing", buildID: targetBuildID } }),
      { ...deadSource("absent", "owned"), intent: undefined },
      { ...absentDead("absent", "owned"), intent: undefined },
    ]
    for (const input of inputs) {
      const result = action(input)
      expect(result.action).not.toBe("fail_closed")
      if (!("reobserve" in result)) throw new Error(`expected mutating action, received ${result.action}`)
      expect(result.reobserve).toBe(true)
      expect(result.fence.database).toEqual(input.database)
      expect(result.fence.intent).toEqual(input.intent ? { state: "valid", intent: input.intent } : { state: "absent" })
      expect(result.fence.owner).toEqual(input.owner)
      expect(result.fence.process).toEqual(input.process)
      expect(result.fence.health).toEqual(input.health)
      expect(result.fence.targetBuild).toEqual(input.targetBuild)
      if (input.manifest.state === "valid") {
        expect(result.fence.manifest).toEqual({
          state: "valid",
          manifest: input.manifest.manifest,
        })
        expect(result.fence.manifest.manifest.token).toBe(input.manifest.manifest.token)
        expect(result.fence.manifest.manifest.password).toBe(input.manifest.manifest.password)
      }
      if (input.handoff.state === "valid") {
        expect(result.fence.handoff).toEqual({ state: "valid", handoff: input.handoff.handoff })
      }
    }
  })

  test("dead and build dependent actions carry exact direct proofs", () => {
    for (const input of [
      deadSource("accepted", "owned"),
      deadSource("requested", "owned"),
      deadTarget("ready", "owned"),
      { ...deadSource("absent", "owned"), intent: undefined },
      { ...absentDead("absent", "owned"), intent: undefined },
    ]) {
      const result = action(input)
      if (result.action === "fail_closed" || !("process" in result)) throw new Error("missing dead proof")
      expect(result.process).toEqual(input.process)
      expect(result.process.state).toBe("dead")
      if ("build" in result) expect(result.build).toEqual(input.targetBuild)
    }
  })

  test("never starts, opens, activates, commits, cleans, removes, or recovers without owned lock", () => {
    const ownerRequired = new Set([
      "ordinary_start",
      "remove_exact_dead_manifest",
      "recover_requested",
      "start_retry_target",
      "open_verify_target",
      "finish_activation",
      "commit",
      "cleanup",
    ])
    const scenarios = [
      { ...absentDead("absent", "owned"), intent: undefined },
      { ...deadSource("absent", "owned"), intent: undefined },
      deadSource("requested", "owned"),
      deadSource("accepted", "owned"),
      targetLive("accepted", false, false),
      targetLive("accepted", false, false, "verified"),
      targetLive("ready", true, true, "verified"),
      targetLive("committed", true, true, "verified"),
    ]
    for (const scenario of scenarios) {
      for (const ownerState of ["owned", "available", "unavailable"] as const) {
        scenario.owner = ownerState === "owned" ? owner() : { state: ownerState, key }
        const result = action(scenario)
        if (ownerRequired.has(result.action)) {
          expect(ownerState).toBe("owned")
          expect("owner" in result && result.owner.state).toBe("owned")
        }
      }
    }
  })

  test("future phase is rejected as malformed at the observation boundary", () => {
    expect(
      action(
        recovery({
          handoff: {
            state: "valid",
            handoff: { ...handoff("accepted"), phase: "future" as CoordinatorHandoffPhase },
          },
        }),
      ),
    ).toEqual({ action: "fail_closed", reason: "malformed_handoff" })
  })

  test("rejects equal source and target epochs at the reducer boundary", () => {
    const value = intent()
    const invalid = {
      ...value,
      targetEpoch: value.source.authorityEpoch,
      request: coordinatorHandoffRequestID(value.source.authorityEpoch, value.source.authorityEpoch),
    }
    expect(action(recovery({ intent: invalid }))).toEqual({ action: "fail_closed", reason: "invalid_intent" })
  })

  test("is total for malformed runtime envelopes and nested values", () => {
    const malformed: Array<[unknown, string]> = [
      [null, "malformed_recovery_input"],
      [{}, "malformed_recovery_input"],
      [{ ...recovery(), extra: true }, "malformed_recovery_input"],
      [{ ...recovery(), database: null }, "malformed_database_proof"],
      [{ ...recovery(), manifest: null }, "malformed_manifest"],
      [{ ...recovery(), manifest: { state: "future" } }, "malformed_manifest"],
      [{ ...recovery(), manifest: { state: "valid", manifest: null } }, "malformed_manifest"],
      [{ ...recovery(), handoff: null }, "malformed_handoff"],
      [
        { ...recovery(), handoff: { state: "valid", handoff: { ...handoff("accepted"), phase: "future" } } },
        "malformed_handoff",
      ],
      [{ ...recovery(), owner: null }, "malformed_owner"],
      [{ ...recovery(), owner: { state: "owned", key } }, "malformed_owner"],
      [{ ...recovery(), process: null }, "malformed_process"],
      [{ ...recovery(), process: { state: "alive", key, pid: sourcePID, startIdentity: null } }, "malformed_process"],
      [{ ...recovery(), health: null }, "malformed_health"],
      [{ ...recovery(), health: { state: "healthy", key, pid: sourcePID } }, "malformed_health"],
      [{ ...recovery(), targetBuild: null }, "malformed_build"],
      [{ ...recovery(), targetBuild: { state: "future" } }, "malformed_build"],
      [{ ...recovery(), intent: null }, "invalid_intent"],
      [{ ...recovery(), intent: { version: 1 } }, "invalid_intent"],
    ]
    for (const [value, reason] of malformed) {
      expect(() => reduceCoordinatorSupervisorRecovery(value)).not.toThrow()
      expect(reduceCoordinatorSupervisorRecovery(value)).toEqual({ action: "fail_closed", reason })
    }
  })
})
