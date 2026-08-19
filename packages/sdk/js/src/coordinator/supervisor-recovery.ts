import {
  isCoordinatorHandoffRecord,
  isCoordinatorKey,
  isCoordinatorManifest,
  type CoordinatorHandoffRecord,
  type CoordinatorManifest,
} from "./manifest.js"
import {
  isCoordinatorSupervisorBuildID,
  isCoordinatorSupervisorIntent,
  type CoordinatorSupervisorIntent,
} from "./supervisor-intent.js"

export type CoordinatorStartIdentityProof = { state: "available"; value: string } | { state: "unavailable" }

export type CoordinatorSupervisorManifestObservation =
  | { state: "absent" }
  | { state: "malformed"; kind: "malformed" | "legacy" }
  | { state: "valid"; manifest: CoordinatorManifest; database?: { derivedKey: string } }

export type CoordinatorSupervisorHandoffObservation =
  | { state: "absent" }
  | { state: "malformed" }
  | { state: "legacy" }
  | { state: "valid"; handoff: CoordinatorHandoffRecord }

export type CoordinatorOwnerLockProof = {
  state: "owned"
  key: string
  token: string
  supervisorPID: number
  supervisorStartIdentity: CoordinatorStartIdentityProof
}

export type CoordinatorSupervisorOwnerObservation =
  | CoordinatorOwnerLockProof
  | { state: "available"; key: string }
  | { state: "unavailable"; key: string }
  | { state: "unknown"; key?: string }

export type CoordinatorSupervisorProcessProof = {
  state: "alive" | "dead"
  key: string
  pid: number
  startIdentity: CoordinatorStartIdentityProof
}

export type CoordinatorSupervisorAliveProcessProof = CoordinatorSupervisorProcessProof & { state: "alive" }
export type CoordinatorSupervisorDeadProcessProof = CoordinatorSupervisorProcessProof & { state: "dead" }

export type CoordinatorSupervisorProcessObservation =
  | CoordinatorSupervisorProcessProof
  | { state: "unknown"; key?: string; pid?: number; startIdentity?: CoordinatorStartIdentityProof }

export type CoordinatorSupervisorHealthyProof = {
  state: "healthy"
  key: string
  pid: number
  startIdentity: CoordinatorStartIdentityProof
  authorityEpoch: string
  status: "running" | "verified"
  admission: boolean
  ready: boolean
}

export type CoordinatorSupervisorHealthObservation =
  | CoordinatorSupervisorHealthyProof
  | {
      state: "unhealthy" | "unavailable"
      key: string
      pid: number
      startIdentity: CoordinatorStartIdentityProof
    }

export type CoordinatorSupervisorAvailableBuildProof = { state: "available"; buildID: string }
export type CoordinatorSupervisorMissingBuildProof = { state: "missing"; buildID: string }
export type CoordinatorSupervisorBuildObservation =
  | CoordinatorSupervisorAvailableBuildProof
  | CoordinatorSupervisorMissingBuildProof
  | { state: "unknown"; buildID?: string }

export type CoordinatorSupervisorRecoveryInput = {
  key: string
  database: { derivedKey: string }
  manifest: CoordinatorSupervisorManifestObservation
  handoff: CoordinatorSupervisorHandoffObservation
  intent?: CoordinatorSupervisorIntent
  owner: CoordinatorSupervisorOwnerObservation
  process: CoordinatorSupervisorProcessObservation
  health: CoordinatorSupervisorHealthObservation
  targetBuild: CoordinatorSupervisorBuildObservation
}

/**
 * Sensitive in-memory execution fence. It contains full manifest credentials and
 * owner tokens. It must never be serialized, logged, persisted, or returned by a
 * diagnostic API.
 */
export type CoordinatorSupervisorRecoveryFence = {
  key: string
  database: { derivedKey: string }
  intent: { state: "absent" } | { state: "valid"; intent: CoordinatorSupervisorIntent }
  manifest: { state: "absent" } | { state: "valid"; manifest: CoordinatorManifest }
  handoff: { state: "absent" } | { state: "valid"; handoff: CoordinatorHandoffRecord }
  owner: CoordinatorSupervisorOwnerObservation
  process: CoordinatorSupervisorProcessProof
  health: CoordinatorSupervisorHealthObservation
  targetBuild: CoordinatorSupervisorBuildObservation
}

export type CoordinatorSupervisorFailReason =
  | "invalid_key"
  | "malformed_recovery_input"
  | "malformed_database_proof"
  | "malformed_manifest"
  | "legacy_manifest"
  | "malformed_handoff"
  | "legacy_handoff"
  | "orphaned_handoff"
  | "invalid_intent"
  | "intent_manifest_mismatch"
  | "intent_handoff_mismatch"
  | "database_key_mismatch"
  | "proof_key_mismatch"
  | "proof_pid_mismatch"
  | "proof_start_identity_mismatch"
  | "malformed_owner"
  | "malformed_process"
  | "malformed_health"
  | "malformed_build"
  | "owner_ambiguous"
  | "owner_required"
  | "process_unknown"
  | "contradictory_process_health"
  | "live_health_unavailable"
  | "health_epoch_mismatch"
  | "health_state_mismatch"
  | "target_build_unknown"
  | "target_build_proof_mismatch"
  | "target_build_missing_after_acceptance"
  | "invalid_recovery_state"

type Fenced = { fence: CoordinatorSupervisorRecoveryFence }
type Reobserve = Fenced & { reobserve: true }
type OwnedMutation = Reobserve & { owner: CoordinatorOwnerLockProof }
type LiveProof = {
  process: CoordinatorSupervisorAliveProcessProof
  health: CoordinatorSupervisorHealthyProof
}
type DeadProof = { process: CoordinatorSupervisorDeadProcessProof }

/** Sensitive execution commands: never serialize, persist, log, or expose them diagnostically. */
export type CoordinatorSupervisorRecoveryAction =
  | ({ action: "attach_source" } & Fenced)
  | ({ action: "attach_target"; credentialSource: "exact_target_manifest" } & Fenced)
  | ({ action: "wait_source" } & Fenced)
  | ({ action: "request_continue_drain"; build: CoordinatorSupervisorAvailableBuildProof } & Reobserve & LiveProof)
  | ({ action: "stop_exact_source"; build: CoordinatorSupervisorAvailableBuildProof } & Reobserve & LiveProof)
  | ({ action: "acquire_owner"; process: CoordinatorSupervisorProcessProof } & Reobserve)
  | ({ action: "remove_exact_dead_manifest"; build: CoordinatorSupervisorBuildObservation } & OwnedMutation & DeadProof)
  | ({
      action: "start_retry_target"
      credentialSource: "generate_before_publication" | "exact_target_manifest"
      build: CoordinatorSupervisorAvailableBuildProof
    } & OwnedMutation &
      DeadProof)
  | ({
      action: "open_verify_target"
      credentialSource: "exact_target_manifest"
      build: CoordinatorSupervisorAvailableBuildProof
    } & OwnedMutation &
      LiveProof)
  | ({
      action: "finish_activation"
      credentialSource: "exact_target_manifest"
      build: CoordinatorSupervisorAvailableBuildProof
    } & OwnedMutation &
      LiveProof)
  | ({ action: "commit"; build: CoordinatorSupervisorAvailableBuildProof } & OwnedMutation & LiveProof)
  | ({ action: "cleanup"; build: CoordinatorSupervisorAvailableBuildProof } & OwnedMutation & LiveProof)
  | ({ action: "abort_exact_requested"; build: CoordinatorSupervisorMissingBuildProof } & Reobserve & LiveProof)
  | ({ action: "recover_requested"; build: CoordinatorSupervisorBuildObservation } & OwnedMutation & DeadProof)
  | ({
      action: "abandon_intent"
      build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof
      process: CoordinatorSupervisorProcessProof
    } & Reobserve)
  | ({ action: "ordinary_start" } & OwnedMutation & DeadProof)
  | { action: "fail_closed"; reason: CoordinatorSupervisorFailReason }

export function reduceCoordinatorSupervisorRecovery(value: unknown): CoordinatorSupervisorRecoveryAction {
  const decoded = decodeRecoveryInput(value)
  if (typeof decoded === "string") return fail(decoded)
  const input = decoded
  const invalid = validateObservations(input)
  if (invalid) return fail(invalid)
  if (input.manifest.state === "malformed") {
    return fail(input.manifest.kind === "legacy" ? "legacy_manifest" : "malformed_manifest")
  }
  if (input.handoff.state === "legacy") return fail("legacy_handoff")
  if (input.handoff.state === "malformed") return fail("malformed_handoff")
  if (input.process.state === "unknown") return fail("process_unknown")
  if (!input.intent) return reduceOrdinary(input as ValidatedInput)
  if (!isCoordinatorSupervisorIntent(input.intent) || input.intent.key !== input.key) return fail("invalid_intent")
  if (input.manifest.state === "absent" && input.process.pid !== input.intent.source.pid) {
    return fail("proof_pid_mismatch")
  }
  const recovery = { ...input, intent: input.intent } as ValidatedIntentInput
  const build = classifyBuild(recovery.targetBuild, recovery.intent.targetBuildID)
  if (build === "mismatch") return fail("target_build_proof_mismatch")
  if (build === "unknown") return fail("target_build_unknown")
  if (recovery.handoff.state === "absent") return reduceWithoutHandoff(recovery, build)
  if (recovery.handoff.state !== "valid") return fail("invalid_recovery_state")
  if (!handoffMatchesIntent(recovery.handoff.handoff, recovery.intent)) return fail("intent_handoff_mismatch")
  const manifest = classifyManifest(recovery.manifest, recovery.intent)
  if (manifest === "mismatch") return fail("intent_manifest_mismatch")
  switch (recovery.handoff.handoff.phase) {
    case "requested":
      return reduceRequested(recovery, manifest, build)
    case "accepted":
      return reduceAccepted(recovery, manifest, build)
    case "ready":
      return reduceReady(recovery, manifest, build)
    case "committed":
      return reduceCommitted(recovery, manifest, build)
  }
}

type ValidatedInput = CoordinatorSupervisorRecoveryInput & {
  process: CoordinatorSupervisorProcessProof
  manifest: Exclude<CoordinatorSupervisorManifestObservation, { state: "malformed" }>
  handoff: Exclude<CoordinatorSupervisorHandoffObservation, { state: "malformed" } | { state: "legacy" }>
}
type ValidatedIntentInput = ValidatedInput & { intent: CoordinatorSupervisorIntent }
type ManifestClassification = "source" | "target" | "absent" | "mismatch"

function reduceOrdinary(input: ValidatedInput): CoordinatorSupervisorRecoveryAction {
  if (input.handoff.state === "valid") return fail("orphaned_handoff")
  if (input.manifest.state === "valid") {
    if (input.process.state === "alive") {
      if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
      const health = requireHealth(input, input.manifest.manifest.authorityEpoch, true, true)
      if (typeof health === "string") return fail(health)
      return { action: "attach_source", ...fenced(input) }
    }
    if (input.owner.state === "available") return acquire(input)
    const owner = owned(input)
    if (!owner) return waitOrOwnerFailure(input)
    return {
      action: "remove_exact_dead_manifest",
      owner,
      process: deadProof(input),
      build: buildProof(input),
      ...reobserve(input),
    }
  }
  if (input.process.state !== "dead") return fail("invalid_recovery_state")
  if (input.owner.state === "available") return acquire(input)
  const owner = owned(input)
  if (!owner) return waitOrOwnerFailure(input)
  return { action: "ordinary_start", owner, process: deadProof(input), ...reobserve(input) }
}

function reduceWithoutHandoff(
  input: ValidatedIntentInput,
  build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof,
): CoordinatorSupervisorRecoveryAction {
  const manifest = classifyManifest(input.manifest, input.intent)
  if (manifest === "mismatch") return fail("intent_manifest_mismatch")
  if (manifest === "target") {
    if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
    if (input.process.state !== "alive") return fail("invalid_recovery_state")
    const health = requireHealth(input, input.intent.targetEpoch, true, true)
    if (typeof health === "string") return fail(health)
    return { action: "attach_target", credentialSource: "exact_target_manifest", ...fenced(input) }
  }
  if (manifest === "source" && input.process.state === "alive") {
    if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
    const health = requireHealth(input, input.intent.source.authorityEpoch)
    if (typeof health === "string") return fail(health)
    if (health.admission !== health.ready) return fail("health_state_mismatch")
    if (build.state === "missing") {
      return { action: "abandon_intent", build, process: aliveProof(input), ...reobserve(input) }
    }
    return {
      action: "request_continue_drain",
      build,
      process: aliveProof(input),
      health,
      ...reobserve(input),
    }
  }
  if (input.process.state !== "dead") return fail("invalid_recovery_state")
  return { action: "abandon_intent", build, process: deadProof(input), ...reobserve(input) }
}

function reduceRequested(
  input: ValidatedIntentInput,
  manifest: ManifestClassification,
  build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof,
): CoordinatorSupervisorRecoveryAction {
  if (manifest === "target") return fail("invalid_recovery_state")
  if (input.process.state === "dead") {
    if (input.owner.state === "available") return acquire(input)
    const owner = owned(input)
    if (!owner) return waitOrOwnerFailure(input)
    return { action: "recover_requested", owner, process: deadProof(input), build, ...reobserve(input) }
  }
  if (manifest !== "source") return fail("intent_manifest_mismatch")
  if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
  const health = requireHealth(input, input.intent.source.authorityEpoch, false, false)
  if (typeof health === "string") return fail(health)
  if (build.state === "missing") {
    return { action: "abort_exact_requested", build, process: aliveProof(input), health, ...reobserve(input) }
  }
  return {
    action: "request_continue_drain",
    build,
    process: aliveProof(input),
    health,
    ...reobserve(input),
  }
}

function reduceAccepted(
  input: ValidatedIntentInput,
  manifest: ManifestClassification,
  build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof,
): CoordinatorSupervisorRecoveryAction {
  if (build.state === "missing") return fail("target_build_missing_after_acceptance")
  if (manifest === "source") {
    if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
    if (input.process.state === "alive") {
      const health = requireHealth(input, input.intent.source.authorityEpoch, false, false)
      if (typeof health === "string") return fail(health)
      return { action: "stop_exact_source", build, process: aliveProof(input), health, ...reobserve(input) }
    }
    if (input.owner.state === "available") return acquire(input)
    const owner = owned(input)
    if (!owner) return waitOrOwnerFailure(input)
    return {
      action: "start_retry_target",
      credentialSource: "generate_before_publication",
      build,
      owner,
      process: deadProof(input),
      ...reobserve(input),
    }
  }
  if (manifest === "target") {
    if (!manifestHasState(input, false, false)) return fail("health_state_mismatch")
    if (input.process.state === "dead") return restartPublishedTarget(input, build)
    const health = requireHealth(input, input.intent.targetEpoch, false, false)
    if (typeof health === "string") return fail(health)
    if (input.owner.state === "available") return acquire(input)
    const owner = owned(input)
    if (!owner) return waitOrOwnerFailure(input)
    if (health.status === "running") {
      return {
        action: "open_verify_target",
        credentialSource: "exact_target_manifest",
        build,
        owner,
        process: aliveProof(input),
        health,
        ...reobserve(input),
      }
    }
    return {
      action: "finish_activation",
      credentialSource: "exact_target_manifest",
      build,
      owner,
      process: aliveProof(input),
      health,
      ...reobserve(input),
    }
  }
  if (input.process.state !== "dead") return fail("invalid_recovery_state")
  if (input.owner.state === "available") return acquire(input)
  const owner = owned(input)
  if (!owner) return waitOrOwnerFailure(input)
  return {
    action: "start_retry_target",
    credentialSource: "generate_before_publication",
    build,
    owner,
    process: deadProof(input),
    ...reobserve(input),
  }
}

function reduceReady(
  input: ValidatedIntentInput,
  manifest: ManifestClassification,
  build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof,
): CoordinatorSupervisorRecoveryAction {
  if (build.state === "missing") return fail("target_build_missing_after_acceptance")
  if (manifest !== "target") return fail("intent_manifest_mismatch")
  if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
  if (input.process.state === "dead") return restartPublishedTarget(input, build)
  const health = requireHealth(input, input.intent.targetEpoch, true, true, "verified")
  if (typeof health === "string") return fail(health)
  if (input.owner.state === "available") return acquire(input)
  const owner = owned(input)
  if (!owner) return waitOrOwnerFailure(input)
  return { action: "commit", build, owner, process: aliveProof(input), health, ...reobserve(input) }
}

function reduceCommitted(
  input: ValidatedIntentInput,
  manifest: ManifestClassification,
  build: CoordinatorSupervisorAvailableBuildProof | CoordinatorSupervisorMissingBuildProof,
): CoordinatorSupervisorRecoveryAction {
  if (build.state === "missing") return fail("target_build_missing_after_acceptance")
  if (manifest !== "target") return fail("intent_manifest_mismatch")
  if (!manifestHasState(input, true, true)) return fail("health_state_mismatch")
  if (input.process.state === "dead") return restartPublishedTarget(input, build)
  const health = requireHealth(input, input.intent.targetEpoch, true, true, "verified")
  if (typeof health === "string") return fail(health)
  if (input.owner.state === "available") return acquire(input)
  const owner = owned(input)
  if (!owner) return waitOrOwnerFailure(input)
  return { action: "cleanup", build, owner, process: aliveProof(input), health, ...reobserve(input) }
}

function restartPublishedTarget(input: ValidatedIntentInput, build: CoordinatorSupervisorAvailableBuildProof) {
  if (input.owner.state === "available") return acquire(input)
  const owner = owned(input)
  if (!owner) return waitOrOwnerFailure(input)
  return {
    action: "start_retry_target" as const,
    credentialSource: "exact_target_manifest" as const,
    build,
    owner,
    process: deadProof(input),
    ...reobserve(input),
  }
}

function decodeRecoveryInput(value: unknown): CoordinatorSupervisorRecoveryInput | CoordinatorSupervisorFailReason {
  if (
    !objectWithKeys(
      value,
      ["key", "database", "manifest", "handoff", "owner", "process", "health", "targetBuild"],
      ["key", "database", "manifest", "handoff", "intent", "owner", "process", "health", "targetBuild"],
    )
  ) {
    return "malformed_recovery_input"
  }
  if (typeof value.key !== "string") return "invalid_key"
  if ("intent" in value && value.intent !== undefined && !isCoordinatorSupervisorIntent(value.intent)) {
    return "invalid_intent"
  }
  if (!databaseProof(value.database)) return "malformed_database_proof"
  const manifest = validateManifestEnvelope(value.manifest)
  if (manifest) return manifest
  const handoff = validateHandoffEnvelope(value.handoff)
  if (handoff) return handoff
  if (!validateOwnerEnvelope(value.owner)) return "malformed_owner"
  if (!validateProcessEnvelope(value.process)) return "malformed_process"
  if (!validateHealthEnvelope(value.health)) return "malformed_health"
  if (!validateBuildEnvelope(value.targetBuild)) return "malformed_build"
  return value as CoordinatorSupervisorRecoveryInput
}

function validateManifestEnvelope(value: unknown): CoordinatorSupervisorFailReason | undefined {
  if (!objectWithKeys(value, ["state"], ["state", "kind", "manifest", "database"])) return "malformed_manifest"
  if (value.state === "absent") return Object.keys(value).length === 1 ? undefined : "malformed_manifest"
  if (value.state === "malformed") {
    return Object.keys(value).length === 2 && (value.kind === "malformed" || value.kind === "legacy")
      ? undefined
      : "malformed_manifest"
  }
  if (value.state !== "valid" || !("manifest" in value)) return "malformed_manifest"
  if (Object.keys(value).some((key) => !["state", "manifest", "database"].includes(key))) return "malformed_manifest"
  if ("database" in value && !databaseProof(value.database)) return "malformed_database_proof"
  return isCoordinatorManifest(value.manifest) ? undefined : "malformed_manifest"
}

function validateHandoffEnvelope(value: unknown): CoordinatorSupervisorFailReason | undefined {
  if (!objectWithKeys(value, ["state"], ["state", "handoff"])) return "malformed_handoff"
  if (value.state === "absent" || value.state === "malformed" || value.state === "legacy") {
    return Object.keys(value).length === 1 ? undefined : "malformed_handoff"
  }
  if (value.state !== "valid" || Object.keys(value).length !== 2 || !("handoff" in value)) {
    return "malformed_handoff"
  }
  return isCoordinatorHandoffRecord(value.handoff) ? undefined : "malformed_handoff"
}

function validateOwnerEnvelope(value: unknown): value is CoordinatorSupervisorOwnerObservation {
  if (!objectWithKeys(value, ["state"], ["state", "key", "token", "supervisorPID", "supervisorStartIdentity"])) {
    return false
  }
  if (value.state === "unknown") {
    return (
      Object.keys(value).every((key) => key === "state" || key === "key") &&
      (!("key" in value) || typeof value.key === "string")
    )
  }
  if (value.state === "available" || value.state === "unavailable") {
    return Object.keys(value).length === 2 && typeof value.key === "string"
  }
  return (
    value.state === "owned" &&
    Object.keys(value).length === 5 &&
    typeof value.key === "string" &&
    typeof value.token === "string" &&
    typeof value.supervisorPID === "number" &&
    validStartIdentity(value.supervisorStartIdentity)
  )
}

function validateProcessEnvelope(value: unknown): value is CoordinatorSupervisorProcessObservation {
  if (!objectWithKeys(value, ["state"], ["state", "key", "pid", "startIdentity"])) return false
  if (value.state === "unknown") {
    return (
      Object.keys(value).every((key) => ["state", "key", "pid", "startIdentity"].includes(key)) &&
      (!("key" in value) || typeof value.key === "string") &&
      (!("pid" in value) || typeof value.pid === "number") &&
      (!("startIdentity" in value) || validStartIdentity(value.startIdentity))
    )
  }
  return (
    (value.state === "alive" || value.state === "dead") &&
    Object.keys(value).length === 4 &&
    typeof value.key === "string" &&
    typeof value.pid === "number" &&
    validStartIdentity(value.startIdentity)
  )
}

function validateHealthEnvelope(value: unknown): value is CoordinatorSupervisorHealthObservation {
  if (
    !objectWithKeys(
      value,
      ["state", "key", "pid", "startIdentity"],
      ["state", "key", "pid", "startIdentity", "authorityEpoch", "status", "admission", "ready"],
    )
  ) {
    return false
  }
  if (typeof value.key !== "string" || typeof value.pid !== "number" || !validStartIdentity(value.startIdentity)) {
    return false
  }
  if (value.state === "unhealthy" || value.state === "unavailable") return Object.keys(value).length === 4
  return (
    value.state === "healthy" &&
    Object.keys(value).length === 8 &&
    typeof value.authorityEpoch === "string" &&
    (value.status === "running" || value.status === "verified") &&
    typeof value.admission === "boolean" &&
    typeof value.ready === "boolean"
  )
}

function validateBuildEnvelope(value: unknown): value is CoordinatorSupervisorBuildObservation {
  if (!objectWithKeys(value, ["state"], ["state", "buildID"])) return false
  if (value.state === "unknown") {
    return Object.keys(value).length === 1 || (Object.keys(value).length === 2 && typeof value.buildID === "string")
  }
  return (
    (value.state === "available" || value.state === "missing") &&
    Object.keys(value).length === 2 &&
    typeof value.buildID === "string"
  )
}

function databaseProof(value: unknown): value is { derivedKey: string } {
  return objectWithKeys(value, ["derivedKey"], ["derivedKey"]) && typeof value.derivedKey === "string"
}

function objectWithKeys(value: unknown, required: string[], allowed: string[]): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const keys = Object.keys(value)
  return required.every((key) => keys.includes(key)) && keys.every((key) => allowed.includes(key))
}

function validateObservations(input: CoordinatorSupervisorRecoveryInput): CoordinatorSupervisorFailReason | undefined {
  if (!isCoordinatorKey(input.key)) return "invalid_key"
  if (input.database.derivedKey !== input.key) return "database_key_mismatch"
  if (input.manifest.state === "valid") {
    if (!isCoordinatorManifest(input.manifest.manifest)) return "malformed_manifest"
    if (input.manifest.database && input.manifest.database.derivedKey !== input.key) return "database_key_mismatch"
    if (input.manifest.manifest.key !== input.key) return "proof_key_mismatch"
    if (
      input.manifest.manifest.serverVersion === undefined ||
      input.manifest.manifest.authorityEpoch === undefined ||
      input.manifest.manifest.admission === undefined ||
      input.manifest.manifest.ready === undefined
    ) {
      return "legacy_manifest"
    }
  }
  if (input.handoff.state === "valid" && !isCoordinatorHandoffRecord(input.handoff.handoff)) {
    return "malformed_handoff"
  }
  for (const proofKey of [input.owner.key, input.process.key, input.health.key]) {
    if (proofKey !== undefined && proofKey !== input.key) return "proof_key_mismatch"
  }
  if (input.owner.state === "owned") {
    if (!bounded(input.owner.token) || !pid(input.owner.supervisorPID)) return "owner_ambiguous"
    if (!validStartIdentity(input.owner.supervisorStartIdentity)) return "proof_start_identity_mismatch"
  }
  if (input.process.state === "unknown") return undefined
  if (!pid(input.process.pid) || !pid(input.health.pid)) return "proof_pid_mismatch"
  if (!validStartIdentity(input.process.startIdentity) || !validStartIdentity(input.health.startIdentity)) {
    return "proof_start_identity_mismatch"
  }
  if (input.health.pid !== input.process.pid) return "proof_pid_mismatch"
  if (!sameStartIdentity(input.health.startIdentity, input.process.startIdentity)) {
    return "proof_start_identity_mismatch"
  }
  if (input.manifest.state === "valid" && input.manifest.manifest.pid !== input.process.pid) {
    return "proof_pid_mismatch"
  }
  if (input.process.state === "dead" && input.health.state === "healthy") return "contradictory_process_health"
  if (input.process.state === "alive" && input.health.state !== "healthy") return "live_health_unavailable"
  if (input.targetBuild.state !== "unknown" && !isCoordinatorSupervisorBuildID(input.targetBuild.buildID)) {
    return "target_build_proof_mismatch"
  }
  return undefined
}

function classifyManifest(
  observation: CoordinatorSupervisorManifestObservation,
  intent: CoordinatorSupervisorIntent,
): ManifestClassification {
  if (observation.state === "absent") return "absent"
  if (observation.state !== "valid") return "mismatch"
  const manifest = observation.manifest
  if (
    manifest.version === intent.source.version &&
    manifest.key === intent.source.key &&
    manifest.pid === intent.source.pid &&
    manifest.createdAt === intent.source.createdAt &&
    manifest.serverVersion === intent.source.serverVersion &&
    manifest.authorityEpoch === intent.source.authorityEpoch
  ) {
    return "source"
  }
  if (
    manifest.key === intent.key &&
    manifest.authorityEpoch === intent.targetEpoch &&
    manifest.serverVersion === intent.targetBuildID
  ) {
    return "target"
  }
  return "mismatch"
}

function requireHealth(
  input: ValidatedInput,
  authorityEpoch: string | undefined,
  admission?: boolean,
  ready?: boolean,
  status?: CoordinatorSupervisorHealthyProof["status"],
): CoordinatorSupervisorHealthyProof | CoordinatorSupervisorFailReason {
  if (input.process.state !== "alive" || input.health.state !== "healthy") return "live_health_unavailable"
  if (authorityEpoch === undefined || input.health.authorityEpoch !== authorityEpoch) return "health_epoch_mismatch"
  if (
    (admission !== undefined && input.health.admission !== admission) ||
    (ready !== undefined && input.health.ready !== ready) ||
    (status !== undefined && input.health.status !== status)
  ) {
    return "health_state_mismatch"
  }
  return cloneHealth(input.health)
}

function manifestHasState(input: ValidatedInput, admission: boolean, ready: boolean) {
  return (
    input.manifest.state === "valid" &&
    input.manifest.manifest.admission === admission &&
    input.manifest.manifest.ready === ready
  )
}

function classifyBuild(build: CoordinatorSupervisorBuildObservation, expected: string) {
  if (build.state === "unknown") {
    if (build.buildID !== undefined && build.buildID !== expected) return "mismatch" as const
    return "unknown" as const
  }
  if (build.buildID !== expected) return "mismatch" as const
  return cloneBuild(build)
}

function handoffMatchesIntent(handoff: CoordinatorHandoffRecord, intent: CoordinatorSupervisorIntent) {
  return (
    handoff.request === intent.request &&
    handoff.sourceEpoch === intent.source.authorityEpoch &&
    (handoff.phase === "requested" ? handoff.targetEpoch === undefined : handoff.targetEpoch === intent.targetEpoch)
  )
}

function owned(input: ValidatedInput) {
  if (input.owner.state !== "owned" || input.owner.key !== input.key) return undefined
  return cloneOwner(input.owner)
}

function acquire(input: ValidatedInput): CoordinatorSupervisorRecoveryAction {
  return { action: "acquire_owner", process: processProof(input), ...reobserve(input) }
}

function waitOrOwnerFailure(input: ValidatedInput): CoordinatorSupervisorRecoveryAction {
  if (input.owner.state === "unavailable") return { action: "wait_source", ...fenced(input) }
  if (input.owner.state === "unknown") return fail("owner_ambiguous")
  return fail("owner_required")
}

function fenced(input: ValidatedInput): Fenced {
  return { fence: recoveryFence(input) }
}

function reobserve(input: ValidatedInput): Reobserve {
  return { ...fenced(input), reobserve: true }
}

function recoveryFence(input: ValidatedInput): CoordinatorSupervisorRecoveryFence {
  return {
    key: input.key,
    database: { ...input.database },
    intent: input.intent
      ? { state: "valid", intent: { ...input.intent, source: { ...input.intent.source } } }
      : { state: "absent" },
    manifest:
      input.manifest.state === "valid"
        ? {
            state: "valid",
            manifest: { ...input.manifest.manifest },
          }
        : { state: "absent" },
    handoff:
      input.handoff.state === "valid" ? { state: "valid", handoff: { ...input.handoff.handoff } } : { state: "absent" },
    owner: cloneOwnerObservation(input.owner),
    process: processProof(input),
    health: cloneHealthObservation(input.health),
    targetBuild: cloneBuild(input.targetBuild),
  }
}

function aliveProof(input: ValidatedInput): CoordinatorSupervisorAliveProcessProof {
  if (input.process.state !== "alive") throw new Error("Validated live process proof is unavailable")
  return { ...input.process, state: "alive", startIdentity: { ...input.process.startIdentity } }
}

function deadProof(input: ValidatedInput): CoordinatorSupervisorDeadProcessProof {
  if (input.process.state !== "dead") throw new Error("Validated dead process proof is unavailable")
  return { ...input.process, state: "dead", startIdentity: { ...input.process.startIdentity } }
}

function processProof(input: ValidatedInput): CoordinatorSupervisorProcessProof {
  return { ...input.process, startIdentity: { ...input.process.startIdentity } }
}

function buildProof(input: ValidatedInput) {
  return cloneBuild(input.targetBuild)
}

function cloneOwner(proof: CoordinatorOwnerLockProof): CoordinatorOwnerLockProof {
  return { ...proof, supervisorStartIdentity: { ...proof.supervisorStartIdentity } }
}

function cloneOwnerObservation(proof: CoordinatorSupervisorOwnerObservation): CoordinatorSupervisorOwnerObservation {
  return proof.state === "owned" ? cloneOwner(proof) : { ...proof }
}

function cloneHealth(proof: CoordinatorSupervisorHealthyProof): CoordinatorSupervisorHealthyProof {
  return { ...proof, startIdentity: { ...proof.startIdentity } }
}

function cloneHealthObservation(proof: CoordinatorSupervisorHealthObservation): CoordinatorSupervisorHealthObservation {
  return { ...proof, startIdentity: { ...proof.startIdentity } }
}

function cloneBuild<T extends CoordinatorSupervisorBuildObservation>(proof: T): T {
  return { ...proof }
}

function validStartIdentity(value: unknown): value is CoordinatorStartIdentityProof {
  if (typeof value !== "object" || value === null) return false
  const proof = value as Partial<CoordinatorStartIdentityProof>
  if (proof.state === "unavailable") return Object.keys(value).length === 1
  return proof.state === "available" && bounded(proof.value) && Object.keys(value).length === 2
}

function sameStartIdentity(left: CoordinatorStartIdentityProof, right: CoordinatorStartIdentityProof) {
  if (left.state === "unavailable" || right.state === "unavailable") return left.state === right.state
  return left.value === right.value
}

function bounded(value: unknown) {
  return typeof value === "string" && value.length > 0 && value.length <= 4_096 && !value.includes("\0")
}

function pid(value: unknown) {
  return Number.isSafeInteger(value) && (value as number) > 0
}

function fail(reason: CoordinatorSupervisorFailReason): CoordinatorSupervisorRecoveryAction {
  return { action: "fail_closed", reason }
}
