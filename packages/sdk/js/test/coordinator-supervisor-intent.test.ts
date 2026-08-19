import { describe, expect, test } from "bun:test"
import {
  COORDINATOR_MANIFEST_VERSION,
  coordinatorHandoffRequestID,
  coordinatorSourceManifestFence,
  isCoordinatorSupervisorBuildID,
  isCoordinatorSupervisorIntent,
  parseCoordinatorSupervisorIntent,
  sameCoordinatorSourceManifestFence,
  serializeCoordinatorSupervisorIntent,
  type CoordinatorManifest,
  type CoordinatorSupervisorIntent,
} from "../src/coordinator/manifest"

function manifest(overrides: Partial<CoordinatorManifest> = {}): CoordinatorManifest {
  return {
    version: COORDINATOR_MANIFEST_VERSION,
    key: "a".repeat(40),
    directory: "/work/project",
    database: "/data/coordinator.sqlite",
    pid: 1234,
    url: "http://127.0.0.1:4096/",
    username: "secret-user",
    password: "secret-password",
    token: "secret-token",
    createdAt: "2026-08-18T20:00:00.000Z",
    serverVersion: "build-source-1",
    authorityEpoch: "source-epoch-0001",
    admission: true,
    ready: true,
    ...overrides,
  }
}

function intent(overrides: Partial<CoordinatorSupervisorIntent> = {}): CoordinatorSupervisorIntent {
  const source = coordinatorSourceManifestFence(manifest())
  const targetEpoch = "target-epoch-0001"
  return {
    version: 1,
    key: source.key,
    source: {
      ...source,
      serverVersion: source.serverVersion!,
      authorityEpoch: source.authorityEpoch!,
    },
    targetEpoch,
    request: coordinatorHandoffRequestID(source.authorityEpoch!, targetEpoch),
    targetBuildID: "build-target-1",
    revision: 0,
    createdAt: "2026-08-18T20:00:01.000Z",
    updatedAt: "2026-08-18T20:00:01.000Z",
    ...overrides,
  }
}

describe("coordinator supervisor intent pure schema", () => {
  test("round-trips strict bounded deterministic values", () => {
    const value = intent()
    expect(isCoordinatorSupervisorIntent(value)).toBe(true)
    expect(parseCoordinatorSupervisorIntent(serializeCoordinatorSupervisorIntent(value))).toEqual(value)
    expect(isCoordinatorSupervisorIntent({ ...value, request: "0".repeat(64) })).toBe(false)
    expect(
      isCoordinatorSupervisorIntent({
        ...value,
        targetEpoch: value.source.authorityEpoch,
        request: coordinatorHandoffRequestID(value.source.authorityEpoch, value.source.authorityEpoch),
      }),
    ).toBe(false)
    expect(isCoordinatorSupervisorIntent({ ...value, revision: -1 })).toBe(false)
    expect(isCoordinatorSupervisorIntent({ ...value, extra: true })).toBe(false)
    expect(() => parseCoordinatorSupervisorIntent("x".repeat(16_385))).toThrow("exceeds maximum size")
    expect(() =>
      parseCoordinatorSupervisorIntent(
        JSON.stringify({
          ...value,
          targetEpoch: value.source.authorityEpoch,
          request: coordinatorHandoffRequestID(value.source.authorityEpoch, value.source.authorityEpoch),
        }),
      ),
    ).toThrow("Invalid coordinator supervisor intent")
    expect(() =>
      serializeCoordinatorSupervisorIntent({ ...value, extra: true } as CoordinatorSupervisorIntent),
    ).toThrow("Invalid coordinator supervisor intent")
  })

  test("contains no credential, capability, URL, executable, argv, or filesystem path fields", () => {
    const value = intent()
    for (const mutation of [
      { ...value, database: "/forbidden.sqlite" },
      { ...value, directory: "/forbidden" },
      { ...value, capability: "forbidden" },
      { ...value, url: "http://forbidden" },
      { ...value, argv: ["--secret"] },
      { ...value, executable: "/bin/forbidden" },
      { ...value, source: { ...value.source, token: "forbidden" } },
      { ...value, source: { ...value.source, username: "forbidden" } },
      { ...value, source: { ...value.source, password: "forbidden" } },
    ]) {
      expect(isCoordinatorSupervisorIntent(mutation)).toBe(false)
    }

    const serialized = serializeCoordinatorSupervisorIntent(value)
    for (const forbidden of [
      "username",
      "password",
      "token",
      "capability",
      "database",
      "directory",
      "url",
      "argv",
      "executable",
      "/data/coordinator.sqlite",
    ]) {
      expect(serialized).not.toContain(forbidden)
    }
    expect(sameCoordinatorSourceManifestFence({ ...manifest(), database: "/other.sqlite" }, value.source)).toBe(true)
    expect(sameCoordinatorSourceManifestFence({ ...manifest(), pid: 9999 }, value.source)).toBe(false)
  })

  test("rejects paths and traversal in all persisted opaque identifiers", () => {
    for (const value of [".", "..", "/tmp/target", "..\\target", "build/target", "", "x".repeat(257)]) {
      expect(isCoordinatorSupervisorBuildID(value)).toBe(false)
      expect(isCoordinatorSupervisorIntent({ ...intent(), targetBuildID: value })).toBe(false)
    }
    expect(isCoordinatorSupervisorIntent({ ...intent(), targetEpoch: "/private/epoch" })).toBe(false)
    expect(
      isCoordinatorSupervisorIntent({
        ...intent(),
        source: { ...intent().source, authorityEpoch: "..\\private" },
      }),
    ).toBe(false)
    expect(isCoordinatorSupervisorBuildID("release-1.2.3+darwin_arm64")).toBe(true)
  })
})
