import { COORDINATOR_HANDOFF_VERSION, type CoordinatorHandoffRecord } from "@opencode-ai/sdk/coordinator"
import { compareAndSwapCoordinatorHandoff } from "../../../../src/cli/cmd/tui/coordinator-registry"

const [stateRoot, key, targetEpoch] = process.argv.slice(2)
if (!stateRoot || !key || !targetEpoch) throw new Error("Expected state root, coordinator key, and target epoch")

const requested = {
  version: COORDINATOR_HANDOFF_VERSION,
  request: "request",
  phase: "requested",
  revision: 0,
  sourceEpoch: "source",
  createdAt: "2026-08-18T20:00:00.000Z",
  updatedAt: "2026-08-18T20:00:00.000Z",
} as const satisfies CoordinatorHandoffRecord

const result = await compareAndSwapCoordinatorHandoff(
  key,
  requested,
  {
    ...requested,
    phase: "accepted",
    revision: 1,
    targetEpoch,
    updatedAt: "2026-08-18T20:00:01.000Z",
  },
  stateRoot,
)
process.stdout.write(String(result))
