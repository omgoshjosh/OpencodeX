import { Effect } from "effect"
import { CoordinatorBootstrap } from "@/cli/cmd/tui/coordinator-bootstrap"

const directory = process.argv[2]
const key = process.argv[3]
if (!directory || !key) throw new Error("directory and key are required")

const controller = new AbortController()
process.stdin.once("data", () => controller.abort())
const bootstrap = CoordinatorBootstrap.readCoordinatorBootstrap(true)

const runtime = await import("@/gui-coordinator-runtime")
await Effect.runPromise(runtime.initializeGuiCoordinator())
const { runCoordinator } = await import("@/cli/cmd/tui/coordinator-runner")
await Effect.runPromise(
  runCoordinator({
    directory,
    key,
    bootstrap,
    signal: controller.signal,
    beforeStart: runtime.migrateGuiCoordinatorDatabase(),
  }),
)
