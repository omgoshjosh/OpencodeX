import { Flag } from "@opencode-ai/core/flag/flag"
import { Effect } from "effect"
import path from "path"

const preserveExerciseGlobalRoot = !!process.env.OPENCODE_HTTPAPI_EXERCISE_GLOBAL
export const exerciseGlobalRoot =
  process.env.OPENCODE_HTTPAPI_EXERCISE_GLOBAL ??
  path.join(process.env.TMPDIR ?? "/tmp", `opencode-httpapi-global-${process.pid}`)
process.env.XDG_DATA_HOME = path.join(exerciseGlobalRoot, "data")
process.env.XDG_CONFIG_HOME = path.join(exerciseGlobalRoot, "config")
process.env.XDG_STATE_HOME = path.join(exerciseGlobalRoot, "state")
process.env.XDG_CACHE_HOME = path.join(exerciseGlobalRoot, "cache")
export const exerciseConfigDirectory = path.join(exerciseGlobalRoot, "config", "opencode")
export const exerciseDataDirectory = path.join(exerciseGlobalRoot, "data", "opencode")

const preserveExerciseDatabase = !!process.env.OPENCODE_HTTPAPI_EXERCISE_DB
export let exerciseDatabasePath =
  process.env.OPENCODE_HTTPAPI_EXERCISE_DB ??
  path.join(process.env.TMPDIR ?? "/tmp", `opencode-httpapi-exercise-${process.pid}.db`)
const exerciseDatabasePaths = [exerciseDatabasePath]
process.env.OPENCODE_DB = exerciseDatabasePath
Flag.OPENCODE_DB = exerciseDatabasePath

export const original = {
  OPENCODE_SERVER_PASSWORD: Flag.OPENCODE_SERVER_PASSWORD,
  OPENCODE_SERVER_USERNAME: Flag.OPENCODE_SERVER_USERNAME,
}

export const resetExerciseDatabase = Effect.promise(async () => {
  // AppRuntime can be reached by individual routes, so keep its old database intact
  // and give the next scenario a fresh database instead of unlinking a live handle.
  exerciseDatabasePath = `${exerciseDatabasePaths[0]}.${exerciseDatabasePaths.length}`
  exerciseDatabasePaths.push(exerciseDatabasePath)
  process.env.OPENCODE_DB = exerciseDatabasePath
  Flag.OPENCODE_DB = exerciseDatabasePath
})

export const cleanupExercisePaths = Effect.promise(async () => {
  if (!preserveExerciseDatabase) {
    await (await import("../../../src/effect/app-runtime")).AppRuntime.dispose().catch(() => undefined)
    const fs = await import("fs/promises")
    await Promise.all(
      exerciseDatabasePaths.flatMap((database) => [database, `${database}-wal`, `${database}-shm`]).map((file) =>
        fs.rm(file, { force: true }).catch(() => undefined),
      ),
    )
  }
  const fs = await import("fs/promises")
  if (!preserveExerciseGlobalRoot)
    await fs.rm(exerciseGlobalRoot, { recursive: true, force: true }).catch(() => undefined)
})
