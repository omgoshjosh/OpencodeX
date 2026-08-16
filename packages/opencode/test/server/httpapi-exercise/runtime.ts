export type Runtime = {
  PublicApi: (typeof import("../../../src/server/routes/instance/httpapi/public"))["PublicApi"]
  HttpApiApp: (typeof import("../../../src/server/routes/instance/httpapi/server"))["HttpApiApp"]
  AppLayer: (typeof import("../../../src/effect/app-runtime"))["AppLayer"]
  memoMap: (typeof import("@opencode-ai/core/effect/memo-map"))["memoMap"]
  InstanceRef: (typeof import("../../../src/effect/instance-ref"))["InstanceRef"]
  InstanceStore: (typeof import("../../../src/project/instance-store"))["InstanceStore"]
  Session: (typeof import("../../../src/session/session"))["Session"]
  Todo: (typeof import("../../../src/session/todo"))["Todo"]
  Worktree: (typeof import("../../../src/worktree"))["Worktree"]
  Project: (typeof import("../../../src/project/project"))["Project"]
  Tui: typeof import("../../../src/server/shared/tui-control")
  disposeAllInstances: (typeof import("../../fixture/fixture"))["disposeAllInstances"]
  tmpdir: (typeof import("../../fixture/fixture"))["tmpdir"]
  resetDatabase: () => Promise<void>
}

let runtimePromise: Promise<Runtime> | undefined

export function runtime() {
  return (runtimePromise ??= (async () => {
    const publicApi = await import("../../../src/server/routes/instance/httpapi/public")
    const httpApiServer = await import("../../../src/server/routes/instance/httpapi/server")
    const appRuntime = await import("../../../src/effect/app-runtime")
    const memoMap = await import("@opencode-ai/core/effect/memo-map")
    const instanceRef = await import("../../../src/effect/instance-ref")
    const instanceStore = await import("../../../src/project/instance-store")
    const session = await import("../../../src/session/session")
    const todo = await import("../../../src/session/todo")
    const worktree = await import("../../../src/worktree")
    const project = await import("../../../src/project/project")
    const tui = await import("../../../src/server/shared/tui-control")
    const fixture = await import("../../fixture/fixture")
    const database = await import("@opencode-ai/core/database/database")
    const drizzle = await import("drizzle-orm")
    const effect = await import("effect")
    return {
      PublicApi: publicApi.PublicApi,
      HttpApiApp: httpApiServer.HttpApiApp,
      AppLayer: appRuntime.AppLayer,
      memoMap: memoMap.memoMap,
      InstanceRef: instanceRef.InstanceRef,
      InstanceStore: instanceStore.InstanceStore,
      Session: session.Session,
      Todo: todo.Todo,
      Worktree: worktree.Worktree,
      Project: project.Project,
      Tui: tui,
      disposeAllInstances: fixture.disposeAllInstances,
      tmpdir: fixture.tmpdir,
      resetDatabase: () =>
        appRuntime.AppRuntime.runPromise(
          database.Database.Service.use(({ db }) =>
            effect.Effect.gen(function* () {
              const tables = yield* db.all<{ name: string }>(
                drizzle.sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
              )
              yield* db.run("PRAGMA foreign_keys = OFF")
              yield* effect.Effect.forEach(
                tables.filter((table) => table.name !== "migration" && table.name !== "__drizzle_migrations"),
                (table) => db.run(drizzle.sql`DELETE FROM ${drizzle.sql.identifier(table.name)}`),
                { discard: true },
              ).pipe(effect.Effect.ensuring(db.run("PRAGMA foreign_keys = ON").pipe(effect.Effect.ignore)))
            }),
          ),
        ),
    }
  })())
}
