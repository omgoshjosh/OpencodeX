import path from "path"
import os from "os"
import { Effect, Option, Schema } from "effect"
import * as Stream from "effect/Stream"
import { InstanceState } from "@/effect/instance-state"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Ripgrep } from "../file/ripgrep"
import { assertExternalDirectoryEffect } from "./external-directory"
import DESCRIPTION from "./glob.txt"
import * as Tool from "./tool"
import { Reference } from "@/reference/reference"
import { Config } from "@/config/config"
import { SEARCH_TOOL_DEADLINE_MS, searchTimeoutNotice, withSearchDeadline } from "./file-deadline"

export const Parameters = Schema.Struct({
  pattern: Schema.String.annotate({ description: "The glob pattern to match files against" }),
  path: Schema.optional(Schema.String).annotate({
    description: `The directory to search in. If not specified, the current working directory will be used. IMPORTANT: Omit this field to use the default directory. DO NOT enter "undefined" or "null" - simply omit it for the default behavior. Must be a valid directory path if provided.`,
  }),
})

export const GlobTool = Tool.define(
  "glob",
  Effect.gen(function* () {
    const rg = yield* Ripgrep.Service
    const fs = yield* AppFileSystem.Service
    const reference = yield* Reference.Service
    const config = yield* Config.Service

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: { pattern: string; path?: string }, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const ins = yield* InstanceState.context
          yield* ctx.ask({
            permission: "glob",
            patterns: [params.pattern],
            always: ["*"],
            metadata: {
              pattern: params.pattern,
              path: params.path,
            },
          })

          const cfg = yield* config.get()
          const timeoutMs = cfg.experimental?.search_timeout ?? SEARCH_TOOL_DEADLINE_MS

          const limit = 100
          const files: { path: string; mtime: number }[] = []
          let searchRoot = ins.directory

          // Reads the accumulator, so it renders a completed search and a search
          // stopped at the deadline the same way.
          const render = (timedOut: boolean) => {
            const truncated = files.length > limit
            if (truncated) files.length = limit
            files.sort((a, b) => b.mtime - a.mtime)

            const output = []
            if (files.length === 0) output.push("No files found")
            if (files.length > 0) output.push(...files.map((file) => file.path))
            if (truncated) {
              output.push("")
              output.push(
                `(Results are truncated: showing first ${limit} results. Consider using a more specific path or pattern.)`,
              )
            }
            if (timedOut) {
              output.push("")
              output.push(searchTimeoutNotice(timeoutMs, files.length, "files"))
            }

            return {
              title: path.relative(ins.worktree, searchRoot),
              metadata: {
                count: files.length,
                truncated,
                timedOut,
              },
              output: output.join("\n"),
            }
          }

          return yield* withSearchDeadline(
            "glob",
            ctx.abort,
            timeoutMs,
            (signal) =>
              Effect.gen(function* () {
                const requested = params.path ?? ins.directory
                const search = yield* fs
                  .realPath(path.isAbsolute(requested) ? requested : path.resolve(ins.directory, requested))
                  .pipe(Effect.catch(() => Effect.succeed(path.resolve(ins.directory, requested))))
                const home = yield* fs.realPath(os.homedir()).pipe(Effect.catch(() => Effect.succeed(os.homedir())))
                if (search === path.parse(search).root || search === home) {
                  return yield* Effect.fail(
                    new Error("glob path must be narrower than the filesystem or home directory"),
                  )
                }
                yield* reference.ensure(search)
                const info = yield* fs.stat(search).pipe(Effect.catch(() => Effect.succeed(undefined)))
                if (info?.type === "File") {
                  return yield* Effect.fail(new Error(`glob path must be a directory: ${search}`))
                }
                yield* assertExternalDirectoryEffect(ctx, search, {
                  bypass: yield* reference.contains(search),
                  kind: "directory",
                })
                searchRoot = search

                yield* rg.files({ cwd: search, glob: [params.pattern], signal }).pipe(
                  Stream.take(limit + 1),
                  Stream.runForEach((file) =>
                    Effect.gen(function* () {
                      const full = path.resolve(search, file)
                      const info = yield* fs.stat(full).pipe(Effect.catch(() => Effect.succeed(undefined)))
                      const mtime =
                        info?.mtime.pipe(
                          Option.map((date) => date.getTime()),
                          Option.getOrElse(() => 0),
                        ) ?? 0
                      files.push({ path: full, mtime })
                    }),
                  ),
                )

                return render(false)
              }),
            () => render(true),
          )
        }).pipe(Effect.orDie),
    }
  }),
)
