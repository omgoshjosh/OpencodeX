import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260829000000_durable_adaptive_steering",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run("ALTER TABLE `session_command` ADD `adopted_by` text;")
      yield* tx.run("ALTER TABLE `session_command` ADD `adopted_generation` integer;")
      yield* tx.run("ALTER TABLE `session_command` ADD `offer_ordinal` integer;")
      yield* tx.run("ALTER TABLE `session_command` ADD `offered_at` integer;")
    })
  },
} satisfies DatabaseMigration.Migration
