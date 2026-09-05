import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260905092002_groovy_meteorite",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`opencodex_session_state\` ADD \`marked_unread_at\` integer;`)
    })
  },
} satisfies DatabaseMigration.Migration
