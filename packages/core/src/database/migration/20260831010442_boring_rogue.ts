import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260831010442_boring_rogue",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`ALTER TABLE \`opencodex_swarm_role\` ADD \`fallback_models\` text;`)
    })
  },
} satisfies DatabaseMigration.Migration
