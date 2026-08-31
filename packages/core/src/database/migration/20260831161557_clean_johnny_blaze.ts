import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260831161557_clean_johnny_blaze",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE INDEX \`event_compaction_entity_idx\` ON \`event\` (\`aggregate_id\`,\`type\`,CASE "type"
                WHEN 'message.part.updated.1' THEN json_extract("data", '$.part.id')
                WHEN 'message.updated.1' THEN json_extract("data", '$.info.id')
                ELSE ''
              END,\`seq\`);
      `)
    })
  },
} satisfies DatabaseMigration.Migration
