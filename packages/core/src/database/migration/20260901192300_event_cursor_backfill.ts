import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260901192300_event_cursor_backfill",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        INSERT OR IGNORE INTO \`event_cursor\` (\`event_id\`)
        SELECT \`id\` FROM \`event\` ORDER BY \`rowid\`;
      `)
      yield* tx.run(`
        CREATE TRIGGER \`event_cursor_insert\`
        AFTER INSERT ON \`event\`
        BEGIN
          INSERT INTO \`event_cursor\` (\`event_id\`) VALUES (NEW.\`id\`);
        END;
      `)
    })
  },
} satisfies DatabaseMigration.Migration
