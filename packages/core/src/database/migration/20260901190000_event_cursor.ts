import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260901190000_event_cursor",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`event_cursor\` (
          \`position\` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
          \`event_id\` text NOT NULL UNIQUE REFERENCES \`event\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`
        INSERT OR IGNORE INTO \`event_cursor\` (\`event_id\`)
        SELECT \`id\` FROM \`event\` ORDER BY \`rowid\`;
      `)
      yield* tx.run(`
        CREATE TRIGGER IF NOT EXISTS \`event_cursor_insert\`
        AFTER INSERT ON \`event\`
        BEGIN
          INSERT INTO \`event_cursor\` (\`event_id\`) VALUES (NEW.\`id\`);
        END;
      `)
      yield* tx.run(`
        CREATE TABLE IF NOT EXISTS \`event_cursor_lease\` (
          \`token\` text PRIMARY KEY NOT NULL,
          \`fence\` integer NOT NULL,
          \`expires_at\` integer NOT NULL
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
