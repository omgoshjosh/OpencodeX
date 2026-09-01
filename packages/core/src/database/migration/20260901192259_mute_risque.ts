import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260901192259_mute_risque",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`event_cursor_lease\` (
          \`token\` text PRIMARY KEY,
          \`fence\` integer NOT NULL,
          \`expires_at\` integer NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`event_cursor\` (
          \`position\` integer PRIMARY KEY AUTOINCREMENT,
          \`event_id\` text NOT NULL UNIQUE,
          CONSTRAINT \`fk_event_cursor_event_id_event_id_fk\` FOREIGN KEY (\`event_id\`) REFERENCES \`event\`(\`id\`) ON DELETE CASCADE
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
