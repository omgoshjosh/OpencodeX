import { sql } from "drizzle-orm"
import { sqliteTable, text, integer, index } from "drizzle-orm/sqlite-core"
import type { EventV2 } from "../event"

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  owner_id: text(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<EventV2.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    // Journal replay and cascade deletion are aggregate-local.
    index("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    // Retention asks whether a revision has an older and newer sibling. This
    // expression keeps that lookup indexed without coupling the generic event
    // row to session-specific entity columns.
    index("event_compaction_entity_idx").on(
      table.aggregate_id,
      table.type,
      sql`CASE ${table.type}
        WHEN 'message.part.updated.1' THEN json_extract(${table.data}, '$.part.id')
        WHEN 'message.updated.1' THEN json_extract(${table.data}, '$.info.id')
        ELSE ''
      END`,
      table.seq,
    ),
  ],
)
