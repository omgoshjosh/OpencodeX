CREATE INDEX `event_compaction_entity_idx` ON `event` (`aggregate_id`,`type`,CASE "type"
        WHEN 'message.part.updated.1' THEN json_extract("data", '$.part.id')
        WHEN 'message.updated.1' THEN json_extract("data", '$.info.id')
        ELSE ''
      END,`seq`);