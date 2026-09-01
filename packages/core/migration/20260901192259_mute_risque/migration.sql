CREATE TABLE `event_cursor_lease` (
	`token` text PRIMARY KEY,
	`fence` integer NOT NULL,
	`expires_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `event_cursor` (
	`position` integer PRIMARY KEY AUTOINCREMENT,
	`event_id` text NOT NULL UNIQUE,
	CONSTRAINT `fk_event_cursor_event_id_event_id_fk` FOREIGN KEY (`event_id`) REFERENCES `event`(`id`) ON DELETE CASCADE
);
