package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// schema is idempotent and applied at every startup.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS taskiq_admin_settings (key text PRIMARY KEY NOT NULL, value text)`,
	// Timestamps are unix milliseconds. JSON columns hold raw JSON text.
	`CREATE TABLE IF NOT EXISTS taskiq_admin_tasks (
		id text PRIMARY KEY NOT NULL,
		name text NOT NULL,
		state text NOT NULL,
		error text,
		worker text,
		execution_time real,
		queued_at integer NOT NULL,
		started_at integer,
		finished_at integer,
		args text,
		kwargs text,
		labels text,
		return_value text,
		attempt integer NOT NULL DEFAULT 0
	)`,
	// listing order / cursor pagination / purge / histogram
	`CREATE INDEX IF NOT EXISTS idx_taskiq_admin_tasks__queued_at ON taskiq_admin_tasks (queued_at, id)`,
	// exact task-name filter and the task types pages
	`CREATE INDEX IF NOT EXISTS idx_taskiq_admin_tasks__name_queued_at ON taskiq_admin_tasks (name, queued_at)`,
	// state filter + list, and the startup "abandoned" sweep
	`CREATE INDEX IF NOT EXISTS idx_taskiq_admin_tasks__state_queued_at ON taskiq_admin_tasks (state, queued_at)`,
	`CREATE INDEX IF NOT EXISTS idx_taskiq_admin_tasks__state_started_at ON taskiq_admin_tasks (state, started_at)`,
	// FTS5 external-content table with the trigram tokenizer: indexed
	// case-insensitive substring search on the task name.
	`CREATE VIRTUAL TABLE IF NOT EXISTS taskiq_admin_tasks_fts USING fts5(name, content='taskiq_admin_tasks', content_rowid='rowid', tokenize='trigram')`,
	`CREATE TRIGGER IF NOT EXISTS trg_taskiq_admin_tasks_fts_ai AFTER INSERT ON taskiq_admin_tasks BEGIN
		INSERT INTO taskiq_admin_tasks_fts(rowid, name) VALUES (new.rowid, new.name); END`,
	`CREATE TRIGGER IF NOT EXISTS trg_taskiq_admin_tasks_fts_ad AFTER DELETE ON taskiq_admin_tasks BEGIN
		INSERT INTO taskiq_admin_tasks_fts(taskiq_admin_tasks_fts, rowid, name) VALUES ('delete', old.rowid, old.name); END`,
	// Every lifecycle event is an UPDATE: only re-index when the name changes.
	`CREATE TRIGGER IF NOT EXISTS trg_taskiq_admin_tasks_fts_au AFTER UPDATE OF name ON taskiq_admin_tasks WHEN old.name IS NOT new.name BEGIN
		INSERT INTO taskiq_admin_tasks_fts(taskiq_admin_tasks_fts, rowid, name) VALUES ('delete', old.rowid, old.name);
		INSERT INTO taskiq_admin_tasks_fts(rowid, name) VALUES (new.rowid, new.name); END`,
	`INSERT OR IGNORE INTO taskiq_admin_settings (key, value) VALUES ('delete_old_ttl_minutes', NULL)`,
}

func createSchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range schema {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%w (in %.60q)", err, stmt)
		}
	}
	return nil
}
