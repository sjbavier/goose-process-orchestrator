-- Forward-only initial task audit schema. Never edit an applied migration.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    recipe TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('queued','starting','running','succeeded','failed','timed_out','cancelled','interrupted','unknown')),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    cancel_requested_at TIMESTAMP,
    exit_code INTEGER,
    result TEXT NOT NULL DEFAULT '',
    failure TEXT NOT NULL DEFAULT '',
    max_turns INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS tasks_claim_queue ON tasks(status, created_at, id);
