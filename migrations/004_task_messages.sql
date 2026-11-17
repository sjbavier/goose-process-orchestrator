-- Forward-only: add the append-only per-task message mailbox and the
-- tasks.inbox_policy column that gates who may enqueue into it. Never edit
-- an applied migration.
--
-- task_messages.id is a monotonically increasing surrogate key, never reused,
-- so a later subscription layer can use "messages after id N" as a cursor.
-- sender is either the literal 'controller' or another task's id (task-id
-- addressing, hub-and-spoke only); delivered_at is NULL until the recipient
-- reads it, then set exactly once. kind is short free text so typed events
-- can layer on later without a migration.
--
-- inbox_policy defaults to 'controller_only', so every task row that existed
-- before this migration gets the same restriction a newly created one would,
-- with no backfill needed. 'open' lets an operator allow agent-to-agent
-- messages into one task's inbox.
--
-- Message bodies are durable local audit data, like task parameters: never
-- put secrets in a message body.
CREATE TABLE IF NOT EXISTS task_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    sender TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS task_messages_inbox ON task_messages(task_id, delivered_at, id);

ALTER TABLE tasks ADD COLUMN inbox_policy TEXT NOT NULL DEFAULT 'controller_only';
