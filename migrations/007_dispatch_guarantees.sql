-- Forward-only: add dispatch-guarantee audit columns (recipe hash, effective
-- provider/model/goose version, spawn expectations, per-task timeout, and
-- leftover-process reporting). Never edit an applied migration.
--
-- Empty string / 0 / NULL all mean "absent", which keeps every row created
-- before this migration reading back unchanged:
--   recipe_sha256, provider, model, goose_version: '' = never recorded.
--   expect_*: '' = caller set no expectation.
--   timeout_seconds: 0 = no per-task timeout.
--   leftover_pids: NULL = never scanned; otherwise a JSON array of ints
--     (possibly '[]'). leftover_detection: '' = never scanned, else 'ok' or
--     'unavailable'. An unavailable scan must never be read as "no leftovers".
ALTER TABLE tasks ADD COLUMN recipe_sha256 TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN provider TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN model TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN goose_version TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN expect_recipe_sha256 TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN expect_provider TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN expect_model TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN leftover_pids TEXT;
ALTER TABLE tasks ADD COLUMN leftover_detection TEXT NOT NULL DEFAULT '';
