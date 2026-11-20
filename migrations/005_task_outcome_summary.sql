-- Forward-only: add the task outcome summary columns that let `result`/
-- `status`/`progress` report what actually happened, surviving both the
-- stdout capture limit and an orchestrator restart. Never edit an applied
-- migration.
--
-- All columns are nullable with no backfill: NULL means "no live summary was
-- ever flushed for this row" (every row created before this migration, and
-- any task re-adopted after a restart, since reconciliation never attaches a
-- live parser). `result`/`status` must report that absence honestly, never
-- guess a value from it.
--
-- final_text/final_text_truncated mirror the live-reassembled last assistant
-- message, bounded the same way progress.last_assistant_text already is.
-- outcome is one of 'completed', 'stalled_max_turns', 'no_complete_event',
-- 'process_failed', or NULL (absent); never derived lazily elsewhere.
-- complete_seen/stalled_max_turns are 0/1 flags, not booleans, matching this
-- schema's existing convention (e.g. cancel_requested_at's 0/1-shaped peers).
-- tool_call_count/last_event_at come from the live incremental parser
-- (chunk C1). stdout_truncated/bytes_dropped come from the disk capture's
-- own accounting (chunk C2), independent of the live parser.
ALTER TABLE tasks ADD COLUMN final_text TEXT;
ALTER TABLE tasks ADD COLUMN final_text_truncated INTEGER;
ALTER TABLE tasks ADD COLUMN outcome TEXT;
ALTER TABLE tasks ADD COLUMN complete_seen INTEGER;
ALTER TABLE tasks ADD COLUMN total_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN input_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN output_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN cache_read_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN cache_write_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN cost_usd REAL;
ALTER TABLE tasks ADD COLUMN tool_call_count INTEGER;
ALTER TABLE tasks ADD COLUMN last_event_at TIMESTAMP;
ALTER TABLE tasks ADD COLUMN stalled_max_turns INTEGER;
ALTER TABLE tasks ADD COLUMN stdout_truncated INTEGER;
ALTER TABLE tasks ADD COLUMN bytes_dropped INTEGER;
