-- Forward-only: add persisted process-identity columns for restart
-- reconciliation. Never edit an applied migration.
--
-- All three columns are nullable: NULL means "no process identity recorded",
-- which is already the correct meaning for every row created before this
-- migration and for any task that never reached Running (e.g. failed before
-- exec). No backfill is needed.
--
-- pid/pgid are the OS identifiers already implicit in supervisor.Process/
-- exec.Cmd today, just not persisted. process_start_ticks is the Linux
-- /proc/<pid>/stat field 22 (starttime), stored verbatim as the integer
-- clock-tick count since boot that the kernel reports, so reconciliation can
-- do a cheap, exact, same-host integer comparison instead of a lossy
-- derived wall-clock conversion.
ALTER TABLE tasks ADD COLUMN pid INTEGER;
ALTER TABLE tasks ADD COLUMN pgid INTEGER;
ALTER TABLE tasks ADD COLUMN process_start_ticks INTEGER;
