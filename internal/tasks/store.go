package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/sqlite"
)

var ErrNotFound = errors.New("task not found")
var ErrIdempotencyConflict = errors.New("idempotency key belongs to a different request")

// OpenDB opens the sqlite file at path with the one DSN every caller
// (production and tests) must share: _busy_timeout so a concurrent writer
// (e.g. the scheduler's worker claiming a task) and a concurrent reader
// (e.g. a status poll) wait out a brief lock instead of surfacing
// SQLITE_BUSY immediately. It does not migrate or wrap the result in a
// Store; callers still do that themselves.
func OpenDB(path string) (*sql.DB, error) {
	return sql.Open("sqlite", path+"?_busy_timeout=5000")
}

// Store persists task state through a caller-supplied SQL driver. Opening a
// SQLite driver is deliberately outside C02's dependency-edit boundary.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db, now: func() time.Time { return time.Now().UTC() }}
}

type createResult struct {
	task     Task
	inserted bool
}

// Create inserts a queued task, or returns the original task for an equivalent
// idempotency key. The unique key is enforced by the database schema.
func (s *Store) Create(ctx context.Context, task Task) (Task, bool, error) {
	if task.ID == "" || task.Recipe == "" || task.IdempotencyKey == "" {
		return Task{}, false, errors.New("id, recipe, and idempotency key are required")
	}
	result, err := withBusyRetry(ctx, func() (createResult, error) { return s.createOnce(ctx, task) })
	return result.task, result.inserted, err
}
func (s *Store) createOnce(ctx context.Context, task Task) (createResult, error) {
	at := s.now()
	task.Status, task.CreatedAt, task.UpdatedAt = Queued, at, at
	if task.InboxPolicy == "" {
		task.InboxPolicy = ControllerOnly
	}
	parameters, err := CanonicalParameters(task.Parameters)
	if err != nil {
		return createResult{}, err
	}
	fileParameters, err := CanonicalFileParameters(task.FileParameters)
	if err != nil {
		return createResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return createResult{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tasks (id, recipe, idempotency_key, status, created_at, updated_at, max_turns, parameters, file_parameters, inbox_policy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, task.ID, task.Recipe, task.IdempotencyKey, task.Status, task.CreatedAt, task.UpdatedAt, task.MaxTurns, parameters, fileParameters, task.InboxPolicy)
	if err == nil {
		return createResult{task, true}, tx.Commit()
	}
	existing, getErr := scanTask(tx.QueryRowContext(ctx, taskByKeySQL, task.IdempotencyKey))
	if getErr != nil {
		return createResult{}, err
	}
	// Idempotency equivalence covers the recipe, the canonical parameter
	// encoding, and the normalized inbox policy: replaying the same key with
	// a different policy would otherwise silently return a task whose
	// mailbox behaves differently than the caller just requested.
	existingParameters, err := CanonicalParameters(existing.Parameters)
	if err != nil {
		return createResult{}, err
	}
	existingFileParameters, err := CanonicalFileParameters(existing.FileParameters)
	if err != nil {
		return createResult{}, err
	}
	if existing.Recipe != task.Recipe || existingParameters != parameters || existingFileParameters != fileParameters || existing.InboxPolicy != task.InboxPolicy {
		return createResult{}, ErrIdempotencyConflict
	}
	return createResult{existing, false}, tx.Commit()
}

// withBusyRetry retries fn only for SQLite's explicit busy/locked outcomes,
// with a small bounded exponential backoff; any other error or a zero
// attempts-remaining stop returns immediately. One connection's write commit
// racing a concurrent connection's read is expected and recoverable under
// SQLite's default rollback-journal locking — every store method whose
// write must not be silently lost uses this instead of a single attempt.
func withBusyRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	const attempts = 5
	var zero T
	for attempt := 0; attempt < attempts; attempt++ {
		result, err := fn()
		if err == nil || !sqliteBusyOrLocked(err) || attempt == attempts-1 {
			return result, err
		}
		delay := time.Duration(1<<attempt) * 5 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
	return zero, nil // unreachable; retained for compiler exhaustiveness.
}

// Claim atomically moves one queued task to starting. A nil task means none is available.
func (s *Store) Claim(ctx context.Context) (*Task, error) {
	return withBusyRetry(ctx, func() (*Task, error) { return s.claimOnce(ctx) })
}

func (s *Store) claimOnce(ctx context.Context) (*Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT id FROM tasks WHERE status = ? ORDER BY created_at, id LIMIT 1`, Queued)
	var id string
	if err := row.Scan(&id); errors.Is(err, sql.ErrNoRows) {
		return nil, tx.Commit()
	} else if err != nil {
		return nil, err
	}
	at := s.now()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ?, started_at = ?, updated_at = ? WHERE id = ? AND status = ?`, Starting, at, at, id, Queued)
	if err != nil {
		return nil, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return nil, err
	}
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return nil, err
	}
	return &task, tx.Commit()
}

func sqliteBusyOrLocked(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	// SQLite extended result codes retain the primary code in the low byte.
	switch sqliteErr.Code() & 0xff {
	case 5, 6: // SQLITE_BUSY, SQLITE_LOCKED
		return true
	default:
		return false
	}
}

func (s *Store) Get(ctx context.Context, id string) (Task, error) {
	return scanTask(s.db.QueryRowContext(ctx, taskByIDSQL, id))
}
func (s *Store) Transition(ctx context.Context, id string, next Status, resultText string, exitCode *int) (Task, error) {
	return withBusyRetry(ctx, func() (Task, error) { return s.transitionOnce(ctx, id, next, resultText, exitCode) })
}
func (s *Store) transitionOnce(ctx context.Context, id string, next Status, resultText string, exitCode *int) (Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return Task{}, err
	}
	at := s.now()
	if err := task.Transition(next, at); err != nil {
		return Task{}, err
	}
	task.Result, task.ExitCode = resultText, exitCode
	_, err = tx.ExecContext(ctx, `UPDATE tasks SET status=?, updated_at=?, started_at=?, ended_at=?, result=?, exit_code=? WHERE id=?`, task.Status, task.UpdatedAt, task.StartedAt, task.EndedAt, task.Result, task.ExitCode, id)
	if err != nil {
		return Task{}, err
	}
	return task, tx.Commit()
}
func (s *Store) RequestCancel(ctx context.Context, id string) (Task, error) {
	return withBusyRetry(ctx, func() (Task, error) { return s.requestCancelOnce(ctx, id) })
}
func (s *Store) requestCancelOnce(ctx context.Context, id string) (Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return Task{}, err
	}
	at := s.now()
	if err := task.RequestCancel(at); err != nil {
		return Task{}, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE tasks SET cancel_requested_at=?, updated_at=? WHERE id=?`, task.CancelRequestedAt, task.UpdatedAt, id)
	if err != nil {
		return Task{}, err
	}
	return task, tx.Commit()
}

// ListFilter bounds a List call. A nil Status means no status filter. Limit
// is the exact row cap the caller wants applied; List does not know about
// any server-side maximum policy, so callers must resolve that first.
type ListFilter struct {
	Status    *Status
	KeyPrefix string
	Limit     int
}

// List returns tasks newest first (by created_at, then id, both descending),
// optionally filtered by status, capped at filter.Limit rows.
func (s *Store) List(ctx context.Context, filter ListFilter) ([]Task, error) {
	query := taskColumnsSQL + ` FROM tasks`
	args := make([]any, 0, 3)
	conditions := make([]string, 0, 2)
	if filter.Status != nil {
		conditions = append(conditions, `status = ?`)
		args = append(args, *filter.Status)
	}
	if filter.KeyPrefix != "" {
		conditions = append(conditions, `substr(idempotency_key, 1, length(?)) = ?`)
		args = append(args, filter.KeyPrefix, filter.KeyPrefix)
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, filter.Limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Task, 0)
	for rows.Next() {
		task, err := scanTaskRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

// CountStatuses returns exact durable row counts for the requested statuses.
func (s *Store) CountStatuses(ctx context.Context, statuses ...Status) (map[Status]int, error) {
	counts := make(map[Status]int, len(statuses))
	for _, status := range statuses {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks WHERE status = ?`, status).Scan(&count); err != nil {
			return nil, err
		}
		counts[status] = count
	}
	return counts, nil
}

// ListNonTerminal returns every task row whose status is not one of
// Status.Terminal()'s set (i.e. includes Queued, Starting, Running,
// Interrupted, and Unknown), ordered the same way List already is. This is
// the set restart reconciliation must revisit.
func (s *Store) ListNonTerminal(ctx context.Context) ([]Task, error) {
	query := taskColumnsSQL + ` FROM tasks WHERE status NOT IN (?, ?, ?, ?) ORDER BY created_at DESC, id DESC`
	rows, err := s.db.QueryContext(ctx, query, Succeeded, Failed, TimedOut, Cancelled)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Task, 0)
	for rows.Next() {
		task, err := scanTaskRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

// RecordProcessIdentity persists exactly the OS process identity of a task's
// launched child (pid, pgid, process start ticks) plus updated_at, in one
// transaction, following the store's existing withBusyRetry pattern. It is
// called immediately after a process is started and before the task's
// Running transition, so a surviving child can be re-adopted after an
// orchestrator restart.
func (s *Store) RecordProcessIdentity(ctx context.Context, id string, pid, pgid int, startTicks int64) (Task, error) {
	return withBusyRetry(ctx, func() (Task, error) { return s.recordProcessIdentityOnce(ctx, id, pid, pgid, startTicks) })
}
func (s *Store) recordProcessIdentityOnce(ctx context.Context, id string, pid, pgid int, startTicks int64) (Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	at := s.now()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET pid=?, pgid=?, process_start_ticks=?, updated_at=? WHERE id=?`, pid, pgid, startTicks, at, id)
	if err != nil {
		return Task{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return Task{}, err
	}
	if n != 1 {
		return Task{}, ErrNotFound
	}
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return Task{}, err
	}
	return task, tx.Commit()
}

// RecordOutcomeSummary persists the live-parsed/capture-accounted outcome
// summary (migration 005) for one task, following the store's existing
// withBusyRetry pattern like every other write here. It is independent of
// where the data came from (a periodic flush or the final one) and may be
// called any number of times for the same task; each call overwrites the
// previous summary in full.
func (s *Store) RecordOutcomeSummary(ctx context.Context, id string, summary OutcomeSummary) (Task, error) {
	return withBusyRetry(ctx, func() (Task, error) { return s.recordOutcomeSummaryOnce(ctx, id, summary) })
}
func (s *Store) recordOutcomeSummaryOnce(ctx context.Context, id string, summary OutcomeSummary) (Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET
		final_text=?, final_text_truncated=?, outcome=?, complete_seen=?,
		total_tokens=?, input_tokens=?, output_tokens=?, cache_read_tokens=?, cache_write_tokens=?, cost_usd=?,
		tool_call_count=?, last_event_at=?, stalled_max_turns=?, stdout_truncated=?, bytes_dropped=?
		WHERE id=?`,
		summary.FinalText, summary.FinalTextTruncated, summary.Outcome, summary.CompleteSeen,
		summary.TotalTokens, summary.InputTokens, summary.OutputTokens, summary.CacheReadTokens, summary.CacheWriteTokens, summary.CostUSD,
		summary.ToolCallCount, summary.LastEventAt, summary.StalledMaxTurns, summary.StdoutTruncated, summary.BytesDropped,
		id)
	if err != nil {
		return Task{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return Task{}, err
	}
	if n != 1 {
		return Task{}, ErrNotFound
	}
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return Task{}, err
	}
	return task, tx.Commit()
}

// RecordFailure persists an orchestrator-level failure reason for a task
// that never produced a real process exit code (e.g. Launcher.Start itself
// erroring before any process existed), following the store's existing
// withBusyRetry pattern. It touches only the failure column — never Result,
// status, or any other field.
func (s *Store) RecordFailure(ctx context.Context, id, failure string) (Task, error) {
	return withBusyRetry(ctx, func() (Task, error) { return s.recordFailureOnce(ctx, id, failure) })
}
func (s *Store) recordFailureOnce(ctx context.Context, id, failure string) (Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET failure=? WHERE id=?`, failure, id)
	if err != nil {
		return Task{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return Task{}, err
	}
	if n != 1 {
		return Task{}, ErrNotFound
	}
	task, err := scanTask(tx.QueryRowContext(ctx, taskByIDSQL, id))
	if err != nil {
		return Task{}, err
	}
	return task, tx.Commit()
}

const taskColumnsSQL = `SELECT id, recipe, idempotency_key, status, created_at, updated_at, started_at, ended_at, cancel_requested_at, exit_code, result, failure, max_turns, parameters, file_parameters, pid, pgid, process_start_ticks, inbox_policy,
	final_text, final_text_truncated, outcome, complete_seen, total_tokens, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd,
	tool_call_count, last_event_at, stalled_max_turns, stdout_truncated, bytes_dropped`
const taskByIDSQL = taskColumnsSQL + ` FROM tasks WHERE id = ?`
const taskByKeySQL = taskColumnsSQL + ` FROM tasks WHERE idempotency_key = ?`

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(row *sql.Row) (Task, error) {
	t, err := scanTaskRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	return t, err
}

func scanTaskRow(row rowScanner) (Task, error) {
	var t Task
	var started, ended, cancelled, lastEventAt sql.NullTime
	var exit sql.NullInt64
	var result, failure, parameters, fileParameters, finalText, outcome sql.NullString
	var pid, pgid, startTicks sql.NullInt64
	var finalTextTruncated, completeSeen, stalledMaxTurns, stdoutTruncated sql.NullBool
	var totalTokens, inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens, toolCallCount, bytesDropped sql.NullInt64
	var costUSD sql.NullFloat64
	err := row.Scan(&t.ID, &t.Recipe, &t.IdempotencyKey, &t.Status, &t.CreatedAt, &t.UpdatedAt, &started, &ended, &cancelled, &exit, &result, &failure, &t.MaxTurns, &parameters, &fileParameters, &pid, &pgid, &startTicks, &t.InboxPolicy,
		&finalText, &finalTextTruncated, &outcome, &completeSeen, &totalTokens, &inputTokens, &outputTokens, &cacheReadTokens, &cacheWriteTokens, &costUSD,
		&toolCallCount, &lastEventAt, &stalledMaxTurns, &stdoutTruncated, &bytesDropped)
	if err != nil {
		return Task{}, err
	}
	if pid.Valid {
		v := int(pid.Int64)
		t.PID = &v
	}
	if pgid.Valid {
		v := int(pgid.Int64)
		t.PGID = &v
	}
	if startTicks.Valid {
		v := startTicks.Int64
		t.ProcessStartTicks = &v
	}
	if t.Parameters, err = decodeParameters(parameters.String); err != nil {
		return Task{}, err
	}
	if t.FileParameters, err = decodeFileParameters(fileParameters.String); err != nil {
		return Task{}, err
	}
	if started.Valid {
		t.StartedAt = ptrTime(started.Time)
	}
	if ended.Valid {
		t.EndedAt = ptrTime(ended.Time)
	}
	if cancelled.Valid {
		t.CancelRequestedAt = ptrTime(cancelled.Time)
	}
	if exit.Valid {
		v := int(exit.Int64)
		t.ExitCode = &v
	}
	t.Result, t.Failure = result.String, failure.String
	t.FinalText, t.Outcome = finalText.String, outcome.String
	t.FinalTextTruncated = nullBoolPtr(finalTextTruncated)
	t.CompleteSeen = nullBoolPtr(completeSeen)
	t.StalledMaxTurns = nullBoolPtr(stalledMaxTurns)
	t.StdoutTruncated = nullBoolPtr(stdoutTruncated)
	t.TotalTokens = nullInt64Ptr(totalTokens)
	t.InputTokens = nullInt64Ptr(inputTokens)
	t.OutputTokens = nullInt64Ptr(outputTokens)
	t.CacheReadTokens = nullInt64Ptr(cacheReadTokens)
	t.CacheWriteTokens = nullInt64Ptr(cacheWriteTokens)
	t.ToolCallCount = nullInt64Ptr(toolCallCount)
	t.BytesDropped = nullInt64Ptr(bytesDropped)
	if costUSD.Valid {
		v := costUSD.Float64
		t.CostUSD = &v
	}
	if lastEventAt.Valid {
		t.LastEventAt = ptrTime(lastEventAt.Time)
	}
	if !knownStatus(t.Status) {
		return Task{}, fmt.Errorf("unknown persisted task status %q", t.Status)
	}
	return t, nil
}

func nullBoolPtr(v sql.NullBool) *bool {
	if !v.Valid {
		return nil
	}
	b := v.Bool
	return &b
}
func nullInt64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	n := v.Int64
	return &n
}
func knownStatus(s Status) bool {
	switch s {
	case Queued, Starting, Running, Succeeded, Failed, TimedOut, Cancelled, Interrupted, Unknown:
		return true
	}
	return false
}
