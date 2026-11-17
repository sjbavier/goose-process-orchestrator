package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"modernc.org/sqlite"
)

var ErrNotFound = errors.New("task not found")
var ErrIdempotencyConflict = errors.New("idempotency key belongs to a different request")

// Store persists task state through a caller-supplied SQL driver. Opening a
// SQLite driver is deliberately outside C02's dependency-edit boundary.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

func NewStore(db *sql.DB) *Store { return &Store{db: db, now: time.Now().UTC} }

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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return createResult{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tasks (id, recipe, idempotency_key, status, created_at, updated_at, max_turns, parameters, inbox_policy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, task.ID, task.Recipe, task.IdempotencyKey, task.Status, task.CreatedAt, task.UpdatedAt, task.MaxTurns, parameters, task.InboxPolicy)
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
	if existing.Recipe != task.Recipe || existingParameters != parameters || existing.InboxPolicy != task.InboxPolicy {
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
	Status *Status
	Limit  int
}

// List returns tasks newest first (by created_at, then id, both descending),
// optionally filtered by status, capped at filter.Limit rows.
func (s *Store) List(ctx context.Context, filter ListFilter) ([]Task, error) {
	query := taskColumnsSQL + ` FROM tasks`
	args := make([]any, 0, 2)
	if filter.Status != nil {
		query += ` WHERE status = ?`
		args = append(args, *filter.Status)
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

const taskColumnsSQL = `SELECT id, recipe, idempotency_key, status, created_at, updated_at, started_at, ended_at, cancel_requested_at, exit_code, result, failure, max_turns, parameters, pid, pgid, process_start_ticks, inbox_policy`
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
	var started, ended, cancelled sql.NullTime
	var exit sql.NullInt64
	var result, failure, parameters sql.NullString
	var pid, pgid, startTicks sql.NullInt64
	err := row.Scan(&t.ID, &t.Recipe, &t.IdempotencyKey, &t.Status, &t.CreatedAt, &t.UpdatedAt, &started, &ended, &cancelled, &exit, &result, &failure, &t.MaxTurns, &parameters, &pid, &pgid, &startTicks, &t.InboxPolicy)
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
	if !knownStatus(t.Status) {
		return Task{}, fmt.Errorf("unknown persisted task status %q", t.Status)
	}
	return t, nil
}
func knownStatus(s Status) bool {
	switch s {
	case Queued, Starting, Running, Succeeded, Failed, TimedOut, Cancelled, Interrupted, Unknown:
		return true
	}
	return false
}
