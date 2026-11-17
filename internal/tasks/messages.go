package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"
)

// InboxPolicy controls which senders a task's mailbox accepts. It is
// enforced fail-closed at Enqueue time, before any row is written.
type InboxPolicy string

const (
	ControllerOnly InboxPolicy = "controller_only"
	OpenInbox      InboxPolicy = "open"

	// ControllerSender is the fixed sender identity the orchestrator stamps
	// on controller-originated messages. Every other sender is a task id.
	ControllerSender = "controller"
)

// Message bounds. A message body is durable local audit data and becomes
// part of a running agent's context, so the safe set is deliberately
// conservative, mirroring goose.ValidateParameterShape's rationale.
const (
	MaxMessageBodyBytes = 16 << 10
	MaxMessageKindBytes = 64
	MaxSenderBytes      = 128
	// MaxPendingMessages bounds how many undelivered messages one task's
	// inbox may hold, so a sender cannot grow the table unbounded while the
	// recipient is slow to check in.
	MaxPendingMessages = 100
)

var ErrInvalidMessage = errors.New("invalid task message")
var ErrInboxClosed = errors.New("recipient inbox policy rejects this sender")
var ErrInboxFull = errors.New("recipient inbox is full")

// Message is one durable row in task_messages. It is append-only: nothing
// ever updates Sender/Kind/Body after insertion, only DeliveredAt.
type Message struct {
	ID          int64
	TaskID      string
	Sender      string
	Kind        string
	Body        string
	CreatedAt   time.Time
	DeliveredAt *time.Time
}

// KnownInboxPolicy reports whether p is one of the enforced InboxPolicy
// values. It is exported so callers outside this package (spawn-time input
// validation) can reject an unknown policy before any row is written.
func KnownInboxPolicy(p InboxPolicy) bool { return p == ControllerOnly || p == OpenInbox }

// Enqueue validates and stores one message for taskID's inbox. Shape
// validation runs before any database access; the recipient's inbox_policy
// and pending-quota checks run inside the same transaction as the insert, so
// a rejected message never reaches storage and a concurrent enqueue cannot
// race past the quota.
func (s *Store) Enqueue(ctx context.Context, taskID, sender, kind, body string) (Message, error) {
	if err := validateMessageShape(sender, kind, body); err != nil {
		return Message{}, err
	}
	return withBusyRetry(ctx, func() (Message, error) { return s.enqueueOnce(ctx, taskID, sender, kind, body) })
}

func (s *Store) enqueueOnce(ctx context.Context, taskID, sender, kind, body string) (Message, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback()

	policy, err := queryInboxPolicy(ctx, tx, taskID)
	if err != nil {
		return Message{}, err
	}
	if policy == ControllerOnly && sender != ControllerSender {
		return Message{}, fmt.Errorf("%w: task %q accepts messages only from %q", ErrInboxClosed, taskID, ControllerSender)
	}

	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_messages WHERE task_id = ? AND delivered_at IS NULL`, taskID).Scan(&pending); err != nil {
		return Message{}, err
	}
	if pending >= MaxPendingMessages {
		return Message{}, fmt.Errorf("%w: task %q already has %d undelivered messages", ErrInboxFull, taskID, pending)
	}

	at := s.now()
	result, err := tx.ExecContext(ctx, `INSERT INTO task_messages (task_id, sender, kind, body, created_at) VALUES (?, ?, ?, ?, ?)`, taskID, sender, kind, body, at)
	if err != nil {
		return Message{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Message{}, err
	}
	msg := Message{ID: id, TaskID: taskID, Sender: sender, Kind: kind, Body: body, CreatedAt: at}
	return msg, tx.Commit()
}

// ReadInbox returns every undelivered message for taskID, oldest first, and
// marks each one delivered at the same timestamp in the same transaction —
// delivery is recorded exactly once per read. A nil/empty slice means an
// empty inbox, not an error.
func (s *Store) ReadInbox(ctx context.Context, taskID string) ([]Message, error) {
	return withBusyRetry(ctx, func() ([]Message, error) { return s.readInboxOnce(ctx, taskID) })
}

func (s *Store) readInboxOnce(ctx context.Context, taskID string) ([]Message, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id, task_id, sender, kind, body, created_at FROM task_messages WHERE task_id = ? AND delivered_at IS NULL ORDER BY id ASC`, taskID)
	if err != nil {
		return nil, err
	}
	var messages []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.TaskID, &m.Sender, &m.Kind, &m.Body, &m.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		messages = append(messages, m)
	}
	closeErr := rows.Err()
	rows.Close()
	if closeErr != nil {
		return nil, closeErr
	}
	if len(messages) == 0 {
		return nil, tx.Commit()
	}

	at := s.now()
	for i := range messages {
		if _, err := tx.ExecContext(ctx, `UPDATE task_messages SET delivered_at = ? WHERE id = ?`, at, messages[i].ID); err != nil {
			return nil, err
		}
		messages[i].DeliveredAt = ptrTime(at)
	}
	return messages, tx.Commit()
}

// InboxPolicy returns the persisted inbox_policy for taskID.
func (s *Store) InboxPolicy(ctx context.Context, taskID string) (InboxPolicy, error) {
	return queryInboxPolicy(ctx, s.db, taskID)
}

// sqlQueryRower is satisfied by both *sql.DB and *sql.Tx.
type sqlQueryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryInboxPolicy(ctx context.Context, q sqlQueryRower, taskID string) (InboxPolicy, error) {
	var policy InboxPolicy
	err := q.QueryRowContext(ctx, `SELECT inbox_policy FROM tasks WHERE id = ?`, taskID).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if !KnownInboxPolicy(policy) {
		return "", fmt.Errorf("unknown persisted inbox policy %q for task %q", policy, taskID)
	}
	return policy, nil
}

func validateMessageShape(sender, kind, body string) error {
	if sender == "" {
		return fmt.Errorf("%w: sender is required", ErrInvalidMessage)
	}
	if err := validateMessageField("sender", sender, MaxSenderBytes); err != nil {
		return err
	}
	if err := validateMessageField("kind", kind, MaxMessageKindBytes); err != nil {
		return err
	}
	if body == "" {
		return fmt.Errorf("%w: body is required", ErrInvalidMessage)
	}
	if err := validateMessageField("body", body, MaxMessageBodyBytes); err != nil {
		return err
	}
	return nil
}

// validateMessageField enforces the same bounded-and-printable shape rule
// for every text field: a non-empty value already checked by the caller
// must stay within its byte budget, be valid UTF-8, and carry no control or
// line-terminator character, so it can never desynchronize framing in a
// downstream consumer that treats messages as line-oriented text.
func validateMessageField(name, value string, maxBytes int) error {
	if len(value) > maxBytes {
		return fmt.Errorf("%w: %s is %d bytes, exceeding the maximum of %d", ErrInvalidMessage, name, len(value), maxBytes)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrInvalidMessage, name)
	}
	for _, r := range value {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return fmt.Errorf("%w: %s contains the disallowed control character %U", ErrInvalidMessage, name, r)
		}
	}
	return nil
}
