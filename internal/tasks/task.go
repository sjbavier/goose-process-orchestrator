// Package tasks defines the durable task lifecycle and database-backed store contract.
package tasks

import (
	"encoding/json"
	"fmt"
	"time"
)

// Status describes a persisted task lifecycle state.
type Status string

const (
	Queued      Status = "queued"
	Starting    Status = "starting"
	Running     Status = "running"
	Succeeded   Status = "succeeded"
	Failed      Status = "failed"
	TimedOut    Status = "timed_out"
	Cancelled   Status = "cancelled"
	Interrupted Status = "interrupted"
	Unknown     Status = "unknown"
)

// Task is the durable audit record for one allowlisted recipe request. Result and
// exit fields are intentionally data only; process launch belongs to a later chunk.
type Task struct {
	ID                string
	Recipe            string
	IdempotencyKey    string
	Status            Status
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StartedAt         *time.Time
	EndedAt           *time.Time
	CancelRequestedAt *time.Time
	ExitCode          *int
	Result            string
	Failure           string
	// MaxTurns overrides the server-wide --max-turns default for this one
	// task when positive; zero means "use the server default."
	MaxTurns int
	// Parameters holds the declared recipe parameter map for this request.
	// It is durable local audit data and part of idempotency equivalence, and
	// it must never carry secrets. A nil or empty map means "no parameters"
	// and is equivalent to a task created before parameters existed.
	Parameters map[string]string
	// InboxPolicy controls which senders this task's mailbox accepts. An
	// empty value means "use the default" and is normalized to
	// ControllerOnly before any row is written; it is never persisted as
	// empty.
	InboxPolicy InboxPolicy
	// PID, PGID, and ProcessStartTicks record the OS process identity of
	// this task's launched child, for restart reconciliation. They are
	// nullable pointers because every row created before this field existed,
	// and any task that never reached Running, has no process identity
	// recorded — nil, never a zero-value false positive.
	PID               *int
	PGID              *int
	ProcessStartTicks *int64
}

// CanonicalParameters returns the deterministic encoding persisted for a
// parameter map and compared for idempotency equivalence. Key order never
// affects the result, because encoding/json sorts map keys. An empty or nil map
// encodes as the empty string so that rows predating the parameters column stay
// equivalent to a new request that passes none.
//
// This is an encoding boundary only: the approved key, count, and value limits
// are enforced by the validator that owns the recipe declaration contract,
// before a task is ever admitted.
func CanonicalParameters(params map[string]string) (string, error) {
	if len(params) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encode task parameters: %w", err)
	}
	return string(encoded), nil
}

// decodeParameters reverses CanonicalParameters. It fails closed: a stored
// value that is neither empty nor a JSON string map is a corrupt audit record,
// never silently an empty parameter set.
func decodeParameters(stored string) (map[string]string, error) {
	if stored == "" {
		return nil, nil
	}
	var params map[string]string
	if err := json.Unmarshal([]byte(stored), &params); err != nil {
		return nil, fmt.Errorf("decode persisted task parameters: %w", err)
	}
	if params == nil {
		return nil, fmt.Errorf("decode persisted task parameters: stored value %q is not a parameter map", stored)
	}
	return params, nil
}

// Transition applies a legal lifecycle transition and records its timestamp.
func (t *Task) Transition(next Status, at time.Time) error {
	if !allowed(t.Status, next) {
		return fmt.Errorf("invalid task transition %q -> %q", t.Status, next)
	}
	if next == Starting && t.StartedAt == nil {
		t.StartedAt = ptrTime(at)
	}
	if isTerminal(next) {
		t.EndedAt = ptrTime(at)
	}
	t.Status, t.UpdatedAt = next, at
	return nil
}

// RequestCancel records an explicit cancellation intent. It never derives intent
// from a client context or request disconnect.
func (t *Task) RequestCancel(at time.Time) error {
	if isTerminal(t.Status) || t.Status == Interrupted || t.Status == Unknown {
		return fmt.Errorf("cannot cancel task in %q state", t.Status)
	}
	if t.CancelRequestedAt == nil {
		t.CancelRequestedAt = ptrTime(at)
	}
	t.UpdatedAt = at
	return nil
}

func ptrTime(v time.Time) *time.Time { return &v }
func allowed(from, to Status) bool {
	return (from == Queued && (to == Starting || to == Cancelled || to == Interrupted || to == Unknown)) ||
		(from == Starting && (to == Running || to == Failed || to == Cancelled || to == Interrupted || to == Unknown)) ||
		(from == Running && (to == Succeeded || to == Failed || to == TimedOut || to == Cancelled || to == Interrupted || to == Unknown))
}
func isTerminal(status Status) bool { return status.Terminal() }

// Terminal reports whether a status is a final lifecycle outcome: the task
// is finished and no legal transition remains. Interrupted and Unknown are
// deliberately NOT terminal — they record that the task's process state
// could not be established, which is not the same as knowing it ended.
//
// Anything deciding whether a task's evidence is safe to discard must ask
// this, not compare against a hand-written status list of its own.
func (s Status) Terminal() bool {
	return s == Succeeded || s == Failed || s == TimedOut || s == Cancelled
}
