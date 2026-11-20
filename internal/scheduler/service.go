package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

var ErrInvalidRequest = errors.New("recipe and idempotency key are required")

// ErrInvalidStatus means a caller-supplied status filter does not match any
// known task status.
var ErrInvalidStatus = errors.New("unknown task status")

// ErrInvalidStream means a caller-supplied Logs stream selector is not one
// of the recognized values.
var ErrInvalidStream = errors.New("stream must be \"stdout\", \"stderr\", or \"both\"")

// maxListLimit is the server-side maximum number of rows List ever
// returns, regardless of what a caller requests, to bound the cost of a
// single read-only call.
const maxListLimit = 500

type Store interface {
	Create(context.Context, tasks.Task) (tasks.Task, bool, error)
	Get(context.Context, string) (tasks.Task, error)
	RequestCancel(context.Context, string) (tasks.Task, error)
	List(context.Context, tasks.ListFilter) ([]tasks.Task, error)
	Enqueue(ctx context.Context, taskID, sender, kind, body string) (tasks.Message, error)
}

type Service struct {
	store     Store
	scheduler *Scheduler
	newID     func() string
	// runsRoot is the same directory artifacts.Manager{Root: runsRoot} used
	// to create every task's run directory. Logs/Progress derive the run
	// directory from it on demand (internal/artifacts.Manager.FindRun);
	// nothing is stored in the tasks table for this.
	runsRoot string
	// validator is nil unless WithParameterValidator enabled recipe
	// parameters; a nil validator makes any parameterized request fail closed.
	validator ParameterValidator
}

// ParameterValidator rejects a request's recipe parameters at admission time,
// before any capacity reservation or task row exists. The launcher implements
// it, so the same contract governs admission and the eventual argv.
type ParameterValidator interface {
	ValidateRequest(recipe string, params map[string]string) error
}

// Option configures a Service at construction.
type Option func(*Service)

// WithParameterValidator enables recipe parameters on this service. Without
// it, a Service rejects any request carrying parameters rather than admitting
// one it cannot validate.
func WithParameterValidator(validator ParameterValidator) Option {
	return func(s *Service) { s.validator = validator }
}

func NewService(store Store, scheduler *Scheduler, newID func() string, runsRoot string, opts ...Option) (*Service, error) {
	if store == nil || scheduler == nil || newID == nil {
		return nil, errors.New("store, scheduler, and id generator are required")
	}
	service := &Service{store: store, scheduler: scheduler, newID: newID, runsRoot: runsRoot}
	for _, opt := range opts {
		opt(service)
	}
	return service, nil
}

type SpawnRequest struct {
	Recipe         string
	IdempotencyKey string
	// MaxTurns overrides the server-wide --max-turns default for this one
	// task when positive; zero means "use the server default."
	MaxTurns int
	// Parameters holds the declared recipe parameters for this request. It is
	// validated against the recipe's own declarations before admission, is
	// part of the task's durable identity, and must never carry secrets.
	Parameters map[string]string
	// InboxPolicy selects the spawned task's mailbox policy. Empty means
	// unchanged (the store's own ControllerOnly default); a non-empty value
	// must be one of tasks.KnownInboxPolicy's recognized strings, checked
	// before Reserve so an invalid value consumes no capacity and creates no
	// task row.
	InboxPolicy string
}

// Spawn persists and returns a queued ID without waiting for worker execution.
// Its context is used only for the immediate store operation: worker execution
// is deliberately not derived from request cancellation.
func (s *Service) Spawn(ctx context.Context, request SpawnRequest) (tasks.Task, error) {
	if request.Recipe == "" || request.IdempotencyKey == "" {
		return tasks.Task{}, ErrInvalidRequest
	}
	// Parameters are validated before Reserve, so an invalid request consumes
	// no capacity, creates no task row, and starts no process. A service with
	// no validator fails closed rather than admitting unvalidated parameters.
	if len(request.Parameters) > 0 {
		if s.validator == nil {
			return tasks.Task{}, fmt.Errorf("%w: recipe parameters are not enabled on this server", ErrInvalidRequest)
		}
		if err := s.validator.ValidateRequest(request.Recipe, request.Parameters); err != nil {
			return tasks.Task{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	}
	inboxPolicy := tasks.InboxPolicy(request.InboxPolicy)
	if inboxPolicy != "" && !tasks.KnownInboxPolicy(inboxPolicy) {
		return tasks.Task{}, fmt.Errorf("%w: unknown inbox_policy %q", ErrInvalidRequest, request.InboxPolicy)
	}
	if err := s.scheduler.Reserve(); err != nil {
		return tasks.Task{}, err
	}
	task, inserted, err := s.store.Create(ctx, tasks.Task{ID: s.newID(), Recipe: request.Recipe, IdempotencyKey: request.IdempotencyKey, MaxTurns: request.MaxTurns, Parameters: request.Parameters, InboxPolicy: inboxPolicy})
	if err != nil || !inserted {
		s.scheduler.Release()
		return task, err
	}
	s.scheduler.Enqueue()
	return task, nil
}

func (s *Service) Status(ctx context.Context, id string) (tasks.Task, error) {
	return s.store.Get(ctx, id)
}
func (s *Service) Result(ctx context.Context, id string) (tasks.Task, error) {
	return s.store.Get(ctx, id)
}

// Cancel records only explicit cancellation intent. It does not cancel work
// because the caller context is disconnected or cancelled.
func (s *Service) Cancel(ctx context.Context, id string) (tasks.Task, error) {
	return s.store.RequestCancel(ctx, id)
}

// SendMessage queues one message for id's inbox, always stamping the sender
// as tasks.ControllerSender: the controller's own connection is privileged by
// construction, per TMB-02's connection-derived-identity decision, and this
// method has no sender parameter for a caller to override. The recipient's
// inbox_policy, quota, and shape validation are tasks.Store.Enqueue's job,
// enforced fail-closed before any row is written.
func (s *Service) SendMessage(ctx context.Context, id, body, kind string) (tasks.Message, error) {
	return s.store.Enqueue(ctx, id, tasks.ControllerSender, kind, body)
}

var listableStatuses = map[string]tasks.Status{
	string(tasks.Queued): tasks.Queued, string(tasks.Starting): tasks.Starting,
	string(tasks.Running): tasks.Running, string(tasks.Succeeded): tasks.Succeeded,
	string(tasks.Failed): tasks.Failed, string(tasks.TimedOut): tasks.TimedOut,
	string(tasks.Cancelled): tasks.Cancelled, string(tasks.Interrupted): tasks.Interrupted,
	string(tasks.Unknown): tasks.Unknown,
}

// List returns tasks newest first, optionally filtered by status, capped at
// min(limit, maxListLimit). limit <= 0 defaults to maxListLimit. An empty
// status means no filter; a non-empty, unrecognized status is rejected
// rather than silently returning no rows.
func (s *Service) List(ctx context.Context, status string, limit int) ([]tasks.Task, error) {
	filter := tasks.ListFilter{}
	if status != "" {
		known, ok := listableStatuses[status]
		if !ok {
			return nil, ErrInvalidStatus
		}
		filter.Status = &known
	}
	if limit <= 0 || limit > maxListLimit {
		limit = maxListLimit
	}
	filter.Limit = limit
	return s.store.List(ctx, filter)
}

// LogsResult is a bounded tail of one task's captured output.
type LogsResult struct {
	Stdout          string
	StdoutTruncated bool
	Stderr          string
	StderrTruncated bool
}

// Logs returns a bounded tail of a task's stdout.raw/stderr.raw, located by
// id without any stored run-directory reference (artifacts.Manager.FindRun).
// stream selects "stdout", "stderr", or "" / "both" (default both).
// maxBytes <= 0 defaults to logs.DefaultTailBytes; any value is clamped to
// logs.MaxTailBytes per stream. Returns tasks.ErrNotFound for an unknown id,
// or an error wrapping artifacts.ErrRunNotFound — with a message
// distinguishing "never started" from "run directory no longer available"
// (e.g. swept by retention) — for a known task with no run directory.
func (s *Service) Logs(ctx context.Context, id, stream string, maxBytes int) (LogsResult, error) {
	switch stream {
	case "", "both", "stdout", "stderr":
	default:
		return LogsResult{}, ErrInvalidStream
	}
	run, err := artifacts.Manager{Root: s.runsRoot}.FindRun(id)
	if err != nil {
		if errors.Is(err, artifacts.ErrRunNotFound) {
			return LogsResult{}, s.runUnavailableError(ctx, id)
		}
		return LogsResult{}, err
	}
	limit := int64(maxBytes)
	var result LogsResult
	if stream == "" || stream == "both" || stream == "stdout" {
		data, truncated, err := logs.Tail(run.StdoutPath, limit)
		if err != nil {
			return LogsResult{}, err
		}
		result.Stdout, result.StdoutTruncated = string(data), truncated
	}
	if stream == "" || stream == "both" || stream == "stderr" {
		data, truncated, err := logs.Tail(run.StderrPath, limit)
		if err != nil {
			return LogsResult{}, err
		}
		result.Stderr, result.StderrTruncated = string(data), truncated
	}
	return result, nil
}

// ProgressResult extends logs.ParseProgress's on-demand re-parse with the
// fields that only the task row can supply: tool_call_count/last_event_at
// (sourced from the live parser's periodic/final flush, chunk C5) and
// stdout_truncated/bytes_dropped (sourced from the disk capture's own
// accounting, chunk C2). For a running task these four reflect the most
// recent periodic flush, not byte-exact live state — the same bounded
// staleness logs.Progress.MessageEventCount already documents for itself.
// They are simply zero for a task whose row predates migration 005 or was
// reconciled after a restart (decision D): never guessed, never derived.
type ProgressResult struct {
	logs.Progress
	ToolCallCount   int64
	LastEventAt     time.Time
	StdoutTruncated bool
	BytesDropped    int64
}

// Progress defensively parses a task's stdout.raw stream-json output. See
// logs.ParseProgress and logs.Progress for the exact fields and the
// documented message_event_count caveat. Returns the same errors as Logs
// under the same conditions.
func (s *Service) Progress(ctx context.Context, id string) (ProgressResult, error) {
	run, err := artifacts.Manager{Root: s.runsRoot}.FindRun(id)
	if err != nil {
		if errors.Is(err, artifacts.ErrRunNotFound) {
			return ProgressResult{}, s.runUnavailableError(ctx, id)
		}
		return ProgressResult{}, err
	}
	raw, err := logs.ReadBounded(run.StdoutPath, logs.MaxProgressReadBytes)
	if err != nil {
		return ProgressResult{}, err
	}
	result := ProgressResult{Progress: logs.ParseProgress(raw)}
	task, err := s.store.Get(ctx, id)
	if err != nil {
		return ProgressResult{}, err
	}
	if task.ToolCallCount != nil {
		result.ToolCallCount = *task.ToolCallCount
	}
	if task.LastEventAt != nil {
		result.LastEventAt = *task.LastEventAt
	}
	if task.StdoutTruncated != nil {
		result.StdoutTruncated = *task.StdoutTruncated
	}
	if task.BytesDropped != nil {
		result.BytesDropped = *task.BytesDropped
	}
	return result, nil
}

// OutcomeResult is the combined "what happened" view status/result need,
// built from the task row plus a best-effort run-directory lookup. Outcome
// is computed here (not stored on the row, see internal/orchestrator's
// outcomeSummaryFromLive comment) because the correct terminal status is
// only known once the task is actually terminal, which this method's caller
// always has.
type OutcomeResult struct {
	Task tasks.Task
	// RunDir is best-effort: empty if the run directory is gone (e.g. swept
	// by retention) or never existed (task never started) — never an error.
	RunDir string
	// Outcome is one of "completed", "stalled_max_turns",
	// "no_complete_event", "process_failed", or "" for a non-terminal or
	// Cancelled task (cancellation already fully explains itself via
	// status+cancel_requested).
	Outcome             string
	StderrTail          string
	StderrTailTruncated bool
}

// outcomeFromTask computes OutcomeResult.Outcome per plan.md's semantics
// table. A Succeeded task with no live summary ever recorded (CompleteSeen
// nil, e.g. reconciled or predating migration 005) reports
// "no_complete_event" rather than guessing "completed" — this method never
// claims success it cannot verify.
func outcomeFromTask(t tasks.Task) string {
	switch t.Status {
	case tasks.Succeeded:
		if t.StalledMaxTurns != nil && *t.StalledMaxTurns {
			return "stalled_max_turns"
		}
		if t.CompleteSeen != nil && *t.CompleteSeen {
			return "completed"
		}
		return "no_complete_event"
	case tasks.Failed, tasks.TimedOut:
		return "process_failed"
	default:
		return ""
	}
}

// Outcome assembles the status/result tool response: the task row itself,
// its outcome classification, a best-effort run directory, and (only when
// Outcome is "process_failed" and the run directory is still available) a
// bounded stderr tail, reusing logs.Tail exactly as the logs tool does.
func (s *Service) Outcome(ctx context.Context, id string) (OutcomeResult, error) {
	task, err := s.store.Get(ctx, id)
	if err != nil {
		return OutcomeResult{}, err
	}
	result := OutcomeResult{Task: task, Outcome: outcomeFromTask(task)}
	run, err := artifacts.Manager{Root: s.runsRoot}.FindRun(id)
	if err != nil {
		return result, nil
	}
	result.RunDir = run.Dir
	if result.Outcome == "process_failed" {
		if data, truncated, terr := logs.Tail(run.StderrPath, logs.DefaultStderrTailBytes); terr == nil {
			result.StderrTail, result.StderrTailTruncated = string(data), truncated
		}
	}
	return result, nil
}

// runUnavailableError distinguishes, for a task whose run directory
// FindRun could not locate, why: an unknown id (tasks.ErrNotFound, passed
// through unchanged), a task that has not reached Starting yet (no run
// directory was ever created), or a task whose run directory existed once
// but is gone now — most likely swept by retention. The last two both wrap
// artifacts.ErrRunNotFound, so errors.Is(err, artifacts.ErrRunNotFound)
// still holds for every existing caller that checks it.
func (s *Service) runUnavailableError(ctx context.Context, id string) error {
	task, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if task.Status == tasks.Queued {
		return fmt.Errorf("task %s has not started yet, so it has no run directory: %w", id, artifacts.ErrRunNotFound)
	}
	return fmt.Errorf("task %s's run directory is no longer available, possibly removed by retention: %w", id, artifacts.ErrRunNotFound)
}
