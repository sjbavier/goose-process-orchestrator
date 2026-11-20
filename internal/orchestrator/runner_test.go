package orchestrator

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// shLauncher runs the task's Recipe field as a shell command via the real
// local process-group supervisor, standing in for goose.Runner so these
// tests never invoke the real Goose CLI or any provider.
type shLauncher struct{}

func (shLauncher) Start(request goose.Request, taskID string) (*supervisor.Process, artifacts.Run, *goose.Live, error) {
	p, err := supervisor.Start("sh", []string{"-c", request.Recipe}, ".", nil, &bytes.Buffer{}, &bytes.Buffer{})
	return p, artifacts.Run{Dir: "run-" + taskID}, nil, err
}
func (shLauncher) Cancel(ctx context.Context, p *supervisor.Process) supervisor.Result {
	return p.Cancel(ctx, 100*time.Millisecond)
}

// memStore is a minimal in-memory Store for adapter tests; it is not a
// substitute for the real SQLite store's tests.
type memStore struct {
	mu    chan struct{} // 1-buffered mutex substitute to keep the type trivially race-safe
	task  tasks.Task
	calls []string // records call order for tests asserting happens-before relationships
}

func newMemStore(task tasks.Task) *memStore {
	s := &memStore{mu: make(chan struct{}, 1), task: task}
	s.mu <- struct{}{}
	return s
}
func (s *memStore) lock()   { <-s.mu }
func (s *memStore) unlock() { s.mu <- struct{}{} }

func (s *memStore) Get(ctx context.Context, id string) (tasks.Task, error) {
	s.lock()
	defer s.unlock()
	return s.task, nil
}
func (s *memStore) Transition(ctx context.Context, id string, next tasks.Status, resultText string, exitCode *int) (tasks.Task, error) {
	s.lock()
	defer s.unlock()
	s.calls = append(s.calls, "Transition:"+string(next))
	if err := s.task.Transition(next, time.Now().UTC()); err != nil {
		return tasks.Task{}, err
	}
	s.task.Result, s.task.ExitCode = resultText, exitCode
	return s.task, nil
}
func (s *memStore) RecordOutcomeSummary(ctx context.Context, id string, summary tasks.OutcomeSummary) (tasks.Task, error) {
	s.lock()
	defer s.unlock()
	s.calls = append(s.calls, "RecordOutcomeSummary")
	s.task.FinalText = summary.FinalText
	ft, cs, smt, st := summary.FinalTextTruncated, summary.CompleteSeen, summary.StalledMaxTurns, summary.StdoutTruncated
	s.task.FinalTextTruncated, s.task.CompleteSeen, s.task.StalledMaxTurns, s.task.StdoutTruncated = &ft, &cs, &smt, &st
	tt, it, ot := summary.TotalTokens, summary.InputTokens, summary.OutputTokens
	s.task.TotalTokens, s.task.InputTokens, s.task.OutputTokens = &tt, &it, &ot
	crt, cwt := summary.CacheReadTokens, summary.CacheWriteTokens
	s.task.CacheReadTokens, s.task.CacheWriteTokens = &crt, &cwt
	cost := summary.CostUSD
	s.task.CostUSD = &cost
	tcc := summary.ToolCallCount
	s.task.ToolCallCount = &tcc
	lea := summary.LastEventAt
	s.task.LastEventAt = &lea
	bd := summary.BytesDropped
	s.task.BytesDropped = &bd
	return s.task, nil
}
func (s *memStore) RecordFailure(ctx context.Context, id, failure string) (tasks.Task, error) {
	s.lock()
	defer s.unlock()
	s.calls = append(s.calls, "RecordFailure")
	s.task.Failure = failure
	return s.task, nil
}
func (s *memStore) requestCancel() {
	s.lock()
	defer s.unlock()
	_ = s.task.RequestCancel(time.Now().UTC())
}
func (s *memStore) status() tasks.Status {
	s.lock()
	defer s.unlock()
	return s.task.Status
}

func startingTask(recipe string) tasks.Task {
	return tasks.Task{ID: "t1", Recipe: recipe, IdempotencyKey: "k1", Status: tasks.Starting, CreatedAt: time.Now().UTC()}
}

func TestRunReportsSuccessAndClearsRegistry(t *testing.T) {
	store := newMemStore(startingTask("exit 0"))
	r := New(shLauncher{}, store)
	r.Run(context.Background(), store.task)

	if got := store.status(); got != tasks.Succeeded {
		t.Fatalf("status = %q, want succeeded", got)
	}
	r.mu.Lock()
	n := len(r.processes)
	r.mu.Unlock()
	if n != 0 {
		t.Fatalf("registry not cleared: %d entries remain", n)
	}
}

func TestRunReportsFailureExitCode(t *testing.T) {
	store := newMemStore(startingTask("exit 7"))
	r := New(shLauncher{}, store)
	r.Run(context.Background(), store.task)

	task := store.task
	if task.Status != tasks.Failed {
		t.Fatalf("status = %q, want failed", task.Status)
	}
	if task.ExitCode == nil || *task.ExitCode != 7 {
		t.Fatalf("exit code = %v, want 7", task.ExitCode)
	}
}

func TestRunCancellationReachesOwnedProcess(t *testing.T) {
	store := newMemStore(startingTask("sleep 30 & wait"))
	r := New(shLauncher{}, store)

	done := make(chan struct{})
	go func() { r.Run(context.Background(), store.task); close(done) }()

	// Give the process a moment to actually start before requesting cancellation.
	deadline := time.After(5 * time.Second)
	for store.status() != tasks.Running {
		select {
		case <-deadline:
			t.Fatal("task never reached running before cancellation could be requested")
		case <-time.After(10 * time.Millisecond):
		}
	}
	store.requestCancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if got := store.status(); got != tasks.Cancelled {
		t.Fatalf("status = %q, want cancelled", got)
	}
}
