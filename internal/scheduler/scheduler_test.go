package scheduler

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"os"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

type blockingRunner struct {
	started chan tasks.Task
	release chan struct{}
}

func (r *blockingRunner) Run(ctx context.Context, task tasks.Task) {
	r.started <- task
	<-r.release
}

func testStore(t *testing.T) *tasks.Store {
	t.Helper()
	db, err := tasks.OpenDB(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := tasks.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return tasks.NewStore(db)
}

// testScheduler owns the workers so cleanup can drain dispatch and join them
// before closing SQLite or deleting fixture directories. Register runner
// release after this helper if the runner blocks.
func testScheduler(t *testing.T, store *tasks.Store, runner Runner, workers, capacity int) *Scheduler {
	t.Helper()
	s := newScheduler(store, runner, capacity)
	var workersDone sync.WaitGroup
	for range workers {
		workersDone.Go(s.worker)
	}
	t.Cleanup(func() {
		close(s.jobs)
		workersDone.Wait()
	})
	return s
}

func newService(t *testing.T, store *tasks.Store, runner Runner, workers, capacity int) *Service {
	t.Helper()
	runsRoot := t.TempDir()
	s := testScheduler(t, store, runner, workers, capacity)
	var mu sync.Mutex
	n := 0
	returnService, err := NewService(store, s, func() string { mu.Lock(); defer mu.Unlock(); n++; return "task-" + string(rune('0'+n)) }, runsRoot)
	if err != nil {
		t.Fatal(err)
	}
	return returnService
}

func TestServiceRejectsInvalidAndOverCapacityRequests(t *testing.T) {
	store := testStore(t)
	runner := &blockingRunner{started: make(chan tasks.Task, 1), release: make(chan struct{})}
	service := newService(t, store, runner, 1, 1)
	t.Cleanup(func() { close(runner.release) })
	if _, err := service.Spawn(context.Background(), SpawnRequest{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid Spawn: %v", err)
	}
	if _, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "one"}); err != nil {
		t.Fatal(err)
	}
	<-runner.started
	if _, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "three"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("over-capacity Spawn: %v", err)
	}
}

func TestServiceSpawnValidatesInboxPolicyBeforeReserve(t *testing.T) {
	store := testStore(t)
	runner := &blockingRunner{started: make(chan tasks.Task, 1), release: make(chan struct{})}
	service := newService(t, store, runner, 1, 1)
	t.Cleanup(func() { close(runner.release) })
	if _, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "one", InboxPolicy: "bogus"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid inbox_policy: %v", err)
	}
	// The rejected request must have consumed no capacity: a full-capacity
	// Spawn afterward still succeeds.
	task, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "two", InboxPolicy: "open"})
	if err != nil {
		t.Fatal(err)
	}
	<-runner.started
	if task.InboxPolicy != tasks.OpenInbox {
		t.Fatalf("InboxPolicy = %q, want %q", task.InboxPolicy, tasks.OpenInbox)
	}
	stored, err := store.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.InboxPolicy != tasks.OpenInbox {
		t.Fatalf("stored InboxPolicy = %q, want %q", stored.InboxPolicy, tasks.OpenInbox)
	}
}

func TestServiceSpawnIsImmediateAndRequestCancellationDoesNotCancelWork(t *testing.T) {
	store := testStore(t)
	runner := &blockingRunner{started: make(chan tasks.Task, 1), release: make(chan struct{})}
	service := newService(t, store, runner, 1, 1)
	t.Cleanup(func() { close(runner.release) })
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	task, err := service.Spawn(ctx, SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Spawn blocked for %s", elapsed)
	}
	cancel()
	select {
	case running := <-runner.started:
		if running.ID != task.ID {
			t.Fatalf("runner task = %q, want %q", running.ID, task.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker was cancelled with the request context")
	}
}

func TestServiceReadsPersistedStatusAndDelegatesExplicitCancel(t *testing.T) {
	store := testStore(t)
	service := newService(t, store, nil, 1, 1)
	task, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "one"})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := NewService(store, service.scheduler, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	status, err := fresh.Status(context.Background(), task.ID)
	if err != nil || status.ID != task.ID {
		t.Fatalf("fresh Status = (%+v, %v)", status, err)
	}
	cancelled, err := fresh.Cancel(context.Background(), task.ID)
	if err != nil || cancelled.CancelRequestedAt == nil {
		t.Fatalf("Cancel = (%+v, %v)", cancelled, err)
	}
}

func TestServiceSendMessageAlwaysStampsControllerSender(t *testing.T) {
	store := testStore(t)
	// Create directly through the store so the task row (and its default
	// controller_only inbox_policy) exists without racing the scheduler's
	// background worker.
	if _, _, err := store.Create(context.Background(), tasks.Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "one"}); err != nil {
		t.Fatal(err)
	}
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, sched, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg, err := service.SendMessage(context.Background(), "task-1", "do the thing", "instruction")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Sender != tasks.ControllerSender {
		t.Fatalf("Sender = %q, want %q", msg.Sender, tasks.ControllerSender)
	}
	inbox, err := store.ReadInbox(context.Background(), "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 1 || inbox[0].Body != "do the thing" || inbox[0].Kind != "instruction" {
		t.Fatalf("ReadInbox = %+v", inbox)
	}
}

func TestServiceSendMessageRejectsUnknownTask(t *testing.T) {
	store := testStore(t)
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, sched, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(context.Background(), "no-such-task", "hi", ""); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatalf("SendMessage to unknown task: %v", err)
	}
}

func TestServiceListValidatesStatusAndClampsLimit(t *testing.T) {
	store := testStore(t)
	// Create tasks directly through the store, bypassing the scheduler's
	// background worker, so status stays exactly "queued" for this
	// filter/limit assertion instead of racing a claim.
	for _, id := range []string{"task-1", "task-2", "task-3"} {
		if _, _, err := store.Create(context.Background(), tasks.Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
			t.Fatal(err)
		}
	}
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, sched, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	all, err := service.List(context.Background(), "", "", 0)
	if err != nil || len(all) != 3 {
		t.Fatalf("List(no filter, limit 0) = (%d, %v), want 3 tasks and no error", len(all), err)
	}

	limited, err := service.List(context.Background(), "", "", 2)
	if err != nil || len(limited) != 2 {
		t.Fatalf("List(limit=2) = (%d, %v), want 2 tasks", len(limited), err)
	}

	overMax, err := service.List(context.Background(), "", "", maxListLimit+1000)
	if err != nil || len(overMax) != 3 {
		t.Fatalf("List(limit over max) = (%d, %v), want clamp to not error and still return all 3", len(overMax), err)
	}

	queued, err := service.List(context.Background(), "queued", "", 0)
	if err != nil || len(queued) != 3 {
		t.Fatalf("List(status=queued) = (%d, %v), want 3 tasks", len(queued), err)
	}

	if _, err := service.List(context.Background(), "not-a-real-status", "", 0); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("List(invalid status) = %v, want ErrInvalidStatus", err)
	}
}

func TestServiceLogsAndProgressDelegateAndBound(t *testing.T) {
	store := testStore(t)
	if _, _, err := store.Create(context.Background(), tasks.Task{ID: "task-logs", Recipe: "allowed.yaml", IdempotencyKey: "task-logs"}); err != nil {
		t.Fatal(err)
	}
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	runsRoot := t.TempDir()
	service, err := NewService(store, sched, func() string { return "unused" }, runsRoot)
	if err != nil {
		t.Fatal(err)
	}

	// Unknown task id: tasks.ErrNotFound, unwrapped, with no mention of a
	// run directory at all.
	if _, err := service.Logs(context.Background(), "no-such-task", "", 0); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatalf("Logs(unknown id) = %v, want tasks.ErrNotFound", err)
	}
	if _, err := service.Progress(context.Background(), "no-such-task"); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatalf("Progress(unknown id) = %v, want tasks.ErrNotFound", err)
	}

	// No run directory has been created yet: a just-queued task has no
	// run directory to find. Still wraps ErrRunNotFound for any existing
	// caller check, but says "not started" rather than "removed".
	logsErr, err := service.Logs(context.Background(), "task-logs", "", 0)
	_ = logsErr
	if !errors.Is(err, artifacts.ErrRunNotFound) {
		t.Fatalf("Logs(no run dir) = %v, want ErrRunNotFound", err)
	}
	if !strings.Contains(err.Error(), "has not started yet") {
		t.Fatalf("Logs(no run dir) = %q, want it to say the task has not started", err)
	}
	_, err = service.Progress(context.Background(), "task-logs")
	if !errors.Is(err, artifacts.ErrRunNotFound) {
		t.Fatalf("Progress(no run dir) = %v, want ErrRunNotFound", err)
	}
	if !strings.Contains(err.Error(), "has not started yet") {
		t.Fatalf("Progress(no run dir) = %q, want it to say the task has not started", err)
	}

	mgr := artifacts.Manager{Root: runsRoot}
	run, err := mgr.Create("task-logs")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run.StdoutPath, []byte(`{"type":"message","message":{"id":"r1","role":"assistant","content":[{"type":"text","text":"hi"}]}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run.StderrPath, []byte("warn: fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := service.Logs(context.Background(), "task-logs", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stderr != "warn: fixture\n" || result.StdoutTruncated || result.StderrTruncated {
		t.Fatalf("Logs(both) = %+v", result)
	}

	stdoutOnly, err := service.Logs(context.Background(), "task-logs", "stdout", 0)
	if err != nil {
		t.Fatal(err)
	}
	if stdoutOnly.Stderr != "" || stdoutOnly.Stdout == "" {
		t.Fatalf("Logs(stdout) = %+v, want stderr empty and stdout populated", stdoutOnly)
	}

	if _, err := service.Logs(context.Background(), "task-logs", "not-a-stream", 0); !errors.Is(err, ErrInvalidStream) {
		t.Fatalf("Logs(invalid stream) = %v, want ErrInvalidStream", err)
	}

	progress, err := service.Progress(context.Background(), "task-logs")
	if err != nil {
		t.Fatal(err)
	}
	if progress.MessageEventCount != 1 || progress.LastAssistantText != "hi" {
		t.Fatalf("Progress = %+v", progress)
	}

	// A known task that has started (and even finished) but whose run
	// directory is now gone — e.g. removed by retention — gets the
	// "no longer available" wording, not "not started yet", while still
	// wrapping ErrRunNotFound.
	if _, _, err := store.Create(context.Background(), tasks.Task{ID: "task-swept", Recipe: "allowed.yaml", IdempotencyKey: "task-swept"}); err != nil {
		t.Fatal(err)
	}
	// task-logs (created earlier in this test, still Queued) claims first by
	// creation order; a second Claim reaches task-swept.
	if _, err := store.Claim(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(context.Background(), "task-swept", tasks.Running, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(context.Background(), "task-swept", tasks.Succeeded, "", nil); err != nil {
		t.Fatal(err)
	}
	// No run directory is ever created for task-swept, which is
	// indistinguishable to FindRun from one that was created and later
	// removed — exactly the condition this wording is meant to describe.

	_, err = service.Logs(context.Background(), "task-swept", "", 0)
	if !errors.Is(err, artifacts.ErrRunNotFound) {
		t.Fatalf("Logs(swept) = %v, want ErrRunNotFound", err)
	}
	if !strings.Contains(err.Error(), "no longer available") {
		t.Fatalf("Logs(swept) = %q, want it to say the run directory is no longer available", err)
	}
	_, err = service.Progress(context.Background(), "task-swept")
	if !errors.Is(err, artifacts.ErrRunNotFound) {
		t.Fatalf("Progress(swept) = %v, want ErrRunNotFound", err)
	}
	if !strings.Contains(err.Error(), "no longer available") {
		t.Fatalf("Progress(swept) = %q, want it to say the run directory is no longer available", err)
	}
}
