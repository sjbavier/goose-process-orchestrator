package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// countingRunner counts launches and blocks until released.
type countingRunner struct {
	launches atomic.Int32
	release  chan struct{}
}

func (r *countingRunner) Run(ctx context.Context, task tasks.Task) {
	r.launches.Add(1)
	<-r.release
}

func idempotencyService(t *testing.T, workers, capacity int, opts ...Option) (*Service, *tasks.Store, *countingRunner) {
	t.Helper()
	store := testStore(t)
	runner := &countingRunner{release: make(chan struct{})}
	t.Cleanup(func() { close(runner.release) })
	sched, err := New(store, runner, workers, capacity)
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	service, err := NewService(store, sched, func() string { return "task-" + string(rune('a'+n.Add(1))) }, t.TempDir(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, runner
}

func TestSpawnSameKeyIdenticalReturnsSameTaskWithoutSecondLaunch(t *testing.T) {
	service, _, runner := idempotencyService(t, 4, 8)
	ctx := context.Background()
	first, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("ids differ: %q vs %q", first.ID, second.ID)
	}
	listed, err := service.List(ctx, "", "", 100)
	if err != nil || len(listed) != 1 {
		t.Fatalf("rows = %d (err %v), want 1", len(listed), err)
	}
	waitFor(t, func() bool { return runner.launches.Load() >= 1 })
	if got := runner.launches.Load(); got != 1 {
		t.Fatalf("launches = %d, want exactly 1", got)
	}
}

func TestSpawnSameKeyDifferingRequestConflicts(t *testing.T) {
	service, _, _ := idempotencyService(t, 4, 8)
	ctx := context.Background()
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only"}); err != nil {
		t.Fatal(err)
	}
	for name, req := range map[string]SpawnRequest{
		"recipe":          {Recipe: "other.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only"},
		"policy":          {Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "open"},
		"max_turns":       {Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only", MaxTurns: 3},
		"timeout_seconds": {Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only", TimeoutSeconds: 30},
		"expect_provider": {Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only", ExpectProvider: "p"},
		"expect_model":    {Recipe: "r.yaml", IdempotencyKey: "k", InboxPolicy: "controller_only", ExpectModel: "m"},
	} {
		if _, err := service.Spawn(ctx, req); !errors.Is(err, tasks.ErrIdempotencyConflict) {
			t.Fatalf("%s change = %v, want ErrIdempotencyConflict", name, err)
		}
	}
	if listed, _ := service.List(ctx, "", "", 100); len(listed) != 1 {
		t.Fatalf("rows = %d after conflicts, want 1", len(listed))
	}
}

func TestSpawnConcurrentIdenticalRequestsCreateSingleRow(t *testing.T) {
	service, _, runner := idempotencyService(t, 4, 32)
	ctx := context.Background()
	const n = 16
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "same"})
			ids[i], errs[i] = task.ID, err
		}(i)
	}
	wg.Wait()
	for i := range ids {
		if errs[i] != nil || ids[i] != ids[0] {
			t.Fatalf("spawn %d = %q, %v; want id %q", i, ids[i], errs[i], ids[0])
		}
	}
	if listed, _ := service.List(ctx, "", "", 100); len(listed) != 1 {
		t.Fatalf("rows = %d, want 1", len(listed))
	}
	waitFor(t, func() bool { return runner.launches.Load() >= 1 })
	if got := runner.launches.Load(); got != 1 {
		t.Fatalf("launches = %d, want exactly 1", got)
	}
}

func TestSpawnTimeoutValidationAndPersistence(t *testing.T) {
	service, _, _ := idempotencyService(t, 1, 8, WithServerInfo(ServerInfo{TimeoutCeilingSeconds: 60}))
	ctx := context.Background()
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "neg", TimeoutSeconds: -1}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("negative = %v, want ErrInvalidRequest", err)
	}
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "big", TimeoutSeconds: 61}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("above ceiling = %v, want ErrInvalidRequest", err)
	}
	if listed, _ := service.List(ctx, "", "", 100); len(listed) != 0 {
		t.Fatalf("rejected spawns left %d rows", len(listed))
	}
	task, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "ok", TimeoutSeconds: 60})
	if err != nil || task.TimeoutSeconds != 60 {
		t.Fatalf("spawn = %+v, %v; want timeout 60", task, err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestSpawnReplayWithDifferentCaseExpectHashIsSameTask(t *testing.T) {
	store := testStore(t)
	runner := &countingRunner{release: make(chan struct{})}
	t.Cleanup(func() { close(runner.release) })
	sched, err := New(store, runner, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	sha := strings.Repeat("a", 64)
	service, err := NewService(store, sched, func() string { return "task-" + string(rune('a'+n.Add(1))) }, t.TempDir(), WithParameterValidator(&hashingValidator{sha: sha}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k", ExpectRecipeSHA256: sha})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k", ExpectRecipeSHA256: strings.ToUpper(sha)})
	if err != nil || second.ID != first.ID {
		t.Fatalf("replay = %+v, %v; want task %q", second, err, first.ID)
	}
}
