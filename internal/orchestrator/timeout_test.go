package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// A long-running worker with timeout_seconds set is cancelled through the
// normal cancel path and ends timed_out with a recorded failure.
func TestRealLauncherPerTaskTimeoutEndsTimedOut(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "sleep.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	task := startingTask(filepath.Join(recipeRoot, "sleep.yaml"))
	task.TimeoutSeconds = 1
	store := newMemStore(task)

	done := make(chan struct{})
	start := time.Now()
	go func() { New(launcher, store).Run(context.Background(), store.task); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after the per-task timeout")
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("returned after %v, before the 1s timeout", elapsed)
	}
	if got := store.status(); got != tasks.TimedOut {
		t.Fatalf("status = %q, want timed_out", got)
	}
	if !strings.HasPrefix(store.task.Failure, "timed_out") {
		t.Fatalf("failure = %q, want timed_out prefix", store.task.Failure)
	}
}

// A task that finishes inside its timeout is unaffected.
func TestRealLauncherTimeoutNotReachedSucceeds(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "success.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	task := startingTask(filepath.Join(recipeRoot, "success.yaml"))
	task.TimeoutSeconds = 30
	store := newMemStore(task)
	New(launcher, store).Run(context.Background(), store.task)
	if got := store.status(); got != tasks.Succeeded {
		t.Fatalf("status = %q, want succeeded", got)
	}
}

func TestDeadlineFor(t *testing.T) {
	now := time.Now()
	if !deadlineFor(now, 0).IsZero() || !deadlineFor(now, -1).IsZero() {
		t.Fatal("non-positive timeout must mean no deadline")
	}
	if got := deadlineFor(now, 5); !got.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("deadline = %v", got)
	}
}
