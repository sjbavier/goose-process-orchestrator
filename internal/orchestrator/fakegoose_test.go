package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// fakeGooseScript stands in for the real `goose` CLI. It never calls a
// provider or network endpoint: it only inspects the --recipe path's base
// name (set by each test fixture below) and exits deterministically. This
// is the only executable these tests invoke.
const fakeGooseScript = `#!/bin/sh
recipe=""
while [ $# -gt 0 ]; do
  case "$1" in
    --recipe) recipe="$2"; shift 2 ;;
    *) shift ;;
  esac
done
name=$(basename "$recipe")
case "$name" in
  success.yaml) echo "ok"; exit 0 ;;
  fail.yaml) echo "boom" 1>&2; exit 7 ;;
  sleep.yaml) sleep 30 & wait ;;
  *) echo "unknown fixture $name" 1>&2; exit 2 ;;
esac
`

// newFakeGoose writes the fake binary and an allowlisted recipe root
// containing the named fixtures, returning the binary path and recipe root.
func newFakeGoose(t *testing.T, fixtures ...string) (binary, recipeRoot string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "fakegoose.sh")
	if err := os.WriteFile(binary, []byte(fakeGooseScript), 0o755); err != nil {
		t.Fatal(err)
	}
	recipeRoot = t.TempDir()
	for _, f := range fixtures {
		if err := os.WriteFile(filepath.Join(recipeRoot, f), []byte("title: fixture\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return binary, recipeRoot
}

func newRealLauncher(t *testing.T, binary, recipeRoot string) goose.Runner {
	t.Helper()
	return goose.Runner{
		Config:    goose.Config{Binary: binary, RecipeRoot: recipeRoot, MaxTurns: 1},
		Artifacts: artifacts.Manager{Root: t.TempDir()},
	}
}

// TestRealLauncherSuccessAndFailure proves the exact chain cmd/orchestrator
// wires (goose.Runner -> orchestrator.Runner -> store) against a controlled
// fake `goose` binary, with the real allowlist/argv-construction path
// (internal/goose.Config.Build) in the loop rather than bypassed.
func TestRealLauncherSuccessAndFailure(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "success.yaml", "fail.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)

	store := newMemStore(startingTask(filepath.Join(recipeRoot, "success.yaml")))
	New(launcher, store).Run(context.Background(), store.task)
	if store.task.Status != tasks.Succeeded {
		t.Fatalf("success fixture status = %q, want succeeded", store.task.Status)
	}

	store = newMemStore(startingTask(filepath.Join(recipeRoot, "fail.yaml")))
	New(launcher, store).Run(context.Background(), store.task)
	if store.task.Status != tasks.Failed || store.task.ExitCode == nil || *store.task.ExitCode != 7 {
		t.Fatalf("fail fixture task = %+v, want failed/exit 7", store.task)
	}
}

// TestRealLauncherCancellation proves explicit cancellation intent reaches
// the real owned process group through the full adapter, not just the row.
func TestRealLauncherCancellation(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "sleep.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	store := newMemStore(startingTask(filepath.Join(recipeRoot, "sleep.yaml")))
	r := New(launcher, store)

	done := make(chan struct{})
	go func() { r.Run(context.Background(), store.task); close(done) }()

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

// TestRealLauncherTwoConcurrentRunsAreIsolated covers acceptance criterion 2:
// two independent recipes run as two independent OS processes with unique
// run directories, through the real launcher and adapter.
func TestRealLauncherTwoConcurrentRunsAreIsolated(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "success.yaml", "fail.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)

	var wg sync.WaitGroup
	results := make([]*memStore, 2)
	fixtures := []string{"success.yaml", "fail.yaml"}
	for i := range 2 {
		i := i
		store := newMemStore(startingTask(filepath.Join(recipeRoot, fixtures[i])))
		store.task.ID = fmt.Sprintf("t%d", i)
		results[i] = store
		wg.Add(1)
		go func() { defer wg.Done(); New(launcher, store).Run(context.Background(), store.task) }()
	}
	wg.Wait()

	if results[0].task.Status != tasks.Succeeded {
		t.Fatalf("run 0 status = %q, want succeeded", results[0].task.Status)
	}
	if results[1].task.Status != tasks.Failed {
		t.Fatalf("run 1 status = %q, want failed", results[1].task.Status)
	}
	if results[0].task.Result == "" || results[1].task.Result == "" || results[0].task.Result == results[1].task.Result {
		t.Fatalf("run directories not distinct/recorded: %q vs %q", results[0].task.Result, results[1].task.Result)
	}
}
