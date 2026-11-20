package orchestrator

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// TestRunPersistsFinalOutcomeSummary proves acceptance criterion 1 end to
// end at the orchestrator layer: a completed run's final text, complete
// flag, and token totals are persisted via Store.RecordOutcomeSummary before
// Run returns.
func TestRunPersistsFinalOutcomeSummary(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "slow.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	store := newMemStore(startingTask(filepath.Join(recipeRoot, "slow.yaml")))

	New(launcher, store).Run(context.Background(), store.task)

	task := store.task
	if task.Status != tasks.Succeeded {
		t.Fatalf("status = %q, want succeeded", task.Status)
	}
	wantText := "part0 part1 part2 part3 part4 part5 "
	if task.FinalText != wantText {
		t.Fatalf("FinalText = %q, want %q", task.FinalText, wantText)
	}
	if task.CompleteSeen == nil || !*task.CompleteSeen {
		t.Fatalf("CompleteSeen = %v, want true", task.CompleteSeen)
	}
	if task.TotalTokens == nil || *task.TotalTokens != 7 || task.InputTokens == nil || *task.InputTokens != 3 || task.OutputTokens == nil || *task.OutputTokens != 4 {
		t.Fatalf("token fields = (%v, %v, %v), want (7, 3, 4)", task.TotalTokens, task.InputTokens, task.OutputTokens)
	}
}

// TestRunFlushesPeriodicOutcomeSummaryDuringLongRun covers the periodic-flush
// focused check: a run long enough to span multiple CancelPoll ticks must
// call RecordOutcomeSummary more than once (at least one periodic flush, plus
// the final one), not only at the very end.
func TestRunFlushesPeriodicOutcomeSummaryDuringLongRun(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "slow.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	store := newMemStore(startingTask(filepath.Join(recipeRoot, "slow.yaml")))

	New(launcher, store).Run(context.Background(), store.task)

	count := 0
	for _, c := range store.calls {
		if c == "RecordOutcomeSummary" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("RecordOutcomeSummary called %d times, want at least 2 (one periodic flush plus the final one) for a run spanning ~1.8s of CancelPoll(%s) ticks", count, CancelPoll)
	}
}

// TestRunFinalOutcomeSummaryPersistsBeforeTerminalTransition covers the
// happens-before focused check: the final RecordOutcomeSummary call must be
// observable before the terminal Transition call, never after.
func TestRunFinalOutcomeSummaryPersistsBeforeTerminalTransition(t *testing.T) {
	binary, recipeRoot := newFakeGoose(t, "slow.yaml")
	launcher := newRealLauncher(t, binary, recipeRoot)
	store := newMemStore(startingTask(filepath.Join(recipeRoot, "slow.yaml")))

	New(launcher, store).Run(context.Background(), store.task)

	lastSummary, terminalTransition := -1, -1
	for i, c := range store.calls {
		if c == "RecordOutcomeSummary" {
			lastSummary = i
		}
		if c == "Transition:succeeded" || c == "Transition:failed" {
			terminalTransition = i
		}
	}
	if lastSummary == -1 || terminalTransition == -1 || lastSummary > terminalTransition {
		t.Fatalf("call order = %v, want the last RecordOutcomeSummary before the terminal Transition", store.calls)
	}
}

// TestRunLaunchFailurePopulatesFailureColumn covers the launch-failure
// focused check: a task whose Launcher.Start fails outright gets Failure
// populated, and Result is unchanged from today's behavior (still
// err.Error() there too).
func TestRunLaunchFailurePopulatesFailureColumn(t *testing.T) {
	store := newMemStore(startingTask("unused"))
	failer := failingLauncher{err: errBoom}
	New(failer, store).Run(context.Background(), store.task)

	task := store.task
	if task.Status != tasks.Failed {
		t.Fatalf("status = %q, want failed", task.Status)
	}
	if task.Result != errBoom.Error() {
		t.Fatalf("Result = %q, want %q (unchanged byte-identical behavior)", task.Result, errBoom.Error())
	}
	if task.Failure != errBoom.Error() {
		t.Fatalf("Failure = %q, want %q", task.Failure, errBoom.Error())
	}
}

var errBoom = errStr("boom: no process ever existed")

type errStr string

func (e errStr) Error() string { return string(e) }

// failingLauncher always fails Start before any process exists, standing in
// for Launcher.Start erroring (e.g. an invalid recipe or build failure).
type failingLauncher struct{ err error }

func (f failingLauncher) Start(request goose.Request, taskID string) (*supervisor.Process, artifacts.Run, *goose.Live, error) {
	return nil, artifacts.Run{}, nil, f.err
}
func (f failingLauncher) Cancel(ctx context.Context, p *supervisor.Process) supervisor.Result {
	panic("not reached")
}
