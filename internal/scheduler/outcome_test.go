package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// newTerminalTask creates a queued task and drives it straight to want via
// Starting/Running, so Outcome's switch on status sees a real, legally
// reached terminal state rather than a hand-set field.
func newTerminalTask(t *testing.T, store *tasks.Store, id string, want tasks.Status) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := store.Create(ctx, tasks.Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, id, tasks.Starting, "", nil); err != nil {
		t.Fatal(err)
	}
	if want == tasks.Cancelled {
		if _, err := store.Transition(ctx, id, tasks.Cancelled, "", nil); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := store.Transition(ctx, id, tasks.Running, "", nil); err != nil {
		t.Fatal(err)
	}
	if want == tasks.Running {
		return
	}
	exit := 0
	if want == tasks.Failed {
		exit = 1
	}
	if _, err := store.Transition(ctx, id, want, "", &exit); err != nil {
		t.Fatal(err)
	}
}

// TestOutcomeClassifiesPerSemanticsTable covers plan.md's outcome semantics
// table directly: every (status, live-summary) combination it defines.
func TestOutcomeClassifiesPerSemanticsTable(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, sched, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name        string
		id          string
		status      tasks.Status
		summary     *tasks.OutcomeSummary // nil means "never recorded"
		wantOutcome string
	}{
		{"succeeded, complete, no stall", "t-completed", tasks.Succeeded, &tasks.OutcomeSummary{CompleteSeen: true}, "completed"},
		{"succeeded, stall marker seen", "t-stalled", tasks.Succeeded, &tasks.OutcomeSummary{CompleteSeen: true, StalledMaxTurns: true}, "stalled_max_turns"},
		{"succeeded, present, complete_seen=0, no stall", "t-incomplete", tasks.Succeeded, &tasks.OutcomeSummary{CompleteSeen: false}, "no_complete_event"},
		{"succeeded, absent (reconciled/pre-migration)", "t-absent", tasks.Succeeded, nil, "no_complete_event"},
		{"failed, any", "t-failed", tasks.Failed, &tasks.OutcomeSummary{CompleteSeen: false}, "process_failed"},
		{"failed, legacy text marker", "t-failed-legacy", tasks.Failed, &tasks.OutcomeSummary{StalledMaxTurns: true}, "process_failed"},
		{"failed, absent", "t-failed-absent", tasks.Failed, nil, "process_failed"},
		{"timed out", "t-timedout", tasks.TimedOut, nil, "process_failed"},
		{"timed out with stall evidence", "t-timedout-stall", tasks.TimedOut, &tasks.OutcomeSummary{Outcome: tasks.OutcomeStalledMaxTurns}, "process_failed"},
		{"cancelled: not one of the four buckets", "t-cancelled", tasks.Cancelled, nil, ""},
		{"cancelled with stall evidence", "t-cancelled-stall", tasks.Cancelled, &tasks.OutcomeSummary{Outcome: tasks.OutcomeStalledMaxTurns}, ""},
		{"running: non-terminal", "t-running", tasks.Running, nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			newTerminalTask(t, store, c.id, c.status)
			if c.summary != nil {
				if _, err := store.RecordOutcomeSummary(ctx, c.id, *c.summary); err != nil {
					t.Fatal(err)
				}
			}
			result, err := service.Outcome(ctx, c.id)
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome != c.wantOutcome {
				t.Fatalf("Outcome(%s) = %q, want %q", c.id, result.Outcome, c.wantOutcome)
			}
		})
	}
}

// TestOutcomeStderrTailOnlyWhenProcessFailedAndRunDirAvailable covers C6's
// completion criterion: stderr_tail only appears when outcome ==
// "process_failed" and a run directory is actually available.
func TestOutcomeStderrTailOnlyWhenProcessFailedAndRunDirAvailable(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	runsRoot := t.TempDir()
	service, err := NewService(store, sched, func() string { return "unused" }, runsRoot)
	if err != nil {
		t.Fatal(err)
	}

	// Failed, with a real run directory and stderr content: stderr_tail
	// appears.
	newTerminalTask(t, store, "t-failed-with-run", tasks.Failed)
	run, err := (artifacts.Manager{Root: runsRoot}).Create("t-failed-with-run")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run.StderrPath, []byte("boom: fixture failure\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := service.Outcome(ctx, "t-failed-with-run")
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "process_failed" || result.RunDir == "" || result.StderrTail != "boom: fixture failure\n" {
		t.Fatalf("Outcome(failed, with run dir) = %+v", result)
	}

	// Failed, but the run directory is gone (e.g. swept by retention):
	// stderr_tail is gracefully absent, never an error.
	newTerminalTask(t, store, "t-failed-no-run", tasks.Failed)
	result, err = service.Outcome(ctx, "t-failed-no-run")
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "process_failed" || result.RunDir != "" || result.StderrTail != "" {
		t.Fatalf("Outcome(failed, no run dir) = %+v, want empty RunDir/StderrTail, no error", result)
	}

	// Succeeded, with a run directory and stderr content: stderr_tail must
	// still be absent, since outcome is not process_failed.
	newTerminalTask(t, store, "t-succeeded-with-run", tasks.Succeeded)
	if _, err := store.RecordOutcomeSummary(ctx, "t-succeeded-with-run", tasks.OutcomeSummary{CompleteSeen: true}); err != nil {
		t.Fatal(err)
	}
	run2, err := (artifacts.Manager{Root: runsRoot}).Create("t-succeeded-with-run")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run2.StderrPath, []byte("irrelevant\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err = service.Outcome(ctx, "t-succeeded-with-run")
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "completed" || result.StderrTail != "" {
		t.Fatalf("Outcome(succeeded, with run dir) = %+v, want StderrTail empty", result)
	}
}

// TestProgressSourcesNewFieldsFromRowAndZeroWhenAbsent covers C6's
// completion criterion for progress: the four row-sourced fields reflect
// what RecordOutcomeSummary last persisted, and are the documented
// zero/absent values when no summary was ever recorded.
func TestProgressSourcesNewFieldsFromRowAndZeroWhenAbsent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	runsRoot := t.TempDir()
	service, err := NewService(store, sched, func() string { return "unused" }, runsRoot)
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"t-progress-set", "t-progress-absent"} {
		if _, _, err := store.Create(ctx, tasks.Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Transition(ctx, id, tasks.Starting, "", nil); err != nil {
			t.Fatal(err)
		}
		run, err := (artifacts.Manager{Root: runsRoot}).Create(id)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(run.StdoutPath, []byte(""), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	lastEvent := time.Date(2026, 11, 19, 19, 0, 0, 0, time.UTC)
	if _, err := store.RecordOutcomeSummary(ctx, "t-progress-set", tasks.OutcomeSummary{
		ToolCallCount: 5, LastEventAt: lastEvent, StdoutTruncated: true, BytesDropped: 1024,
	}); err != nil {
		t.Fatal(err)
	}

	set, err := service.Progress(ctx, "t-progress-set")
	if err != nil {
		t.Fatal(err)
	}
	if set.ToolCallCount != 5 || !set.LastEventAt.Equal(lastEvent) || !set.StdoutTruncated || set.BytesDropped != 1024 {
		t.Fatalf("Progress(with summary) = %+v, want (5, %v, true, 1024)", set, lastEvent)
	}

	absent, err := service.Progress(ctx, "t-progress-absent")
	if err != nil {
		t.Fatal(err)
	}
	if absent.ToolCallCount != 0 || !absent.LastEventAt.IsZero() || absent.StdoutTruncated || absent.BytesDropped != 0 {
		t.Fatalf("Progress(no summary) = %+v, want all zero/absent", absent)
	}
}

// TestOutcomeSourcesCaptureStatsFromTaskRow covers status/result's row-backed
// capture accounting, including the documented zero values for an older row
// with no recorded live summary.
func TestOutcomeSourcesCaptureStatsFromTaskRow(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	sched, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, sched, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	newTerminalTask(t, store, "t-outcome-stats", tasks.Succeeded)
	if _, err := store.RecordOutcomeSummary(ctx, "t-outcome-stats", tasks.OutcomeSummary{
		CompleteSeen: true, StdoutTruncated: true, BytesDropped: 4096,
	}); err != nil {
		t.Fatal(err)
	}
	set, err := service.Outcome(ctx, "t-outcome-stats")
	if err != nil {
		t.Fatal(err)
	}
	if !set.StdoutTruncated || set.BytesDropped != 4096 {
		t.Fatalf("Outcome(with summary) capture stats = (%v, %d), want (true, 4096)", set.StdoutTruncated, set.BytesDropped)
	}

	newTerminalTask(t, store, "t-outcome-stats-absent", tasks.Succeeded)
	absent, err := service.Outcome(ctx, "t-outcome-stats-absent")
	if err != nil {
		t.Fatal(err)
	}
	if absent.StdoutTruncated || absent.BytesDropped != 0 {
		t.Fatalf("Outcome(no summary) capture stats = (%v, %d), want (false, 0)", absent.StdoutTruncated, absent.BytesDropped)
	}
}
