package mcpserver

import (
	"time"

	"testing"

	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// TestOutcomeOutputFullSummaryProducesExpectedFields covers C6's completion
// criterion: a seeded task row with a full summary produces the exact
// expected TaskOutput additions.
func TestOutcomeOutputFullSummaryProducesExpectedFields(t *testing.T) {
	created := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	started := created.Add(time.Second)
	ended := started.Add(2 * time.Minute)
	exit := 0
	truncated := true
	task := tasks.Task{
		ID: "t1", Status: tasks.Succeeded, Result: "run-dir-path",
		CreatedAt: created, StartedAt: &started, EndedAt: &ended, ExitCode: &exit,
		FinalText: "the answer", FinalTextTruncated: &truncated,
	}
	out := outcomeOutput(scheduler.OutcomeResult{
		Task: task, RunDir: "/runs/t1", Outcome: "completed",
		StdoutTruncated: true, BytesDropped: 2048,
	})

	if out.ID != "t1" || out.Status != "succeeded" || out.Result != "run-dir-path" {
		t.Fatalf("base fields = %+v, want unchanged from output(t)", out)
	}
	if out.RunDir != "/runs/t1" || out.Outcome != "completed" {
		t.Fatalf("RunDir/Outcome = (%q, %q), want (/runs/t1, completed)", out.RunDir, out.Outcome)
	}
	if out.FinalText != "the answer" || !out.FinalTextTruncated {
		t.Fatalf("FinalText/FinalTextTruncated = (%q, %v), want (the answer, true)", out.FinalText, out.FinalTextTruncated)
	}
	if !out.StdoutTruncated || out.BytesDropped != 2048 {
		t.Fatalf("capture stats = (%v, %d), want (true, 2048)", out.StdoutTruncated, out.BytesDropped)
	}
	if out.ExitCode == nil || *out.ExitCode != 0 {
		t.Fatalf("ExitCode = %v, want 0", out.ExitCode)
	}
	if out.CreatedAt != created.Format(time.RFC3339) || out.StartedAt != started.Format(time.RFC3339) || out.EndedAt != ended.Format(time.RFC3339) {
		t.Fatalf("timestamps = (%q, %q, %q)", out.CreatedAt, out.StartedAt, out.EndedAt)
	}
}

// TestOutcomeOutputAbsentSummaryProducesDocumentedFallback covers C6's
// completion criterion: a seeded row with all-NULL summary columns produces
// the documented absent/fallback values.
func TestOutcomeOutputAbsentSummaryProducesDocumentedFallback(t *testing.T) {
	created := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	task := tasks.Task{ID: "t2", Status: tasks.Succeeded, CreatedAt: created}
	out := outcomeOutput(scheduler.OutcomeResult{Task: task, Outcome: "no_complete_event"})

	if out.FinalText != "" || out.FinalTextTruncated {
		t.Fatalf("FinalText/FinalTextTruncated = (%q, %v), want empty/false", out.FinalText, out.FinalTextTruncated)
	}
	if out.Outcome != "no_complete_event" {
		t.Fatalf("Outcome = %q, want no_complete_event", out.Outcome)
	}
	if out.ExitCode != nil {
		t.Fatalf("ExitCode = %v, want nil", out.ExitCode)
	}
	if out.StartedAt != "" || out.EndedAt != "" {
		t.Fatalf("StartedAt/EndedAt = (%q, %q), want both empty", out.StartedAt, out.EndedAt)
	}
	if out.RunDir != "" || out.StderrTail != "" {
		t.Fatalf("RunDir/StderrTail = (%q, %q), want both empty", out.RunDir, out.StderrTail)
	}
}

// TestOutcomeOutputIsEmptyForEveryNonTerminalAndCancelledStatus covers C6's
// completion criterion that outcome is empty for every non-terminal status
// and for Cancelled.
func TestOutcomeOutputIsEmptyForEveryNonTerminalAndCancelledStatus(t *testing.T) {
	for _, status := range []tasks.Status{tasks.Queued, tasks.Starting, tasks.Running, tasks.Cancelled, tasks.Interrupted, tasks.Unknown} {
		out := outcomeOutput(scheduler.OutcomeResult{Task: tasks.Task{ID: "t", Status: status}, Outcome: ""})
		if out.Outcome != "" {
			t.Fatalf("status %q: Outcome = %q, want empty", status, out.Outcome)
		}
	}
}

// TestProgressOutputIncludesNewFieldsAndOmitsWhenAbsent covers C6's
// completion criterion for progress's new fields at the output-construction
// layer.
func TestProgressOutputIncludesNewFieldsAndOmitsWhenAbsent(t *testing.T) {
	lastEvent := time.Date(2026, 11, 19, 19, 0, 0, 0, time.UTC)
	full := progressOutput(scheduler.ProgressResult{ToolCallCount: 3, LastEventAt: lastEvent, StdoutTruncated: true, BytesDropped: 99})
	if full.ToolCallCount != 3 || full.LastEventAt != lastEvent.Format(time.RFC3339) || !full.StdoutTruncated || full.BytesDropped != 99 {
		t.Fatalf("progressOutput(full) = %+v", full)
	}

	empty := progressOutput(scheduler.ProgressResult{})
	if empty.ToolCallCount != 0 || empty.LastEventAt != "" || empty.StdoutTruncated || empty.BytesDropped != 0 {
		t.Fatalf("progressOutput(empty) = %+v, want all zero/empty", empty)
	}
}

// TestProgressOutputLastEventAtRendersUTC covers CR-018: a LastEventAt
// carrying a non-UTC Location still renders with a trailing Z, not a
// numeric offset.
func TestProgressOutputLastEventAtRendersUTC(t *testing.T) {
	local := time.FixedZone("UTC-5", -5*60*60)
	lastEvent := time.Date(2026, 11, 19, 19, 0, 0, 0, local)
	out := progressOutput(scheduler.ProgressResult{LastEventAt: lastEvent})
	if want := lastEvent.UTC().Format(time.RFC3339); out.LastEventAt != want {
		t.Fatalf("LastEventAt = %q, want %q", out.LastEventAt, want)
	}
	if out.LastEventAt == "" || out.LastEventAt[len(out.LastEventAt)-1] != 'Z' {
		t.Fatalf("LastEventAt = %q, want trailing Z", out.LastEventAt)
	}
}
