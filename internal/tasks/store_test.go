package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"os"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestTaskAuditFieldsAndFailClosedStatus(t *testing.T) {
	now := time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)
	task := Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1", Status: Queued, CreatedAt: now}
	if err := task.RequestCancel(now); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if task.CancelRequestedAt == nil {
		t.Fatal("cancellation intent was not recorded")
	}
	if err := task.Transition(Unknown, now.Add(time.Second)); err != nil {
		t.Fatalf("Transition(unknown): %v", err)
	}
	if err := task.RequestCancel(now.Add(2 * time.Second)); err == nil {
		t.Fatal("unknown state must fail closed")
	}
	if knownStatus(Status("invented")) {
		t.Fatal("unknown persisted status must not be accepted")
	}
}

func TestInitialMigrationHasRequiredAuditAndSafetyColumns(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/001_tasks.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	for _, required := range []string{"idempotency_key TEXT NOT NULL UNIQUE", "cancel_requested_at", "exit_code", "interrupted", "unknown", "tasks_claim_queue"} {
		if !strings.Contains(text, required) {
			t.Errorf("migration missing %q", required)
		}
	}
	if !strings.Contains(text, "Forward-only") {
		t.Fatal("migration must document forward-only safety")
	}
}

func openSQLiteStore(t *testing.T) (*sql.DB, *Store) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate empty database: %v", err)
	}
	return db, NewStore(db)
}

func TestStoreSQLiteMigrationIdempotencyAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate empty database: %v", err)
	}
	store := NewStore(db)
	ctx := context.Background()
	created, inserted, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"})
	if err != nil || !inserted || created.Status != Queued {
		t.Fatalf("Create initial = (%+v, %t, %v)", created, inserted, err)
	}
	existing, inserted, err := store.Create(ctx, Task{ID: "ignored", Recipe: "allowed.yaml", IdempotencyKey: "key-1"})
	if err != nil || inserted || existing.ID != created.ID {
		t.Fatalf("Create equivalent = (%+v, %t, %v)", existing, inserted, err)
	}
	if _, _, err := store.Create(ctx, Task{ID: "conflict", Recipe: "other.yaml", IdempotencyKey: "key-1"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("Create conflicting = %v, want ErrIdempotencyConflict", err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil || claimed == nil || claimed.ID != created.ID || claimed.Status != Starting {
		t.Fatalf("Claim = (%+v, %v)", claimed, err)
	}
	if _, err := store.Transition(ctx, created.ID, Running, "", nil); err != nil {
		t.Fatalf("Transition running: %v", err)
	}
	exit := 0
	if _, err := store.Transition(ctx, created.ID, Succeeded, "done", &exit); err != nil {
		t.Fatalf("Transition succeeded: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	persisted, err := NewStore(db).Get(ctx, created.ID)
	if err != nil || persisted.Status != Succeeded || persisted.Result != "done" || persisted.ExitCode == nil || *persisted.ExitCode != 0 || persisted.EndedAt == nil {
		t.Fatalf("reopened task = (%+v, %v)", persisted, err)
	}
}

func TestStoreSQLiteConcurrentClaimIsAtomic(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	const claimers = 4
	claims := make(chan *Task, claimers)
	errs := make(chan error, claimers)
	for range claimers {
		go func() { task, err := store.Claim(ctx); claims <- task; errs <- err }()
	}
	count := 0
	for range claimers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Claim: %v", err)
		}
		if task := <-claims; task != nil {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("claimed %d tasks, want exactly one", count)
	}
}

// TestTransitionSurvivesConcurrentReads proves Transition no longer silently
// loses a status update under a concurrent reader. Before this fix, a
// Store.Get racing Store.Transition's commit under SQLite's default
// rollback-journal locking could return SQLITE_BUSY from Transition, which
// callers like internal/orchestrator.Runner discarded, leaving a task
// permanently stuck at its prior status even though the real process kept
// running. Transition now shares Claim's bounded busy/locked retry.
func TestTransitionSurvivesConcurrentReads(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = store.Get(ctx, fmt.Sprintf("task-%d", i%50))
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() { close(stop); <-done }()

	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("task-%d", i)
		if _, _, err := store.Create(ctx, Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Claim(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Transition(ctx, id, Running, "", nil); err != nil {
			t.Fatalf("Transition raced a concurrent reader and lost: %v", err)
		}
		got, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != Running {
			t.Fatalf("status=%q after Transition(Running), want running", got.Status)
		}
	}
}

func TestStoreListFiltersAndLimits(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()
	for _, id := range []string{"task-1", "task-2", "task-3"} {
		if _, _, err := store.Create(ctx, Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Transition(ctx, "task-2", Starting, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, "task-2", Running, "", nil); err != nil {
		t.Fatal(err)
	}

	all, err := store.List(ctx, ListFilter{Limit: 10})
	if err != nil || len(all) != 3 {
		t.Fatalf("List(all) = (%d, %v), want 3 tasks", len(all), err)
	}
	// created_at ties resolve to id DESC.
	if all[0].ID != "task-3" || all[1].ID != "task-2" || all[2].ID != "task-1" {
		t.Fatalf("List(all) order = %v, want task-3, task-2, task-1", []string{all[0].ID, all[1].ID, all[2].ID})
	}

	limited, err := store.List(ctx, ListFilter{Limit: 2})
	if err != nil || len(limited) != 2 {
		t.Fatalf("List(limit=2) = (%d, %v), want 2 tasks", len(limited), err)
	}

	running := Running
	filtered, err := store.List(ctx, ListFilter{Status: &running, Limit: 10})
	if err != nil || len(filtered) != 1 || filtered[0].ID != "task-2" {
		t.Fatalf("List(status=running) = (%+v, %v), want only task-2", filtered, err)
	}

	queued := Queued
	queuedOnly, err := store.List(ctx, ListFilter{Status: &queued, Limit: 10})
	if err != nil || len(queuedOnly) != 2 {
		t.Fatalf("List(status=queued) = (%d, %v), want 2 tasks", len(queuedOnly), err)
	}
}

func TestRecordProcessIdentityPersistsAndRoundTripsThroughGetAndList(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(ctx, Task{ID: "task-2", Recipe: "allowed.yaml", IdempotencyKey: "key-2"}); err != nil {
		t.Fatal(err)
	}

	updated, err := store.RecordProcessIdentity(ctx, "task-1", 4242, 4242, 123456789)
	if err != nil {
		t.Fatalf("RecordProcessIdentity: %v", err)
	}
	if updated.PID == nil || *updated.PID != 4242 {
		t.Fatalf("PID = %v, want 4242", updated.PID)
	}
	if updated.PGID == nil || *updated.PGID != 4242 {
		t.Fatalf("PGID = %v, want 4242", updated.PGID)
	}
	if updated.ProcessStartTicks == nil || *updated.ProcessStartTicks != 123456789 {
		t.Fatalf("ProcessStartTicks = %v, want 123456789", updated.ProcessStartTicks)
	}

	// Round-trip through Get: identity set.
	got, err := store.Get(ctx, "task-1")
	if err != nil {
		t.Fatalf("Get(task-1): %v", err)
	}
	if got.PID == nil || *got.PID != 4242 || got.PGID == nil || *got.PGID != 4242 || got.ProcessStartTicks == nil || *got.ProcessStartTicks != 123456789 {
		t.Fatalf("Get(task-1) identity = (pid=%v, pgid=%v, ticks=%v), want all set to 4242/4242/123456789", got.PID, got.PGID, got.ProcessStartTicks)
	}

	// Round-trip through Get: pre-existing row with NULL identity decodes to
	// nil pointers, never a zero-value false positive.
	untouched, err := store.Get(ctx, "task-2")
	if err != nil {
		t.Fatalf("Get(task-2): %v", err)
	}
	if untouched.PID != nil || untouched.PGID != nil || untouched.ProcessStartTicks != nil {
		t.Fatalf("Get(task-2) identity = (pid=%v, pgid=%v, ticks=%v), want all nil", untouched.PID, untouched.PGID, untouched.ProcessStartTicks)
	}

	// Round-trip through List: same two expectations.
	all, err := store.List(ctx, ListFilter{Limit: 10})
	if err != nil || len(all) != 2 {
		t.Fatalf("List(all) = (%d, %v), want 2 tasks", len(all), err)
	}
	byID := map[string]Task{all[0].ID: all[0], all[1].ID: all[1]}
	if task1 := byID["task-1"]; task1.PID == nil || *task1.PID != 4242 {
		t.Fatalf("List task-1 PID = %v, want 4242", task1.PID)
	}
	if task2 := byID["task-2"]; task2.PID != nil || task2.PGID != nil || task2.ProcessStartTicks != nil {
		t.Fatalf("List task-2 identity = (pid=%v, pgid=%v, ticks=%v), want all nil", task2.PID, task2.PGID, task2.ProcessStartTicks)
	}

	if _, err := store.RecordProcessIdentity(ctx, "no-such-task", 1, 1, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RecordProcessIdentity(missing id) = %v, want ErrNotFound", err)
	}
}

func TestListNonTerminalExcludesOnlyTerminalStatuses(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()

	// One task per status reachable by Create+Transition, covering every
	// member of Status.Terminal()'s set plus every non-terminal status,
	// including Interrupted/Unknown which Status.Terminal() deliberately
	// excludes.
	makeTask := func(id string) {
		if _, _, err := store.Create(ctx, Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
			t.Fatalf("Create(%s): %v", id, err)
		}
	}
	transition := func(id string, next Status) {
		if _, err := store.Transition(ctx, id, next, "", nil); err != nil {
			t.Fatalf("Transition(%s, %s): %v", id, next, err)
		}
	}

	makeTask("queued-1") // left at Queued

	makeTask("starting-1")
	transition("starting-1", Starting)

	makeTask("running-1")
	transition("running-1", Starting)
	transition("running-1", Running)

	makeTask("succeeded-1")
	transition("succeeded-1", Starting)
	transition("succeeded-1", Running)
	transition("succeeded-1", Succeeded)

	makeTask("failed-1")
	transition("failed-1", Starting)
	transition("failed-1", Failed)

	makeTask("timedout-1")
	transition("timedout-1", Starting)
	transition("timedout-1", Running)
	transition("timedout-1", TimedOut)

	makeTask("cancelled-1")
	transition("cancelled-1", Cancelled)

	makeTask("interrupted-1")
	transition("interrupted-1", Interrupted)

	makeTask("unknown-1")
	transition("unknown-1", Unknown)

	nonTerminal, err := store.ListNonTerminal(ctx)
	if err != nil {
		t.Fatalf("ListNonTerminal: %v", err)
	}
	gotIDs := make(map[string]bool, len(nonTerminal))
	for _, task := range nonTerminal {
		if task.Status.Terminal() {
			t.Fatalf("ListNonTerminal returned terminal task %s with status %q", task.ID, task.Status)
		}
		gotIDs[task.ID] = true
	}
	wantIDs := []string{"queued-1", "starting-1", "running-1", "interrupted-1", "unknown-1"}
	if len(nonTerminal) != len(wantIDs) {
		t.Fatalf("ListNonTerminal returned %d tasks, want %d: got %v", len(nonTerminal), len(wantIDs), gotIDs)
	}
	for _, id := range wantIDs {
		if !gotIDs[id] {
			t.Fatalf("ListNonTerminal missing expected non-terminal task %s", id)
		}
	}
}

// TestStoreNormalizesAndRoundTripsInboxPolicy proves Create normalizes an
// empty InboxPolicy to ControllerOnly before any row is written, persists an
// explicitly requested OpenInbox, and includes the normalized policy in
// idempotency-key equivalence exactly like Parameters: the same key replayed
// with a different normalized policy is a conflict, not a silent reuse.
func TestStoreNormalizesAndRoundTripsInboxPolicy(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()

	defaulted, inserted, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"})
	if err != nil || !inserted || defaulted.InboxPolicy != ControllerOnly {
		t.Fatalf("Create with empty InboxPolicy = (%+v, %t, %v), want normalized ControllerOnly", defaulted, inserted, err)
	}
	persisted, err := store.Get(ctx, defaulted.ID)
	if err != nil || persisted.InboxPolicy != ControllerOnly {
		t.Fatalf("Get after empty InboxPolicy = (%+v, %v), want ControllerOnly", persisted, err)
	}

	open, inserted, err := store.Create(ctx, Task{ID: "task-2", Recipe: "allowed.yaml", IdempotencyKey: "key-2", InboxPolicy: OpenInbox})
	if err != nil || !inserted || open.InboxPolicy != OpenInbox {
		t.Fatalf("Create with OpenInbox = (%+v, %t, %v), want OpenInbox", open, inserted, err)
	}
	persisted, err = store.Get(ctx, open.ID)
	if err != nil || persisted.InboxPolicy != OpenInbox {
		t.Fatalf("Get after OpenInbox = (%+v, %v), want OpenInbox", persisted, err)
	}

	// Same key, same normalized policy (explicit ControllerOnly == the
	// empty-value default already persisted) returns the original task.
	existing, inserted, err := store.Create(ctx, Task{ID: "ignored", Recipe: "allowed.yaml", IdempotencyKey: "key-1", InboxPolicy: ControllerOnly})
	if err != nil || inserted || existing.ID != defaulted.ID {
		t.Fatalf("Create with same normalized policy = (%+v, %t, %v), want reuse of %s", existing, inserted, err, defaulted.ID)
	}

	// Same key, different normalized policy is a conflict.
	if _, _, err := store.Create(ctx, Task{ID: "conflict", Recipe: "allowed.yaml", IdempotencyKey: "key-1", InboxPolicy: OpenInbox}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("Create with changed InboxPolicy = %v, want ErrIdempotencyConflict", err)
	}
}

// TestNewStoreClockAdvances guards against the clock being a method value
// (time.Now().UTC), which evaluates time.Now() once and then returns that same
// instant forever, freezing every stored timestamp at process start.
func TestNewStoreClockAdvances(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()

	first, _, err := store.Create(ctx, Task{ID: "clock-1", Recipe: "allowed.yaml", IdempotencyKey: "clock-1"})
	if err != nil {
		t.Fatalf("Create(clock-1): %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	second, _, err := store.Create(ctx, Task{ID: "clock-2", Recipe: "allowed.yaml", IdempotencyKey: "clock-2"})
	if err != nil {
		t.Fatalf("Create(clock-2): %v", err)
	}
	if !second.CreatedAt.After(first.CreatedAt) {
		t.Fatalf("CreatedAt did not advance: first=%s second=%s", first.CreatedAt, second.CreatedAt)
	}
}

// TestRecordOutcomeSummaryRoundTripsEveryFieldAndLeavesOtherColumnsUntouched
// round-trips every new field including the zero/false/empty case, and
// diffs a full row before/after on fields outside the new ones, per C3's
// focused checks.
func TestRecordOutcomeSummaryRoundTripsEveryFieldAndLeavesOtherColumnsUntouched(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()
	created, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1", MaxTurns: 7, Parameters: map[string]string{"a": "1"}})
	if err != nil {
		t.Fatal(err)
	}

	lastEvent := time.Date(2026, 11, 19, 18, 0, 0, 0, time.UTC)
	summary := OutcomeSummary{
		FinalText: "the assistant's last message", FinalTextTruncated: true,
		Outcome: "stalled_max_turns", CompleteSeen: false,
		TotalTokens: 123, InputTokens: 100, OutputTokens: 23, CacheReadTokens: 5, CacheWriteTokens: 6,
		CostUSD: 0.042, ToolCallCount: 9, LastEventAt: lastEvent, StalledMaxTurns: true,
		StdoutTruncated: true, BytesDropped: 4096,
	}
	updated, err := store.RecordOutcomeSummary(ctx, "task-1", summary)
	if err != nil {
		t.Fatalf("RecordOutcomeSummary: %v", err)
	}
	assertSummary := func(t *testing.T, got Task) {
		t.Helper()
		if got.FinalText != summary.FinalText || got.FinalTextTruncated == nil || *got.FinalTextTruncated != true {
			t.Fatalf("FinalText/FinalTextTruncated = (%q, %v), want (%q, true)", got.FinalText, got.FinalTextTruncated, summary.FinalText)
		}
		if got.Outcome != summary.Outcome || got.CompleteSeen == nil || *got.CompleteSeen != false {
			t.Fatalf("Outcome/CompleteSeen = (%q, %v), want (%q, false)", got.Outcome, got.CompleteSeen, summary.Outcome)
		}
		if got.TotalTokens == nil || *got.TotalTokens != 123 || got.InputTokens == nil || *got.InputTokens != 100 || got.OutputTokens == nil || *got.OutputTokens != 23 {
			t.Fatalf("token fields = (%v, %v, %v), want (123, 100, 23)", got.TotalTokens, got.InputTokens, got.OutputTokens)
		}
		if got.CacheReadTokens == nil || *got.CacheReadTokens != 5 || got.CacheWriteTokens == nil || *got.CacheWriteTokens != 6 {
			t.Fatalf("cache token fields = (%v, %v), want (5, 6)", got.CacheReadTokens, got.CacheWriteTokens)
		}
		if got.CostUSD == nil || *got.CostUSD != 0.042 {
			t.Fatalf("CostUSD = %v, want 0.042", got.CostUSD)
		}
		if got.ToolCallCount == nil || *got.ToolCallCount != 9 {
			t.Fatalf("ToolCallCount = %v, want 9", got.ToolCallCount)
		}
		if got.LastEventAt == nil || !got.LastEventAt.Equal(lastEvent) {
			t.Fatalf("LastEventAt = %v, want %v", got.LastEventAt, lastEvent)
		}
		if got.StalledMaxTurns == nil || *got.StalledMaxTurns != true {
			t.Fatalf("StalledMaxTurns = %v, want true", got.StalledMaxTurns)
		}
		if got.StdoutTruncated == nil || *got.StdoutTruncated != true {
			t.Fatalf("StdoutTruncated = %v, want true", got.StdoutTruncated)
		}
		if got.BytesDropped == nil || *got.BytesDropped != 4096 {
			t.Fatalf("BytesDropped = %v, want 4096", got.BytesDropped)
		}
	}
	assertSummary(t, updated)
	reGot, err := store.Get(ctx, "task-1")
	if err != nil {
		t.Fatalf("Get after RecordOutcomeSummary: %v", err)
	}
	assertSummary(t, reGot)

	// Every pre-existing column outside the new summary fields must be
	// byte-identical to the row Create produced.
	if reGot.ID != created.ID || reGot.Recipe != created.Recipe || reGot.IdempotencyKey != created.IdempotencyKey ||
		reGot.Status != created.Status || !reGot.CreatedAt.Equal(created.CreatedAt) || reGot.MaxTurns != created.MaxTurns {
		t.Fatalf("RecordOutcomeSummary altered an unrelated column: before=%+v after=%+v", created, reGot)
	}
	if len(reGot.Parameters) != 1 || reGot.Parameters["a"] != "1" {
		t.Fatalf("RecordOutcomeSummary altered Parameters: %+v", reGot.Parameters)
	}

	// The zero/false/empty case: a second task that never had a summary
	// recorded must decode every new field as nil/empty, never a zero-value
	// false positive.
	if _, _, err := store.Create(ctx, Task{ID: "task-2", Recipe: "allowed.yaml", IdempotencyKey: "key-2"}); err != nil {
		t.Fatal(err)
	}
	untouched, err := store.Get(ctx, "task-2")
	if err != nil {
		t.Fatal(err)
	}
	if untouched.FinalText != "" || untouched.FinalTextTruncated != nil || untouched.Outcome != "" || untouched.CompleteSeen != nil ||
		untouched.TotalTokens != nil || untouched.InputTokens != nil || untouched.OutputTokens != nil ||
		untouched.CacheReadTokens != nil || untouched.CacheWriteTokens != nil || untouched.CostUSD != nil ||
		untouched.ToolCallCount != nil || untouched.LastEventAt != nil || untouched.StalledMaxTurns != nil ||
		untouched.StdoutTruncated != nil || untouched.BytesDropped != nil {
		t.Fatalf("task with no recorded summary has non-absent summary fields: %+v", untouched)
	}

	if _, err := store.RecordOutcomeSummary(ctx, "no-such-task", OutcomeSummary{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RecordOutcomeSummary(missing id) = %v, want ErrNotFound", err)
	}
}

func TestStoreListFiltersByLiteralIdempotencyKeyPrefix(t *testing.T) {
	_, store := openSQLiteStore(t)
	ctx := context.Background()
	for i, key := range []string{"ENG-29341:plan", "ENG-29341:build", "ENGX29341:other"} {
		id := fmt.Sprintf("prefix-%d", i)
		if _, _, err := store.Create(ctx, Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: key}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.List(ctx, ListFilter{KeyPrefix: "ENG-29341:", Limit: 10})
	if err != nil || len(got) != 2 {
		t.Fatalf("List(key prefix) = (%+v, %v), want 2", got, err)
	}
	got, err = store.List(ctx, ListFilter{KeyPrefix: "ENG%", Limit: 10})
	if err != nil || len(got) != 0 {
		t.Fatalf("wildcard-like prefix must be literal: (%+v, %v)", got, err)
	}
}
