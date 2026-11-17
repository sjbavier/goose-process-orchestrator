//go:build linux

package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// openReconcileStore opens a fresh, migrated SQLite-backed tasks.Store, the
// concrete type Reconcile's signature requires (unlike Runner's own narrower
// Store interface, used elsewhere in this package's tests via memStore).
func openReconcileStore(t *testing.T) *tasks.Store {
	t.Helper()
	// _busy_timeout is a DSN-level setting (matches cmd/orchestrator/main.go
	// and internal/mcpserver's own test DSNs) so a brief writer/reader
	// overlap between this test's own polling and Reconcile's background
	// await/report goroutine retries instead of immediately returning
	// SQLITE_BUSY.
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "reconcile.db")+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := tasks.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return tasks.NewStore(db)
}

// createRunningTask drives a fresh task row through Queued->Starting->Running
// via the real store, optionally persisting a process identity partway
// through exactly where internal/orchestrator.Runner.Run does (immediately
// after a process exists, before the Running transition). A non-positive pid
// leaves the identity columns NULL, standing in for a task that never
// recorded one.
func createRunningTask(t *testing.T, store *tasks.Store, id string, pid, pgid int, startTicks int64) tasks.Task {
	t.Helper()
	ctx := context.Background()
	if _, _, err := store.Create(ctx, tasks.Task{ID: id, Recipe: "sleep.yaml", IdempotencyKey: id}); err != nil {
		t.Fatalf("create task %s: %v", id, err)
	}
	if _, err := store.Transition(ctx, id, tasks.Starting, "", nil); err != nil {
		t.Fatalf("transition %s to starting: %v", id, err)
	}
	if pid > 0 {
		if _, err := store.RecordProcessIdentity(ctx, id, pid, pgid, startTicks); err != nil {
			t.Fatalf("record identity for %s: %v", id, err)
		}
	}
	task, err := store.Transition(ctx, id, tasks.Running, "run-"+id, nil)
	if err != nil {
		t.Fatalf("transition %s to running: %v", id, err)
	}
	return task
}

// startRealFixture starts a real process group leader directly via os/exec
// (mirroring supervisor.Start's Setpgid invariant, but deliberately not
// going through this orchestrator's own Launcher/supervisor.Start), so its
// pid/pgid/start-ticks stand in for "a process that survived an orchestrator
// restart" — this test process never fork+exec'd it through the code under
// test. It reaps the process itself in a background goroutine, exactly as a
// real orchestrator process never does for a re-adopted one (reconciliation
// must poll, never wait(2), which this fixture setup exercises faithfully).
func startRealFixture(t *testing.T, script string) (pid int, startTicks int64) {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	pid = cmd.Process.Pid
	ticks, err := readStartTicks(pid)
	if err != nil {
		t.Fatalf("read start ticks for fixture pid %d: %v", pid, err)
	}
	waited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(waited) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		<-waited
	})
	return pid, ticks
}

// startAndReapFixture starts and fully waits out a short-lived fixture,
// returning its now-exited, already-reaped pid: a pid guaranteed not to
// exist by the time any caller uses it, standing in for "verifiably gone."
func startAndReapFixture(t *testing.T) (pid int) {
	t.Helper()
	cmd := exec.Command("sh", "-c", "exit 0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	pid = cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait fixture: %v", err)
	}
	return pid
}

// readStartTicks independently duplicates procstat_linux.go's
// /proc/<pid>/stat field-22 parse, rather than importing the unexported
// production helper (impossible from this package anyway), so this test's
// expected values are not tautologically derived from the same code path
// supervisor.VerifiedIdentity exercises.
func readStartTicks(pid int) (int64, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	line := string(raw)
	closeIdx := strings.LastIndex(line, ")")
	if closeIdx < 0 || closeIdx+1 >= len(line) {
		return 0, fmt.Errorf("parse /proc/%d/stat: no closing ) for comm field", pid)
	}
	rest := strings.Fields(line[closeIdx+1:])
	const starttimeIndex = 19
	if len(rest) <= starttimeIndex {
		return 0, fmt.Errorf("parse /proc/%d/stat: too few fields", pid)
	}
	return strconv.ParseInt(rest[starttimeIndex], 10, 64)
}

func waitForStatus(t *testing.T, store *tasks.Store, id string, want tasks.Status, timeout time.Duration) tasks.Task {
	t.Helper()
	deadline := time.After(timeout)
	for {
		task, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get task %s: %v", id, err)
		}
		if task.Status == want {
			return task
		}
		select {
		case <-deadline:
			t.Fatalf("task %s status = %q after %s, want %q", id, task.Status, timeout, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestReconcileReAdoptsAndCancelReachesIt covers Validation matrix case 1
// (re-adopt + cancel): a verified-live long-running fixture process must be
// re-registered by Reconcile so a subsequent explicit cancellation request
// reaches it through the normal await/report path.
func TestReconcileReAdoptsAndCancelReachesIt(t *testing.T) {
	store := openReconcileStore(t)
	pid, ticks := startRealFixture(t, "sleep 30")
	createRunningTask(t, store, "t-readopt-cancel", pid, pid, ticks)

	runner := New(nil, store)
	if err := Reconcile(context.Background(), store, runner); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Reconcile registers a re-adopted process synchronously before
	// returning, so the entry must already be present.
	runner.mu.Lock()
	_, registered := runner.processes["t-readopt-cancel"]
	runner.mu.Unlock()
	if !registered {
		t.Fatal("re-adopted task was not registered")
	}
	if task, err := store.Get(context.Background(), "t-readopt-cancel"); err != nil || task.Status != tasks.Running {
		t.Fatalf("re-adoption must not change status; got %+v, err=%v", task, err)
	}

	if _, err := store.RequestCancel(context.Background(), "t-readopt-cancel"); err != nil {
		t.Fatalf("request cancel: %v", err)
	}
	waitForStatus(t, store, "t-readopt-cancel", tasks.Cancelled, 5*time.Second)
}

// TestReconcileReAdoptsAndObservesNaturalExit covers Validation matrix case
// 1 (re-adopt + natural exit): a verified-live short-lived fixture process
// must be re-registered, and its real exit (observed only via polling, since
// it is not this process's child) must still be reported as Succeeded.
func TestReconcileReAdoptsAndObservesNaturalExit(t *testing.T) {
	store := openReconcileStore(t)
	// Long enough to still be alive when Reconcile verifies it, short enough
	// that polling observes its exit well within the test's deadline.
	pid, ticks := startRealFixture(t, "sleep 0.2")
	createRunningTask(t, store, "t-readopt-exit", pid, pid, ticks)

	runner := New(nil, store)
	if err := Reconcile(context.Background(), store, runner); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	waitForStatus(t, store, "t-readopt-exit", tasks.Succeeded, 10*time.Second)
}

// TestReconcileVerifiablyGoneTransitionsToInterrupted covers Validation
// matrix case 2: a pid that no longer exists at reconciliation time must be
// moved to Interrupted with a reason mentioning the pid, and never adopted.
func TestReconcileVerifiablyGoneTransitionsToInterrupted(t *testing.T) {
	store := openReconcileStore(t)
	pid := startAndReapFixture(t)
	createRunningTask(t, store, "t-gone", pid, pid, 1)

	runner := New(nil, store)
	if err := Reconcile(context.Background(), store, runner); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	task, err := store.Get(context.Background(), "t-gone")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != tasks.Interrupted {
		t.Fatalf("status = %q, want interrupted", task.Status)
	}
	if !strings.Contains(task.Result, fmt.Sprintf("%d", pid)) {
		t.Fatalf("reason %q does not mention pid %d", task.Result, pid)
	}

	runner.mu.Lock()
	_, registered := runner.processes["t-gone"]
	runner.mu.Unlock()
	if registered {
		t.Fatal("a verifiably gone pid must never be registered/adopted")
	}
}

// TestReconcileNoPersistedIdentityTransitionsToUnknown covers the fourth,
// simpler case this chunk's completion criteria call for: a non-terminal row
// with no persisted process identity at all is ambiguous, not "gone" — it is
// marked Unknown with a reason, never signaled.
func TestReconcileNoPersistedIdentityTransitionsToUnknown(t *testing.T) {
	store := openReconcileStore(t)
	createRunningTask(t, store, "t-no-identity", 0, 0, 0)

	runner := New(nil, store)
	if err := Reconcile(context.Background(), store, runner); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	task, err := store.Get(context.Background(), "t-no-identity")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != tasks.Unknown {
		t.Fatalf("status = %q, want unknown", task.Status)
	}
	if task.Result == "" {
		t.Fatal("expected a non-empty reason for the unknown classification")
	}

	runner.mu.Lock()
	_, registered := runner.processes["t-no-identity"]
	runner.mu.Unlock()
	if registered {
		t.Fatal("a row with no persisted identity must never be registered/adopted")
	}
}
