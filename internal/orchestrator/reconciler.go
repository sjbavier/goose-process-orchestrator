package orchestrator

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/local/goose-process-orchestrator/internal/supervisor"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// reconcileCancelGrace bounds the TERM-then-KILL grace period Cancel uses for
// a re-adopted process, matching the default internal/goose.Runner.Cancel
// already uses for a freshly started one (there is no per-task Launcher
// configuration available for a process this orchestrator instance did not
// start itself).
const reconcileCancelGrace = 5 * time.Second

// Reconcile revisits every non-terminal task row against real OS process
// state — intended to run once at startup, before the scheduler begins
// claiming new work — so that a child process that survived an orchestrator
// crash/restart is re-adopted and remains cancellable/observable, a child
// that is verifiably gone is moved to a terminal state with an explicit
// reason, and a row whose process identity cannot be positively verified is
// never signaled and is marked Unknown with a reason instead. It never
// signals a pid supervisor.VerifiedIdentity has not confirmed.
//
// Reconcile returns once every row has been classified and, for a re-adopted
// row, registered into runner's processes map; a reconciled process's
// eventual exit/cancellation is awaited and reported in its own goroutine
// (mirroring how scheduler.worker already runs Runner.Run), so Reconcile
// itself does not block on any reconciled task's eventual outcome.
func Reconcile(ctx context.Context, store *tasks.Store, runner *Runner) error {
	rows, err := store.ListNonTerminal(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: list non-terminal tasks: %w", err)
	}
	for _, row := range rows {
		reconcileOne(ctx, store, runner, row)
	}
	return nil
}

func reconcileOne(ctx context.Context, store *tasks.Store, runner *Runner, row tasks.Task) {
	if row.PID == nil || row.PGID == nil || row.ProcessStartTicks == nil {
		transitionReconciled(ctx, store, row.ID, tasks.Unknown,
			fmt.Sprintf("task %s: no persisted process identity recorded", row.ID))
		return
	}
	pid, pgid, startTicks := *row.PID, *row.PGID, *row.ProcessStartTicks

	if supervisor.VerifiedIdentity(pid, pgid, startTicks) {
		adopted, err := supervisor.Adopt(pid, pgid, startTicks)
		if err != nil {
			// Identity could have stopped verifying in the instant between the
			// check above and Adopt's own re-check; fail closed to Unknown
			// rather than guess, exactly as an ambiguous identity would.
			transitionReconciled(ctx, store, row.ID, tasks.Unknown,
				fmt.Sprintf("task %s: pid %d: adopt failed after verification: %v", row.ID, pid, err))
			return
		}
		cancel := func(cancelCtx context.Context) supervisor.Result {
			return adopted.Cancel(cancelCtx, reconcileCancelGrace)
		}
		runner.register(row.ID, adopted)
		var readopted expectation
		if row.StartedAt != nil {
			readopted.deadline = deadlineFor(*row.StartedAt, row.TimeoutSeconds)
		}
		go func() {
			defer runner.unregister(row.ID)
			// live is nil: a re-adopted process never had Start called again
			// in this process's lifetime, so it structurally has no live
			// parser attached — the new outcome-summary columns stay NULL
			// for it, per decision D.
			runner.awaitAndReport(ctx, row.ID, adopted, row.Result, nil, readopted, cancel)
		}()
		return
	}

	if !processExists(pid) {
		transitionReconciled(ctx, store, row.ID, tasks.Interrupted,
			fmt.Sprintf("task %s: process %d no longer exists after orchestrator restart", row.ID, pid))
		return
	}

	transitionReconciled(ctx, store, row.ID, tasks.Unknown,
		fmt.Sprintf("task %s: pid %d exists but its process identity does not match the persisted record", row.ID, pid))
}

// transitionReconciled records a reconciliation classification's outcome,
// logging rather than failing the whole pass if one row's transition is
// rejected (e.g. an already-visited Interrupted/Unknown row whose state
// machine does not permit a further transition) — one row's audit-trail
// oddity must not abandon reconciling every other row.
func transitionReconciled(ctx context.Context, store *tasks.Store, id string, status tasks.Status, reason string) {
	if _, err := store.Transition(ctx, id, status, reason, nil); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: reconcile task %s: record %s status: %v\n", id, status, err)
	}
}

// processExists reports whether pid still refers to a live process, using
// the standard Unix convention of sending signal 0: this delivers no actual
// signal and changes nothing about the target process, only checking
// existence (and permission), so it does not violate "never signal an
// unverified pid." It is used only to choose between Interrupted
// ("verifiably gone") and Unknown ("ambiguous": pid exists but identity
// doesn't match) once VerifiedIdentity has already failed.
func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
