// Package orchestrator adapts the local Goose launcher to the scheduler's
// Runner seam and keeps an in-memory registry so explicit cancellation
// intent recorded in the store can reach the owned OS process group.
package orchestrator

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// CancelPoll bounds how often a running task's cancellation intent is
// rechecked. No notification mechanism exists; this stays a short, bounded
// poll rather than a schema or interface change.
const CancelPoll = 200 * time.Millisecond

// Store is the subset of tasks.Store this adapter needs.
type Store interface {
	Get(ctx context.Context, id string) (tasks.Task, error)
	Transition(ctx context.Context, id string, next tasks.Status, resultText string, exitCode *int) (tasks.Task, error)
}

// Launcher starts and cancels one owned OS process for a validated request.
// internal/goose.Runner satisfies this directly.
type Launcher interface {
	Start(request goose.Request, taskID string) (*supervisor.Process, artifacts.Run, error)
	Cancel(ctx context.Context, p *supervisor.Process) supervisor.Result
}

// handle is the minimal process-handle contract register/resume need: the
// shape common to both a freshly started *supervisor.Process (this process's
// real child) and a re-adopted *supervisor.ReconciledProcess (a process that
// survived an orchestrator restart and is tracked by polling instead). Both
// already satisfy this via their existing Wait method; no change to either
// type was needed.
type handle interface {
	Wait() supervisor.Result
}

// Runner implements scheduler.Runner over a Launcher and Store. It never
// reports success without a real observed exit, and it owns no process past
// the exit it observes.
type Runner struct {
	Launcher Launcher
	Store    Store
	// Tokens, when non-nil, mints one mailbox token per task at the moment
	// it actually starts (not at admission, so a task that never starts
	// never mints one) and forwards it to goose.Request.MailboxToken; the
	// token is revoked once the process reaches a terminal state. Nil (the
	// default, and every existing caller/test) disables the mailbox
	// entirely: no token, no --with-extension, no env var.
	Tokens *mailbox.Tokens

	mu        sync.Mutex
	processes map[string]handle
}

func New(launcher Launcher, store Store) *Runner {
	return &Runner{Launcher: launcher, Store: store, processes: make(map[string]handle)}
}

// Run drives one claimed task to a terminal state. It is the scheduler.Runner
// seam; the caller (scheduler.worker) does not inspect its return.
func (r *Runner) Run(ctx context.Context, task tasks.Task) {
	request := goose.Request{Recipe: task.Recipe, MaxTurns: task.MaxTurns, Parameters: task.Parameters}
	if r.Tokens != nil {
		token, err := r.Tokens.Mint(task.ID)
		if err != nil {
			r.report(ctx, task.ID, tasks.Failed, err.Error(), nil)
			return
		}
		request.MailboxToken = token
		defer r.Tokens.Revoke(task.ID)
	}
	p, run, err := r.Launcher.Start(request, task.ID)
	if err != nil {
		r.report(ctx, task.ID, tasks.Failed, err.Error(), nil)
		return
	}

	// A store-level race on this transition should not abandon an
	// already-started process; await its real exit regardless. Store.Transition
	// already retries SQLite busy/locked outcomes; a remaining error here is
	// unexpected and worth surfacing, not silently losing.
	if _, err := r.Store.Transition(ctx, task.ID, tasks.Running, "", nil); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record running status: %v\n", task.ID, err)
	}

	r.resume(ctx, task.ID, p, run.Dir, func(cancelCtx context.Context) supervisor.Result {
		return r.Launcher.Cancel(cancelCtx, p)
	})
}

// resume registers p as the process backing task id, awaits its real exit
// (cancelling via the supplied cancel callback the first time explicit
// cancellation intent appears in the store), and reports the resulting
// terminal status to the store using resultText as the stored result. It is
// the shared register->await->report tail used by Run for a freshly started
// process, and by Reconcile (via awaitAndReport, below) for a re-adopted one.
func (r *Runner) resume(ctx context.Context, id string, p handle, resultText string, cancel func(context.Context) supervisor.Result) {
	r.register(id, p)
	defer r.unregister(id)
	r.awaitAndReport(ctx, id, p, resultText, cancel)
}

// awaitAndReport is resume's await+report half, split out so a caller that
// must guarantee synchronous registration (Reconcile, so a process is
// immediately cancellable once it returns) can call register itself and run
// just this half in its own goroutine.
func (r *Runner) awaitAndReport(ctx context.Context, id string, p handle, resultText string, cancel func(context.Context) supervisor.Result) {
	result := r.await(ctx, id, p, cancel)

	status := tasks.Succeeded
	switch {
	case result.Cancelled:
		status = tasks.Cancelled
	case result.ExitCode != 0:
		status = tasks.Failed
	}
	exit := result.ExitCode
	r.report(ctx, id, status, resultText, &exit)
}

// await blocks for the process's real exit, cancelling it the first time
// explicit cancellation intent appears in the store while it is still running.
func (r *Runner) await(ctx context.Context, id string, p handle, cancel func(context.Context) supervisor.Result) supervisor.Result {
	done := make(chan supervisor.Result, 1)
	go func() { done <- p.Wait() }()

	ticker := time.NewTicker(CancelPoll)
	defer ticker.Stop()
	for {
		select {
		case result := <-done:
			return result
		case <-ticker.C:
			current, err := r.Store.Get(ctx, id)
			if err == nil && current.CancelRequestedAt != nil {
				return cancel(context.Background())
			}
		}
	}
}

func (r *Runner) report(ctx context.Context, id string, status tasks.Status, resultText string, exitCode *int) {
	if _, err := r.Store.Transition(ctx, id, status, resultText, exitCode); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record %s status: %v\n", id, status, err)
	}
}

func (r *Runner) register(id string, p handle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.processes[id] = p
}
func (r *Runner) unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.processes, id)
}
