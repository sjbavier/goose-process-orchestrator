// Package orchestrator adapts the local Goose launcher to the scheduler's
// Runner seam and keeps an in-memory registry so explicit cancellation
// intent recorded in the store can reach the owned OS process group.
package orchestrator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// CancelPoll bounds how often a running task's cancellation intent is
// rechecked. No notification mechanism exists; this stays a short, bounded
// poll rather than a schema or interface change.
const CancelPoll = 200 * time.Millisecond

// flushEveryNTicks throttles the periodic outcome-summary flush inside
// await's existing select loop to no tighter than CancelPoll itself — a
// flush every tick would add SQLite write pressure to a loop that already
// shares its cadence with cancellation polling.
const flushEveryNTicks = 5

// Store is the subset of tasks.Store this adapter needs.
type Store interface {
	Get(ctx context.Context, id string) (tasks.Task, error)
	Transition(ctx context.Context, id string, next tasks.Status, resultText string, exitCode *int) (tasks.Task, error)
	RecordOutcomeSummary(ctx context.Context, id string, summary tasks.OutcomeSummary) (tasks.Task, error)
	RecordFailure(ctx context.Context, id, failure string) (tasks.Task, error)
}

// effectiveRecorder is optionally implemented by the Store (tasks.Store does)
// to persist the launched recipe hash and the observed provider/model.
type effectiveRecorder interface {
	RecordRecipeSHA256(ctx context.Context, id, sha string) (tasks.Task, error)
	RecordEffectiveProviderModel(ctx context.Context, id, provider, model string) (tasks.Task, error)
}

// leftoverRecorder is optionally implemented by the Store to persist the
// goose version and the post-exit leftover scan.
type leftoverRecorder interface {
	RecordGooseVersion(ctx context.Context, id, version string) (tasks.Task, error)
	RecordLeftovers(ctx context.Context, id string, pids []int, detection string) (tasks.Task, error)
}

// leftoverReporter is implemented by handles that can scan their process
// group after exit (*supervisor.Process). Re-adopted processes cannot, and
// their leftover columns stay NULL (not scanned).
type leftoverReporter interface {
	Leftovers() ([]int, bool)
}

// expectation carries a task's optional provider/model expectations and its
// optional wall-clock deadline (zero means none).
type expectation struct {
	provider, model string
	deadline        time.Time
}

// timedOutFailure prefixes the failure recorded when a task exceeds its
// per-task wall-clock timeout; awaitAndReport maps it to status timed_out.
const timedOutFailure = "timed_out"

func (e expectation) set() bool { return e.provider != "" || e.model != "" }

// mismatch returns a failure description when the observed provider/model
// contradict an expectation, or "" otherwise. Empty observed values are not
// a mismatch here (nothing observed yet).
func (e expectation) mismatch(provider, model string) string {
	if e.provider != "" && provider != "" && provider != e.provider {
		return fmt.Sprintf("provider_mismatch: expected %q, observed %q", e.provider, provider)
	}
	if e.model != "" && model != "" && model != e.model {
		return fmt.Sprintf("model_mismatch: expected %q, observed %q", e.model, model)
	}
	return ""
}

// Launcher starts and cancels one owned OS process for a validated request.
// internal/goose.Runner satisfies this directly. The *goose.Live return
// value exposes a live-parsed stdout snapshot and the disk capture's
// truncation accounting, both independent of the process's own exit; it may
// be nil only if a test double chooses not to provide one.
type Launcher interface {
	Start(request goose.Request, taskID string) (*supervisor.Process, artifacts.Run, *goose.Live, error)
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
	request := goose.Request{Recipe: task.Recipe, MaxTurns: task.MaxTurns, Parameters: task.Parameters, FileParameters: task.FileParameters, ExpectRecipeSHA256: task.ExpectRecipeSHA256}
	if r.Tokens != nil {
		token, err := r.Tokens.Mint(task.ID)
		if err != nil {
			r.reportLaunchFailure(ctx, task.ID, err)
			return
		}
		request.MailboxToken = token
		defer r.Tokens.Revoke(task.ID)
	}
	p, run, live, err := r.Launcher.Start(request, task.ID)
	if err != nil {
		r.reportLaunchFailure(ctx, task.ID, err)
		return
	}

	// A store-level race on this transition should not abandon an
	// already-started process; await its real exit regardless. Store.Transition
	// already retries SQLite busy/locked outcomes; a remaining error here is
	// unexpected and worth surfacing, not silently losing.
	if _, err := r.Store.Transition(ctx, task.ID, tasks.Running, "", nil); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record running status: %v\n", task.ID, err)
	}

	if rec, ok := r.Store.(effectiveRecorder); ok && live != nil && live.RecipeSHA256 != "" {
		if _, err := rec.RecordRecipeSHA256(ctx, task.ID, live.RecipeSHA256); err != nil {
			fmt.Fprintf(os.Stderr, "orchestrator: task %s: record recipe sha256: %v\n", task.ID, err)
		}
	}

	if rec, ok := r.Store.(leftoverRecorder); ok && live != nil && live.GooseVersion != "" {
		if _, err := rec.RecordGooseVersion(ctx, task.ID, live.GooseVersion); err != nil {
			fmt.Fprintf(os.Stderr, "orchestrator: task %s: record goose version: %v\n", task.ID, err)
		}
	}

	r.resume(ctx, task.ID, p, run.Dir, live, expectation{provider: task.ExpectProvider, model: task.ExpectModel, deadline: deadlineFor(time.Now(), task.TimeoutSeconds)}, func(cancelCtx context.Context) supervisor.Result {
		return r.Launcher.Cancel(cancelCtx, p)
	})
}

// deadlineFor returns start plus the per-task timeout, or the zero time when
// no timeout is set.
func deadlineFor(start time.Time, seconds int) time.Time {
	if seconds <= 0 {
		return time.Time{}
	}
	return start.Add(time.Duration(seconds) * time.Second)
}

// reportLaunchFailure records an orchestrator-level failure that occurs
// before any process exists and so has no real exit code: Tokens.Mint
// erroring, or Launcher.Start itself erroring. It keeps the existing
// Result/Transition behavior byte-identical (resultText == err.Error(), same
// as before this feature) and additionally records the same text in the new
// Failure column — the first thing that ever populates it, since a process
// that starts and later exits non-zero reports via ExitCode/outcome instead.
func (r *Runner) reportLaunchFailure(ctx context.Context, id string, err error) {
	r.report(ctx, id, tasks.Failed, err.Error(), nil)
	if _, ferr := r.Store.RecordFailure(ctx, id, err.Error()); ferr != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record failure: %v\n", id, ferr)
	}
}

// resume registers p as the process backing task id, awaits its real exit
// (cancelling via the supplied cancel callback the first time explicit
// cancellation intent appears in the store), and reports the resulting
// terminal status to the store using resultText as the stored result. It is
// the shared register->await->report tail used by Run for a freshly started
// process, and by Reconcile (via awaitAndReport, below) for a re-adopted one.
// live is nil for a re-adopted process (Reconcile never calls
// goose.Runner.Start again), in which case no outcome summary is ever
// flushed for it — the new columns stay NULL, exactly as decision D requires.
func (r *Runner) resume(ctx context.Context, id string, p handle, resultText string, live *goose.Live, expect expectation, cancel func(context.Context) supervisor.Result) {
	r.register(id, p)
	defer r.unregister(id)
	r.awaitAndReport(ctx, id, p, resultText, live, expect, cancel)
}

// awaitAndReport is resume's await+report half, split out so a caller that
// must guarantee synchronous registration (Reconcile, so a process is
// immediately cancellable once it returns) can call register itself and run
// just this half in its own goroutine.
func (r *Runner) awaitAndReport(ctx context.Context, id string, p handle, resultText string, live *goose.Live, expect expectation, cancel func(context.Context) supervisor.Result) {
	result, failure := r.await(ctx, id, p, live, expect, cancel)

	// The final summary flush is sequenced strictly before the terminal
	// report/Transition below, so a caller observing the store never sees a
	// terminal status without whatever live summary this process produced.
	if live != nil {
		live.Flush()
		failure = r.recordEffective(ctx, id, live, expect, failure, result)
		r.flushLiveSummary(ctx, id, live, failure == "" && !result.Cancelled && result.ExitCode > 0)
	}

	r.recordLeftovers(ctx, id, p)

	status := tasks.Succeeded
	switch {
	case failure != "":
		status = tasks.Failed
		if strings.HasPrefix(failure, timedOutFailure) {
			status = tasks.TimedOut
		}
		if _, ferr := r.Store.RecordFailure(ctx, id, failure); ferr != nil {
			fmt.Fprintf(os.Stderr, "orchestrator: task %s: record failure: %v\n", id, ferr)
		}
	case result.Cancelled:
		status = tasks.Cancelled
	case result.ExitCode != 0:
		status = tasks.Failed
	}
	exit := result.ExitCode
	r.report(ctx, id, status, resultText, &exit)
}

// recordLeftovers persists the post-exit process-group scan, strictly before
// the terminal report so an observer of a terminal status sees it. Leftovers
// are reported, never silently ignored; detection failures are recorded as
// unavailable rather than as an empty list.
func (r *Runner) recordLeftovers(ctx context.Context, id string, p handle) {
	lr, ok := p.(leftoverReporter)
	rec, ok2 := r.Store.(leftoverRecorder)
	if !ok || !ok2 {
		return
	}
	pids, scanned := lr.Leftovers()
	var err error
	if scanned {
		_, err = rec.RecordLeftovers(ctx, id, pids, tasks.LeftoverDetectionOK)
	} else {
		_, err = rec.RecordLeftovers(ctx, id, nil, tasks.LeftoverDetectionUnavailable)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record leftovers: %v\n", id, err)
	}
}

// await blocks for the process's real exit, cancelling it the first time
// explicit cancellation intent appears in the store while it is still
// running, and (when live is non-nil) flushing a throttled periodic outcome
// summary so progress/result can observe a long-running task's live state,
// not only its final one.
func (r *Runner) await(ctx context.Context, id string, p handle, live *goose.Live, expect expectation, cancel func(context.Context) supervisor.Result) (supervisor.Result, string) {
	done := make(chan supervisor.Result, 1)
	go func() { done <- p.Wait() }()

	ticker := time.NewTicker(CancelPoll)
	defer ticker.Stop()
	ticks := 0
	for {
		select {
		case result := <-done:
			return result, ""
		case <-ticker.C:
			ticks++
			if live != nil && ticks%flushEveryNTicks == 0 {
				r.flushLiveSummary(ctx, id, live, false)
			}
			if live != nil && expect.set() {
				snap := live.Snapshot()
				if failure := expect.mismatch(snap.Provider, snap.Model); failure != "" {
					return cancel(context.Background()), failure
				}
			}
			current, err := r.Store.Get(ctx, id)
			if err == nil && current.CancelRequestedAt != nil {
				return cancel(context.Background()), ""
			}
			if !expect.deadline.IsZero() && !time.Now().Before(expect.deadline) {
				return cancel(context.Background()), timedOutFailure + ": exceeded per-task timeout_seconds"
			}
		}
	}
}

// recordEffective persists the observed provider/model and returns the
// failure to report: an earlier mid-run mismatch, a final-snapshot mismatch,
// or expectation_unverifiable when a clean exit never exposed what was
// expected (an expectation is never silently passed).
func (r *Runner) recordEffective(ctx context.Context, id string, live *goose.Live, expect expectation, failure string, result supervisor.Result) string {
	snap := live.Snapshot()
	if rec, ok := r.Store.(effectiveRecorder); ok && (snap.Provider != "" || snap.Model != "") {
		if _, err := rec.RecordEffectiveProviderModel(ctx, id, snap.Provider, snap.Model); err != nil {
			fmt.Fprintf(os.Stderr, "orchestrator: task %s: record provider/model: %v\n", id, err)
		}
	}
	if failure != "" || !expect.set() {
		return failure
	}
	if m := expect.mismatch(snap.Provider, snap.Model); m != "" {
		return m
	}
	if !result.Cancelled && result.ExitCode == 0 &&
		((expect.provider != "" && snap.Provider == "") || (expect.model != "" && snap.Model == "")) {
		return "expectation_unverifiable: stream exposed no provider/model to check"
	}
	return ""
}

// flushLiveSummary persists live's current snapshot and capture stats,
// logging rather than failing the caller if the write itself errors — the
// same tolerance report already has for its own Transition call.
func (r *Runner) flushLiveSummary(ctx context.Context, id string, live *goose.Live, failedExit bool) {
	if _, err := r.Store.RecordOutcomeSummary(ctx, id, outcomeSummaryFromLive(live, failedExit)); err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator: task %s: record outcome summary: %v\n", id, err)
	}
}

// outcomeSummaryFromLive translates a goose.Live snapshot (logs.Snapshot)
// plus its capture stats into the plain-primitive tasks.OutcomeSummary that
// Store.RecordOutcomeSummary persists. Persist a failed stall only after the
// process exits nonzero on its own and the stream confirms a max-turns error.
//
// FinalTextTruncated is a best-effort inference, not a field the parser
// reports directly: Incremental caps assistant text at exactly
// logs.MaxAssistantTextBytes while accumulating, so text landing exactly on
// that boundary is treated as truncated. This can at most false-positive on
// a real message whose length happens to match the cap exactly, never
// false-negative, matching this feature's documented "under-detect, never
// over-claim" posture for inferred signals.
func outcomeSummaryFromLive(live *goose.Live, failedExit bool) tasks.OutcomeSummary {
	snap := live.Snapshot()
	_, dropped, truncated := live.Stats()
	var outcome string
	if failedExit && snap.StallErrorSeen {
		outcome = tasks.OutcomeStalledMaxTurns
	}
	return tasks.OutcomeSummary{
		FinalText:          snap.LastAssistantText,
		FinalTextTruncated: len(snap.LastAssistantText) >= logs.MaxAssistantTextBytes,
		Outcome:            outcome,
		CompleteSeen:       snap.Complete,
		TotalTokens:        snap.TotalTokens,
		InputTokens:        snap.InputTokens,
		OutputTokens:       snap.OutputTokens,
		CacheReadTokens:    snap.CacheReadTokens,
		CacheWriteTokens:   snap.CacheWriteTokens,
		CostUSD:            snap.CostUSD,
		ToolCallCount:      int64(snap.ToolCallCount),
		LastEventAt:        snap.LastEventAt,
		StalledMaxTurns:    snap.StalledMaxTurns || outcome == tasks.OutcomeStalledMaxTurns,
		StdoutTruncated:    truncated,
		BytesDropped:       dropped,
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
