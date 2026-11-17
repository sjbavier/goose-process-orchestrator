//go:build linux

package supervisor

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"time"
)

// VerifiedIdentity reports whether pid is still the exact same process that
// was previously observed to have process-group id pgid and kernel-reported
// start time startTicks. It is strictly additive to, and never a relaxation
// of, verifiedGroupLeader: in addition to pid being its own group leader, the
// live /proc/<pid>/stat start time (via startTicks, from procstat_linux.go)
// must match the persisted value exactly. This closes the pid-recycling gap
// that verifiedGroupLeader alone cannot close across an orchestrator
// restart: a recycled pid can trivially satisfy Getpgid(pid) == pid again
// (e.g. it is some other process's own group leader), but the kernel's
// per-boot start-time tick count cannot coincidentally match an unrelated
// process's start time to the same exact tick.
//
// VerifiedIdentity never sends a signal; it only reads process state. Any
// mismatch, any error from Getpgid or startTicks, or a non-positive pid is
// treated as unverifiable and returns false (fail closed).
func VerifiedIdentity(pid, pgid int, startTicks int64) bool {
	if pid <= 0 || pgid <= 0 {
		return false
	}
	livePgid, err := syscall.Getpgid(pid)
	if err != nil || livePgid != pid || livePgid != pgid {
		return false
	}
	liveTicks, err := startTicksFn(pid)
	if err != nil {
		return false
	}
	return liveTicks == startTicks
}

// startTicksFn is a package-level indirection to the platform startTicks
// helper (from procstat_linux.go), so VerifiedIdentity reads the live
// process's actual kernel-reported start time rather than any cached value.
var startTicksFn = startTicks

// reconciledPollInterval is the fixed interval ReconciledProcess.Wait uses to
// detect a non-child process's exit, since this process never fork+exec'd
// pid and so cannot use wait(2) to be notified. 500ms matches the order of
// magnitude of the existing store-side CancelPoll interval.
const reconciledPollInterval = 500 * time.Millisecond

// ReconciledProcess is a handle to a process this orchestrator instance did
// not start itself (e.g. one that survived a restart and was re-adopted by
// Adopt). Its Wait/Cancel contract mirrors Process's from the caller's point
// of view (same Result shape, same TERM-then-grace-then-KILL Cancel
// behavior), but since there is no parent/child relationship, exit is
// detected by polling rather than by wait(2).
type ReconciledProcess struct {
	pid, pgid  int
	startTicks int64

	done       chan struct{}
	cancelOnce sync.Once

	mu     sync.Mutex
	result Result
}

// Adopt verifies pid/pgid/startTicks via VerifiedIdentity immediately, and
// only constructs a live ReconciledProcess handle if that verification
// succeeds. If verification fails, Adopt returns an error and no handle is
// constructed at all — there is nothing a caller could ever call Cancel on
// for an unverified identity, by construction.
func Adopt(pid, pgid int, startTicks int64) (*ReconciledProcess, error) {
	if !VerifiedIdentity(pid, pgid, startTicks) {
		return nil, fmt.Errorf("adopt pid %d: identity not verified (pgid=%d startTicks=%d)", pid, pgid, startTicks)
	}
	rp := &ReconciledProcess{
		pid:        pid,
		pgid:       pgid,
		startTicks: startTicks,
		done:       make(chan struct{}),
	}
	go rp.pollUntilGone()
	return rp, nil
}

// pollUntilGone re-verifies identity on a fixed interval until the process
// is no longer verifiably the same one that was adopted (either it exited,
// or — treated identically and safely — its identity no longer matches),
// then records the result and closes done. This is the only place that
// closes done, so it is closed exactly once.
func (rp *ReconciledProcess) pollUntilGone() {
	ticker := time.NewTicker(reconciledPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		if VerifiedIdentity(rp.pid, rp.pgid, rp.startTicks) {
			continue
		}
		close(rp.done)
		return
	}
}

// Wait blocks until the adopted process is gone (per pollUntilGone) and
// returns the final Result. No exit code is observable for a non-child
// process, so ExitCode is always left at its zero value.
func (rp *ReconciledProcess) Wait() Result {
	<-rp.done
	return rp.readResult()
}

func (rp *ReconciledProcess) readResult() Result {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.result
}

// Cancel sends TERM to the adopted process group, waits grace, then
// escalates to KILL — mirroring Process.Cancel's shape exactly — but
// re-verifies identity via VerifiedIdentity immediately before every single
// signal it sends, since this process never forked/exec'd pid and so has no
// other guarantee that pid still refers to the same process.
func (rp *ReconciledProcess) Cancel(ctx context.Context, grace time.Duration) Result {
	rp.cancelOnce.Do(func() { rp.cancel(ctx, grace) })
	<-rp.done
	rp.mu.Lock()
	rp.result.Cancelled = true
	r := rp.result
	rp.mu.Unlock()
	return r
}

func (rp *ReconciledProcess) cancel(ctx context.Context, grace time.Duration) {
	if !VerifiedIdentity(rp.pid, rp.pgid, rp.startTicks) {
		return
	}
	_ = syscall.Kill(-rp.pid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-rp.done:
		return
	case <-ctx.Done():
		if VerifiedIdentity(rp.pid, rp.pgid, rp.startTicks) {
			_ = syscall.Kill(-rp.pid, syscall.SIGKILL)
		}
	case <-t.C:
		if VerifiedIdentity(rp.pid, rp.pgid, rp.startTicks) {
			_ = syscall.Kill(-rp.pid, syscall.SIGKILL)
		}
	}
}
