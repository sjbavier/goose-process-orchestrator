//go:build darwin

package supervisor

import (
	"context"
	"fmt"
	"time"
)

// VerifiedIdentity always returns false on Darwin. startTicks (see
// procstat_darwin.go) always errors on this platform because there is no
// /proc and no evidence-checked equivalent (sysctl KERN_PROC_PID /
// kinfo_proc.p_starttime) has been implemented or verified on a real Darwin
// host yet, so VerifiedIdentity could never positively confirm a start-time
// match here even if pid/pgid happened to line up. Rather than let that fall
// out only implicitly through startTicks's error, this is made explicit: per
// the plan's Darwin non-goal, any restart-time reconciliation on Darwin must
// always fail closed to mark-only (never re-adopt, never signal) until real
// Darwin-host evidence exists for a start-time-equivalent primitive. This
// function never sends a signal.
func VerifiedIdentity(pid, pgid int, startTicks int64) bool {
	return false
}

// ReconciledProcess mirrors the Linux type's exported surface so cross-
// platform callers (e.g. a future orchestrator reconciler) can share a
// single signature, but on Darwin it can never be constructed: Adopt always
// errors below, since VerifiedIdentity can never positively confirm an
// identity on this platform yet.
type ReconciledProcess struct{}

// Wait is unreachable in practice: no ReconciledProcess can ever be
// constructed on Darwin (Adopt always errors), so there is no live handle
// to call this on. Defined only so the type's exported surface matches
// Linux's.
func (rp *ReconciledProcess) Wait() Result { return Result{} }

// Cancel is unreachable in practice for the same reason as Wait, and never
// sends a signal.
func (rp *ReconciledProcess) Cancel(ctx context.Context, grace time.Duration) Result {
	return Result{}
}

// Adopt always returns an error on Darwin: VerifiedIdentity (above) can
// never positively confirm an identity on this platform yet, so Adopt must
// never construct a live handle here. This keeps the "never signal an
// unverified pid" rule intact without guessing at an unverified Darwin
// start-time primitive.
func Adopt(pid, pgid int, startTicks int64) (*ReconciledProcess, error) {
	return nil, fmt.Errorf("adopt pid %d: identity verification is not supported on darwin yet", pid)
}
