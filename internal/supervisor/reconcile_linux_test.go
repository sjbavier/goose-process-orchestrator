//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"syscall"
	"testing"
	"time"
)

// TestVerifiedIdentityDeterministicRecycledPID is the deterministic
// recycled-PID test required by C4: it starts a real, short-lived fixture
// process, reads its real pid/pgid/startTicks, and asserts VerifiedIdentity
// is true for the exact recorded identity and false for a deliberately wrong
// start-time value (simulating a recycled pid whose new occupant happens to
// share the same pid and even the same pgid, but not the same kernel start
// time). It is deterministic because it compares the fixture's own real,
// already-observed startTicks value against a wrong value, rather than
// racing on any timing/sleep window.
//
// Critically, this test also asserts VerifiedIdentity never sends a signal:
// after calling VerifiedIdentity with the wrong startTicks (the mismatch
// path), the fixture process must still be alive, proving no Kill/Term was
// reachable from that call. The only signal sent anywhere in this test is
// the explicit cleanup Cancel at the end, after all assertions complete.
func TestVerifiedIdentityDeterministicRecycledPID(t *testing.T) {
	var out, errBuf bytes.Buffer
	p, err := Start("sleep", []string{"30"}, t.TempDir(), nil, &out, &errBuf)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		// Cleanup only: terminate the fixture after every assertion below has
		// run, so no signal is sent during the assertions themselves.
		p.Cancel(context.Background(), 100*time.Millisecond)
	}()

	pid := p.cmd.Process.Pid

	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid(%d): %v", pid, err)
	}
	if pgid != pid {
		t.Fatalf("fixture process is not its own group leader: pid=%d pgid=%d", pid, pgid)
	}

	ticks, err := startTicks(pid)
	if err != nil {
		t.Fatalf("startTicks(%d): %v", pid, err)
	}

	if !VerifiedIdentity(pid, pgid, ticks) {
		t.Fatalf("VerifiedIdentity(%d, %d, %d) = false, want true for the fixture's real identity", pid, pgid, ticks)
	}

	// Simulate a recycled pid: same pid, same pgid, but a start time that
	// does not match the live process's real kernel-reported start time.
	if VerifiedIdentity(pid, pgid, ticks+1) {
		t.Fatalf("VerifiedIdentity(%d, %d, %d) = true, want false for a mismatched start time", pid, pgid, ticks+1)
	}

	// No signal must have been sent by either VerifiedIdentity call above:
	// the fixture process must still be a live, verified group leader.
	if !verifiedGroupLeader(pid) {
		t.Fatal("fixture process is no longer a verified, live group leader after VerifiedIdentity calls; a signal may have been sent unexpectedly")
	}
}

// TestVerifiedIdentityNonexistentPidIsFalse asserts the fail-closed default
// for a pid that does not exist at all: VerifiedIdentity must return false,
// and must never attempt to signal it.
func TestVerifiedIdentityNonexistentPidIsFalse(t *testing.T) {
	const nonexistentPid = 1 << 30
	if VerifiedIdentity(nonexistentPid, nonexistentPid, 1) {
		t.Fatalf("VerifiedIdentity(%d, ...) = true, want false for a nonexistent pid", nonexistentPid)
	}
}

// fixtureIdentity starts a real process via the existing Start helper (which
// sets Setpgid: true, so the fixture is its own group leader by
// construction) and returns its real pid/pgid/startTicks, for use by Adopt
// in the tests below.
func fixtureIdentity(t *testing.T, binary string, args []string) (*Process, int, int, int64) {
	t.Helper()
	var out, errBuf bytes.Buffer
	p, err := Start(binary, args, t.TempDir(), nil, &out, &errBuf)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := p.cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid(%d): %v", pid, err)
	}
	ticks, err := startTicks(pid)
	if err != nil {
		t.Fatalf("startTicks(%d): %v", pid, err)
	}
	return p, pid, pgid, ticks
}

// TestAdoptShortLivedProcessWaitReturnsOnExit covers C5's acceptance
// criterion 1 ("re-adopt + natural exit"): Adopt a real short-lived fixture
// process, and assert Wait() returns (i.e. the polling loop observes the
// process is gone) once it exits on its own, without this test process ever
// having forked/exec'd it as a direct child relationship relied upon by
// Wait (Wait only polls, it never calls wait(2)).
func TestAdoptShortLivedProcessWaitReturnsOnExit(t *testing.T) {
	_, pid, pgid, ticks := fixtureIdentity(t, "sh", []string{"-c", "exit 0"})

	rp, err := Adopt(pid, pgid, ticks)
	if err != nil {
		t.Fatalf("Adopt(%d, %d, %d): %v", pid, pgid, ticks, err)
	}

	done := make(chan Result, 1)
	go func() { done <- rp.Wait() }()

	select {
	case <-done:
		// Wait returned, as expected once the short-lived fixture exits.
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() did not return within 5s of adopting a short-lived fixture process")
	}
}

// TestAdoptLongLivedProcessCancelTerminatesIt covers C5's acceptance
// criterion 1 ("re-adopt + cancel"): Adopt a real long-lived fixture process
// and assert Cancel() actually terminates it (re-verifying identity before
// every signal, per the plan) and returns a Result with Cancelled set.
func TestAdoptLongLivedProcessCancelTerminatesIt(t *testing.T) {
	_, pid, pgid, ticks := fixtureIdentity(t, "sleep", []string{"30"})

	rp, err := Adopt(pid, pgid, ticks)
	if err != nil {
		t.Fatalf("Adopt(%d, %d, %d): %v", pid, pgid, ticks, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := rp.Cancel(ctx, 200*time.Millisecond)
	if !result.Cancelled {
		t.Fatalf("Cancel() result = %+v, want Cancelled = true", result)
	}

	if VerifiedIdentity(pid, pgid, ticks) {
		t.Fatalf("pid %d is still a verified live identity after Cancel(); expected it to be terminated", pid)
	}
}

// TestAdoptWrongStartTicksFailsBeforeAnyLiveHandleExists covers C5's third
// required case: Adopt with a deliberately wrong startTicks value must
// return an error, and must never construct a live ReconciledProcess handle
// at all — proven here by asserting the fixture process is still alive
// after the failed Adopt call, with no handle ever obtained to even attempt
// calling Cancel on.
func TestAdoptWrongStartTicksFailsBeforeAnyLiveHandleExists(t *testing.T) {
	p, pid, pgid, ticks := fixtureIdentity(t, "sleep", []string{"30"})
	defer p.Cancel(context.Background(), 100*time.Millisecond)

	wrongTicks := ticks + 1
	rp, err := Adopt(pid, pgid, wrongTicks)
	if err == nil {
		t.Fatalf("Adopt(%d, %d, %d) succeeded, want error for mismatched startTicks", pid, pgid, wrongTicks)
	}
	if rp != nil {
		t.Fatalf("Adopt(%d, %d, %d) returned a non-nil handle alongside an error; want nil so no live handle ever exists to call Cancel on", pid, pgid, wrongTicks)
	}

	// No handle exists to call Cancel on (as asserted above), so no signal
	// could possibly have been sent by Adopt's failed verification. Confirm
	// the fixture is still alive and verified, proving nothing was signaled.
	if !verifiedGroupLeader(pid) {
		t.Fatalf("fixture process pid %d is no longer a verified, live group leader after a failed Adopt call; a signal may have been sent unexpectedly", pid)
	}
	if !VerifiedIdentity(pid, pgid, ticks) {
		t.Fatalf("fixture process pid %d no longer verifies against its real original startTicks %d after a failed Adopt call", pid, ticks)
	}
}
