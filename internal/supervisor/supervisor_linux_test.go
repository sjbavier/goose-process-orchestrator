//go:build linux

package supervisor

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestCancelTerminatesOwnedProcessGroup(t *testing.T) {
	var out, err bytes.Buffer
	p, e := Start("sh", []string{"-c", "sleep 30 & wait"}, t.TempDir(), nil, &out, &err)
	if e != nil {
		t.Fatal(e)
	}
	r := p.Cancel(context.Background(), 100*time.Millisecond)
	if !r.Cancelled && r.Err == nil {
		t.Fatal("cancel did not produce exit")
	}
}

// TestCancelSkipsSignalForExitedPID guards against the finding #2 regression:
// cancel() must verify the group leader is still alive before signaling, so an
// already-exited (and potentially PID-recycled) process is never signaled.
func TestCancelSkipsSignalForExitedPID(t *testing.T) {
	var out, err bytes.Buffer
	p, e := Start("sh", []string{"-c", "exit 0"}, t.TempDir(), nil, &out, &err)
	if e != nil {
		t.Fatal(e)
	}
	<-p.done // wait for the process to exit and be reaped by cmd.Wait

	if verifiedGroupLeader(p.cmd.Process.Pid) {
		t.Fatal("verifiedGroupLeader returned true for an exited, reaped PID")
	}

	// cancel() must be a no-op here: it must not attempt to signal a PID the
	// kernel may have already reassigned to an unrelated process.
	r := p.Cancel(context.Background(), 100*time.Millisecond)
	if !r.Cancelled {
		t.Fatal("Cancel did not record cancellation even though it skipped signaling")
	}
}
