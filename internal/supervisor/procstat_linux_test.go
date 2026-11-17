//go:build linux

package supervisor

import (
	"os"
	"testing"
)

func TestStartTicksSelfIsPositiveAndStable(t *testing.T) {
	pid := os.Getpid()

	first, err := startTicks(pid)
	if err != nil {
		t.Fatalf("startTicks(%d) returned error: %v", pid, err)
	}
	if first <= 0 {
		t.Fatalf("startTicks(%d) = %d, want positive value", pid, first)
	}

	second, err := startTicks(pid)
	if err != nil {
		t.Fatalf("startTicks(%d) second call returned error: %v", pid, err)
	}
	if second != first {
		t.Fatalf("startTicks(%d) not stable across calls: first=%d second=%d", pid, first, second)
	}
}

func TestStartTicksNonexistentPidErrors(t *testing.T) {
	// A pid that cannot exist: os.Getpid() is a real pid, adding a large
	// offset well beyond /proc/sys/kernel/pid_max on any normal host
	// while staying a valid positive int.
	const nonexistentPid = 1 << 30

	if _, err := startTicks(nonexistentPid); err == nil {
		t.Fatalf("startTicks(%d) expected non-nil error for nonexistent pid", nonexistentPid)
	}
}
