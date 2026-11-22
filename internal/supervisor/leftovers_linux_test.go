//go:build linux

package supervisor

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func startLeaver(t *testing.T) (*Process, int) {
	t.Helper()
	// Background sleep survives the shell leader; it prints its pid.
	var out strings.Builder
	p, err := Start("sh", []string{"-c", "sleep 60 >/dev/null 2>&1 & echo $!"}, t.TempDir(), nil, &out, &out)
	if err != nil {
		t.Fatal(err)
	}
	p.Wait()
	pid, err := strconv.Atoi(strings.TrimSpace(out.String()))
	if err != nil {
		t.Fatalf("pid parse %q: %v", out.String(), err)
	}
	return p, pid
}

func alive(pid int) bool { _, ok := readProcEntry(pid); return ok }

func TestLeftoversReportedAndCancelSweeps(t *testing.T) {
	p, pid := startLeaver(t)
	t.Cleanup(func() { _ = exec.Command("kill", "-9", strconv.Itoa(pid)).Run() })
	pids, ok := p.Leftovers()
	if !ok || len(pids) != 1 || pids[0] != pid {
		t.Fatalf("Leftovers = %v ok=%v, want [%d]", pids, ok, pid)
	}
	bystander := exec.Command("sleep", "60")
	if err := bystander.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bystander.Process.Kill(); _ = bystander.Wait() }()

	p.Cancel(context.Background(), 200*time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for alive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive(pid) {
		t.Fatal("leftover survived Cancel")
	}
	if !alive(bystander.Process.Pid) {
		t.Fatal("unrelated process was signalled")
	}
}

func TestNoLeftoversReportsEmptyOK(t *testing.T) {
	p, err := Start("sh", []string{"-c", "true"}, t.TempDir(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Wait()
	if pids, ok := p.Leftovers(); !ok || len(pids) != 0 {
		t.Fatalf("Leftovers = %v ok=%v, want empty ok", pids, ok)
	}
}
