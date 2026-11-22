//go:build linux

// Package supervisor starts and cancels only verified Linux process groups.
package supervisor

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	ExitCode  int
	Cancelled bool
	Err       error
}
type Process struct {
	cmd    *exec.Cmd
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	result Result

	// leftovers is the post-exit scan of the leader's process group, taken
	// by the waiter goroutine right after the leader is reaped and before
	// done closes. scanOK is false when /proc could not be scanned.
	leftovers []procEntry
	scanOK    bool
}

// Start launches binary with args in dir, capturing stdout/stderr. env holds
// extra "KEY=VALUE" entries added on top of this process's own inherited
// environment; nil/empty leaves the child's environment exactly as before
// this parameter existed.
func Start(binary string, args []string, dir string, env []string, stdout, stderr io.Writer) (*Process, error) {
	cmd := exec.Command(binary, args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = dir, stdout, stderr
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &Process{cmd: cmd, done: make(chan struct{})}
	go func() {
		res := exitResult(cmd.Wait())
		// The group id stays reserved by the kernel while any member lives,
		// so a non-empty scan right after reaping the leader can only be
		// this task's own descendants.
		members, err := scanGroup(cmd.Process.Pid)
		p.mu.Lock()
		p.result = res
		p.leftovers, p.scanOK = members, err == nil
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}

// Leftovers reports the pids still alive in the task's process group when
// its leader exited. ok is false when detection was unavailable. It blocks
// until the process has exited.
func (p *Process) Leftovers() (pids []int, ok bool) {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.scanOK {
		return nil, false
	}
	pids = make([]int, 0, len(p.leftovers))
	for _, e := range p.leftovers {
		pids = append(pids, e.pid)
	}
	return pids, true
}

// sweepLeftovers TERMs then, after grace, KILLs each recorded leftover that
// is still the same process (same pid, group and start time). It never
// signals a pid whose identity no longer matches the recorded scan.
func (p *Process) sweepLeftovers(ctx context.Context, grace time.Duration) {
	p.mu.Lock()
	targets := append([]procEntry(nil), p.leftovers...)
	p.mu.Unlock()
	same := func(t procEntry) bool {
		e, ok := readProcEntry(t.pid)
		return ok && e.pgrp == t.pgrp && e.ticks == t.ticks
	}
	alive := func() []procEntry {
		var out []procEntry
		for _, t := range targets {
			if same(t) {
				out = append(out, t)
			}
		}
		return out
	}
	live := alive()
	if len(live) == 0 {
		return
	}
	for _, t := range live {
		if same(t) {
			_ = syscall.Kill(t.pid, syscall.SIGTERM)
		}
	}
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()
wait:
	for {
		select {
		case <-poll.C:
			if len(alive()) == 0 {
				return
			}
		case <-ctx.Done():
			break wait
		case <-deadline.C:
			break wait
		}
	}
	for _, t := range alive() {
		if same(t) {
			_ = syscall.Kill(t.pid, syscall.SIGKILL)
		}
	}
}
func exitResult(err error) Result {
	r := Result{Err: err}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		r.ExitCode = exit.ExitCode()
	}
	return r
}
func (p *Process) Wait() Result       { <-p.done; return p.readResult() }
func (p *Process) readResult() Result { p.mu.Lock(); defer p.mu.Unlock(); return p.result }

// Cancel sends TERM to the verified child group, waits grace, then escalates to KILL.
func (p *Process) Cancel(ctx context.Context, grace time.Duration) Result {
	p.once.Do(func() { p.cancel(ctx, grace) })
	<-p.done
	p.mu.Lock()
	p.result.Cancelled = true
	r := p.result
	p.mu.Unlock()
	return r
}
func (p *Process) cancel(ctx context.Context, grace time.Duration) {
	pid := p.cmd.Process.Pid
	if !verifiedGroupLeader(pid) {
		// Leader already reaped: only recorded, identity-checked leftovers
		// may be signalled (the pid itself may have been recycled).
		<-p.done
		p.sweepLeftovers(ctx, grace)
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
		// Leader exited on TERM but descendants may have ignored it.
		p.sweepLeftovers(ctx, grace)
		return
	case <-ctx.Done():
		if verifiedGroupLeader(pid) {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	case <-t.C:
		if verifiedGroupLeader(pid) {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	}
}

// verifiedGroupLeader rejects exited, reused, and non-leader PIDs before a group signal.
func verifiedGroupLeader(pid int) bool {
	if pid <= 0 {
		return false
	}
	pgid, err := syscall.Getpgid(pid)
	return err == nil && pgid == pid
}
