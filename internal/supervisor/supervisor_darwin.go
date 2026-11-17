//go:build darwin

// Package supervisor starts and cancels only its verified Darwin process groups.
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

// Result is the terminal result for a supervised task-owned process group.
type Result struct {
	ExitCode  int
	Cancelled bool
	Err       error
}

// Process is a task-owned child whose process group is created at start.
type Process struct {
	cmd    *exec.Cmd
	done   chan struct{}
	once   sync.Once
	mu     sync.Mutex
	result Result
}

// Start launches binary without a shell in a distinct process group owned by
// the task. env holds extra "KEY=VALUE" entries added on top of this
// process's own inherited environment; nil/empty leaves the child's
// environment exactly as before this parameter existed.
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
	go func() { p.mu.Lock(); p.result = exitResult(cmd.Wait()); p.mu.Unlock(); close(p.done) }()
	return p, nil
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

// Cancel sends TERM only after verifying that the child still leads its owned group,
// then waits grace before escalating that same verified group to KILL.
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
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
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
