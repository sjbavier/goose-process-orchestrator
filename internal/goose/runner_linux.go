//go:build linux

package goose

import (
	"context"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
)

// Runner combines only validated launch construction, private evidence capture,
// and the Linux owned-process-group boundary. It never invokes a shell.
type Runner struct {
	Config    Config
	Artifacts artifacts.Manager
	Grace     time.Duration
}
type RunResult struct {
	Run       artifacts.Run
	Summary   string
	ExitCode  int
	Cancelled bool
	Err       error
}

func (r Runner) Start(request Request, taskID string) (*supervisor.Process, artifacts.Run, error) {
	run, err := r.Artifacts.Create(taskID)
	if err != nil {
		return nil, artifacts.Run{}, err
	}
	request.RunDir = run.Dir
	launch, err := r.Config.Build(request)
	if err != nil {
		return nil, run, err
	}
	out, err := r.Artifacts.OpenCapture(run.StdoutPath)
	if err != nil {
		return nil, run, err
	}
	defer func() {
		if err != nil {
			out.Close()
		}
	}()
	errout, err := r.Artifacts.OpenCapture(run.StderrPath)
	if err != nil {
		out.Close()
		return nil, run, err
	}
	p, err := supervisor.Start(launch.Binary, launch.Args, launch.Dir, launch.Env, out, errout)
	if err != nil {
		out.Close()
		errout.Close()
		return nil, run, err
	}
	return p, run, nil
}
func (r Runner) Cancel(ctx context.Context, p *supervisor.Process) supervisor.Result {
	grace := r.Grace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	return p.Cancel(ctx, grace)
}
