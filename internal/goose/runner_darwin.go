//go:build darwin

package goose

import (
	"context"
	"io"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/supervisor"
)

// Runner combines only validated launch construction, private evidence capture,
// and the Darwin owned-process-group boundary. It never invokes a shell.
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

func (r Runner) Start(request Request, taskID string) (*supervisor.Process, artifacts.Run, *Live, error) {
	// Validate uploaded contents and declarations before creating any run artifact.
	if len(request.FileParameters) > 0 {
		if err := r.Config.ValidateRequestWithFiles(request.Recipe, request.Parameters, request.FileParameters); err != nil {
			return nil, artifacts.Run{}, nil, err
		}
	}
	run, err := r.Artifacts.Create(taskID)
	if err != nil {
		return nil, artifacts.Run{}, nil, err
	}
	request.RunDir = run.Dir
	request.Parameters, err = materializeFileParameters(run.Dir, request.Parameters, request.FileParameters)
	if err != nil {
		return nil, run, nil, err
	}
	request.FileParameters = nil
	launch, err := r.Config.Build(request)
	if err != nil {
		return nil, run, nil, err
	}
	out, err := r.Artifacts.OpenCapture(run.StdoutPath)
	if err != nil {
		return nil, run, nil, err
	}
	defer func() {
		if err != nil {
			out.Close()
		}
	}()
	errout, err := r.Artifacts.OpenCapture(run.StderrPath)
	if err != nil {
		out.Close()
		return nil, run, nil, err
	}
	parser := &logs.Incremental{}
	live := &Live{parser: parser, stdout: out}
	p, err := supervisor.Start(launch.Binary, launch.Args, launch.Dir, launch.Env, io.MultiWriter(out, parser), errout)
	if err != nil {
		out.Close()
		errout.Close()
		return nil, run, nil, err
	}
	return p, run, live, nil
}

// Live exposes a live view of one started process's stdout: a point-in-time
// parsed snapshot and the disk capture's own truncation accounting. Both
// accessors are safe to call at any time, including concurrently with the
// process still writing, because the io.MultiWriter in Start feeds both the
// disk capture and the parser every byte regardless of the capture's Limit —
// this is what lets Snapshot/Stats survive truncation.
type Live struct {
	parser *logs.Incremental
	stdout *artifacts.Capture
}

// Snapshot returns the live-parsed stdout state as of now.
func (l *Live) Snapshot() logs.Snapshot { return l.parser.Snapshot() }

// Flush processes a trailing line that never received its closing newline.
// Call it once after the process has exited, before taking a final Snapshot.
func (l *Live) Flush() { l.parser.Flush() }

// Stats returns the disk capture's truncation accounting as of now.
func (l *Live) Stats() (written, dropped int64, truncated bool) { return l.stdout.Stats() }
func (r Runner) Cancel(ctx context.Context, p *supervisor.Process) supervisor.Result {
	grace := r.Grace
	if grace <= 0 {
		grace = 5 * time.Second
	}
	return p.Cancel(ctx, grace)
}
