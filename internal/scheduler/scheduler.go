// Package scheduler provides bounded, in-process admission and worker dispatch.
package scheduler

import (
	"context"
	"errors"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

var ErrQueueFull = errors.New("task queue is at capacity")

type Claimer interface {
	Claim(context.Context) (*tasks.Task, error)
}

// Runner is the runner-facing seam. It receives an atomically claimed task;
// process launch and lifecycle completion remain outside this package.
type Runner interface {
	Run(context.Context, tasks.Task)
}

type Scheduler struct {
	store  Claimer
	runner Runner
	jobs   chan struct{}
	slots  chan struct{}
}

// New starts workers immediately. Queue capacity bounds accepted-but-unclaimed
// work; worker execution is independently bounded by workers.
func New(store Claimer, runner Runner, workers, queueCapacity int) (*Scheduler, error) {
	if store == nil || workers < 1 || queueCapacity < 1 {
		return nil, errors.New("store, at least one worker, and positive queue capacity are required")
	}
	s := &Scheduler{store: store, runner: runner, jobs: make(chan struct{}, queueCapacity), slots: make(chan struct{}, queueCapacity)}
	for range workers {
		go s.worker()
	}
	return s, nil
}

// Reserve admits one new request without blocking. Release must be called if
// persistence fails or reveals an existing idempotent request.
func (s *Scheduler) Reserve() error {
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *Scheduler) Release() { <-s.slots }

// Enqueue schedules an already-persisted task. It cannot block after Reserve.
func (s *Scheduler) Enqueue() { s.jobs <- struct{}{} }

func (s *Scheduler) worker() {
	for range s.jobs {
		task, err := s.store.Claim(context.Background())
		s.Release()
		if err != nil || task == nil || s.runner == nil {
			continue
		}
		s.runner.Run(context.Background(), *task)
	}
}
