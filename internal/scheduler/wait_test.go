package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

func waitTestService(t *testing.T, count int) (*Service, *tasks.Store, []string) {
	t.Helper()
	store := testStore(t)
	dispatcher, err := New(store, nil, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(store, dispatcher, func() string { return "unused" }, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("task-%02d", i)
		if _, _, err := store.Create(context.Background(), tasks.Task{ID: ids[i], Recipe: "r", IdempotencyKey: ids[i]}); err != nil {
			t.Fatal(err)
		}
	}
	return service, store, ids
}

func transitionWaitTask(t *testing.T, store *tasks.Store, id string, status tasks.Status) {
	t.Helper()
	if status != tasks.Cancelled && status != tasks.Interrupted && status != tasks.Unknown {
		if _, err := store.Transition(context.Background(), id, tasks.Starting, "", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Transition(context.Background(), id, tasks.Running, "", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Transition(context.Background(), id, status, "done-"+id, nil); err != nil {
		t.Fatal(err)
	}
}

func TestWaitImmediateCallerOrderAndWakeStates(t *testing.T) {
	for _, status := range []tasks.Status{tasks.Succeeded, tasks.Failed, tasks.TimedOut, tasks.Cancelled, tasks.Interrupted, tasks.Unknown} {
		t.Run(string(status), func(t *testing.T) {
			s, store, ids := waitTestService(t, 2)
			transitionWaitTask(t, store, ids[1], status)
			got, err := s.Wait(context.Background(), []string{ids[1], ids[0]}, "any", time.Second)
			if err != nil || got.TimedOut {
				t.Fatalf("Wait = (%+v, %v)", got, err)
			}
			if got.Tasks[0].Task.ID != ids[1] || got.Tasks[1].Task.ID != ids[0] {
				t.Fatalf("order = %q, %q", got.Tasks[0].Task.ID, got.Tasks[1].Task.ID)
			}
		})
	}
}

func TestWaitAllAndAnyTransitions(t *testing.T) {
	for _, mode := range []string{"any", "all"} {
		t.Run(mode, func(t *testing.T) {
			s, store, ids := waitTestService(t, 2)
			go func() {
				time.Sleep(20 * time.Millisecond)
				transitionWaitTask(t, store, ids[0], tasks.Cancelled)
				if mode == "all" {
					time.Sleep(120 * time.Millisecond)
					transitionWaitTask(t, store, ids[1], tasks.Unknown)
				}
			}()
			start := time.Now()
			got, err := s.Wait(context.Background(), ids, mode, time.Second)
			if err != nil || got.TimedOut {
				t.Fatalf("Wait = (%+v, %v)", got, err)
			}
			if mode == "all" && time.Since(start) < 150*time.Millisecond {
				t.Fatalf("all returned before second transition: %s", time.Since(start))
			}
		})
	}
}

func TestWaitTimeoutAndContextCancellation(t *testing.T) {
	s, _, ids := waitTestService(t, 1)
	got, err := s.Wait(context.Background(), ids, "", 25*time.Millisecond)
	if err != nil || !got.TimedOut || got.Tasks[0].Task.Status != tasks.Queued {
		t.Fatalf("timeout Wait = (%+v, %v)", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Wait(ctx, ids, "all", time.Second); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Wait did not stop on context cancellation")
	}
}

func TestWaitRejectsInvalidAndMissingIDs(t *testing.T) {
	s, _, ids := waitTestService(t, 1)
	tooMany := make([]string, MaxWaitIDs+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprint(i)
	}
	cases := []struct {
		ids     []string
		mode    string
		timeout time.Duration
	}{
		{nil, "all", time.Second}, {[]string{""}, "all", time.Second},
		{[]string{ids[0], ids[0]}, "all", time.Second}, {tooMany, "all", time.Second},
		{ids, "some", time.Second}, {ids, "all", -time.Second}, {ids, "all", MaxWaitTimeout + time.Second},
	}
	for _, tc := range cases {
		if _, err := s.Wait(context.Background(), tc.ids, tc.mode, tc.timeout); !errors.Is(err, ErrInvalidWait) {
			t.Fatalf("Wait(%v,%q,%s) error = %v", tc.ids, tc.mode, tc.timeout, err)
		}
	}
	start := time.Now()
	if _, err := s.Wait(context.Background(), []string{ids[0], "missing"}, "all", time.Second); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatalf("mixed missing error = %v", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("missing ID did not fail before waiting")
	}
}

func TestWaitBoundedManyIDsAndEnrichedOutcome(t *testing.T) {
	s, store, ids := waitTestService(t, MaxWaitIDs)
	for _, id := range ids {
		transitionWaitTask(t, store, id, tasks.Succeeded)
	}
	got, err := s.Wait(context.Background(), ids, "all", time.Second)
	if err != nil || got.TimedOut || len(got.Tasks) != MaxWaitIDs {
		t.Fatalf("Wait = (%d tasks, timeout=%v, err=%v)", len(got.Tasks), got.TimedOut, err)
	}
	if got.Tasks[0].Outcome != "no_complete_event" || got.Tasks[0].Task.Result != "done-"+ids[0] {
		t.Fatalf("enriched outcome = %+v", got.Tasks[0])
	}
}
