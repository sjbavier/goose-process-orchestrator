package tasks

import (
	"testing"
	"time"
)

func TestTaskLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 25, 18, 19, 0, 0, time.UTC)
	task := Task{ID: "demo-1", Recipe: "recipes/test-recipe.yaml", Status: Queued, CreatedAt: now}
	for _, status := range []Status{Starting, Running, Succeeded} {
		if err := task.Transition(status, now); err != nil {
			t.Fatalf("Transition(%q): %v", status, err)
		}
	}
	if task.StartedAt == nil || task.EndedAt == nil || task.Status != Succeeded {
		t.Fatalf("task did not record its successful lifecycle: %#v", task)
	}
}

func TestRejectsInvalidTransition(t *testing.T) {
	task := Task{Status: Queued}
	if err := task.Transition(Succeeded, time.Now()); err == nil {
		t.Fatal("Transition allowed queued -> succeeded")
	}
}
