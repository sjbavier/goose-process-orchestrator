//go:build darwin

package supervisor

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestCancelTerminatesVerifiedDarwinProcessGroup(t *testing.T) {
	var out, err bytes.Buffer
	p, startErr := Start("sh", []string{"-c", "sleep 30 & wait"}, t.TempDir(), nil, &out, &err)
	if startErr != nil {
		t.Fatal(startErr)
	}

	done := make(chan Result, 1)
	go func() { done <- p.Cancel(context.Background(), 100*time.Millisecond) }()
	select {
	case result := <-done:
		if !result.Cancelled {
			t.Fatal("cancel did not record cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verified task-owned process group did not terminate")
	}
}
