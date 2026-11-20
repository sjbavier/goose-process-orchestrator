package mcpserver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type waitFakeService struct {
	fakeService
	ctx     context.Context
	ids     []string
	mode    string
	timeout time.Duration
	result  scheduler.WaitResult
}

func (f *waitFakeService) Wait(ctx context.Context, ids []string, mode string, timeout time.Duration) (scheduler.WaitResult, error) {
	f.ctx, f.ids, f.mode, f.timeout = ctx, ids, mode, timeout
	if f.result.Tasks == nil {
		<-ctx.Done()
		return scheduler.WaitResult{}, ctx.Err()
	}
	return f.result, nil
}

func waitSession(t *testing.T, service Service) *mcp.ClientSession {
	t.Helper()
	server, err := New(service)
	if err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	if _, err := server.Connect(context.Background(), st, nil); err != nil {
		t.Fatal(err)
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestWaitToolPreservesOrderTimeoutAndEnrichment(t *testing.T) {
	complete := true
	f := &waitFakeService{result: scheduler.WaitResult{TimedOut: true, Tasks: []scheduler.OutcomeResult{
		{Task: tasks.Task{ID: "b", Status: tasks.Succeeded, FinalText: "answer", CompleteSeen: &complete}, Outcome: "completed"},
		{Task: tasks.Task{ID: "a", Status: tasks.Running}},
	}}}
	res, err := waitSession(t, f).CallTool(context.Background(), &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"ids": []string{"b", "a"}, "mode": "any", "timeout_seconds": 7}})
	if err != nil || res.IsError {
		t.Fatalf("CallTool = (%+v, %v)", res, err)
	}
	if f.mode != "any" || f.timeout != 7*time.Second || f.ids[0] != "b" {
		t.Fatalf("input = (%v, %q, %s)", f.ids, f.mode, f.timeout)
	}
	raw, _ := json.Marshal(res.StructuredContent)
	var out WaitOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !out.TimedOut || len(out.Tasks) != 2 || out.Tasks[0].ID != "b" || out.Tasks[0].FinalText != "answer" || out.Tasks[0].Outcome != "completed" || out.Tasks[1].ID != "a" {
		t.Fatalf("output = %+v", out)
	}
}

func TestWaitToolRejectsTimeoutBeforeService(t *testing.T) {
	f := &waitFakeService{result: scheduler.WaitResult{Tasks: []scheduler.OutcomeResult{}}}
	res, err := waitSession(t, f).CallTool(context.Background(), &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{"ids": []string{"a"}, "timeout_seconds": 281}})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || f.ctx != nil {
		t.Fatalf("result=%+v, service ctx=%v", res, f.ctx)
	}
}
