package mcpserver

import (
	"context"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fakeService struct {
	task                      tasks.Task
	spawnCtx                  context.Context
	spawnRequest              scheduler.SpawnRequest
	sentMessage               *tasks.Message
	listStatus, listKeyPrefix string
	listLimit                 int
}

func (f *fakeService) ServerInfo(context.Context) (scheduler.ServerInfo, error) {
	return scheduler.ServerInfo{}, nil
}
func (f *fakeService) Spawn(ctx context.Context, r scheduler.SpawnRequest) (tasks.Task, error) {
	f.spawnCtx = ctx
	f.spawnRequest = r
	f.task = tasks.Task{ID: "task-1", Status: tasks.Queued, Recipe: r.Recipe, InboxPolicy: tasks.InboxPolicy(r.InboxPolicy)}
	return f.task, nil
}
func (f *fakeService) Outcome(context.Context, string) (scheduler.OutcomeResult, error) {
	f.task.Result = "done"
	return scheduler.OutcomeResult{Task: f.task}, nil
}
func (f *fakeService) Wait(_ context.Context, ids []string, _ string, _ time.Duration) (scheduler.WaitResult, error) {
	out := make([]scheduler.OutcomeResult, len(ids))
	for i, id := range ids {
		out[i] = scheduler.OutcomeResult{Task: tasks.Task{ID: id, Status: tasks.Succeeded, Result: "done"}}
	}
	return scheduler.WaitResult{Tasks: out}, nil
}
func (f *fakeService) Cancel(context.Context, string) (tasks.Task, error) { return f.task, nil }
func (f *fakeService) List(_ context.Context, status, keyPrefix string, limit int) ([]tasks.Task, error) {
	f.listStatus, f.listKeyPrefix, f.listLimit = status, keyPrefix, limit
	return []tasks.Task{f.task}, nil
}
func (f *fakeService) Logs(context.Context, string, string, int) (scheduler.LogsResult, error) {
	return scheduler.LogsResult{Stdout: "fake stdout", Stderr: "fake stderr"}, nil
}
func (f *fakeService) Progress(context.Context, string) (scheduler.ProgressResult, error) {
	return scheduler.ProgressResult{Progress: logs.Progress{MessageEventCount: 1, LastAssistantText: "fake progress"}}, nil
}
func (f *fakeService) SendMessage(_ context.Context, id, body, kind string) (tasks.Message, error) {
	f.sentMessage = &tasks.Message{ID: 1, TaskID: id, Sender: tasks.ControllerSender, Kind: kind, Body: body}
	return *f.sentMessage, nil
}
func TestOnlyApprovedToolsAndRoundTrips(t *testing.T) {
	f := &fakeService{}
	server, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err = server.Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) != 10 {
		t.Fatalf("tools=%d, want 10", len(list.Tools))
	}
	for _, call := range []struct {
		name string
		args map[string]any
	}{{"server_info", map[string]any{}}, {"spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "one"}}, {"status", map[string]any{"id": "task-1"}}, {"result", map[string]any{"id": "task-1"}}, {"wait", map[string]any{"ids": []string{"task-1"}}}, {"cancel", map[string]any{"id": "task-1"}}, {"list", map[string]any{}}, {"list", map[string]any{"status": "queued", "key_prefix": "ENG-29341:", "limit": 10}}, {"logs", map[string]any{"id": "task-1"}}, {"logs", map[string]any{"id": "task-1", "stream": "stdout", "max_bytes": 100}}, {"progress", map[string]any{"id": "task-1"}}, {"send_message", map[string]any{"id": "task-1", "body": "hello"}}, {"send_message", map[string]any{"id": "task-1", "body": "hello", "kind": "note"}}} {
		if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: call.name, Arguments: call.args}); err != nil {
			t.Fatalf("%s: %v", call.name, err)
		}
	}
	if f.listStatus != "queued" || f.listKeyPrefix != "ENG-29341:" || f.listLimit != 10 {
		t.Fatalf("list filters = (%q, %q, %d)", f.listStatus, f.listKeyPrefix, f.listLimit)
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": "allowed.yaml"}}); err != nil {
		t.Fatalf("invalid request is a tool result, not protocol failure: %v", err)
	}
}
func TestNewRejectsNilService(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
}

// TestSendMessageNeverAcceptsASenderArgument guards TMB-04's core contract:
// the controller connection is always privileged and the tool has no sender
// field for a caller to smuggle one in through. A caller-supplied "sender"
// argument must be silently ignored (unknown input fields), not honored.
func TestSendMessageNeverAcceptsASenderArgument(t *testing.T) {
	f := &fakeService{}
	server, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err = server.Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	// A caller-supplied "sender" field is not merely ignored: the tool's
	// input schema has no such property, so the SDK rejects the call before
	// the handler ever runs (as a tool-result error, not a transport
	// error), proving there is no sender field to smuggle a value through.
	rejected, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"id": "task-1", "body": "hello", "sender": "task-999"}})
	if err != nil {
		t.Fatal(err)
	}
	if !rejected.IsError {
		t.Fatal("send_message with an unknown \"sender\" argument unexpectedly succeeded")
	}
	if f.sentMessage != nil {
		t.Fatalf("sentMessage = %+v, want nil: the rejected call must never reach the handler", f.sentMessage)
	}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"id": "task-1", "body": "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("send_message tool error: %+v", res.Content)
	}
	if f.sentMessage == nil || f.sentMessage.Sender != tasks.ControllerSender {
		t.Fatalf("sentMessage = %+v, want sender %q", f.sentMessage, tasks.ControllerSender)
	}
}

// TestSpawnThreadsInboxPolicy guards MBO-03's contract: an "open" inbox_policy
// is accepted with no additional caller-class gating (Selection B) and reaches
// scheduler.SpawnRequest unchanged; an omitted field behaves as before.
func TestSpawnThreadsInboxPolicy(t *testing.T) {
	f := &fakeService{}
	server, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err = server.Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": "allowed.yaml", "idempotency_key": "one", "inbox_policy": "open"}}); err != nil {
		t.Fatal(err)
	}
	if f.spawnRequest.InboxPolicy != "open" {
		t.Fatalf("spawnRequest.InboxPolicy = %q, want %q", f.spawnRequest.InboxPolicy, "open")
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": "allowed.yaml", "idempotency_key": "two"}}); err != nil {
		t.Fatal(err)
	}
	if f.spawnRequest.InboxPolicy != "" {
		t.Fatalf("spawnRequest.InboxPolicy = %q, want empty when omitted", f.spawnRequest.InboxPolicy)
	}
}

func TestSpawnContextSurvivesClientDisconnect(t *testing.T) {
	f := &fakeService{}
	server, err := New(f)
	if err != nil {
		t.Fatal(err)
	}
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err := server.Connect(ctx, serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": "allowed.yaml", "idempotency_key": "disconnect"}}); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if f.spawnCtx == nil {
		t.Fatal("spawn did not receive a context")
	}
	select {
	case <-f.spawnCtx.Done():
		t.Fatal("client disconnect canceled spawn context")
	default:
	}
}
