package mcpserver_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/mcpserver"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

type heldRunner struct {
	started chan tasks.Task
	release chan struct{}
}

func (r *heldRunner) Run(_ context.Context, task tasks.Task) {
	r.started <- task
	<-r.release
}

func newAcceptanceServer(t *testing.T) (*mcp.Server, *tasks.Store, *heldRunner, string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "acceptance.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := tasks.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := tasks.NewStore(db)
	runner := &heldRunner{started: make(chan tasks.Task, 2), release: make(chan struct{})}
	t.Cleanup(func() { close(runner.release) })
	dispatcher, err := scheduler.New(store, runner, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	n := 0
	runsRoot := t.TempDir()
	service, err := scheduler.NewService(store, dispatcher, func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return "acceptance-task-" + string(rune('0'+n))
	}, runsRoot)
	if err != nil {
		t.Fatal(err)
	}
	server, err := mcpserver.New(service)
	if err != nil {
		t.Fatal(err)
	}
	return server, store, runner, runsRoot
}

func connect(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	if _, err := server.Connect(context.Background(), serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "black-box-acceptance", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func call(t *testing.T, session *mcp.ClientSession, name string, arguments map[string]any) {
	t.Helper()
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: arguments}); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestBlackBoxAcceptanceAndFailurePaths(t *testing.T) {
	server, store, runner, _ := newAcceptanceServer(t)
	firstClient := connect(t, server)

	start := time.Now()
	call(t, firstClient, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "unique"})
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("spawn blocked for %s", elapsed)
	}
	select {
	case task := <-runner.started:
		if task.ID != "acceptance-task-1" {
			t.Fatalf("started task = %q", task.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("spawn did not reach the owned worker")
	}

	// Repeating an idempotency key is a read of the original task, not a second launch.
	call(t, firstClient, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "unique"})
	select {
	case duplicate := <-runner.started:
		t.Fatalf("duplicate idempotency key launched %q", duplicate.ID)
	case <-time.After(25 * time.Millisecond):
	}

	// Client transport lifetime is not task lifetime; a fresh client can read it.
	if err := firstClient.Close(); err != nil {
		t.Fatal(err)
	}
	secondClient := connect(t, server)
	call(t, secondClient, "status", map[string]any{"id": "acceptance-task-1"})
	call(t, secondClient, "result", map[string]any{"id": "acceptance-task-1"})
	afterDisconnect, err := store.Get(context.Background(), "acceptance-task-1")
	if err != nil {
		t.Fatal(err)
	}
	if afterDisconnect.CancelRequestedAt != nil {
		t.Fatal("client disconnect created cancellation intent")
	}

	// Invalid input and a full queue are tool-level rejections, never launches.
	call(t, secondClient, "spawn", map[string]any{"recipe": "allowed.yaml"})
	call(t, secondClient, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "queued"})
	call(t, secondClient, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "rejected-at-capacity"})
	select {
	case unexpected := <-runner.started:
		t.Fatalf("capacity-rejected task launched %q", unexpected.ID)
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := store.Get(context.Background(), "acceptance-task-4"); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatalf("capacity-rejected task persisted: %v", err)
	}

	call(t, secondClient, "cancel", map[string]any{"id": "acceptance-task-1"})
	cancelled, err := store.Get(context.Background(), "acceptance-task-1")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.CancelRequestedAt == nil {
		t.Fatal("explicit cancel did not persist cancellation intent")
	}
}

// TestLogsAndProgressReadRealCapturedRunDirectory proves logs/progress work
// through the full MCP stack against a run directory artifacts.Manager
// actually created (the same shape internal/orchestrator's fake-binary
// goose double produces), not just a fakeService stand-in. No real
// goose/provider invocation: the stdout.raw content is a synthetic
// stream-json fixture written directly, matching the documented shape.
func TestLogsAndProgressReadRealCapturedRunDirectory(t *testing.T) {
	server, _, _, runsRoot := newAcceptanceServer(t)
	client := connect(t, server)

	start := time.Now()
	call(t, client, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "logs-case"})
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("spawn blocked for %s", elapsed)
	}

	mgr := artifacts.Manager{Root: runsRoot}
	run, err := mgr.Create("acceptance-task-1")
	if err != nil {
		t.Fatal(err)
	}
	stdout := `{"type":"message","message":{"id":"r1","role":"assistant","content":[{"type":"text","text":"working"}],"metadata":{"inference":{"provider":"anthropic","requestedModel":"claude-test"}}}}` + "\n" +
		`{"type":"complete","total_tokens":10,"input_tokens":8,"output_tokens":2,"cache_read_input_tokens":0,"cache_write_input_tokens":0,"cost_usd":0.0001}` + "\n"
	if err := os.WriteFile(run.StdoutPath, []byte(stdout), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(run.StderrPath, []byte("fixture stderr line\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	logsResult, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: "logs", Arguments: map[string]any{"id": "acceptance-task-1"}})
	if err != nil {
		t.Fatalf("logs: %v", err)
	}
	logsOut, ok := logsResult.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("logs structured content = %#v", logsResult.StructuredContent)
	}
	if logsOut["stderr"] != "fixture stderr line\n" {
		t.Fatalf("logs stderr = %v", logsOut["stderr"])
	}

	progressResult, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: "progress", Arguments: map[string]any{"id": "acceptance-task-1"}})
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	progressOut, ok := progressResult.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("progress structured content = %#v", progressResult.StructuredContent)
	}
	if progressOut["last_assistant_text"] != "working" || progressOut["provider"] != "anthropic" {
		t.Fatalf("progress = %#v", progressOut)
	}
	if progressOut["complete"] != true {
		t.Fatalf("progress complete = %v, want true", progressOut["complete"])
	}

	// A task ID with no run directory at all is a clear tool-level error,
	// not a Go/protocol-level failure or a silently empty success.
	noRun, err := client.CallTool(context.Background(), &mcp.CallToolParams{Name: "logs", Arguments: map[string]any{"id": "never-spawned"}})
	if err != nil {
		t.Fatalf("logs(never-spawned): %v", err)
	}
	if !noRun.IsError {
		t.Fatal("logs(never-spawned) did not report a tool error")
	}
}

// TestStatusAndResultShowOutcomeSummaryFields is a real round-trip
// status/result call, through the full MCP stack, against a task produced
// by the full fake-goose pipeline shape (a real store-backed task, driven
// to Succeeded with a recorded outcome summary) — proving the new JSON
// fields appear with correct omitempty behavior, not just at the Go struct
// level TestOutcomeOutput* already covers.
func TestStatusAndResultShowOutcomeSummaryFields(t *testing.T) {
	server, store, runner, _ := newAcceptanceServer(t)
	client := connect(t, server)

	call(t, client, "spawn", map[string]any{"recipe": "allowed.yaml", "idempotency_key": "outcome-case"})
	id := "acceptance-task-1"
	ctx := context.Background()
	// Wait for the dispatcher's worker to actually claim the task
	// (Queued->Starting) before driving it further; spawn itself only
	// enqueues and returns.
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("spawn did not reach the owned worker")
	}
	if _, err := store.Transition(ctx, id, tasks.Running, "", nil); err != nil {
		t.Fatal(err)
	}
	exit := 0
	if _, err := store.Transition(ctx, id, tasks.Succeeded, "run-dir-path", &exit); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordOutcomeSummary(ctx, id, tasks.OutcomeSummary{FinalText: "done talking", CompleteSeen: true, TotalTokens: 12}); err != nil {
		t.Fatal(err)
	}

	for _, tool := range []string{"status", "result"} {
		res, err := client.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{"id": id}})
		if err != nil {
			t.Fatalf("%s: %v", tool, err)
		}
		out, ok := res.StructuredContent.(map[string]any)
		if !ok {
			t.Fatalf("%s structured content = %#v", tool, res.StructuredContent)
		}
		if out["outcome"] != "completed" || out["final_text"] != "done talking" {
			t.Fatalf("%s = %#v, want outcome=completed final_text=\"done talking\"", tool, out)
		}
		if out["result"] != "run-dir-path" {
			t.Fatalf("%s result field changed meaning: %#v", tool, out)
		}
		if _, present := out["stderr_tail"]; present {
			t.Fatalf("%s: stderr_tail present for a non-process_failed outcome: %#v", tool, out)
		}
	}
}
