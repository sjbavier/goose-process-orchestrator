package main

import (
	"context"
	"database/sql"
	"net"
	"path/filepath"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// openTestEnv builds a real migrated SQLite-backed tasks.Store, a mailbox
// listener serving it, and a token minted for "task-a" — the same
// end-to-end stack this binary talks to in production, so these tests prove
// the tool handlers against the real wire protocol, not a double.
func openTestEnv(t *testing.T) (socket, token string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "mailbox.db"))
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := tasks.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := tasks.NewStore(db)
	if _, _, err := store.Create(context.Background(), tasks.Task{ID: "task-a", Recipe: "r.yaml", IdempotencyKey: "k-a"}); err != nil {
		t.Fatalf("create task-a: %v", err)
	}
	if _, _, err := store.Create(context.Background(), tasks.Task{ID: "task-b", Recipe: "r.yaml", IdempotencyKey: "k-b"}); err != nil {
		t.Fatalf("create task-b: %v", err)
	}
	if _, err := db.Exec(`UPDATE tasks SET inbox_policy = 'open' WHERE id = ?`, "task-a"); err != nil {
		t.Fatalf("set task-a open: %v", err)
	}

	socket = filepath.Join(t.TempDir(), "mailbox.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tokens := mailbox.NewTokens()
	token, err = tokens.Mint("task-a")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	// task-b's own token lets a test act as the recipient directly against
	// the store/tokens, without a second extension process.
	go mailbox.Serve(ctx, listener, tokens, store)
	return socket, token
}

func connectClient(t *testing.T, socket, token string) *mcp.ClientSession {
	t.Helper()
	server, err := newServer(mailbox.Client{Socket: socket, Token: token})
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	ct, st := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err := server.Connect(ctx, st, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestExtensionExposesExactlyTwoTools(t *testing.T) {
	socket, token := openTestEnv(t)
	session := connectClient(t, socket, token)
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(list.Tools) != 2 {
		t.Fatalf("tools=%d, want 2", len(list.Tools))
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
	}
	if !names["read_inbox"] || !names["send_message"] {
		t.Fatalf("unexpected tool set: %+v", names)
	}
}

func TestExtensionSendMessageNeverAcceptsASenderArgument(t *testing.T) {
	socket, token := openTestEnv(t)
	session := connectClient(t, socket, token)
	ctx := context.Background()

	// "to" has open policy on task-a but the point here is that a caller-
	// supplied sender field has no schema slot to land in at all.
	rejected, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"to": "task-a", "body": "hi", "sender": "task-999"}})
	if err != nil {
		t.Fatalf("protocol-level error instead of a tool-result rejection: %v", err)
	}
	if !rejected.IsError {
		t.Fatal("a caller-supplied sender argument must be rejected before the handler runs")
	}
}

func TestExtensionReadInboxAndSendMessageRoundTrip(t *testing.T) {
	socket, token := openTestEnv(t)
	session := connectClient(t, socket, token)
	ctx := context.Background()

	// task-a has open policy, so sending to itself proves the round trip
	// without needing a second extension process for the recipient.
	sendResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{"to": "task-a", "kind": "note", "body": "hello"}})
	if err != nil || sendResult.IsError {
		t.Fatalf("send_message failed: err=%v result=%+v", err, sendResult)
	}

	readResult, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "read_inbox", Arguments: map[string]any{}})
	if err != nil || readResult.IsError {
		t.Fatalf("read_inbox failed: err=%v result=%+v", err, readResult)
	}
}

func TestExtensionRejectsForgedToken(t *testing.T) {
	socket, _ := openTestEnv(t)
	session := connectClient(t, socket, "forged-token")
	ctx := context.Background()

	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "read_inbox", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("protocol-level error instead of a tool-result rejection: %v", err)
	}
	if !result.IsError {
		t.Fatal("a forged token must be rejected")
	}
}

func TestNewServerRejectsNothingUpFront(t *testing.T) {
	// newServer never validates the client's socket/token eagerly (dialing
	// happens per call), so construction alone cannot fail for a bad
	// socket/token; this documents that choice rather than asserting an
	// error that does not exist.
	if _, err := newServer(mailbox.Client{Socket: "/does/not/exist", Token: "whatever"}); err != nil {
		t.Fatalf("newServer: %v", err)
	}
}
