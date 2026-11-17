package mailbox

import (
	"context"
	"database/sql"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
	_ "modernc.org/sqlite"
)

// openTestStore builds a real migrated SQLite-backed tasks.Store, so this
// package's server/client round trip is proven against the actual
// inbox_policy/quota/shape enforcement in internal/tasks, not a double. It
// also returns the raw *sql.DB so a test can flip a task's inbox_policy
// directly, mirroring internal/tasks' own test precedent (there is no
// exported setter; inbox_policy is otherwise admin-only by design).
func openTestStore(t *testing.T) (*tasks.Store, *sql.DB) {
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
	return store, db
}

func startServer(t *testing.T, tokens *Tokens, store Store) (socket string) {
	t.Helper()
	dir := t.TempDir()
	socket = filepath.Join(dir, "mailbox.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go Serve(ctx, listener, tokens, store)
	return socket
}

func TestServeEnforcesControllerOnlyDefaultPolicy(t *testing.T) {
	store, _ := openTestStore(t)
	tokens := NewTokens()
	tokenA, _ := tokens.Mint("task-a")
	socket := startServer(t, tokens, store)

	client := Client{Socket: socket, Token: tokenA}
	resp, err := client.SendMessage("task-b", "note", "hello from a")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.OK {
		t.Fatal("task-b defaults to controller_only, so a task-identity sender must be rejected")
	}
}

func TestServeAllowsSendUnderOpenPolicyAndReadInboxDeliversOnce(t *testing.T) {
	store, db := openTestStore(t)
	if _, err := db.Exec(`UPDATE tasks SET inbox_policy = 'open' WHERE id = ?`, "task-b"); err != nil {
		t.Fatalf("set task-b open: %v", err)
	}
	tokens := NewTokens()
	tokenA, _ := tokens.Mint("task-a")
	tokenB, _ := tokens.Mint("task-b")
	socket := startServer(t, tokens, store)

	sender := Client{Socket: socket, Token: tokenA}
	sendResp, err := sender.SendMessage("task-b", "note", "hello from a")
	if err != nil || !sendResp.OK {
		t.Fatalf("SendMessage = %+v, %v", sendResp, err)
	}

	recipient := Client{Socket: socket, Token: tokenB}
	readResp, err := recipient.ReadInbox()
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if !readResp.OK || len(readResp.Messages) != 1 {
		t.Fatalf("ReadInbox = %+v", readResp)
	}
	if readResp.Messages[0].Sender != "task-a" {
		t.Fatalf("message sender = %q, want task-a", readResp.Messages[0].Sender)
	}

	// Delivered exactly once: a second read sees an empty inbox.
	second, err := recipient.ReadInbox()
	if err != nil {
		t.Fatalf("second ReadInbox: %v", err)
	}
	if len(second.Messages) != 0 {
		t.Fatalf("second ReadInbox = %+v, want empty (already delivered)", second)
	}
}

func TestServeCrossTaskTokenCannotReadAnotherTasksInbox(t *testing.T) {
	store, db := openTestStore(t)
	if _, err := db.Exec(`UPDATE tasks SET inbox_policy = 'open' WHERE id = ?`, "task-b"); err != nil {
		t.Fatalf("set task-b open: %v", err)
	}
	tokens := NewTokens()
	tokenA, _ := tokens.Mint("task-a")
	tokenB, _ := tokens.Mint("task-b")
	socket := startServer(t, tokens, store)

	// Deliver one message into task-b's inbox.
	sender := Client{Socket: socket, Token: tokenA}
	if resp, err := sender.SendMessage("task-b", "note", "for b only"); err != nil || !resp.OK {
		t.Fatalf("SendMessage = %+v, %v", resp, err)
	}

	// task-a's own token can only ever read task-a's own inbox: OpReadInbox
	// carries no target field, so there is no way to ask for task-b's.
	aReadsOwnInbox, err := sender.ReadInbox()
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if len(aReadsOwnInbox.Messages) != 0 {
		t.Fatalf("task-a's token must never see task-b's message, got %+v", aReadsOwnInbox)
	}

	// Only task-b's own token reads it.
	recipient := Client{Socket: socket, Token: tokenB}
	bReadsOwnInbox, err := recipient.ReadInbox()
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if len(bReadsOwnInbox.Messages) != 1 || bReadsOwnInbox.Messages[0].Body != "for b only" {
		t.Fatalf("task-b's token got %+v", bReadsOwnInbox)
	}
}

func TestServeRejectsUnknownTokenOverTheWire(t *testing.T) {
	store, _ := openTestStore(t)
	tokens := NewTokens()
	socket := startServer(t, tokens, store)

	client := Client{Socket: socket, Token: "forged-token"}
	resp, err := client.ReadInbox()
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if resp.OK {
		t.Fatal("a forged token must be rejected")
	}
}

func TestServeMalformedRequestIsRejectedNotFatal(t *testing.T) {
	store, _ := openTestStore(t)
	tokens := NewTokens()
	socket := startServer(t, tokens, store)

	conn, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("not json\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n == 0 {
		t.Fatal("expected a response for a malformed request, not a silent close")
	}

	// The server must still be accepting new connections afterward.
	client := Client{Socket: socket, Token: "forged-token"}
	if _, err := client.ReadInbox(); err != nil {
		t.Fatalf("server did not survive a malformed request: %v", err)
	}
}
