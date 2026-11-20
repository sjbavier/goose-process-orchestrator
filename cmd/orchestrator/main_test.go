package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// builtOrchestrator builds this package's own binary once per test run, so
// flag-validation tests can observe main()'s os.Exit behavior from outside
// rather than needing to call main() in-process.
var builtOrchestratorOnce = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "orchestrator-self-build")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "orchestrator")
	cmd := exec.Command("go", "build", "-o", path, "github.com/local/goose-process-orchestrator/cmd/orchestrator")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("go build cmd/orchestrator: %w: %s", err, out)
	}
	return path, nil
})

func builtOrchestrator(t *testing.T) string {
	t.Helper()
	path, err := builtOrchestratorOnce()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// testConfig is the server configuration these tests run against: the
// production defaults for everything except the paths and the explicitly
// varied limits. Run-directory retention stays off (as it is by default), so
// no test's captured artifacts are swept out from under it.
func testConfig(dbPath, recipeRoot, gooseBinary, runsDir string, maxConcurrent int) serverConfig {
	return serverConfig{
		dbPath: dbPath, recipeRoot: recipeRoot, gooseBinary: gooseBinary, runsDir: runsDir,
		maxConcurrent: maxConcurrent, maxTurns: 25, maxTurnsCeiling: 200,
		sweepInterval: 15 * time.Minute, outputLimit: artifacts.DefaultOutputLimit,
	}
}

// fakeGooseScript stands in for the real `goose` CLI so these tests never
// invoke a provider or network endpoint. `sleep.yaml` keeps the task
// "running" long enough to observe it from a second, independently dialed
// connection.
const fakeGooseScript = `#!/bin/sh
recipe=""
maxturns=""
extension=""
allargs="$*"
while [ $# -gt 0 ]; do
  case "$1" in
    --recipe) recipe="$2"; shift 2 ;;
    --max-turns) maxturns="$2"; shift 2 ;;
    --with-extension) extension="$2"; shift 2 ;;
    *) shift ;;
  esac
done
name=$(basename "$recipe")
case "$name" in
  sleep.yaml) sleep 30 & wait ;;
  echo-max-turns.yaml) echo "$maxturns" > "$(dirname "$recipe")/observed-max-turns.txt"; exit 0 ;;
  flood.yaml)
    echo "$allargs" > "$(dirname "$recipe")/observed-argv.txt"
    i=0
    while [ $i -lt 200 ]; do echo "0123456789012345678901234567890123456789"; i=$((i+1)); done
    exit 0 ;;
  mailbox-check.yaml)
    printf '%s\n%s\n' "$extension" "$MAILBOX_TOKEN" > "$(dirname "$recipe")/observed-mailbox.txt"
    exit 0 ;;
  mailbox-e2e-a.yaml|mailbox-e2e-b.yaml)
    base=$(basename "$name" .yaml)
    printf '%s\n%s\n' "$extension" "$MAILBOX_TOKEN" > "$(dirname "$recipe")/observed-$base.txt"
    proceed="$(dirname "$recipe")/proceed-$base"
    i=0
    while [ ! -f "$proceed" ] && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
    exit 0 ;;
  *) echo "unknown fixture $name" 1>&2; exit 2 ;;
esac
`

func newFakeGoose(t *testing.T) (binary, recipeRoot string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "fakegoose.sh")
	if err := os.WriteFile(binary, []byte(fakeGooseScript), 0o755); err != nil {
		t.Fatal(err)
	}
	recipeRoot = t.TempDir()
	for _, fixture := range []string{"sleep.yaml", "echo-max-turns.yaml", "flood.yaml", "mailbox-check.yaml", "mailbox-e2e-a.yaml", "mailbox-e2e-b.yaml"} {
		if err := os.WriteFile(filepath.Join(recipeRoot, fixture), []byte("title: fixture\nprompt: Run fixture.\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return binary, recipeRoot
}

func dialSession(t *testing.T, ctx context.Context, socketPath string) *mcp.ClientSession {
	t.Helper()
	var conn net.Conn
	var err error
	for i := 0; i < 100; i++ {
		conn, err = net.Dial("unix", socketPath)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial %q: %v", socketPath, err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.IOTransport{Reader: conn, Writer: conn}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return session
}

func structuredField(t *testing.T, res *mcp.CallToolResult, field string) string {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		var text string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				text += tc.Text
			}
		}
		t.Fatalf("structured content is %T (isError=%v, text=%q), want map[string]any", res.StructuredContent, res.IsError, text)
	}
	v, _ := m[field].(string)
	return v
}

// pollStatus polls "status" for id via session until it reports want, up to
// timeout, tolerating transient tool errors by retrying rather than failing
// on the first one, the same way a real MCP client would.
func pollStatus(t *testing.T, ctx context.Context, session *mcp.ClientSession, id, want string, timeout time.Duration) string {
	t.Helper()
	var last string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{"id": id}})
		if err == nil && !res.IsError {
			last = structuredField(t, res, "status")
			if last == want {
				return last
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return last
}

// TestSocketServingSurvivesConnectionClose proves C01's focused check: a
// task spawned over one connection remains observable and keeps progressing
// after that connection closes, via a fresh connection to the same socket.
func TestSocketServingSurvivesConnectionClose(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, testConfig(dbPath, recipeRoot, gooseBinary, runsDir, 20)) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session1 := dialSession(t, ctx, socketPath)
	spawnRes, err := session1.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": filepath.Join(recipeRoot, "sleep.yaml"), "idempotency_key": "k1"}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id := structuredField(t, spawnRes, "id")
	if id == "" {
		t.Fatal("spawn returned empty task id")
	}

	// Simulate "Goose Desktop closes": close connection 1 entirely. The
	// IOTransport closes both the read and write side of the same conn, so a
	// benign "already closed" error from the second close is expected.
	_ = session1.Close()

	// A fresh connection must still see the real task state, not "unknown".
	session2 := dialSession(t, ctx, socketPath)
	defer session2.Close()

	if status := pollStatus(t, ctx, session2, id, "running", 20*time.Second); status != "running" {
		t.Fatalf("status=%q after reconnect, want running (registry did not survive the connection close)", status)
	}

	cancelRes, err := session2.CallTool(ctx, &mcp.CallToolParams{Name: "cancel", Arguments: map[string]any{"id": id}})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cr, _ := cancelRes.StructuredContent.(map[string]any)["cancel_requested"].(bool); !cr {
		t.Fatal("cancel_requested=false after cancel via the second connection")
	}

	// Wait for the real OS process to actually die before this test returns,
	// so a lingering fake-binary sleep never leaks into a later test's run
	// and skews its timing under -race/-count stress.
	if status := pollStatus(t, ctx, session2, id, "cancelled", 15*time.Second); status != "cancelled" {
		t.Fatalf("status=%q, want cancelled after cancel via the second connection", status)
	}
}

// TestMaxConcurrentAllowsParallelTasks proves --max-concurrent actually
// raises the scheduler's worker ceiling rather than leaving every spawn
// serialized behind the single hardcoded worker this package used to pass.
// Five long-running tasks are spawned with --max-concurrent=5; all five
// must reach "running" well within one sleep.yaml fixture's 30s runtime,
// which is only possible if they are genuinely running concurrently.
func TestMaxConcurrentAllowsParallelTasks(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")

	const parallel = 5
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runSocket(ctx, socketPath, testConfig(dbPath, recipeRoot, gooseBinary, runsDir, parallel))
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	ids := make([]string, parallel)
	for i := 0; i < parallel; i++ {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
			"recipe":          filepath.Join(recipeRoot, "sleep.yaml"),
			"idempotency_key": fmt.Sprintf("parallel-%d", i),
		}})
		if err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
		ids[i] = structuredField(t, res, "id")
	}

	deadline := time.Now().Add(15 * time.Second)
	remaining := map[string]bool{}
	for _, id := range ids {
		remaining[id] = true
	}
	for time.Now().Before(deadline) && len(remaining) > 0 {
		for id := range remaining {
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{"id": id}})
			if err == nil && !res.IsError && structuredField(t, res, "status") == "running" {
				delete(remaining, id)
			}
		}
		if len(remaining) > 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if len(remaining) > 0 {
		t.Fatalf("%d/%d tasks never reached running concurrently (--max-concurrent did not raise the worker ceiling): %v", len(remaining), parallel, remaining)
	}

	for _, id := range ids {
		if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "cancel", Arguments: map[string]any{"id": id}}); err != nil {
			t.Fatalf("cancel %s: %v", id, err)
		}
	}
	for _, id := range ids {
		if status := pollStatus(t, ctx, session, id, "cancelled", 15*time.Second); status != "cancelled" {
			t.Fatalf("task %s: status=%q, want cancelled", id, status)
		}
	}
}

// TestMaxTurnsDefaultAndPerSpawnOverride proves the server-wide --max-turns
// default actually reaches the real goose invocation, and that a spawn's
// own max_turns input overrides it for that one task without touching the
// default used by any other task.
func TestMaxTurnsDefaultAndPerSpawnOverride(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	observedPath := filepath.Join(recipeRoot, "observed-max-turns.txt")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, testConfig(dbPath, recipeRoot, gooseBinary, runsDir, 20)) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	// No max_turns given: the server default (25, set above) must be used.
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"recipe": filepath.Join(recipeRoot, "echo-max-turns.yaml"), "idempotency_key": "default",
	}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id := structuredField(t, res, "id")
	if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
		t.Fatalf("status=%q, want succeeded", status)
	}
	got, err := os.ReadFile(observedPath)
	if err != nil {
		t.Fatal(err)
	}
	if want := "25\n"; string(got) != want {
		t.Fatalf("observed --max-turns=%q, want %q (server default not applied)", got, want)
	}
	if err := os.Remove(observedPath); err != nil {
		t.Fatal(err)
	}

	// max_turns=7 given: this one spawn must override the server default.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"recipe": filepath.Join(recipeRoot, "echo-max-turns.yaml"), "idempotency_key": "override", "max_turns": 7,
	}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id = structuredField(t, res, "id")
	if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
		t.Fatalf("status=%q, want succeeded", status)
	}
	got, err = os.ReadFile(observedPath)
	if err != nil {
		t.Fatal(err)
	}
	if want := "7\n"; string(got) != want {
		t.Fatalf("observed --max-turns=%q, want %q (per-spawn override did not reach the real invocation)", got, want)
	}
}

// TestMailboxAttachesExtensionAndForwardsToken proves TMB-06's spawn-time
// attach wiring end to end through a real runSocket instance: when
// --mailbox-extension-binary/--mailbox-socket are configured, a real spawned
// invocation receives --with-extension with the expected "mailbox:<binary>
// --socket <socket>" spec and a non-empty MAILBOX_TOKEN in its environment;
// each task's token differs, proving one is minted per spawn rather than
// reused from the server's own environment.
func TestMailboxAttachesExtensionAndForwardsToken(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	mailboxSocket := filepath.Join(dir, "mailbox.sock")
	observedPath := filepath.Join(recipeRoot, "observed-mailbox.txt")

	config := testConfig(dbPath, recipeRoot, gooseBinary, runsDir, 20)
	config.mailboxExtensionBinary = gooseBinary // any accessible regular file; fakegoose never execs it
	config.mailboxSocket = mailboxSocket

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, config) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	spawnOnce := func(key string) string {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
			"recipe": filepath.Join(recipeRoot, "mailbox-check.yaml"), "idempotency_key": key,
		}})
		if err != nil {
			t.Fatalf("spawn: %v", err)
		}
		id := structuredField(t, res, "id")
		if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
			t.Fatalf("status=%q, want succeeded", status)
		}
		got, err := os.ReadFile(observedPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(observedPath); err != nil {
			t.Fatal(err)
		}
		return string(got)
	}

	wantExtension := fmt.Sprintf("mailbox:%s --socket %s", gooseBinary, mailboxSocket)
	first := spawnOnce("mb-1")
	lines := strings.SplitN(first, "\n", 2)
	if len(lines) != 2 {
		t.Fatalf("observed output = %q, want two lines (extension spec, token)", first)
	}
	if lines[0] != wantExtension {
		t.Fatalf("observed --with-extension = %q, want %q", lines[0], wantExtension)
	}
	if lines[1] == "" {
		t.Fatal("observed MAILBOX_TOKEN is empty, want a minted token")
	}

	second := spawnOnce("mb-2")
	secondLines := strings.SplitN(second, "\n", 2)
	if len(secondLines) != 2 || secondLines[1] == "" {
		t.Fatalf("second spawn observed output = %q, want a non-empty token line", second)
	}
	if secondLines[1] == lines[1] {
		t.Fatalf("two spawns got the same mailbox token %q, want a fresh one per task", lines[1])
	}
}

// TestMailboxEndToEndCheckpointDeliveryAndInboxPolicy proves TMB-07's
// objective: checkpoint-time delivery works end to end through a real
// runSocket instance, using the real per-task token TMB-06 mints and
// forwards at spawn time to actually read/write the real mailbox socket
// TMB-05/06 bind at startup — not a double for any of those pieces. It also
// proves a task-to-task send honors the recipient's inbox_policy (default
// controller_only rejects it; flipping the admin-only column to open admits
// it), the same enforcement internal/mailbox's own tests prove in isolation,
// now observed through the full spawn-to-delivery path.
func TestMailboxEndToEndCheckpointDeliveryAndInboxPolicy(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	mailboxSocket := filepath.Join(dir, "mailbox.sock")

	config := testConfig(dbPath, recipeRoot, gooseBinary, runsDir, 20)
	config.mailboxExtensionBinary = gooseBinary // any accessible regular file; fakegoose never execs it
	config.mailboxSocket = mailboxSocket

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, config) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	// spawnRunningMailboxTask spawns a task that attaches the mailbox
	// extension and blocks (simulating an agent between checkpoints) until
	// this test writes its proceed file, so the test can exercise the real
	// per-task token minted and forwarded for it — exactly as a real spawned
	// agent's extension process would receive it via $MAILBOX_TOKEN — while
	// that task's token is still live, rather than after Runner.Run has
	// already revoked it on task completion.
	spawnRunningMailboxTask := func(fixture, key string) (id, token string) {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
			"recipe": filepath.Join(recipeRoot, fixture), "idempotency_key": key,
		}})
		if err != nil {
			t.Fatalf("spawn: %v", err)
		}
		id = structuredField(t, res, "id")
		if status := pollStatus(t, ctx, session, id, "running", 10*time.Second); status != "running" {
			t.Fatalf("status=%q, want running", status)
		}
		base := strings.TrimSuffix(fixture, ".yaml")
		var observed []byte
		deadline := time.Now().Add(5 * time.Second)
		for {
			observed, err = os.ReadFile(filepath.Join(recipeRoot, "observed-"+base+".txt"))
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("read observed file for %s: %v", fixture, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		lines := strings.SplitN(strings.TrimRight(string(observed), "\n"), "\n", 2)
		if len(lines) != 2 || lines[1] == "" {
			t.Fatalf("observed mailbox output = %q, want extension line + non-empty token line", observed)
		}
		return id, lines[1]
	}
	releaseMailboxTask := func(fixture, id string) {
		proceed := filepath.Join(recipeRoot, "proceed-"+strings.TrimSuffix(fixture, ".yaml"))
		if err := os.WriteFile(proceed, []byte("go"), 0o600); err != nil {
			t.Fatal(err)
		}
		if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
			t.Fatalf("status=%q, want succeeded", status)
		}
	}

	idA, tokenA := spawnRunningMailboxTask("mailbox-e2e-a.yaml", "e2e-a")
	idB, tokenB := spawnRunningMailboxTask("mailbox-e2e-b.yaml", "e2e-b")
	defer releaseMailboxTask("mailbox-e2e-a.yaml", idA)
	defer releaseMailboxTask("mailbox-e2e-b.yaml", idB)

	clientA := mailbox.Client{Socket: mailboxSocket, Token: tokenA}
	clientB := mailbox.Client{Socket: mailboxSocket, Token: tokenB}

	// Checkpoint delivery: the controller enqueues through the real
	// send_message tool; task A's agent reads it back via the real mailbox
	// socket, authenticated by its own real token. Delivery is recorded
	// exactly once: a second read sees nothing further.
	sendRes, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "send_message", Arguments: map[string]any{
		"id": idA, "body": "do the next step",
	}})
	if err != nil {
		t.Fatalf("send_message: %v", err)
	}
	if sendRes.IsError {
		t.Fatalf("send_message reported an error: %+v", sendRes.Content)
	}

	firstRead, err := clientA.ReadInbox()
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if len(firstRead.Messages) != 1 || firstRead.Messages[0].Sender != "controller" || firstRead.Messages[0].Body != "do the next step" {
		t.Fatalf("ReadInbox = %+v, want exactly one controller message", firstRead)
	}
	secondRead, err := clientA.ReadInbox()
	if err != nil {
		t.Fatalf("second ReadInbox: %v", err)
	}
	if len(secondRead.Messages) != 0 {
		t.Fatalf("second ReadInbox = %+v, want delivered-once (empty)", secondRead)
	}

	// Task-to-task messaging honors inbox_policy: task A defaults to
	// controller_only, so task B's message is rejected before storage.
	rejected, err := clientB.SendMessage(idA, "note", "hello from B")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if rejected.OK {
		t.Fatal("task A defaults to controller_only, so a task-identity sender must be rejected")
	}

	// Flipping inbox_policy to open has no tool surface (admin-only by
	// design, mirroring internal/mailbox's own test precedent) and is done
	// directly against the real database this orchestrator instance opened.
	db, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE tasks SET inbox_policy = 'open' WHERE id = ?`, idA); err != nil {
		t.Fatalf("set inbox_policy open: %v", err)
	}

	accepted, err := clientB.SendMessage(idA, "note", "hello from B")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if !accepted.OK {
		t.Fatalf("task A is now open, so task B's message should be accepted: %+v", accepted)
	}
	finalRead, err := clientA.ReadInbox()
	if err != nil {
		t.Fatalf("final ReadInbox: %v", err)
	}
	if len(finalRead.Messages) != 1 || finalRead.Messages[0].Sender != idB || finalRead.Messages[0].Body != "hello from B" {
		t.Fatalf("final ReadInbox = %+v, want exactly one message from %q", finalRead, idB)
	}
}

// TestBindOrDialConcurrentStartExactlyOneOwner proves C02's first focused
// check: when several callers race to bind the same socket path concurrently,
// exactly one gets a real listener and every other one cleanly reports
// errAlreadyOwned rather than a bind error or a second independent listener.
func TestBindOrDialConcurrentStartExactlyOneOwner(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "orch.sock")

	const racers = 8
	type outcome struct {
		listener net.Listener
		err      error
	}
	results := make(chan outcome, racers)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < racers; i++ {
		go func() {
			start.Wait()
			ln, err := bindOrDial(socketPath)
			results <- outcome{ln, err}
		}()
	}
	start.Done()

	owners, deferred := 0, 0
	for i := 0; i < racers; i++ {
		res := <-results
		switch {
		case res.err == nil:
			owners++
			defer res.listener.Close()
		case errors.Is(res.err, errAlreadyOwned):
			deferred++
		default:
			t.Fatalf("racer returned unexpected error: %v", res.err)
		}
	}
	if owners != 1 {
		t.Fatalf("owners=%d, want exactly 1 (double-bind or lost race)", owners)
	}
	if deferred != racers-1 {
		t.Fatalf("deferred=%d, want %d", deferred, racers-1)
	}
}

// TestBindOrDialRecoversStaleSocket proves C02's second focused check: a
// socket file left behind by a prior process that exited without unlinking
// it (the out-of-scope crash case) is detected as unconnectable and replaced,
// rather than permanently blocking every future start attempt.
func TestBindOrDialRecoversStaleSocket(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "orch.sock")

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: the socket file survives on disk, but nothing is
	// listening behind it anymore, by closing without unlinking it.
	if u, ok := ln.(*net.UnixListener); ok {
		u.SetUnlinkOnClose(false)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("stale socket file should still exist: %v", err)
	}

	fresh, err := bindOrDial(socketPath)
	if err != nil {
		t.Fatalf("bindOrDial did not recover from a stale socket file: %v", err)
	}
	defer fresh.Close()

	conn, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("dial recovered socket: %v", err)
	}
	conn.Close()
}

// TestBindOrDialNeverExposesGroupOrOtherPermissions guards against finding
// #4: even under a loose inherited umask, the socket file must never exist,
// even momentarily, with group/other permission bits set, closing the
// bind-then-chmod window a connect()-time permission check could otherwise
// race.
// TestRetentionFlagValidationRejectsBadInput proves each new retention flag
// is validated before any server starts, with a clear stderr message and a
// non-zero exit, the same way the pre-existing flags (--max-concurrent,
// --max-turns, --max-turns-ceiling) already are.
func TestRetentionFlagValidationRejectsBadInput(t *testing.T) {
	orch := builtOrchestrator(t)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"negative max age", []string{"--runs-max-age=-1s"}, "--runs-max-age cannot be negative"},
		{"negative max count", []string{"--runs-max-count=-1"}, "--runs-max-count cannot be negative"},
		{"non-positive interval with age set", []string{"--runs-max-age=1m", "--runs-sweep-interval=0s"}, "--runs-sweep-interval must be positive"},
		{"non-positive interval with count set", []string{"--runs-max-count=5", "--runs-sweep-interval=-1s"}, "--runs-sweep-interval must be positive"},
		{"missing mailbox extension binary", []string{"--mailbox-extension-binary=/does/not/exist"}, "is not an accessible regular file"},
		{"empty mailbox socket with extension binary set", []string{"--mailbox-extension-binary=" + orch, "--mailbox-socket="}, "--mailbox-socket cannot be empty"},
		{"missing workdir", []string{"--workdir=/does/not/exist"}, "is not an accessible directory"},
		{"zero output limit", []string{"--output-limit=0"}, "--output-limit must be at least 1"},
		{"negative output limit", []string{"--output-limit=-5"}, "--output-limit must be at least 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(orch, tc.args...)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected a non-zero exit, got none; output: %s", out)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Fatalf("output = %q, want it to contain %q", out, tc.want)
			}
		})
	}
}

// TestOutputLimitCapsCaptureWithoutReachingGooseArgv proves both halves of
// --output-limit's contract end to end through a real socket server: the
// configured value actually reaches the artifacts.Manager that caps
// stdout.raw (a recipe flooding ~8 KiB is kept at exactly the limit), and it
// never leaks into the spawned `goose run`'s own argv — it is a local
// capture bound, not something the child is told about.
func TestOutputLimitCapsCaptureWithoutReachingGooseArgv(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	runsDir := filepath.Join(dir, "runs")

	const limit = 1024
	config := testConfig(filepath.Join(dir, "orchestrator.db"), recipeRoot, gooseBinary, runsDir, 20)
	config.outputLimit = limit

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, config) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"recipe": filepath.Join(recipeRoot, "flood.yaml"), "idempotency_key": "flood",
	}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id := structuredField(t, res, "id")
	if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
		t.Fatalf("status=%q, want succeeded", status)
	}

	matches, err := filepath.Glob(filepath.Join(runsDir, id+"-*", "stdout.raw"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("stdout.raw for %q: matches=%v err=%v", id, matches, err)
	}
	captured, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != limit {
		t.Fatalf("captured %d bytes, want exactly the configured %d-byte limit", len(captured), limit)
	}

	argv, err := os.ReadFile(filepath.Join(recipeRoot, "observed-argv.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(argv), "output-limit") {
		t.Fatalf("goose argv = %q, want no mention of output-limit", argv)
	}
}

// TestRetentionSweepRemovesAgedTerminalRunDir proves C2's wiring end to end:
// an enabled policy, driven by the periodic loop started alongside a real
// socket server, actually removes a terminal task's run directory under
// --runs-dir once it is older than --runs-max-age, without any test reaching
// into internal/retention directly.
func TestRetentionSweepRemovesAgedTerminalRunDir(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")

	config := testConfig(dbPath, recipeRoot, gooseBinary, runsDir, 20)
	config.retention.MaxAge = 3 * time.Second
	config.sweepInterval = 500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runSocket(ctx, socketPath, config) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Logf("runSocket exited: %v", err)
		}
	})

	session := dialSession(t, ctx, socketPath)
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"recipe": filepath.Join(recipeRoot, "echo-max-turns.yaml"), "idempotency_key": "swept",
	}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id := structuredField(t, res, "id")
	if status := pollStatus(t, ctx, session, id, "succeeded", 10*time.Second); status != "succeeded" {
		t.Fatalf("status=%q, want succeeded", status)
	}

	// The run directory is named taskID + "-" + a random suffix (see
	// artifacts.Manager.Create), never the bare task ID.
	findRunDir := func() (string, error) {
		matches, err := filepath.Glob(filepath.Join(runsDir, id+"-*"))
		if err != nil || len(matches) == 0 {
			return "", err
		}
		return matches[0], nil
	}

	if match, err := findRunDir(); err != nil || match == "" {
		t.Fatalf("run directory for %q should exist right after the task finished: match=%q err=%v", id, match, err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		match, err := findRunDir()
		if err == nil && match == "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run directory for %q was never swept", id)
}

// seedReconcilableRunningTask opens dbPath directly (migrating it, exactly as
// newServer itself will), drives one task row through Queued->Starting-
// >Running via the real store, and records a pid that is guaranteed to no
// longer exist (a fixture process started and fully waited out before this
// helper returns). This stands in for a task left `running` by an
// orchestrator that crashed and is now restarting against the same DB file.
// The connection is closed before returning so newServer's own sql.Open
// against the same file is never racing a second open connection.
func seedReconcilableRunningTask(t *testing.T, dbPath, id string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open %q: %v", dbPath, err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := tasks.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate %q: %v", dbPath, err)
	}
	store := tasks.NewStore(db)
	if _, _, err := store.Create(ctx, tasks.Task{ID: id, Recipe: "sleep.yaml", IdempotencyKey: id}); err != nil {
		t.Fatalf("create task %s: %v", id, err)
	}
	if _, err := store.Transition(ctx, id, tasks.Starting, "", nil); err != nil {
		t.Fatalf("transition %s to starting: %v", id, err)
	}
	// A real process, started and fully waited out here, so its pid is
	// guaranteed not to exist by the time Reconcile looks it up.
	cmd := exec.Command("sh", "-c", "exit 0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait fixture: %v", err)
	}
	if _, err := store.RecordProcessIdentity(ctx, id, pid, pid, 1); err != nil {
		t.Fatalf("record identity for %s: %v", id, err)
	}
	if _, err := store.Transition(ctx, id, tasks.Running, "pre-restart", nil); err != nil {
		t.Fatalf("transition %s to running: %v", id, err)
	}
}

// TestNewServerReconcilesBeforeSchedulerStarts proves C7's scope: a DB file
// carrying a non-terminal row from a prior (simulated-crashed) orchestrator
// run must already be reconciled by the time newServer returns, i.e.
// orchestrator.Reconcile ran synchronously on the construction path before
// scheduler.New's workers were ever created. The seeded row's pid is
// verifiably gone, so a correctly wired newServer leaves it Interrupted;
// before C7, nothing in the startup path ever called Reconcile, so the row
// would still read back as Running.
func TestNewServerReconcilesBeforeSchedulerStarts(t *testing.T) {
	_, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	const id = "t-restart-gone"
	seedReconcilableRunningTask(t, dbPath, id)

	config := testConfig(dbPath, recipeRoot, "goose", runsDir, 5)
	server, _, _, closeDB, err := newServer(config)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	defer closeDB()
	_ = server

	// Verify through an independent connection to the same file, rather than
	// reaching into newServer's own store, since newServer does not return it.
	verifyDB, err := sql.Open("sqlite", dbPath+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open %q for verification: %v", dbPath, err)
	}
	defer verifyDB.Close()
	verifyStore := tasks.NewStore(verifyDB)
	task, err := verifyStore.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get task %s: %v", id, err)
	}
	if task.Status != tasks.Interrupted {
		t.Fatalf("status = %q immediately after newServer returned, want %q (Reconcile did not run on the startup path before this check)", task.Status, tasks.Interrupted)
	}
	if task.Result == "" {
		t.Fatal("expected a non-empty reconciliation reason")
	}
}

func TestBindOrDialNeverExposesGroupOrOtherPermissions(t *testing.T) {
	old := syscall.Umask(0) // simulate the loosest plausible inherited umask
	defer syscall.Umask(old)

	socketPath := filepath.Join(t.TempDir(), "orch.sock")
	ln, err := bindOrDial(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("socket file exposed group/other permissions under a loose umask: %v", info.Mode().Perm())
	}
}
