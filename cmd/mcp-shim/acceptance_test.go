package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// shimFixture bundles everything a real mcp-shim subprocess needs to run
// against a shared socket path and fake-binary goose setup.
type shimFixture struct {
	shimBin, orchBin, gooseBin, recipeRoot, socketPath, dbPath, runsDir string
}

func newShimFixture(t *testing.T) shimFixture {
	t.Helper()
	gooseBin, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := filepath.Join(dir, "orch.sock")
	t.Cleanup(func() { killOrchestratorFor(socketPath) })
	return shimFixture{
		shimBin:    shimBinary(t),
		orchBin:    orchestratorBinary(t),
		gooseBin:   gooseBin,
		recipeRoot: recipeRoot,
		socketPath: socketPath,
		dbPath:     filepath.Join(dir, "orchestrator.db"),
		runsDir:    filepath.Join(dir, "runs"),
	}
}

// killOrchestratorFor kills whatever auto-started orchestrator process is
// serving socketPath. The real orchestrator this fixture starts is
// deliberately detached (Setsid, reparented away from the shim that spawned
// it, and away from this test process once that shim exits) and is
// expected to outlive any one shim's disconnect — that is this feature's
// entire point — so a test must find it by its unique socket path argument
// in /proc rather than by a process handle it never has.
func killOrchestratorFor(socketPath string) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
		if containsArgPair(args, "--listen", socketPath) {
			if p, err := os.FindProcess(pid); err == nil {
				_ = p.Signal(syscall.SIGKILL)
			}
		}
	}
}

func containsArgPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

// launchedShim is a real mcp-shim subprocess, its stdio piped exactly as an
// MCP client would, with an MCP client session already connected over it.
type launchedShim struct {
	cmd     *exec.Cmd
	session *mcp.ClientSession
}

// launchShim starts the real cmd/mcp-shim binary as an external process and
// connects an MCP client session over its piped stdio, simulating exactly
// what an MCP client (Goose Desktop, Claude Code, etc.) does.
func launchShim(t *testing.T, ctx context.Context, f shimFixture) launchedShim {
	t.Helper()
	cmd := exec.Command(f.shimBin,
		"--socket", f.socketPath,
		"--orchestrator-binary", f.orchBin,
		"--recipe-root", f.recipeRoot,
		"--goose-binary", f.gooseBin,
		"--db", f.dbPath,
		"--runs-dir", f.runsDir,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	client := mcp.NewClient(&mcp.Implementation{Name: "acceptance-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.IOTransport{Reader: stdout, Writer: stdin}, nil)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("connect through shim subprocess: %v", err)
	}
	return launchedShim{cmd: cmd, session: session}
}

// closeAsClientDisconnect simulates "Goose Desktop closes": the client's
// end of the shim's stdio pipes close, which is exactly what the shim's
// relay observes as an EOF on its own stdin. It waits for the real shim
// process to actually exit (not merely for the session handle to report
// closed), which is the strongest form of "the connection is gone" the
// test can observe.
func closeAsClientDisconnect(t *testing.T, ls launchedShim) {
	t.Helper()
	_ = ls.session.Close()
	done := make(chan error, 1)
	go func() { done <- ls.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = ls.cmd.Process.Kill()
		t.Fatal("shim subprocess did not exit after its client connection closed")
	}
}

func callTool(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

func field(t *testing.T, res *mcp.CallToolResult, key string) string {
	t.Helper()
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		var text string
		for _, c := range res.Content {
			if tc, ok := c.(*mcp.TextContent); ok {
				text += tc.Text
			}
		}
		t.Fatalf("structured content is %T (isError=%v, text=%q)", res.StructuredContent, res.IsError, text)
	}
	v, _ := m[key].(string)
	return v
}

func pollField(t *testing.T, ctx context.Context, session *mcp.ClientSession, id, key, want string, timeout time.Duration) string {
	t.Helper()
	var last string
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{"id": id}})
		if err == nil && !res.IsError {
			last = field(t, res, key)
			if last == want {
				return last
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last
}

// TestAcceptance1and2_SurvivesClientDisconnectAndReconnect proves plan.md's
// acceptance criteria 1 and 2 end-to-end through the real shim binary:
// closing shim A's connection (its process actually exits) does not stop
// the orchestrator or the task it started, and a fresh shim B sees the
// task's real current state rather than "unknown".
func TestAcceptance1and2_SurvivesClientDisconnectAndReconnect(t *testing.T) {
	f := newShimFixture(t)
	ctx := context.Background()

	a := launchShim(t, ctx, f)
	spawnRes := callTool(t, ctx, a.session, "spawn", map[string]any{"recipe": filepath.Join(f.recipeRoot, "sleep.yaml"), "idempotency_key": "k1"})
	id := field(t, spawnRes, "id")
	if id == "" {
		t.Fatal("spawn returned empty task id")
	}

	closeAsClientDisconnect(t, a) // criterion 1: shim A's process (the "client") is gone.

	b := launchShim(t, ctx, f) // criterion 2: a fresh shim must reach the same live orchestrator, not start a new one.
	defer closeAsClientDisconnect(t, b)

	if status := pollField(t, ctx, b.session, id, "status", "running", 15*time.Second); status != "running" {
		t.Fatalf("status via shim B = %q, want running (orchestrator/task state did not survive shim A's disconnect)", status)
	}
}

// TestAcceptance3_CancelViaSecondShimReachesRealProcess proves plan.md's
// acceptance criterion 3: a cancel issued through a second, independently
// launched shim reaches the real owned OS process group started by the
// first shim's spawn, and actually terminates it.
func TestAcceptance3_CancelViaSecondShimReachesRealProcess(t *testing.T) {
	f := newShimFixture(t)
	ctx := context.Background()

	a := launchShim(t, ctx, f)
	spawnRes := callTool(t, ctx, a.session, "spawn", map[string]any{"recipe": filepath.Join(f.recipeRoot, "sleep.yaml"), "idempotency_key": "k1"})
	id := field(t, spawnRes, "id")
	closeAsClientDisconnect(t, a)

	b := launchShim(t, ctx, f)
	defer closeAsClientDisconnect(t, b)
	if status := pollField(t, ctx, b.session, id, "status", "running", 15*time.Second); status != "running" {
		t.Fatalf("status via shim B = %q, want running", status)
	}

	cancelRes := callTool(t, ctx, b.session, "cancel", map[string]any{"id": id})
	m, _ := cancelRes.StructuredContent.(map[string]any)
	if cr, _ := m["cancel_requested"].(bool); !cr {
		t.Fatal("cancel_requested=false after cancel via the second shim")
	}
	if status := pollField(t, ctx, b.session, id, "status", "cancelled", 15*time.Second); status != "cancelled" {
		t.Fatalf("status via shim B = %q after cancel, want cancelled (the real process was not actually terminated)", status)
	}
}

// TestAcceptance4_TwoConcurrentlyConnectedShimsShareTaskSet proves plan.md's
// acceptance criterion 4: two shims connected at the same time (not one
// after the other) both see and act on the same task set.
func TestAcceptance4_TwoConcurrentlyConnectedShimsShareTaskSet(t *testing.T) {
	f := newShimFixture(t)
	ctx := context.Background()

	a := launchShim(t, ctx, f)
	defer closeAsClientDisconnect(t, a)
	b := launchShim(t, ctx, f)
	defer closeAsClientDisconnect(t, b)

	spawnRes := callTool(t, ctx, a.session, "spawn", map[string]any{"recipe": filepath.Join(f.recipeRoot, "sleep.yaml"), "idempotency_key": "k1"})
	id := field(t, spawnRes, "id")

	// Both connections are still open right now: b must see a's task, and a
	// must see the effect of cancelling it via b, with neither closed yet.
	if status := pollField(t, ctx, b.session, id, "status", "running", 15*time.Second); status != "running" {
		t.Fatalf("status via b (while a is still connected) = %q, want running", status)
	}
	callTool(t, ctx, b.session, "cancel", map[string]any{"id": id})
	if status := pollField(t, ctx, a.session, id, "status", "cancelled", 15*time.Second); status != "cancelled" {
		t.Fatalf("status via a (while b is still connected) = %q after b cancelled it, want cancelled", status)
	}
}

// TestAcceptance5_ConcurrentShimAutoStartRaceIsSafe proves plan.md's
// acceptance criterion 5 through the real binaries: when nothing is
// listening yet and several shims race to auto-start the orchestrator
// concurrently, exactly one real orchestrator ends up serving, and every
// shim reaches that same one rather than an independent registry.
func TestAcceptance5_ConcurrentShimAutoStartRaceIsSafe(t *testing.T) {
	f := newShimFixture(t)
	ctx := context.Background()

	const racers = 4
	results := make(chan launchedShim, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- launchShim(t, ctx, f)
		}()
	}
	wg.Wait()
	close(results)

	var shims []launchedShim
	for ls := range results {
		shims = append(shims, ls)
	}
	defer func() {
		for _, ls := range shims {
			closeAsClientDisconnect(t, ls)
		}
	}()
	if len(shims) != racers {
		t.Fatalf("launched %d shims, want %d", len(shims), racers)
	}

	spawnRes := callTool(t, ctx, shims[0].session, "spawn", map[string]any{"recipe": filepath.Join(f.recipeRoot, "sleep.yaml"), "idempotency_key": "race"})
	id := field(t, spawnRes, "id")
	if id == "" {
		t.Fatal("spawn returned empty task id")
	}

	for i, ls := range shims {
		status := pollField(t, ctx, ls.session, id, "status", "running", 15*time.Second)
		if status != "running" {
			t.Fatalf("shim %d: status=%q, want running (a racing shim reached an independent orchestrator instance)", i, status)
		}
	}
}
