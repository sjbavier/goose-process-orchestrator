package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/testutil"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

// testFlags is the forwarded-flag set these tests auto-start an orchestrator
// with: the production defaults for everything except the paths, so run-
// directory retention stays off (as it is by default) and no test's
// artifacts are swept out from under it.
func testFlags(dbPath, recipeRoot, gooseBinary, runsDir string) orchestratorFlags {
	return orchestratorFlags{
		dbPath: dbPath, recipeRoot: recipeRoot, gooseBinary: gooseBinary, runsDir: runsDir,
		maxConcurrent: 20, maxTurns: 25, runsSweepInterval: 15 * time.Minute,
		mailboxSocket: "mailbox.sock", maxTurnsCeiling: 200,
		outputLimit: artifacts.DefaultOutputLimit,
	}
}

// killDetached terminates a process started by autoStart. Since Setsid made
// it its own session leader, its pid is also its session/process-group id.
func killDetached(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_, _ = cmd.Process.Wait()
}

// fakeGooseScript stands in for the real `goose` CLI so these tests never
// invoke a provider or network endpoint.
const fakeGooseScript = `#!/bin/sh
recipe=""
while [ $# -gt 0 ]; do
  case "$1" in
    --recipe) recipe="$2"; shift 2 ;;
    *) shift ;;
  esac
done
name=$(basename "$recipe")
case "$name" in
  success.yaml) echo ok; exit 0 ;;
  sleep.yaml) sleep 30 & wait ;;
  *) echo "unknown fixture $name" 1>&2; exit 2 ;;
esac
`

var (
	buildOnce sync.Once
	builtPath string
	buildErr  error

	shimBuildOnce sync.Once
	shimBuiltPath string
	shimBuildErr  error
)

// orchestratorBinary builds the real cmd/orchestrator binary once per test
// run, so these tests exercise mcp-shim's actual auto-start exec path
// against the real program, not a stand-in.
func orchestratorBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mcp-shim-orchestrator-build")
		if err != nil {
			buildErr = err
			return
		}
		builtPath = filepath.Join(dir, "orchestrator")
		cmd := exec.Command("go", "build", "-o", builtPath, "github.com/local/goose-process-orchestrator/cmd/orchestrator")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build cmd/orchestrator: %w: %s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtPath
}

// shimBinary builds the real cmd/mcp-shim binary once per test run, so the
// acceptance tests launch it as an actual external process (its stdio
// piped, exactly as an MCP client would) rather than calling its internal
// functions directly.
func shimBinary(t *testing.T) string {
	t.Helper()
	shimBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mcp-shim-build")
		if err != nil {
			shimBuildErr = err
			return
		}
		shimBuiltPath = filepath.Join(dir, "mcp-shim")
		cmd := exec.Command("go", "build", "-o", shimBuiltPath, "github.com/local/goose-process-orchestrator/cmd/mcp-shim")
		if out, err := cmd.CombinedOutput(); err != nil {
			shimBuildErr = fmt.Errorf("go build cmd/mcp-shim: %w: %s", err, out)
		}
	})
	if shimBuildErr != nil {
		t.Fatal(shimBuildErr)
	}
	return shimBuiltPath
}

func newFakeGoose(t *testing.T) (binary, recipeRoot string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "fakegoose.sh")
	if err := os.WriteFile(binary, []byte(fakeGooseScript), 0o755); err != nil {
		t.Fatal(err)
	}
	recipeRoot = t.TempDir()
	for _, fixture := range []string{"success.yaml", "sleep.yaml"} {
		if err := os.WriteFile(filepath.Join(recipeRoot, fixture), []byte("title: fixture\nprompt: Run fixture.\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return binary, recipeRoot
}

// TestOrchestratorFlagsArgsCarriesRetentionFlags proves the three retention
// flags reach the auto-started orchestrator's argv unconditionally,
// including their zero values, so that process's policy is always exactly
// what this shim was configured with rather than silently falling back to
// the orchestrator binary's own defaults.
func TestOrchestratorFlagsArgsCarriesRetentionFlags(t *testing.T) {
	f := orchestratorFlags{
		dbPath: "db", recipeRoot: "recipes", gooseBinary: "goose", runsDir: "runs",
		maxConcurrent: 20, maxTurns: 25,
		runsMaxAge: 90 * time.Minute, runsMaxCount: 50, runsSweepInterval: 5 * time.Minute,
	}
	args := f.args()
	want := map[string]string{
		"--runs-max-age":        "1h30m0s",
		"--runs-max-count":      "50",
		"--runs-sweep-interval": "5m0s",
	}
	for flag, value := range want {
		found := false
		for i, a := range args {
			if a == flag {
				found = true
				if i+1 >= len(args) || args[i+1] != value {
					t.Fatalf("%s value = %v, want %q", flag, args[i+1:i+2], value)
				}
			}
		}
		if !found {
			t.Fatalf("args() = %v, missing %s", args, flag)
		}
	}
}

// TestOrchestratorFlagsArgsCarriesMailboxAndCeilingFlagsAtDefaults proves a
// shim invoked with none of the four mailbox/ceiling/workdir flags set still
// forwards them, at values matching the orchestrator binary's own defaults —
// behaviorally equivalent to today, never silently omitted.
func TestOrchestratorFlagsArgsCarriesMailboxAndCeilingFlagsAtDefaults(t *testing.T) {
	f := orchestratorFlags{
		dbPath: "db", recipeRoot: "recipes", gooseBinary: "goose", runsDir: "runs",
		maxConcurrent: 20, maxTurns: 25,
		mailboxSocket: "mailbox.sock", maxTurnsCeiling: 200,
		outputLimit: artifacts.DefaultOutputLimit,
	}
	args := f.args()
	want := map[string]string{
		"--mailbox-extension-binary": "",
		"--mailbox-socket":           "mailbox.sock",
		"--max-turns-ceiling":        "200",
		"--workdir":                  "",
		"--output-limit":             strconv.FormatInt(artifacts.DefaultOutputLimit, 10),
	}
	for flag, value := range want {
		found := false
		for i, a := range args {
			if a == flag {
				found = true
				if i+1 >= len(args) || args[i+1] != value {
					t.Fatalf("%s value = %v, want %q", flag, args[i+1:i+2], value)
				}
			}
		}
		if !found {
			t.Fatalf("args() = %v, missing %s", args, flag)
		}
	}
}

// TestOrchestratorFlagsArgsCarriesMailboxAndCeilingFlagsExplicit proves a
// shim invoked with the four mailbox/ceiling/workdir flags set forwards the exact
// given values, mirroring the retention-flag test's assertion shape.
func TestOrchestratorFlagsArgsCarriesMailboxAndCeilingFlagsExplicit(t *testing.T) {
	f := orchestratorFlags{
		dbPath: "db", recipeRoot: "recipes", gooseBinary: "goose", runsDir: "runs",
		maxConcurrent: 20, maxTurns: 25,
		mailboxExtensionBinary: "/usr/local/bin/mailbox-extension",
		mailboxSocket:          "custom-mailbox.sock",
		maxTurnsCeiling:        500,
		workdir:                "/home/you/project",
		outputLimit:            4096,
	}
	args := f.args()
	want := map[string]string{
		"--mailbox-extension-binary": "/usr/local/bin/mailbox-extension",
		"--mailbox-socket":           "custom-mailbox.sock",
		"--max-turns-ceiling":        "500",
		"--workdir":                  "/home/you/project",
		"--output-limit":             "4096",
	}
	for flag, value := range want {
		found := false
		for i, a := range args {
			if a == flag {
				found = true
				if i+1 >= len(args) || args[i+1] != value {
					t.Fatalf("%s value = %v, want %q", flag, args[i+1:i+2], value)
				}
			}
		}
		if !found {
			t.Fatalf("args() = %v, missing %s", args, flag)
		}
	}
}

// TestOutputLimitValidationRejectsNonPositive proves the shim refuses a
// non-positive --output-limit itself rather than forwarding it and leaving
// the operator with an opaque dial failure once the auto-started
// orchestrator (which rejects it too) exits immediately.
func TestOutputLimitValidationRejectsNonPositive(t *testing.T) {
	shim := shimBinary(t)
	_, recipeRoot := newFakeGoose(t)
	for _, value := range []string{"0", "-5"} {
		t.Run(value, func(t *testing.T) {
			cmd := exec.Command(shim,
				"--socket", filepath.Join(t.TempDir(), "orch.sock"),
				"--orchestrator-binary", orchestratorBinary(t),
				"--recipe-root", recipeRoot,
				"--output-limit", value,
			)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected a non-zero exit, got none; output: %s", out)
			}
			if want := "--output-limit must be at least 1"; !strings.Contains(string(out), want) {
				t.Fatalf("output = %q, want it to contain %q", out, want)
			}
		})
	}
}

// TestAutoStartsOrchestratorAndRelays proves the shim starts a real
// orchestrator when nothing is listening yet, and successfully relays a
// round-trip MCP call through to it.
func TestAutoStartsOrchestratorAndRelays(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := testutil.ShortSocketPath(t, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	orch := orchestratorBinary(t)

	cmd, err := autoStart(orch, socketPath, testFlags(dbPath, recipeRoot, gooseBinary, runsDir))
	if err != nil {
		t.Fatalf("autoStart: %v", err)
	}
	t.Cleanup(func() { killDetached(cmd) })

	conn, err := dialWithBackoff(socketPath, 10*time.Second)
	if err != nil {
		t.Fatalf("dialWithBackoff: %v", err)
	}
	defer conn.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	ctx := context.Background()
	session, err := client.Connect(ctx, &mcp.IOTransport{Reader: conn, Writer: conn}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": filepath.Join(recipeRoot, "success.yaml"), "idempotency_key": "k1"}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	m, ok := res.StructuredContent.(map[string]any)
	if !ok || m["id"] == "" {
		t.Fatalf("spawn result = %+v, want a task id", res.StructuredContent)
	}
}

// TestSecondShimReusesRunningOrchestrator proves a second auto-start
// attempt against a socket that already has a live owner does not start a
// second orchestrator: it just reaches the existing one.
func TestSecondShimReusesRunningOrchestrator(t *testing.T) {
	gooseBinary, recipeRoot := newFakeGoose(t)
	dir := t.TempDir()
	socketPath := testutil.ShortSocketPath(t, "orch.sock")
	dbPath := filepath.Join(dir, "orchestrator.db")
	runsDir := filepath.Join(dir, "runs")
	orch := orchestratorBinary(t)

	cmd, err := autoStart(orch, socketPath, testFlags(dbPath, recipeRoot, gooseBinary, runsDir))
	if err != nil {
		t.Fatalf("first autoStart: %v", err)
	}
	t.Cleanup(func() { killDetached(cmd) })
	first, err := dialWithBackoff(socketPath, 10*time.Second)
	if err != nil {
		t.Fatalf("dial first orchestrator: %v", err)
	}
	defer first.Close()

	session1 := connectSession(t, first)
	spawnRes, err := session1.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{"recipe": filepath.Join(recipeRoot, "success.yaml"), "idempotency_key": "k1"}})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	id, _ := spawnRes.StructuredContent.(map[string]any)["id"].(string)
	if id == "" {
		t.Fatal("spawn returned empty task id")
	}

	// A second "shim" sees the socket already in use and just dials it
	// directly, exactly as run() does when dial succeeds on the first try.
	second, err := dial(socketPath)
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	defer second.Close()
	session2 := connectSession(t, second)

	statusRes, err := session2.CallTool(context.Background(), &mcp.CallToolParams{Name: "status", Arguments: map[string]any{"id": id}})
	if err != nil {
		t.Fatalf("status via second connection: %v", err)
	}
	status, _ := statusRes.StructuredContent.(map[string]any)["status"].(string)
	if status == "" || status == "unknown" {
		t.Fatalf("status via second connection = %q, want the real task's status (second shim started an independent orchestrator)", status)
	}
}

func connectSession(t *testing.T, conn net.Conn) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.IOTransport{Reader: conn, Writer: conn}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return session
}
