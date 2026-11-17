// Command mcp-shim is what an MCP client launches as its stdio child. It
// relays raw bytes between its own stdin/stdout and a Unix domain socket
// served by cmd/orchestrator --listen, auto-starting that orchestrator
// (detached, new session) if nothing is listening yet. It never parses
// MCP/JSON-RPC itself: a client disconnecting (the shim's stdio closing)
// only ends the shim, never the orchestrator or the tasks it tracks.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func main() {
	socket := flag.String("socket", "", "Unix domain socket path the orchestrator listens on (required)")
	orchestratorBinary := flag.String("orchestrator-binary", "", "path to the cmd/orchestrator binary to auto-start if nothing is listening (required)")
	dbPath := flag.String("db", "orchestrator.db", "local SQLite task database, forwarded to an auto-started orchestrator")
	recipeRoot := flag.String("recipe-root", "", "allowlisted recipe root directory, forwarded to an auto-started orchestrator (required)")
	gooseBinary := flag.String("goose-binary", "goose", "goose executable, forwarded to an auto-started orchestrator")
	runsDir := flag.String("runs-dir", "runs", "local per-task artifact directory, forwarded to an auto-started orchestrator")
	maxConcurrent := flag.Int("max-concurrent", 20, "maximum number of spawned recipes allowed to run at the same time, forwarded to an auto-started orchestrator")
	maxTurns := flag.Int("max-turns", 25, "default --max-turns passed to goose run, forwarded to an auto-started orchestrator")
	runsMaxAge := flag.Duration("runs-max-age", 0, "run-directory retention age, forwarded to an auto-started orchestrator; 0 (the default) keeps run directories forever")
	runsMaxCount := flag.Int("runs-max-count", 0, "run-directory retention count, forwarded to an auto-started orchestrator; 0 (the default) keeps run directories forever")
	runsSweepInterval := flag.Duration("runs-sweep-interval", 15*time.Minute, "retention sweep interval, forwarded to an auto-started orchestrator; only used there when a retention limit is set")
	mailboxExtensionBinary := flag.String("mailbox-extension-binary", "", "path to the mailbox-extension binary, forwarded to an auto-started orchestrator; empty (the default) leaves the mailbox feature entirely disabled")
	mailboxSocket := flag.String("mailbox-socket", "mailbox.sock", "local Unix domain socket path the mailbox server listens on, forwarded to an auto-started orchestrator")
	maxTurnsCeiling := flag.Int("max-turns-ceiling", 200, "hard cap on a spawn's max_turns input, forwarded to an auto-started orchestrator")
	workdir := flag.String("workdir", "", "override the spawned goose run child's working directory, forwarded to an auto-started orchestrator; empty (the default) leaves it at the task's own isolated run directory")
	flag.Parse()

	if *socket == "" {
		fmt.Fprintln(os.Stderr, "--socket is required")
		os.Exit(1)
	}
	if *orchestratorBinary == "" {
		fmt.Fprintln(os.Stderr, "--orchestrator-binary is required")
		os.Exit(1)
	}
	if *recipeRoot == "" {
		fmt.Fprintln(os.Stderr, "--recipe-root is required")
		os.Exit(1)
	}

	forwarded := orchestratorFlags{
		dbPath:                 *dbPath,
		recipeRoot:             *recipeRoot,
		gooseBinary:            *gooseBinary,
		runsDir:                *runsDir,
		maxConcurrent:          *maxConcurrent,
		maxTurns:               *maxTurns,
		runsMaxAge:             *runsMaxAge,
		runsMaxCount:           *runsMaxCount,
		runsSweepInterval:      *runsSweepInterval,
		mailboxExtensionBinary: *mailboxExtensionBinary,
		mailboxSocket:          *mailboxSocket,
		maxTurnsCeiling:        *maxTurnsCeiling,
		workdir:                *workdir,
	}
	if err := run(*socket, *orchestratorBinary, forwarded); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// orchestratorFlags is everything this shim forwards to an orchestrator it
// auto-starts. It has no effect when the shim merely dials an orchestrator
// that is already running: that process's own values apply.
type orchestratorFlags struct {
	dbPath, recipeRoot, gooseBinary, runsDir string
	maxConcurrent, maxTurns                  int
	runsMaxAge, runsSweepInterval            time.Duration
	runsMaxCount                             int
	mailboxExtensionBinary, mailboxSocket    string
	maxTurnsCeiling                          int
	workdir                                  string
}

func run(socket, orchestratorBinary string, forwarded orchestratorFlags) error {
	conn, err := dial(socket)
	if err != nil {
		if _, err := autoStart(orchestratorBinary, socket, forwarded); err != nil {
			return fmt.Errorf("auto-start orchestrator: %w", err)
		}
		conn, err = dialWithBackoff(socket, 5*time.Second)
		if err != nil {
			return fmt.Errorf("dial %q after auto-start: %w", socket, err)
		}
	}
	defer conn.Close()

	relay(conn)
	return nil
}

func dial(socket string) (net.Conn, error) {
	return net.DialTimeout("unix", socket, 300*time.Millisecond)
}

// dialWithBackoff retries dialing while an auto-started orchestrator binds
// its listener, including the case where it loses the bind-or-dial race to
// a concurrently starting orchestrator and exits almost immediately: this
// shim simply keeps retrying the same socket path until whichever process
// actually owns it is accepting connections.
func dialWithBackoff(socket string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	delay := 20 * time.Millisecond
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := dial(socket)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(delay)
		if delay < 200*time.Millisecond {
			delay *= 2
		}
	}
	return nil, lastErr
}

// autoStart launches orchestratorBinary detached from this shim: a new
// session (no controlling terminal, no shared process group) and stdio
// redirected away from the shim's own, so the orchestrator's lifetime is
// never tied to this shim's lifetime, exactly the property this feature
// exists to deliver. The caller does not wait for or otherwise track the
// returned *exec.Cmd in production (run discards it); it is returned only
// so tests can observe/clean up the spawned process directly.
// cmd/orchestrator's own bind-or-dial safety (C02) makes it harmless if a
// concurrently starting shim's orchestrator wins the race instead.
func autoStart(orchestratorBinary, socket string, forwarded orchestratorFlags) (*exec.Cmd, error) {
	cmd := exec.Command(orchestratorBinary, append([]string{"--listen", socket}, forwarded.args()...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd, cmd.Start()
}

// args renders the forwarded flags as orchestrator argv. Retention flags are
// passed unconditionally, including their zero values, so the auto-started
// orchestrator's policy is exactly what this shim was told — never a default
// that silently differs from it.
func (f orchestratorFlags) args() []string {
	return []string{
		"--db", f.dbPath,
		"--recipe-root", f.recipeRoot,
		"--goose-binary", f.gooseBinary,
		"--runs-dir", f.runsDir,
		"--max-concurrent", strconv.Itoa(f.maxConcurrent),
		"--max-turns", strconv.Itoa(f.maxTurns),
		"--runs-max-age", f.runsMaxAge.String(),
		"--runs-max-count", strconv.Itoa(f.runsMaxCount),
		"--runs-sweep-interval", f.runsSweepInterval.String(),
		"--mailbox-extension-binary", f.mailboxExtensionBinary,
		"--mailbox-socket", f.mailboxSocket,
		"--max-turns-ceiling", strconv.Itoa(f.maxTurnsCeiling),
		"--workdir", f.workdir,
	}
}

// relay copies raw bytes bidirectionally between stdin/stdout and conn,
// with no JSON-RPC parsing, until either side closes.
func relay(conn net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(os.Stdout, conn)
		done <- struct{}{}
	}()
	<-done
}
