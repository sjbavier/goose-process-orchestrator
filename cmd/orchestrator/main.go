// Command orchestrator provides a deterministic lifecycle demo and a local,
// stdio-only MCP server. The MCP server lasts only for this process lifetime;
// it is not a durable daemon and exposes no remote endpoint.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/local/goose-process-orchestrator/internal/mcpserver"
	"github.com/local/goose-process-orchestrator/internal/orchestrator"
	"github.com/local/goose-process-orchestrator/internal/retention"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

func main() {
	demo := flag.Bool("demo", false, "run a deterministic in-memory lifecycle demonstration")
	mcpStdio := flag.Bool("mcp-stdio", false, "run the local stdio MCP server (not a durable daemon)")
	listen := flag.String("listen", "", "serve MCP over a local Unix domain socket at this path instead of stdio (persists across client disconnects; mutually exclusive with --mcp-stdio)")
	dbPath := flag.String("db", "orchestrator.db", "local SQLite task database for --mcp-stdio/--listen")
	recipeRoot := flag.String("recipe-root", "", "allowlisted recipe root directory (required for --mcp-stdio/--listen)")
	gooseBinary := flag.String("goose-binary", "goose", "goose executable used for --mcp-stdio/--listen")
	runsDir := flag.String("runs-dir", "runs", "local per-task artifact directory for --mcp-stdio/--listen")
	maxConcurrent := flag.Int("max-concurrent", 20, "maximum number of spawned recipes allowed to run at the same time, for --mcp-stdio/--listen")
	maxTurns := flag.Int("max-turns", 25, "default --max-turns passed to goose run for --mcp-stdio/--listen; a spawn's own max_turns input overrides this per task")
	maxTurnsCeiling := flag.Int("max-turns-ceiling", 200, "hard cap on a spawn's max_turns input for --mcp-stdio/--listen, regardless of what a client requests")
	runsMaxAge := flag.Duration("runs-max-age", 0, "delete a finished task's run directory under --runs-dir once this much time has passed since it ended; 0 (the default) keeps run directories forever")
	runsMaxCount := flag.Int("runs-max-count", 0, "target total number of run directories to keep under --runs-dir, newest first; 0 (the default) keeps run directories forever")
	runsSweepInterval := flag.Duration("runs-sweep-interval", 15*time.Minute, "how often to apply --runs-max-age/--runs-max-count; only used when one of them is set")
	mailboxExtensionBinary := flag.String("mailbox-extension-binary", "", "path to the mailbox-extension binary; empty (the default) leaves the mailbox feature entirely disabled, with spawned-recipe argv/env unchanged")
	mailboxSocket := flag.String("mailbox-socket", "mailbox.sock", "local Unix domain socket path the mailbox server listens on; only used when --mailbox-extension-binary is set")
	workdir := flag.String("workdir", "", "override the spawned goose run child's working directory; empty (the default) uses the task's own isolated run directory under --runs-dir, unchanged from today")
	outputLimit := flag.Int64("output-limit", artifacts.DefaultOutputLimit, "maximum bytes of a spawned recipe's stdout captured to its run directory's stdout.raw; must be positive (there is no unlimited setting). The live outcome summary is parsed from the full stream regardless of this cap")
	flag.Parse()
	if *mcpStdio && *listen != "" {
		fmt.Fprintln(os.Stderr, "--mcp-stdio and --listen are mutually exclusive")
		os.Exit(1)
	}
	if *maxConcurrent < 1 {
		fmt.Fprintln(os.Stderr, "--max-concurrent must be at least 1")
		os.Exit(1)
	}
	if *maxTurns < 1 {
		fmt.Fprintln(os.Stderr, "--max-turns must be at least 1")
		os.Exit(1)
	}
	if *maxTurnsCeiling < *maxTurns {
		fmt.Fprintln(os.Stderr, "--max-turns-ceiling must be at least --max-turns")
		os.Exit(1)
	}
	if *outputLimit < 1 {
		// Fail closed rather than treating 0/negative as "unlimited": an
		// unbounded capture would let one runaway recipe fill the disk.
		fmt.Fprintln(os.Stderr, "--output-limit must be at least 1; there is no unlimited setting")
		os.Exit(1)
	}
	if *runsMaxAge < 0 {
		fmt.Fprintln(os.Stderr, "--runs-max-age cannot be negative; use 0 to keep run directories forever")
		os.Exit(1)
	}
	if *runsMaxCount < 0 {
		fmt.Fprintln(os.Stderr, "--runs-max-count cannot be negative; use 0 to keep run directories forever")
		os.Exit(1)
	}
	if *mailboxExtensionBinary != "" {
		// Fail closed before any task is ever marked running, rather than
		// discovering a bad/missing extension binary at the first spawn.
		if info, err := os.Stat(*mailboxExtensionBinary); err != nil || !info.Mode().IsRegular() {
			fmt.Fprintf(os.Stderr, "--mailbox-extension-binary %q is not an accessible regular file: %v\n", *mailboxExtensionBinary, err)
			os.Exit(1)
		}
		if *mailboxSocket == "" {
			fmt.Fprintln(os.Stderr, "--mailbox-socket cannot be empty when --mailbox-extension-binary is set")
			os.Exit(1)
		}
	}
	if *workdir != "" {
		// Fail closed before any task is ever accepted, same as the
		// mailbox-extension-binary check above.
		if info, err := os.Stat(*workdir); err != nil || !info.IsDir() {
			fmt.Fprintf(os.Stderr, "--workdir %q is not an accessible directory: %v\n", *workdir, err)
			os.Exit(1)
		}
	}
	config := serverConfig{
		dbPath:                 *dbPath,
		recipeRoot:             *recipeRoot,
		gooseBinary:            *gooseBinary,
		runsDir:                *runsDir,
		maxConcurrent:          *maxConcurrent,
		maxTurns:               *maxTurns,
		maxTurnsCeiling:        *maxTurnsCeiling,
		retention:              retention.Policy{MaxAge: *runsMaxAge, MaxCount: *runsMaxCount},
		sweepInterval:          *runsSweepInterval,
		mailboxExtensionBinary: *mailboxExtensionBinary,
		mailboxSocket:          *mailboxSocket,
		workdir:                *workdir,
		outputLimit:            *outputLimit,
	}
	// The interval only matters once a limit is configured; requiring it to be
	// positive unconditionally would reject an invocation that never sweeps.
	if config.retention.Enabled() && config.sweepInterval <= 0 {
		fmt.Fprintln(os.Stderr, "--runs-sweep-interval must be positive when --runs-max-age or --runs-max-count is set")
		os.Exit(1)
	}
	if *mcpStdio {
		if *recipeRoot == "" {
			fmt.Fprintln(os.Stderr, "--recipe-root is required with --mcp-stdio: every spawned recipe must resolve under an explicit allowlisted directory")
			os.Exit(1)
		}
		if err := runMCP(config); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *listen != "" {
		if *recipeRoot == "" {
			fmt.Fprintln(os.Stderr, "--recipe-root is required with --listen: every spawned recipe must resolve under an explicit allowlisted directory")
			os.Exit(1)
		}
		err := runSocket(context.Background(), *listen, config)
		if errors.Is(err, errAlreadyOwned) {
			// Another orchestrator already owns this socket; exit 0 rather
			// than racing it, per the bind-or-dial single-instance pattern.
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if !*demo {
		fmt.Println("Goose Process Orchestrator scaffold ready. Run with --demo, --mcp-stdio, or see README.md.")
		return
	}
	now := time.Now().UTC()
	task := tasks.Task{ID: "demo-task", Recipe: "recipes/test-recipe.yaml", Status: tasks.Queued, CreatedAt: now}
	for _, status := range []tasks.Status{tasks.Starting, tasks.Running, tasks.Succeeded} {
		if err := task.Transition(status, now); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Printf("task_id=%s status=%s\n", task.ID, task.Status)
}

// serverConfig is one server instance's full configuration, as parsed from
// flags. It is a struct rather than a positional argument list because every
// field is independently optional-ish and several share a type, which makes
// positional calls easy to transpose silently.
type serverConfig struct {
	dbPath, recipeRoot, gooseBinary, runsDir string
	maxConcurrent, maxTurns, maxTurnsCeiling int
	// retention is the run-directory retention policy. Its zero value keeps
	// every run directory forever, which is the default and the behavior
	// this project had before retention existed.
	retention retention.Policy
	// sweepInterval is how often the retention policy is applied. It is only
	// read when retention is enabled.
	sweepInterval time.Duration
	// mailboxExtensionBinary and mailboxSocket enable the mailbox feature
	// when mailboxExtensionBinary is non-empty. Left empty (the default),
	// every spawned recipe's argv/env is exactly as before this feature
	// existed.
	mailboxExtensionBinary, mailboxSocket string
	// workdir, when non-empty, overrides every spawned goose run child's
	// working directory. Left empty (the default), a spawned process's cwd
	// stays its own isolated run directory, exactly as before this feature
	// existed.
	workdir string
	// outputLimit caps how many bytes of a spawned recipe's stdout are
	// captured to disk. It only bounds the on-disk capture: the live outcome
	// summary is parsed from the full stream, so raising or lowering this
	// never changes what a task reports. Zero is not a valid flag value
	// (main rejects it); a zero value here falls back to
	// artifacts.DefaultOutputLimit, matching artifacts.Manager's own
	// permissive behavior for direct library callers.
	outputLimit int64
}

// newServer constructs the service/scheduler/store exactly once and wraps it
// in an MCP server. Every accepted connection (stdio, or one per socket
// accept) runs against this same instance, so the in-memory process registry
// and task state survive any single connection closing.
//
// The returned startRetention starts the periodic run-directory sweep and
// blocks until its context is cancelled; it returns immediately when no
// retention limit is configured. Callers run it in a goroutine so both
// transport modes get the same sweeping behavior from one place.
// The returned startMailbox starts the mailbox server (accepting mailbox
// protocol connections from spawned agents) and blocks until its context is
// cancelled; it returns immediately, doing nothing, when the mailbox feature
// is disabled (config.mailboxExtensionBinary == ""). Callers run it in a
// goroutine, same as startRetention.
func newServer(config serverConfig) (server *mcp.Server, startRetention func(context.Context), startMailbox func(context.Context), closeDB func(), err error) {
	if _, err := os.Stat(config.recipeRoot); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("recipe root %q is not accessible: %w", config.recipeRoot, err)
	}
	// A busy writer (e.g. the scheduler's Transition) and a concurrent reader
	// (e.g. a second live connection's status poll) are now expected under
	// this feature's own design — multiple connections share one store.
	// _busy_timeout is a DSN-level setting (not a one-off db.Exec PRAGMA, which
	// would only apply to whichever pooled connection happened to run it) so
	// the driver waits out a brief lock on every connection it opens, rather
	// than surfacing SQLITE_BUSY to the caller; internal/tasks.Store's own
	// bounded retry remains the backstop if this is ever exceeded.
	db, err := sql.Open("sqlite", config.dbPath+"?_busy_timeout=5000")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// One schema authority: the same forward-only migration runner the tests
	// use, so a schema change can never reach tests without also upgrading a
	// real database on startup.
	if err := tasks.Migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, nil, nil, nil, fmt.Errorf("initialize local task storage: %w", err)
	}
	store := tasks.NewStore(db)
	launcherConfig := goose.Config{Binary: config.gooseBinary, RecipeRoot: config.recipeRoot, MaxTurns: config.maxTurns, MaxTurnsCeiling: config.maxTurnsCeiling, Workdir: config.workdir}
	// startMailbox stays a no-op unless the mailbox feature is enabled below,
	// so every existing caller/config keeps today's exact behavior.
	startMailbox = func(context.Context) {}
	var mailboxTokens *mailbox.Tokens
	if config.mailboxExtensionBinary != "" {
		mailboxTokens = mailbox.NewTokens()
		listener, err := bindOrDial(config.mailboxSocket)
		if err != nil {
			db.Close()
			return nil, nil, nil, nil, fmt.Errorf("bind mailbox socket %q: %w", config.mailboxSocket, err)
		}
		if err := os.Chmod(config.mailboxSocket, 0o600); err != nil {
			listener.Close()
			db.Close()
			return nil, nil, nil, nil, fmt.Errorf("restrict mailbox socket permissions: %w", err)
		}
		launcherConfig.ExtensionBinary = config.mailboxExtensionBinary
		launcherConfig.MailboxSocket = config.mailboxSocket
		startMailbox = func(ctx context.Context) {
			_ = mailbox.Serve(ctx, listener, mailboxTokens, store)
		}
	}
	launcher := goose.Runner{
		Config:    launcherConfig,
		Artifacts: artifacts.Manager{Root: config.runsDir, Limit: config.outputLimit},
	}
	runner := orchestrator.New(launcher, store)
	// Nil (the default, mailbox disabled) keeps Runner.Run's behavior exactly
	// as before this field existed.
	runner.Tokens = mailboxTokens
	// Reconcile every non-terminal row against real OS process state before
	// the scheduler is constructed: scheduler.New starts its worker
	// goroutines immediately, so running this after would let a freshly
	// claimed row race a reconciliation pass that hasn't happened yet.
	if err := orchestrator.Reconcile(context.Background(), store, runner); err != nil {
		db.Close()
		return nil, nil, nil, nil, fmt.Errorf("reconcile persisted process state: %w", err)
	}
	sched, err := scheduler.New(store, runner, config.maxConcurrent, config.maxConcurrent)
	if err != nil {
		db.Close()
		return nil, nil, nil, nil, err
	}
	// The launcher's own Config is the parameter validator, so admission and
	// the eventual argv are governed by one contract against one recipe root.
	service, err := scheduler.NewService(store, sched, func() string { return fmt.Sprintf("task-%d", time.Now().UTC().UnixNano()) }, config.runsDir, scheduler.WithParameterValidator(launcher.Config))
	if err != nil {
		db.Close()
		return nil, nil, nil, nil, err
	}
	server, err = mcpserver.New(service)
	if err != nil {
		db.Close()
		return nil, nil, nil, nil, err
	}
	// The sweeper reads the same store the scheduler writes, so a task's
	// status is always the authoritative answer to "is this run directory
	// still in use" — see internal/retention for the fail-safe-to-retain
	// rules it applies.
	sweeper := retention.Sweeper{Root: config.runsDir, Policy: config.retention, Lookup: store}
	start := func(ctx context.Context) {
		retention.Loop(ctx, sweeper, config.sweepInterval, logSweep)
	}
	return server, start, startMailbox, func() { db.Close() }, nil
}

// logSweep reports each sweep on stderr, which is where this command's other
// diagnostics go and is never part of the MCP protocol stream on stdout. A
// sweep that removed nothing stays silent so an idle orchestrator does not
// write a line every interval forever.
func logSweep(result retention.Result, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "retention sweep failed: %v\n", err)
		return
	}
	if result.Removed > 0 {
		fmt.Fprintf(os.Stderr, "retention sweep removed %d run director%s, kept %d\n", result.Removed, plural(result.Removed), result.Kept)
	}
	for _, skip := range result.Skips {
		// Only genuinely surprising retentions are worth a line; a directory
		// that is simply still within policy, or belongs to a task that is
		// still running, is the normal case.
		switch skip.Reason {
		case retention.SkipLookupFailed, retention.SkipRemoveFailed, retention.SkipUnsafePath:
			fmt.Fprintf(os.Stderr, "retention sweep kept %q: %s (%v)\n", skip.Name, skip.Reason, skip.Err)
		}
	}
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func runMCP(config serverConfig) error {
	server, startRetention, startMailbox, closeDB, err := newServer(config)
	if err != nil {
		return err
	}
	defer closeDB()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startRetention(ctx)
	go startMailbox(ctx)
	return server.Run(ctx, &mcp.StdioTransport{})
}

// errAlreadyOwned is returned by runSocket/bindOrDial when another process
// already owns socketPath. The caller must exit cleanly (status 0) rather
// than treat this as a failure: racing or duplicating the real listener
// would split the in-memory task/process registry across two processes.
var errAlreadyOwned = errors.New("another orchestrator already owns this socket")

// bindOrDial implements the single-instance pattern from plan.md: binding
// is attempted first; if the path is already in use, a short dial decides
// whether a live owner is actually listening (errAlreadyOwned) or the file
// is a stale leftover from a prior process that exited without unlinking it
// (removed and the bind retried exactly once). This makes concurrent starts
// against the same socketPath safe: whichever process's bind wins keeps it,
// every loser either defers to the winner or recovers a genuinely stale file.
// listenUnixRestricted binds a Unix domain socket with the process umask
// temporarily tightened to 0o077, so the socket file never exists even
// momentarily with group/other permissions looser than the 0o600 the caller
// narrows it to afterward — closing the bind-then-chmod window a looser
// inherited umask (e.g. 0) would otherwise leave open, since connect() is
// permission-checked by the kernel at connect() time, not at accept() time.
func listenUnixRestricted(socketPath string) (net.Listener, error) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)
	return net.Listen("unix", socketPath)
}

func bindOrDial(socketPath string) (net.Listener, error) {
	listener, err := listenUnixRestricted(socketPath)
	if err == nil {
		return listener, nil
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		return nil, fmt.Errorf("listen on %q: %w", socketPath, err)
	}
	if conn, dialErr := net.DialTimeout("unix", socketPath, 500*time.Millisecond); dialErr == nil {
		conn.Close()
		return nil, errAlreadyOwned
	}
	// Nothing answered: a stale socket file from a prior process that never
	// unlinked it (out-of-scope crash case). Remove it and bind exactly once more.
	if rmErr := os.Remove(socketPath); rmErr != nil && !os.IsNotExist(rmErr) {
		return nil, fmt.Errorf("remove stale socket %q: %w", socketPath, rmErr)
	}
	listener, err = listenUnixRestricted(socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on %q after removing stale socket: %w", socketPath, err)
	}
	return listener, nil
}

// runSocket binds a Unix domain socket at socketPath and serves one MCP
// session per accepted connection, all against the same server/service
// instance, until ctx is cancelled (production callers pass
// context.Background(), so in practice this runs until the process itself
// is terminated). A client disconnecting only ends its own goroutine, never
// the process or the shared task/process state.
func runSocket(ctx context.Context, socketPath string, config serverConfig) error {
	listener, err := bindOrDial(socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return fmt.Errorf("restrict socket permissions: %w", err)
	}
	server, startRetention, startMailbox, closeDB, err := newServer(config)
	if err != nil {
		return err
	}
	defer closeDB()
	// Retention and the mailbox server are the background work a long-lived
	// orchestrator owns, so both live for exactly as long as the listener does.
	go startRetention(ctx)
	go startMailbox(ctx)
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept on %q: %w", socketPath, err)
		}
		go func(conn net.Conn) {
			defer conn.Close()
			_ = server.Run(ctx, &mcp.IOTransport{Reader: conn, Writer: conn})
		}(conn)
	}
}
