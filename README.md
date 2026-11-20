# Goose Process Orchestrator

A local MCP task server with SQLite-backed task records, bounded scheduling, and explicit cancellation. Two transport modes are available: a stdio-only mode (`--mcp-stdio`) whose server exists only while its owning foreground process is alive, and a persistent Unix-socket mode (`--listen`, normally reached through `cmd/mcp-shim`, see below) whose task state and live process registry survive any one client disconnecting and reconnecting. Neither mode is a network listener, an authenticated remote transport, or a crash/reboot-surviving daemon.

## Current scope

The server exposes only the typed `server_info`, `spawn`, `status`, `result`, `wait`, `cancel`, `list`, `logs`, `progress`, and `send_message` tools, in both transport modes. `spawn` starts an actual owned OS process via `internal/orchestrator`'s adapter against an allowlisted `goose run` recipe, and `cancel` reaches that process's owned group, not just its database row. `list` is read-only: it returns tasks newest first, with an optional `status` filter and a `limit` capped at a server-side maximum, so a client that lost a task ID (chat compaction, a new session, a different client) can rediscover it without direct SQLite access. `send_message` queues an instruction for a running task's inbox, delivered at its next checkpoint — see "Task mailbox" below.

`logs` and `progress` are also read-only, and work while a task is still running, not only after it finishes: `logs` returns a bounded tail of the task's captured `stdout.raw`/`stderr.raw`, and `progress` defensively parses that same stdout stream (`goose run --output-format stream-json`) into a message-event count, the last assistant text seen, provider/model, and tokens/cost once a `complete` event has been seen. Both locate the task's run directory by its on-disk naming convention (`<runs-dir>/<task-id>-<random>/`) rather than any stored path, so no schema change was needed. See "Using tasks from an MCP client" below for the message-event-count caveat and the secret-leakage warning.

In **stdio mode**, closing the client ends the server process entirely; the task set does not survive that. In **socket mode** (`--listen`, via `cmd/mcp-shim`), closing one client connection only ends that connection — the orchestrator process, its task records, and its live process registry keep running, and a new client reaching the same socket sees the same tasks and can still cancel them. This is the one capability socket mode adds over stdio mode; everything else about the task model is identical.

This project does not establish a durable background service with crash/reboot recovery, authenticated remote transport, or multi-operator shared-workspace behavior. Those remain explicitly out of scope — see "Out of scope" below. Run-directory retention (age/count limits under `--runs-dir`) is in scope and off by default; see "Run-directory retention" below.

## Requirements

- Go 1.27.2 (verified locally; `go.mod` declares Go 1.27)
- Goose CLI 1.53.0 is available locally; it is not required for `--demo` but is required for `--mcp-stdio` to actually run a spawned recipe (the fake-binary tests under `internal/orchestrator` substitute a local stand-in and never invoke it).

## Run and verify

```sh
go test ./...
go run ./cmd/orchestrator --demo
```

Expected demo output includes `task_id=demo-task status=succeeded`.

## Tooling policy

Use the standard Go toolchain: `gofmt -w`, `go vet ./...`, `go test ./... -race`, and `go build ./...`. Third-party dependencies are limited to the official MCP Go SDK, the pure-Go `modernc.org/sqlite` driver, and a YAML parser (see [`go.mod`](go.mod)); adding any further dependency requires explicit approval.

## Safety boundaries

- Never turn an MCP disconnect or timeout into task cancellation.
- Use recipe/workspace allowlists and argument arrays, never shell interpolation.
- Treat `--recipe-root` as an operator-controlled trust boundary: a recipe is allowed only when its canonical resolved path remains inside that root. Absolute paths outside it and in-root symlinks resolving outside it are rejected. A relative `--recipe-root` or `spawn` recipe path is resolved to an absolute path before the check, so it still resolves correctly once `goose run` is launched in the task's own run directory, not this process's.
- `cancel` only signals a process group after verifying the target PID is still its original group leader, on both macOS and Linux, so an already-exited and kernel-recycled PID is never signaled.
- The `--listen` Unix socket is bound with the process umask temporarily tightened to `0o077`, so the socket file never exists — even momentarily, under a loose inherited umask — with group/other permissions looser than the `0o600` it is explicitly chmod'd to right after.
- Keep credentials out of logs and source control.
- Treat a future daemon as the lifecycle owner; a stdio child process is not a durability guarantee.
- Shared workspace changes require explicit isolation/coordination, such as Git worktrees.

See [`docs/project-brief.md`](docs/project-brief.md) and the repository-local workflow intake for the full provenance and next routing decision.

## Operator guide

### Scope and supported hosts

This is for **macOS and Linux** with the Go version declared in
[`go.mod`](go.mod). It is local-process software: no remote listener, HTTP
endpoint, authenticated transport, reboot recovery, or global session absence
is provided or claimed in either transport mode. Closing an MCP client is
never itself a request to cancel an accepted task; cancellation is always
explicit through the MCP `cancel` tool, in both modes.

The binary's supported entry points are intentionally small:

```sh
# Deterministic, no-Goose lifecycle demonstration.
go run ./cmd/orchestrator --demo

# Stdio MCP server with a caller-selected SQLite path, allowlisted recipe
# root, and goose binary. This command owns the foreground process; closing
# its client ends it. --recipe-root is required: every spawned recipe must
# resolve under it.
go run ./cmd/orchestrator --mcp-stdio --db ./orchestrator.db \
  --recipe-root ./recipes --goose-binary goose --runs-dir ./runs

# Persistent Unix-socket MCP server: the same task/process model, but
# multiple client connections — sequential or concurrent — share one
# running orchestrator and its task registry. See "Persistent mode" below
# for how a client actually reaches this.
go run ./cmd/orchestrator --listen /path/to/orchestrator.sock \
  --db ./orchestrator.db --recipe-root ./recipes --goose-binary goose --runs-dir ./runs \
  --max-concurrent 20 --max-turns 25 \
  --runs-max-age 168h --runs-max-count 200 --runs-sweep-interval 15m

# Same server with the optional task mailbox enabled (see "Task mailbox"
# below). Build the extension binary first:
#   go build -o mailbox-extension ./cmd/mailbox-extension
go run ./cmd/orchestrator --listen /path/to/orchestrator.sock \
  --db ./orchestrator.db --recipe-root ./recipes --goose-binary goose --runs-dir ./runs \
  --mailbox-extension-binary ./mailbox-extension --mailbox-socket /path/to/mailbox.sock
```

`--max-turns-ceiling` (default `200`) is a hard cap on any single `spawn`'s `max_turns` input, regardless of what a client requests.

`--output-limit` (default `8388608`, 8 MiB) caps how many bytes of a spawned recipe's stdout are captured to its run directory's `stdout.raw`. Output past the cap is still read and counted but not stored, and `progress` reports `stdout_truncated`/`bytes_dropped` when that happened. The cap does **not** affect what a task reports: `final_text`, `outcome`, the token/cost totals, and `tool_call_count` are parsed from the full live stream as it arrives, so they stay correct for a run whose output far exceeds the cap. Must be positive — there is deliberately no "unlimited" setting, since an unbounded capture lets one runaway recipe fill the disk — and a non-positive value fails at startup before any task is accepted, the same way `--workdir` and `--mailbox-extension-binary` do.

`--workdir` (default empty) overrides every spawned `goose run` child's working directory, which otherwise defaults to the task's own isolated run directory under `--runs-dir`. This matters for a recipe authored to assume a real project checkout as its cwd (relative paths like `./src/...` or `.config/...`); set `--workdir /path/to/that/checkout` so every spawn runs there instead. Only the child's cwd changes — `logs`/`progress`/retention still use the task's own run directory for captured `stdout`/`stderr`, since those paths are already absolute. Validated at startup (must be an accessible directory) the same way `--mailbox-extension-binary` is, so a bad path fails before any task is ever accepted. Leaving it empty keeps today's exact behavior.

Use an absolute or otherwise operator-controlled `--db` path when the caller
needs to retain local task records. The database is local state, not a shared
service: do not point concurrent unrelated operators at the same file without
coordination. Both `--mcp-stdio` and `--listen` **do** launch real `goose run`
processes for every accepted `spawn`, scoped to `--recipe-root` and run under
`--runs-dir`.

`--max-concurrent` (default `20`) bounds how many spawned recipes may run at
the same time. A `spawn` while every slot is in active use (or very briefly
backlogged waiting for one) returns `"task queue is at capacity"` rather than
silently queuing indefinitely; retry it once a running task finishes. Raise
or lower this to match how many recipes you actually want running in
parallel — concurrent runs are isolated from each other (separate run
directories, separate OS process groups), so there is no correctness reason
to keep it low, only a cost/resource one.

#### Recipe-root containment and residual race

Recipe validation resolves both the configured root and candidate path before
checking containment. It rejects an absolute path outside `--recipe-root` and
an in-root symlink that resolves outside it; this is the closure evidence for
audit finding #1. The root remains operator-controlled. This check is not an
atomic pathname-to-execution guarantee: a small check-to-exec race remains if
an actor able to write that trusted root changes it after validation. Finding
#5 accepts that residual risk for the unchanged threat model. Validation occurs
when a worker starts a claimed task immediately before launch, rather than at
MCP acceptance, so `--max-concurrent` does not create the proposed queued
validation interval. Descriptor-based/no-follow execution is intentionally
out of scope; if the root becomes shared, untrusted, or writable by lower-trust
actors, reopen this as a separate security design decision.

`--max-turns` (default `25`) sets the server-wide `goose run --max-turns`
value applied to every spawn. A `spawn` may send its own `max_turns` in its
MCP input to override this default for that one task only (every other task,
running or future, keeps using the server default). This value does not
persist as a general server setting change — it is per-task, recorded with
that task's own record.

#### Recipe parameters

A `spawn` may send an optional `parameters` object to supply the recipe's own
declared string parameters. Each entry becomes one `--params key=value` pair in
the child process's argument array, sorted by key, so a value containing `=`,
spaces, or quotes is passed through byte for byte and is never shell-joined.

For larger or multi-line text, `spawn` also accepts `file_parameters: {key:
content}`. Each key satisfies the same declared `input_type: string` contract,
but GPO stores its exact UTF-8 content, creates a private
`<run-dir>/inputs/<key>` file (directory mode 0700, file mode 0600), and passes
only that file's absolute path as the ordinary `--params key=path` value. Raw
content never enters the child argv or any MCP response. A recipe reads the
path using its normal file tools.

> **Never pass a secret in either parameter form.** Scalar values and file
> contents are persisted unencrypted in the local task database as durable
> audit data; file contents are additionally materialized under `--runs-dir`
> and may be sent to Goose/model/provider when the recipe reads them. There is
> no secret detection, encryption, or redaction. Runs retention removes a
> terminal run directory as a unit, including `inputs/`, but does not erase
> the task row or its SQLite audit content.

Parameters are validated before the server reserves capacity or creates a task
row, so an invalid request starts no process and leaves nothing behind:

| Rule | Behavior |
|---|---|
| Keys must be declared by that recipe | an undeclared key is rejected, and the error lists the declared keys |
| Every `requirement: required` declaration must be supplied | a missing one is rejected; an empty string counts as supplied, matching `goose run`'s own observed behavior |
| Only `input_type: string` parameters can be supplied | a value for a number/boolean/other declared type is rejected |
| At most 32 parameters | over that is rejected |
| Keys match `[A-Za-z][A-Za-z0-9_-]{0,63}` | ASCII identifiers only, so a key can never shift its own `key=value` boundary |
| Values are at most 4 KiB of valid UTF-8 | larger or malformed input is rejected |
| No control characters or line terminators in a value | rejected, because Goose 1.53 silently normalizes a newline in a value to a space and so cannot carry one faithfully |

Two deliberate limits worth knowing:

- **Parameters never appear in a response.** `status`, `result`, and `list`
  omit them by design; a caller already holds the values it sent, and
  responses are the widest-read surface here.
- **Every spawn validates that the recipe has a non-empty top-level `prompt`.**
  Prompt-less recipes are rejected before capacity is reserved or an
  idempotency key is consumed, because headless `goose run --recipe` cannot
  supply the missing prompt separately. Parameter declarations are also
  checked at this admission boundary, including required keys.

A task's parameters are part of its durable identity: replaying one
`idempotency_key` with a different parameter map is a conflict rather than a
silent reuse of a task that would run different inputs. Key order never
matters, because the stored form is canonical.

`list` includes each task's caller-chosen `idempotency_key` and accepts an
optional `key_prefix` filter. Use a stable logical prefix (for example
`ENG-29341:`) to rediscover task IDs after compaction; the prefix is matched
literally and can be combined with the existing status filter.

`server_info` reports the configured recipe root, workdir, runs directory,
Goose binary and its startup-cached `--version` output, turn/concurrency
limits, current queued/running counts, mailbox and output-limit settings,
retention policy, daemon start time, and GPO build version/commit when the Go
build metadata provides them. A failed Goose version probe leaves only
`goose_version` empty; it does not prevent server startup.

`--sub-recipe` remains unsupported.

`--max-turns-ceiling` (default `200`) hard-caps the effective `max_turns` for
every spawn, including a client-supplied override: a request for
`max_turns: 2000000000` is silently clamped to the ceiling, not forwarded
verbatim to `goose run`. This bounds the cost/duration a single MCP client
can impose regardless of what it asks for; raise it only if `200` turns is
genuinely too low for a legitimate recipe.

#### Run-directory retention

`--runs-max-age` (default `0`) and `--runs-max-count` (default `0`) are both
off by default, which keeps every run directory under `--runs-dir` forever —
the same behavior this project had before retention existed. Setting either
one enables a periodic sweep, run every `--runs-sweep-interval` (default
`15m`; it must be positive whenever a limit is set), that removes run
directories to satisfy the configured limit(s):

- `--runs-max-age` removes a run directory once this much time has passed
  since its task reached a terminal status (`succeeded`, `failed`,
  `timed_out`, or `cancelled`).
- `--runs-max-count` keeps only the newest N terminal-task run directories,
  removing older ones beyond that count. **Limitation:** an orphan run
  directory (no matching task row — for example one left over from a
  database that was reset) ages out under `--runs-max-age` but is never
  removed by `--runs-max-count` alone, since there is no task to rank it
  against; set `--runs-max-age` as well if orphan cleanup matters.
- A task that is not yet terminal (`queued`, `starting`, `running`, or whose
  state could not be established — `interrupted`/`unknown`) is never swept by
  either limit, regardless of age or count, and a lookup failure fails safe
  by retaining the directory rather than removing it.

The sweep only ever removes a directory strictly inside `--runs-dir`; it
never follows a symlink to a target outside it, and never touches the root
itself. `cmd/mcp-shim` forwards all three flags unconditionally (including
their zero values) to the orchestrator it auto-starts, so that process's
policy is always exactly what the shim was configured with, never a
different default.

#### Task mailbox

Spawned tasks are one-shot `goose run --no-session` processes with no inbound
channel by default. The mailbox feature gives a controller a way to queue a
running task a further instruction, and gives spawned agents a bounded way to
exchange messages with each other, without native Goose session resume (see
"Goose session mode" below, a separate and unrelated decision) or mid-turn
interruption — `cancel` remains the only hard stop; nothing here interrupts a
task mid-turn.

The feature is off by default and changes nothing about a task that doesn't
use it: it is enabled by setting both `--mailbox-extension-binary` (path to
the `cmd/mailbox-extension` binary, built like any other binary in this repo)
and `--mailbox-socket` (a local Unix domain socket path the mailbox server
listens on, bound and `chmod 0600` the same way `--listen` is). Leaving
`--mailbox-extension-binary` empty — the default — leaves every spawned
recipe's argv and environment exactly as they were before this feature
existed; no existing tool's request or response shape changes either way.

When enabled:

- Every `spawn` additionally attaches `--with-extension "mailbox:<binary>
  --socket <socket>"` to the underlying `goose run` invocation and sets
  `MAILBOX_TOKEN` in that process's environment to a token minted for that
  task alone. The token never appears in `status`/`result`/`list`, mirroring
  how recipe parameters are already omitted from those responses.
- The controller's own `send_message` tool (`id`, `body`, optional `kind`)
  queues a message for the named task's inbox; the connection is always
  privileged and the stored sender is always the literal `controller` —
  there is no sender argument to override.
- A spawned agent reaches the same mailbox over its own authenticated
  connection (via `cmd/mailbox-extension`, exposing `read_inbox` and
  `send_message(to, kind, body)` as ordinary tools once a recipe calls them)
  using only its own per-task token. The orchestrator derives that
  connection's sender from the token itself, never from a caller-supplied
  task ID — a task can read only its own inbox, and can never forge a
  different sender when messaging another task by ID.
- **Addressing is task-id based and hub-and-spoke.** A `send_message` call's
  `to` is another task's ID (or, for the controller's own tool, implicitly
  the controller); there is no direct agent-to-agent transport — every
  message still routes through the orchestrator.
- **Delivery is checkpoint-only and pull-based, not mid-turn.** A recipe must
  itself call `read_inbox` at a point it chooses (a checkpoint) to see what has
  queued since the last check; nothing pushes a message into an already-running
  turn. The read and delivered mark commit in one database transaction, and
  each message is returned at most once across successful calls from the same
  task's token, oldest first. This is not an acknowledgement/retry protocol: if
  the agent receives a response but fails before acting on it, GPO does not
  redeliver that message.
- **Every task's inbox defaults to `controller_only`**: a message whose
  sender is another task's identity is rejected, before any storage, unless
  that recipient's `inbox_policy` has been set to `open`. Pass
  `inbox_policy: "open"` to `spawn` to allow agent-to-agent messaging for
  that task; there is no tool to change it after spawn, so a task already
  running at `controller_only` stays that way for its lifetime.
- **Never put secrets in a message body.** Like recipe parameters, message
  bodies are durable local audit data persisted in the task database and
  become part of a recipient's context once delivered; there is no
  redaction.
- **Messages have no independent retention.** `task_messages` rows are not
  swept by "Run-directory retention" above — that sweep only ever touches
  directories under `--runs-dir`, never a database row — and no task row is
  ever deleted by anything in this project today, so in practice a task's
  messages persist for as long as its task row does (indefinitely, same as
  the row itself). If a future task-row reclamation mechanism is ever added,
  deleting a task's row should take its messages with it (the schema's
  `task_id` foreign key supports that); until then there is deliberately no
  separate message-cleanup path to keep in sync with one.

See [`docs/goose-extension-setup.md`](docs/goose-extension-setup.md) for the
exact extension-config change this adds on top of the base setup.

#### Goose session mode (evaluated, not implemented)

Every spawn runs with `--no-session`, unconditionally: no Goose session is
created or used. This was evaluated and is a deliberate, final decision,
not a placeholder — native Goose session resume is **not implemented**, and
there is no plan to wire a `--name`/`--resume`/`--session-id` option into
`spawn`. The default stays `--no-session`.

This was decided after weighing three candidate shapes (caller-named new
session, resumable session by id, or neither) against the facts below,
which remain the rationale for not building native resume rather than a
future roadmap item:

- **Session data does not live in a run directory.** Goose keeps its own
  sessions in a single shared store — a SQLite database at
  `~/.local/share/goose/sessions/sessions.db` on the host running `goose` —
  not as per-session files, and not anywhere under this orchestrator's
  `--runs-dir`.
- **Run-directory retention cannot reach it.** The sweep described above
  (`--runs-max-age`/`--runs-max-count`) only ever removes directories
  strictly inside `--runs-dir`. It has no knowledge of, and no access to,
  Goose's session database, so native session mode would not have changed
  what retention cleans up, and retention could never answer "was the
  session data cleaned up."
- **Resuming a session has a real token cost.** Goose's own `--resume` flag
  replays the full prior session's context back into the model before
  continuing it. That is the opposite of a token-saving shortcut — a long or
  frequently resumed session can cost materially more than starting fresh.
- **A shared, unscoped session store doesn't fit this orchestrator's
  isolation model.** Every other part of this project is one task = one
  owned process = one owned run directory. A Goose session is global state
  outside that model — visible to every `goose` invocation on the host,
  including a human running `goose session` directly — and adding native
  resume would have been the first feature to depend on that shared state.

**The supported way to chain context across spawns** is the existing
["Recipe parameters"](#recipe-parameters) handoff-file pattern: have one
recipe write a checkpoint file, and have a later recipe read it back via a
parameter. This sidesteps the shared-session-store and token-replay
tradeoffs above entirely and is the supported answer today — not a
stopgap pending a future native-resume feature.

### Guidance, checkpoints, and continuing incomplete work

GPO does not resume a completed Goose process or inject a controller message
mid-turn. The supported guidance path for a **live** task is cooperative:
enable the mailbox extension, author the recipe to call `read_inbox` after its
initial instructions and at bounded step boundaries, and send guidance before
the task exits. Checking only once at startup is useful for a message queued
immediately after `spawn`, but it is not enough for later course correction; a
long recipe should check between major phases and before its final reporting
step. Mailbox reads consume turns/tool calls, so checkpoints should be
deliberate rather than inside a tight loop.

A task that already reached `succeeded`, `failed`, `timed_out`, `cancelled`,
a terminal state (`succeeded`, `failed`, `timed_out`, or `cancelled`) has no
live Goose process that can consume a later message. `interrupted` and
`unknown` are wake-up states rather than terminal states, but GPO likewise has
no confirmed live process to which it can deliver guidance. To continue that work today, spawn a **new task** with the relevant
state/guidance supplied through a recipe-declared scalar or file parameter
(for example `resume_notes_path`), and have the recipe load that input near the
start. Use a new idempotency key: changing parameters or file content under an
existing key is intentionally an idempotency conflict. For predictable work,
set an adequate per-spawn `max_turns` up front (within `max_turns_ceiling`) and
combine it with checkpoints; neither setting creates native process/session
resume.

This is the presumed path forward instead of hidden prompt rewriting or a
generic ad-hoc instruction runner: recipes keep an explicit, auditable input
contract, and live guidance remains opt-in and checkpoint-driven.

### Persistent mode via `cmd/mcp-shim`

**Setting this up as a Goose extension?** See
[`docs/goose-extension-setup.md`](docs/goose-extension-setup.md) for a
step-by-step walkthrough (build, config.yaml snippet, verification,
troubleshooting) — including for Goose itself to follow autonomously.

`--listen` by itself only starts the socket server; it does not auto-start
itself, and no MCP client launches a bare `--listen` process as its stdio
child (a socket path is not stdio). `cmd/mcp-shim` is the small binary a
client actually launches instead of `cmd/orchestrator --mcp-stdio`: it relays
raw bytes between its own stdin/stdout and the socket, with no JSON-RPC
parsing of its own, and auto-starts `cmd/orchestrator --listen` (detached,
in a new session, stdio redirected away from the shim's own) the first time
nothing answers at the given socket path.

Point an MCP client (Goose Desktop, Claude Code, or any other MCP-capable
client) at the shim instead of the orchestrator directly:

```sh
go build -o mcp-shim ./cmd/mcp-shim
go build -o orchestrator ./cmd/orchestrator

./mcp-shim --socket /path/to/orchestrator.sock \
  --orchestrator-binary /path/to/orchestrator \
  --recipe-root ./recipes --goose-binary goose \
  --db ./orchestrator.db --runs-dir ./runs --max-concurrent 20 --max-turns 25 \
  --runs-max-age 168h --runs-max-count 200 --runs-sweep-interval 15m
```

`--max-concurrent`/`--max-turns`/`--runs-max-age`/`--runs-max-count`/`--runs-sweep-interval` here are only forwarded to the orchestrator the shim auto-starts; they have no effect when the shim just dials an already-running orchestrator (that orchestrator's own values, from whichever invocation actually started it, apply).

The shim also forwards `--mailbox-extension-binary`, `--mailbox-socket`, `--max-turns-ceiling`, `--workdir`, and `--output-limit` to any orchestrator it auto-starts, unconditionally, with defaults (`""`, `"mailbox.sock"`, `200`, `""`, `8388608`) matching `cmd/orchestrator`'s own — so an orchestrator the shim auto-starts behaves the same as one started directly with no flags. Pass the shim its own `--mailbox-extension-binary`/`--mailbox-socket`/`--max-turns-ceiling`/`--workdir`/`--output-limit` flags to change what it forwards. The shim rejects a non-positive `--output-limit` itself rather than forwarding it, so the failure reads as the actual reason instead of a dial timeout against an orchestrator that exited immediately.

The client's actual stdio child is `mcp-shim`, not `orchestrator`. The first
client to connect through the shim causes an orchestrator to auto-start; every
later client (another shim invocation, including a second one launched
concurrently while racing to start it) reaches that same orchestrator rather
than starting an independent one — `cmd/orchestrator`'s own bind-or-dial logic
makes a concurrent-start race safe, and a stale leftover socket file from a
prior crash is detected and replaced automatically.

**What this buys you:** closing one client (Goose Desktop exiting, a terminal
closing, `mcp-shim`'s stdio pipe closing for any reason) only ends that one
shim process and connection. The orchestrator process, its SQLite task
records, and its live in-memory process registry are unaffected — a new
client reaching the same socket sees the real current status of every task,
including one that is still running, and a `cancel` sent through that new
connection reaches the real owned OS process group, not a stale or "unknown"
record. Two clients connected to the same socket at the same time see and can
act on the same task set.

**What this does not buy you:** if the orchestrator process itself is killed
or crashes and a new orchestrator process is started against the same `--db`
file within the same boot session, every non-terminal task row is
reconciled on that new process's startup against the real OS process state,
verified by persisted pid+pgid+start-time identity: a task whose process
survived is re-adopted (still cancellable, and its eventual real exit is
still observed) and a task whose process is verifiably gone is moved to a
terminal state with an explicit reason; an ambiguous case (identity cannot be
positively verified) is left `unknown` and its pid is never signaled. This is
same-boot-session crash/restart recovery, not reboot survival — a reboot
kills every child outright, and there is still no systemd/launchd unit. See
"Out of scope" below for exactly what remains unimplemented. Run-directory
retention (above) is available, but only while the orchestrator process
itself is running — it is not a crash/reboot-recovery mechanism either.

### Using tasks from an MCP client

Whichever transport mode is in use, call only the server's typed task tools:
`server_info`, `spawn`, `status`, `result`, `wait`, `cancel`, `list`, `logs`,
`progress`, and `send_message`. Call `server_info` once when a client needs the
server's actual paths, limits, cached Goose version, queue counts, retention
settings, or build identity rather than relying on copied configuration.
`wait` accepts 1–32 unique task IDs in caller order, `mode`
(`all` by default or `any`), and integer `timeout_seconds` (240 by default,
maximum 280). It wakes on terminal, `interrupted`, or `unknown` status and
returns all requested tasks in caller order with the same enriched fields as
`status`/`result` plus `timed_out`. Any missing ID fails the whole call. A
cancelled/disconnected wait request stops only that read; it never cancels or
owns the tasks. A normal operator flow is:

1. Request `spawn` with an allowlisted recipe and, if the recipe declares
   inputs, a `parameters` and/or `file_parameters` object (no secrets); record
   the returned task ID.
2. Use `wait` with that ID (or up to 32 IDs for fan-out), then call `result`.
   `status` remains the non-blocking snapshot when a caller does not want to
   wait. All three can be used after reconnecting a client (in persistent
   mode, from a different connection entirely). `status` and `result` both
   return `id`, `status`, `result`, and `cancel_requested`, plus —
   for a task this server actually ran — `run_dir`, `final_text` (the last
   assistant message, with `final_text_truncated` when it was bounded),
   `outcome`, `exit_code`, `failure`, `created_at`/`started_at`/`ended_at`,
   and capture accounting (`stdout_truncated`/`bytes_dropped`), plus, for a
   task whose process failed, a bounded `stderr_tail` (4 KiB,
   with `stderr_tail_truncated`). `result` keeps its existing meaning — the
   run-directory path, not the model's answer; read `final_text` for that.
   `outcome` is the server's own classification of how a terminal task
   ended:

   | `outcome` | Meaning |
   |---|---|
   | `completed` | the stream's own `complete` event was seen — the only value that affirmatively confirms a finished run |
   | `process_failed` | the process ran and exited non-zero; see `exit_code` and `stderr_tail` |
   | `no_complete_event` | the task reached a terminal status but no `complete` event was ever observed, so success cannot be confirmed |
   | `stalled_max_turns` | the run stopped by hitting its turn limit, detected from Goose's own stop message — see the note below |
   | absent | a non-terminal or cancelled task (cancellation is already fully explained by `status` + `cancel_requested`) |

   A task that this server never saw run live — one re-adopted after an
   orchestrator restart, or created before migration `005` — reports these
   fields as absent and `outcome` as `no_complete_event`. That is deliberate:
   the server reports `completed` only when it observed the evidence for it,
   and never guesses it from a `succeeded` status alone.

   **Note:** `stalled_max_turns` is detected by matching Goose's own
   max-turns stop message (confirmed present in goose 1.43.0, 1.52.0
   Desktop, and 1.53.0) against the final assistant text, requiring that the
   text not have arrived with `metadata.inference` — so a model reply that
   merely quotes the same phrase cannot false-positive. Because Goose emits
   its own `complete` event after that stop message, this check is
   evaluated ahead of `completed` in the outcome priority order, so a real
   stall is never misreported as `completed`.

   **Warning:** `final_text` and `stderr_tail` are unfiltered model/process
   output, exactly like `logs` and `progress` — they can contain anything the
   recipe or model wrote, including secrets if the recipe was authored
   carelessly. Nothing here redacts them.
3. Send `cancel` only when cancellation is intended. A disconnected client does
   not cancel a task.
4. If a task ID was lost (chat compaction, a new session, a different
   client), call `list` instead of reaching for `orchestrator.db` directly.
   With no arguments it returns up to the server's default number of tasks,
   newest first; pass `status` (one of `queued`, `starting`, `running`,
   `succeeded`, `failed`, `timed_out`, `cancelled`, `interrupted`, `unknown`)
   to narrow it, and `limit` to bound how many rows come back — `limit` is
   always capped at the server-side maximum regardless of what is requested.
   Each entry has `id`, caller-chosen `idempotency_key`, `recipe`, `status`,
   `created_at`, `updated_at`, and `cancel_requested`; pass literal
   `key_prefix` to rediscover one logical workflow's tasks, optionally together
   with `status`. The returned `id` can be used immediately with `status`,
   `result`, `wait`, or `cancel`.
5. While a task is `running` (or after it finishes, until its run directory
   is removed — by default nothing in this project deletes one; see
   "Run-directory retention" above for the opt-in sweep), call `logs` with
   that task ID to read a bounded tail of its captured output:
   `{"id": ..., "stream": "stdout"|"stderr"|"both" (default both), "max_bytes": ...}`.
   `max_bytes` defaults to 8 KiB per stream and is capped at 64 KiB per
   stream regardless of what is requested; a response also reports whether
   each stream was truncated. **Warning:** this is raw captured output —
   model text and stderr can contain anything the recipe or model wrote,
   including secrets if the recipe was authored carelessly. Neither this
   tool nor `progress` filters or redacts it, and retention deletes it on the
   same schedule as everything else in the run directory — it is not a
   secret-redaction mechanism.
6. Call `progress` with a task ID for a defensively parsed summary of that
   same output: `message_event_count`, `last_assistant_text` (bounded),
   `provider`, `model`, and, once a `complete` event has been seen,
   `complete: true` plus `total_tokens`/`input_tokens`/`output_tokens`/
   `cache_read_tokens`/`cache_write_tokens`/`cost_usd`. It also reports
   `tool_call_count` and `last_event_at` (when the task last produced a
   stream event — useful for telling a slow task from a wedged one), and
   `stdout_truncated`/`bytes_dropped` when the run's output exceeded
   `--output-limit`. Those four come from what was parsed live as the task
   ran, so they are absent for a task re-adopted after a restart or created
   before migration `005`. Unknown or malformed
   lines in the underlying stream are skipped, never fatal. **Read
   `message_event_count` only as a rough activity signal, not a turn
   count** — a single logical reply can be streamed as several `message`
   events (observed directly in this project's own saved sample output,
   `.goose/workflows/phase-1b-runner-activation/phase1/evidence/smoke-attempt2.stdout`,
   where one eight-character reply arrived as eight separate events).
7. Both `logs` and `progress` return a clear tool error — not an empty or
   default-valued success — for a task ID with no run directory, and the
   error wording distinguishes why: an unknown task ID, a known task that
   has not started yet, and a known task whose run directory existed but is
   no longer available (most likely swept by retention).
8. To queue a live task a further instruction without cancelling it, call
   `send_message` with that task's ID and a `body` (no secrets — see "Task
   mailbox" above). It is only picked up at the task's own next checkpoint
   (its recipe calling `read_inbox`), never mid-turn; whether a given recipe
   checks at all is up to how it was authored. The store accepts a message for
   any existing task row, including an already-ended task, but an ended Goose
   process cannot read it: that message remains durable and undelivered.
9. In stdio mode, exit the server when its owning foreground process is no
   longer needed. In persistent mode, closing one client's connection (or its
   `mcp-shim` process) does not stop the orchestrator; stop it directly if it
   should no longer run.

This describes the local MCP task protocol, not a shell-background contract.
Do not use `&`, `nohup`, a service manager, or a remote wrapper to infer
durable or daemon behavior beyond what is documented above. In stdio mode,
ownership does not make work survive terminal, client, host, or reboot loss;
in persistent mode, it survives a client disconnect/reconnect but still not a
host or reboot loss.

### Safe local fixtures, records, and cleanup

Keep fixtures benign, local, and allowlisted. Give every trial a dedicated
workspace and database/run directory; do not reuse a directory holding a task
whose outcome you still need. Bound runtime, queue capacity, and captured
output in the invoking harness. Preserve task IDs, status/result observations,
and failure output needed for diagnosis without copying credentials, tokens, or
provider configuration into fixtures or logs.

Before deleting a local trial directory, stop using its stdio process and make
an explicit retention decision. Delete only a directory that the operator
created for that one trial, after confirming it contains no task evidence to
retain. Never use broad recursive cleanup against a workspace, home/cache,
session location, or an unknown path. The repository does not supply a cleanup
command, and these instructions authorize neither fixture execution nor any
Goose/provider/network action.

### Out of scope

Current limits are deliberate: allowlisted inputs, typed MCP tools, bounded
scheduler capacity, local SQLite records, and explicit cancellation, in both
transport modes. Persistent mode (`--listen`/`cmd/mcp-shim`) delivers only
survival of a client disconnecting and reconnecting — it does **not**
establish a durable background service with crash/kill/reboot recovery,
remote/network-exposed access, authentication, multi-user coordination,
globally complete session inspection, or a safe concurrent shared-workspace
editing model.

**Persisted-PID process-identity reconciliation is now in scope, narrowly.**
On every startup (Linux only — see the Darwin note below), the orchestrator
reconciles every non-terminal task row against the real OS process state
before accepting new work: a task whose process survived an orchestrator
restart within the same boot session is re-adopted via verified
pid+pgid+start-time identity and remains cancellable and observable through
to its real exit; a task whose process is verifiably gone (its pid no longer
exists) is moved to a terminal state (`interrupted`) with an explicit
reason; and a task whose identity cannot be positively verified (for
example, the pid now belongs to an unrelated recycled process) is marked
`unknown` and its pid is never signaled. This closes the restart gap for the
one failure mode it targets — an orchestrator crash or manual bounce while
tasks are running — and nothing more. The following remain explicitly out
of scope:

- **No daemon/systemd/launchd unit.** Nothing starts, supervises, or
  restarts the orchestrator process itself; reconciliation only runs inside
  whatever process a human or script starts next.
- **No reboot survival.** Reconciliation identity is `/proc/<pid>/stat`'s
  start-time field, which is ticks since boot — a value that is only
  meaningful within one boot session. A reboot kills every child process
  outright regardless, so there is nothing to reconcile across one, and this
  feature does not attempt to.
- **No Darwin start-time verification.** The Darwin build of the
  reconciliation identity check (`VerifiedIdentity`/`Adopt`) always fails
  closed — "cannot verify" — on this host, because there is no on-host
  evidence yet for a working start-time-equivalent primitive (`sysctl
  KERN_PROC_PID`/`kinfo_proc.p_starttime`). A restart on macOS therefore
  always falls back to mark-only (`interrupted`/`unknown`); it never
  re-adopts or signals a process there. macOS supervision code is
  cross-compiled here; its runtime behavior still needs evidence on a
  Darwin host.

Run-directory retention is implemented (see "Run-directory retention"
above) but only while the orchestrator process is running, and only on a
timer — not triggered by `spawn`, and not a crash/reboot-recovery mechanism.
Its count limit also only ranks run directories that still have a matching
task row; an orphaned run directory relies on the age limit instead (see the
limitation noted above).

A future phase is where durable service ownership decisions belong,
including a separately designed daemon/service lifecycle (systemd/launchd
unit), Darwin-verified start-time comparison, authenticated remote transport
if ever approved, and multi-operator workspace isolation. None of those
capabilities is enabled by the commands above.

**The task mailbox (see "Task mailbox" above) is deliberately narrow.** No
mid-turn interruption (`cancel` is still the only hard stop); no direct
agent-to-agent transport (hub-and-spoke through the orchestrator only); no
event stream, subscription mechanism, audit log, task ledger/DAG, or UI built
on top of it — this is only the mailbox primitive itself. `spawn`'s optional
`inbox_policy` input sets a task's policy away from its `controller_only`
default to `open` at spawn time; there is no tool to change it afterward.
