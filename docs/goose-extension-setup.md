# Setting up as a Goose extension

This walks through adding `goose-process-orchestrator` as a Goose extension — either by hand or by having Goose itself follow these steps — so `server_info`/`spawn`/`status`/`result`/`wait`/`cancel`/`list`/`logs`/`progress`/`send_message` become tools available in a normal Goose session, backed by background recipe runs that survive you closing the window.

Use **persistent mode** (`cmd/mcp-shim`, this doc's default) unless you have a specific reason not to: it is what makes a spawned task keep running, and stay checkable/cancelable, after the client that started it disconnects. See the root [`README.md`](../README.md) for the full behavioral description and what it does *not* cover (orchestrator-process crash/reboot survival is still out of scope).

## 0. Prerequisites

- Go (matching [`go.mod`](../go.mod)) to build the two binaries.
- Goose itself already configured with a provider/model (`goose configure` → "Configure Providers", or check with `goose info --check`) — this extension does not change how recipes authenticate or run, only how they're launched and tracked.
- An existing directory of recipes you want allowlisted (`--recipe-root`). Only recipes resolving inside this directory can ever be spawned.

## 1. Build the two binaries

```sh
cd /path/to/goose-process-orchestrator
go build -o ~/.local/bin/goose-orchestrator ./cmd/orchestrator
go build -o ~/.local/bin/goose-mcp-shim ./cmd/mcp-shim
```

Build natively on each machine you use this on (Linux here, macOS at work later) rather than relying on a cross-compiled binary — cross-compilation to Darwin is verified to *produce* a valid binary, but has never actually been run on a Darwin host. Pick any stable install directory that's on your `PATH` or that you're willing to reference by full path; `~/.local/bin` above is just an example (and matches where this machine's other stdio extensions, like `gitnexus`, already live).

## 2. Pick stable local paths

You need four local paths. `--db` and `--runs-dir` are created automatically if missing; **the socket's parent directory is not** — create it first.

```sh
mkdir -p ~/.local/state/goose-orchestrator
```

| Flag | Example | Notes |
|---|---|---|
| `--socket` | `~/.local/state/goose-orchestrator/orchestrator.sock` | Parent directory must already exist. Local filesystem permissions only (mode 0600); never a network listener. |
| `--db` | `~/.local/state/goose-orchestrator/orchestrator.db` | SQLite task records. An existing file from an earlier version is upgraded in place on startup: the orchestrator now applies forward-only numbered migrations and records each in a `schema_migrations` table, so you no longer need to delete the file or pick a fresh path when the schema gains a column. Your existing task history is preserved. |
| `--recipe-root` | wherever your recipes live | Must already exist; every spawned recipe must resolve under it. |
| `--runs-dir` | `~/.local/state/goose-orchestrator/runs` | Per-task stdout/stderr/evidence directories. Each task's captured stdout is capped at 8 MiB by default (`--output-limit`); nothing deletes a finished task's directory unless run-directory retention is turned on, so size this path for the total you intend to keep. |

## 3. Find your Goose config file

```sh
goose info
```

This prints `Config yaml:` with the exact path for your machine/OS/install — use that path below rather than assuming a specific location; it genuinely differs between a Goose Desktop install and a bare CLI install, and between OSes.

## 4. Add the extension

**Option A — edit the config file directly** (fastest, and the one to use if Goose itself is doing this setup autonomously). Open the `Config yaml:` path from step 3 and add an entry under the top-level `extensions:` map, alongside whatever is already there:

```yaml
extensions:
  # ... your other extensions stay as they are ...
  goose_orchestrator:
    enabled: true
    type: stdio
    name: Goose Process Orchestrator
    description: Spawn background goose recipe runs that survive client disconnects
    cmd: /home/YOU/.local/bin/goose-mcp-shim
    args:
      - --socket
      - /home/YOU/.local/state/goose-orchestrator/orchestrator.sock
      - --orchestrator-binary
      - /home/YOU/.local/bin/goose-orchestrator
      - --recipe-root
      - /home/YOU/path/to/your/recipes
      - --goose-binary
      - goose
      - --db
      - /home/YOU/.local/state/goose-orchestrator/orchestrator.db
      - --runs-dir
      - /home/YOU/.local/state/goose-orchestrator/runs
      - --max-concurrent
      - '20'
      - --max-turns
      - '25'
    envs:
      GOOSE_MODE: auto
    env_keys: []
    timeout: 300
    cwd: null
    bundled: null
```

Replace every `/home/YOU/...` path with the real absolute paths from steps 1-2. `cmd` must point at `goose-mcp-shim`, **not** `goose-orchestrator` directly — the shim is what Goose actually launches as its stdio child; it relays to (and auto-starts, if needed) the real orchestrator. `--max-concurrent` bounds how many recipes can run at once (raise/lower freely — concurrent runs are isolated from each other); `--max-turns` is the default turn limit for a spawned run, overridable per-`spawn` via that tool's own `max_turns` input.

**`envs: {GOOSE_MODE: auto}` is required, not optional.** Goose's own interactive tool-approval prompt (`smart_approve`, the usual default) has someone to ask only when the session is attached to a real terminal. Native sub-recipe delegation still has that: a delegated child is a session living inside your own interactive Goose process, so Goose can prompt *you* directly when it needs approval. A task dispatched through this extension's `spawn` is a wholly separate `goose run` process, started detached by the orchestrator daemon, with no stdin and no terminal — by design, since surviving you closing the client is the entire point. If that daemon (or anything it spawns) runs under `smart_approve`, the first tool call needing approval fails outright with `Error: Tool approval required in non-interactive mode with GooseMode::smart_approve`; there is no mechanism anywhere in this codebase for a spawned task to bubble an approval request up to a human — no IPC channel, no pause-and-wait, nothing (the mailbox feature in step 6 is a voluntary, checkpoint-only note channel a recipe has to poll itself; it is not a tool-execution intercept). `GOOSE_MODE=auto` trades Goose's own synchronous approval gate for trusting each recipe's *own* self-imposed gates (the `# Approval needed` pattern that stops and leaves a durable record in state instead of prompting) — that tradeoff is a precondition for this extension to run anything beyond the most trivial recipe, not a hardening option layered on top.

This env var is scoped to the orchestrator daemon process and propagates to every `goose run` child it spawns (Go's `exec.Command` inherits the parent's environment when `Env` isn't set explicitly, and neither the shim nor the orchestrator override it) — it does **not** touch your own interactive Goose session, which keeps whatever `GOOSE_MODE` it already had. It is also all-or-nothing per daemon instance: there is no per-task override, so every task anyone spawns through this one orchestrator process runs under the same mode.

**Confining unattended workers (optional).** Because workers run unattended in auto mode, an operator may confine them by passing a wrapper script as `--goose-binary`. The wrapper (`goose-sandboxed`) execs the real `goose` under bubblewrap with identical arguments and environment. Only the task's `repo_root`, the run directory, and goose/provider state are writable; `$HOME` is otherwise an empty tmpfs. GPO itself is unchanged.

Limits: the network is shared (no network filtering), provider credentials remain visible to the worker, and this is Linux only. It is not a complete security boundary. After any daemon restart, check `server_info.goose_binary`: an auto-starting MCP shim reuses its baked-in flags, so the wrapper may not be the binary in use.

Details live in the projects repository (https://github.com/sjbavier/projects): `.goose/scripts/goose-sandboxed`, `.goose/scripts/goose-sandboxed-selftest`, and `.goose/workflows/gpo-sandbox.md`.

**Option B — `goose configure` interactively**, if a human is doing this: run `goose configure`, choose "Add Extension" → "Command-line Extension", and supply the same `cmd`/`args` as above when prompted.

## 5. Restart to pick it up

Restart Goose Desktop, or start a fresh `goose session`/`goose run` — extensions are read at startup. You should now see ten new tools available: `server_info`, `spawn` ("Queue an approved recipe"), `status`, `result`, `wait` (bounded server-side waiting; default 240 seconds, maximum 280, safely below this config's 300-second extension timeout), `cancel`, `list` (rediscover a lost task ID), `logs` (bounded tail of a task's captured stdout/stderr, while it's still running or after), `progress` (parsed message-event count, last assistant text, provider/model, tokens/cost — see the root README for the message-event-count caveat and the secret-leakage warning), and `send_message` (queue a running task an instruction, delivered at its next checkpoint — see "Task mailbox" in the root README, and step 6 below if you also want spawned agents themselves to read/send mailbox messages).

## 6. Optional: let spawned agents read and send mailbox messages

Steps 1-5 give **you** (or whatever is calling this extension's tools) a `send_message` tool to queue a running task an instruction. That alone does nothing for the task's own agent unless it can also reach its inbox — for that, a *second*, independent piece needs to be configured: the mailbox feature on the orchestrator binary itself, which attaches a small extension to each spawned `goose run` process giving it `read_inbox`/`send_message` tools of its own.

This needs two more flags on `goose-orchestrator` itself. `goose-mcp-shim` forwards `--mailbox-extension-binary`/`--mailbox-socket` to any orchestrator it auto-starts, so you can add them to the shim's own `args` in the extension config from step 4 instead of starting the orchestrator by hand:

```yaml
      - --mailbox-extension-binary
      - /home/YOU/.local/bin/goose-mailbox-extension
      - --mailbox-socket
      - /home/YOU/.local/state/goose-orchestrator/mailbox.sock
```

Build the extension binary first:

```sh
go build -o ~/.local/bin/goose-mailbox-extension ./cmd/mailbox-extension
```

If an orchestrator is already running on the shim's `--socket` path without these flags, stop it first, since the first process to own a socket keeps it, and a later shim invocation just dials the existing one without changing its flags. Leave this step out entirely if you only want the controller-side `send_message` tool above; every spawned recipe's argv and environment are byte-identical to before when `--mailbox-extension-binary` is left unset.

Once enabled, a spawned recipe that calls `read_inbox` at a checkpoint sees anything queued for it since the last check, marked delivered in the same transaction as the read — this is checkpoint-only, at-most-once retrieval after a successful read, never mid-turn. For course correction, recipes should check after loading their initial instructions and again between major phases; a startup-only check cannot receive guidance sent later. An already-ended task cannot consume a new message even though the row remains durable. Continue ended/incomplete work by spawning a new task with a new idempotency key and recipe-declared guidance (prefer a file parameter such as `resume_notes_path` for substantial context). See "Task mailbox" and "Guidance, checkpoints, and continuing incomplete work" in the root README for the full contract: task-id addressing, the `controller_only` default `inbox_policy` (pass `inbox_policy: "open"` to `spawn` to allow agent-to-agent messaging for that task), and the no-secrets warning for message bodies.

## 7. Verify it actually works

Ask Goose (or call the tools directly if you're scripting this) to:

1. Call `server_info` and confirm the reported recipe/runs paths and limits match this setup; then `spawn` a trivial, already-allowlisted recipe and note the returned task `id`.
2. `wait` with `ids: [id]` — confirm it returns when the task reaches a terminal, `interrupted`, or `unknown` state (or with `timed_out: true`). A cancelled client call stops only the wait, never the task.
3. `result` with that `id` once it finishes. Check `outcome` is `completed` and read the model's own answer from `final_text` — `result`'s `result` field is the run-directory path, not the answer. An `outcome` of `no_complete_event` on a task this orchestrator ran start to finish is worth investigating; see the `outcome` table in the root README.
4. For persistent mode specifically: close the Goose window/session entirely, then start a brand new one and `status` the same `id` again — it should show the real state, not be lost. That's the capability this whole setup exists for.

If step 4 doesn't work, confirm `cmd` really points at `goose-mcp-shim` and not `goose-orchestrator` — pointing a client directly at the orchestrator binary with `--mcp-stdio` instead would "work" for steps 1-3 but silently drop exactly the disconnect-survival property step 4 is checking.

A trivial single-step recipe (like `repo-tour.yaml`/`smoke-test.yaml`) can succeed in steps 1-3 even without `GOOSE_MODE=auto`, if it never happens to need more than one tool call — that is not sufficient proof the extension is correctly configured. Spawn at least one recipe that needs several sequential tool calls (reading a file, then writing one, say) before trusting that setup is complete; the `GOOSE_MODE` troubleshooting entry below explains what it looks like when that's missing.

## Troubleshooting

- **"recipe root ... is not accessible"**: the `--recipe-root` path in your extension's `args` doesn't exist or isn't readable from wherever Goose runs the extension from. Use an absolute path.
- **"--mcp-stdio and --listen are mutually exclusive"**: don't add both flags; this setup only ever uses `--listen` (via the shim), never `--mcp-stdio`, for persistent mode.
- **`spawn` returns `"task queue is at capacity"`**: `--max-concurrent` tasks are already running (plus a small burst-admission buffer of the same size); wait for one to finish, or raise `--max-concurrent`.
- **`spawn` returns `invalid recipe parameters` or `recipe parameter schema is unusable`**: the `parameters`/`file_parameters` input doesn't match what the recipe declares. Keys must be declared string parameters, every `required` key must be present in exactly one map, and at most 32 combined keys are allowed. Scalar values remain limited to 4 KiB with no controls/line breaks. File values are UTF-8 text (tab/LF/CR allowed, other controls rejected), limited to 256 KiB each and 1 MiB total. Nothing was queued, so fix the call and retry. The second message means the recipe YAML schema could not be trusted.
- **A recipe's parameters show up in `runs/` or the task database**: that is by design. File contents are stored unencrypted in SQLite, materialized as 0600 files under the private run directory, and may reach the model/provider when read. **Never pass secrets in either parameter form**; there is no secret detection, encryption, redaction, or secure deletion. Runs retention removes materialized files with the run directory but leaves SQLite audit data.
- **A `spawn`ed recipe gets cut off early**: it needs more turns than the `--max-turns` default allows — pass a larger `max_turns` on that specific `spawn` call (up to `--max-turns-ceiling`), or raise the extension's default. There is no native resume of the ended Goose process. Spawn a new task with a new idempotency key and recipe-declared resume guidance; for live guidance before exit, enable the mailbox and have the recipe call `read_inbox` between major phases.
- **Nothing happens / orchestrator never seems to start**: check that `--orchestrator-binary` in the shim's `args` is the exact path from step 1, and that it's executable (`chmod +x` if you copied it from somewhere).
- **A spawned task's `result` says `Error: Tool approval required in non-interactive mode with GooseMode::smart_approve...`**: the orchestrator daemon (or whatever shim auto-started it) is missing `envs: {GOOSE_MODE: auto}` from step 4 — it's running under the interactive approval mode with no terminal to prompt. Fix the extension config, then stop the stale orchestrator process (`pkill -f orchestrator-binary-path`) so the next client connection auto-starts a fresh one with the corrected environment — an already-running orchestrator keeps whatever environment it was first started with.
- **Two different extension entries pointing at the same `--socket` path, with different `--recipe-root`/`--db`/etc.**: whichever one happens to auto-start the orchestrator first "wins" for that socket's whole lifetime; the other's flags are simply never used for that run. Give genuinely different setups genuinely different socket paths.

## Out of scope, still

Same boundary as the root README: no survival of the orchestrator process itself being killed or the machine rebooting, no remote/network access, no multi-operator coordination. This setup only makes a client disconnecting/reconnecting (closing Goose, switching windows, a crash in the *client*) not lose track of anything.
