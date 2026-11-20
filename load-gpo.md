# Upgrade and verify Goose Process Orchestrator

Upgrade the locally configured **Goose Process Orchestrator** extension from the
current clean `main` branch without assuming fixed install, config, state, recipe,
or database paths.

## Guardrails

- Read `README.md` and `docs/goose-extension-setup.md` first.
- Discover the active config path with `goose info`. Treat the existing
  extension entry's `cmd`, `args`, `envs`, and `timeout` as authoritative.
- Do not edit the extension config unless explicitly asked. Do not enable the
  optional mailbox or set a workdir merely because the build supports them.
- Do not touch `.goose/workflows/`.
- Do not run a billed recipe/provider smoke test without separate approval.
- Before stopping the daemon, verify there are no `queued`, `starting`, or
  `running` tasks. Stop and report any rather than interrupting them.

## 1. Establish the upgrade delta

1. Require a clean `main` checkout.
2. Inspect the installed orchestrator with `go version -m <binary>`. If its
   `vcs.revision` exists locally, review
   `git log --oneline <revision>..HEAD`. If unavailable or marked dirty,
   report that and use current source/docs as the contract rather than guessing.
3. Read MCP registrations in `internal/mcpserver/server.go` and flags in both
   command binaries. Current source—not a frozen checklist—is authoritative.

## 2. Build and install atomically

Before building, confirm the toolchain that will actually resolve: run
`go version` and `uname -m`, and compare the reported Go version against
`go.mod`'s `go 1.27` and the reported arch against the build target. A
non-interactive shell (such as the one Goose uses) can resolve a different,
older `go` than an interactive shell does — if the version or arch doesn't
match, stop and report every `go` on `PATH` (`which -a go`) rather than
letting the build attempt a toolchain download.

Build all three binaries from a temporary detached worktree at `HEAD`, with outputs in a separate temporary directory. Run with `GOTOOLCHAIN=local` so a wrong resolved toolchain fails fast instead of attempting a network download:

```sh
git worktree add --detach "$src_tmp" HEAD
( cd "$src_tmp" &&
  GOTOOLCHAIN=local go build -o "$bin_tmp/goose-orchestrator" ./cmd/orchestrator &&
  GOTOOLCHAIN=local go build -o "$bin_tmp/goose-mcp-shim" ./cmd/mcp-shim &&
  GOTOOLCHAIN=local go build -o "$bin_tmp/goose-mailbox-extension" ./cmd/mailbox-extension )
```

On macOS, also set `TMPDIR=/tmp` for `go test` until CR-019 lands.

The detached worktree keeps build identity clean even if this instruction file
is being revised in the primary checkout. Remove it with `git worktree remove`
afterward. Only after every build succeeds, atomically replace the binaries at
paths implied by the configured shim/orchestrator arguments. Installing the
mailbox binary does not enable the optional mailbox. Confirm build metadata
identifies current clean HEAD and has `vcs.modified=false`.

## 3. Restart safely

Stop only the daemon that owns the configured Unix socket—never broad-match
unrelated shims or clients. Reload the extension so its stdio child uses the new
shim. Goose default extension changes apply to future sessions, not active ones;
use the current session's supported reload mechanism when available, otherwise
tell the operator to start a new session. The shim auto-starts the daemon.

## 4. Verify without a provider run

Using the freshly loaded extension, report exact results:

1. Call `server_info`. Confirm paths/limits match config, `gpo_commit`
   matches current HEAD when available, daemon start time is fresh, and report
   mailbox status without changing it.
2. Compare exposed tools to source. Current source registers `server_info`,
   `spawn`, `status`, `result`, `wait`, `cancel`, `list`,
   `logs`, `progress`, and `send_message`.
3. If an existing task exists, use `list` and `status` or `result` to
   confirm migrations preserved history. Absent live-summary fields on old or
   re-adopted tasks are not corruption.
4. If an already-terminal task exists, call `wait` with that one ID and a
   short timeout; expect enriched output and `timed_out: false`.
5. Confirm `list` exposes `idempotency_key` and accepts literal
   `key_prefix` when an existing key permits a safe test.

If no task exists, report skipped checks; do not launch billed work to compensate.

## Current contracts to recognize

Verify these from source/docs rather than mutating production state solely to
demonstrate them:

- Prompt-less recipes are rejected before admission.
- Declared scalar `parameters` and bounded UTF-8 `file_parameters`
  participate in idempotency and are omitted from responses.
- `status`/`result` share enriched outcomes, capture accounting,
  timestamps, final text, exit/failure data, and bounded failure stderr.
- `wait` accepts 1–32 unique IDs, `all`/`any`, defaults to 240
  seconds, and caps at 280 below the configured 300-second extension timeout.
- `--output-limit` defaults to 8 MiB independently of live summaries.
- `server_info` reports startup-cached version/config/build facts.
- Max-turn stop-text matching remains disabled without a confirmed Goose sample;
  uncertain successful exits are `no_complete_event`, never `completed`.
- The optional mailbox is pull/checkpoint based, not native process/session
  resume, and cannot revive an ended task.

Finish with old/new build identities, restart status, server facts, tool set,
history/migration evidence, skipped checks, and whether a new session is needed.
