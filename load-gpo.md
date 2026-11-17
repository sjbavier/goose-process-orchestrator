Upgrade and verify the "Goose Process Orchestrator" extension after a code update.

Context: the extension's binaries at ~/.local/bin/goose-orchestrator and
~/.local/bin/goose-mcp-shim are stale — they predate several merged features
(recipe parameters, a documented decision against native session resume,
run-directory retention, persisted-PID restart reconciliation, and the
per-task message mailbox with its `send_message` tool). The extension's
config entry in `goose info`'s reported Config yaml already points at the
right paths; nothing there needs to change. The source is at
~/Projects/goose-process-orchestrator on main (commit 7d7299b or later).

1. Rebuild the binaries from that repo:
     cd ~/Projects/goose-process-orchestrator
     go build -o ~/.local/bin/goose-orchestrator ./cmd/orchestrator
     go build -o ~/.local/bin/goose-mcp-shim ./cmd/mcp-shim
     go build -o ~/.local/bin/goose-mailbox-extension ./cmd/mailbox-extension
   The mailbox-extension binary is only used if the optional mailbox is
   enabled (see the note at the end); building it is harmless otherwise.

2. A live goose-orchestrator process is likely already running the old
   binary (check with `pgrep -fa goose-orchestrator`). Stop it so the next
   shim connection starts a fresh one from the rebuilt binary:
     pkill -f '/goose-orchestrator --listen'
   Confirm it's gone, then leave it stopped — the shim auto-starts it on
   first use.

3. Tell me to restart/reload so the extension's stdio child restarts too
   (or just start a brand new goose session — extensions load at startup).

4. Verify the upgrade actually took:
   - Call `status` or `list` on an existing task ID from before the
     upgrade (if any) and confirm the task history still reads correctly
     — the new build applies forward-only DB migrations in place (the
     newest adds the `task_messages` table and a `tasks.inbox_policy`
     column) and must not lose or corrupt existing rows in
     ~/.local/state/goose-orchestrator/orchestrator.db.
   - Confirm the extension now exposes eight tools: `spawn`, `status`,
     `result`, `cancel`, `list`, `logs`, `progress`, and `send_message`.
   - `spawn` one of the allowlisted recipes under
     ~/.local/state/goose-orchestrator/recipes (repo-tour.yaml or
     smoke-test.yaml) with no parameters, exactly as before — confirm it
     still queues and completes normally (this must be byte-for-byte
     unchanged behavior; the mailbox is off by default).
   - If either recipe declares a `parameters:` block, also try `spawn`
     with a matching `parameters` object and confirm it's accepted; try
     one deliberately invalid value (wrong/undeclared key) and confirm
     `spawn` rejects it with a clear error instead of queuing anything.
   - Call `send_message` with a real task ID and a short, non-secret
     `body`; confirm it returns a message id. With the mailbox disabled
     (the default) the message is simply stored for that task; no agent
     reads it unless the mailbox is enabled and the recipe calls
     `read_inbox`. Also confirm `send_message` on an unknown task ID
     returns a clear error.
   - Report back what each call returned.

Do not add, remove, or edit the extension's config entry — only rebuild
the binaries and restart the running processes. Don't touch anything
under the repo's own .goose/workflows/ directory.

Note — the mailbox is optional and NOT enabled by this upgrade. The shim
does not forward the mailbox flags, so enabling it means starting
`goose-orchestrator --listen <the shim's socket>` yourself with
`--mailbox-extension-binary ~/.local/bin/goose-mailbox-extension` and
`--mailbox-socket <path>` before any shim connects (see
docs/goose-extension-setup.md, step 6). Do not do that as part of this
upgrade unless explicitly asked.
