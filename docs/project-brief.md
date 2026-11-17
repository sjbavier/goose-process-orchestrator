# Project brief: Goose Process Orchestrator

> **Status note (2026-11-15):** the sections below are the original initialization snapshot and are kept for provenance; their "Non-claims" and "next workflow" no longer describe the project. The orchestrator now provides SQLite persistence, an MCP server (stdio and Unix-socket modes), real `goose run` subprocess supervision with explicit cancellation, recipe parameters, run-directory retention, same-boot-session restart reconciliation, and a per-task message mailbox. For current behavior and limits, see the root [`README.md`](../README.md) and [`goose-extension-setup.md`](goose-extension-setup.md); per-feature plans and evidence live under `.goose/workflows/`.

## Provenance and selected idea

- **Selected idea:** Goose Process Orchestrator — a durable MCP task server that launches Goose recipes as independently supervised operating-system processes.
- **Ideation artifact:** a local implementation blueprint document, retained outside this repository and not part of its public history.
- **Project slug:** `goose-process-orchestrator`.
- **Initial feature slug:** `phase-0-phase-1-local-mvp`.
- **Intended first artifact:** a Go repository scaffold with a tested lifecycle model and verified `gen-plan` handoff; it is deliberately not a production process launcher.

## Users and intended benefit

MCP clients and operators who need long-running Goose recipe work to continue independently of a request/client lifecycle. The intended benefit is observable, controllable task execution: immediate task IDs, later status/results/logs, and explicit cancellation.

## Non-claims

This initialization does not claim durable daemon behavior, reboot resilience, MCP transport, SQLite persistence, Goose subprocess launching, task recovery, client-disconnect survival, or safe concurrent repository editing. It does not execute Goose recipes.

## Linked facts, assumptions, and constraints

- The blueprint specifies Go for subprocess supervision, concurrency, single-binary deployment, and predictable resource use; Python remains available inside future recipes.
- Local verification found Go `go1.27.2` and Goose `1.53.0`.
- `goose run --help` on that installation confirms `--recipe`, repeated `--params`, `--name`, `--path`, `--no-session`, `--max-turns`, `--quiet`, `--output-format` including `stream-json`, `--provider`, and `--model`.
- The blueprint warns that `cmd.Start()` and a goroutine do not make work survive a short-lived stdio frontend or reboot.
- No dependency downloads, global tools, credentials, external services, Git staging, commits, remotes, deployment, or publication are authorized.

## Acceptance criteria for the next plan

The plan must cover the blueprint's Phase 0 and Phase 1 acceptance tests: nonblocking spawn, unique concurrent runs, reconnectable status/results/logs, explicit whole-process-group cancellation, crash/restart reconciliation, and rejection of invalid recipe paths/parameters or excessive concurrency. It must first perform a controlled integration spike for Goose 1.53 flags and stream-JSON behavior, then select and justify the official MCP Go SDK and SQLite dependency strategy.

## Stack rationale

Go is the selected implementation language because the problem is local OS-process supervision and a durable service boundary. The standard Go toolchain keeps this first artifact reproducible (`go fmt`, `go test`, `go build`) with no dependencies. The future MVP should use the official MCP Go SDK and SQLite only under an approved implementation plan, because adding those dependencies materially affects interoperability, operational maintenance, and security.

## Exact next workflow

Run `/gen-plan phase-0-phase-1-local-mvp`. Preserve the safety boundaries above and the blueprint's durable-daemon distinction.
