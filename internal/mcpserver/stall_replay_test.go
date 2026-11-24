package mcpserver_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/mcpserver"
	"github.com/local/goose-process-orchestrator/internal/orchestrator"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestGooseStallReplay(t *testing.T) {
	const diagnostic = "I've reached the maximum number of actions I can do without user input. Would you like me to continue?"
	stall := readReplay(t, "testdata/stall-max-turns-goose-1.60.2.jsonl")
	legacy := readReplay(t, "../logs/testdata/stall-max-turns-goose-1.53.0.jsonl")
	cases := []struct {
		name, stream, stderr, status, outcome string
		exit                                  int
	}{
		{"goose-1.60.2", stall, "Error: " + diagnostic + "\n", "failed", "stalled_max_turns", 1},
		{"goose-1.53.0", legacy, "", "succeeded", "stalled_max_turns", 0},
		{"structured-error-only", `{"type":"message","message":{"role":"assistant","content":[{"type":"error","kind":"resourceExhausted","message":"` + diagnostic + `"}]}}`, "", "failed", "stalled_max_turns", 1},
		{"stream-error-only", `{"type":"error","error":"` + diagnostic + `"}`, "", "failed", "stalled_max_turns", 1},
		{"provider-quota", `{"type":"message","message":{"role":"assistant","content":[{"type":"error","kind":"resourceExhausted","message":"provider quota exceeded"}]}}
{"type":"error","error":"provider quota exceeded"}`, "provider quota exceeded\n", "failed", "process_failed", 1},
		{"model-quote", `{"type":"message","message":{"id":"quote","role":"assistant","metadata":{"inference":{"provider":"fixture","requestedModel":"fixture"}},"content":[{"type":"text","text":"` + diagnostic + `"}]}}
{"type":"complete"}`, "", "succeeded", "completed", 0},
		{"plain-text-with-unrelated-failure", `{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"` + diagnostic + `"}]}}
{"type":"error","error":"unrelated process failure"}`, "unrelated process failure\n", "failed", "process_failed", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const id = "stall-replay"
			dir := t.TempDir()
			binary := filepath.Join(dir, "goose")
			script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then printf 'goose version 1.60.2\n'; exit 0; fi
if [ "$#" -ne 9 ] || [ "$1" != run ] || [ "$2" != --recipe ] ||
   [ "$3" != "$0.recipe.yaml" ] || [ "$4" != --no-session ] ||
   [ "$5" != --max-turns ] || [ "$6" != 2 ] || [ "$7" != --quiet ] ||
   [ "$8" != --output-format ] || [ "$9" != stream-json ]; then
    printf 'unexpected replay arguments\n' >&2
    exit 99
fi
cat "$0.stdout"
cat "$0.stderr" >&2
exit %d
`, tc.exit)
			for path, data := range map[string]string{
				binary: script, binary + ".recipe.yaml": "title: stall replay\n",
				binary + ".stdout": tc.stream, binary + ".stderr": tc.stderr,
			} {
				if err := os.WriteFile(path, []byte(data), 0o700); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			db, err := sql.Open("sqlite", filepath.Join(dir, "tasks.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if err := tasks.Migrate(ctx, db); err != nil {
				t.Fatal(err)
			}
			store := tasks.NewStore(db)
			runs := artifacts.Manager{Root: filepath.Join(dir, "runs")}
			launcher := goose.Runner{Config: goose.Config{Binary: binary, RecipeRoot: dir, MaxTurns: 1}, Artifacts: runs}
			dispatcher, err := scheduler.New(store, orchestrator.New(launcher, store), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			service, err := scheduler.NewService(store, dispatcher, func() string { return id }, runs.Root)
			if err != nil {
				t.Fatal(err)
			}
			server, err := mcpserver.New(service)
			if err != nil {
				t.Fatal(err)
			}
			client := connect(t, server)
			invoke := func(name string, args map[string]any) map[string]any {
				t.Helper()
				res, err := client.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				out, ok := res.StructuredContent.(map[string]any)
				if res.IsError || !ok {
					for _, block := range res.Content {
						if message, ok := block.(*mcp.TextContent); ok {
							t.Log(message.Text)
						}
					}
					t.Fatalf("%s: unexpected tool result", name)
				}
				return out
			}
			invoke("spawn", map[string]any{"recipe": "goose.recipe.yaml", "idempotency_key": id, "max_turns": 2})
			wait := invoke("wait", map[string]any{"ids": []string{id}, "timeout_seconds": 10})
			waited, ok := wait["tasks"].([]any)
			if !ok || len(waited) != 1 || wait["timed_out"] != false {
				t.Fatalf("wait: %#v", wait)
			}
			status := invoke("status", map[string]any{"id": id})
			result := invoke("result", map[string]any{"id": id})
			if !reflect.DeepEqual(status, result) || !reflect.DeepEqual(status, waited[0]) {
				t.Fatalf("terminal responses differ: status=%#v result=%#v wait=%#v", status, result, waited[0])
			}
			if status["status"] != tc.status || status["exit_code"] != float64(tc.exit) || status["outcome"] != tc.outcome {
				t.Fatalf("got status=%v exit_code=%v outcome=%v, want status=%s exit_code=%d outcome=%s",
					status["status"], status["exit_code"], status["outcome"], tc.status, tc.exit, tc.outcome)
			}
			if tail, _ := status["stderr_tail"].(string); tail != tc.stderr {
				t.Fatalf("stderr_tail = %q, want %q", tail, tc.stderr)
			}
			run, err := runs.FindRun(id)
			if err != nil {
				t.Fatal(err)
			}
			if readReplay(t, run.StdoutPath) != tc.stream {
				t.Fatal("captured stdout differs from replay stream")
			}
			row, err := store.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			wantComplete := tc.status == "succeeded"
			if row.CompleteSeen == nil || *row.CompleteSeen != wantComplete {
				t.Fatalf("persisted complete flag = %v, want %v", row.CompleteSeen, wantComplete)
			}
			t.Logf("status=%s exit_code=%d outcome=%s complete=%t", tc.status, tc.exit, tc.outcome, *row.CompleteSeen)
		})
	}
}

func readReplay(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
