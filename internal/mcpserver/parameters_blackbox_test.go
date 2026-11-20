package mcpserver_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/goose"
	"github.com/local/goose-process-orchestrator/internal/mcpserver"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"
)

const declaredRecipeBody = `title: acceptance
prompt: |
  Run the declared acceptance recipe.
parameters:
  - key: feature_slug
    input_type: string
    requirement: required
  - key: mode
    input_type: string
    requirement: optional
`

// newParameterServer wires the same chain cmd/orchestrator builds, including
// the launcher Config acting as the admission validator against a real
// allowlisted recipe root.
func newParameterServer(t *testing.T) (*mcp.Server, *tasks.Store, string) {
	t.Helper()
	// Match cmd/orchestrator's DSN: this test reads the store directly while a
	// scheduler worker is concurrently claiming the task, which is exactly the
	// contention the production busy timeout exists to absorb. Without it the
	// read surfaces SQLITE_BUSY.
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "parameters.db")+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := tasks.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := tasks.NewStore(db)
	recipeRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(recipeRoot, "declared.yaml"), []byte(declaredRecipeBody), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &heldRunner{started: make(chan tasks.Task, 8), release: make(chan struct{})}
	t.Cleanup(func() { close(runner.release) })
	dispatcher, err := scheduler.New(store, runner, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	config := goose.Config{Binary: "unused-in-this-test", RecipeRoot: recipeRoot, MaxTurns: 1}
	var mu sync.Mutex
	n := 0
	newID := func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return "param-task-" + strconv.Itoa(n)
	}
	service, err := scheduler.NewService(store, dispatcher, newID, t.TempDir(), scheduler.WithParameterValidator(config))
	if err != nil {
		t.Fatal(err)
	}
	server, err := mcpserver.New(service)
	if err != nil {
		t.Fatal(err)
	}
	return server, store, recipeRoot
}

func TestSpawnAcceptsDeclaredParametersAndOmitsThemFromResponses(t *testing.T) {
	server, store, recipeRoot := newParameterServer(t)
	session := connect(t, server)
	recipe := filepath.Join(recipeRoot, "declared.yaml")

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
		"recipe":          recipe,
		"idempotency_key": "param-key-1",
		"parameters":      map[string]any{"feature_slug": "spawn-recipe-parameters", "mode": "local=autonomous"},
	}})
	if err != nil {
		t.Fatalf("spawn with declared parameters: %v", err)
	}
	if result.IsError {
		t.Fatalf("spawn reported an error: %s", renderContent(result))
	}

	// The durable record carries the parameters...
	listed, err := store.List(context.Background(), tasks.ListFilter{Limit: 10})
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = (%v, %v)", listed, err)
	}
	if listed[0].Parameters["mode"] != "local=autonomous" {
		t.Fatalf("persisted parameters = %v", listed[0].Parameters)
	}

	// ...but no response surface does, per the approved visibility policy.
	id := listed[0].ID
	for _, call := range []struct {
		name      string
		arguments map[string]any
	}{
		{"spawn", map[string]any{"recipe": recipe, "idempotency_key": "param-key-1", "parameters": map[string]any{"feature_slug": "spawn-recipe-parameters", "mode": "local=autonomous"}}},
		{"status", map[string]any{"id": id}},
		{"result", map[string]any{"id": id}},
		{"list", map[string]any{}},
	} {
		out, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: call.name, Arguments: call.arguments})
		if err != nil {
			t.Fatalf("%s: %v", call.name, err)
		}
		rendered := renderContent(out)
		for _, leaked := range []string{"local=autonomous", "spawn-recipe-parameters", "parameters"} {
			if strings.Contains(rendered, leaked) {
				t.Errorf("%s response exposed %q: %s", call.name, leaked, rendered)
			}
		}
	}
}

func TestSpawnRejectsInvalidParametersWithoutCreatingATask(t *testing.T) {
	server, store, recipeRoot := newParameterServer(t)
	session := connect(t, server)
	recipe := filepath.Join(recipeRoot, "declared.yaml")

	cases := map[string]map[string]any{
		"undeclared key":       {"feature_slug": "x", "undeclared": "y"},
		"missing required key": {"mode": "x"},
		"newline in value":     {"feature_slug": "a\nb"},
		"bad key syntax":       {"feature_slug": "x", "bad key": "y"},
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: map[string]any{
				"recipe":          recipe,
				"idempotency_key": "rejected-" + name,
				"parameters":      params,
			}})
			if err == nil && !out.IsError {
				t.Fatalf("spawn accepted invalid parameters: %s", renderContent(out))
			}
		})
	}
	listed, err := store.List(context.Background(), tasks.ListFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Fatalf("rejected spawns created %d task rows", len(listed))
	}
}

// TestSpawnIdempotencyCoversParameters proves the durable identity rule reaches
// the public surface: the same key with a different map is a conflict.
func TestSpawnIdempotencyCoversParameters(t *testing.T) {
	server, _, recipeRoot := newParameterServer(t)
	session := connect(t, server)
	recipe := filepath.Join(recipeRoot, "declared.yaml")
	first := map[string]any{"recipe": recipe, "idempotency_key": "same-key", "parameters": map[string]any{"feature_slug": "a"}}
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: first}); err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	conflicting := map[string]any{"recipe": recipe, "idempotency_key": "same-key", "parameters": map[string]any{"feature_slug": "b"}}
	out, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: conflicting})
	if err == nil && !out.IsError {
		t.Fatalf("replaying one key with different parameters was accepted: %s", renderContent(out))
	}
}

func TestSpawnFileParametersPersistConflictAndNeverEcho(t *testing.T) {
	server, store, recipeRoot := newParameterServer(t)
	session := connect(t, server)
	recipe := filepath.Join(recipeRoot, "declared.yaml")
	const content = "UNIQUE-UPLOADED-CONTENT\nline two"
	args := map[string]any{"recipe": recipe, "idempotency_key": "file-key", "file_parameters": map[string]any{"feature_slug": content}}
	out, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: args})
	if err != nil || out.IsError {
		t.Fatalf("spawn file parameters = %v, %s", err, renderContent(out))
	}
	rows, err := store.List(context.Background(), tasks.ListFilter{Limit: 10})
	if err != nil || len(rows) != 1 || rows[0].FileParameters["feature_slug"] != content {
		t.Fatalf("persisted rows = %#v, %v", rows, err)
	}
	id := rows[0].ID
	for _, call := range []struct {
		name string
		args map[string]any
	}{{"spawn", args}, {"status", map[string]any{"id": id}}, {"result", map[string]any{"id": id}}, {"list", map[string]any{}}} {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: call.name, Arguments: call.args})
		if err != nil {
			t.Fatalf("%s: %v", call.name, err)
		}
		rendered := renderContent(result)
		if strings.Contains(rendered, "UNIQUE-UPLOADED-CONTENT") || strings.Contains(rendered, "file_parameters") {
			t.Fatalf("%s echoed file input: %s", call.name, rendered)
		}
	}
	conflict := map[string]any{"recipe": recipe, "idempotency_key": "file-key", "file_parameters": map[string]any{"feature_slug": "changed"}}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: conflict})
	if err == nil && !result.IsError {
		t.Fatalf("changed content silently reused task: %s", renderContent(result))
	}
}

func TestSpawnRejectsInvalidFileParametersWithoutCreatingATask(t *testing.T) {
	server, store, recipeRoot := newParameterServer(t)
	session := connect(t, server)
	recipe := filepath.Join(recipeRoot, "declared.yaml")
	cases := []map[string]any{
		{"parameters": map[string]any{"feature_slug": "x"}, "file_parameters": map[string]any{"feature_slug": "collision"}},
		{"file_parameters": map[string]any{"../bad": "x", "feature_slug": "ok"}},
		{"file_parameters": map[string]any{"feature_slug": strings.Repeat("x", goose.MaxFileParameterValueBytes+1)}},
		{"file_parameters": map[string]any{"mode": "missing required"}},
	}
	for i, fields := range cases {
		args := map[string]any{"recipe": recipe, "idempotency_key": fmt.Sprintf("bad-file-%d", i)}
		for k, v := range fields {
			args[k] = v
		}
		out, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "spawn", Arguments: args})
		if err == nil && !out.IsError {
			t.Fatalf("case %d accepted: %s", i, renderContent(out))
		}
	}
	rows, err := store.List(context.Background(), tasks.ListFilter{Limit: 10})
	if err != nil || len(rows) != 0 {
		t.Fatalf("invalid requests left rows = %#v, %v", rows, err)
	}
}

func renderContent(result *mcp.CallToolResult) string {
	var b strings.Builder
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			b.WriteString(text.Text)
		}
	}
	if result.StructuredContent != nil {
		encoded, err := json.Marshal(result.StructuredContent)
		if err == nil {
			b.Write(encoded)
		}
	}
	return b.String()
}
