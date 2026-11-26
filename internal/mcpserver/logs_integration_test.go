package mcpserver_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLogsCursorRoundTrip(t *testing.T) {
	server, store, _, runsRoot := newAcceptanceServer(t)
	client := connect(t, server)
	ctx := context.Background()
	const id = "cursor-fixture"
	if _, _, err := store.Create(ctx, tasks.Task{ID: id, Recipe: "allowed.yaml", IdempotencyKey: id}); err != nil {
		t.Fatal(err)
	}

	callLogs := func(args map[string]any) *mcp.CallToolResult {
		t.Helper()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "logs", Arguments: args})
		if err != nil {
			t.Fatalf("logs(%v): %v", args, err)
		}
		return result
	}
	wantError := func(args map[string]any, message string) {
		t.Helper()
		result := callLogs(args)
		for _, content := range result.Content {
			if text, ok := content.(*mcp.TextContent); result.IsError && ok && strings.Contains(text.Text, message) {
				return
			}
		}
		t.Fatalf("logs(%v) = %+v, want tool error containing %q", args, result, message)
	}
	read := func(args map[string]any) map[string]any {
		t.Helper()
		result := callLogs(args)
		out, ok := result.StructuredContent.(map[string]any)
		if result.IsError || !ok {
			t.Fatalf("logs(%v) returned error or invalid output: %+v", args, result)
		}
		return out
	}
	page := func(stream string, cursor, maxBytes int) map[string]any {
		t.Helper()
		return read(map[string]any{"id": id, "stream": stream, "cursor": cursor, "max_bytes": maxBytes})
	}
	wantPage := func(stream string, cursor, maxBytes int, text string, next int, more bool) {
		t.Helper()
		got := page(stream, cursor, maxBytes)
		want := map[string]any{stream: text, "next_cursor": float64(next), "more_available": more}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("page(%s, %d, %d) = %v, want %v", stream, cursor, maxBytes, got, want)
		}
	}
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	wantError(map[string]any{"id": "unknown", "stream": "stdout", "cursor": 0}, "task not found")
	wantError(map[string]any{"id": id, "stream": "stdout", "cursor": 0}, "has not started yet")
	if _, err := store.Transition(ctx, id, tasks.Starting, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(ctx, id, tasks.Running, "", nil); err != nil {
		t.Fatal(err)
	}
	run, err := (artifacts.Manager{Root: runsRoot}).Create(id)
	if err != nil {
		t.Fatal(err)
	}
	wantPage("stdout", 0, 0, "", 0, false)
	wantError(map[string]any{"id": id, "stream": "stdout", "cursor": 1}, "beyond captured end")

	stdout := "ab💡café\nxyz—012\nend"
	write(run.StdoutPath, stdout)
	write(run.StderrPath, "warn\nmore")
	wantPage("stdout", 0, 4, "ab", 2, true)
	wantPage("stdout", 2, 1, "💡", 6, true)
	wantPage("stderr", 0, 4, "warn", 4, true)
	wantPage("stderr", 4, 32, "\nmore", 9, false)

	var joined strings.Builder
	cursor := 0
	for more := true; more; {
		p := page("stdout", cursor, 6)
		text, textOK := p["stdout"].(string)
		next, nextOK := p["next_cursor"].(float64)
		var moreOK bool
		more, moreOK = p["more_available"].(bool)
		if !textOK || !nextOK || !moreOK || !utf8.ValidString(text) || len(text) == 0 || len(text) > 6 ||
			int(next) != cursor+len(text) || more != (int(next) < len(stdout)) {
			t.Fatalf("invalid stdout page at %d: %v", cursor, p)
		}
		joined.WriteString(text)
		cursor = int(next)
	}
	if joined.String() != stdout || cursor != len(stdout) {
		t.Fatalf("joined output = %q at cursor %d, want %q at cursor %d", joined.String(), cursor, stdout, len(stdout))
	}
	wantPage("stdout", cursor, 0, "", cursor, false)
	f, err := os.OpenFile(run.StdoutPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString(" appended")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("append: write=%v close=%v", writeErr, closeErr)
	}
	wantPage("stdout", cursor, 0, " appended", cursor+len(" appended"), false)
	wantPage("stderr", 9, 0, "", 9, false)

	for _, bad := range []any{-1, 1.5, "0", nil, len(stdout) + 1000} {
		wantError(map[string]any{"id": id, "stream": "stdout", "cursor": bad}, "")
	}
	for _, stream := range []string{"", "both", "other"} {
		wantError(map[string]any{"id": id, "stream": stream, "cursor": 0}, "requires stream")
	}
	wantError(map[string]any{"id": id, "cursor": 0}, "requires stream")

	write(run.StderrPath, "\xffa\xf0\x9f")
	wantPage("stderr", 0, 3, "�", 1, true)
	wantPage("stderr", 1, 3, "a", 2, true)
	wantPage("stderr", 2, 3, "�", 3, true)
	wantPage("stderr", 3, 3, "�", 4, false)

	limit := int(logs.MaxTailBytes)
	large := strings.Repeat("x", limit) + "last"
	write(run.StdoutPath, large)
	wantPage("stdout", 0, 0, large[:limit], limit, true)
	wantPage("stdout", 0, limit*2, large[:limit], limit, true)
	wantPage("stdout", limit, limit*2, "last", len(large), false)

	write(run.StderrPath, "warn")
	tail := read(map[string]any{"id": id, "max_bytes": 4})
	wantTail := map[string]any{"stdout": "last", "stdout_truncated": true, "stderr": "warn", "stderr_truncated": false}
	if !reflect.DeepEqual(tail, wantTail) {
		t.Fatalf("tail = %v, want %v", tail, wantTail)
	}
	tail = read(map[string]any{"id": id, "stream": "stdout"})
	wantTail = map[string]any{"stdout": large[len(large)-limit:], "stdout_truncated": true, "stderr_truncated": false}
	if !reflect.DeepEqual(tail, wantTail) {
		t.Fatalf("default stdout tail = %d bytes, want %d with legacy fields", len(tail["stdout"].(string)), limit)
	}

	exitCode := 0
	if _, err := store.Transition(ctx, id, tasks.Succeeded, "", &exitCode); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(run.Dir); err != nil {
		t.Fatal(err)
	}
	wantError(map[string]any{"id": id, "stream": "stdout", "cursor": 0}, "possibly removed by retention")
}
