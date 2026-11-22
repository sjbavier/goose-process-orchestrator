package goose

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
)

// fakeGooseScript stands in for the real goose CLI and exits
// deterministically without calling a provider or network endpoint.
const fakeGooseScript = `#!/bin/sh
recipe=""
while [ $# -gt 0 ]; do
  case "$1" in
    --recipe) recipe="$2"; shift 2 ;;
    *) shift ;;
  esac
done
name=$(basename "$recipe")
case "$name" in
  quiet.yaml) echo "ok"; exit 0 ;;
  mode.yaml) printf '%s\n' "$GOOSE_MODE"; exit 0 ;;
  big.yaml)
    i=0
    while [ $i -lt 20 ]; do
      printf '{"type":"message","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"chunk%03d "}]}}\n' "$i"
      i=$((i + 1))
    done
    printf '{"type":"complete","total_tokens":42,"input_tokens":10,"output_tokens":32,"cost_usd":0.01}\n'
    exit 0
    ;;
  *) echo "unknown fixture $name" 1>&2; exit 2 ;;
esac
`

func newFakeGooseRunner(t *testing.T, limit int64, fixtures ...string) (r Runner, recipeRoot string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "fakegoose.sh")
	if err := os.WriteFile(binary, []byte(fakeGooseScript), 0o755); err != nil {
		t.Fatal(err)
	}
	recipeRoot = t.TempDir()
	for _, f := range fixtures {
		if err := os.WriteFile(filepath.Join(recipeRoot, f), []byte("title: fixture\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return Runner{
		Config:    Config{Binary: binary, RecipeRoot: recipeRoot, MaxTurns: 1},
		Artifacts: artifacts.Manager{Root: t.TempDir(), Limit: limit},
	}, recipeRoot
}

// TestStartCapturesStdoutExactlyAsBefore pins that Start's capture behavior
// for an existing-shaped caller is byte-identical to before this chunk: the
// disk file still holds exactly what the process printed, up to Limit.
func TestStartCapturesStdoutExactlyAsBefore(t *testing.T) {
	r, recipeRoot := newFakeGooseRunner(t, 1024, "quiet.yaml")
	p, run, live, err := r.Start(Request{Recipe: filepath.Join(recipeRoot, "quiet.yaml")}, "task-1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if live == nil {
		t.Fatal("Start returned a nil *Live for a real launch")
	}
	result := p.Wait()
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	data, err := os.ReadFile(run.StdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "ok\n" {
		t.Fatalf("stdout.raw = %q, want %q", data, "ok\n")
	}
}

func TestStartOverridesInheritedGooseMode(t *testing.T) {
	for _, mode := range []string{"approve", "smart_approve"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GOOSE_MODE", mode)
			r, recipeRoot := newFakeGooseRunner(t, 1024, "mode.yaml")
			p, run, _, err := r.Start(Request{Recipe: filepath.Join(recipeRoot, "mode.yaml")}, "task-mode")
			if err != nil {
				t.Fatal(err)
			}
			if result := p.Wait(); result.ExitCode != 0 {
				t.Fatalf("exit code = %d, want 0", result.ExitCode)
			}
			data, err := os.ReadFile(run.StdoutPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "auto\n" {
				t.Fatalf("worker printed GOOSE_MODE = %q, want %q", data, "auto\n")
			}
			if parentMode := os.Getenv("GOOSE_MODE"); parentMode != mode {
				t.Fatalf("parent GOOSE_MODE = %q, want %q", parentMode, mode)
			}
		})
	}
}

// TestStartLiveSnapshotSurvivesTruncation proves the live-parsed accessor
// reflects bytes written to stdout past the configured Limit: the disk
// capture truncates early, but the parser (fed via the same io.MultiWriter,
// independent of Limit) still sees and counts every line.
func TestStartLiveSnapshotSurvivesTruncation(t *testing.T) {
	const limit = 64 // small enough that big.yaml's ~900 bytes of output overflow it many times over.
	r, recipeRoot := newFakeGooseRunner(t, limit, "big.yaml")
	p, run, live, err := r.Start(Request{Recipe: filepath.Join(recipeRoot, "big.yaml")}, "task-2")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	result := p.Wait()
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}

	data, err := os.ReadFile(run.StdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != limit {
		t.Fatalf("stdout.raw len = %d, want exactly the %d-byte limit", len(data), limit)
	}

	live.Flush()
	snap := live.Snapshot()
	if snap.MessageEventCount != 20 {
		t.Fatalf("Snapshot().MessageEventCount = %d, want 20 (the tee must see every line regardless of the disk capture's Limit)", snap.MessageEventCount)
	}
	if !snap.Complete || snap.TotalTokens != 42 {
		t.Fatalf("Snapshot() = %+v, want Complete=true TotalTokens=42", snap)
	}
	wantText := ""
	for i := range 20 {
		wantText += fmt.Sprintf("chunk%03d ", i)
	}
	if snap.LastAssistantText != wantText {
		t.Fatalf("Snapshot().LastAssistantText = %q, want %q", snap.LastAssistantText, wantText)
	}

	written, dropped, truncated := live.Stats()
	if written != limit || dropped <= 0 || !truncated {
		t.Fatalf("Stats() = (%d, %d, %v), want (%d, >0, true)", written, dropped, truncated, limit)
	}
}
