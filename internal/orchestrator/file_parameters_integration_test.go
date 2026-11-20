package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

func TestRealLauncherMaterializesFileParametersAndPassesOnlyPaths(t *testing.T) {
	binary, recipeRoot, logPath := newArgvRecorder(t)
	launcher := newRealLauncher(t, binary, recipeRoot)
	task := startingTask(filepath.Join(recipeRoot, "declared.yaml"))
	const raw = "SENSITIVE-FILE-CONTENT\nsecond line"
	task.Parameters = map[string]string{"mode": "ordinary"}
	task.FileParameters = map[string]string{"feature_slug": raw}
	store := newMemStore(task)
	New(launcher, store).Run(context.Background(), store.task)
	if store.task.Status != tasks.Succeeded {
		t.Fatalf("status = %q; failure = %q", store.task.Status, store.task.Failure)
	}
	argv := recordedArgv(t, logPath)
	joined := strings.Join(argv, "\n")
	if strings.Contains(joined, raw) || strings.Contains(joined, "SENSITIVE-FILE-CONTENT") {
		t.Fatalf("raw content reached argv: %#v", argv)
	}
	var generated string
	for i, arg := range argv {
		if arg == "--params" && i+1 < len(argv) && strings.HasPrefix(argv[i+1], "feature_slug=") {
			generated = strings.TrimPrefix(argv[i+1], "feature_slug=")
		}
	}
	if generated == "" || !filepath.IsAbs(generated) {
		t.Fatalf("generated path = %q, argv = %#v", generated, argv)
	}
	runDir := filepath.Dir(filepath.Dir(generated))
	rel, err := filepath.Rel(runDir, generated)
	if err != nil || rel != filepath.Join("inputs", "feature_slug") {
		t.Fatalf("path %q not contained as inputs/feature_slug (rel %q, err %v)", generated, rel, err)
	}
	contents, err := os.ReadFile(generated)
	if err != nil || string(contents) != raw {
		t.Fatalf("materialized content = %q, %v", contents, err)
	}
	for path, want := range map[string]os.FileMode{filepath.Dir(generated): 0o700, generated: 0o600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("mode %s = %o, want %o", path, got, want)
		}
	}
}
