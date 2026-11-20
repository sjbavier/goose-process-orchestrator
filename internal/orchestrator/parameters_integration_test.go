package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// argvRecordingScript stands in for the real `goose` CLI and records its exact
// argv, one element per line, so a test can prove what the child process
// actually received rather than what the adapter intended. It never calls a
// provider or network endpoint.
const argvRecordingScript = `#!/bin/sh
out="$ARGV_LOG"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$out"
done
exit 0
`

// declaredRecipe declares the two parameters these tests supply.
const declaredRecipe = `title: fixture
prompt: |
  Read the declared inputs.
parameters:
  - key: feature_slug
    input_type: string
    requirement: required
  - key: mode
    input_type: string
    requirement: optional
`

func newArgvRecorder(t *testing.T) (binary, recipeRoot, logPath string) {
	t.Helper()
	dir := t.TempDir()
	binary = filepath.Join(dir, "argvgoose.sh")
	if err := os.WriteFile(binary, []byte(argvRecordingScript), 0o755); err != nil {
		t.Fatal(err)
	}
	recipeRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(recipeRoot, "declared.yaml"), []byte(declaredRecipe), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath = filepath.Join(dir, "argv.log")
	t.Setenv("ARGV_LOG", logPath)
	return binary, recipeRoot, logPath
}

func recordedArgv(t *testing.T, logPath string) []string {
	t.Helper()
	contents, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("the fake goose binary recorded no argv: %v", err)
	}
	return strings.Split(strings.TrimRight(string(contents), "\n"), "\n")
}

// TestRealLauncherPassesParametersAsSeparateArgvElements proves end to end,
// through the exact chain cmd/orchestrator wires, that a persisted parameter
// map reaches the child process as discrete key-sorted argv elements. A value
// containing '=' and a value containing spaces and quotes both have to survive
// byte for byte, which is what distinguishes an argv array from a shell join.
func TestRealLauncherPassesParametersAsSeparateArgvElements(t *testing.T) {
	binary, recipeRoot, logPath := newArgvRecorder(t)
	launcher := newRealLauncher(t, binary, recipeRoot)

	task := startingTask(filepath.Join(recipeRoot, "declared.yaml"))
	task.Parameters = map[string]string{"mode": `a "quoted" phrase`, "feature_slug": "left=right"}
	store := newMemStore(task)
	New(launcher, store).Run(context.Background(), store.task)
	if store.task.Status != tasks.Succeeded {
		t.Fatalf("status = %q, want succeeded; failure = %q", store.task.Status, store.task.Failure)
	}

	argv := recordedArgv(t, logPath)
	var pairs []string
	for i, arg := range argv {
		if arg != "--params" {
			continue
		}
		if i+1 >= len(argv) {
			t.Fatalf("--params was the last argv element: %#v", argv)
		}
		pairs = append(pairs, argv[i+1])
	}
	want := []string{"feature_slug=left=right", `mode=a "quoted" phrase`}
	if len(pairs) != len(want) {
		t.Fatalf("recorded %d parameter pairs, want %d: %#v", len(pairs), len(want), argv)
	}
	for i := range want {
		if pairs[i] != want[i] {
			t.Fatalf("parameter pair %d = %q, want %q (full argv: %#v)", i, pairs[i], want[i], argv)
		}
	}
}

// TestRealLauncherStartsNoProcessForInvalidParameters proves the rejection is
// real at the launch boundary too: the fake binary writes its argv log on every
// invocation, so an absent log file is proof no child ever ran.
func TestRealLauncherStartsNoProcessForInvalidParameters(t *testing.T) {
	cases := map[string]map[string]string{
		"undeclared key":       {"feature_slug": "x", "nope": "y"},
		"missing required key": {"mode": "x"},
		"newline in value":     {"feature_slug": "a\nb"},
		"oversize value":       {"feature_slug": strings.Repeat("v", 4097)},
		"control character":    {"feature_slug": "a\x00b"},
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			binary, recipeRoot, logPath := newArgvRecorder(t)
			launcher := newRealLauncher(t, binary, recipeRoot)
			task := startingTask(filepath.Join(recipeRoot, "declared.yaml"))
			task.Parameters = params
			store := newMemStore(task)
			New(launcher, store).Run(context.Background(), store.task)
			if store.task.Status != tasks.Failed {
				t.Fatalf("status = %q, want failed", store.task.Status)
			}
			if _, err := os.Stat(logPath); err == nil {
				t.Fatalf("a child process ran for invalid parameters: %#v", recordedArgv(t, logPath))
			}
		})
	}
}

// TestRealLauncherWithoutParametersPassesNoParamsFlag pins that existing
// parameterless behavior is untouched.
func TestRealLauncherWithoutParametersPassesNoParamsFlag(t *testing.T) {
	binary, recipeRoot, logPath := newArgvRecorder(t)
	launcher := newRealLauncher(t, binary, recipeRoot)
	store := newMemStore(startingTask(filepath.Join(recipeRoot, "declared.yaml")))
	New(launcher, store).Run(context.Background(), store.task)
	if store.task.Status != tasks.Succeeded {
		t.Fatalf("status = %q, want succeeded", store.task.Status)
	}
	for _, arg := range recordedArgv(t, logPath) {
		if arg == "--params" {
			t.Fatalf("a parameterless task passed --params: %#v", recordedArgv(t, logPath))
		}
	}
}
