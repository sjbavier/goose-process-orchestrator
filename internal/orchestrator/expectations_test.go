package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// providerScript emits one assistant message carrying inference metadata; the
// recipe's base name selects the provider/model. No provider or network use.
const providerScript = `#!/bin/sh
recipe=""
while [ $# -gt 0 ]; do
  case "$1" in
    --recipe) recipe="$2"; shift 2 ;;
    *) shift ;;
  esac
done
case "$(basename "$recipe")" in
  match.yaml) p=claude-acp; m=claude-sonnet-5-5 ;;
  other.yaml) p=other-prov; m=claude-sonnet-5-5 ;;
  none.yaml) echo "ok"; exit 0 ;;
esac
printf '{"type":"message","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"hi"}],"metadata":{"inference":{"provider":"%s","requestedModel":"%s"}}}}\n' "$p" "$m"
exit 0
`

func providerFixture(t *testing.T) (launcherDir string, recipeRoot string, run func(task tasks.Task) *effStore) {
	t.Helper()
	binary, root := newFakeGoose(t, "match.yaml", "other.yaml", "none.yaml")
	if err := os.WriteFile(binary, []byte(providerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	launcher := newRealLauncher(t, binary, root)
	return root, root, func(task tasks.Task) *effStore {
		store := &effStore{memStore: newMemStore(task)}
		New(launcher, store).Run(context.Background(), task)
		return store
	}
}

// effStore adds the optional recorder methods tasks.Store provides.
type effStore struct {
	*memStore
	sha, provider, model string
}

func (s *effStore) RecordRecipeSHA256(_ context.Context, _, sha string) (tasks.Task, error) {
	s.sha = sha
	return s.task, nil
}
func (s *effStore) RecordEffectiveProviderModel(_ context.Context, _, p, m string) (tasks.Task, error) {
	s.provider, s.model = p, m
	return s.task, nil
}

func TestExpectationProviderModelMatchRecordsEffective(t *testing.T) {
	root, _, run := providerFixture(t)
	task := startingTask(filepath.Join(root, "match.yaml"))
	task.ExpectProvider, task.ExpectModel = "claude-acp", "claude-sonnet-5-5"
	s := run(task)
	if s.task.Status != tasks.Succeeded || s.task.Failure != "" {
		t.Fatalf("status=%q failure=%q, want succeeded", s.task.Status, s.task.Failure)
	}
	if s.provider != "claude-acp" || s.model != "claude-sonnet-5-5" || len(s.sha) != 64 {
		t.Fatalf("effective = %q/%q sha %q", s.provider, s.model, s.sha)
	}
}

func TestExpectationProviderMismatchFailsClearly(t *testing.T) {
	root, _, run := providerFixture(t)
	task := startingTask(filepath.Join(root, "other.yaml"))
	task.ExpectProvider = "claude-acp"
	s := run(task)
	if s.task.Status != tasks.Failed || !strings.HasPrefix(s.task.Failure, "provider_mismatch") {
		t.Fatalf("status=%q failure=%q, want failed provider_mismatch", s.task.Status, s.task.Failure)
	}
	task = startingTask(filepath.Join(root, "match.yaml"))
	task.ExpectModel = "some-other-model"
	s = run(task)
	if s.task.Status != tasks.Failed || !strings.HasPrefix(s.task.Failure, "model_mismatch") {
		t.Fatalf("status=%q failure=%q, want failed model_mismatch", s.task.Status, s.task.Failure)
	}
}

func TestExpectationUnverifiableIsNeverSilentlyPassed(t *testing.T) {
	root, _, run := providerFixture(t)
	task := startingTask(filepath.Join(root, "none.yaml"))
	task.ExpectProvider = "claude-acp"
	s := run(task)
	if s.task.Status != tasks.Failed || !strings.HasPrefix(s.task.Failure, "expectation_unverifiable") {
		t.Fatalf("status=%q failure=%q, want expectation_unverifiable", s.task.Status, s.task.Failure)
	}
}

func TestNoExpectationBehaviorUnchanged(t *testing.T) {
	root, _, run := providerFixture(t)
	s := run(startingTask(filepath.Join(root, "other.yaml")))
	if s.task.Status != tasks.Succeeded || s.task.Failure != "" || s.provider != "other-prov" {
		t.Fatalf("status=%q failure=%q provider=%q", s.task.Status, s.task.Failure, s.provider)
	}
}

func TestLaunchRehashMismatchFailsBeforeExec(t *testing.T) {
	root, _, run := providerFixture(t)
	task := startingTask(filepath.Join(root, "match.yaml"))
	task.ExpectRecipeSHA256 = strings.Repeat("0", 64)
	s := run(task)
	if s.task.Status != tasks.Failed || !strings.Contains(s.task.Failure, "recipe_sha256_mismatch") || s.task.ExitCode != nil {
		t.Fatalf("task = %+v, want launch failure recipe_sha256_mismatch with no exit code", s.task)
	}
}
