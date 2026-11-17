package goose

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestBuildUsesC00CompatibleArgumentArray(t *testing.T) {
	root := t.TempDir()
	recipe := filepath.Join(root, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()
	l, e := (Config{RecipeRoot: root}).Build(Request{Recipe: recipe, RunDir: run})
	if e != nil {
		t.Fatal(e)
	}
	if l.Dir != run || slices.Contains(l.Args, "--path") || slices.Contains(l.Args, "--name") || !slices.Contains(l.Args, "--no-session") {
		t.Fatalf("%#v", l)
	}
}

// TestBuildDefaultNoSessionArgvIsUnchanged is a regression lock for
// spawn-optional-session acceptance criterion 1: a request that does not
// opt into any session mode (today's only mode — there are no session
// fields yet) must byte-for-byte produce today's exact argv, including
// --no-session at its current position. This test must keep passing
// unmodified after any future candidate-shape change to Config.Build adds
// a session opt-in field, since omitting that field is exactly this case.
func TestBuildDefaultNoSessionArgvIsUnchanged(t *testing.T) {
	root := t.TempDir()
	recipe := filepath.Join(root, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()

	l, e := (Config{RecipeRoot: root, MaxTurns: 25}).Build(Request{
		Recipe: recipe,
		RunDir: run,
	})
	if e != nil {
		t.Fatal(e)
	}

	wantRecipe, e := filepath.EvalSymlinks(recipe)
	if e != nil {
		t.Fatal(e)
	}
	wantArgs := []string{
		"run", "--recipe", wantRecipe, "--no-session", "--max-turns", "25",
		"--quiet", "--output-format", "stream-json",
	}
	if !reflect.DeepEqual(l.Args, wantArgs) {
		t.Fatalf("default no-session argv changed:\n got:  %#v\nwant: %#v", l.Args, wantArgs)
	}
	if l.Binary != "goose" {
		t.Fatalf("default binary changed: got %q, want %q", l.Binary, "goose")
	}
	wantRun, e := filepath.Abs(run)
	if e != nil {
		t.Fatal(e)
	}
	if l.Dir != wantRun {
		t.Fatalf("Dir changed: got %q, want %q", l.Dir, wantRun)
	}
	if len(l.Env) != 0 {
		t.Fatalf("default Env changed: got %#v, want empty", l.Env)
	}
}

// TestBuildAttachesMailboxExtensionOnlyWhenFullyConfigured is TMB-06's
// mailbox-on/mailbox-off regression lock: the extension flag and the
// MAILBOX_TOKEN env var only appear when Config's ExtensionBinary and
// MailboxSocket and the request's MailboxToken are all non-empty. Any one
// missing must leave argv/env exactly as the fully-disabled default.
func TestBuildAttachesMailboxExtensionOnlyWhenFullyConfigured(t *testing.T) {
	root := t.TempDir()
	recipe := filepath.Join(root, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()
	wantRecipe, e := filepath.EvalSymlinks(recipe)
	if e != nil {
		t.Fatal(e)
	}
	baseArgs := []string{"run", "--recipe", wantRecipe, "--no-session", "--max-turns", "25", "--quiet", "--output-format", "stream-json"}

	cases := []struct {
		name   string
		config Config
		token  string
	}{
		{"fully disabled", Config{RecipeRoot: root, MaxTurns: 25}, ""},
		{"extension+socket configured, no token", Config{RecipeRoot: root, MaxTurns: 25, ExtensionBinary: "/opt/mailbox-extension", MailboxSocket: "/run/mailbox.sock"}, ""},
		{"token present, extension/socket not configured", Config{RecipeRoot: root, MaxTurns: 25}, "tok-123"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := tc.config.Build(Request{Recipe: recipe, RunDir: run, MailboxToken: tc.token})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(l.Args, baseArgs) {
				t.Fatalf("argv changed when mailbox should stay disabled:\n got:  %#v\nwant: %#v", l.Args, baseArgs)
			}
			if len(l.Env) != 0 {
				t.Fatalf("Env changed when mailbox should stay disabled: got %#v", l.Env)
			}
		})
	}

	t.Run("fully configured attaches the extension and forwards the token", func(t *testing.T) {
		config := Config{RecipeRoot: root, MaxTurns: 25, ExtensionBinary: "/opt/mailbox-extension", MailboxSocket: "/run/mailbox.sock"}
		l, err := config.Build(Request{Recipe: recipe, RunDir: run, MailboxToken: "tok-123"})
		if err != nil {
			t.Fatal(err)
		}
		wantArgs := append(append([]string{}, baseArgs...), "--with-extension", "mailbox:/opt/mailbox-extension --socket /run/mailbox.sock")
		if !reflect.DeepEqual(l.Args, wantArgs) {
			t.Fatalf("mailbox-on argv:\n got:  %#v\nwant: %#v", l.Args, wantArgs)
		}
		wantEnv := []string{"MAILBOX_TOKEN=tok-123"}
		if !reflect.DeepEqual(l.Env, wantEnv) {
			t.Fatalf("mailbox-on env: got %#v, want %#v", l.Env, wantEnv)
		}
	})
}

func TestBuildMaxTurnsDefaultAndPerRequestOverride(t *testing.T) {
	root := t.TempDir()
	recipe := filepath.Join(root, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()

	l, e := (Config{RecipeRoot: root, MaxTurns: 25}).Build(Request{Recipe: recipe, RunDir: run})
	if e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(l.Args, "--max-turns") || !slices.Contains(l.Args, "25") {
		t.Fatalf("server default not applied: %#v", l.Args)
	}

	l, e = (Config{RecipeRoot: root, MaxTurns: 25}).Build(Request{Recipe: recipe, RunDir: run, MaxTurns: 3})
	if e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(l.Args, "3") || slices.Contains(l.Args, "25") {
		t.Fatalf("per-request override did not take effect: %#v", l.Args)
	}
}

// TestBuildClampsMaxTurnsToCeiling guards against finding #3: a client-
// supplied max_turns override must never exceed an operator-configured
// ceiling, regardless of how large the request asks for.
func TestBuildClampsMaxTurnsToCeiling(t *testing.T) {
	root := t.TempDir()
	recipe := filepath.Join(root, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()

	l, e := (Config{RecipeRoot: root, MaxTurns: 25, MaxTurnsCeiling: 200}).Build(Request{Recipe: recipe, RunDir: run, MaxTurns: 2000000000})
	if e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(l.Args, "200") || slices.Contains(l.Args, "2000000000") {
		t.Fatalf("client-supplied max_turns was not clamped to the ceiling: %#v", l.Args)
	}

	// A zero ceiling means "no ceiling enforced" (operator opt-out).
	l, e = (Config{RecipeRoot: root, MaxTurns: 25, MaxTurnsCeiling: 0}).Build(Request{Recipe: recipe, RunDir: run, MaxTurns: 2000000000})
	if e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(l.Args, "2000000000") {
		t.Fatalf("a zero ceiling unexpectedly clamped max_turns: %#v", l.Args)
	}
}

// TestBuildResolvesRelativeRecipeRootAndRecipeToAbsolutePath guards against a
// real end-to-end bug: approvedFile used to return whatever EvalSymlinks
// produced for the candidate, which stays relative for a relative input. The
// resulting --recipe argument would then be resolved by goose run against
// its own cwd (the task's isolated RunDir), not this process's cwd, breaking
// every spawn that used a relative --recipe-root/recipe combination.
func TestBuildResolvesRelativeRecipeRootAndRecipeToAbsolutePath(t *testing.T) {
	base := t.TempDir()
	recipeRoot := filepath.Join(base, "recipes")
	if e := os.Mkdir(recipeRoot, 0700); e != nil {
		t.Fatal(e)
	}
	recipe := filepath.Join(recipeRoot, "safe.yaml")
	if e := os.WriteFile(recipe, []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	run := t.TempDir()

	wd, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	if e := os.Chdir(base); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = os.Chdir(wd) }()

	l, e := (Config{RecipeRoot: "recipes"}).Build(Request{Recipe: "recipes/safe.yaml", RunDir: run})
	if e != nil {
		t.Fatal(e)
	}
	wantRecipe, e := filepath.EvalSymlinks(recipe)
	if e != nil {
		t.Fatal(e)
	}
	if !slices.Contains(l.Args, wantRecipe) {
		t.Fatalf("relative recipe-root/recipe did not resolve to an absolute path usable from a different cwd: %#v", l.Args)
	}
	for _, arg := range l.Args {
		if arg != "" && !filepath.IsAbs(arg) && strings.Contains(arg, "recipes") {
			t.Fatalf("recipe argument is still relative, which breaks under goose run's own cwd: %#v", l.Args)
		}
	}
}
func TestBuildRejectsUnsafeInputs(t *testing.T) {
	root := t.TempDir()
	run := t.TempDir()
	_, e := (Config{RecipeRoot: root}).Build(Request{Recipe: filepath.Join(root, "missing"), RunDir: run})
	if !errors.Is(e, ErrUnsafeRecipe) {
		t.Fatal(e)
	}
	// A shape-invalid parameter is rejected before the recipe path is even
	// resolved, so the reported error is the parameter one, not ErrUnsafeRecipe.
	_, e = (Config{RecipeRoot: root}).Build(Request{Recipe: "x", RunDir: run, Parameters: map[string]string{"bad key": "y"}})
	if !errors.Is(e, ErrInvalidParameters) {
		t.Fatal(e)
	}
}
func TestApprovedFileRejectsResolvedEscapes(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.yaml")
	if err := os.WriteFile(secret, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	escape := filepath.Join(root, "escape.yaml")
	if err := os.Symlink(secret, escape); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{secret, escape} {
		if _, err := approvedFile(root, candidate); !errors.Is(err, ErrUnsafeRecipe) {
			t.Errorf("approvedFile(%q, %q) error = %v, want ErrUnsafeRecipe", root, candidate, err)
		}
	}
}

func TestParseSummaryRetainsMalformedEventAsRawEvidence(t *testing.T) {
	if got := ParseSummary([]byte("not-json\n")); got != "raw stream bytes retained: 9" {
		t.Fatal(got)
	}
}

// FuzzApprovedFileNeverEscapesRoot feeds arbitrary candidate strings (the
// shape of client-controlled MCP spawn input) at a fixed allowlisted root
// containing one real file and one symlink that escapes it, asserting only
// that approvedFile never panics and never returns a path outside root.
// security-bug-audit: part of the intake's explicit "fuzzing of ...
// approvedFile" requirement.
//
// security-bug-audit finding #1 (.goose/workflows/security-bug-audit/validation.md):
// approvedFile's containment check currently accepts any candidate whose
// filepath.Rel from root is not exactly "." or "..", which is NOT the same
// as "outside root" (filepath.Rel("/a/root", "/a/other/x") returns
// "../other/x", not an error and not literal ".."). Both the symlink seed
// this fixture creates and a plain absolute path outside root must be
// rejected. The escaping symlink is seeded below so ordinary fuzz baseline
// coverage exercises the regression; run
// `go test ./internal/goose/... -fuzz=FuzzApprovedFileNeverEscapesRoot`
// to exercise it further.
func FuzzApprovedFileNeverEscapesRoot(f *testing.F) {
	root := f.TempDir()
	if err := os.WriteFile(filepath.Join(root, "safe.yaml"), []byte("x"), 0o600); err != nil {
		f.Fatal(err)
	}
	outside := f.TempDir()
	secret := filepath.Join(outside, "secret.yaml")
	if err := os.WriteFile(secret, []byte("x"), 0o600); err != nil {
		f.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(root, "escape.yaml")); err != nil {
		f.Fatal(err)
	}

	seeds := []string{
		"", ".", "..", "/", "safe.yaml",
		filepath.Join(root, "safe.yaml"),
		filepath.Join(root, "escape.yaml"),
		filepath.Join(root, "..", "escape"),
		filepath.Join(root, "missing.yaml"),
		"../../../../etc/passwd",
		string([]byte{0}),
		"safe.yaml\x00../../escape",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	cfg := Config{RecipeRoot: root}
	f.Fuzz(func(t *testing.T, candidate string) {
		resolved, err := approvedFile(cfg.RecipeRoot, candidate)
		if err != nil {
			return
		}
		absRoot, rootErr := filepath.EvalSymlinks(root)
		if rootErr != nil {
			t.Fatalf("root no longer resolves: %v", rootErr)
		}
		rel, relErr := filepath.Rel(absRoot, resolved)
		if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
			t.Fatalf("approvedFile(%q, %q) = %q, escapes root %q", cfg.RecipeRoot, candidate, resolved, absRoot)
		}
	})
}
