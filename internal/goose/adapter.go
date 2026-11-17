// Package goose converts validated local requests into the C00-compatible CLI form.
package goose

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrUnsafeRecipe = errors.New("recipe is outside the allowlisted root")

type Launch struct {
	Binary string
	Args   []string
	Dir    string
	// Env holds extra environment variables (each "KEY=VALUE") to add on top
	// of the spawned process's inherited environment. Empty (the common
	// case) means "inherit only" — the mailbox token is the only current
	// user of this.
	Env []string
}
type Config struct {
	Binary, RecipeRoot string
	MaxTurns           int
	// MaxTurnsCeiling caps the effective MaxTurns (server default or a
	// request's own override) regardless of what a client requests. Zero
	// means no ceiling is enforced.
	MaxTurnsCeiling int
	// ExtensionBinary and MailboxSocket, when both non-empty, make Build
	// attach the mailbox extension via --with-extension for any request that
	// carries a MailboxToken. Either left empty keeps today's argv
	// byte-identical regardless of what a request carries (Selection 1, A-A).
	ExtensionBinary string
	MailboxSocket   string
	// Workdir, when non-empty, overrides the spawned process's working
	// directory, which otherwise defaults to the task's own isolated run
	// directory (RunDir). This only affects cwd: stdout/stderr capture paths
	// come from Artifacts.Create and are already absolute, so they are
	// unaffected either way. Empty (the common case) keeps today's exact
	// behavior.
	Workdir string
}
type Request struct {
	Recipe     string
	Parameters map[string]string
	RunDir     string
	// MaxTurns overrides Config.MaxTurns for this one request when positive.
	// Zero (the common case) means "use the server-wide default."
	MaxTurns int
	// MailboxToken, when non-empty alongside Config.ExtensionBinary and
	// Config.MailboxSocket, attaches the mailbox extension and forwards the
	// token via the spawned process's environment. Empty (the common case,
	// and the only possibility when the server has no Tokens registry)
	// leaves argv and env exactly as before this field existed.
	MailboxToken string
}

// Build uses an argument array. C00 proved that --path cannot be combined with
// --name/--no-session, so the isolated run directory is the child cwd instead.
func (c Config) Build(r Request) (Launch, error) {
	// Shape validation runs before any path resolution or stat, so a malformed
	// request cannot even probe the filesystem.
	if err := ValidateParameterShape(r.Parameters); err != nil {
		return Launch{}, err
	}
	if c.Binary == "" {
		c.Binary = "goose"
	}
	if r.MaxTurns > 0 {
		c.MaxTurns = r.MaxTurns
	}
	if c.MaxTurns < 1 {
		c.MaxTurns = 1
	}
	if c.MaxTurnsCeiling > 0 && c.MaxTurns > c.MaxTurnsCeiling {
		c.MaxTurns = c.MaxTurnsCeiling
	}
	if r.RunDir == "" {
		return Launch{}, errors.New("run directory is required")
	}
	recipe, err := approvedFile(c.RecipeRoot, r.Recipe)
	if err != nil {
		return Launch{}, err
	}
	// Declaration-aware validation needs the resolved allowlisted path, so it
	// runs here — still before any process starts. Shape validation above
	// already rejected malformed input without touching the filesystem.
	if len(r.Parameters) > 0 {
		if err := validateDeclaredParameters(recipe, r.Parameters); err != nil {
			return Launch{}, err
		}
	}
	runDir, err := filepath.Abs(r.RunDir)
	if err != nil {
		return Launch{}, err
	}
	info, err := os.Stat(runDir)
	if err != nil || !info.IsDir() {
		return Launch{}, errors.New("run directory is not a directory")
	}
	childDir := runDir
	if c.Workdir != "" {
		workdir, err := filepath.Abs(c.Workdir)
		if err != nil {
			return Launch{}, err
		}
		info, err := os.Stat(workdir)
		if err != nil || !info.IsDir() {
			return Launch{}, errors.New("workdir is not a directory")
		}
		childDir = workdir
	}
	args := []string{"run", "--recipe", recipe, "--no-session", "--max-turns", fmt.Sprint(c.MaxTurns), "--quiet", "--output-format", "stream-json"}
	args = append(args, parameterArgs(r.Parameters)...)
	launch := Launch{Binary: c.Binary, Dir: childDir, Args: args}
	// Both Config fields and the request's token must be present: a token
	// with no configured extension/socket (should not happen, but fail
	// closed rather than attach nothing yet still leak an unused env var)
	// and a configured extension with no token (mailbox disabled for this
	// server) both leave argv/env exactly as before this feature existed.
	if c.ExtensionBinary != "" && c.MailboxSocket != "" && r.MailboxToken != "" {
		launch.Args = append(launch.Args, "--with-extension", fmt.Sprintf("mailbox:%s --socket %s", c.ExtensionBinary, c.MailboxSocket))
		launch.Env = []string{"MAILBOX_TOKEN=" + r.MailboxToken}
	}
	return launch, nil
}
func approvedFile(root, candidate string) (string, error) {
	if root == "" || candidate == "" {
		return "", ErrUnsafeRecipe
	}
	// Absolute first: EvalSymlinks preserves relativity, and a relative
	// result here would later be resolved against goose run's cwd (the
	// task's isolated run directory), not this process's cwd — breaking
	// every relative --recipe-root/recipe combination at spawn time.
	root, err := filepath.Abs(root)
	if err != nil {
		return "", ErrUnsafeRecipe
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return "", ErrUnsafeRecipe
	}
	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return "", ErrUnsafeRecipe
	}
	candidate, err = filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", ErrUnsafeRecipe
	}
	rel, err := filepath.Rel(root, candidate)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", ErrUnsafeRecipe
	}
	info, err := os.Stat(candidate)
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrUnsafeRecipe
	}
	return candidate, nil
}

// ParseSummary intentionally makes no schema claim: raw stream evidence is
// retained verbatim and callers may present only this conservative count.
func ParseSummary(raw []byte) string { return fmt.Sprintf("raw stream bytes retained: %d", len(raw)) }
