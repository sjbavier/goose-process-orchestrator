package goose

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// realRecipeBody mirrors the declaration shape this repository's own recipes
// use, so the loader is tested against the real format rather than an invented
// one. Only the parameters block matters to the loader; the other keys are
// present precisely to prove they are ignored.
const realRecipeBody = `version: 1.0.0
title: gen-example
description: A declared example.
parameters:
  - key: feature_slug
    input_type: string
    requirement: required
    description: Feature directory name.
  - key: mode
    input_type: string
    requirement: optional
    description: Optional mode.
settings:
  max_turns: 12
instructions: |
  Do the thing for {{ feature_slug }}.
prompt: |
  Run for {{ feature_slug }}.
`

func writeRecipe(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "recipe.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDeclarationsReadsOnlyTheParametersBlock(t *testing.T) {
	declarations, err := LoadDeclarations(writeRecipe(t, realRecipeBody))
	if err != nil {
		t.Fatalf("LoadDeclarations: %v", err)
	}
	if len(declarations) != 2 {
		t.Fatalf("declarations = %+v, want 2", declarations)
	}
	if declarations[0].Key != "feature_slug" || declarations[0].Requirement != "required" || declarations[0].InputType != "string" {
		t.Fatalf("first declaration = %+v", declarations[0])
	}
	if declarations[1].Key != "mode" || declarations[1].Requirement != "optional" {
		t.Fatalf("second declaration = %+v", declarations[1])
	}
}

func TestLoadDeclarationsFailsClosed(t *testing.T) {
	cases := map[string]string{
		"not a mapping":         "just a scalar\n",
		"parameters not a list": "parameters: nope\n",
		"declaration missing a key": `parameters:
  - input_type: string
    requirement: required
`,
		"duplicate declarations": `parameters:
  - key: dup
    input_type: string
  - key: dup
    input_type: string
`,
		"malformed yaml": "parameters:\n  - key: [unclosed\n",
	}
	for name, body := range cases {
		_, err := LoadDeclarations(writeRecipe(t, body))
		if err == nil {
			t.Errorf("%s: accepted, want fail closed", name)
			continue
		}
		if !errors.Is(err, ErrRecipeSchema) {
			t.Errorf("%s: error %v does not wrap ErrRecipeSchema", name, err)
		}
	}
	if _, err := LoadDeclarations(filepath.Join(t.TempDir(), "absent.yaml")); !errors.Is(err, ErrRecipeSchema) {
		t.Errorf("a missing recipe must fail closed, got %v", err)
	}
}

func TestLoadDeclarationsAcceptsARecipeWithNoParameters(t *testing.T) {
	declarations, err := LoadDeclarations(writeRecipe(t, "version: 1.0.0\ntitle: none\n"))
	if err != nil {
		t.Fatalf("LoadDeclarations: %v", err)
	}
	if len(declarations) != 0 {
		t.Fatalf("declarations = %+v, want none", declarations)
	}
}

func TestLoadDeclarationsRejectsAnOversizedRecipe(t *testing.T) {
	body := "parameters:\n" + strings.Repeat("# padding padding padding padding\n", (MaxRecipeBytes/34)+64)
	if len(body) <= MaxRecipeBytes {
		t.Fatalf("test fixture is only %d bytes, needs to exceed %d", len(body), MaxRecipeBytes)
	}
	_, err := LoadDeclarations(writeRecipe(t, body))
	if !errors.Is(err, ErrRecipeSchema) {
		t.Fatalf("oversized recipe = %v, want ErrRecipeSchema", err)
	}
}

// TestLoadDeclarationsBoundsAnAliasBomb guards the admission path against the
// classic YAML expansion attack at the worst case the size cap allows: a deep
// alias chain padded to just under MaxRecipeBytes. The decoder must bound the
// expansion rather than exhaust CPU or memory, because a recipe parse happens
// before a task is admitted.
func TestLoadDeclarationsBoundsAnAliasBomb(t *testing.T) {
	var b strings.Builder
	b.WriteString("a: &a [" + strings.Repeat(`"xxxxxxxxxxxxxxxx",`, 200) + `"x"]` + "\n")
	prev := "a"
	for _, name := range []string{"b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		b.WriteString(name + ": &" + name + " [" + strings.Repeat("*"+prev+",", 200) + "*" + prev + "]\n")
		prev = name
	}
	body := b.String()
	if len(body) < MaxRecipeBytes {
		body += strings.Repeat("# padding padding padding padding padding\n", (MaxRecipeBytes-len(body))/42)
	}
	if len(body) > MaxRecipeBytes {
		t.Fatalf("fixture is %d bytes, which the size cap would reject before decoding", len(body))
	}
	path := writeRecipe(t, body)
	start := time.Now()
	_, err := LoadDeclarations(path)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("decoding a %d byte alias bomb took %s, which is an admission-path hazard", len(body), elapsed)
	}
	if err != nil && !errors.Is(err, ErrRecipeSchema) {
		t.Fatalf("alias bomb error %v does not wrap ErrRecipeSchema", err)
	}
	t.Logf("bounded a %d byte alias bomb in %s (err = %v)", len(body), elapsed, err)
}

func TestValidateParametersEnforcesDeclarations(t *testing.T) {
	recipe := writeRecipe(t, realRecipeBody)
	if err := ValidateParameters(recipe, map[string]string{"feature_slug": "x", "mode": "local"}); err != nil {
		t.Fatalf("fully declared map rejected: %v", err)
	}
	if err := ValidateParameters(recipe, map[string]string{"feature_slug": "x"}); err != nil {
		t.Fatalf("omitting an optional parameter must be allowed: %v", err)
	}
	// PRM-01 observed Goose accepting an empty value for a required parameter,
	// so presence is what "required" means here, not non-emptiness.
	if err := ValidateParameters(recipe, map[string]string{"feature_slug": ""}); err != nil {
		t.Fatalf("an empty required value must be accepted: %v", err)
	}

	unknown := ValidateParameters(recipe, map[string]string{"feature_slug": "x", "nope": "y"})
	if !errors.Is(unknown, ErrInvalidParameters) {
		t.Fatalf("unknown key = %v, want ErrInvalidParameters", unknown)
	}
	if !strings.Contains(unknown.Error(), "feature_slug, mode") {
		t.Fatalf("unknown-key error should list the declared keys: %v", unknown)
	}

	missing := ValidateParameters(recipe, map[string]string{"mode": "local"})
	if !errors.Is(missing, ErrInvalidParameters) || !strings.Contains(missing.Error(), "feature_slug") {
		t.Fatalf("missing required key = %v, want a rejection naming feature_slug", missing)
	}

	// Shape rules still apply first.
	if err := ValidateParameters(recipe, map[string]string{"feature_slug": "a\nb"}); !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("shape violation = %v, want ErrInvalidParameters", err)
	}
}

func TestValidateParametersRejectsNonStringDeclaredTypes(t *testing.T) {
	recipe := writeRecipe(t, `parameters:
  - key: count
    input_type: number
    requirement: optional
`)
	err := ValidateParameters(recipe, map[string]string{"count": "3"})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("number-typed parameter = %v, want ErrInvalidParameters", err)
	}
	if !strings.Contains(err.Error(), "number") {
		t.Fatalf("error should name the declared type: %v", err)
	}
}

func TestBuildRejectsUndeclaredParametersBeforeLaunch(t *testing.T) {
	_, err := buildWithRecipe(t, realRecipeBody, map[string]string{"feature_slug": "x", "undeclared": "y"})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("Build = %v, want ErrInvalidParameters", err)
	}
	_, err = buildWithRecipe(t, realRecipeBody, map[string]string{"mode": "local"})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("Build with a missing required key = %v, want ErrInvalidParameters", err)
	}
	launch, err := buildWithRecipe(t, realRecipeBody, map[string]string{"feature_slug": "x", "mode": "local"})
	if err != nil {
		t.Fatalf("Build with a satisfied declaration: %v", err)
	}
	joined := strings.Join(launch.Args, " ")
	if !strings.Contains(joined, "--params feature_slug=x --params mode=local") {
		t.Fatalf("argv = %#v", launch.Args)
	}
}

// Direct Build callers preserve their historical behavior; spawn admission is
// the fail-fast boundary for prompt validation.
func TestBuildWithoutParametersDoesNotRequireAParsableRecipe(t *testing.T) {
	if _, err := buildWithRecipe(t, "just a scalar\n", nil); err != nil {
		t.Fatalf("a parameterless direct Build must preserve compatibility: %v", err)
	}
}

// FuzzLoadDeclarations feeds arbitrary bytes to the recipe schema loader. The
// loader sits on an admission path, so its invariants are: never panic, and
// never return declarations together with an error or an error that fails to
// wrap ErrRecipeSchema.
func FuzzLoadDeclarations(f *testing.F) {
	f.Add(realRecipeBody)
	f.Add("")
	f.Add("parameters: []\n")
	f.Add("parameters:\n  - key: k\n")
	f.Add("just a scalar\n")
	f.Add("parameters:\n  - key: [unclosed\n")
	f.Add("a: &a [*a]\n")
	f.Fuzz(func(t *testing.T, body string) {
		path := filepath.Join(t.TempDir(), "fuzz.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Skip()
		}
		declarations, err := LoadDeclarations(path)
		if err != nil {
			if !errors.Is(err, ErrRecipeSchema) {
				t.Fatalf("error %v does not wrap ErrRecipeSchema", err)
			}
			if declarations != nil {
				t.Fatalf("declarations %+v returned alongside error %v", declarations, err)
			}
			return
		}
		// Accepted declarations must be internally consistent: every key is
		// non-empty and unique, so validation cannot be fooled by a recipe.
		seen := make(map[string]bool, len(declarations))
		for _, declaration := range declarations {
			if declaration.Key == "" {
				t.Fatalf("accepted a declaration with an empty key: %+v", declarations)
			}
			if seen[declaration.Key] {
				t.Fatalf("accepted duplicate key %q: %+v", declaration.Key, declarations)
			}
			seen[declaration.Key] = true
		}
	})
}

func TestValidateRequestRejectsPromptlessRecipe(t *testing.T) {
	root := t.TempDir()
	withoutPrompt := filepath.Join(root, "without.yaml")
	if err := os.WriteFile(withoutPrompt, []byte("version: 1.0.0\ntitle: no-prompt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := Config{RecipeRoot: root}
	err := config.ValidateRequest(withoutPrompt, nil)
	if !errors.Is(err, ErrRecipeSchema) || !strings.Contains(err.Error(), "recipe has no prompt; headless goose run requires one") {
		t.Fatalf("ValidateRequest(prompt-less) = %v", err)
	}
	withPrompt := filepath.Join(root, "with.yaml")
	if err := os.WriteFile(withPrompt, []byte("version: 1.0.0\nprompt: |\n  Run this recipe.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := config.ValidateRequest(withPrompt, nil); err != nil {
		t.Fatalf("ValidateRequest(with prompt) = %v", err)
	}
}
