package goose

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateParameterShapeAcceptsTheApprovedContract(t *testing.T) {
	valid := map[string]string{
		"feature_slug": "spawn-recipe-parameters",
		"Mode":         "local-autonomous",
		"a":            "",
		"with-hyphen":  "x",
		// PRM-01 evidence: Goose renders an '=' inside a value unchanged.
		"pair": "left=right",
		// Spaces, quotes, and non-ASCII text are ordinary data in an argv
		// element; only control characters are excluded.
		"prose": `he said "hi there" — ok`,
	}
	if err := ValidateParameterShape(valid); err != nil {
		t.Fatalf("ValidateParameterShape(valid) = %v", err)
	}
	if err := ValidateParameterShape(nil); err != nil {
		t.Fatalf("ValidateParameterShape(nil) = %v", err)
	}
	atLimit := make(map[string]string, MaxParameters)
	for i := 0; i < MaxParameters; i++ {
		atLimit["k"+strings.Repeat("x", i)] = strings.Repeat("v", MaxParameterValueBytes)
	}
	if err := ValidateParameterShape(atLimit); err != nil {
		t.Fatalf("exactly at the limits must be accepted: %v", err)
	}
}

func TestValidateParameterShapeRejectsOutOfContractInput(t *testing.T) {
	tooMany := make(map[string]string, MaxParameters+1)
	for i := 0; i <= MaxParameters; i++ {
		tooMany["k"+strings.Repeat("x", i)] = "v"
	}
	cases := map[string]map[string]string{
		"too many parameters":   tooMany,
		"empty key":             {"": "v"},
		"key starting a digit":  {"1bad": "v"},
		"key with a space":      {"bad key": "v"},
		"key with an equals":    {"bad=key": "v"},
		"key with a dot":        {"bad.key": "v"},
		"key too long":          {"k" + strings.Repeat("x", MaxParameterKeyBytes): "v"},
		"non-ASCII key":         {"clé": "v"},
		"value too long":        {"k": strings.Repeat("v", MaxParameterValueBytes+1)},
		"newline in value":      {"k": "a\nb"},
		"carriage return":       {"k": "a\rb"},
		"tab in value":          {"k": "a\tb"},
		"NUL in value":          {"k": "a\x00b"},
		"escape in value":       {"k": "a\x1bb"},
		"next line U+0085":      {"k": "a\u0085b"},
		"line separator U+2028": {"k": "a b"},
		"para separator U+2029": {"k": "a b"},
		"invalid UTF-8":         {"k": "a\xffb"},
	}
	for name, params := range cases {
		err := ValidateParameterShape(params)
		if err == nil {
			t.Errorf("%s: accepted, want rejection", name)
			continue
		}
		if !errors.Is(err, ErrInvalidParameters) {
			t.Errorf("%s: error %v does not wrap ErrInvalidParameters", name, err)
		}
	}
}

func TestValidateParameterShapeReportsADeterministicFirstOffender(t *testing.T) {
	// Two bad keys in one map: the message must not depend on map order.
	params := map[string]string{"aaa bad": "v", "zzz bad": "v"}
	first := ValidateParameterShape(params)
	for i := 0; i < 50; i++ {
		if got := ValidateParameterShape(params); got.Error() != first.Error() {
			t.Fatalf("non-deterministic rejection: %v then %v", first, got)
		}
	}
	if !strings.Contains(first.Error(), "aaa bad") {
		t.Fatalf("error = %v, want the lowest-sorting offending key", first)
	}
}

// buildWithRecipe builds against a recipe whose body the caller controls.
func buildWithRecipe(t *testing.T, body string, params map[string]string) (Launch, error) {
	t.Helper()
	root := t.TempDir()
	run := t.TempDir()
	recipe := filepath.Join(root, "allowed.yaml")
	if err := os.WriteFile(recipe, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Config{Binary: "goose", RecipeRoot: root, MaxTurns: 3}.Build(Request{Recipe: recipe, RunDir: run, Parameters: params})
}

// buildWithParameters builds against a recipe that declares every supplied key
// as an optional string, isolating shape and argv behavior from the
// declaration rules that schema_test.go covers.
func buildWithParameters(t *testing.T, params map[string]string) (Launch, error) {
	t.Helper()
	body := "version: 1.0.0\nparameters:\n"
	for _, key := range sortedKeys(params) {
		body += "  - key: " + key + "\n    input_type: string\n    requirement: optional\n"
	}
	return buildWithRecipe(t, body, params)
}

func TestBuildAppendsDeterministicSortedParameterArgs(t *testing.T) {
	launch, err := buildWithParameters(t, map[string]string{"zebra": "last", "alpha": "first", "pair": "a=b"})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	args := launch.Args
	// The base flags must keep their proven order; parameters are appended.
	if args[0] != "run" || args[1] != "--recipe" {
		t.Fatalf("base argv changed: %#v", args)
	}
	tail := args[len(args)-6:]
	want := []string{"--params", "alpha=first", "--params", "pair=a=b", "--params", "zebra=last"}
	for i := range want {
		if tail[i] != want[i] {
			t.Fatalf("parameter argv = %#v, want %#v", tail, want)
		}
	}
	// Every value is its own argv element: nothing is shell-joined.
	for _, arg := range args {
		if strings.HasPrefix(arg, "--params") && arg != "--params" {
			t.Fatalf("argv element %q joined the flag and its value", arg)
		}
	}
}

func TestBuildWithoutParametersIsUnchanged(t *testing.T) {
	withNone, err := buildWithParameters(t, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	withEmpty, err := buildWithParameters(t, map[string]string{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, launch := range []Launch{withNone, withEmpty} {
		for _, arg := range launch.Args {
			if arg == "--params" {
				t.Fatalf("a parameterless request must not pass --params: %#v", launch.Args)
			}
		}
	}
	if len(withNone.Args) != len(withEmpty.Args) {
		t.Fatalf("nil and empty maps produced different argv: %#v vs %#v", withNone.Args, withEmpty.Args)
	}
}

func TestBuildRejectsInvalidParametersBeforeTouchingTheFilesystem(t *testing.T) {
	// RecipeRoot and RunDir are deliberately absent: if validation did not run
	// first, Build would fail with a path error instead of a parameter one.
	_, err := Config{Binary: "goose"}.Build(Request{Recipe: "x", Parameters: map[string]string{"k": "a\nb"}})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("Build = %v, want ErrInvalidParameters before any path handling", err)
	}
}

// FuzzValidateParameterShape asserts the validator's two safety invariants over
// arbitrary input: it never panics, and anything it accepts renders to argv
// elements that keep the flag and the key boundary intact.
func FuzzValidateParameterShape(f *testing.F) {
	f.Add("key", "value")
	f.Add("", "")
	f.Add("k", "a=b")
	f.Add("k", "a\nb")
	f.Add("bad key", "v")
	f.Add("k", "\xff")
	f.Fuzz(func(t *testing.T, key, value string) {
		params := map[string]string{key: value}
		if err := ValidateParameterShape(params); err != nil {
			if !errors.Is(err, ErrInvalidParameters) {
				t.Fatalf("rejection %v does not wrap ErrInvalidParameters", err)
			}
			return
		}
		args := parameterArgs(params)
		if len(args) != 2 || args[0] != "--params" {
			t.Fatalf("accepted %q=%q rendered as %#v", key, value, args)
		}
		pair := args[1]
		gotKey, gotValue, found := strings.Cut(pair, "=")
		if !found || gotKey != key || gotValue != value {
			t.Fatalf("accepted %q=%q did not round trip through %q", key, value, pair)
		}
	})
}
