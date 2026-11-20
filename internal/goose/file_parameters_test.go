package goose

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateRequestParameterShape(t *testing.T) {
	if err := ValidateRequestParameterShape(map[string]string{"plain": "x"}, map[string]string{"payload": "one\ntwo\r\n\tthree"}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name          string
		scalar, files map[string]string
	}{
		{"collision", map[string]string{"same": "x"}, map[string]string{"same": "y"}},
		{"traversal", nil, map[string]string{"../bad": "x"}},
		{"nul", nil, map[string]string{"file": "a\x00b"}},
		{"escape", nil, map[string]string{"file": "a\x1bb"}},
		{"invalid utf8", nil, map[string]string{"file": "a\xff"}},
		{"per file", nil, map[string]string{"file": strings.Repeat("x", MaxFileParameterValueBytes+1)}},
	}
	for _, tc := range cases {
		if err := ValidateRequestParameterShape(tc.scalar, tc.files); !errors.Is(err, ErrInvalidParameters) {
			t.Errorf("%s = %v", tc.name, err)
		}
	}
	many := map[string]string{}
	for i := 0; i < MaxParameters; i++ {
		many["k"+strings.Repeat("x", i)] = ""
	}
	if err := ValidateRequestParameterShape(map[string]string{"scalar": "x"}, many); !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("combined count = %v", err)
	}
	total := map[string]string{"a": strings.Repeat("a", MaxFileParameterValueBytes), "b": strings.Repeat("b", MaxFileParameterValueBytes), "c": strings.Repeat("c", MaxFileParameterValueBytes), "d": strings.Repeat("d", MaxFileParameterValueBytes), "e": "x"}
	if err := ValidateRequestParameterShape(nil, total); !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("aggregate = %v", err)
	}
}

func TestBuildRejectsUnmaterializedFileParameters(t *testing.T) {
	_, err := (Config{}).Build(Request{Recipe: "unused", FileParameters: map[string]string{"payload": "raw"}})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("Build raw file parameters = %v", err)
	}
}

func TestValidateRequestWithFilesRejectsNonStringDeclaration(t *testing.T) {
	recipe := writeRecipe(t, "prompt: run\nparameters:\n  - key: payload\n    input_type: number\n")
	err := (Config{RecipeRoot: filepath.Dir(recipe)}).ValidateRequestWithFiles(recipe, nil, map[string]string{"payload": "12"})
	if !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("non-string declaration = %v", err)
	}
}

func TestValidateRequestWithFilesUsesUnionDeclarations(t *testing.T) {
	recipe := writeRecipe(t, realRecipeBody)
	config := Config{RecipeRoot: filepath.Dir(recipe)}
	if err := config.ValidateRequestWithFiles(recipe, map[string]string{"mode": "local"}, map[string]string{"feature_slug": "multi\nline"}); err != nil {
		t.Fatalf("valid union = %v", err)
	}
	if err := config.ValidateRequestWithFiles(recipe, nil, map[string]string{"nope": "x", "feature_slug": "x"}); !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("unknown = %v", err)
	}
	if err := config.ValidateRequestWithFiles(recipe, nil, map[string]string{"mode": "x"}); !errors.Is(err, ErrInvalidParameters) {
		t.Fatalf("missing required = %v", err)
	}
}
