package goose

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaterializeFileParametersRejectsPreexistingSymlinkDestination(t *testing.T) {
	run := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(run, "inputs")); err != nil {
		t.Fatal(err)
	}
	_, err := materializeFileParameters(run, nil, map[string]string{"payload": "do-not-leak-this"})
	if err == nil {
		t.Fatal("materializer followed a pre-existing inputs symlink")
	}
	if strings.Contains(err.Error(), "do-not-leak-this") {
		t.Fatalf("error leaked content: %v", err)
	}
	if entries, readErr := os.ReadDir(outside); readErr != nil || len(entries) != 0 {
		t.Fatalf("outside target changed: %v, %v", entries, readErr)
	}
}

func TestMaterializeFileParametersUsesExclusiveFiles(t *testing.T) {
	run := t.TempDir()
	inputs := filepath.Join(run, "inputs")
	if err := os.Mkdir(inputs, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputs, "payload"), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeFileParameters(run, nil, map[string]string{"payload": "replacement"}); err == nil {
		t.Fatal("materializer overwrote an existing destination")
	}
	got, _ := os.ReadFile(filepath.Join(inputs, "payload"))
	if string(got) != "existing" {
		t.Fatalf("existing destination changed to %q", got)
	}
}
