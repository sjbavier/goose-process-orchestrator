package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// ShortSocketPath returns a socket path in a unique directory under /tmp,
// so even with macOS's default TMPDIR it fits within sun_path.
func ShortSocketPath(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gpo-test-")
	if err != nil {
		t.Fatalf("create socket directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove socket directory: %v", err)
		}
	})
	return filepath.Join(dir, name)
}
