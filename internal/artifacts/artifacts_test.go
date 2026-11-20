package artifacts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunIsPrivateAndCaptureDrainsBounded(t *testing.T) {
	m := Manager{Root: t.TempDir(), Limit: 3}
	r, e := m.Create("task-1")
	if e != nil {
		t.Fatal(e)
	}
	if filepath.Dir(r.Dir) != m.Root {
		t.Fatal("run escaped root")
	}
	w, e := m.OpenCapture(r.StdoutPath)
	if e != nil {
		t.Fatal(e)
	}
	n, e := w.Write([]byte("abcdef"))
	if e != nil || n != 6 {
		t.Fatalf("drain %d %v", n, e)
	}
	w.Close()
	b, _ := os.ReadFile(r.StdoutPath)
	if string(b) != "abc" {
		t.Fatalf("%q", b)
	}
	if _, e := m.Create("../bad"); e == nil {
		t.Fatal("accepted unsafe id")
	}
	if _, e := r.Resolve("../x"); e == nil {
		t.Fatal("escaped artifact")
	}
	if mode, _ := os.Stat(r.Dir); mode.Mode().Perm() != 0700 {
		t.Fatal(mode.Mode())
	}
	_ = strings.Builder{}
}

func TestFindRunLocatesByPrefixWithoutStoredReference(t *testing.T) {
	m := Manager{Root: t.TempDir()}
	created, e := m.Create("task-7")
	if e != nil {
		t.Fatal(e)
	}
	found, e := m.FindRun("task-7")
	if e != nil {
		t.Fatal(e)
	}
	if found.Dir != created.Dir || found.StdoutPath != created.StdoutPath || found.StderrPath != created.StderrPath {
		t.Fatalf("FindRun = %+v, want %+v", found, created)
	}
}

func TestFindRunUnknownTaskIsNotFound(t *testing.T) {
	m := Manager{Root: t.TempDir()}
	if _, e := m.FindRun("never-created"); !errors.Is(e, ErrRunNotFound) {
		t.Fatalf("FindRun(unknown) = %v, want ErrRunNotFound", e)
	}
}

func TestFindRunRejectsUnsafeID(t *testing.T) {
	m := Manager{Root: t.TempDir()}
	if _, e := m.FindRun("../escape"); e == nil {
		t.Fatal("accepted unsafe id")
	}
}

func TestFindRunDoesNotFollowSymlink(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if e := os.Symlink(target, filepath.Join(root, "task-9-abc123")); e != nil {
		t.Fatal(e)
	}
	m := Manager{Root: root}
	if _, e := m.FindRun("task-9"); !errors.Is(e, ErrRunNotFound) {
		t.Fatalf("FindRun(symlinked) = %v, want ErrRunNotFound", e)
	}
}

// FuzzFindRunTaskID feeds arbitrary task-ID strings (the shape of the `id`
// field every logs/progress/status/result/cancel MCP tool takes as
// client-controlled input) at a fixed runs root holding one real run and
// one symlink planted to look like a run directory, asserting only that
// FindRun never panics and never returns a Run whose Dir escapes root or
// resolves through that symlink. security-bug-audit: part of the intake's
// explicit "fuzzing of MCP input handling" requirement.
func FuzzFindRunTaskID(f *testing.F) {
	root := f.TempDir()
	m := Manager{Root: root}
	if _, err := m.Create("real-task"); err != nil {
		f.Fatal(err)
	}
	outside := f.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "evil-task-x")); err != nil {
		f.Fatal(err)
	}

	for _, s := range []string{"", ".", "..", "/", "real-task", "evil-task", "../escape", string([]byte{0}), "real-task\x00../escape"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, taskID string) {
		run, err := m.FindRun(taskID)
		if err != nil {
			return
		}
		absRoot, rootErr := filepath.Abs(root)
		if rootErr != nil {
			t.Fatalf("root no longer resolves: %v", rootErr)
		}
		if filepath.Dir(run.Dir) != absRoot {
			t.Fatalf("FindRun(%q) = %q, escapes root %q", taskID, run.Dir, absRoot)
		}
		if info, statErr := os.Lstat(run.Dir); statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("FindRun(%q) returned a symlink entry: %q", taskID, run.Dir)
		}
	})
}

func TestCaptureStatsAcrossMultipleWrites(t *testing.T) {
	m := Manager{Root: t.TempDir(), Limit: 5}
	r, e := m.Create("task-stats")
	if e != nil {
		t.Fatal(e)
	}
	w, e := m.OpenCapture(r.StdoutPath)
	if e != nil {
		t.Fatal(e)
	}
	for _, chunk := range []string{"ab", "cd", "ef", "gh"} {
		if _, e := w.Write([]byte(chunk)); e != nil {
			t.Fatal(e)
		}
	}
	w.Close()
	written, dropped, truncated := w.Stats()
	if written != 5 || dropped != 3 || !truncated {
		t.Fatalf("Stats() = (%d, %d, %v), want (5, 3, true)", written, dropped, truncated)
	}
}

func TestCaptureStatsExactlyAtLimitIsNotTruncated(t *testing.T) {
	m := Manager{Root: t.TempDir(), Limit: 4}
	r, e := m.Create("task-exact")
	if e != nil {
		t.Fatal(e)
	}
	w, e := m.OpenCapture(r.StdoutPath)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := w.Write([]byte("abcd")); e != nil {
		t.Fatal(e)
	}
	w.Close()
	written, dropped, truncated := w.Stats()
	if written != 4 || dropped != 0 || truncated {
		t.Fatalf("Stats() = (%d, %d, %v), want (4, 0, false)", written, dropped, truncated)
	}
}

func TestDefaultOutputLimitAppliesToZeroValueManager(t *testing.T) {
	if DefaultOutputLimit != 8*1024*1024 {
		t.Fatalf("DefaultOutputLimit = %d, want 8 MiB", DefaultOutputLimit)
	}
	m := Manager{Root: t.TempDir()}
	r, e := m.Create("task-default")
	if e != nil {
		t.Fatal(e)
	}
	w, e := m.OpenCapture(r.StdoutPath)
	if e != nil {
		t.Fatal(e)
	}
	payload := make([]byte, DefaultOutputLimit+1)
	if _, e := w.Write(payload); e != nil {
		t.Fatal(e)
	}
	w.Close()
	written, dropped, truncated := w.Stats()
	if written != DefaultOutputLimit || dropped != 1 || !truncated {
		t.Fatalf("Stats() = (%d, %d, %v), want (%d, 1, true)", written, dropped, truncated, DefaultOutputLimit)
	}
}

func TestFindRunDoesNotConfusePrefixedTaskIDs(t *testing.T) {
	m := Manager{Root: t.TempDir()}
	if _, e := m.Create("task-1"); e != nil {
		t.Fatal(e)
	}
	if _, e := m.Create("task-10"); e != nil {
		t.Fatal(e)
	}
	run1, e := m.FindRun("task-1")
	if e != nil {
		t.Fatal(e)
	}
	run10, e := m.FindRun("task-10")
	if e != nil {
		t.Fatal(e)
	}
	if run1.Dir == run10.Dir {
		t.Fatalf("task-1 and task-10 resolved to the same run dir %q", run1.Dir)
	}
}
