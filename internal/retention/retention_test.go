package retention

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/artifacts"
	"github.com/local/goose-process-orchestrator/internal/tasks"
)

var now = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

// fakeLookup answers task lookups without a database. A task ID mapped to a
// non-nil err returns that error; an unmapped ID returns tasks.ErrNotFound,
// matching a real store's answer for a row that does not exist.
type fakeLookup struct {
	tasks map[string]tasks.Task
	errs  map[string]error
}

func (f fakeLookup) Get(_ context.Context, id string) (tasks.Task, error) {
	if err := f.errs[id]; err != nil {
		return tasks.Task{}, err
	}
	task, ok := f.tasks[id]
	if !ok {
		return tasks.Task{}, tasks.ErrNotFound
	}
	return task, nil
}

// terminal builds a terminal task that ended endedAgo before the test clock.
func terminal(id string, status tasks.Status, endedAgo time.Duration) tasks.Task {
	ended := now.Add(-endedAgo)
	return tasks.Task{ID: id, Status: status, UpdatedAt: ended, EndedAt: &ended}
}

// makeRun creates a run directory the way artifacts.Manager.Create does, so
// the sweeper is tested against the real on-disk naming convention rather
// than a hand-written imitation of it.
func makeRun(t *testing.T, root, taskID string) string {
	t.Helper()
	run, err := artifacts.Manager{Root: root}.Create(taskID)
	if err != nil {
		t.Fatalf("create run for %s: %v", taskID, err)
	}
	if err := os.WriteFile(run.StdoutPath, []byte("captured output\n"), 0600); err != nil {
		t.Fatalf("write stdout for %s: %v", taskID, err)
	}
	return run.Dir
}

func sweep(t *testing.T, root string, policy Policy, lookup Lookup) Result {
	t.Helper()
	result, err := Sweeper{Root: root, Policy: policy, Lookup: lookup, Now: func() time.Time { return now }}.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	return result
}

func assertExists(t *testing.T, dir string, want bool) {
	t.Helper()
	_, err := os.Lstat(dir)
	if want && err != nil {
		t.Fatalf("expected %s to be retained, but: %v", dir, err)
	}
	if !want && err == nil {
		t.Fatalf("expected %s to be removed, but it still exists", dir)
	}
}

func TestPolicyEnabled(t *testing.T) {
	cases := []struct {
		name   string
		policy Policy
		want   bool
	}{
		{"zero value keeps everything", Policy{}, false},
		{"negative values are not limits", Policy{MaxAge: -time.Hour, MaxCount: -1}, false},
		{"age only", Policy{MaxAge: time.Hour}, true},
		{"count only", Policy{MaxCount: 1}, true},
		{"both", Policy{MaxAge: time.Hour, MaxCount: 1}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.policy.Enabled(); got != c.want {
				t.Fatalf("Enabled() = %v, want %v", got, c.want)
			}
		})
	}
}

// A disabled policy is the default, so it must never delete anything.
func TestSweepDisabledPolicyRemovesNothing(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 1000*time.Hour)}}

	result := sweep(t, root, Policy{}, lookup)

	if result.Removed != 0 || result.Scanned != 0 {
		t.Fatalf("disabled policy did work: %+v", result)
	}
	assertExists(t, dir, true)
}

// A nil Lookup leaves no authority on task state, so nothing is provably
// safe to delete.
func TestSweepWithoutLookupRemovesNothing(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")

	result, err := Sweeper{Root: root, Policy: Policy{MaxAge: time.Minute}, Now: func() time.Time { return now }}.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Removed != 0 {
		t.Fatalf("nil lookup removed %d directories", result.Removed)
	}
	assertExists(t, dir, true)
}

func TestSweepMissingRootIsNotAnError(t *testing.T) {
	result, err := Sweeper{Root: filepath.Join(t.TempDir(), "never-created"), Policy: Policy{MaxAge: time.Minute}, Lookup: fakeLookup{}}.Sweep(context.Background())
	if err != nil {
		t.Fatalf("missing root: %v", err)
	}
	if result.Scanned != 0 || result.Removed != 0 {
		t.Fatalf("missing root produced work: %+v", result)
	}
}

func TestSweepAgeLimit(t *testing.T) {
	root := t.TempDir()
	old := makeRun(t, root, "task-old")
	recent := makeRun(t, root, "task-recent")
	lookup := fakeLookup{tasks: map[string]tasks.Task{
		"task-old":    terminal("task-old", tasks.Succeeded, 48*time.Hour),
		"task-recent": terminal("task-recent", tasks.Failed, 30*time.Minute),
	}}

	result := sweep(t, root, Policy{MaxAge: 24 * time.Hour}, lookup)

	assertExists(t, old, false)
	assertExists(t, recent, true)
	if result.Removed != 1 || result.Kept != 1 || result.Scanned != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(result.RemovedDirs) != 1 || !isRunOf(result.RemovedDirs[0], "task-old") {
		t.Fatalf("RemovedDirs = %v, want one entry for task-old", result.RemovedDirs)
	}
}

// Age is measured from the task's EndedAt, not the directory's mtime: a
// just-created directory for a task that ended long ago is still expired.
func TestSweepAgeUsesTaskEndedAtNotDirectoryMtime(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 72*time.Hour)}}

	sweep(t, root, Policy{MaxAge: time.Hour}, lookup)

	assertExists(t, dir, false)
}

// Exactly at the boundary the directory is retained: removal requires the
// age to be strictly past the limit.
func TestSweepAgeBoundaryRetains(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 24*time.Hour)}}

	sweep(t, root, Policy{MaxAge: 24 * time.Hour}, lookup)

	assertExists(t, dir, true)
}

func TestSweepCountLimitRemovesOldestFirst(t *testing.T) {
	root := t.TempDir()
	dirs := map[string]string{}
	taskMap := map[string]tasks.Task{}
	for i, age := range []time.Duration{time.Hour, 2 * time.Hour, 3 * time.Hour, 4 * time.Hour} {
		id := []string{"task-a", "task-b", "task-c", "task-d"}[i]
		dirs[id] = makeRun(t, root, id)
		taskMap[id] = terminal(id, tasks.Succeeded, age)
	}

	result := sweep(t, root, Policy{MaxCount: 2}, fakeLookup{tasks: taskMap})

	assertExists(t, dirs["task-a"], true)  // newest
	assertExists(t, dirs["task-b"], true)  // second newest
	assertExists(t, dirs["task-c"], false) // oldest two go
	assertExists(t, dirs["task-d"], false)
	if result.Removed != 2 || result.Kept != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

// Each status that is not terminal must survive both limits: a live process
// may still be writing there, and interrupted/unknown record that the
// process state could not be established at all.
func TestSweepNeverRemovesNonTerminalTasks(t *testing.T) {
	for _, status := range []tasks.Status{tasks.Queued, tasks.Starting, tasks.Running, tasks.Interrupted, tasks.Unknown} {
		t.Run(string(status), func(t *testing.T) {
			root := t.TempDir()
			dir := makeRun(t, root, "task-live")
			// Old enough and over count, so only the status protects it.
			lookup := fakeLookup{tasks: map[string]tasks.Task{
				"task-live": {ID: "task-live", Status: status, UpdatedAt: now.Add(-1000 * time.Hour)},
			}}

			result := sweep(t, root, Policy{MaxAge: time.Minute, MaxCount: 1}, lookup)

			assertExists(t, dir, true)
			if result.Removed != 0 {
				t.Fatalf("removed a %s task's run directory: %+v", status, result)
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != SkipNotTerminal {
				t.Fatalf("skips = %+v, want one SkipNotTerminal", result.Skips)
			}
		})
	}
}

// A count limit must not be satisfied by deleting a protected directory:
// bounding disk use never outranks keeping a running task's evidence.
func TestSweepCountLimitYieldsToNonTerminalTasks(t *testing.T) {
	root := t.TempDir()
	live1 := makeRun(t, root, "task-live1")
	live2 := makeRun(t, root, "task-live2")
	done := makeRun(t, root, "task-done")
	lookup := fakeLookup{tasks: map[string]tasks.Task{
		"task-live1": {ID: "task-live1", Status: tasks.Running, UpdatedAt: now},
		"task-live2": {ID: "task-live2", Status: tasks.Running, UpdatedAt: now},
		"task-done":  terminal("task-done", tasks.Succeeded, time.Hour),
	}}

	sweep(t, root, Policy{MaxCount: 1}, lookup)

	assertExists(t, live1, true)
	assertExists(t, live2, true)
	assertExists(t, done, false) // the only eligible one
}

// A store error means the task's state is unknown, which must degrade to "no
// deletion", never to "delete everything".
func TestSweepRetainsOnLookupError(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{errs: map[string]error{"task-1": errors.New("database is locked")}}

	result := sweep(t, root, Policy{MaxAge: time.Minute, MaxCount: 0}, lookup)

	assertExists(t, dir, true)
	if result.Removed != 0 {
		t.Fatalf("removed despite lookup error: %+v", result)
	}
	if len(result.Skips) != 1 || result.Skips[0].Reason != SkipLookupFailed || result.Skips[0].Err == nil {
		t.Fatalf("skips = %+v, want one SkipLookupFailed carrying its error", result.Skips)
	}
}

// An orphan (no task row) ages out but is never count-swept: without a row
// there is no trustworthy ordering key for deciding which orphans are newest.
func TestSweepOrphanAgesOutButIsNotCountSwept(t *testing.T) {
	t.Run("age removes it", func(t *testing.T) {
		root := t.TempDir()
		dir := makeRun(t, root, "task-orphan")
		stale := now.Add(-100 * time.Hour)
		if err := os.Chtimes(dir, stale, stale); err != nil {
			t.Fatalf("chtimes: %v", err)
		}

		sweep(t, root, Policy{MaxAge: time.Hour}, fakeLookup{})

		assertExists(t, dir, false)
	})
	t.Run("count does not", func(t *testing.T) {
		root := t.TempDir()
		older := makeRun(t, root, "task-orphan1")
		newer := makeRun(t, root, "task-orphan2")
		stale := now.Add(-100 * time.Hour)
		for _, dir := range []string{older, newer} {
			if err := os.Chtimes(dir, stale, stale); err != nil {
				t.Fatalf("chtimes: %v", err)
			}
		}

		sweep(t, root, Policy{MaxCount: 1}, fakeLookup{})

		assertExists(t, older, true)
		assertExists(t, newer, true)
	})
}

// A symlink in the runs root is never a sweep candidate, however it
// resolves, and its target is untouched. This is the containment regression
// test: a sweeper that used os.Stat instead of DirEntry.IsDir, or that
// followed the link before removing, would delete outside --runs-dir.
func TestSweepNeverFollowsSymlinkOutOfRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "runs")
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(filepath.Join(outside, "precious"), 0700); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	keepFile := filepath.Join(outside, "precious", "keep.txt")
	if err := os.WriteFile(keepFile, []byte("do not delete"), 0600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	// Named exactly like a run directory for a long-finished task, so only
	// the symlink check protects the target.
	link := filepath.Join(root, "task-evil-123456")
	if err := os.Symlink(filepath.Join(outside, "precious"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-evil": terminal("task-evil", tasks.Succeeded, 1000*time.Hour)}}

	result := sweep(t, root, Policy{MaxAge: time.Minute, MaxCount: 0}, lookup)

	assertExists(t, keepFile, true)
	assertExists(t, link, true)
	if result.Removed != 0 {
		t.Fatalf("removed a symlink entry: %+v", result)
	}
	if len(result.Skips) != 1 || result.Skips[0].Reason != SkipNotRunDir {
		t.Fatalf("skips = %+v, want one SkipNotRunDir", result.Skips)
	}
}

// A symlink inside a doomed run directory must not have its target deleted:
// os.RemoveAll unlinks rather than follows.
func TestSweepDoesNotFollowSymlinkInsideRemovedRunDir(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "runs")
	outside := filepath.Join(base, "outside.txt")
	if err := os.WriteFile(outside, []byte("do not delete"), 0600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	dir := makeRun(t, root, "task-1")
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 100*time.Hour)}}

	sweep(t, root, Policy{MaxAge: time.Hour}, lookup)

	assertExists(t, dir, false)
	assertExists(t, outside, true)
}

// Entries that are not run directories are counted as scanned, reported, and
// left strictly alone — including a plain file and a directory whose name has
// no random suffix (which could be an operator's own notes directory).
func TestSweepLeavesNonRunEntriesAlone(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	file := filepath.Join(root, "task-1-99999")
	if err := os.WriteFile(file, []byte("not a directory"), 0600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	noSuffix := filepath.Join(root, "notes")
	if err := os.MkdirAll(noSuffix, 0700); err != nil {
		t.Fatalf("mkdir notes: %v", err)
	}
	trailing := filepath.Join(root, "task-2-")
	if err := os.MkdirAll(trailing, 0700); err != nil {
		t.Fatalf("mkdir trailing: %v", err)
	}
	lookup := fakeLookup{tasks: map[string]tasks.Task{
		"task-1": terminal("task-1", tasks.Succeeded, 1000*time.Hour),
		"task-2": terminal("task-2", tasks.Succeeded, 1000*time.Hour),
	}}

	result := sweep(t, root, Policy{MaxAge: time.Minute, MaxCount: 1}, lookup)

	for _, path := range []string{file, noSuffix, trailing} {
		assertExists(t, path, true)
	}
	if result.Removed != 0 || result.Scanned != 3 || result.Kept != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

// The runs root itself is never the thing removed, even if a limit would
// otherwise apply to everything in it.
func TestSweepNeverRemovesRootItself(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 1000*time.Hour)}}

	sweep(t, root, Policy{MaxAge: time.Minute, MaxCount: 0}, lookup)

	assertExists(t, dir, false)
	assertExists(t, root, true)
}

func TestTaskIDFromRunDir(t *testing.T) {
	cases := []struct {
		name   string
		want   string
		wantOK bool
	}{
		{"task-1762819200000000000-123456789", "task-1762819200000000000", true},
		{"a-1", "a", true},
		{"no-separator-suffix-", "", false},
		{"nodashatall", "", false},
		{"-123456", "", false},
		{"..-123456", "", true}, // task ID "..", rejected below
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := taskIDFromRunDir(c.name)
			if c.name == "..-123456" {
				// A ".." task ID could never have been created (see
				// artifacts.validSegment) and must not be derivable either.
				if ok {
					t.Fatalf("taskIDFromRunDir(%q) = %q, want rejection", c.name, got)
				}
				return
			}
			if ok != c.wantOK || got != c.want {
				t.Fatalf("taskIDFromRunDir(%q) = (%q, %v), want (%q, %v)", c.name, got, ok, c.want, c.wantOK)
			}
		})
	}
}

func TestContainedIn(t *testing.T) {
	cases := []struct {
		root, path string
		want       bool
	}{
		{"/runs", "/runs/task-1-2", true},
		{"/runs", "/runs", false},
		{"/runs", "/runs/../outside", false},
		{"/runs", "/runsother/task-1-2", false},
		{"/runs", "/outside", false},
		{"/runs", "/runs/deep/task-1-2", true},
	}
	for _, c := range cases {
		if got := containedIn(c.root, c.path); got != c.want {
			t.Fatalf("containedIn(%q, %q) = %v, want %v", c.root, c.path, got, c.want)
		}
	}
}

// removeRunDir is the last line of defense, so it is tested directly against
// the paths that must never be removed.
func TestRemoveRunDirRefusesUnsafePaths(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "runs")
	nested := filepath.Join(root, "task-1-123", "deeper")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	outside := filepath.Join(base, "outside")
	if err := os.MkdirAll(outside, 0700); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	link := filepath.Join(root, "task-2-123")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	for name, path := range map[string]string{
		"the root itself":      root,
		"outside the root":     outside,
		"deeper than a child":  nested,
		"a symlink to a child": link,
	} {
		t.Run(name, func(t *testing.T) {
			if err := removeRunDir(root, path); !errors.Is(err, errUnsafePath) {
				t.Fatalf("removeRunDir(%q) error = %v, want errUnsafePath", path, err)
			}
			assertExists(t, path, true)
		})
	}
}

// Loop sweeps immediately so that restarting the orchestrator applies a new
// policy without waiting out an interval, and returns when its context ends.
func TestLoopSweepsImmediatelyThenStops(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 100*time.Hour)}}
	sweeper := Sweeper{Root: root, Policy: Policy{MaxAge: time.Hour}, Lookup: lookup, Now: func() time.Time { return now }}

	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan Result, 4)
	done := make(chan struct{})
	go func() {
		Loop(ctx, sweeper, time.Hour, func(r Result, err error) {
			if err != nil {
				t.Errorf("sweep error: %v", err)
			}
			results <- r
		})
		close(done)
	}()

	select {
	case first := <-results:
		if first.Removed != 1 {
			t.Fatalf("immediate sweep removed %d, want 1", first.Removed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Loop did not sweep immediately")
	}
	assertExists(t, dir, false)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Loop did not return after context cancellation")
	}
}

// A disabled policy or a non-positive interval must not start a loop at all.
func TestLoopDoesNothingWhenNotConfigured(t *testing.T) {
	root := t.TempDir()
	dir := makeRun(t, root, "task-1")
	lookup := fakeLookup{tasks: map[string]tasks.Task{"task-1": terminal("task-1", tasks.Succeeded, 100*time.Hour)}}

	for name, call := range map[string]func(){
		"disabled policy": func() {
			Loop(context.Background(), Sweeper{Root: root, Lookup: lookup}, time.Hour, nil)
		},
		"non-positive interval": func() {
			Loop(context.Background(), Sweeper{Root: root, Policy: Policy{MaxAge: time.Minute}, Lookup: lookup}, 0, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() { call(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Loop did not return immediately")
			}
			assertExists(t, dir, true)
		})
	}
}

func isRunOf(dirName, taskID string) bool {
	got, ok := taskIDFromRunDir(dirName)
	return ok && got == taskID
}
