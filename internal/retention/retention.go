// Package retention bounds how long a task's captured run directory is kept
// below the orchestrator's runs root.
//
// Nothing here is enabled by default: a zero Policy keeps every run directory
// forever, which is exactly the behavior this project had before retention
// existed. Deletion only ever happens for a directory whose owning task the
// task store affirmatively reports as terminal, and only ever for a direct
// child of the runs root. Every uncertain case — a store lookup that failed, a
// non-terminal or unknown-state task, an entry that is not really a directory,
// a symlink — is kept, not deleted. "Fail safe" here means "retain".
package retention

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// Policy bounds retained run directories. A zero value is "keep everything
// forever" and is the default; each limit is independently optional.
type Policy struct {
	// MaxAge removes a terminal task's run directory once this much time has
	// passed since the task ended. Zero or negative means no age limit.
	MaxAge time.Duration
	// MaxCount is the target total number of run directories to retain,
	// newest first. Zero or negative means no count limit. Non-terminal
	// directories are counted but never removed to satisfy it, so a burst of
	// live tasks can legitimately hold the total above MaxCount.
	MaxCount int
}

// Enabled reports whether any limit is configured. A disabled policy must
// never be swept: callers skip starting a Loop at all.
func (p Policy) Enabled() bool { return p.MaxAge > 0 || p.MaxCount > 0 }

// Lookup resolves a run directory's owning task. *tasks.Store satisfies it.
// A tasks.ErrNotFound answer means the directory is an orphan (its row is
// gone, e.g. the database was recreated); any other error means the sweeper
// could not establish the task's state and must retain the directory.
type Lookup interface {
	Get(ctx context.Context, id string) (tasks.Task, error)
}

// SkipReason explains why a scanned entry was retained.
type SkipReason string

const (
	// SkipNotRunDir covers an entry that is not a plain directory named like
	// a run directory: a file, a symlink (however it resolves), or a name
	// without the "<task-id>-<random>" shape.
	SkipNotRunDir SkipReason = "not a run directory"
	// SkipNotTerminal covers a task that is queued, starting, running,
	// interrupted, or unknown. Its process may still be writing here.
	SkipNotTerminal SkipReason = "task is not terminal"
	// SkipLookupFailed covers a store error. The task's state could not be
	// established, so the directory is retained.
	SkipLookupFailed SkipReason = "task lookup failed"
	// SkipWithinPolicy covers a directory the policy simply still keeps.
	SkipWithinPolicy SkipReason = "within retention policy"
	// SkipRemoveFailed covers a removal that was attempted and failed.
	SkipRemoveFailed SkipReason = "removal failed"
	// SkipUnsafePath covers a path that failed the containment or
	// pre-removal re-verification check. It should be unreachable; it exists
	// so that it is recorded rather than acted on if it ever is reached.
	SkipUnsafePath SkipReason = "path failed containment check"
)

// Skip is one retained entry and the reason, for auditable sweeps.
type Skip struct {
	Name   string
	Reason SkipReason
	Err    error
}

// Result summarizes one sweep.
type Result struct {
	// Scanned counts every entry read from the runs root, including ones
	// that are not run directories at all.
	Scanned int
	// Removed counts directories actually removed; RemovedDirs names them.
	Removed     int
	RemovedDirs []string
	// Kept counts run directories still present after the sweep.
	Kept int
	// Skips records why each retained entry was retained.
	Skips []Skip
}

// Sweeper removes run directories below Root according to Policy.
type Sweeper struct {
	// Root is the orchestrator's --runs-dir. Removal is confined to its
	// direct children.
	Root string
	// Policy bounds what is retained. A disabled policy makes Sweep a no-op.
	Policy Policy
	// Lookup resolves each candidate's owning task. A nil Lookup makes Sweep
	// a no-op: without an authority on task state, nothing is provably safe
	// to delete.
	Lookup Lookup
	// Now defaults to time.Now().UTC(); tests override it.
	Now func() time.Time
}

// candidate is one run directory under consideration.
type candidate struct {
	name string
	path string
	// reference is the instant the directory's age is measured from: the
	// task's EndedAt, else UpdatedAt, else the directory's own ModTime.
	reference time.Time
	// orphan is true when no task row exists. An orphan is removable by age
	// but never by count: without a row there is no trustworthy ordering key
	// for deciding which orphans are newest.
	orphan bool
}

// Sweep scans Root once and removes the run directories the policy no longer
// retains. It returns a Result even when some removals failed; the error
// return is reserved for a failure to scan Root at all. A missing Root is not
// an error: no task has run yet.
func (s Sweeper) Sweep(ctx context.Context) (Result, error) {
	var result Result
	if !s.Policy.Enabled() || s.Lookup == nil {
		return result, nil
	}
	now := time.Now().UTC
	if s.Now != nil {
		now = s.Now
	}
	root, err := filepath.Abs(s.Root)
	if err != nil {
		return result, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return result, err
	}

	// Classification pass: no removal happens until every entry's task state
	// is known, so a count limit is applied against a consistent view.
	candidates := make([]candidate, 0, len(entries))
	for _, entry := range entries {
		result.Scanned++
		name := entry.Name()
		// DirEntry.IsDir reports the type of the entry itself rather than a
		// followed symlink (unlike os.Stat), so a symlink planted in the
		// runs root — even one pointing at a directory — is never a sweep
		// candidate. Same rationale as artifacts.Manager.FindRun.
		if !entry.IsDir() {
			result.Skips = append(result.Skips, Skip{Name: name, Reason: SkipNotRunDir})
			continue
		}
		taskID, ok := taskIDFromRunDir(name)
		if !ok {
			result.Skips = append(result.Skips, Skip{Name: name, Reason: SkipNotRunDir})
			continue
		}
		// The name came from os.ReadDir(root), so it is a single path
		// segment and cannot escape root. The check is kept as a defensive
		// assertion rather than as the thing escape prevention relies on.
		path := filepath.Join(root, name)
		if !containedIn(root, path) {
			result.Skips = append(result.Skips, Skip{Name: name, Reason: SkipUnsafePath})
			continue
		}
		result.Kept++

		task, err := s.Lookup.Get(ctx, taskID)
		switch {
		case errors.Is(err, tasks.ErrNotFound):
			reference := now()
			if info, statErr := entry.Info(); statErr == nil {
				reference = info.ModTime()
			}
			candidates = append(candidates, candidate{name: name, path: path, reference: reference, orphan: true})
		case err != nil:
			// Could not establish the task's state: retain. A database that
			// is locked or holding a corrupt row degrades this sweep to "no
			// deletion", never to "delete everything".
			result.Skips = append(result.Skips, Skip{Name: name, Reason: SkipLookupFailed, Err: err})
		case !task.Status.Terminal():
			// queued/starting/running: a live process may still be writing
			// here. interrupted/unknown: the process state is by definition
			// not established, so it is not provably safe to delete.
			result.Skips = append(result.Skips, Skip{Name: name, Reason: SkipNotTerminal})
		default:
			candidates = append(candidates, candidate{name: name, path: path, reference: referenceTime(task, entry)})
		}
	}

	doomed := s.selectForRemoval(candidates, result.Kept, now())
	for _, c := range candidates {
		if _, ok := doomed[c.name]; !ok {
			result.Skips = append(result.Skips, Skip{Name: c.name, Reason: SkipWithinPolicy})
			continue
		}
		if err := removeRunDir(root, c.path); err != nil {
			result.Skips = append(result.Skips, Skip{Name: c.name, Reason: skipReasonFor(err), Err: err})
			continue
		}
		result.Removed++
		result.Kept--
		result.RemovedDirs = append(result.RemovedDirs, c.name)
	}
	return result, nil
}

// selectForRemoval decides which candidates the policy no longer retains.
// total is the number of run directories currently present (including
// protected ones), because the count limit bounds the whole directory.
func (s Sweeper) selectForRemoval(candidates []candidate, total int, now time.Time) map[string]struct{} {
	doomed := make(map[string]struct{})
	if s.Policy.MaxAge > 0 {
		for _, c := range candidates {
			if now.Sub(c.reference) > s.Policy.MaxAge {
				doomed[c.name] = struct{}{}
			}
		}
	}
	if s.Policy.MaxCount > 0 {
		// Newest first, so removal walks from the oldest end. Ties break on
		// name for a deterministic sweep.
		eligible := make([]candidate, 0, len(candidates))
		for _, c := range candidates {
			if c.orphan {
				continue // never count-swept: see candidate.orphan.
			}
			eligible = append(eligible, c)
		}
		sort.Slice(eligible, func(i, j int) bool {
			if eligible[i].reference.Equal(eligible[j].reference) {
				return eligible[i].name < eligible[j].name
			}
			return eligible[i].reference.After(eligible[j].reference)
		})
		remaining := total - len(doomed)
		for i := len(eligible) - 1; i >= 0 && remaining > s.Policy.MaxCount; i-- {
			if _, already := doomed[eligible[i].name]; already {
				continue
			}
			doomed[eligible[i].name] = struct{}{}
			remaining--
		}
	}
	return doomed
}

// referenceTime is the instant a terminal task's age is measured from. A
// directory's mtime only tracks when a file was last created inside it —
// appending to an existing stdout.raw does not update it — so mtime is
// approximately the task's start, not its end, and is used only as a last
// resort.
func referenceTime(task tasks.Task, entry os.DirEntry) time.Time {
	if task.EndedAt != nil {
		return *task.EndedAt
	}
	if !task.UpdatedAt.IsZero() {
		return task.UpdatedAt
	}
	if info, err := entry.Info(); err == nil {
		return info.ModTime()
	}
	return time.Time{}
}

// taskIDFromRunDir recovers the task ID from a run directory's name.
// artifacts.Manager.Create names every run directory os.MkdirTemp(root,
// taskID+"-"), i.e. the task ID, a literal "-", then a digits-only random
// suffix. Splitting at the final "-" therefore recovers the task ID exactly,
// including for a task ID that itself contains "-" (every ID this project
// generates does: "task-<nanos>"). A name with no "-", an empty task ID, or
// an empty suffix is not a run directory this package created and is left
// alone.
func taskIDFromRunDir(name string) (string, bool) {
	i := strings.LastIndex(name, "-")
	if i <= 0 || i == len(name)-1 {
		return "", false
	}
	taskID := name[:i]
	// The same single-segment guarantee artifacts.validSegment enforces on
	// the way in, re-checked on the way out.
	if taskID == "." || taskID == ".." || strings.ContainsAny(taskID, `/\`) {
		return "", false
	}
	return taskID, true
}

// containedIn reports whether path is a direct-or-deeper descendant of root.
// Both are cleaned first; equality is not containment, so the runs root
// itself can never be the thing removed.
func containedIn(root, path string) bool {
	root, path = filepath.Clean(root), filepath.Clean(path)
	if root == path {
		return false
	}
	return strings.HasPrefix(path, root+string(filepath.Separator))
}

// errUnsafePath marks a pre-removal verification failure.
var errUnsafePath = errors.New("refusing to remove: path is not a contained plain directory")

// removeRunDir removes one run directory after re-verifying, immediately
// before the irreversible call, that it is still inside root and still a
// plain directory rather than a symlink. The classification pass and the
// removal pass are separated in time, so this closes the window in which an
// entry could be swapped for a symlink in between. os.Lstat (not os.Stat)
// reports the type of the path itself, so a symlink fails the check instead
// of being followed. os.RemoveAll itself unlinks rather than follows, so a
// symlink inside the doomed directory cannot be used to delete its target.
func removeRunDir(root, path string) error {
	if !containedIn(root, path) || filepath.Dir(path) != filepath.Clean(root) {
		return fmt.Errorf("%w: %s", errUnsafePath, path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s", errUnsafePath, path)
	}
	return os.RemoveAll(path)
}

func skipReasonFor(err error) SkipReason {
	if errors.Is(err, errUnsafePath) {
		return SkipUnsafePath
	}
	return SkipRemoveFailed
}

// Loop sweeps once immediately, then once per interval, until ctx is done.
// The immediate sweep means restarting the orchestrator is enough to apply a
// newly configured policy without waiting out an interval. onResult, if
// non-nil, receives every sweep's outcome (including a scan error) for
// logging; Loop itself never terminates because a sweep failed, since a
// transient filesystem or database problem must not silently disable
// retention for the rest of the process's life.
func Loop(ctx context.Context, sweeper Sweeper, interval time.Duration, onResult func(Result, error)) {
	if !sweeper.Policy.Enabled() || interval <= 0 {
		return
	}
	sweep := func() {
		result, err := sweeper.Sweep(ctx)
		if onResult != nil {
			onResult(result, err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}
