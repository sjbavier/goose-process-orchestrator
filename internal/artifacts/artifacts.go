// Package artifacts creates and bounds task-owned execution evidence.
package artifacts

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const DefaultOutputLimit int64 = 256 * 1024

// ErrRunNotFound means no run directory matching a task ID exists below
// Root, or more than one does (ambiguous — treated as not found rather
// than guessing).
var ErrRunNotFound = errors.New("run directory not found")

// Run owns one private directory below Root.
type Run struct{ Dir, StdoutPath, StderrPath string }

type Manager struct {
	Root  string
	Limit int64
}

func (m Manager) Create(taskID string) (Run, error) {
	if err := validSegment(taskID); err != nil {
		return Run{}, err
	}
	root, err := filepath.Abs(m.Root)
	if err != nil {
		return Run{}, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return Run{}, err
	}
	dir, err := os.MkdirTemp(root, taskID+"-")
	if err != nil {
		return Run{}, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		_ = os.RemoveAll(dir)
		return Run{}, err
	}
	return Run{Dir: dir, StdoutPath: filepath.Join(dir, "stdout.raw"), StderrPath: filepath.Join(dir, "stderr.raw")}, nil
}

// FindRun locates the run directory Create previously made for taskID,
// without any stored reference to it: Create always names it
// os.MkdirTemp(root, taskID+"-"), i.e. the literal prefix taskID+"-"
// followed by a random suffix, so a directory listing plus a prefix match
// recovers it exactly. This makes the run directory discoverable while a
// task is still running, not only after a terminal transition records it.
//
// Matching uses strings.HasPrefix against directory-entry names, never
// filepath.Glob, so a taskID containing glob metacharacters cannot change
// matching semantics. Entries are checked with DirEntry.IsDir(), which
// (unlike os.Stat) reports the type of the entry itself rather than a
// followed symlink, so a symlink placed in root is never mistaken for a
// task's run directory. More than one match is ambiguous and rejected
// rather than silently chosen between.
func (m Manager) FindRun(taskID string) (Run, error) {
	if err := validSegment(taskID); err != nil {
		return Run{}, err
	}
	root, err := filepath.Abs(m.Root)
	if err != nil {
		return Run{}, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return Run{}, ErrRunNotFound
		}
		return Run{}, err
	}
	prefix := taskID + "-"
	var dir string
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		if dir != "" {
			return Run{}, ErrRunNotFound
		}
		dir = filepath.Join(root, e.Name())
	}
	if dir == "" {
		return Run{}, ErrRunNotFound
	}
	return Run{Dir: dir, StdoutPath: filepath.Join(dir, "stdout.raw"), StderrPath: filepath.Join(dir, "stderr.raw")}, nil
}

func validSegment(s string) error {
	if s == "" || s == "." || s == ".." || filepath.Base(s) != s || strings.ContainsAny(s, `/\\`) {
		return fmt.Errorf("unsafe task id")
	}
	return nil
}

// OpenCapture returns a writer that always drains its input while retaining at
// most Limit bytes. Call Close after the child has exited.
func (m Manager) OpenCapture(path string) (io.WriteCloser, error) {
	if m.Limit <= 0 {
		m.Limit = DefaultOutputLimit
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	return &capture{file: f, limit: m.Limit}, nil
}

type capture struct {
	file           *os.File
	limit, written int64
}

func (c *capture) Write(p []byte) (int, error) {
	original := len(p)
	if c.written < c.limit {
		keep := int64(len(p))
		if remain := c.limit - c.written; keep > remain {
			keep = remain
		}
		if keep > 0 {
			n, err := c.file.Write(p[:keep])
			c.written += int64(n)
			if err != nil {
				return 0, err
			}
			if n != int(keep) {
				return 0, io.ErrShortWrite
			}
		}
	}
	return original, nil
}
func (c *capture) Close() error { return c.file.Close() }

// Resolve confirms a requested evidence name remains a plain path beneath run.
func (r Run) Resolve(name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || filepath.Base(name) != name {
		return "", errors.New("unsafe artifact name")
	}
	return filepath.Join(r.Dir, name), nil
}
