//go:build linux

package supervisor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// startTicks returns the kernel-reported start time of pid, in clock ticks
// since boot, read from /proc/<pid>/stat field 22 (starttime). The comm
// field (field 2) is parenthesized and can itself contain spaces and
// parentheses, so the field is located by finding the LAST ")" in the line
// rather than naively splitting on spaces first.
func startTicks(pid int) (int64, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, fmt.Errorf("read /proc/%d/stat: %w", pid, err)
	}
	line := string(raw)
	closeIdx := strings.LastIndex(line, ")")
	if closeIdx < 0 || closeIdx+1 >= len(line) {
		return 0, fmt.Errorf("parse /proc/%d/stat: no closing ) for comm field", pid)
	}
	// Fields after comm start at field 3 (state); starttime is field 22,
	// i.e. index 22-3 = 19 in the space-split remainder.
	rest := strings.Fields(line[closeIdx+1:])
	const starttimeIndex = 19
	if len(rest) <= starttimeIndex {
		return 0, fmt.Errorf("parse /proc/%d/stat: expected at least %d fields after comm, got %d", pid, starttimeIndex+1, len(rest))
	}
	ticks, err := strconv.ParseInt(rest[starttimeIndex], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse /proc/%d/stat starttime field: %w", pid, err)
	}
	return ticks, nil
}
