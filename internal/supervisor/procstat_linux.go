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

// procEntry is one live, non-zombie process observed in /proc.
type procEntry struct {
	pid, pgrp int
	ticks     int64
}

// readProcEntry parses /proc/<pid>/stat for state, pgrp (field 5) and
// starttime (field 22). Zombies are reported with ok=false: they hold no
// resources a caller could act on and are reaped by their parent/init.
func readProcEntry(pid int) (e procEntry, ok bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return e, false
	}
	line := string(raw)
	closeIdx := strings.LastIndex(line, ")")
	if closeIdx < 0 || closeIdx+1 >= len(line) {
		return e, false
	}
	rest := strings.Fields(line[closeIdx+1:])
	if len(rest) <= 19 || rest[0] == "Z" || rest[0] == "X" {
		return e, false
	}
	pgrp, err1 := strconv.Atoi(rest[2])
	ticks, err2 := strconv.ParseInt(rest[19], 10, 64)
	if err1 != nil || err2 != nil {
		return e, false
	}
	return procEntry{pid: pid, pgrp: pgrp, ticks: ticks}, true
}

// scanGroup lists live non-zombie members of process group pgid by scanning
// /proc. An error means the scan could not be performed at all (detection
// unavailable); an empty result with nil error means the group is empty.
func scanGroup(pgid int) ([]procEntry, error) {
	dir, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat("/proc/self/stat"); err != nil {
		return nil, err
	}
	members := []procEntry{}
	for _, d := range dir {
		pid, err := strconv.Atoi(d.Name())
		if err != nil || pid <= 0 {
			continue
		}
		if e, ok := readProcEntry(pid); ok && e.pgrp == pgid {
			members = append(members, e)
		}
	}
	return members, nil
}
