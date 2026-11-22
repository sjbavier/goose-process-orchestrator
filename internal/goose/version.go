package goose

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var (
	versionMu    sync.Mutex
	versionCache = map[string]string{}
)

// VersionOf returns the first line of `<binary> --version`, cached per
// binary for the process lifetime. It returns "" when the version cannot be
// determined; callers must treat that as unknown, never as a pass.
func VersionOf(binary string) string {
	if binary == "" {
		binary = "goose"
	}
	versionMu.Lock()
	defer versionMu.Unlock()
	if v, ok := versionCache[binary]; ok {
		return v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "--version").Output()
	v := ""
	if err == nil {
		v = strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	}
	versionCache[binary] = v
	return v
}
