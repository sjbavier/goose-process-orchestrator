//go:build darwin

package supervisor

import "fmt"

// startTicks always fails closed on Darwin: there is no /proc on this
// platform, and no evidence-checked equivalent (e.g. sysctl KERN_PROC_PID /
// kinfo_proc.p_starttime) has been implemented or verified on a real Darwin
// host yet. Per the plan's Darwin non-goal, this is an intentional stub,
// not a TODO left accidentally unimplemented: any caller relying on
// startTicks must treat it as always-unverifiable on this platform.
func startTicks(pid int) (int64, error) {
	return 0, fmt.Errorf("startTicks: process start-time verification is unsupported/unverified on Darwin (pid %d)", pid)
}
