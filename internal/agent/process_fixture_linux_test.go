//go:build linux

package agent

import "syscall"

// Only outer helpers use Pdeathsig: business children must survive same-PID
// exec, which replaces the parent's Go threads. Fixture teardown owns those
// children explicitly instead of making their survival depend on a thread.
// origin: hot-update S5 PROC-LEAK / interrupted isolated helpers.
func testProcessAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}
