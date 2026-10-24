//go:build !linux

package agent

import "syscall"

func testProcessAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
