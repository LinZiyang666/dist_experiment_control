//go:build linux

package main

import "syscall"

// dupOntoStderr uses dup3(fd, 2, 0) — identical to dup2 on Linux, and the
// ONLY form available on linux/arm64 (the kernel dropped the dup2 syscall
// there; Go's syscall package reflects that).
func dupOntoStderr(fd int) error { return syscall.Dup3(fd, 2, 0) }
