//go:build darwin

package main

import "syscall"

// dupOntoStderr uses dup2 — darwin has no dup3.
func dupOntoStderr(fd int) error { return syscall.Dup2(fd, 2) }
