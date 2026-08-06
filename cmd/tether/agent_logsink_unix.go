//go:build linux || darwin

package main

// dup2Stderr re-points fd 2 at f (h1 F). Split into a platform file because
// the syscall differs by arch: linux/arm64 has no dup2 at all (dup3 only), so
// syscall.Dup2 is not universally available — syscall.Dup3 with flags=0 is
// exactly dup2 where it exists, and is the only form on arm64. darwin has
// Dup2 and no Dup3. Both are covered here without adding golang.org/x/sys as
// a direct dependency for one call.
//
// The tether release matrix is {linux, darwin} × {amd64, arm64}
// (build/goreleaser.yaml); a future non-unix port needs its own file, and
// until it exists this package simply will not build there — a loud, correct
// failure rather than a silently unarmed panic sink.

import (
	"os"
	"syscall"
)

func dup2Stderr(f *os.File) error {
	return dupOntoStderr(int(f.Fd()))
}

// dupFD duplicates fd onto the lowest free descriptor. Used only by the
// panic-sink tests, to save and restore the test process's own fd 2 around a
// test that re-points it (an un-restored redirect would swallow the test
// binary's own failure output).
func dupFD(fd int) (int, error) { return syscall.Dup(fd) }
