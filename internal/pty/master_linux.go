//go:build linux

package pty

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// prepareMaster replaces the Go File wrapper, preserving the original PTY.
// creack/pty's open-time ioctls call File.Fd, which makes that File blocking.
// A fresh nonblocking duplicate registers cleanly with Go's poller and keeps
// both read and write deadlines usable. Allocate owns the pair exclusively;
// no read, write or child spawn may race this preparation.
// On success this consumes original. On failure the caller still closes it.
func prepareMaster(original *os.File) (*os.File, error) {
	raw, err := original.SyscallConn()
	if err != nil {
		return nil, err
	}
	duplicate := -1
	var inner error
	err = raw.Control(func(fd uintptr) {
		duplicate, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3)
		if inner == nil {
			inner = unix.SetNonblock(duplicate, true)
		}
	})
	if err != nil || inner != nil {
		if duplicate >= 0 {
			_ = unix.Close(duplicate)
		}
		if err != nil {
			return nil, err
		}
		return nil, inner
	}
	master := os.NewFile(uintptr(duplicate), original.Name())
	if err := master.SetReadDeadline(time.Time{}); err != nil {
		_ = master.Close()
		return nil, err
	}
	if err := master.SetWriteDeadline(time.Time{}); err != nil {
		_ = master.Close()
		return nil, err
	}
	if err := original.Close(); err != nil {
		_ = master.Close()
		return nil, err
	}
	return master, nil
}

// resizeMaster must never use File.Fd: its blocking-mode compatibility side
// effect is shared with duplicate descriptors and can defeat freeze deadlines.
func resizeMaster(master *os.File, cols, rows uint16) error {
	raw, err := master.SyscallConn()
	if err != nil {
		return err
	}
	var inner error
	err = raw.Control(func(fd uintptr) {
		inner = unix.IoctlSetWinsize(int(fd), unix.TIOCSWINSZ, &unix.Winsize{Col: cols, Row: rows})
	})
	if err != nil {
		return err
	}
	return inner
}
