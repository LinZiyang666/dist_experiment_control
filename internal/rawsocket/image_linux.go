//go:build linux

package rawsocket

import (
	"os"

	"golang.org/x/sys/unix"
)

// ExportFile borrows the original only while its owner proves it is frozen.
// origin: hot-update S5 plan §5.1 / §5.3
func (s *Socket) ExportFile() (*os.File, error) {
	if s == nil || s.state == nil {
		return nil, ErrClosed
	}
	owner, ok := any(s.state).(socketImageSource)
	if !ok {
		return nil, ErrBusy
	}
	f, want, err := owner.imageFile()
	if err != nil {
		return nil, err
	}
	if err := validateImageFile(f, want); err != nil {
		return nil, err
	}
	return f, nil
}

// SyscallConn avoids File.Fd's transition to blocking mode. This read-only
// predicate is safe for a failed/pending connect: SO_ERROR has one consumer,
// the connector, and checking it here would erase its original receipt.
// origin: hot-update S5 plan §3.2 / §5.1
func validateImageFile(f *os.File, want Fingerprint) error {
	if f == nil || want.Ino == 0 || want.Type != unix.S_IFSOCK {
		return ErrIdentity
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return ErrIdentity
	}
	var checked error
	err = raw.Control(func(fd uintptr) {
		var stat unix.Stat_t
		if unix.Fstat(int(fd), &stat) != nil ||
			(Fingerprint{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT}) != want {
			checked = ErrIdentity
			return
		}
		flags, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if e != nil || flags&unix.O_NONBLOCK == 0 {
			checked = ErrIdentity
			return
		}
		flags, e = unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e != nil || flags&unix.FD_CLOEXEC == 0 {
			checked = ErrIdentity
		}
	})
	if err != nil {
		return ErrIdentity
	}
	return checked
}
