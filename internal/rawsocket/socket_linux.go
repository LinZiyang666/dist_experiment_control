//go:build linux

package rawsocket

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// inspectSocket is deliberately read-only. In particular SO_ERROR clears a
// failed connect's pending error; only CompleteConnect is allowed to read it.
// origin: hot-update S5 plan §3.2 / T08 / T09
func inspectSocket(f *os.File) (Fingerprint, syscall.RawConn, error) {
	if f == nil {
		return Fingerprint{}, nil, ErrIdentity
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return Fingerprint{}, nil, ErrIdentity
	}
	var fp Fingerprint
	var check error
	err = raw.Control(func(fd uintptr) {
		var st unix.Stat_t
		if check = unix.Fstat(int(fd), &st); check != nil {
			return
		}
		if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			check = ErrIdentity
			return
		}
		flags, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if e != nil || flags&unix.O_NONBLOCK == 0 || flags&unix.O_ACCMODE != unix.O_RDWR {
			check = ErrIdentity
			return
		}
		flags, e = unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e != nil || flags&unix.FD_CLOEXEC == 0 {
			check = ErrIdentity
			return
		}
		typ, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if e != nil || typ != unix.SOCK_STREAM {
			check = ErrIdentity
			return
		}
		listening, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
		if e != nil || listening != 0 {
			check = ErrIdentity
			return
		}
		// Image and live adoption use the same Fstat identity. SO_TYPE proves
		// stream semantics above, but its value is not the file type in the ABI.
		// origin: hot-update S5 plan §3.2 / §5.1 / T08
		fp = Fingerprint{Dev: uint64(st.Dev), Ino: st.Ino, Type: st.Mode & unix.S_IFMT}
	})
	if err != nil || check != nil {
		return Fingerprint{}, nil, ErrIdentity
	}
	return fp, raw, nil
}

func Adopt(f *os.File, want Fingerprint) (*Socket, error) {
	fp, raw, err := inspectSocket(f)
	if err != nil {
		return nil, err
	}
	if fp != want || want == (Fingerprint{}) {
		return nil, ErrIdentity
	}
	st := &socketState{file: f, raw: raw, identity: fp, connectState: ConnectConnected}
	_ = raw.Control(func(fd uintptr) {
		local, le := unix.Getsockname(int(fd))
		peer, pe := unix.Getpeername(int(fd))
		if le == nil {
			st.tuple.Local = socketAddress(local)
		}
		if pe == nil {
			st.tuple.Remote = socketAddress(peer)
		} else {
			st.connectState = ConnectConnecting
		}
	})
	return &Socket{state: st}, nil
}

func ValidateFile(f *os.File, want Fingerprint) error {
	fp, _, err := inspectSocket(f)
	if err != nil || want == (Fingerprint{}) || fp != want {
		return ErrIdentity
	}
	return nil
}

func readSocket(raw syscall.RawConn, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var n int
	var op error
	err := raw.Read(func(fd uintptr) bool {
		for {
			n, op = unix.Read(int(fd), p)
			if op != unix.EINTR {
				break
			}
		}
		return op != unix.EAGAIN && op != unix.EWOULDBLOCK
	})
	if n < 0 {
		n = 0
	}
	if err != nil {
		return n, err
	}
	if n == 0 && op == nil {
		op = io.EOF
	}
	return n, op
}
func writeSocket(raw syscall.RawConn, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var n int
	var op error
	err := raw.Write(func(fd uintptr) bool {
		for {
			n, op = unix.Write(int(fd), p)
			if op != unix.EINTR {
				break
			}
		}
		return op != unix.EAGAIN && op != unix.EWOULDBLOCK
	})
	if n < 0 {
		n = 0
	}
	if err != nil {
		return n, err
	}
	return n, op
}
func shutdownSocket(raw syscall.RawConn, both bool) error {
	how := unix.SHUT_WR
	if both {
		how = unix.SHUT_RDWR
	}
	var op error
	err := raw.Control(func(fd uintptr) { op = unix.Shutdown(int(fd), how) })
	return errors.Join(err, op)
}

// BootNow is the local absolute clock used by inheritable IO decisions.
func BootNow() (int64, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	return ts.Nano(), nil
}

// TakeTCP transfers a netpoll connection to an explicitly owned descriptor of
// the same OFD. The old net.Conn is closed only after adoption succeeds.
func TakeTCP(conn *net.TCPConn) (*Socket, error) {
	if conn == nil {
		return nil, ErrIdentity
	}
	rc, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var fd int
	var op error
	err = rc.Control(func(raw uintptr) { fd, op = unix.FcntlInt(raw, unix.F_DUPFD_CLOEXEC, 0) })
	if err != nil {
		return nil, err
	}
	if op != nil {
		return nil, op
	}
	file := os.NewFile(uintptr(fd), "flow-raw")
	fp, _, err := inspectSocket(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	s, err := Adopt(file, fp)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	_ = conn.Close()
	return s, nil
}
