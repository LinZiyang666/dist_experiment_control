//go:build linux

package rawsocket

import (
	"errors"
	"net/netip"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func socketAddress(sa unix.Sockaddr) netip.AddrPort {
	switch a := sa.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port))
	case *unix.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port))
	default:
		return netip.AddrPort{}
	}
}

// Connect returns the original nonblocking FD, including EINPROGRESS. The caller
// journals it before CompleteConnect; a cut never redials a pending connection.
func Connect(addr netip.AddrPort, deadline time.Time, stop <-chan struct{}, beforeConnect func(Tuple) error) (*Socket, error) {
	if !addr.IsValid() || addr.Port() == 0 || addr.Addr().Zone() != "" {
		return nil, ErrIdentity
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return nil, os.ErrDeadlineExceeded
	}
	select {
	case <-stop:
		return nil, ErrClosed
	default:
	}
	domain := unix.AF_INET6
	var target, local unix.Sockaddr
	if addr.Addr().Unmap().Is4() {
		domain = unix.AF_INET
		target = &unix.SockaddrInet4{Addr: addr.Addr().Unmap().As4(), Port: int(addr.Port())}
		local = &unix.SockaddrInet4{}
	} else {
		target = &unix.SockaddrInet6{Addr: addr.Addr().As16(), Port: int(addr.Port())}
		local = &unix.SockaddrInet6{}
	}
	fd, err := unix.Socket(domain, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "flow-raw")
	fail := func(e error) (*Socket, error) { _ = f.Close(); return nil, e }
	if err = unix.Bind(fd, local); err != nil {
		return fail(err)
	}
	bound, err := unix.Getsockname(fd)
	if err != nil {
		return fail(err)
	}
	tuple := Tuple{Local: socketAddress(bound), Remote: addr}
	// The callback sees the reserved source port before the first SYN or payload.
	// Loopback local addresses are made concrete without changing the bound OFD.
	if addr.Addr().IsLoopback() {
		tuple.Local = netip.AddrPortFrom(addr.Addr(), tuple.Local.Port())
	}
	if beforeConnect != nil {
		if err = beforeConnect(tuple); err != nil {
			return fail(err)
		}
	}
	select {
	case <-stop:
		return fail(ErrClosed)
	default:
	}
	fp, raw, err := inspectSocket(f)
	if err != nil {
		return fail(err)
	}
	st := &socketState{file: f, raw: raw, identity: fp, tuple: tuple, connectDeadline: deadline, connectState: ConnectConnecting}
	err = unix.Connect(fd, target)
	if err == nil {
		st.connectState = ConnectConnected
		if actual, e := unix.Getsockname(fd); e == nil {
			st.tuple.Local = socketAddress(actual)
		}
		st.connectConsumed = true
	} else if !errors.Is(err, unix.EINPROGRESS) {
		st.connectState = ConnectFailed
		st.connectError = err
		st.connectConsumed = true
	}
	// Routing chooses the concrete source address during connect, including
	// EINPROGRESS. A cut before SO_ERROR consumption must preserve that tuple.
	if actual, e := unix.Getsockname(fd); e == nil {
		st.tuple.Local = socketAddress(actual)
	}
	return &Socket{state: st}, nil
}

// CompleteConnect is the sole consumer of SO_ERROR. A short poll interval lets
// Stop interrupt a connect without closing the inherited socket or spawning an
// unjoinable timeout goroutine. Timeout leaves Connecting and its first deadline.
// origin: hot-update S5 plan §3.2 / T09
func (s *Socket) CompleteConnect(deadline time.Time, stop <-chan struct{}) error {
	st, err := s.beginIO()
	if err != nil {
		return err
	}
	defer s.endIO()
	st.connectMu.Lock()
	defer st.connectMu.Unlock()
	for {
		st.mu.Lock()
		phase, result, original := st.connectState, st.connectError, st.connectDeadline
		st.mu.Unlock()
		if phase == ConnectConnected {
			return nil
		}
		if phase == ConnectFailed {
			return result
		}
		if !original.IsZero() && (deadline.IsZero() || original.Before(deadline)) {
			deadline = original
		}
		select {
		case <-stop:
			return ErrBusy
		default:
		}
		var ready bool
		var op error
		err = st.raw.Control(func(fd uintptr) {
			wait := 10
			if !deadline.IsZero() {
				if ms := int(time.Until(deadline).Milliseconds()); ms < wait {
					wait = max(ms, 0)
				}
			}
			p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
			_, op = unix.Poll(p, wait)
			ready = p[0].Revents != 0
			if op == unix.EINTR {
				op = nil
				ready = false
			}
			if op != nil || !ready {
				return
			}
			var code int
			code, op = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ERROR)
			st.mu.Lock()
			st.connectConsumed = true
			if op != nil {
				st.connectError = op
				st.connectState = ConnectFailed
			} else if code != 0 {
				st.connectError = unix.Errno(code)
				st.connectState = ConnectFailed
			} else {
				st.connectState = ConnectConnected
				if actual, e := unix.Getsockname(int(fd)); e == nil {
					st.tuple.Local = socketAddress(actual)
				}
			}
			st.mu.Unlock()
		})
		if err != nil {
			return err
		}
		if op != nil {
			return op
		}
		// A cut can pause the sole SO_ERROR consumer after the original SYN
		// completed. Inspect an immediately ready result before rejecting an
		// expired wait budget. Poll's timeout is still clamped to the original
		// deadline (zero after expiry); a pending socket never gains more time.
		// origin: hot-update S5 plan §3.2 / §9.1 / T09 / DRILL-RUN 75.A
		if !ready && !deadline.IsZero() && !time.Now().Before(deadline) {
			return os.ErrDeadlineExceeded
		}
	}
}
