//go:build linux

package clusterharness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

type brokerFixtureFileInfo struct {
	os.FileInfo
	kernel any
}

func (f brokerFixtureFileInfo) Sys() any { return f.kernel }

type cohortSyscallFailure struct {
	net.Conn
	err error
}

func (c cohortSyscallFailure) SyscallConn() (syscall.RawConn, error) { return nil, c.err }

type cohortHalfCloseWriter struct {
	brokerWitnessCloser
	halfClosed int
}

func (w *cohortHalfCloseWriter) CloseWrite() error {
	w.halfClosed++
	return w.err
}

// Invalid witness types cannot stand in for an original kernel socket or FIN.
// The real TCP success path is exercised by the full-tuple census below.
// origin: hot-update S6 plan §6.3 / T48
func TestBrokerCohortWitnessRejectsMissingSocketOperations(t *testing.T) {
	for _, address := range []net.Addr{nil, (*net.TCPAddr)(nil), &net.UDPAddr{Port: 1234}, &net.UnixAddr{Name: "fixture"}} {
		if _, err := brokerFixtureTCPPort(address); err == nil {
			t.Fatal("non-TCP witness address accepted")
		}
	}
	if port, err := brokerFixtureTCPPort(&net.TCPAddr{Port: 1234}); err != nil || port != 1234 {
		t.Fatalf("original TCP port changed: %d %v", port, err)
	}
	if _, err := cohortFD(nil); err == nil {
		t.Fatal("missing kernel socket accepted")
	}
	left, right := net.Pipe()
	defer func() { _ = left.Close(); _ = right.Close() }()
	if _, err := cohortFD(left); err == nil {
		t.Fatal("in-memory connection certified a kernel socket")
	}
	failure := errors.New("socket witness failure")
	if _, err := cohortFD(cohortSyscallFailure{Conn: left, err: failure}); !errors.Is(err, failure) {
		t.Fatal("kernel witness error lost")
	}
	if err := (cohortSSWriter{p: &productSSPeer{conn: left}}).CloseWrite(); err == nil {
		t.Fatal("non-TCP SS peer certified half-close")
	}
	if err := (cohortSSWriter{p: &productSSPeer{conn: (*net.TCPConn)(nil)}}).CloseWrite(); err == nil {
		t.Fatal("missing TCP SS peer certified half-close")
	}
	for _, kernel := range []any{nil, (*syscall.Stat_t)(nil), struct{}{}} {
		if _, err := brokerFixtureExecutableIdentity(brokerFixtureFileInfo{kernel: kernel}); err == nil {
			t.Fatal("missing executable kernel identity accepted")
		}
	}
	identity, err := brokerFixtureExecutableIdentity(brokerFixtureFileInfo{kernel: &syscall.Stat_t{Dev: 3, Ino: 5, Size: 7}})
	if err != nil || identity != "3:5:7" {
		t.Fatalf("executable kernel identity changed: %s %v", identity, err)
	}
	c := &productCohort{}
	e := &cohortEndpoint{evidence: cohortStream{HalfClosed: true}}
	if err := c.send(e, &bytes.Buffer{}, 0, 0); err == nil {
		t.Fatal("writer without FIN operation accepted")
	}
	for _, closeErr := range []error{nil, failure} {
		w := &brokerWitnessCloser{err: closeErr}
		if err := c.send(e, w, 0, 0); !errors.Is(err, closeErr) || w.closed != 1 {
			t.Fatalf("SSH stdin close error/count lost: %v %d", err, w.closed)
		}
		half := &cohortHalfCloseWriter{brokerWitnessCloser: brokerWitnessCloser{err: closeErr}}
		if err := c.send(e, half, 0, 0); !errors.Is(err, closeErr) || half.halfClosed != 1 || half.closed != 0 {
			t.Fatalf("original socket half-close error/count lost: %v %d %d", err, half.halfClosed, half.closed)
		}
	}
}

// /proc/PID/net is the whole network namespace, not that PID's sockets. Two
// live tuples can share a remote source port; only the full tuple plus the
// owning process's FD table selects the original broker socket.
// origin: hot-update S6 plan §6.3 / T48
func TestBrokerCohortSocketCensusUsesFullTuple(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerSocketTuple(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "original socket tuple census x5", g)
	testharness.AssertNoFDLeak(t, "original socket tuple census x5", fds, 0)
}
func exerciseBrokerSocketTuple(t *testing.T) {
	t.Helper()
	var listeners []net.Listener
	var peers, targets []net.Conn
	defer func() {
		for _, c := range peers {
			_ = c.Close()
		}
		for _, c := range targets {
			_ = c.Close()
		}
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	for i := 0; i < 2; i++ {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		dialer := net.Dialer{LocalAddr: local, Timeout: time.Second, Control: func(_, _ string, raw syscall.RawConn) error {
			var optionErr error
			err := raw.Control(func(fd uintptr) { optionErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1) })
			if err != nil {
				return err
			}
			return optionErr
		}}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		peer, err := dialer.DialContext(ctx, "tcp4", l.Addr().String())
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer)
		target, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
		local = peer.LocalAddr().(*net.TCPAddr)
	}
	if peers[0].LocalAddr().String() != peers[1].LocalAddr().String() {
		t.Fatal("collision control did not share its real source port")
	}
	for i := 1; i >= 0; i-- {
		fd, err := cohortRemoteFD(os.Getpid(), local.Port, listeners[i].Addr().(*net.TCPAddr).Port)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := cohortFD(targets[i])
		if err != nil {
			t.Fatal(err)
		}
		actualID, err := cohortSocketAt(os.Getpid(), fd)
		if err != nil {
			t.Fatal(err)
		}
		expectedID, err := cohortSocketAt(os.Getpid(), expected)
		if err != nil {
			t.Fatal(err)
		}
		if actualID != expectedID {
			t.Fatal("source-port-only lookup selected another live tuple")
		}
	}
}

// A not-yet-bound control address is still reserved. A public listener taking
// that address would send application bytes into the tunnel TLS handshake.
// origin: hot-update S6 plan §6.3 / T48
func TestBrokerCohortReservesFutureListenerAddresses(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerFutureAddresses(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "future listener addresses x5", g)
	testharness.AssertNoFDLeak(t, "future listener addresses x5", fds, 0)
}
func exerciseBrokerFutureAddresses(t *testing.T) {
	t.Helper()
	control, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first := control.Addr().String()
	_ = control.Close()
	public, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	second := public.Addr().String()
	_ = public.Close()
	if first == second {
		t.Fatal("distinct port control was not established")
	}
	f := &brokerProductFixture{test: t}
	if address := productFixtureAddressUsing(f, func() string { return first }); address != first {
		t.Fatal("control address was not reserved")
	}
	calls := 0
	actual := productFixtureAddressUsing(f, func() string {
		calls++
		if calls == 1 {
			return first
		}
		return second
	})
	if actual == first || actual != second || calls != 2 {
		t.Fatal("future public listener reused a reserved control address")
	}
}

// origin: hot-update S6 plan §6.3 / T28 / T56 / TESTFIX-2
func TestBrokerProductPublicPortReservation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerProductPublicPortReservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "product public port reservations", g)
	testharness.AssertNoFDLeak(t, "product public port reservations", fd, 0)
}

func exerciseBrokerProductPublicPortReservation(t *testing.T) {
	f := &brokerProductFixture{test: t}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	address := productReservePublicAddress(f)
	competitor, err := net.Listen("tcp4", address)
	if err == nil {
		_ = competitor.Close()
		t.Fatal("another fixture stole the public port before REGISTER")
	}
	_, text, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscan(text, &port); err != nil {
		t.Fatal(err)
	}
	if err := productReleasePublicPort(f, port); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("REGISTER could not take the reserved port: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	// An aborted fixture also closes reservations it never handed to REGISTER.
	aborted := productReservePublicAddress(f)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err = net.Listen("tcp4", aborted)
	if err != nil {
		t.Fatalf("failed setup retained its public reservation: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}
