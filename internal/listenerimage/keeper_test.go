//go:build linux

package listenerimage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"golang.org/x/sys/unix"
)

// Closing an Accept view must preserve the original LISTEN OFD, established
// application sockets and backlog. A successful handshake alone is insufficient:
// every fresh connection receives an actual business reply within the 5s budget.
// origin: hot-update S6 plan §6.1 / T18
func TestListenerKeeperViewPause(t *testing.T) {
	for _, network := range []string{"tcp4", "tcp6", "unix"} {
		t.Run(network, func(t *testing.T) { exerciseKeeperContinuity(t, network) })
	}
	t.Run("blocked_accept_join", exerciseKeeperAcceptJoin)
	t.Run("accepted_socket_fence", exerciseKeeperAcceptedFence)
	t.Run("view_close_join", exerciseKeeperViewCloseJoin)
	t.Run("capture_refuses_inputs", exerciseKeeperCaptureRefusal)
	t.Run("descriptor_and_flags", exerciseKeeperValidation)
	t.Run("alias_ownership", exerciseKeeperAlias)
	t.Run("unix_path_replacement", exerciseKeeperUnixReplacement)
	t.Run("invalid_deadline_preserves_view", exerciseKeeperInvalidDeadline)
}

// These cycles include real I/O, keeper/view separation, partial refusal,
// duplicate inheritance, pristine transfer, adoption and normal Unix cleanup.
// Identity multisets supplement counts so closing an unrelated old descriptor
// cannot conceal a leaked listener duplicate with an unchanged total FD count.
// origin: hot-update S6 plan §9.3 / §12.1 P08
func TestBrokerListenerLeakCycles(t *testing.T) {
	// Warm the netpoller before recording the baseline; its permanent epoll/event
	// descriptors are runtime owners, not listener resources from an exercise.
	ln := keeperTestListener(t, "tcp4")
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	identityBefore := keeperFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseKeeperPauseAdopt(t)
	}
	testharness.AssertNoGoroutineLeak(t, "listener keeper cycles", gBefore)
	testharness.AssertNoFDLeak(t, "listener keeper cycles", fdBefore, 0)
	if got := keeperFDIdentities(t); !reflect.DeepEqual(got, identityBefore) {
		t.Errorf("FD identity multiset changed: before=%v after=%v", identityBefore, got)
	}
}

func exerciseKeeperContinuity(t *testing.T, network string) {
	t.Helper()
	ln := keeperTestListener(t, network)
	addr := ln.Addr().String()
	k, err := Capture(ln, "public.listener")
	if err != nil {
		if k != nil {
			_ = k.Close()
		}
		t.Fatal(err)
	}
	defer func() { _ = k.Close() }()
	before := k.Descriptor()
	if !before.Nonblock || !before.CloseOnExec || before.Cookie == 0 || before.Inode == 0 {
		t.Fatalf("incomplete original listener identity: %+v", before)
	}
	oldView := k.View()
	peer := keeperTestDial(t, network, addr)
	defer func() { _ = peer.Close() }()
	business, err := oldView.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = business.Close() }()
	keeperBusinessReply(t, business, peer, "before")
	if _, _, err := k.Export(); !errors.Is(err, ErrBusy) {
		t.Fatalf("active export accepted: %v", err)
	}
	if _, err := k.Detach(); !errors.Is(err, ErrBusy) {
		t.Fatalf("active pristine detach accepted: %v", err)
	}
	if err := oldView.Close(); err != nil {
		t.Fatal(err)
	}
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	desc, resource, err := k.Export()
	if err != nil {
		t.Fatal(err)
	}
	if desc != before || resource.ID != desc.ID || resource.Kind != "listener" || len(resource.Files) != 1 {
		t.Fatalf("export lost original identity or resource purpose: %+v", desc)
	}
	var encoded Descriptor
	if err := json.Unmarshal(resource.Data, &encoded); err != nil || encoded != before {
		t.Fatalf("resource descriptor mismatch: %v", err)
	}
	if got, err := inspectListener(resource.Files["listener"], desc.ID); err != nil || got != before {
		t.Fatalf("original OFD stopped listening after view close: %v", err)
	}
	if conn, err := oldView.Accept(); conn != nil || !errors.Is(err, net.ErrClosed) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("closed generation accepted a socket: %v", err)
	}
	// Dial during the pause proves the original socket still LISTENs. The
	// backlog socket must subsequently be accepted and answered by the new view.
	started := time.Now()
	fresh := keeperTestDial(t, network, addr)
	defer func() { _ = fresh.Close() }()
	keeperBusinessReply(t, business, peer, "while-paused")
	if err := k.Resume(); err != nil {
		t.Fatal(err)
	}
	newView := k.View()
	if newView == oldView {
		t.Fatal("Resume reused the closed Accept generation")
	}
	reply, err := newView.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reply.Close() }()
	keeperBusinessReply(t, reply, fresh, "fresh-business")
	if time.Since(started) > upgradebudget.RequestLatency {
		t.Fatal("fresh business reply exceeded the request budget")
	}
	keeperBusinessReply(t, business, peer, "after-resume")
	if k.Descriptor() != before {
		t.Fatal("Resume rebound the original socket")
	}
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	if before.Path != "" {
		if _, err := os.Lstat(before.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("normal stop did not unlink the original Unix pathname: %v", err)
		}
	}
}

func exerciseKeeperAcceptJoin(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "blocked.listener")
	defer func() { _ = k.Close() }()
	view := k.View().(*acceptView)
	done := make(chan error, 1)
	go func() {
		conn, err := view.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()
	// Observe the actual entered Accept, not an unconsumed "about to enter"
	// marker. Yielding until the counter changes is a deterministic barrier.
	limit := time.Now().Add(upgradebudget.RequestLatency)
	for {
		view.mu.Lock()
		entered := view.active == 1
		view.mu.Unlock()
		if entered {
			break
		}
		if time.Now().After(limit) {
			_ = k.Close()
			<-done
			t.Fatal("Accept did not enter the view")
		}
		runtime.Gosched()
	}
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("paused Accept returned %v", err)
		}
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatal("paused Accept did not join")
	}
	view.mu.Lock()
	active := view.active
	view.mu.Unlock()
	if active != 0 {
		t.Fatal("Pause declared completion before Accept joined")
	}
	if _, _, err := k.Export(); err != nil {
		t.Fatal(err)
	}
}

func exerciseKeeperValidation(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "validation.listener")
	defer func() { _ = k.Close() }()
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	desc, resource, err := k.Export()
	if err != nil {
		t.Fatal(err)
	}
	file, err := k.Detach()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if file != resource.Files["listener"] {
		t.Fatal("Export did not borrow the keeper descriptor")
	}
	for _, field := range []string{"ID", "Network", "Address", "Path", "Device", "Inode", "Cookie", "UnixPathInode", "Family", "SockType", "Nonblock", "CloseOnExec"} {
		t.Run(field, func(t *testing.T) {
			bad := desc
			value := reflect.ValueOf(&bad).Elem().FieldByName(field)
			switch value.Kind() {
			case reflect.String:
				value.SetString("invalid:identity")
			case reflect.Bool:
				value.SetBool(!value.Bool())
			case reflect.Int:
				value.SetInt(value.Int() + 1)
			case reflect.Uint64:
				value.SetUint(value.Uint() + 1)
			default:
				t.Fatalf("uncovered descriptor type: %s", field)
			}
			keeperRefusesAdoption(t, bad, file)
		})
	}
	// Both real flag defects must be rejected without repair. Restore them in
	// the fixture only, then prove the correct inherited socket remains usable.
	for _, flag := range []struct {
		name string
		get  int
		set  int
		bit  int
	}{{"nonblock", unix.F_GETFL, unix.F_SETFL, unix.O_NONBLOCK}, {"cloexec", unix.F_GETFD, unix.F_SETFD, unix.FD_CLOEXEC}} {
		t.Run(flag.name, func(t *testing.T) {
			keeperWithFD(t, file, func(fd uintptr) {
				flags, err := unix.FcntlInt(fd, flag.get, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := unix.FcntlInt(fd, flag.set, flags & ^flag.bit); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := unix.FcntlInt(fd, flag.set, flags); err != nil {
						t.Error(err)
					}
				}()
				keeperRefusesAdoption(t, desc, file)
				if got, err := unix.FcntlInt(fd, flag.get, 0); err != nil || got&flag.bit != 0 {
					t.Fatalf("adoption repaired the invalid shared flag: %v", err)
				}
			})
		})
	}
	// A connected stream has the right family, type and flags but is not a
	// listener. Its actual identity must still fail the SO_ACCEPTCONN guard.
	ln := keeperTestListener(t, "tcp4")
	peer := keeperTestDial(t, "tcp4", ln.Addr().String())
	defer func() { _ = peer.Close() }()
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	connected := keeperDupConn(t, conn)
	defer func() { _ = connected.Close() }()
	keeperRefusesAdoption(t, keeperConnectedDescriptor(t, connected, desc), connected)
	regular, err := os.CreateTemp(t.TempDir(), "not-socket")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = regular.Close() }()
	keeperRefusesAdoption(t, desc, regular)
	keeperRefusesAdoption(t, desc, nil)
	adopted, err := Adopt(desc, file)
	if err != nil {
		t.Fatalf("valid inherited listener refused: %v", err)
	}
	defer func() { _ = adopted.Close() }()
	if _, err := adopted.View().Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("adopted listener admitted before Resume: %v", err)
	}
	if err := adopted.Resume(); err != nil {
		t.Fatal(err)
	}
}

func exerciseKeeperAlias(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "alias.listener")
	defer func() { _ = k.Close() }()
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	desc, resource, err := k.Export()
	if err != nil {
		t.Fatal(err)
	}
	alias := keeperDupFile(t, resource.Files["listener"])
	defer func() { _ = alias.Close() }()
	otherID := desc
	otherID.ID = "second.owner"
	keeperRefusesAdoption(t, otherID, alias)
	keeperRefusesAdoption(t, desc, resource.Files["listener"])
	original, err := k.Detach()
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	adopted, err := Adopt(desc, alias)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adopted.Close() }()
	if err := adopted.Resume(); err != nil {
		t.Fatal(err)
	}
	peer := keeperTestDial(t, "tcp4", desc.Address)
	defer func() { _ = peer.Close() }()
	conn, err := adopted.View().Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	keeperBusinessReply(t, conn, peer, "adopted-original-OFD")
}

func exerciseKeeperUnixReplacement(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "unix"), "unix.original")
	defer func() { _ = k.Close() }()
	desc := k.Descriptor()
	// Rename retains the old inode, so a replacement cannot reuse its number.
	if err := os.Rename(desc.Path, desc.Path+".old"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", desc.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	var stat unix.Stat_t
	if err := unix.Lstat(desc.Path, &stat); err != nil || stat.Ino == desc.UnixPathInode {
		t.Fatalf("replacement inode was not distinct: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(desc.Path); err != nil {
		t.Fatalf("old owner unlinked the replacement: %v", err)
	}
	peer := keeperTestDial(t, "unix", desc.Path)
	defer func() { _ = peer.Close() }()
	conn, err := replacement.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	keeperBusinessReply(t, conn, peer, "replacement-owner")
}

func exerciseKeeperInvalidDeadline(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "deadline.listener")
	defer func() { _ = k.Close() }()
	for _, deadline := range []controlbudget.Deadline{0, -1, 1} {
		if err := k.Pause(deadline); !errors.Is(err, ErrDeadline) {
			t.Fatalf("invalid deadline %d accepted: %v", deadline, err)
		}
	}
	if _, _, err := k.Export(); !errors.Is(err, ErrBusy) {
		t.Fatalf("invalid deadline stopped the live view: %v", err)
	}
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	if err := k.Resume(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed keeper resumed: %v", err)
	}
	if _, _, err := k.Export(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed keeper exported: %v", err)
	}
	if _, err := k.Detach(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed keeper detached: %v", err)
	}
}

func exerciseKeeperPauseAdopt(t *testing.T) {
	t.Helper()
	exerciseKeeperContinuity(t, "tcp4")
	exerciseKeeperContinuity(t, "unix")
	exerciseKeeperAcceptJoin(t)
	exerciseKeeperAcceptedFence(t)
	exerciseKeeperViewCloseJoin(t)
	exerciseKeeperAlias(t)
	// The Unix pristine path covers the no-unlink transfer rather than only
	// the TCP dup lifecycle; the new normal owner must remove that same inode.
	k := keeperTestCapture(t, keeperTestListener(t, "unix"), "inherited.unix")
	defer func() { _ = k.Close() }()
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	desc := k.Descriptor()
	file, err := k.Detach()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	bad := desc
	bad.Family++
	keeperRefusesAdoption(t, bad, file)
	adopted, err := Adopt(desc, file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adopted.Close() }()
	if err := adopted.Resume(); err != nil {
		t.Fatal(err)
	}
	peer := keeperTestDial(t, "unix", desc.Path)
	defer func() { _ = peer.Close() }()
	conn, err := adopted.View().Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	keeperBusinessReply(t, conn, peer, "inherited-unix")
	if err := adopted.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(desc.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new normal owner did not remove its Unix inode: %v", err)
	}
}

func keeperTestListener(t *testing.T, network string) net.Listener {
	t.Helper()
	address := "127.0.0.1:0"
	switch network {
	case "tcp6":
		address = "[::1]:0"
	case "unix", "unixpacket":
		// Unix sockaddr paths are limited to 108 bytes; verbose test names in
		// t.TempDir exceed that limit. This fixture still owns an isolated dir.
		dir, err := os.MkdirTemp("", "listener-test-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		address = filepath.Join(dir, "socket")
	}
	ln, err := net.Listen(network, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func keeperTestCapture(t *testing.T, ln net.Listener, id string) Keeper {
	t.Helper()
	k, err := Capture(ln, id)
	if err != nil {
		if k != nil {
			_ = k.Close()
		}
		t.Fatal(err)
	}
	return k
}

func keeperTestDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(ts.Nano() + int64(upgradebudget.RequestLatency))
}

func keeperTestDial(t *testing.T, network, address string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout(network, address, upgradebudget.RequestLatency)
	if err != nil {
		t.Fatalf("fresh connect refused or failed: %v", err)
	}
	return conn
}

func keeperBusinessReply(t *testing.T, business, peer net.Conn, reply string) {
	t.Helper()
	deadline := time.Now().Add(upgradebudget.RequestLatency)
	if err := business.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write([]byte("request:" + reply)); err != nil {
		t.Fatal(err)
	}
	request := make([]byte, len("request:"+reply))
	if _, err := io.ReadFull(business, request); err != nil || string(request) != "request:"+reply {
		t.Fatalf("business request changed: %q %v", request, err)
	}
	if _, err := business.Write([]byte(reply)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(reply))
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != reply {
		t.Fatalf("business reply changed: %q %v", got, err)
	}
}

func keeperWithFD(t *testing.T, file *os.File, fn func(uintptr)) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(fn); err != nil {
		t.Fatal(err)
	}
}

func keeperDupFile(t *testing.T, file *os.File) *os.File {
	t.Helper()
	var fd int
	keeperWithFD(t, file, func(source uintptr) {
		var err error
		fd, err = unix.FcntlInt(source, unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
	})
	return os.NewFile(uintptr(fd), "prepared-listener-alias")
}

func keeperDupConn(t *testing.T, conn net.Conn) *os.File {
	t.Helper()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd int
	if err := raw.Control(func(source uintptr) {
		var err error
		fd, err = unix.FcntlInt(source, unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	return os.NewFile(uintptr(fd), "connected-non-listener")
}

func keeperRefusesAdoption(t *testing.T, desc Descriptor, file *os.File) {
	t.Helper()
	k, err := Adopt(desc, file)
	if k != nil && !reflect.ValueOf(k).IsNil() {
		// Detach an erroneously accepted alias so the negative-control failure
		// cannot accidentally close the fixture's original owner as cleanup.
		_, _ = k.Detach()
	}
	if k != nil || !errors.Is(err, ErrIdentity) {
		t.Errorf("invalid inherited listener accepted: %v", err)
	}
}

func keeperConnectedDescriptor(t *testing.T, file *os.File, desc Descriptor) Descriptor {
	t.Helper()
	keeperWithFD(t, file, func(fd uintptr) {
		var stat unix.Stat_t
		if err := unix.Fstat(int(fd), &stat); err != nil {
			t.Fatal(err)
		}
		cookie, err := unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
		if err != nil {
			t.Fatal(err)
		}
		address, err := unix.Getsockname(int(fd))
		if err != nil {
			t.Fatal(err)
		}
		local := address.(*unix.SockaddrInet4)
		desc.Device, desc.Inode, desc.Cookie = uint64(stat.Dev), stat.Ino, cookie
		desc.Address = (&net.TCPAddr{IP: net.IP(local.Addr[:]), Port: local.Port}).String()
	})
	return desc
}

func keeperFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[string]int)
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); errors.Is(err, unix.EBADF) {
			continue // The directory read's temporary FD is already closed.
		} else if err != nil {
			t.Fatal(err)
		}
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		identities[fmt.Sprintf("%d:%d:%d:%s", stat.Dev, stat.Ino, stat.Mode, strings.TrimSpace(target))]++
	}
	return identities
}

// The socket really enters the application-side Accept before the barrier. The
// fence must close that unreturned connection and join it before exporting.
// origin: hot-update S6 plan §6.1 / T18
func exerciseKeeperAcceptedFence(t *testing.T) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "racing.listener")
	defer func() { _ = k.Close() }()
	view := k.View().(*acceptView)
	barrier := &keeperAcceptBarrier{Listener: view.ln, accepted: make(chan struct{}), release: make(chan struct{})}
	view.ln = barrier
	peer := keeperTestDial(t, "tcp4", k.Descriptor().Address)
	defer func() { _ = peer.Close() }()
	accepted := make(chan error, 1)
	go func() {
		conn, err := view.Accept()
		if conn != nil {
			_ = conn.Close()
			accepted <- errors.New("socket escaped the pause fence")
			return
		}
		accepted <- err
	}()
	select {
	case <-barrier.accepted:
	case <-time.After(upgradebudget.RequestLatency):
		close(barrier.release)
		_ = k.Close()
		<-accepted
		t.Fatal("native listener did not accept the racing socket")
	}
	// stop is the same admission fence used by Pause. Keeping the accepted
	// connection at the barrier makes the pre-join export refusal deterministic.
	if err := view.stop(); err != nil {
		close(barrier.release)
		<-accepted
		t.Fatal(err)
	}
	if _, _, err := k.Export(); !errors.Is(err, ErrBusy) {
		t.Errorf("unjoined accepted socket exported: %v", err)
	}
	// Hold the real accepted socket beyond a short slice of the production cut
	// budget. Timeout must retain the keeper, and a later Pause must still join.
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
		close(barrier.release)
		<-accepted
		t.Fatal(err)
	}
	shortDeadline := controlbudget.Deadline(now.Nano() + int64(upgradebudget.RequestLatency/100))
	if err := k.Pause(shortDeadline); !errors.Is(err, ErrDeadline) {
		t.Errorf("unjoined Accept ignored the absolute pause deadline: %v", err)
	}
	close(barrier.release)
	if err := k.Pause(keeperTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := <-accepted; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("accepted socket crossed the pause fence: %v", err)
	}
	if err := peer.SetReadDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	if n, err := peer.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("unreturned accepted socket was not closed: n=%d err=%v", n, err)
	}
}

type keeperAcceptBarrier struct {
	net.Listener
	accepted chan struct{}
	release  chan struct{}
}

func (b *keeperAcceptBarrier) Accept() (net.Conn, error) {
	conn, err := b.Listener.Accept()
	close(b.accepted)
	<-b.release
	return conn, err
}

func exerciseKeeperCaptureRefusal(t *testing.T) {
	t.Helper()
	var tcp *net.TCPListener
	var socket *net.UnixListener
	ln := keeperTestListener(t, "tcp4")
	packet := keeperTestListener(t, "unixpacket")
	for _, input := range []struct {
		ln net.Listener
		id string
	}{{nil, "listener"}, {tcp, "listener"}, {socket, "listener"}, {ln, ""}, {ln, "bad:id"},
		{ln, strings.Repeat("x", 257)}, {&keeperAcceptBarrier{Listener: ln}, "wrapper"}, {packet, "packet"}} {
		k, err := Capture(input.ln, input.id)
		if k != nil {
			_ = k.Close()
		}
		if k != nil || !errors.Is(err, ErrIdentity) {
			t.Fatalf("invalid capture input accepted: %v", err)
		}
	}
	// Refusal must leave the caller's listener serving, rather than stealing it
	// or closing it as if an import had already transferred ownership.
	peer := keeperTestDial(t, "tcp4", ln.Addr().String())
	defer func() { _ = peer.Close() }()
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	keeperBusinessReply(t, conn, peer, "refusal-keeps-caller-owner")
}

// Even without an active Accept, export must wait for the duplicate itself to
// close. A close barrier detects done being published before netpoll releases FD.
// origin: hot-update S6 plan §6.1 / T18
func exerciseKeeperViewCloseJoin(t *testing.T) {
	t.Helper()
	exerciseKeeperViewCloseResult(t, false)
	exerciseKeeperViewCloseResult(t, true)
}

func exerciseKeeperViewCloseResult(t *testing.T, fail bool) {
	t.Helper()
	k := keeperTestCapture(t, keeperTestListener(t, "tcp4"), "closing.listener")
	defer func() { _ = k.Close() }()
	view := k.View().(*acceptView)
	barrier := &keeperCloseBarrier{Listener: view.ln, closing: make(chan struct{}), release: make(chan struct{})}
	if fail {
		barrier.closeError = errors.New("injected view close failure")
	}
	view.ln = barrier
	closed := make(chan error, 1)
	go func() { closed <- view.Close() }()
	select {
	case <-barrier.closing:
	case <-time.After(upgradebudget.RequestLatency):
		close(barrier.release)
		<-closed
		t.Fatal("view did not enter native listener close")
	}
	if _, _, err := k.Export(); !errors.Is(err, ErrBusy) {
		t.Errorf("view FD exported before close joined: %v", err)
	}
	if err := k.Resume(); !errors.Is(err, ErrBusy) {
		t.Errorf("view resumed before old duplicate close joined: %v", err)
	}
	close(barrier.release)
	err := <-closed
	if fail {
		if !errors.Is(err, barrier.closeError) {
			t.Fatalf("view close error lost: %v", err)
		}
		if _, _, err := k.Export(); !errors.Is(err, ErrBusy) {
			t.Errorf("failed view close permitted export: %v", err)
		}
		if err := k.Resume(); !errors.Is(err, ErrBusy) {
			t.Errorf("failed view close permitted replacement: %v", err)
		}
		if _, err := k.Detach(); !errors.Is(err, ErrBusy) {
			t.Errorf("failed view close permitted pristine transfer: %v", err)
		}
		if err := k.Close(); !errors.Is(err, barrier.closeError) {
			t.Errorf("normal stop lost the view close error: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := k.Export(); err != nil {
		t.Fatal(err)
	}
}

type keeperCloseBarrier struct {
	net.Listener
	closing    chan struct{}
	release    chan struct{}
	closeError error
}

func (b *keeperCloseBarrier) Close() error {
	close(b.closing)
	<-b.release
	return errors.Join(b.Listener.Close(), b.closeError)
}
