//go:build linux

package drillobserve_test

import (
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// An interrupted receive consumes neither the packet nor its descriptor rights.
// Inject only the interruption; the retry must receive a real kernel packet.
// origin: S10 internal review FINAL-REAL-FAILS F1
func TestRuntimeSampleCustodyRetriesInterruptedReceive(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRuntimeInterruptedReceive(t)
	}
	testharness.AssertNoGoroutineLeak(t, "interrupted custody receive x5", g)
	testharness.AssertNoFDLeak(t, "interrupted custody receive x5", fd, 0)
}

func exerciseRuntimeInterruptedReceive(t *testing.T) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
	if err := unix.Sendmsg(pair[0], []byte("custody-packet"), unix.UnixRights(pair[0]), nil, 0); err != nil {
		t.Fatal(err)
	}
	original := runtimeCustodyRecvmsg
	defer func() { runtimeCustodyRecvmsg = original }()
	calls := 0
	runtimeCustodyRecvmsg = func(fd int, p, oob []byte, flags int) (int, int, int, unix.Sockaddr, error) {
		calls++
		if calls == 1 {
			return 0, 0, 0, nil, unix.EINTR
		}
		return original(fd, p, oob, flags)
	}
	packet, rights := receiveRuntimePacket(t, pair[1])
	defer func() {
		for _, right := range rights {
			_ = unix.Close(right)
		}
	}()
	if calls != 2 || string(packet) != "custody-packet" || len(rights) != 1 {
		t.Fatal("interruption lost or duplicated the real custody packet or rights")
	}
}
