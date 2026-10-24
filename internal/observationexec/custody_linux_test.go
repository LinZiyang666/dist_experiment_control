//go:build linux

package observationexec_test

import (
	"crypto/sha256"
	"encoding/binary"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/LinZiyang666/tether/internal/observationexec"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: S8 internal review activation-and-route-migration F1
func TestObservationExecCannotResetPoisonedCustody(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseObservationPoison(t)
	}
	testharness.AssertNoGoroutineLeak(t, "observation exec poison x5", g)
	testharness.AssertNoFDLeak(t, "observation exec poison x5", fd, 0)
}

func exerciseObservationPoison(t *testing.T) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
	fd, err := unix.MemfdCreate("observation-exec-authority", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	data := append([]byte("TDCFG2\x00\x00"), make([]byte, 24)...)
	binary.LittleEndian.PutUint64(data[8:16], uint64(pair[0]))
	if _, err := unix.Write(fd, data); err != nil {
		t.Fatal(err)
	}
	const seals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
		t.Fatal(err)
	}
	// This package owns the singleton; each exercise restores its unbound state.
	defer observationexec.ResetCustodyForTest()
	identity := sha256.Sum256(data)
	if observationexec.Register(fd, pair[0], identity, nil) == nil {
		t.Fatal("observation custody omitted its poison owner")
	}
	var poison atomic.Bool
	if err := observationexec.Register(fd, pair[0], identity, &poison); err != nil {
		t.Fatal(err)
	}
	if fds, err := observationexec.Descriptors(); err != nil || len(fds) != 2 {
		t.Fatalf("healthy sealed custody rejected: %v %v", fds, err)
	}
	poison.Store(true)
	if fds, err := observationexec.Descriptors(); err == nil || len(fds) != 0 {
		t.Fatal("poisoned observer gained a fresh exec incarnation")
	}
	env := observationexec.Environment([]string{"SAFE=1", "TETHER_TEST_DRILL_OBSERVER=v1", "TETHER_TEST_DRILL_CUSTODY_FD=3"})
	if strings.Join(env, ",") != "SAFE=1" {
		t.Fatal("poisoned observer startup enablers survived exec")
	}
}
