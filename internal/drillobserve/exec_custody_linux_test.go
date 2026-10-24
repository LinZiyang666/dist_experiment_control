//go:build linux

package drillobserve

import (
	"crypto/sha256"
	"encoding/binary"
	"reflect"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: S8 internal review activation-and-route-migration F1
func TestExecCustodyRequiresBothSealedProtocols(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseExecCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "exec custody protocols x5", g)
	testharness.AssertNoFDLeak(t, "exec custody protocols x5", fd, 0)
}

func exerciseExecCustody(t *testing.T) {
	t.Helper()
	previous := active
	active = nil
	defer func() { active = previous }()
	if fds, err := ExecDescriptors(); err != nil || len(fds) != 0 {
		t.Fatal("ordinary process acquired observation inheritance")
	}
	for _, version := range []string{"TDCFG1\x00\x00", "TDCFG2\x00\x00"} {
		pair := transferSocketPair(t)
		func() {
			defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
			data := append([]byte(version), make([]byte, 24)...)
			binary.LittleEndian.PutUint64(data[8:16], uint64(pair[0]))
			create := func(sealed bool) int {
				fd, err := unix.MemfdCreate("exec-custody-authority", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := unix.Write(fd, data); err != nil {
					_ = unix.Close(fd)
					t.Fatal(err)
				}
				if sealed {
					if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, custodySeals); err != nil {
						_ = unix.Close(fd)
						t.Fatal(err)
					}
				}
				return fd
			}
			sealed, unsealed := create(true), create(false)
			defer func() { _ = unix.Close(sealed); _ = unix.Close(unsealed) }()
			makeActive := func() { active = &custody{fd: sealed, channel: pair[0], identity: sha256.Sum256(data)} }
			makeActive()
			if fds, err := ExecDescriptors(); err != nil || !reflect.DeepEqual(fds, []int{sealed, pair[0]}) {
				t.Fatalf("validated protocol %q omitted from exact inheritance: %v %v", version, fds, err)
			}
			for _, defect := range []string{"identity", "channel", "unsealed"} {
				makeActive()
				switch defect {
				case "identity":
					active.identity[0] ^= 1
				case "channel":
					active.channel = pair[1]
				case "unsealed":
					active.fd = unsealed
				}
				if fds, err := ExecDescriptors(); err == nil || len(fds) != 0 {
					t.Fatalf("%s custody authorized a foreign descriptor", defect)
				}
			}
			makeActive()
			active.broken.Store(true)
			if fds, err := ExecDescriptors(); err != nil || len(fds) != 0 {
				t.Fatal("poisoned custody acquired inheritance")
			}
		}()
	}
}
