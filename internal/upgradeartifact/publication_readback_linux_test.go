//go:build linux

package upgradeartifact

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Kernel read-byte accounting pins the avoided whole-image pass without a
// throughput or timing assumption. The real files, fsyncs, receipt and final
// SHA verification remain exercised; only authority and decoder are test ports.
// origin: S8 internal review ROOT-STAGE-LATENCY F2
func TestPublicationDoesNotRepeatFinalWholeImageReadback(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePublicationReadBudget(t)
	}
	testharness.AssertNoGoroutineLeak(t, "publication read budget", g)
	testharness.AssertNoFDLeak(t, "publication read budget", fd, 0)
}

func exercisePublicationReadBudget(t *testing.T) {
	t.Helper()
	f := newPublicationFixture(t)
	f.source = bytes.Repeat([]byte("s"), 2<<20)
	f.target = bytes.Repeat([]byte("t"), 2<<20)
	f.grant.SourceSHA, f.grant.TargetSHA = digest(f.source), digest(f.target)
	f.port.grant = f.grant
	before := publicationReadBytes(t)
	_ = publishFixture(t, f)
	read := publicationReadBytes(t) - before
	t.Logf("publication readback bytes=%d", read)
	// The input readers are in memory. Each file is read during immutable-slot
	// readback, the fixture's decoder and final publication readback. Metadata
	// gets headroom well below one extra image, so another pass cannot hide in it.
	limit := uint64(3*(len(f.source)+len(f.target)) + 128<<10)
	if read > limit {
		t.Fatalf("publication repeated a whole-image readback: read=%d limit=%d", read, limit)
	}
}

// ReadPublication must remain usable for historical grants, while a new
// publication must refuse a grant that expires during its last directory Sync.
// origin: S8 internal review ROOT-STAGE-LATENCY F2
func TestPublicationRejectsGrantExpiryDuringReceiptSync(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePublicationSyncExpiry(t)
	}
	testharness.AssertNoGoroutineLeak(t, "publication sync expiry", g)
	testharness.AssertNoFDLeak(t, "publication sync expiry", fd, 0)
}

func exercisePublicationSyncExpiry(t *testing.T) {
	t.Helper()
	f := newPublicationFixture(t)
	f.grant.ExpiresAt = time.Now().Add(time.Second).UnixNano()
	f.port.grant = f.grant
	reg, err := upgradelock.RegistrationFor(f.grant.Handle.Key)
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	receipt, err := publishWithReceipt(WithPublicationPort(t.Context(), f.port), f.grant,
		bytes.NewReader(f.source), bytes.NewReader(f.target), func(file *os.File) error {
			if err := file.Sync(); err != nil {
				return err
			}
			info, err := file.Stat()
			if err != nil || !info.IsDir() || filepath.Clean(file.Name()) != reg.ControlDir {
				return err
			}
			reached = true
			timer := time.NewTimer(time.Until(time.Unix(0, f.grant.ExpiresAt)) + time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return nil
			case <-t.Context().Done():
				return t.Context().Err()
			}
		}, true)
	if !reached {
		t.Fatal("expiry injection did not reach receipt directory Sync")
	}
	if err == nil || receipt != (Receipt{}) {
		t.Fatalf("expired publication returned success: %v", err)
	}
	if failure, ok := err.(*upgradetx.Error); !ok || failure.Code != upgradetx.CodeGrantExpired {
		t.Fatalf("expired publication classification changed: %v", err)
	}
	if _, err := ReadPublication(reg.ControlDir, f.grant); err != nil {
		t.Fatalf("expiry must not destroy immutable historical evidence: %v", err)
	}
}

func publicationReadBytes(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if value, ok := strings.CutPrefix(line, "rchar: "); ok {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatal("kernel read accounting has no rchar")
	return 0
}

// Moving the single final verification after receipt persistence must still
// catch corruption at the last Sync, for either publication entry point.
// origin: S8 internal review ROOT-STAGE-LATENCY F2
func TestPublicationFinalReadbackRejectsLateImageChanges(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePublicationLateChanges(t)
	}
	testharness.AssertNoGoroutineLeak(t, "publication late changes", g)
	testharness.AssertNoFDLeak(t, "publication late changes", fd, 0)
}

func exercisePublicationLateChanges(t *testing.T) {
	t.Helper()
	for _, register := range []bool{false, true} {
		for _, object := range []string{"source", "target", "manifest"} {
			t.Run(fmt.Sprintf("registered=%t/%s", register, object), func(t *testing.T) {
				f := newPublicationFixture(t)
				reg, err := upgradelock.RegistrationFor(f.grant.Handle.Key)
				if err != nil {
					t.Fatal(err)
				}
				changed := false
				receipt, err := publishWithReceipt(WithPublicationPort(t.Context(), f.port), f.grant,
					bytes.NewReader(f.source), bytes.NewReader(f.target), func(file *os.File) error {
						if err := file.Sync(); err != nil {
							return err
						}
						info, err := file.Stat()
						if err != nil || !info.IsDir() || changed {
							return err
						}
						if register && filepath.Clean(file.Name()) != reg.ControlDir || !register && filepath.Clean(file.Name()) != f.location.Directory {
							return nil
						}
						manifests, err := filepath.Glob(filepath.Join(f.location.Directory, "manifest-*"))
						if err != nil || len(manifests) == 0 {
							return err
						}
						path, mode := filepath.Join(f.location.Directory, f.grant.SourceSHA), os.FileMode(0555)
						switch object {
						case "target":
							path = filepath.Join(f.location.Directory, f.grant.TargetSHA)
						case "manifest":
							path, mode = manifests[0], 0444
						}
						if err := os.Chmod(path, 0600); err != nil {
							return err
						}
						if err := os.WriteFile(path, []byte("late corruption"), 0600); err != nil {
							return err
						}
						changed = true
						return os.Chmod(path, mode)
					}, register)
				if !changed {
					t.Fatal("late corruption did not reach the final durability boundary")
				}
				if err == nil || receipt != (Receipt{}) {
					t.Fatalf("late %s corruption produced a success receipt: %v", object, err)
				}
				requireRefusal(t, err)
			})
		}
	}
}
