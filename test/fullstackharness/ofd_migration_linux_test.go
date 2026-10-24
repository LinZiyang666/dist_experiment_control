package fullstackharness

import (
	"context"
	"errors"
	"os"
	"runtime"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"testing"

	"golang.org/x/sys/unix"
)

// origin: S10 internal review rv-harness F6
func TestOFDWitnessAcceptsDescriptorMigrationAcrossCut(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseOFDDescriptorMigration(t)
	}
	testharness.AssertNoGoroutineLeak(t, "OFD migration lifecycle", g)
	testharness.AssertNoFDLeak(t, "OFD migration lifecycle", f, 0)
}

// origin: S10 internal review rv-harness F6
func TestOFDWitnessRejectsUnprovenDescriptorMigration(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUnprovenOFDTransition(t)
	}
	testharness.AssertNoGoroutineLeak(t, "OFD transition rejection", g)
	testharness.AssertNoFDLeak(t, "OFD transition rejection", f, 0)
}

func exerciseUnprovenOFDTransition(t *testing.T) {
	t.Helper()
	owner := fixtureStamp(t)
	original, err := os.CreateTemp(t.TempDir(), "original")
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := original.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	w, err := NewOFDWitness(owner, int(original.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	moved, err := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if moved >= 0 {
			if err := unix.Close(moved); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	next := owner
	next.Generation++
	next.CutID = "next-cut"
	next.Sequence++
	next.AtBootNS++
	for _, tc := range []struct {
		name   string
		change func(*contract.Stamp)
	}{
		{"generation-only", func(s *contract.Stamp) { s.CutID = owner.CutID }},
		{"cut-only", func(s *contract.Stamp) { s.Generation = owner.Generation }},
		{"empty-cut", func(s *contract.Stamp) { s.CutID = "" }},
		{"missing-sequence", func(s *contract.Stamp) { s.Sequence = 0 }},
		{"stale-time", func(s *contract.Stamp) { s.AtBootNS = owner.AtBootNS }},
		{"past-generation", func(s *contract.Stamp) { s.Generation = 0 }},
	} {
		bad := next
		tc.change(&bad)
		if _, err := w.Check(t.Context(), bad, moved); err == nil {
			t.Fatalf("unproven transition accepted: %s", tc.name)
		}
	}
	// Matching inode, position and flags still cannot replace KCMP_FILE.
	reopened, err := os.OpenFile(original.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := w.Check(t.Context(), next, int(reopened.Fd())); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("same-inode replacement accepted across cut: %v", err)
	}
	if _, err := w.Check(t.Context(), next, moved); err != nil {
		t.Fatal(err)
	}
	// The original number has been reused by the reopen; subsequent checks
	// must follow the new binding, while still rejecting its loss within-image.
	if _, err := w.Check(t.Context(), next, moved); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Check(t.Context(), owner, moved); err == nil {
		t.Fatal("old stamp replay accepted")
	}
	alias, err := unix.FcntlInt(uintptr(moved), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := unix.Close(alias); err != nil {
			t.Error(err)
		}
	}()
	if err := unix.Close(moved); err != nil {
		t.Fatal(err)
	}
	moved = -1
	if _, err := w.Check(t.Context(), next, alias); err == nil {
		t.Fatal("lost current binding hidden by duplicate")
	}
}

func exerciseOFDDescriptorMigration(t *testing.T) {
	t.Helper()
	owner := fixtureStamp(t)
	original, err := os.CreateTemp(t.TempDir(), "original")
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := original.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	witness, err := NewOFDWitness(owner, int(original.Fd()))
	if err != nil {
		t.Fatalf("SETUP: real KCMP_FILE required: %v", err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := witness.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Reproduce Prepare's F_DUPFD_CLOEXEC and exec's close of the original.
	moved, err := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if moved >= 0 {
			if err := unix.Close(moved); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	next := owner
	next.Generation++
	next.CutID = "next-cut"
	next.Sequence++
	next.AtBootNS++
	// ctx-root: synchronous kernel witness regression.
	if _, err := witness.Check(context.Background(), next, moved); err != nil {
		t.Fatalf("same OFD at the product's new descriptor rejected: %v", err)
	}
}
