//go:build linux

package agentupgradeport

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// origin: S8 internal review activation-and-route-migration F10
func TestPendingReverseBorrowsTheInheritedDomainLease(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePendingReverseLease(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pending reverse inherited OFD x5", g)
	testharness.AssertNoFDLeak(t, "pending reverse inherited OFD x5", fd, 0)
}

func exercisePendingReverseLease(t *testing.T) {
	t.Helper()
	r := publicationEvidenceFixture(t)
	g, key := r.Grant, r.Grant.Handle.Key
	lease, err := upgradelock.Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if lease != nil {
			_ = lease.Close()
		}
	}()
	// A pending reverse retains its forward identity in the same journal.
	reverse := g.Handle
	reverse.CutID = "44444444444444444444444444444444"
	reverse.Generation++
	view := &upgradetx.View{Historical: upgradetx.Record{Key: key, Grant: g,
		Forward: upgradetx.Attempt{Handle: g.Handle},
		Reverse: upgradetx.Attempt{Handle: reverse, Direction: upgradetx.DirectionReverse, SourceSHA: g.TargetSHA, TargetSHA: g.SourceSHA}}}
	p := NewRuntime(Host{Key: key, Journal: recordJournal{view: view}, Inherited: &upgradecut.State{Attempt: g.Handle, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA}}).(*runtimePort)
	var mu sync.Mutex
	BindInheritedLease(p, &lease, &mu)
	if p.host.Lease == nil {
		t.Fatal("pending reverse has no inherited-lease binding")
	}
	for _, mutate := range []func(*upgradetx.Attempt){
		func(a *upgradetx.Attempt) { a.Handle.TxID = "other" },
		func(a *upgradetx.Attempt) { a.Handle.Generation++ },
		func(a *upgradetx.Attempt) { a.SourceSHA = g.SourceSHA },
		func(a *upgradetx.Attempt) { a.Direction = upgradetx.DirectionForward },
	} {
		good := view.Historical.Reverse
		mutate(&view.Historical.Reverse)
		if borrowed, err := p.host.Lease(t.Context(), key); err == nil || borrowed != nil {
			t.Fatal("unrelated attempt borrowed the pending cut's lease")
		}
		view.Historical.Reverse = good
	}
	borrowed, err := p.host.Lease(t.Context(), key)
	if err != nil || borrowed == nil || borrowed.FD() == lease.FD() || borrowed.DomainID() != key.DomainID {
		t.Fatalf("current reverse self-locked instead of borrowing the OFD: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	// A separately opened descriptor probes the real kernel reservation;
	// an in-memory owner map alone cannot prove that Close avoided LOCK_UN.
	var stat unix.Stat_t
	if err := unix.Fstat(int(lease.FD()), &stat); err != nil {
		t.Fatal(err)
	}
	path, err := os.Readlink(filepath.Join("/proc/self/fd", strconv.FormatUint(uint64(lease.FD()), 10)))
	if err != nil {
		t.Fatal(err)
	}
	contender, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("borrowed cleanup unlocked the pending owner's OFD: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease = nil
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("last owner did not release the kernel reservation: %v", err)
	}
	if err := contender.Close(); err != nil {
		t.Fatal(err)
	}
	acquired, err := p.host.Lease(t.Context(), key)
	if err != nil {
		t.Fatalf("completed owner could not acquire its next reservation: %v", err)
	}
	_ = acquired.Close()
}
