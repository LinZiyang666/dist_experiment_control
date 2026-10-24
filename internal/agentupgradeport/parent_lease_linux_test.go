//go:build linux

package agentupgradeport

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func TestParentLeaseQueriesAndReleaseShareOneFence(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseParentLeaseFence(t)
	}
	testharness.AssertNoGoroutineLeak(t, "parent lease couriers", beforeG)
	testharness.AssertNoFDLeak(t, "parent lease couriers", beforeFD, 0)
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4 / hot-update S8 plan §5.1
func TestBorrowedRuntimeLeaseKeepsOriginalReservation(t *testing.T) {
	fd, g := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		exerciseBorrowedRuntimeLease(t)
	}
	testharness.AssertNoFDLeak(t, "pending reverse lease x5", fd, 0)
	testharness.AssertNoGoroutineLeak(t, "pending reverse lease x5", g)
}

func exerciseBorrowedRuntimeLease(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "image")
	if err := os.WriteFile(binary, []byte("registered image"), 0700); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node", UID: uint32(os.Geteuid())}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: binary, ControlDir: dir,
		DeploymentID: "pending-reverse", Participants: []upgradelock.Participant{{Key: key, ServiceID: "agent"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = domain
	original, err := upgradelock.Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = original.Close() }()
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: "instance"}
	cut := upgradecut.State{Attempt: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2}, Owner: owner, TargetSHA: strings.Repeat("b", 64)}
	view := upgradetx.View{Historical: upgradetx.Record{Grant: upgradetx.Grant{Issuer: owner},
		Reverse: upgradetx.Attempt{Direction: upgradetx.DirectionReverse, Handle: upgradetx.Handle{Key: key, TxID: cut.Attempt.TxID, CutID: strings.Repeat("3", 32), Generation: 3}, SourceGeneration: 2, SourceSHA: cut.TargetSHA}}}
	journal := retirementJournal{records: map[string]upgradetx.View{"": view}}
	var mu sync.Mutex
	borrowed, err := AcquireRuntimeLease(t.Context(), key, journal, &cut, &original, &mu)
	if err != nil || borrowed.FD() == original.FD() {
		t.Fatalf("pending reverse did not duplicate inherited OFD custody: %v", err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*upgradetx.View){
		"foreign-owner":           func(v *upgradetx.View) { v.Historical.Grant.Issuer.InstanceID = "foreign" },
		"non-adjacent-generation": func(v *upgradetx.View) { v.Historical.Reverse.Handle.Generation++ },
		"wrong-source-generation": func(v *upgradetx.View) { v.Historical.Reverse.SourceGeneration-- },
		"wrong-running-image":     func(v *upgradetx.View) { v.Historical.Reverse.SourceSHA = strings.Repeat("c", 64) },
		"new-forward":             func(v *upgradetx.View) { v.Historical.Reverse.Direction = upgradetx.DirectionForward },
	} {
		bad := view
		mutate(&bad)
		unrelated := retirementJournal{records: map[string]upgradetx.View{"": bad}}
		if got, err := AcquireRuntimeLease(t.Context(), key, unrelated, &cut, &original, &mu); err == nil {
			_ = got.Close()
			t.Fatalf("%s borrowed the inherited reservation", name)
		}
	}
	if contender, err := upgradelock.Acquire(t.Context(), key, ""); err == nil {
		_ = contender.Close()
		t.Fatal("closing the reverse duplicate released the pending forward's lock")
	}
	foreign := key
	foreign.ConfiguredNID = "other"
	if got, err := BorrowRuntimeLease(original, foreign, ""); err == nil {
		_ = got.Close()
		t.Fatal("a foreign component borrowed a pending cut's execution authority")
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := upgradelock.Acquire(t.Context(), key, "")
	if err != nil {
		t.Fatalf("last owner did not release the reservation: %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}
func exerciseParentLeaseFence(t *testing.T) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "lease"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var lease upgradelock.Lease = fileLease{f}
	var mu sync.Mutex
	has, release := ParentLeaseAccess(&lease, &mu)
	if !has() {
		t.Fatal("live parent lease is missing")
	}
	var joined sync.WaitGroup
	start := make(chan struct{})
	failures := make(chan error, 32)
	for range 32 {
		joined.Go(func() {
			<-start
			for range 16 {
				_ = has()
			}
			if err := release(); err != nil {
				failures <- err
			}
		})
	}
	close(start)
	joined.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("parent release was not idempotent: %v", err)
	}
	if has() {
		t.Fatal("released parent lease remained visible")
	}
}
