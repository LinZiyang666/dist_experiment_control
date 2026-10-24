package brokerruntime

import (
	"errors"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
)

// origin: S6 internal review SINGLEACK F4 / lead decision D-C / plan §4.2 / T49
func TestLocalMaintenanceEndsOnlyAfterActivationCommits(t *testing.T) {
	lease := brokermaint.LocalLease{Record: brokermaint.Record{CutID: "original-cut"}, ExpiresLocalBootNS: 123}
	original := lease
	var calls []string
	failure := errors.New("maintenance end failed")
	refuse := false
	finish := func(*cluster.Node, brokermaint.LocalLease) error {
		calls = append(calls, "finish")
		if refuse {
			return failure
		}
		return nil
	}
	if err := FinishLocalActivation(&lease, true, nil, finish); err != nil || lease != original || len(calls) != 0 {
		t.Fatalf("pending activation lost its original hold: %v calls=%v lease=%+v", err, calls, lease)
	}
	refuse = true
	if err := FinishLocalActivation(&lease, false, nil, finish); !errors.Is(err, failure) || lease != original {
		t.Fatalf("failed release lost its retained owner: %v lease=%+v", err, lease)
	}
	refuse = false
	if err := FinishLocalActivation(&lease, false, nil, finish); err != nil || lease != (brokermaint.LocalLease{}) || !reflect.DeepEqual(calls, []string{"finish", "finish"}) {
		t.Fatalf("committed activation did not release exactly once: %v calls=%v lease=%+v", err, calls, lease)
	}
	if err := FinishLocalActivation(&lease, false, nil, finish); err != nil || len(calls) != 2 {
		t.Fatal("empty local admission was ended again")
	}
}

// origin: S6 internal review SINGLEACK F2 / lead decision D-C / plan §7.1
func TestGuardReloadRoleRefusesBeforeAnyFreezeStep(t *testing.T) {
	refusal := errors.New("maintenance admission missing")
	steps := 0
	original := &brokerimage.StoreRun[int]{Freeze: func(brokerimage.Config) (brokerimage.Cut, error) {
		steps++
		return nil, nil
	}, WithExecLatch: func(exec func() error) error { return exec() }}
	guarded, _ := GuardReloadRole(original, brokerimage.StoreShutdown{}, func(bool) error { return refusal })
	if _, err := guarded.Freeze(brokerimage.Config{}); !errors.Is(err, refusal) {
		t.Fatalf("freeze did not require maintenance admission: %v", err)
	}
	if steps != 0 {
		t.Fatal("maintenance refusal happened after a cut step")
	}
}

// The reload role is re-validated at every cut boundary that can still refuse:
// reversible preparation, the final pre-close sample and the irreversible exec
// latch (stopped). A refusal stops that step; the caller's reader choice (the
// original run value) is never modified in place.
// origin: hot-update S6 plan §5 / rv-cluster-7 / rv-final F1; S8 §6.3 / §12.1 (P15 move)
func TestGuardReloadRoleResamplesAtEveryCutBoundary(t *testing.T) {
	var samples []bool
	var refuse bool
	validate := func(stopped bool) error {
		samples = append(samples, stopped)
		if refuse {
			return errors.New("role moved")
		}
		return nil
	}
	execs, prepares := 0, 0
	original := &brokerimage.StoreRun[int]{WithExecLatch: func(exec func() error) error { return exec() }}
	guarded, shutdown := GuardReloadRole(original, brokerimage.StoreShutdown{Prepare: func(controlbudget.Deadline) error { prepares++; return nil }}, validate)
	if guarded == original {
		t.Fatal("the caller's run was modified in place")
	}
	if err := shutdown.Prepare(0); err != nil || prepares != 1 {
		t.Fatalf("prepare: %v prepares=%d", err, prepares)
	}
	if err := shutdown.AfterPrepare(); err != nil {
		t.Fatal(err)
	}
	if err := guarded.WithExecLatch(func() error { execs++; return nil }); err != nil || execs != 1 {
		t.Fatalf("exec latch: %v execs=%d", err, execs)
	}
	if len(samples) != 3 || samples[0] || samples[1] || !samples[2] {
		t.Fatalf("role samples = %v, want prepare/after-prepare live and exec stopped", samples)
	}
	refuse = true
	if err := shutdown.Prepare(0); err == nil || prepares != 1 {
		t.Fatal("a lost role still prepared the store to stop")
	}
	if err := shutdown.AfterPrepare(); err == nil {
		t.Fatal("a lost role passed the final pre-close sample")
	}
	if err := guarded.WithExecLatch(func() error { execs++; return nil }); err == nil || execs != 1 {
		t.Fatal("a lost role still crossed the exec latch")
	}
	if err := original.WithExecLatch(func() error { execs++; return nil }); err != nil || execs != 2 {
		t.Fatal("the unguarded original latch changed behaviour")
	}
	// Single mode has no Node: preparation and close are both no-ops.
	single := NodeShutdown(func() *cluster.Node { return nil }, func(*cluster.Node, controlbudget.Deadline) error {
		t.Fatal("prepared a node that does not exist")
		return nil
	})
	if single.Prepare(0) != nil || single.Close() != nil {
		t.Fatal("single-mode shutdown was not inert")
	}
}

// Only this process's own ("local-") admission is restored as the local lease;
// remote holders' leases stay history.
// origin: hot-update S6 plan §4.1; S8 §12.1 (P15 move)
func TestLocalMaintenanceSelectsOnlyTheLocalHolder(t *testing.T) {
	remote := brokermaint.LocalLease{Record: brokermaint.Record{Holder: "roll-1", CutID: "a"}}
	local := brokermaint.LocalLease{Record: brokermaint.Record{Holder: "local-1", CutID: "b"}}
	if got := LocalMaintenance([]brokermaint.LocalLease{remote, local}); got != local {
		t.Fatalf("selected %+v", got)
	}
	if got := LocalMaintenance([]brokermaint.LocalLease{remote}); got != (brokermaint.LocalLease{}) {
		t.Fatalf("a remote holder became the local lease: %+v", got)
	}
}
