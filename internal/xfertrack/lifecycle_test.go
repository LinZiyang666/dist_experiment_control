package xfertrack

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.2 / §11.3 / T13
func TestTransferLifecycleNoLeaks(t *testing.T) {
	goroutines := runtime.NumGoroutine()
	fds := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer tracker lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "transfer tracker lifecycle", fds, 0)
}
func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newFixture(t, "b", "push")
	f.admit(t)
	grant := f.reserve(t)
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	// Enter a real callback barrier. A short Freeze deadline must defer and
	// keep the same owner; it cannot export while that callback can still write.
	entered, release := make(chan struct{}), make(chan struct{})
	f.setResolver(t, resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
		close(entered)
		<-release
		return x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeInflight}, nil
	}))
	canceled := make(chan error, 1)
	go func() {
		_, err := f.owner.Cancel(f.permit.Identity, x.CancelRequest{Origin: f.permit.Origin, Reason: x.CancelRevoked}, f.clock.deadline())
		canceled <- err
	}()
	<-entered
	_, ns, _ := f.clock.clock()
	if frozen, err := f.owner.Freeze(x.Deadline(ns + int64(10*time.Millisecond))); err == nil || frozen != nil {
		t.Fatal("Freeze ignored entered callback")
	}
	close(release)
	requireCode(t, <-canceled, x.CodeCommitInflight)
	f.history.setFailure(true)
	_, err = f.owner.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline())
	if err == nil {
		t.Fatal("offline history unexpectedly acknowledged")
	}
	frozen, err := f.owner.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil || len(resources) != 0 {
		t.Fatalf("export: %v %v", resources, err)
	}
	if len(state.Entries) != 1 || len(state.GrantSlots) != 1 || len(state.PendingTerminal) != 1 || len(state.WatchdogDeadlines) != 1 {
		t.Fatalf("incomplete responsibility closure: %+v", state)
	}
	// The startup scan was empty; exporting it here would discard protection
	// for the row admitted since startup and fail the strict image codec.
	// origin: hot-update S7 plan §5.3 / §8.2 / T36
	if state.Census.Ready || state.Census.Complete || len(state.Census.Entries) != 1 || len(state.Census.Rows) != 1 || state.Census.Rows[0].Revision != state.Entries[0].Revision {
		t.Fatalf("image kept a stale ready census: %+v", state.Census)
	}
	imported, err := Import(f.owner.cfg, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = imported.Stop(f.clock.deadline()) })
	s, err := imported.Status(x.StatusRequest{Identity: f.permit.Identity, Origin: f.permit.Origin}, f.clock.deadline())
	if err != nil || s.State != x.StatusLive {
		t.Fatal("sleeping import lost live protection")
	}
	if _, err = imported.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: grant.ReceiverInstance}, f.clock.deadline()); err == nil {
		t.Fatal("sleeping import granted work")
	}
	// Production constructs the finalizer before activation. Its view must
	// target that next binding, while old views continue to reject callbacks.
	// origin: hot-update S7 plan §8.2 / T13 / T36
	nextView, err := DecisionsForBinding(imported, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = nextView.Read(key(f.permit.Identity), f.clock.deadline()); err == nil {
		t.Fatal("next finalizer reached the sleeping backend")
	}
	if err = imported.Bind(Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if row, err := nextView.Read(key(f.permit.Identity), f.clock.deadline()); err != nil || row.Identity != f.permit.Identity {
		t.Fatalf("activated finalizer retained a stale generation: %+v %v", row.Identity, err)
	}
	f.history.setFailure(false)
	receipt, err := imported.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline())
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("pending terminal did not resume: %v", err)
	}
	// A fresh current cut contains the newly acknowledged receipt, not the
	// pristine image's still-pending decision.
	current, err := imported.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	currentState, _, err := current.Export()
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(state, currentState) || currentState.Entries[0].TerminalDelivery != x.DeliveryHistoryConfirmed {
		t.Fatal("current rollback re-exported pristine state")
	}
	if _, err = current.DetachPristine(); err == nil {
		t.Fatal("ACTIVE owner allowed pristine detach")
	}
	if err = f.owner.Bind(Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err == nil {
		t.Fatal("old pending image accepted newer durable history revision")
	}
	// Thaw on an unchanged backend rejects callbacks captured before its bind.
	g := newFixture(t, "a", "push")
	g.admit(t)
	oldView, err := Decisions(g.owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.owner.Freeze(g.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if err = g.owner.Bind(Bindings{Ledger: g.ledger, OwnerGeneration: 2}, g.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if _, err = oldView.Read(key(g.permit.Identity), g.clock.deadline()); err == nil {
		t.Fatal("old generation callback accessed rebound backend")
	}
	// The original captured view remains fenced while the owner is frozen.
	if _, err = view.Read(key(f.permit.Identity), f.clock.deadline()); err == nil {
		t.Fatal("frozen callback reached old backend")
	}
	if err = g.owner.Stop(g.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if err = imported.Stop(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if err = f.owner.Stop(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.2 / T13
func TestTransferImportRejectsIncompleteResponsibility(t *testing.T) {
	f := newFixture(t, "a", "push")
	f.admit(t)
	f.reserve(t)
	frozen, err := f.owner.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*x.TrackerState){
		func(s *x.TrackerState) { s.WatchdogDeadlines = nil },
		func(s *x.TrackerState) { s.GrantSlots = nil },
		func(s *x.TrackerState) { s.PendingStart = nil },
		func(s *x.TrackerState) { s.Cleanup = nil },
		func(s *x.TrackerState) { s.Entries[0].CanonicalStart.Bytes[0] = '!' },
		func(s *x.TrackerState) { s.GrantSlots[0].Grant.GrantNonce = "other" },
		func(s *x.TrackerState) { s.Entries[0].EntryNonce = "other" },
	} {
		candidate, _, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		mutate(&candidate)
		imported, err := Import(f.owner.cfg, candidate)
		if err == nil {
			_ = imported.Stop(f.clock.deadline())
			t.Fatal("incomplete/corrupt image accepted")
		}
	}
	pristine, err := Import(f.owner.cfg, state)
	if err != nil {
		t.Fatal(err)
	}
	held, err := pristine.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = held.DetachPristine(); err != nil {
		t.Fatal(err)
	}
	if err = pristine.Bind(Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err == nil {
		t.Fatal("detached owner reactivated")
	}
}
