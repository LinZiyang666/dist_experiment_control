//go:build linux

package brokerimage

import (
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

type cutImported struct {
	server  *tunnel.Server
	stopped bool
}

func (s *cutImported) Server() *tunnel.Server { return s.server }
func (s *cutImported) Activate(controlbudget.Deadline) error {
	return errors.New("contract test imported server must not activate")
}
func (s *cutImported) DetachPristine() (tunnel.Salvage, error) {
	return tunnel.Salvage{}, errors.New("contract test active server must not detach pristine")
}

func runtimeFixture(t *testing.T, f *cutFixture) (RuntimeSlot, *cutImported) {
	t.Helper()
	server := &cutImported{server: f.cfg.Server}
	slot, err := NewRuntimeSlot(func(owner StoreOwner, _ controlbudget.Deadline) error {
		store := owner.(*cutStore)
		if err := store.events.call("current.store.stop"); err != nil {
			return err
		}
		store.terminal = true
		return store.lock.Close()
	}, func(owner tunnel.ServerImported, _ controlbudget.Deadline) error {
		imported := owner.(*cutImported)
		if err := f.events.call("current.server.stop"); err != nil {
			return err
		}
		imported.stopped = true
		return f.keeper.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := slot.Install(f.store, f.control, server); err != nil {
		t.Fatal(err)
	}
	return slot, server
}

// origin: hot-update S6 plan §1.2 / §4.2 / T02 / §12.1 P01
func TestCurrentRuntimeStopAfterExecError(t *testing.T) {
	f := newCutFixture(t)
	slot, server := runtimeFixture(t, f)
	current := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
	current.stamp.AppliedIndex++
	newControl := &cutControl{events: f.events, name: "current"}
	f.store.reopen = current
	f.cfg.NewControl = func(store StoreOwner, _ controlbudget.Deadline) (ControlOwner, error) {
		if store != current {
			return nil, errors.New("control constructed against captured old store")
		}
		return newControl, f.events.call("control.new")
	}
	cut := freezeFixture(t, f)
	if err := BindRuntime(cut, slot); err != nil {
		t.Fatal(err)
	}
	bundle, resources := exportFixture(t, f, cut)
	prepared, bundle := prepareFixture(t, cut, bundle, resources)
	if err := ExecCommit(cut, prepared, func(Bundle) error { return nil },
		"/nonexistent-broker-image-contract-test", bundle.Args, []string{}); err == nil {
		t.Fatal("exec failure was not exercised")
	}
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	// Simulated new ACK is recorded in the replacement store after auth/flow /
	// business recovered. The normal-stop callback must receive that exact owner.
	current.stamp.AppliedIndex++
	if !f.server.thawed || newControl.stopped || f.store.openN != 1 {
		t.Fatal("current recovery did not reopen once and thaw its original server")
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if !current.terminal || f.store.terminal || !newControl.stopped || f.control.stopped || !server.stopped {
		t.Fatal("normal exit stopped captured old owners instead of the current ACK owner")
	}
	if _, err := f.store.lock.Stat(); err == nil {
		t.Fatal("normal current stop retained the original lock FD")
	}
	if _, err := f.keeper.file.Stat(); err == nil {
		t.Fatal("normal current stop retained the original listener FD")
	}
	got := f.events.snapshot()
	wantTail := []string{"store.reopen", "control.new", "current.auth", "server.thaw", "public.listener.resume", "current.business", "current.transfers.resume", "current.stop", "current.server.stop", "current.store.stop"}
	if !reflect.DeepEqual(got[len(got)-len(wantTail):], wantTail) {
		t.Fatalf("current recovery/stop order: %v", got)
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if err := slot.Install(f.store, f.control, server); err == nil {
		t.Fatal("old runtime reinstalled after normal stop")
	}
}

// origin: hot-update S6 plan §4.2 / T49 / §12.1 P01
func TestBrokerActiveRollbackKeepsNewACK(t *testing.T) {
	f := newCutFixture(t)
	slot, imported := runtimeFixture(t, f)
	active := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
	active.stamp.AppliedIndex = 31 // Candidate already acknowledged a new row.
	active.seal = cloneSeal(f.store.seal)
	active.seal.Stamp = cloneStamp(active.stamp)
	control := &cutControl{events: f.events, name: "active"}
	if err := slot.Install(active, control, imported); err != nil {
		t.Fatal(err)
	}
	if err := slot.Install(f.store, f.control, imported); err == nil {
		t.Fatal("retired pristine owners were allowed to become current again")
	}
	// The rollback input is a new cut of candidate state, not the original
	// exporter stamp. The simulated stream's received/applied prefix is carried
	// in the actual flow.Image projection used by both Export and Prepare.
	f.cfg.Store, f.cfg.Control = active, control
	currentGeneration := uint64(2)
	f.server.version = 3
	f.server.offset = 53
	cut := freezeFixture(t, f)
	seal, err := active.CloseForImage(f.cfg.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	bundle, resources, err := cut.Export(seal)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Store.AppliedIndex != 31 || bundle.Tunnel.Flows.Records[0].RxApplied != 53 {
		t.Fatal("current rollback lost new ACK/byte prefix")
	}
	bundle.Generation = currentGeneration + 1
	prepared, bundle := prepareFixture(t, cut, bundle, resources)
	if err := ExecCommit(cut, prepared, func(sealed Bundle) error {
		if sealed.Generation != currentGeneration+1 || sealed.Store.AppliedIndex != 31 || sealed.Tunnel.Flows.Records[0].RxApplied != 53 {
			t.Fatal("sealed current rollback substituted pristine state")
		}
		return errors.New("rollback contract barrier")
	}, "/missing", bundle.Args, []string{}); err == nil {
		t.Fatal("rollback validation barrier was not exercised")
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if !active.terminal || f.store.terminal || !control.stopped || f.control.stopped {
		t.Fatal("ACTIVE rollback normal stop did not retain the candidate ACK owner")
	}
}

// origin: hot-update S6 plan §3.2 / §4.2 / T49 / §12.1 P01
func TestBrokerCurrentRuntimePartialConstructor(t *testing.T) {
	for _, point := range []string{"store", "control"} {
		t.Run(point, func(t *testing.T) {
			f := newCutFixture(t)
			slot, _ := runtimeFixture(t, f)
			partialStore := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
			partialControl := &cutControl{events: f.events, name: "partial"}
			f.store.reopen = partialStore
			if point == "store" {
				f.store.openErr = errors.New("injected non-nil store constructor error")
			}
			constructed := 0
			f.cfg.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
				constructed++
				return partialControl, errors.New("injected non-nil control constructor error")
			}
			cut := freezeFixture(t, f)
			if err := BindRuntime(cut, slot); err != nil {
				t.Fatal(err)
			}
			_, _ = exportFixture(t, f, cut)
			if err := cut.Thaw(f.cfg.Deadline); err == nil {
				t.Fatal("partial constructor was treated as ready")
			}
			if err := cut.Thaw(f.cfg.Deadline); !errors.Is(err, ErrStoreClose) || f.store.openN != 1 {
				t.Fatalf("partial owner caused a second open: err=%v opens=%d", err, f.store.openN)
			}
			if err := cut.Discard(); err == nil {
				t.Fatal("ACTIVE partial constructor took pristine discard path")
			}
			if (point == "control") != (constructed == 1) {
				t.Fatal("control constructor crossed an unknown store close")
			}
			if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
				t.Fatal(err)
			}
			if !partialStore.terminal || point == "control" && !partialControl.stopped {
				t.Fatal("non-nil failed constructor lost its current owner")
			}
		})
	}
}

// origin: hot-update S6 plan §3.2 / §4.2 / §12.1 P01
func TestBrokerCurrentRuntimeStopRetriesJoin(t *testing.T) {
	f := newCutFixture(t)
	slot, _ := runtimeFixture(t, f)
	f.control.stopHook = func() error {
		r := slot.(*currentRuntime)
		if !r.mu.TryLock() {
			return errors.New("terminal join held the slot mutex")
		}
		r.mu.Unlock()
		return nil
	}
	f.events.fail = "old.stop"
	if err := slot.StopCurrent(f.cfg.Deadline); err == nil {
		t.Fatal("failed control join was ignored")
	}
	if f.store.terminal || f.keeper.closed {
		t.Fatal("failed producer join closed a current data owner")
	}
	if err := slot.Install(f.store, f.control, nil); err == nil {
		t.Fatal("runtime replaced during partial stop")
	}
	f.events.fail = "current.store.stop"
	if err := slot.StopCurrent(f.cfg.Deadline); err == nil {
		t.Fatal("failed store close was ignored")
	}
	f.events.fail = ""
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, event := range f.events.snapshot() {
		counts[event]++
	}
	if counts["old.stop"] != 2 || counts["current.server.stop"] != 1 || counts["current.store.stop"] != 2 {
		t.Fatalf("partial stop repeated completed closes or dropped a failed close: %v", counts)
	}
}

// origin: hot-update S6 plan §1.2 / §4.2 / T02 / §12.1 P01
func TestBrokerCurrentRuntimeReopenStopSerializes(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRuntimeReopenStop(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker current reopen/stop", beforeG)
	testharness.AssertNoFDLeak(t, "broker current reopen/stop", beforeFD, 0)
}

func exerciseRuntimeReopenStop(t *testing.T) {
	t.Helper()
	f := newCutFixture(t)
	slot, _ := runtimeFixture(t, f)
	current := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
	f.store.reopen = current
	f.cfg.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
		return &cutControl{events: f.events, name: "current"}, nil
	}
	cut := freezeFixture(t, f)
	if err := BindRuntime(cut, slot); err != nil {
		t.Fatal(err)
	}
	_, _ = exportFixture(t, f, cut)
	entered, release := make(chan struct{}), make(chan struct{})
	f.store.openHook = func() { close(entered); <-release }
	thawDone, stopDone := make(chan error, 1), make(chan error, 1)
	go func() { thawDone <- cut.Thaw(f.cfg.Deadline) }()
	<-entered
	// No stop contender exists yet; the held mutex must belong to the recovery
	// that is inside its first potential opener, not merely to Install afterwards.
	r := slot.(*currentRuntime)
	if r.mu.TryLock() {
		r.mu.Unlock()
		close(release)
		<-thawDone
		_ = slot.StopCurrent(f.cfg.Deadline)
		t.Fatal("normal stop can release the lock during current store construction")
	}
	go func() { stopDone <- slot.StopCurrent(f.cfg.Deadline) }()
	close(release)
	if err := <-thawDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if !current.terminal || f.store.terminal {
		t.Fatal("normal stop captured the old store during recovery")
	}
}
