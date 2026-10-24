//go:build linux

package brokerimage

import (
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferRuntimeStopKeepsBackendUntilTrackerJoins(t *testing.T) {
	f := newCutFixture(t)
	slot, server := runtimeFixture(t, f)
	events := []string{}
	owner := &transferOwnerDouble{events: &events, stopErr: errors.New("callback not joined")}
	if err := InstallTransferOwner(slot, owner); err != nil {
		t.Fatal(err)
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err == nil {
		t.Fatal("failed tracker join allowed backend stop")
	}
	if f.store.terminal || f.control.stopped || server.stopped {
		t.Fatal("backend disappeared under an unjoined tracker callback")
	}
	owner.stopErr = nil
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if owner.stopCalls != 2 || !f.store.terminal || !server.stopped {
		t.Fatal("retry lost the fourth current owner")
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil || owner.stopCalls != 2 {
		t.Fatal("completed tracker stop was repeated")
	}
}

// origin: hot-update S7 plan §8.2 / P24
func TestTransferRuntimeRejectsRetiredOwner(t *testing.T) {
	slot := transferImportSlot(t)
	events := []string{}
	old, current := &transferOwnerDouble{events: &events}, &transferOwnerDouble{events: &events}
	if err := InstallTransferOwner(slot, old); err != nil {
		t.Fatal(err)
	}
	if err := InstallTransferOwner(slot, current); err != nil {
		t.Fatal(err)
	}
	if err := InstallTransferOwner(slot, old); err == nil {
		t.Fatal("old callback reinstalled a retired transfer generation")
	}
	if err := slot.StopCurrent(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if old.stopCalls != 0 || current.stopCalls != 1 {
		t.Fatal("terminal cleanup captured an old tracker")
	}
	if err := InstallTransferOwner(slot, current); err == nil {
		t.Fatal("transfer installed during terminal stop")
	}
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferCutRebindsBeforeBusinessAfterExecReturn(t *testing.T) {
	f := newCutFixture(t)
	slot, _ := runtimeFixture(t, f)
	events := []string{}
	owner := &transferOwnerDouble{events: &events}
	currentStore := &cutStore{events: f.events, stamp: cloneStamp(f.store.stamp), lock: f.store.lock}
	currentControl := &cutControl{events: f.events, name: "current"}
	f.store.reopen = currentStore
	f.cfg.NewControl = func(store StoreOwner, _ controlbudget.Deadline) (ControlOwner, error) {
		if store != currentStore {
			return nil, errors.New("retired store binding")
		}
		return currentControl, nil
	}
	owner.beforeRebind = func() error {
		r := slot.(*currentRuntime) // Thaw already holds this slot's latch.
		if r.store != currentStore || r.control != currentControl {
			return errors.New("old backend callback")
		}
		seenAuth := false
		for _, event := range f.events.snapshot() {
			if event == "current.business" {
				return errors.New("business admitted before tracker bind")
			}
			if event == "current.auth" {
				seenAuth = true
			}
		}
		if !seenAuth {
			return errors.New("current NC not ready before tracker bind")
		}
		return nil
	}
	f.cfg.Transfers = owner
	if err := InstallTransferOwner(slot, owner); err != nil {
		t.Fatal(err)
	}
	cut := freezeFixture(t, f)
	if err := BindRuntime(cut, slot); err != nil {
		t.Fatal(err)
	}
	seal, err := f.store.CloseForImage(f.cfg.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	// A seal is sufficient to exercise returned-exec recovery. Export uses the
	// image/resource test separately; no actor receipt is fabricated here.
	cut.(*frozenCut).seal = &seal
	cut.(*frozenCut).closeAttempted = true
	if err := cut.Thaw(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
	if !owner.bound || len(events) != 2 || events[0] != "transfer-freeze" || events[1] != "transfer-bind" {
		t.Fatal("tracker did not rebind exactly once before thaw")
	}
	if err := slot.StopCurrent(f.cfg.Deadline); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.2 / M11 / P24
func TestTransferActiveRollbackExportsCurrentTrackerState(t *testing.T) {
	f := newCutFixture(t)
	events := []string{}
	owner := &transferOwnerDouble{events: &events, ref: xfercontract.ImageRef{Schema: "transfer-v1", ResourceID: "transfers.current", Digest: "current-receipt", Count: 1, EncodedBytes: 23}}
	snapshot, err := sealedstate.New(owner.ref.ResourceID, []byte("new-generation-progress"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshot.Close() }()
	snapshot.Resources[0].Kind = "xfer-state-v1"
	owner.resources = snapshot.Resources
	f.cfg.Transfers = owner
	cut := freezeFixture(t, f)
	seal, err := f.store.CloseForImage(f.cfg.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	bundle, resources, err := cut.Export(seal)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Transfers == nil || *bundle.Transfers != owner.ref || !contains(bundle.RequiredDomains, "transfer-v1") {
		t.Fatal("current tracker or required domain omitted from fresh rollback")
	}
	found := false
	for _, resource := range resources {
		if resource.ID == owner.ref.ResourceID {
			found = resource.Files["state"] == snapshot.Owned[0]
		}
	}
	if !found {
		t.Fatal("transfer sealed state missing from handoff union")
	}
	changed := bundle
	ref := *bundle.Transfers
	ref.Digest = "newer-receipt"
	changed.Transfers = &ref
	if sameProjection(changed, bundle) {
		t.Fatal("final Prepare accepted an obsolete transfer checkpoint")
	}
	if err := cut.Thaw(f.cfg.Deadline); err == nil {
		t.Fatal("closed store thaw lacked a current runtime slot")
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.3 / P24
func TestTransferSealedCapacityUsesWholeCut(t *testing.T) {
	a, err := sealedstate.New("flow.capacity", make([]byte, 17<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, err := sealedstate.New("xfer.capacity", make([]byte, 16<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	b.Resources[0].Kind = "xfer-state-v1"
	if _, err := privateFiles(append(a.Resources, b.Resources...)); err == nil {
		t.Fatal("separate leaf limits exceeded the shared 32 MiB cut limit")
	}
	alias := handoff.Resource{ID: "xfer.alias", Kind: "xfer-state-v1", Files: map[string]*os.File{"state": a.Owned[0]}}
	if err := ValidateResources(append(a.Resources, alias)); err == nil {
		t.Fatal("nested transfer resource gave a sealed OFD two terminal owners")
	}
}

// origin: hot-update S7 plan §11.3 / P24
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("lifecycle", func(t *testing.T) { exerciseTransferLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer image lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "transfer image lifecycle", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	for _, partial := range []bool{false, true} {
		f := newImportFixture(t)
		factory := addTransferFixture(t, f)
		if partial {
			f.config.NewControl = func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
				return f.control, errors.New("partial control")
			}
		}
		image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
		if err != nil {
			closeSalvage(salvage)
			t.Fatal(err)
		}
		slot := transferImportSlot(t)
		err = image.Activate(slot, f.config.Deadline)
		if partial != errors.Is(err, ErrActive) {
			t.Fatalf("partial activation: %v", err)
		}
		if err := slot.StopCurrent(f.config.Deadline); err != nil || factory.imported.stopCalls != 1 {
			t.Fatal("tracker stop lost")
		}
		resources, err := ImportedResources(image)
		if err != nil {
			t.Fatal(err)
		}
		closeSalvage(resources)
	}
	// Both timeout rollback and a current-store reopen exercise the same owner.
	TestTransferCutRebindsBeforeBusinessAfterExecReturn(t)
	TestTransferImportRefusalRetainsPristineSalvage(t)
}

// origin: hot-update S7 plan §8.2 / T39
func TestTransferRefusedCutResumesBorrowersAfterRebind(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRefusedJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "refused transfer cut", beforeG)
	testharness.AssertNoFDLeak(t, "refused transfer cut", beforeFD, 0)
}

type transferJoinControl struct {
	*cutControl
	users     StoreAdmission
	resumeErr error
}

func (c *transferJoinControl) JoinStoreUsers(d controlbudget.Deadline) error {
	if err := c.users.Fence(d); err != nil {
		return err
	}
	return errors.New("retained transfer callback refused cut")
}
func (c *transferJoinControl) ResumeStoreUsers() error {
	if c.resumeErr != nil {
		return c.resumeErr
	}
	c.users.Resume()
	return c.events.call("transfer.users.resume")
}
func (c *transferJoinControl) StartBusiness(d controlbudget.Deadline) error {
	if !c.users.Begin() {
		return errors.New("transfer callback admission remained fenced")
	}
	c.users.End()
	return c.cutControl.StartBusiness(d)
}
func exerciseTransferRefusedJoin(t *testing.T) {
	t.Helper()
	for _, refuseResume := range []bool{false, true} {
		t.Run("retained_callback", func(t *testing.T) {
			f := newCutFixture(t)
			control := &transferJoinControl{cutControl: f.control}
			if refuseResume {
				control.resumeErr = errors.New("current binding not restored")
			}
			events := []string{}
			owner := &transferOwnerDouble{events: &events}
			owner.beforeRebind = func() error {
				if control.users.Begin() {
					control.users.End()
					return errors.New("borrowers resumed before current binding")
				}
				return nil
			}
			f.cfg.Control, f.cfg.Transfers = control, owner
			cut, err := Freeze(f.cfg)
			if !refuseResume {
				if err == nil || cut != nil || !owner.bound || f.store.closed {
					t.Fatalf("refused cut did not restore original borrowers: cut=%v bound=%v err=%v", cut, owner.bound, err)
				}
				return
			}
			if err == nil || cut == nil || f.store.closed {
				t.Fatalf("refusal lost cut or closed store: cut=%v closed=%v err=%v", cut, f.store.closed, err)
			}
			for _, event := range f.events.snapshot() {
				if event == f.control.name+".business" {
					t.Fatal("business started after failed borrower resume")
				}
			}
			control.resumeErr = nil
			if err := cut.Thaw(f.cfg.Deadline); err != nil {
				t.Fatal(err)
			}
			resumed, business := -1, -1
			for i, event := range f.events.snapshot() {
				if event == "transfer.users.resume" {
					resumed = i
				}
				if event == f.control.name+".business" {
					business = i
				}
			}
			if !owner.bound || resumed < 0 || business <= resumed || f.store.closed {
				t.Fatalf("retained owners did not resume in order: %v", f.events.snapshot())
			}
		})
	}
}
