package xferbridge

// Real sealed descriptors and bridge lifetime transitions; the tracker and
// encoder are explicit contract doubles. These tests do not certify P11/P18
// leaf semantics or P32's actual same-PID broker exec.
import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

type bridgeTracker struct {
	xfertrack.Manager
	state     xfercontract.TrackerState
	bindings  xfertrack.Bindings
	freezeErr error
	bindErr   error
	stopErr   error
	frozen    bool
	detached  bool
	stopCalls int
}

func (m *bridgeTracker) Freeze(controlbudget.Deadline) (xfertrack.Frozen, error) {
	m.frozen = true
	return &bridgeFrozen{manager: m}, m.freezeErr
}
func (m *bridgeTracker) Bind(b xfertrack.Bindings, _ controlbudget.Deadline) error {
	if !m.frozen {
		return errors.New("bind outside tracker fence")
	}
	if m.bindErr != nil {
		return m.bindErr
	}
	m.bindings, m.frozen = b, false
	return nil
}
func (m *bridgeTracker) Stop(controlbudget.Deadline) error {
	m.stopCalls++
	return m.stopErr
}

type bridgeFrozen struct{ manager *bridgeTracker }

func (f *bridgeFrozen) Export() (xfercontract.TrackerState, []handoff.Resource, error) {
	if !f.manager.frozen {
		return xfercontract.TrackerState{}, nil, ErrOwner
	}
	return f.manager.state, nil, nil
}
func (f *bridgeFrozen) DetachPristine() (xfercontract.Salvage, error) {
	f.manager.detached = true
	return xfercontract.Salvage{}, nil
}

type bridgeLedger struct{ xfercontract.LedgerPort }

type bridgeCapacity struct {
	reservations map[xfercontract.ReservationID]xfercontract.Reservation
	next         int
	peak         int
	err          error
}

func (a *bridgeCapacity) Reserve(r xfercontract.Reservation) (xfercontract.ReservationID, error) {
	if a.err != nil {
		return "", a.err
	}
	a.next++
	id := xfercontract.ReservationID(strconv.Itoa(a.next))
	if a.reservations == nil {
		a.reservations = make(map[xfercontract.ReservationID]xfercontract.Reservation)
	}
	a.reservations[id] = r
	if len(a.reservations) > a.peak {
		a.peak = len(a.reservations)
	}
	return id, nil
}
func (a *bridgeCapacity) Release(id xfercontract.ReservationID) error {
	if _, ok := a.reservations[id]; !ok {
		return errors.New("unknown or repeated capacity release")
	}
	delete(a.reservations, id)
	return nil
}

type bridgeSource struct {
	source *handofftest.DupSource
	fail   bool
}

func (s bridgeSource) TakeFile(ref string) (*os.File, error) {
	id, name, present := strings.Cut(ref, "/")
	if !present || name != "state" {
		return nil, errors.New("adapter did not pass explicit ID/name")
	}
	file, err := s.source.TakeFile(id, name)
	if s.fail {
		return file, errors.New("partial Take")
	}
	return file, err
}

func bridgeFixture(t *testing.T) (*Owner, *bridgeTracker, *bridgeLedger, Config) {
	t.Helper()
	ledger := &bridgeLedger{}
	state := xfercontract.TrackerState{Schema: "test-tracker-contract", Generation: 4,
		Entries: []xfercontract.LedgerRow{{EntryNonce: "original-nonce", Revision: 8,
			StartedAt: time.Unix(10, 0).UTC(), Phase: xfercontract.PhaseCommitReserved}}}
	manager := &bridgeTracker{state: state}
	cfg := Config{Tracker: xfertrack.Config{Base: xfercontract.Base{Capacity: &bridgeCapacity{}}}, ResourceID: "transfers.original", Encode: func(s xfercontract.TrackerState) ([]byte, error) { return json.Marshal(s) },
		Decode: func(data []byte) (xfercontract.TrackerState, error) {
			var s xfercontract.TrackerState
			err := json.Unmarshal(data, &s)
			return s, err
		},
		Import: func(_ xfertrack.Config, s xfercontract.TrackerState) (xfertrack.Manager, error) {
			return &bridgeTracker{state: s}, nil
		},
		CurrentBindings: func(controlbudget.Deadline) (xfertrack.Bindings, error) {
			return xfertrack.Bindings{Ledger: ledger, OwnerGeneration: 4}, nil
		}}
	owner, err := New(cfg, manager)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Stop(controlbudget.Deadline(1 << 62)) })
	return owner, manager, ledger, cfg
}

func exportBridgeFixture(t *testing.T, owner *Owner) (xfercontract.ImageRef, []handoff.Resource) {
	t.Helper()
	if err := owner.FreezeImage(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	ref, resources, err := owner.ExportImage()
	if err != nil {
		t.Fatal(err)
	}
	return ref, resources
}

// origin: hot-update S7 plan §8.2 / P24
func TestTransferBridgeImportsSealedStateBeforeBinding(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	owner, manager, _, _ := bridgeFixture(t)
	ref, resources := exportBridgeFixture(t, owner)
	source := handofftest.DupResources(t, resources)
	current, salvage, err := owner.ImportImage(ref, bridgeSource{source: source}, controlbudget.Deadline(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBridgeResources(salvage.Resources)
	imported := current.(*Owner).manager.(*bridgeTracker)
	if !imported.frozen || imported.bindings.OwnerGeneration != 0 || !reflect.DeepEqual(imported.state, manager.state) {
		t.Fatal("import discarded decisions or activated callbacks before installation")
	}
	if err := current.Rebind(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if imported.frozen || imported.bindings.OwnerGeneration != 4 {
		t.Fatal("sleeping owner was not bound to current generation")
	}
	if _, err := current.DetachImage(); err == nil {
		t.Fatal("ACTIVE transfer restored pristine state")
	}
	if err := current.Stop(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if _, err := resources[0].Files["state"].Stat(); err != nil {
		t.Fatal("import closed original export rather than its own decode-only descriptor")
	}
}

// origin: hot-update S7 plan §8.3 / P24
func TestTransferBridgeRejectsAlteredSealedReference(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	owner, _, _, _ := bridgeFixture(t)
	ref, resources := exportBridgeFixture(t, owner)
	for _, defect := range []string{"digest", "count", "size", "partial-take", "missing"} {
		t.Run(defect, func(t *testing.T) {
			bad := ref
			source := handofftest.DupResources(t, resources)
			switch defect {
			case "digest":
				bad.Digest = strings.Repeat("0", 64)
			case "count":
				bad.Count++
			case "size":
				bad.EncodedBytes++
			case "missing":
				bad.ResourceID = "absent"
			}
			current, salvage, err := owner.ImportImage(bad, bridgeSource{source: source, fail: defect == "partial-take"}, controlbudget.Deadline(1<<62))
			defer closeBridgeResources(salvage.Resources)
			if err == nil || current != nil {
				t.Fatal("altered sealed reference activated an owner")
			}
			if defect != "missing" && (len(salvage.Resources) != 1 || salvage.Resources[0].Files["state"] == nil) {
				t.Fatal("file+error was omitted from salvage")
			}
		})
	}
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferBridgeRebindUsesCurrentBackend(t *testing.T) {
	owner, manager, oldLedger, cfg := bridgeFixture(t)
	currentLedger := &bridgeLedger{}
	currentGeneration := uint64(4)
	activeLedger := oldLedger
	owner.cfg.CurrentBindings = func(controlbudget.Deadline) (xfertrack.Bindings, error) {
		return xfertrack.Bindings{Ledger: activeLedger, OwnerGeneration: currentGeneration}, nil
	}
	manager.freezeErr = errors.New("callback join deadline")
	if err := owner.FreezeImage(controlbudget.Deadline(1 << 62)); err == nil {
		t.Fatal("timeout falsely completed freeze")
	}
	activeLedger, currentGeneration = currentLedger, 5
	if err := owner.Rebind(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if manager.bindings.Ledger != currentLedger || manager.bindings.OwnerGeneration != 5 || manager.frozen {
		t.Fatal("returned exec rebound a retired backend or failed to thaw")
	}
	manager.freezeErr = nil
	if err := owner.FreezeImage(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Bind(xfertrack.Bindings{Ledger: oldLedger, OwnerGeneration: 4}, controlbudget.Deadline(1<<62)); err == nil {
		t.Fatal("old backend callback regressed current generation")
	}
	if _, err := New(cfg, nil); err != nil {
		t.Fatal("pure importer configuration rejected")
	}
}

// origin: hot-update S7 plan §8.2 / P24
func TestTransferBridgePartialImportAndStopKeepResponsibility(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	owner, _, _, _ := bridgeFixture(t)
	ref, resources := exportBridgeFixture(t, owner)
	partial := &bridgeTracker{stopErr: errors.New("callback still running")}
	owner.cfg.Import = func(_ xfertrack.Config, state xfercontract.TrackerState) (xfertrack.Manager, error) {
		partial.state = state
		return partial, errors.New("partial import")
	}
	source := handofftest.DupResources(t, resources)
	current, salvage, err := owner.ImportImage(ref, bridgeSource{source: source}, controlbudget.Deadline(1<<62))
	defer closeBridgeResources(salvage.Resources)
	if err == nil || current == nil {
		t.Fatal("partial tracker disappeared behind import error")
	}
	if _, err := current.DetachImage(); err != nil || !partial.detached {
		t.Fatal("pristine partial tracker was not detached")
	}
	if err := owner.Stop(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if _, err := resources[0].Files["state"].Stat(); err == nil {
		t.Fatal("terminal owner leaked its export memfd")
	}
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferBridgePartialBindCannotRegressGeneration(t *testing.T) {
	owner, manager, ledger, _ := bridgeFixture(t)
	if err := owner.FreezeImage(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	manager.bindErr = errors.New("partial current backend activation")
	if err := owner.Bind(xfertrack.Bindings{Ledger: ledger, OwnerGeneration: 6}, controlbudget.Deadline(1<<62)); err == nil {
		t.Fatal("partial Bind error was lost")
	}
	manager.bindErr = nil
	if err := owner.Bind(xfertrack.Bindings{Ledger: ledger, OwnerGeneration: 5}, controlbudget.Deadline(1<<62)); err == nil {
		t.Fatal("partial ACTIVE progress authorized an older backend generation")
	}
	if _, err := owner.DetachImage(); err == nil {
		t.Fatal("partial Bind authorized pristine rollback")
	}
}

// origin: hot-update S7 plan §8.2 / §8.3 / P24
func TestTransferBridgeProjectionPreservesTransferResource(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	owner, _, _, _ := bridgeFixture(t)
	ref, resources := exportBridgeFixture(t, owner)
	cut := &Cut{projection: func() (Projection, error) {
		return Projection{Generation: 6, CutID: "current", Maintenance: []brokermaint.LocalLease{}}, nil
	}}
	bundle, merged, err := cut.project(brokerimage.Bundle{RequiredDomains: []string{brokerimage.Domain, Domain}, Transfers: &ref}, resources)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.state.Close() }()
	if bundle.Transfers == nil || *bundle.Transfers != ref || !hasDomain(bundle.RequiredDomains, Domain) || len(merged) != 2 {
		t.Fatal("broker projection omitted transfer domain or nested sealed resource")
	}
	data, _, err := sealedstate.Read(borrowBridgeSource{merged}, bundle.StateResource, "state", sealedstate.MaxStateBytes)
	if err != nil {
		t.Fatal(err)
	}
	private, err := brokerimage.DecodePrivateState(data)
	if err != nil || len(private.Resources) != 1 || private.Resources[0].ID != ref.ResourceID || private.Resources[0].Kind != StateKind {
		t.Fatal("broker sealed provenance lost the transfer owner")
	}
}

// origin: hot-update S7 plan §8.3 / P24
func TestTransferBridgeReservesExportBeforeDescriptorAllocation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	owner, _, _, _ := bridgeFixture(t)
	capacity := owner.cfg.Tracker.Capacity.(*bridgeCapacity)
	if err := owner.FreezeImage(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	capacity.err = errors.New("shared cut capacity exhausted")
	before := testharness.FDCount()
	if _, _, err := owner.ExportImage(); err == nil {
		t.Fatal("export opened a descriptor without shared capacity")
	}
	if testharness.FDCount() != before || owner.snapshot != nil {
		t.Fatal("failed reservation left a sealed descriptor")
	}
	capacity.err = nil
	ref, _, err := owner.ExportImage()
	if err != nil {
		t.Fatal(err)
	}
	for _, reservation := range capacity.reservations {
		if reservation.SealedBytes != int64(ref.EncodedBytes)+16 || reservation.NonReplayBytes != int64(ref.EncodedBytes) || reservation.FDs != 3 || reservation.Resources != 1 {
			t.Fatal("export or Prepared/witness peak was not charged")
		}
	}
	if _, _, err := owner.ExportImage(); err != nil {
		t.Fatal(err)
	}
	if capacity.peak != 2 || len(capacity.reservations) != 1 {
		t.Fatal("re-export dropped its old reservation before allocating replacement")
	}
	if err := owner.Stop(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if len(capacity.reservations) != 0 {
		t.Fatal("terminal export capacity was not released")
	}
}

type borrowBridgeSource struct{ resources []handoff.Resource }

func (s borrowBridgeSource) TakeFile(id, name string) (*os.File, error) {
	for _, resource := range s.resources {
		if resource.ID == id {
			return resource.Files[name], nil
		}
	}
	return nil, ErrOwner
}

func closeBridgeResources(resources []handoff.Resource) {
	for _, resource := range resources {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
}

// origin: hot-update S7 plan §11.3 / P24
func TestTransferLifecycleNoLeaks(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("sealed transfer images require Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("lifecycle", func(t *testing.T) { exerciseTransferLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer bridge lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "transfer bridge lifecycle", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	owner, manager, _, _ := bridgeFixture(t)
	_, resources := exportBridgeFixture(t, owner)
	manager.stopErr = errors.New("join timeout")
	if err := owner.Stop(controlbudget.Deadline(1 << 62)); err == nil {
		t.Fatal("stop ignored unjoined callback")
	}
	if _, err := resources[0].Files["state"].Stat(); err != nil {
		t.Fatal("failed join prematurely closed its snapshot")
	}
	manager.stopErr = nil
	if err := owner.Stop(controlbudget.Deadline(1 << 62)); err != nil {
		t.Fatal(err)
	}
	if _, err := resources[0].Files["state"].Stat(); err == nil {
		t.Fatal("snapshot survived successful terminal stop")
	}
	TestTransferBridgeImportsSealedStateBeforeBinding(t)
	TestTransferBridgeRebindUsesCurrentBackend(t)
	TestTransferBridgePartialImportAndStopKeepResponsibility(t)
	TestTransferBridgeReservesExportBeforeDescriptorAllocation(t)
	TestTransferBridgePartialBindCannotRegressGeneration(t)
}
