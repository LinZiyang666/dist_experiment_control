//go:build linux

package brokerimage

// Real sealed memfds, resource identities and ownership orchestration are used
// here. Transfer/tunnel contracts are doubles; P11/P18 and P32 separately prove
// actual tracker decisions, codec state and same-PID exec continuity.
import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

type transferOwnerDouble struct {
	xfertrack.Manager // Request operations are never simulated by these lifecycle tests.
	events            *[]string
	resources         []handoff.Resource
	ref               xfercontract.ImageRef
	rebindErr         error
	stopErr           error
	freezeErr         error
	importErr         error
	stopCalls         int
	importCalls       int
	detached          bool
	bound             bool
	imported          *transferOwnerDouble
	beforeRebind      func() error
	beforeImport      func() error
}

func (o *transferOwnerDouble) FreezeImage(controlbudget.Deadline) error {
	*o.events = append(*o.events, "transfer-freeze")
	return o.freezeErr
}
func (o *transferOwnerDouble) ExportImage() (xfercontract.ImageRef, []handoff.Resource, error) {
	return o.ref, o.resources, nil
}
func (o *transferOwnerDouble) ImportImage(ref xfercontract.ImageRef, source tunnel.FileSource, _ controlbudget.Deadline) (TransferOwner, tunnel.Salvage, error) {
	o.importCalls++
	*o.events = append(*o.events, "transfer-import")
	if o.beforeImport != nil {
		if err := o.beforeImport(); err != nil {
			return nil, tunnel.Salvage{}, err
		}
	}
	file, err := source.TakeFile(ref.ResourceID + "/state")
	if err != nil {
		return nil, tunnel.Salvage{}, err
	}
	current := &transferOwnerDouble{events: o.events, ref: ref, rebindErr: o.rebindErr, stopErr: o.stopErr, beforeRebind: o.beforeRebind,
		resources: []handoff.Resource{{ID: ref.ResourceID, Kind: "xfer-state-v1", Files: map[string]*os.File{"state": file}}}}
	o.imported = current
	return current, tunnel.Salvage{}, o.importErr
}
func (o *transferOwnerDouble) DetachImage() (xfercontract.Salvage, error) {
	if o.bound || o.detached {
		return xfercontract.Salvage{}, ErrActive
	}
	o.detached = true
	return xfercontract.Salvage{Resources: o.resources}, nil
}
func (o *transferOwnerDouble) Rebind(controlbudget.Deadline) error {
	*o.events = append(*o.events, "transfer-bind")
	if o.beforeRebind != nil {
		if err := o.beforeRebind(); err != nil {
			return err
		}
	}
	o.bound = true
	return o.rebindErr
}
func (o *transferOwnerDouble) Stop(controlbudget.Deadline) error {
	o.stopCalls++
	*o.events = append(*o.events, "transfer-stop")
	return o.stopErr
}

func addTransferFixture(t *testing.T, f *importFixture) *transferOwnerDouble {
	t.Helper()
	data := []byte("sealed tracker decisions with retained grant and original deadline")
	snapshot, err := sealedstate.New("transfers.fixture", data)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Resources[0].Kind = "xfer-state-v1"
	t.Cleanup(func() { _ = snapshot.Close() })
	sum := sha256.Sum256(data)
	ref := xfercontract.ImageRef{Schema: "transfer-v1", ResourceID: "transfers.fixture", Digest: hex.EncodeToString(sum[:]), Count: 1, EncodedBytes: uint64(len(data))}
	f.bundle.Transfers = &ref
	f.bundle.RequiredDomains = append(f.bundle.RequiredDomains, "transfer-v1")
	sort.Strings(f.bundle.RequiredDomains)
	state := fixturePrivate(f.bundle)
	for _, res := range append(f.resources, snapshot.Resources...) {
		if res.ID == f.bundle.StateResource {
			continue
		}
		for name, file := range res.Files {
			identity, err := handoff.ResourceFileIdentity(file)
			if err != nil {
				t.Fatal(err)
			}
			purpose := "runtime-file"
			if res.ID == f.bundle.LockResource {
				purpose = "store-lock"
			}
			state.Resources = append(state.Resources, ResourceOwnership{ID: res.ID, Kind: res.Kind, Name: name, Purpose: purpose, Identity: identity, Owner: "broker", CutID: state.Runtime.CutID})
		}
	}
	sort.Slice(state.Resources, func(i, j int) bool {
		return state.Resources[i].ID+"/"+state.Resources[i].Name < state.Resources[j].ID+"/"+state.Resources[j].Name
	})
	encoded, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	brokerState, err := sealedstate.New(f.bundle.StateResource, encoded)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = brokerState.Close() })
	var resources []handoff.Resource
	for _, res := range f.resources {
		if res.ID != f.bundle.StateResource {
			resources = append(resources, res)
		}
	}
	resources = append(resources, snapshot.Resources...)
	resources = append(resources, brokerState.Resources...)
	f.source = handofftest.DupResources(t, resources)
	owner := &transferOwnerDouble{events: &f.events, ref: ref, resources: snapshot.Resources}
	f.config.Transfers = owner
	return owner
}

func transferImportSlot(t *testing.T) RuntimeSlot {
	t.Helper()
	slot, err := NewRuntimeSlot(func(StoreOwner, controlbudget.Deadline) error { return nil }, func(tunnel.ServerImported, controlbudget.Deadline) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferImportInstallsSleepingOwnerBeforeConstructors(t *testing.T) {
	for _, failure := range []string{"store-nil", "store-partial", "control-nil", "control-partial", "bind", "none"} {
		t.Run(failure, func(t *testing.T) {
			f := newImportFixture(t)
			factory := addTransferFixture(t, f)
			slot := transferImportSlot(t)
			originalStore, originalControl := f.config.NewStore, f.config.NewControl
			checkInstalled := func() {
				if slot.(*currentRuntime).transfer != factory.imported || factory.imported == nil || factory.imported.bound {
					t.Fatal("constructor ran without the installed sleeping transfer owner")
				}
			}
			f.config.NewStore = func(stamp storage.StoreStamp, file *os.File, d controlbudget.Deadline) (StoreOwner, error) {
				checkInstalled()
				if failure == "store-nil" {
					return nil, errors.New("injected nil store")
				}
				store, err := originalStore(stamp, file, d)
				if failure == "store-partial" {
					return store, errors.New("injected partial store")
				}
				return store, err
			}
			f.config.NewControl = func(store StoreOwner, d controlbudget.Deadline) (ControlOwner, error) {
				checkInstalled()
				if failure == "control-nil" {
					return nil, errors.New("injected nil control")
				}
				control, err := originalControl(store, d)
				if failure == "control-partial" {
					return control, errors.New("injected partial control")
				}
				return control, err
			}
			if failure == "bind" {
				factory.rebindErr = errors.New("injected bind failure")
			}
			image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
			if err != nil {
				closeSalvage(salvage)
				t.Fatal(err)
			}
			t.Cleanup(func() { resources, _ := ImportedResources(image); closeSalvage(resources) })
			manager, active := CurrentTransferOwner(image)
			if manager != factory.imported || active || factory.importCalls != 1 {
				t.Fatal("sleeping tracker was lost or activated during import")
			}
			err = image.Activate(slot, f.config.Deadline)
			if failure == "none" && err != nil || failure != "none" && !errors.Is(err, ErrActive) {
				t.Fatalf("activation result: %v", err)
			}
			manager, active = CurrentTransferOwner(image)
			if manager != factory.imported || !active {
				t.Fatal("ACTIVE error dropped the current tracker")
			}
			if _, err := image.DetachPristine(); !errors.Is(err, ErrActive) {
				t.Fatal("ACTIVE permitted pristine checkpoint restoration")
			}
			if failure == "none" {
				want := []string{"transfer-import", "store", "control", "auth", "transfer-bind", "server-active", "business"}
				if !reflect.DeepEqual(f.events, want) {
					t.Fatalf("activation order: %v", f.events)
				}
			}
			if err := slot.StopCurrent(f.config.Deadline); err != nil || factory.imported.stopCalls != 1 {
				t.Fatal("partial tracker did not have exactly one terminal stop")
			}
		})
	}
}

// origin: hot-update S7 plan §8.2 / §8.3 / P24
func TestTransferImportRefusalRetainsPristineSalvage(t *testing.T) {
	f := newImportFixture(t)
	factory := addTransferFixture(t, f)
	factory.importErr = errors.New("injected partial sleeping tracker")
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	defer closeSalvage(salvage)
	if err == nil || image != nil || factory.imported == nil || !factory.imported.detached {
		t.Fatal("partial import lost its pristine responsibility")
	}
	merged, err := handoff.MergeResources(salvage.Resources, f.source.Borrow())
	if err != nil || len(merged) != 5 {
		t.Fatalf("incomplete pristine salvage: resources=%d err=%v", len(merged), err)
	}
	for _, resource := range merged {
		for _, file := range resource.Files {
			if _, err := file.Stat(); err != nil {
				t.Fatal("pristine file was closed")
			}
		}
	}
	if !reflect.DeepEqual(f.events, []string{"transfer-import"}) {
		t.Fatal("import refusal opened a backend")
	}
}

// origin: hot-update S7 plan §8.3 / P24
func TestTransferImageReferenceAndResourceUnion(t *testing.T) {
	f := newImportFixture(t)
	addTransferFixture(t, f)
	for _, defect := range []string{"digest", "size", "count", "domain", "dangling", "kind", "name", "metadata", "orphan-state"} {
		t.Run(defect, func(t *testing.T) {
			bundle := cloneBundle(f.bundle)
			state, err := importedFixtureState(f)
			if err != nil {
				t.Fatal(err)
			}
			switch defect {
			case "digest":
				bundle.Transfers.Digest = "bad"
			case "size":
				bundle.Transfers.EncodedBytes = sealedstate.MaxStateBytes
			case "count":
				bundle.Transfers.Count = 1025
			case "domain":
				bundle.RequiredDomains = []string{Domain}
			case "dangling":
				bundle.Transfers.ResourceID = "missing"
				var resources []ResourceOwnership
				for _, row := range state.Resources {
					if row.Kind != "xfer-state-v1" {
						resources = append(resources, row)
					}
				}
				state.Resources = resources // Isolate missing-ref refusal from the separate orphan guard.
			case "orphan-state":
				for _, row := range state.Resources {
					if row.ID == bundle.Transfers.ResourceID {
						row.ID = "transfers.unclaimed"
						state.Resources = append(state.Resources, row)
						break
					}
				}
				sort.Slice(state.Resources, func(i, j int) bool {
					return state.Resources[i].ID+"/"+state.Resources[i].Name < state.Resources[j].ID+"/"+state.Resources[j].Name
				})
			case "kind", "name", "metadata":
				for i := range state.Resources {
					if state.Resources[i].ID == bundle.Transfers.ResourceID {
						if defect == "kind" {
							state.Resources[i].Kind = "sealed-state-v1"
						}
						if defect == "name" {
							state.Resources[i].Name = "wrong"
						}
						if defect == "metadata" {
							state.Resources[i].Data = []byte(`{"private":"forbidden"}`)
						}
					}
				}
			}
			if defect == "dangling" || defect == "kind" || defect == "name" || defect == "metadata" || defect == "orphan-state" {
				if CheckPrivateState(state, bundle) == nil {
					t.Fatal("invalid transfer resource union accepted")
				}
			} else if Validate(bundle) == nil {
				t.Fatal("invalid public transfer reference accepted")
			}
		})
	}
}

// origin: hot-update S7 plan §8.2 / M11 / P24
func TestTransferImportKeepsZeroCountPendingOwner(t *testing.T) {
	f := newImportFixture(t)
	factory := addTransferFixture(t, f)
	f.bundle.Transfers.Count = 0 // Pending publication/config ownership can have no live entry.
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	if err != nil {
		closeSalvage(salvage)
		t.Fatal(err)
	}
	resources, _ := ImportedResources(image)
	defer closeSalvage(resources)
	manager, _ := CurrentTransferOwner(image)
	if manager == nil || factory.importCalls != 1 {
		t.Fatal("zero-count pending responsibility was treated as an empty cold tracker")
	}
	if _, err := image.DetachPristine(); err != nil {
		t.Fatal(err)
	}
}

func importedFixtureState(f *importFixture) (PrivateState, error) {
	for _, resource := range f.source.Borrow() {
		if resource.ID == f.bundle.StateResource {
			data, _, err := sealedstate.Read(borrowedState{[]handoff.Resource{resource}}, resource.ID, "state", sealedstate.MaxStateBytes)
			if err != nil {
				return PrivateState{}, err
			}
			return DecodePrivateState(data)
		}
	}
	return PrivateState{}, ErrImage
}
