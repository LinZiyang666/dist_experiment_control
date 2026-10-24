//go:build linux

// transfer_image_test.go uses the real runtime, handoff duplicates and sealed
// memfds. Recording worker/codec ports isolate composition from P06/P10 actor
// behavior; the actual same-PID watchdog and worker codec are P32's T38/T17.
package agentimage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

// origin: hot-update S7 plan §8.3–§8.4 / P21 / T38 wiring
func TestTransferImageRequiresEveryRetainedReader(t *testing.T) {
	weak := Capabilities{ABI: 1, Domains: []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1"}}
	full := weak
	full.Domains = append(append([]string(nil), weak.Domains...), xfercontract.Version)
	for _, count := range []uint32{0, 1} {
		b := Bundle{Transfers: &xfercontract.ImageRef{Schema: xfercontract.StateSchema, Count: count}}
		for _, readers := range [][]Capabilities{{weak, full}, {full, weak}, {Capabilities{}, full}, {full, Capabilities{}}} {
			if err := CheckBundleCapabilities(b, readers...); !errors.Is(err, ErrABIUnsupported) {
				t.Fatalf("retained transfer state accepted an unreadable tuple: %v", err)
			}
		}
		if err := CheckBundleCapabilities(b, full, full); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckBundleCapabilities(Bundle{}, Capabilities{}); err != nil {
		t.Fatal("empty legacy bundle lost compatibility")
	}
}

// origin: hot-update S7 plan §8.2–§8.3 / P21 / T38 wiring
func TestTransferImageLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferImageLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer image lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "transfer image lifecycle", beforeFD, 0)
}

func exerciseTransferImageLifecycle(t *testing.T) {
	for _, arm := range []string{"success", "freeze_timeout", "export_error", "empty_encoding", "partial_take_error", "nil_owner", "partial_owner_error", "file_and_error", "ignored_file_error", "ignored_duplicate_take", "activation_failure", "pristine_detach", "detach_failure", "digest_mismatch", "size_mismatch", "count_mismatch", "wrong_resource_kind", "missing_business_resource", "missing_export_resource", "combined_replay", "extra_sealed_capacity", "reexport", "wrong_boot", "wrong_instance", "open_admission", "inflight_admission", "wrong_business_kind", "public_business_data", "duplicate_business_fd", "negative_capacity", "nonreplay_capacity", "sealed_capacity", "public_capacity", "handoff_capacity"} {
		t.Run(arm, func(t *testing.T) { exerciseTransferImageArm(t, arm) })
	}
}

func exerciseTransferImageArm(t *testing.T, arm string) {
	f := newBundleFixture(t, "transfer-image")
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	tmp, err := os.CreateTemp(t.TempDir(), "receiver")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tmp.Close() }()
	if _, err := tmp.Write([]byte("synced-prefix")); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		t.Fatal(err)
	}
	ref := xfercontract.ResourceRef{ResourceID: "xfer-tmp.receiver", Name: xfercontract.TmpFileName, Kind: xfercontract.ResourceTmp}
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	originalBudget := xfercontract.BudgetSeal{StartedAt: time.Unix(123456789, 0).UTC(), EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: 17}
	originalDeadline := xfercontract.BootDeadline{BootID: boot, Deadline: controlbudget.Deadline(now + int64(300*time.Second))}
	old := &imageTransferPort{state: xfercontract.WorkerState{Schema: xfercontract.StateSchema,
		Lineage:   xfercontract.Lineage{InstanceID: "transfer-image", BootID: boot, Generation: 1},
		Entries:   []xfercontract.WorkerEntry{{Prepare: xfercontract.Prepare{Inline: []byte("private-inline")}}},
		Receivers: []xfercontract.ReceiverState{{Budget: originalBudget, ByteDeadline: originalDeadline, Atomic: xfercontract.AtomicState{Tmp: ref, DurableOffset: 13}}},
		Couriers:  []xfercontract.CourierState{{Capsule: xfercontract.SettlementCapsule{Signature: "private-capsule"}}}},
		resources: []handoff.Resource{{ID: ref.ResourceID, Kind: ref.Kind, Files: map[string]*os.File{ref.Name: tmp}}},
		binding:   xferworker.Bindings{OwnerGeneration: 1}, encode: encodeImageTransfer}
	owners := Owners{Runtime: f.rt, Manager: f.manager, Transfers: old, Lineage: Lineage{Generation: 2}}
	if arm == "freeze_timeout" {
		old.freezeErr = os.ErrDeadlineExceeded
	}
	if arm == "export_error" {
		old.encode = func(xfercontract.WorkerState) ([]byte, error) { return nil, os.ErrInvalid }
	}
	if arm == "empty_encoding" {
		old.encode = func(xfercontract.WorkerState) ([]byte, error) { return nil, nil }
	}
	if arm == "wrong_boot" {
		old.state.Lineage.BootID = "another-boot"
	}
	if arm == "wrong_instance" {
		old.state.Lineage.InstanceID = "another-agent"
	}
	if arm == "open_admission" {
		old.openAdmission = true
	}
	if arm == "inflight_admission" {
		old.state.Admission.Inflight = 1
	}
	if arm == "wrong_business_kind" {
		old.state.Receivers[0].Atomic.Tmp.Kind = "unknown-business-kind"
		old.resources[0].Kind = "unknown-business-kind"
	}
	if arm == "public_business_data" {
		old.resources[0].Data = []byte("private-business-data")
	}
	if arm == "missing_export_resource" {
		old.resources = nil
	}
	if arm == "duplicate_business_fd" {
		refCopy := ref
		refCopy.ResourceID = "xfer-tmp.another-receiver"
		old.state.Receivers = append(old.state.Receivers, xfercontract.ReceiverState{Atomic: xfercontract.AtomicState{Tmp: refCopy}})
		old.resources = append(old.resources, handoff.Resource{ID: refCopy.ResourceID, Kind: refCopy.Kind, Files: map[string]*os.File{refCopy.Name: tmp}})
	}
	if strings.HasSuffix(arm, "_capacity") && arm != "extra_sealed_capacity" {
		reservation := xfercontract.Reservation{}
		switch arm {
		case "negative_capacity":
			reservation.ReplayBytes = -1
		case "nonreplay_capacity":
			reservation.NonReplayBytes = (64 << 20) + 1
		case "sealed_capacity":
			reservation.SealedBytes = sealedstate.MaxStateBytes + 1
		case "public_capacity":
			reservation.PublicBytes = MaxBundleBytes + 1
		case "handoff_capacity":
			reservation.HandoffBytes = (64 << 20) + 1
		}
		old.state.CapacityReservations = []xfercontract.CapacityReservation{{ID: "one", Reservation: reservation}}
	}
	if arm == "combined_replay" {
		old.state.CapacityReservations = []xfercontract.CapacityReservation{
			{ID: "one", Reservation: xfercontract.Reservation{ReplayBytes: workload.DefaultReplayBudget}},
			{ID: "two", Reservation: xfercontract.Reservation{ReplayBytes: 1}},
		}
	}
	cut, err := Freeze(owners, time.Now().Add(bundleTestBudget))
	if arm == "freeze_timeout" || arm == "export_error" || arm == "empty_encoding" || arm == "combined_replay" || (strings.HasSuffix(arm, "_capacity") && arm != "extra_sealed_capacity") ||
		arm == "wrong_boot" || arm == "wrong_instance" || arm == "open_admission" || arm == "inflight_admission" || arm == "wrong_business_kind" || arm == "public_business_data" || arm == "duplicate_business_fd" || arm == "missing_export_resource" {
		if err == nil || cut != nil {
			t.Fatal("unrepresentable transfer cut reported success")
		}
		if old.thawUntil != old.freezeUntil {
			t.Fatal("failed cut did not thaw using its original deadline")
		}
		if _, err := tmp.Stat(); err != nil || f.rt.Frozen() || f.manager.Frozen() {
			t.Fatal("failed cut closed an original or retained an unrelated fence")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !cut.done {
			if err := cut.Thaw(); err != nil {
				t.Error(err)
			}
		}
	}()
	b := cut.Bundle()
	if b.Transfers == nil || b.Transfers.Count != 1 || b.Transfers.EncodedBytes == 0 {
		t.Fatal("transfer reference has no retained projection")
	}
	metadata, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(metadata, []byte("private-inline")) || bytes.Contains(metadata, []byte("private-capsule")) {
		t.Fatal("transfer private bytes entered the public bundle")
	}
	if arm == "extra_sealed_capacity" {
		// A sparse file makes the size-accounting control cheap. This exercises
		// composition's allowance, while sealedstate owns kernel seal validation.
		extra, err := os.CreateTemp(t.TempDir(), "auxiliary-state")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = extra.Close() }()
		if err := extra.Truncate(sealedstate.MaxStateBytes); err != nil {
			t.Fatal(err)
		}
		p, err := cut.PrepareChecked(nil, handoff.Resource{ID: "auxiliary-state", Kind: "sealed-state-v1", Files: map[string]*os.File{"state": extra}})
		if p != nil {
			_ = p.Close()
		}
		var deferred *DeferredError
		if !errors.As(err, &deferred) || deferred.Reason != ReasonImageTooLarge {
			t.Fatal("auxiliary state received a separate sealed-state allowance")
		}
		return
	}
	prepared, err := cut.PrepareChecked(func(final Bundle) error {
		return CheckBundleCapabilities(final, Capabilities{ABI: 1, Domains: []string{"exec-v1", xfercontract.Version}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Stat(); err != nil {
		t.Fatal("closing Prepared closed an original receiver FD")
	}
	if arm == "reexport" {
		previous := cut.Bundle().Transfers.Digest
		old.state.Receivers[0].Atomic.DurableOffset++
		old.state.Couriers[0].Capsule.Signature = "private-updated-capsule"
		observed := false
		p, err := cut.PrepareChecked(func(final Bundle) error {
			observed = final.Transfers != nil && final.Transfers.Digest != previous
			return ErrABIUnsupported
		})
		if p != nil || !errors.Is(err, ErrABIUnsupported) || !observed || cut.Bundle().Transfers.Digest != previous {
			t.Fatal("final compatibility check used cached bytes or replaced a rejected cut")
		}
		p, err = cut.PrepareChecked(func(final Bundle) error {
			if final.Transfers.Digest == previous {
				return errors.New("cached transfer state")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = p.Close()
		return
	}
	b = cut.Bundle()
	resources := cut.resources
	if arm == "wrong_resource_kind" {
		resources = append([]handoff.Resource(nil), resources...)
		for i := range resources {
			if resources[i].ID == b.Transfers.ResourceID {
				resources[i].Kind = "sealed-state-v1"
			}
		}
	}
	source := handofftest.DupResources(t, resources)
	defer source.Close()
	if arm == "missing_business_resource" {
		source.Drop(ref.ResourceID, ref.Name)
	}
	if arm == "digest_mismatch" || arm == "count_mismatch" || arm == "size_mismatch" {
		refCopy := *b.Transfers
		b.Transfers = &refCopy
		switch arm {
		case "digest_mismatch":
			b.Transfers.Digest = strings.Repeat("0", 64)
		case "count_mismatch":
			b.Transfers.Count++
		default:
			b.Transfers.EncodedBytes++
		}
	}
	current := &imageTransferPort{binding: xferworker.Bindings{OwnerGeneration: 2}, encode: encodeImageTransfer}
	if arm == "activation_failure" {
		current.thawErr = os.ErrDeadlineExceeded
	}
	importer := &imageTransferImporter{}
	target := Target{Runtime: f.cfg, TransferDeadline: old.freezeUntil, Transfers: importer}
	importer.importFn = func(projected TransferState, files TransferFileSource) (TransferWorker, xfercontract.Salvage, error) {
		state, err := decodeImageTransfer(projected.Encoded)
		if err != nil {
			return nil, xfercontract.Salvage{}, err
		}
		file, err := files.TakeFile(ref.ResourceID, ref.Name)
		if err != nil && arm != "ignored_file_error" {
			return nil, xfercontract.Salvage{}, err
		}
		current.state, current.resources = state, []handoff.Resource{{ID: ref.ResourceID, Kind: ref.Kind, Files: map[string]*os.File{ref.Name: file}}}
		if arm == "partial_take_error" || arm == "nil_owner" {
			// Intentionally omit leaf salvage: the composition tracks TakeFile.
			if arm == "nil_owner" {
				return nil, xfercontract.Salvage{}, nil
			}
			return nil, xfercontract.Salvage{}, os.ErrInvalid
		}
		if arm == "partial_owner_error" {
			return current, xfercontract.Salvage{Resources: current.resources}, os.ErrInvalid
		}
		if arm == "ignored_duplicate_take" {
			_, _ = files.TakeFile(ref.ResourceID, ref.Name)
		}
		return current, xfercontract.Salvage{}, nil
	}
	var files interface {
		TakeFile(string, string) (*os.File, error)
	} = source
	if arm == "file_and_error" || arm == "ignored_file_error" {
		files = &imageTransferFileError{DupSource: source, id: ref.ResourceID}
	}
	imported, salvage, err := Import(b, b.Processes.Records, files, target)
	if arm == "partial_take_error" || arm == "nil_owner" || arm == "partial_owner_error" || arm == "file_and_error" || arm == "ignored_file_error" || arm == "ignored_duplicate_take" || arm == "digest_mismatch" || arm == "size_mismatch" || arm == "count_mismatch" || arm == "wrong_resource_kind" || arm == "missing_business_resource" {
		if err == nil || imported != nil || salvage == nil {
			t.Fatal("invalid transfer import reported success or lost salvage")
		}
		taken := 0
		for _, resource := range salvage.Resources {
			for _, file := range resource.Files {
				taken++
				if _, err := file.Stat(); err != nil {
					t.Fatal("salvage closed a taken descriptor")
				}
			}
		}
		if (arm == "partial_take_error" || arm == "nil_owner" || arm == "partial_owner_error" || arm == "file_and_error" || arm == "ignored_file_error" || arm == "ignored_duplicate_take") && taken != 2 {
			t.Fatalf("partial constructor salvage=%d files want=2", taken)
		}
		if arm == "partial_owner_error" && len(current.resources) != 0 {
			t.Fatal("partial+error manager was not detached before returning salvage")
		}
		if err := salvage.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil || salvage != nil || imported.Transfers != current || current.thawUntil != 0 {
		t.Fatalf("sleeping transfer import failed or woke actors: %v", err)
	}
	if current.state.Receivers[0].Budget != originalBudget || current.state.Receivers[0].ByteDeadline != originalDeadline {
		t.Fatal("transfer import refreshed the original byte budget or deadline")
	}
	if arm == "pristine_detach" || arm == "detach_failure" {
		if arm == "detach_failure" {
			current.freezeErr = os.ErrDeadlineExceeded
		}
		salvage, err := imported.Detach()
		if (err != nil) != (arm == "detach_failure") || len(salvage.Resources) != 2 {
			t.Fatalf("pristine transfer detach lost its resource union: %v", err)
		}
		prepared, err := salvage.Prepare(source.Borrow()...)
		if err != nil {
			t.Fatal(err)
		}
		_ = prepared.Close()
		if err := salvage.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if arm == "activation_failure" {
		if err := imported.Thaw(); err == nil || !imported.Active() || imported.ActivationComplete() {
			t.Fatal("partial ACTIVE import lost current ownership or claimed readiness")
		}
		if _, err := imported.Detach(); err == nil {
			t.Fatal("ACTIVE import re-used pristine transfer bytes")
		}
		if imported.detached || len(current.resources) == 0 {
			t.Fatal("rejected pristine detach mutated an ACTIVE transfer owner")
		}
		current.thawErr = nil
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
	if !imported.ActivationComplete() || current.thawed.OwnerGeneration != 2 || current.thawUntil != old.freezeUntil {
		t.Fatal("transfer activation completed without current bindings or changed the deadline")
	}
	if _, err := current.resources[0].Files[ref.Name].Stat(); err != nil {
		t.Fatal("discarding the exporter closed the imported business descriptor")
	}
	if _, err := imported.Detach(); err == nil {
		t.Fatal("activated transfer import remained pristine")
	}
	if err := closeTransferResources(current.resources); err != nil {
		t.Fatal(err)
	}
	if err := imported.Runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// The recording port never executes file transfer logic. It makes owner
// transitions and exact descriptor custody observable to the composition test.
// binding is the backend "current" at thaw, as the agent adapter resolves it.
type imageTransferPort struct {
	state                  xfercontract.WorkerState
	resources              []handoff.Resource
	freezeErr, thawErr     error
	openAdmission          bool
	freezeUntil, thawUntil controlbudget.Deadline
	binding, thawed        xferworker.Bindings
	encode                 func(xfercontract.WorkerState) ([]byte, error)
}

type imageTransferImporter struct {
	importFn func(TransferState, TransferFileSource) (TransferWorker, xfercontract.Salvage, error)
}

func (i *imageTransferImporter) Decode(data []byte) (TransferState, error) {
	state, err := decodeImageTransfer(data)
	return projectImageTransfer(state, data), err
}
func (i *imageTransferImporter) Import(state TransferState, files TransferFileSource) (TransferWorker, xfercontract.Salvage, error) {
	return i.importFn(state, files)
}

// projectImageTransfer mirrors the production adapter (xferagent) so these
// composition arms stay independent of the agent package.
func projectImageTransfer(state xfercontract.WorkerState, data []byte) TransferState {
	p := TransferState{Encoded: data, Schema: state.Schema, BootID: state.Lineage.BootID, InstanceID: state.Lineage.InstanceID,
		Generation: state.Lineage.Generation, AdmissionClosed: state.Admission.Closed, Inflight: state.Admission.Inflight,
		Entries: uint32(len(state.Entries))}
	for _, reservation := range state.CapacityReservations {
		p.Reservations = append(p.Reservations, reservation.Reservation)
	}
	for _, sender := range state.Senders {
		p.Refs = append(p.Refs, sender.Source)
	}
	for _, receiver := range state.Receivers {
		p.Refs = append(p.Refs, receiver.Atomic.Tmp, receiver.Atomic.Parent, receiver.Atomic.Source)
	}
	return p
}

type imageTransferFileError struct {
	*handofftest.DupSource
	id string
}

func (s *imageTransferFileError) TakeFile(id, name string) (*os.File, error) {
	file, err := s.DupSource.TakeFile(id, name)
	if err == nil && id == s.id {
		return file, os.ErrInvalid
	}
	return file, err
}

func (p *imageTransferPort) Freeze(until controlbudget.Deadline) (TransferFrozen, error) {
	p.freezeUntil, p.state.Admission.Closed = until, true
	if p.openAdmission {
		p.state.Admission.Closed = false
	}
	if p.freezeErr != nil {
		return nil, p.freezeErr
	}
	return p, nil
}
func (p *imageTransferPort) Export() (TransferState, []handoff.Resource, error) {
	data, err := p.encode(p.state)
	if err != nil {
		return TransferState{}, nil, err
	}
	return projectImageTransfer(p.state, data), p.resources, nil
}
func (p *imageTransferPort) DetachPristine() (xfercontract.Salvage, error) {
	resources := p.resources
	p.resources = nil
	return xfercontract.Salvage{Resources: resources}, nil
}
func (p *imageTransferPort) Thaw(until controlbudget.Deadline) error {
	p.thawed, p.thawUntil = p.binding, until
	return p.thawErr
}

func encodeImageTransfer(state xfercontract.WorkerState) ([]byte, error) { return json.Marshal(state) }
func decodeImageTransfer(data []byte) (state xfercontract.WorkerState, err error) {
	err = json.Unmarshal(data, &state)
	return state, err
}

// origin: hot-update S7 plan §8.3 / P21
func TestTransferImageRejectsMalformedPublicReference(t *testing.T) {
	f := newBundleFixture(t, "reference-validation")
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.Thaw() }()
	valid := xfercontract.ImageRef{Schema: xfercontract.StateSchema, ResourceID: "xfer-state." + strings.Repeat("1", 32),
		Digest: strings.Repeat("2", 64), EncodedBytes: 1}
	for _, change := range []func(*xfercontract.ImageRef){
		func(r *xfercontract.ImageRef) { r.Schema = "unknown" }, func(r *xfercontract.ImageRef) { r.ResourceID = "path/private" },
		func(r *xfercontract.ImageRef) { r.Digest = "bad" }, func(r *xfercontract.ImageRef) { r.EncodedBytes = 0 },
		func(r *xfercontract.ImageRef) { r.EncodedBytes = sealedstate.MaxStateBytes + 1 },
	} {
		b := cut.Bundle()
		ref := valid
		change(&ref)
		b.Transfers = &ref
		if err := b.Validate(); !errors.Is(err, ErrBundle) {
			t.Fatalf("malformed transfer public reference accepted: %v", err)
		}
	}
}

// origin: hot-update S7 plan §11.1 / P21; S5 domain projection move regression
func TestTransferImagePreservesDomainObservationProjection(t *testing.T) {
	b := Bundle{Lineage: Lineage{Generation: 7}}
	b.Runtime.Tasks = []execsession.TaskImage{
		{Files: map[string]execsession.FileIdentity{"stdout": {Dev: 3, Ino: 4}},
			Stdout: &execsession.StreamImage{Replay: workload.ReplayState{Capacity: 32, Base: 1, End: 5}}},
		{Files: map[string]execsession.FileIdentity{"master": {Dev: 5, Ino: 6}}, PTY: &ptysession.Image{
			Input: workload.ReplayState{Capacity: 8, Base: 2, End: 3}, Output: workload.ReplayState{Capacity: 16, Base: 5, End: 10}}},
	}
	b.Flow = &tunnel.ClientImage{Flows: flow.Image{ReservedBytes: 128,
		Records: []flow.RecordImage{{RawRef: "raw", ReplayRef: "replay", TxBase: 3, TxEnd: 15}}}}
	events := DomainObservations(b)
	if len(events) != 6 || events[0].Kind != "domain-file" || events[0].Domain != "exec" ||
		events[0].Values[0] != 3 || events[0].Values[1] != 4 || events[1].Domain != "pty" ||
		events[1].Values[0] != 5 || events[1].Values[1] != 6 {
		t.Fatal("domain projection lost file fingerprints or changed the event order")
	}
	for i, want := range []struct {
		domain                    string
		files, capacity, retained uint64
	}{{"exec", 1, 32, 4}, {"pty", 1, 24, 6}, {"flow", 2, 128, 12}, {"ss", 0, 0, 0}} {
		event := events[i+2]
		if event.Kind != "domain-cut" || event.Domain != want.domain || event.Values[0] != 7 ||
			event.Values[1] != want.files || event.Values[2] != want.capacity || event.Values[3] != want.retained {
			t.Fatalf("domain projection changed %s counts", want.domain)
		}
	}
}

// origin: hot-update S7 plan §8.3 / P21
func TestTransferImageCapacityIncludesRuntimeAndFlow(t *testing.T) {
	for _, owner := range []string{"runtime", "flow"} {
		b := Bundle{Instance: "capacity-node", Lineage: Lineage{BootID: "capacity-boot", Generation: 1}}
		if owner == "runtime" {
			b.Runtime.BudgetUsed = workload.DefaultReplayBudget - 1
		} else {
			b.Flow = &tunnel.ClientImage{Flows: flow.Image{ReservedBytes: workload.DefaultReplayBudget - 1}}
		}
		state := xfercontract.WorkerState{Schema: xfercontract.StateSchema,
			Lineage:              xfercontract.Lineage{InstanceID: b.Instance, BootID: b.Lineage.BootID, Generation: 1},
			Admission:            xfercontract.AdmissionState{Closed: true},
			CapacityReservations: []xfercontract.CapacityReservation{{ID: "transfer", Reservation: xfercontract.Reservation{ReplayBytes: 2}}}}
		var deferred *DeferredError
		if err := validateTransferState(b, projectImageTransfer(state, []byte("{}")), nil); !errors.As(err, &deferred) || deferred.Reason != ReasonImageTooLarge {
			t.Fatalf("transfer received a second replay allowance beside %s", owner)
		}
		state.CapacityReservations[0].Reservation.ReplayBytes = 1
		if err := validateTransferState(b, projectImageTransfer(state, []byte("{}")), nil); err != nil {
			t.Fatalf("exact shared replay bound refused beside %s: %v", owner, err)
		}
	}
}
