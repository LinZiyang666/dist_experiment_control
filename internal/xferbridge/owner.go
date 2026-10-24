// Package xferbridge joins transfer leaves to the broker image transaction.
// It never imports broker or opens a second control/store backend.
// origin: hot-update S7 plan §8.2 / P24
package xferbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"sync"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

const Domain = "transfer-v1"
const StateKind = "xfer-state-v1"

var ErrOwner = errors.New("xferbridge: transfer image owner unavailable")

// CurrentBindings resolves today's NC/authority/ledger after StartAuth. Captured
// pre-cut handles must never escape into a thawed callback. Tracker is only the
// sleeping import configuration; its lifecycle is owned explicitly by the slot.
type Config struct {
	Tracker         xfertrack.Config
	Import          xfertrack.ImportFactory
	Encode          xferimage.TrackerEncoder
	Decode          xferimage.TrackerDecoder
	CurrentBindings func(controlbudget.Deadline) (xfertrack.Bindings, error)
	ResourceID      string
}

// The mutex protects lifecycle transitions, not tracker operations. Freeze's
// join may wait for an admitted callback, which must not need this mutex.
type Owner struct {
	mu          sync.Mutex
	cfg         Config
	manager     xfertrack.Manager
	frozen      xfertrack.Frozen
	snapshot    *sealedstate.Snapshot
	reservation xfercontract.ReservationID
	active      bool
	detached    bool
	stopped     bool
	generation  uint64
	deadline    controlbudget.Deadline
}

var _ brokerimage.TransferOwner = (*Owner)(nil)

// New can configure a pure import factory with no live manager. Such a template
// cannot freeze, bind or serve requests until ImportImage supplies its owner.
func New(cfg Config, manager xfertrack.Manager) (*Owner, error) {
	if cfg.Import == nil || cfg.Encode == nil || cfg.Decode == nil || cfg.CurrentBindings == nil || cfg.Tracker.Capacity == nil || !resourceID(cfg.ResourceID) || manager != nil && !pointer(manager) {
		return nil, ErrOwner
	}
	return &Owner{cfg: cfg, manager: manager, active: manager != nil}, nil
}

func (o *Owner) FreezeImage(d controlbudget.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped || o.detached || o.manager == nil {
		return ErrOwner
	}
	o.deadline = d
	if o.frozen != nil {
		return nil
	}
	var err error
	o.frozen, err = o.manager.Freeze(d) // Retain a partial frozen owner on error.
	if o.frozen == nil {
		return errors.Join(err, ErrOwner)
	}
	return err
}

// ExportImage claims only its own memfd. Nested files remain with the tracker;
// callers merge them into the same cut graph and account the whole sealed union.
func (o *Owner) ExportImage() (xfercontract.ImageRef, []handoff.Resource, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.frozen == nil || o.detached || o.stopped {
		return xfercontract.ImageRef{}, nil, ErrOwner
	}
	state, resources, err := o.frozen.Export()
	if err != nil {
		return xfercontract.ImageRef{}, resources, err
	}
	data, err := o.cfg.Encode(state)
	if err != nil || len(data) > sealedstate.MaxStateBytes-16 || len(state.Entries) > 1024 {
		return xfercontract.ImageRef{}, resources, errors.Join(err, ErrOwner)
	}
	// Reserve the output copy and original/Prepared/witness peak before opening
	// any descriptor. Re-export temporarily retains the preceding reservation;
	// capacity pressure must defer the cut without destroying its last snapshot.
	// origin: hot-update S7 plan §8.3 / P24
	reservation, err := o.cfg.Tracker.Capacity.Reserve(xfercontract.Reservation{SealedBytes: int64(len(data) + 16), NonReplayBytes: int64(len(data)), FDs: 3, Resources: 1})
	if err != nil {
		return xfercontract.ImageRef{}, resources, err
	}
	snapshot, err := sealedstate.New(o.cfg.ResourceID, data)
	if err != nil {
		return xfercontract.ImageRef{}, resources, errors.Join(err, o.cfg.Tracker.Capacity.Release(reservation))
	}
	snapshot.Resources[0].Kind = StateKind
	if o.snapshot != nil {
		if err := closeSnapshot(o); err != nil {
			return xfercontract.ImageRef{}, resources, errors.Join(err, snapshot.Close(), o.cfg.Tracker.Capacity.Release(reservation))
		}
	}
	o.snapshot, o.reservation = snapshot, reservation
	digest := sha256.Sum256(data)
	ref := xfercontract.ImageRef{Schema: Domain, ResourceID: o.cfg.ResourceID, Digest: hex.EncodeToString(digest[:]), Count: uint32(len(state.Entries)), EncodedBytes: uint64(len(data))}
	return ref, append(resources, snapshot.Resources...), nil
}

// ImportImage is pure sleeping construction. The outer tracked source records
// file+error before inspecting it; its state FD stays available for pristine
// salvage until Activate closes decode-only files. No Bind runs here.
func (o *Owner) ImportImage(ref xfercontract.ImageRef, source tunnel.FileSource, d controlbudget.Deadline) (brokerimage.TransferOwner, tunnel.Salvage, error) {
	if ValidateRef(ref) != nil || source == nil {
		return nil, tunnel.Salvage{}, ErrOwner
	}
	data, file, err := sealedstate.Read(transferSource{source}, ref.ResourceID, "state", int64(ref.EncodedBytes))
	salvage := tunnel.Salvage{}
	if file != nil {
		salvage.Resources = []handoff.Resource{{ID: ref.ResourceID, Kind: StateKind, Files: map[string]*os.File{"state": file}}}
	}
	digest := sha256.Sum256(data)
	if err != nil || uint64(len(data)) != ref.EncodedBytes || hex.EncodeToString(digest[:]) != ref.Digest {
		return nil, salvage, ErrOwner
	}
	state, err := o.cfg.Decode(data)
	if err != nil || uint32(len(state.Entries)) != ref.Count || len(state.Entries) > 1024 {
		return nil, salvage, ErrOwner
	}
	manager, err := o.cfg.Import(o.cfg.Tracker, state)
	if manager == nil {
		return nil, salvage, errors.Join(err, ErrOwner)
	}
	if !pointer(manager) {
		return nil, salvage, ErrOwner
	}
	cfg := o.cfg
	cfg.ResourceID = ref.ResourceID
	current := &Owner{cfg: cfg, manager: manager, generation: state.Generation, deadline: d}
	// Obtain the leaf's pristine detachment witness while it is still sleeping.
	// A failed partial import remains owned even if this second step also fails.
	freezeErr := current.FreezeImage(d)
	return current, salvage, errors.Join(err, freezeErr)
}

func (o *Owner) DetachImage() (xfercontract.Salvage, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.active || o.detached || o.frozen == nil {
		return xfercontract.Salvage{}, ErrOwner
	}
	salvage, err := o.frozen.DetachPristine()
	if err == nil {
		o.detached = true
	}
	return salvage, err
}

// Rebind resolves and installs the current backend before watchdogs, finalizers
// or subscriptions can run. A failed Bind retains the frozen owner for retry.
// origin: hot-update S7 plan §8.2 / P24
func (o *Owner) Rebind(d controlbudget.Deadline) error {
	bindings, err := o.cfg.CurrentBindings(d)
	if err != nil {
		return err
	}
	return o.Bind(bindings, d)
}

func (o *Owner) Bind(bindings xfertrack.Bindings, d controlbudget.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.detached || o.stopped || o.manager == nil || bindings.OwnerGeneration == 0 || bindings.OwnerGeneration < o.generation {
		return ErrOwner
	}
	// A partial Bind can already have launched callbacks of this generation.
	// Record its high-water mark before calling the leaf, including on error.
	// origin: hot-update S7 plan §8.2 / P24
	o.active, o.generation = true, bindings.OwnerGeneration
	if err := o.manager.Bind(bindings, d); err != nil {
		return err
	}
	o.frozen = nil
	return closeSnapshot(o)
}

func (o *Owner) Stop(d controlbudget.Deadline) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped {
		return nil
	}
	if o.detached || o.manager == nil {
		return ErrOwner
	}
	if err := o.manager.Stop(d); err != nil {
		return err // Keep snapshots and backend custody on a failed callback join.
	}
	if err := closeSnapshot(o); err != nil {
		return err
	}
	o.stopped = true
	return nil
}

// The owner latch serializes descriptor retirement with its reservation. Failed
// release remains retryable; no other domain inherits an uncharged live file.
func closeSnapshot(o *Owner) error {
	if err := o.snapshot.Close(); err != nil {
		return err
	}
	if o.reservation != "" {
		if err := o.cfg.Tracker.Capacity.Release(o.reservation); err != nil {
			return err
		}
	}
	o.snapshot, o.reservation = nil, ""
	return nil
}

// ValidateRef checks public bounds before Take or allocation. A zero Count can
// still retain grants/publications/config retirement; it never skips import.
func ValidateRef(ref xfercontract.ImageRef) error {
	digest, err := hex.DecodeString(ref.Digest)
	if ref.Schema != Domain || !resourceID(ref.ResourceID) || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != ref.Digest || ref.Count > 1024 || ref.EncodedBytes == 0 || ref.EncodedBytes > sealedstate.MaxStateBytes-16 {
		return ErrOwner
	}
	return nil
}

type transferSource struct{ source tunnel.FileSource }

func (s transferSource) TakeFile(id, name string) (*os.File, error) {
	return s.source.TakeFile(id + "/" + name) // Explicit two-part adapter; never guess a singleton name.
}

func pointer(value any) bool {
	v := reflect.ValueOf(value)
	return v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil()
}

func resourceID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for _, c := range id {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// Request methods delegate to the leaf; bridge lifecycle does not duplicate its
// decision machine, grant arbitration, nonce checks or callback generation fence.
func (o *Owner) Admit(p xfercontract.CreationPermit, in xfercontract.Intent, d controlbudget.Deadline) (xfercontract.Status, error) {
	if o.manager == nil {
		return xfercontract.Status{}, ErrOwner
	}
	return o.manager.Admit(p, in, d)
}
func (o *Owner) SourceSeal(id xfercontract.Identity, s xfercontract.SourceSeal, d controlbudget.Deadline) (xfercontract.Status, error) {
	if o.manager == nil {
		return xfercontract.Status{}, ErrOwner
	}
	return o.manager.SourceSeal(id, s, d)
}
func (o *Owner) Reserve(id xfercontract.Identity, r xfercontract.ReserveRequest, d controlbudget.Deadline) (xfercontract.CommitGrant, error) {
	if o.manager == nil {
		return xfercontract.CommitGrant{}, ErrOwner
	}
	return o.manager.Reserve(id, r, d)
}
func (o *Owner) Finish(id xfercontract.Identity, out xfercontract.ReceiverOutcome, d controlbudget.Deadline) (xfercontract.TerminalReceipt, error) {
	if o.manager == nil {
		return xfercontract.TerminalReceipt{}, ErrOwner
	}
	return o.manager.Finish(id, out, d)
}
func (o *Owner) Cancel(id xfercontract.Identity, c xfercontract.CancelRequest, d controlbudget.Deadline) (xfercontract.Status, error) {
	if o.manager == nil {
		return xfercontract.Status{}, ErrOwner
	}
	return o.manager.Cancel(id, c, d)
}
func (o *Owner) Status(r xfercontract.StatusRequest, d controlbudget.Deadline) (xfercontract.Status, error) {
	if o.manager == nil {
		return xfercontract.Status{}, ErrOwner
	}
	return o.manager.Status(r, d)
}
func (o *Owner) Freeze(d controlbudget.Deadline) (xfertrack.Frozen, error) {
	err := o.FreezeImage(d)
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.frozen, err
}
