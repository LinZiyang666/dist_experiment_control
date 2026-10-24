// Package infratransfer adapts retained S7 owners to the S9 migration fence.
// It borrows business owners and descriptors; infrastructure retirement must
// never cancel a transfer, mint an identity or renew its byte deadline.
// origin: hot-update S9 plan §2.1 / §4.3 / §8 / P19
package infratransfer

import (
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Current resolves the authenticated successor after the cohort has moved.
// It must not create a new business invocation or capture the previous NC.
type Current func(i.Deadline) (x.Bindings, error)

type Worker struct {
	Owner   x.WorkerManager
	Current Current
	// ResumeActors reopens retained receiver/atomic admission using S7's
	// package operations, after data bindings and before Worker.Thaw. It is
	// required when the frozen worker contains receivers; no import factory
	// can replace a live source or sink OFD here.
	ResumeActors func(i.Deadline) error
}

type Tracker struct {
	Owner   x.TrackerManager
	Current Current
	// S7 Bind installs the ledger, not its captured history/finalizer ports.
	// A live tracker requires this host binding for those stable backend ports;
	// an empty owner can bind without it. This hook never resumes callbacks.
	RebindBackend func(x.Bindings, i.Deadline) error
}

type Config struct {
	SourceSHA256 i.Digest
	SchemaSHA256 i.Digest
	Workers      []Worker
	Trackers     []Tracker
	Writers      []*Writer
	Readers      []*Reader
	// FenceData joins the current caller's write/Sync/ConfirmSynced section.
	// S7 workers provide that join themselves; independent invocation owners
	// need this explicit product hook. A reader port cannot prove sink custody
	// merely by closing its socket. An unbound caller must remain unsupported.
	FenceData func(i.ID, i.Deadline) error
	// BeforeReplay is the product recovery gate, not a harness pause. P18
	// verifies original ACK prefixes before tracker Bind can replay history.
	// P15 owns cohort registration and must close late admission before Fence.
	BeforeReplay func(i.ID, i.Deadline) error
}

type workerCut struct {
	owner    Worker
	frozen   x.WorkerFrozen
	state    x.WorkerState
	bindings x.Bindings
	released bool
}
type trackerCut struct {
	owner    Tracker
	frozen   x.TrackerFrozen
	state    x.TrackerState
	bindings x.Bindings
	released bool
}

type Adapter struct {
	cfg      Config
	serial   chan struct{}
	tx       i.ID
	seal     i.TransferSeal
	workers  []workerCut
	trackers []trackerCut
	rebound  bool
}

var _ i.Transfers = (*Adapter)(nil)

// New validates the complete registration closure without freezing it. A
// genuinely empty cohort is allowed, but missing recovery/source/schema proof
// cannot become a complete inventory. Registrations stay fixed for this owner.
func New(cfg Config) (*Adapter, error) {
	if cfg.SourceSHA256 == (i.Digest{}) || cfg.SchemaSHA256 != ObservationSchema() || cfg.BeforeReplay == nil ||
		len(cfg.Workers)+len(cfg.Trackers)+len(cfg.Writers)+len(cfg.Readers) > 1024 {
		return nil, i.ErrIncomplete
	}
	seen := make(map[any]bool)
	add := func(v any) bool {
		if absent(v) || !reflect.TypeOf(v).Comparable() || seen[v] {
			return false
		}
		seen[v] = true
		return true
	}
	for _, w := range cfg.Workers {
		if !add(w.Owner) || w.Current == nil {
			return nil, i.ErrIncomplete
		}
	}
	for _, t := range cfg.Trackers {
		if !add(t.Owner) || t.Current == nil {
			return nil, i.ErrIncomplete
		}
	}
	for _, w := range cfg.Writers {
		if !add(w) || w.current == nil {
			return nil, i.ErrIncomplete
		}
	}
	for _, r := range cfg.Readers {
		if !add(r) || r.current == nil {
			return nil, i.ErrIncomplete
		}
	}
	if len(cfg.Writers)+len(cfg.Readers) > 0 && cfg.FenceData == nil && len(cfg.Workers) == 0 {
		return nil, i.ErrUnsupported
	}
	cfg.Workers = append([]Worker(nil), cfg.Workers...)
	cfg.Trackers = append([]Tracker(nil), cfg.Trackers...)
	cfg.Writers = append([]*Writer(nil), cfg.Writers...)
	cfg.Readers = append([]*Reader(nil), cfg.Readers...)
	return &Adapter{cfg: cfg, serial: make(chan struct{}, 1)}, nil
}

func absent(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return r.IsNil()
	default:
		return false
	}
}

// All lifecycle calls use the original operation deadline even while queued.
// There is no timeout goroutine which can continue mutating a later generation.
func take(serial chan struct{}, d i.Deadline) error {
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case serial <- struct{}{}:
		if _, err = controlbudget.Remaining(d); err != nil {
			<-serial
			return err
		}
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

// Fence closes worker callbacks before snapshotting their data children, then
// joins tracker watchdogs. A refused cut retains the same closed owners for a
// retry; it never returns a partial Complete seal or automatically thaws them.
// origin: hot-update S9 plan §4.3 / §8 / T42–T43
func (a *Adapter) Fence(tx i.ID, d i.Deadline) (i.TransferSeal, error) {
	if err := take(a.serial, d); err != nil {
		return i.TransferSeal{}, err
	}
	defer func() { <-a.serial }()
	if tx == (i.ID{}) || a.tx != (i.ID{}) && a.tx != tx {
		return i.TransferSeal{}, i.ErrIncomplete
	}
	if a.seal.Complete {
		return a.seal, nil
	}
	a.tx = tx
	if a.cfg.FenceData != nil {
		if err := a.cfg.FenceData(tx, d); err != nil {
			return i.TransferSeal{}, err
		}
	}
	a.workers, a.trackers = nil, nil
	for _, w := range a.cfg.Workers {
		f, err := w.Owner.Freeze(d)
		if err != nil || absent(f) {
			return i.TransferSeal{}, errors.Join(i.ErrIncomplete, err)
		}
		state, _, err := f.Export() // Borrowed S7 resources stay with the original owner.
		if err != nil {
			return i.TransferSeal{}, err
		}
		if !state.Admission.Closed || state.Admission.Inflight != 0 || len(state.Receivers) != 0 && w.ResumeActors == nil {
			return i.TransferSeal{}, i.ErrUnsupported
		}
		a.workers = append(a.workers, workerCut{owner: w, frozen: f, state: state})
	}
	for _, t := range a.cfg.Trackers {
		f, err := t.Owner.Freeze(d)
		if err != nil || absent(f) {
			return i.TransferSeal{}, errors.Join(i.ErrIncomplete, err)
		}
		state, _, err := f.Export()
		if err != nil {
			return i.TransferSeal{}, err
		}
		if !state.Admission.Closed || !state.ProvisioningFence.Closed || state.Admission.Inflight != 0 || state.ProvisioningFence.Inflight != 0 {
			return i.TransferSeal{}, i.ErrIncomplete
		}
		if (len(state.Entries)+len(state.PendingStart)+len(state.PendingTerminal)+len(state.Cleanup) > 0) && t.RebindBackend == nil {
			return i.TransferSeal{}, i.ErrUnsupported
		}
		a.trackers = append(a.trackers, trackerCut{owner: t, frozen: f, state: state})
	}
	for _, w := range a.cfg.Writers {
		if _, err := w.Freeze(d); err != nil {
			return i.TransferSeal{}, err
		}
	}
	for _, r := range a.cfg.Readers {
		if _, err := r.Freeze(d); err != nil {
			return i.TransferSeal{}, err
		}
	}
	seal, err := checkpoint(a, d)
	if err != nil {
		return i.TransferSeal{}, err
	}
	a.seal = seal
	return seal, nil
}

// Rebind prepares data owners while business admission remains fenced. In
// particular Tracker.Bind is deferred: it can replay canonical publications.
// A failed successor or JS503 retains pending attempts and the original seal.
// origin: hot-update S9 plan §4.3 / §8 / T42
func (a *Adapter) Rebind(seal i.TransferSeal, d i.Deadline) error {
	if err := take(a.serial, d); err != nil {
		return err
	}
	defer func() { <-a.serial }()
	if seal != a.seal || !seal.Complete {
		return i.ErrIncomplete
	}
	if a.rebound {
		return nil
	}
	if err := a.cfg.BeforeReplay(a.tx, d); err != nil {
		return err
	}
	for n := range a.trackers {
		c := &a.trackers[n]
		b, err := c.owner.Current(d)
		if err != nil {
			return err
		}
		if absent(b.Ledger) || b.OwnerGeneration < c.state.Generation || b.OwnerGeneration == 0 {
			return i.ErrIncomplete
		}
		if c.owner.RebindBackend != nil {
			if err := c.owner.RebindBackend(b, d); err != nil {
				return err
			}
		}
		c.bindings = b
	}
	for n := range a.workers {
		c := &a.workers[n]
		b, err := c.owner.Current(d)
		if err != nil {
			return err
		}
		if absent(b.Authority) || absent(b.Transport) || b.OwnerGeneration < c.state.Lineage.Generation || b.OwnerGeneration == 0 {
			return i.ErrIncomplete
		}
		c.bindings = b
	}
	for _, w := range a.cfg.Writers {
		if err := w.rebind(d); err != nil {
			return err
		}
	}
	for _, r := range a.cfg.Readers {
		if err := r.rebind(d); err != nil {
			return err
		}
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	a.rebound = true
	return nil
}

// Release is admission, not business completion. No Stop/cancel/finalize or
// synthetic terminal is sent. Partial releases record which owner resumed so
// retry cannot Bind an already active tracker or lose its remaining obligations.
// origin: hot-update S9 plan §4.3 / §8 / T42–T43
func (a *Adapter) Release(seal i.TransferSeal, d i.Deadline) error {
	if err := take(a.serial, d); err != nil {
		return err
	}
	defer func() { <-a.serial }()
	if seal != a.seal || !seal.Complete || !a.rebound {
		return i.ErrIncomplete
	}
	if err := a.cfg.BeforeReplay(a.tx, d); err != nil {
		return err
	}
	for _, w := range a.cfg.Writers {
		if err := w.release(d); err != nil {
			return err
		}
	}
	for _, r := range a.cfg.Readers {
		if err := r.release(d); err != nil {
			return err
		}
	}
	for n := range a.trackers {
		c := &a.trackers[n]
		if !c.released {
			if err := c.owner.Owner.Bind(c.bindings, d); err != nil {
				return err
			}
			c.released = true
		}
	}
	for n := range a.workers {
		c := &a.workers[n]
		if !c.released {
			if c.owner.ResumeActors != nil {
				if err := c.owner.ResumeActors(d); err != nil {
					return err
				}
			}
			if err := c.owner.Owner.Thaw(c.bindings, d); err != nil {
				return err
			}
			c.released = true
		}
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	a.tx, a.seal, a.rebound = i.ID{}, i.TransferSeal{}, false
	a.workers, a.trackers = nil, nil
	return nil
}
