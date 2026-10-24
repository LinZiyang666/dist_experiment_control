// owner.go owns the transfer admission fence and current/pristine resource
// lifetime. A control connection replacement never owns this lifetime.
// origin: hot-update S7 plan §4 / §8.2
package xferworker

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/jsstream"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/nats-io/nats.go/jetstream"
)

// Package xferworker exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
type Config = x.WorkerConfig
type Bindings = x.Bindings
type Manager = x.WorkerManager
type Frozen = x.WorkerFrozen
type Factory = x.WorkerFactory
type ImportFactory = x.WorkerImportFactory

var _ Factory = NewWorker
var _ ImportFactory = ImportWorker
var _ Manager = (*owner)(nil)
var _ Frozen = (*frozen)(nil)

type entry struct {
	value     x.WorkerEntry
	retention x.RetentionState
	sender    x.Sender
	receiver  x.Receiver
	courier   x.Courier
	// The token serializes controls and actor steps without holding the registry
	// mutex across filesystem/network callbacks. Waiters retain their deadline.
	token      chan struct{}
	running    bool
	done       chan struct{}
	actorError error
	sealed     bool
	// published: the sender's fixed object version is complete; it holds no
	// further byte work, only retained ownership until its retirement.
	published    bool
	byteDeadline x.BootDeadline
}

type owner struct {
	// Registry data uses this mutex; callbacks use admission plus entry tokens.
	// Keeping the two separate lets a cut expire while a child is in D-state.
	mu        sync.Mutex
	cfg       Config
	bootID    string
	transport *transportBinding
	state     x.WorkerState
	entries   map[x.Key]*entry
	resources []handoff.Resource
	changed   chan struct{}
	creating  chan struct{}
	// Serialize cut/rollback/stop decisions separately from admitted business
	// calls. Otherwise Thaw could reopen admission between join and snapshot.
	lifecycle chan struct{}
	// A failed exec may Thaw without advancing the generation. Cut handles
	// still need invalidation, so their lifetime also includes this local epoch.
	epoch                               uint64
	inflight                            uint32
	closed, stopped, stopping, pristine bool
	freezeJob                           chan struct{}
	freezeErr                           error
	actorsImported                      bool
	cleanup                             []*jsstream.ConsumerCleanupOwner
}

type frozen struct {
	owner             *owner
	generation, epoch uint64
}

func workerError(code string) error { return fmt.Errorf("xferworker: %s", code) }

// NewWorker creates no transport, goroutine or file. Creation permits are
// authenticated by the outer control adapter; this owner enforces their bound
// identity, original origin and first-creation expiry before reserving capacity.
func NewWorker(cfg Config) (Manager, error) { return newWorkerOwner(cfg) }

// newWorkerOwner returns the concrete owner so ImportWorker installs retained
// state without asserting on the public Manager.
func newWorkerOwner(cfg Config) (*owner, error) {
	if cfg.Clock == nil || cfg.UTC == nil || cfg.Capacity == nil || cfg.Authority == nil || cfg.Identity.AgentInstance == "" {
		return nil, workerError(x.CodeOwnerRecovering)
	}
	boot, now, err := cfg.Clock()
	if err != nil || boot == "" || now <= 0 {
		return nil, errors.Join(workerError(x.CodeOwnerRecovering), err)
	}
	o := &owner{cfg: cfg, bootID: boot, entries: make(map[x.Key]*entry), changed: make(chan struct{}), creating: make(chan struct{}, 1), lifecycle: make(chan struct{}, 1)}
	o.transport = &transportBinding{current: cfg.Transport}
	o.state = x.WorkerState{Schema: x.StateSchema, Lineage: x.Lineage{InstanceID: cfg.Identity.AgentInstance, BootID: boot, Generation: 1}, Admission: x.AdmissionState{Generation: 1}}
	return o, nil
}

// ImportWorker remains sleeping. TakeFile's nonnil-file-plus-error case still
// transfers ownership and is returned in salvage; a partial failure cannot
// close a business OFD needed by the caller's pristine rollback.
// origin: hot-update S7 plan §8.2 / T14
func ImportWorker(cfg Config, state x.WorkerState, source x.FileSource) (Manager, x.Salvage, error) {
	o, err := newWorkerOwner(cfg)
	if err != nil {
		return nil, x.Salvage{}, err
	}
	if err = validateImport(o, state); err != nil {
		return nil, x.Salvage{}, err
	}
	var resources []handoff.Resource
	for _, ref := range xferimage.WorkerResourceRefs(state) {
		if source == nil {
			return nil, x.Salvage{Resources: resources}, workerError(x.CodeOwnerRecovering)
		}
		file, takeErr := source.TakeFile(ref.ResourceID, ref.Name)
		if file != nil {
			resources = append(resources, handoff.Resource{ID: ref.ResourceID, Kind: ref.Kind, Files: map[string]*os.File{ref.Name: file}})
		}
		if takeErr != nil || file == nil {
			return nil, x.Salvage{Resources: resources}, errors.Join(workerError(x.CodeOwnerRecovering), takeErr)
		}
	}
	// Reservations in an imported image name the still-live process account.
	// They are not reserved again: doing so charges the same originals twice.
	o.state = xferimage.CloneWorkerState(state)
	o.closed, o.pristine = true, true
	o.state.Admission.Closed = true
	o.resources = resources
	for _, value := range state.Entries {
		e := &entry{value: xferimage.CloneWorkerEntry(value), token: make(chan struct{}, 1)}
		for _, retained := range state.ResultRetention {
			if retained.Identity == value.Prepare.Identity {
				e.retention = retained
			}
		}
		e.byteDeadline = e.retention.Deadline
		o.entries[key(value.Prepare.Identity)] = e
	}
	// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 2
	// Install every detached duty while the worker is still sleeping. Partial
	// import keeps the owner reachable with its taken descriptors and handles.
	for _, state := range state.ConsumerCleanup {
		duty, err := jsstream.ImportConsumerCleanup(state)
		if err != nil {
			return o, x.Salvage{Resources: resources}, err
		}
		o.cleanup = append(o.cleanup, duty)
	}
	o.observe(x.Observation{Kind: x.ObserveImport, AdmissionClosed: true})
	return o, x.Salvage{Resources: append([]handoff.Resource(nil), resources...)}, nil
}

func (o *owner) signalLocked() { close(o.changed); o.changed = make(chan struct{}) }

func (o *owner) remaining(deadline x.Deadline) (time.Duration, error) {
	boot, now, err := o.cfg.Clock()
	if err != nil {
		return 0, err
	}
	// The immutable boot identity survives replacement of the snapshot value.
	// Reading it out of state would race the exporter replacing the whole state.
	if boot != o.bootID || deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (o *owner) wait(ch <-chan struct{}, deadline x.Deadline) error {
	// All waits consume the caller's original absolute deadline. A per-phase
	// timeout here would silently extend the maintenance/cut allowance.
	remaining, err := o.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (o *owner) enter(deadline x.Deadline) error {
	if _, err := o.remaining(deadline); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.stopped {
		return workerError(x.CodeOwnerRecovering)
	}
	// Include authorization and factory callbacks, not only byte IO. Either
	// can allocate a retained entry after a misleadingly empty snapshot.
	o.inflight++
	return nil
}

func (o *owner) leave() { o.mu.Lock(); o.inflight--; o.signalLocked(); o.mu.Unlock() }

func (o *owner) join(deadline x.Deadline) error {
	// changed is replaced under the same mutex as the count. A waiter cannot
	// miss the transition to zero, including a callback finishing before select.
	for {
		o.mu.Lock()
		count, changed := o.inflight, o.changed
		o.mu.Unlock()
		if count == 0 {
			return nil
		}
		if err := o.wait(changed, deadline); err != nil {
			return err
		}
	}
}

func (o *owner) lockEntry(e *entry, deadline x.Deadline) error {
	remaining, err := o.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case e.token <- struct{}{}:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (o *owner) observe(event x.Observation) {
	o.mu.Lock()
	observer := o.cfg.Observe
	event.Generation, event.BootID = o.state.Lineage.Generation, o.state.Lineage.BootID
	o.mu.Unlock()
	if observer != nil {
		observer(event)
	}
}

// Freeze closes future admission first. An already-entered callback owns its
// old generation until it returns; timeout leaves the same owner and actors.
// A blocked child Freeze is still a tracked job, never an exportable image.
// origin: hot-update S7 plan §8.2 / T18
func (o *owner) Freeze(deadline x.Deadline) (Frozen, error) {
	if err := o.lockEntry(&entry{token: o.lifecycle}, deadline); err != nil {
		return nil, err
	}
	defer func() { <-o.lifecycle }()
	if _, err := o.remaining(deadline); err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.stopped || o.stopping {
		o.mu.Unlock()
		return nil, workerError(x.CodeOwnerRecovering)
	}
	o.closed = true
	o.signalLocked()
	o.mu.Unlock()
	if err := o.join(deadline); err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.freezeJob == nil {
		o.freezeJob = make(chan struct{})
		// This goroutine belongs to the owner until the completion barrier joins
		// it. Thaw/Stop also join it after a deferred cut; no late snapshot can
		// authorize another owner while a filesystem callback remains blocked.
		go o.snapshot(deadline)
	}
	done := o.freezeJob
	o.mu.Unlock()
	if err := o.wait(done, deadline); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.freezeErr != nil {
		return nil, o.freezeErr
	}
	return &frozen{owner: o, generation: o.state.Lineage.Generation, epoch: o.epoch}, nil
}

func (o *owner) snapshot(deadline x.Deadline) {
	o.mu.Lock()
	state := xferimage.CloneWorkerState(o.state)
	state.Entries, state.ResultRetention, state.Senders, state.Receivers, state.Couriers = nil, nil, nil, nil, nil
	var entries []*entry
	for _, e := range o.entries {
		entries = append(entries, e)
	}
	o.mu.Unlock()
	// Stable ordering makes repeated current cuts describe the same registry,
	// independently of Go's map iteration order and the prior image's ordering.
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].value.Prepare.Identity, entries[j].value.Prepare.Identity
		if a.SID != b.SID {
			return a.SID < b.SID
		}
		return a.TransferID < b.TransferID
	})
	var err error
	var resources []handoff.Resource
	for _, e := range entries {
		state.Entries = append(state.Entries, xferimage.CloneWorkerEntry(e.value))
		state.ResultRetention = append(state.ResultRetention, e.retention)
		if e.value.Status.Phase == x.PhaseSettled {
			// A settled entry's actors already stopped and closed their files
			// (a stopped actor cannot freeze again); only its registry value and
			// retention cross the cut. Freezing them failed the closure check,
			// so every cut after any settled transfer failed until GC.
			// origin: hot-update S7 plan §8.2 / P21 — S7 production activation
			continue
		}
		if e.sender != nil {
			var s x.SenderState
			s, err = e.sender.Freeze(deadline)
			if err != nil {
				break
			}
			o.mu.Lock()
			if s.Budget != (x.BudgetSeal{}) {
				e.value.Prepare.Budget, e.value.Status.Budget = s.Budget, s.Budget
			}
			if s.ByteDeadline.Deadline > 0 {
				e.byteDeadline, e.retention.Deadline = s.ByteDeadline, s.ByteDeadline
			}
			if s.Budget.SourceSealed {
				seal := s.SourceSeal
				e.value.Prepare.Source = &seal
			}
			o.mu.Unlock()
			state.Senders = append(state.Senders, s)
		}
		if e.receiver != nil {
			var r x.ReceiverState
			r, err = e.receiver.Freeze(deadline)
			if err != nil {
				break
			}
			o.mu.Lock()
			if r.Budget != (x.BudgetSeal{}) {
				e.value.Prepare.Budget, e.value.Status.Budget = r.Budget, r.Budget
			}
			if r.ByteDeadline.Deadline > 0 {
				e.byteDeadline, e.retention.Deadline = r.ByteDeadline, r.ByteDeadline
			}
			e.value.GateClosed = e.value.GateClosed || r.GateClosed
			e.value.Status.DurableOffset, e.value.Status.ChunkIndex, e.value.Status.LastConsumedStreamSeq = r.Checkpoint.DurableOffset, r.Checkpoint.ChunkIndex, r.Checkpoint.LastConsumedStreamSeq
			o.mu.Unlock()
			state.Receivers = append(state.Receivers, r)
		}
		if e.courier != nil {
			var c x.CourierState
			c, err = e.courier.Freeze(deadline)
			if err != nil {
				break
			}
			state.Couriers = append(state.Couriers, c)
			if c.Canonical.Digest != "" {
				xferimage.RetainWorkerOutcome(&state, x.CanonicalOutcome{Identity: c.Identity, Outcome: c.Outcome, Canonical: c.Canonical, Receipt: c.Receipt})
			}
		}
		state.Entries[len(state.Entries)-1] = xferimage.CloneWorkerEntry(e.value)
		state.ResultRetention[len(state.ResultRetention)-1] = e.retention
		for _, actor := range []any{e.sender, e.receiver} {
			if exporter, ok := actor.(interface{ Resources() []handoff.Resource }); ok {
				resources = append(resources, exporter.Resources()...)
			}
		}
	}
	if !o.pristine {
		o.resources = resources
	}
	// Imported actors remain immutable until the restoration contract exists.
	// Their absence from the live heap must not erase them on a pristine cut.
	if o.pristine {
		state.Senders, state.Receivers, state.Couriers = o.state.Senders, o.state.Receivers, o.state.Couriers
	}
	// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 2
	// Child Freeze joined and retired readers first; exporting a live reader's
	// cleanup owner is refused rather than dropping its future deletion duty.
	if err == nil {
		for _, duty := range jsstream.ConsumerCleanupOwners() {
			for _, receiver := range state.Receivers {
				if duty.ID() == receiver.Reader.ConnectionIdentity {
					_ = RetainConsumerCleanup(o, duty)
				}
			}
		}
		state.ConsumerCleanup = nil
		o.mu.Lock()
		duties := append([]*jsstream.ConsumerCleanupOwner(nil), o.cleanup...)
		o.mu.Unlock()
		sort.Slice(duties, func(i, j int) bool { return duties[i].ID() < duties[j].ID() })
		for _, duty := range duties {
			var cleanup x.ConsumerCleanupState
			cleanup, err = duty.Freeze(deadline)
			if err != nil {
				break
			}
			state.ConsumerCleanup = append(state.ConsumerCleanup, cleanup)
		}
	}
	state.Admission = x.AdmissionState{Closed: true, Generation: state.Lineage.Generation}
	if err == nil {
		err = o.checkResourceClosure(state)
	}
	if err == nil {
		err = validateImport(o, state)
	}
	o.mu.Lock()
	if err == nil {
		o.state = xferimage.CloneWorkerState(state)
	}
	o.freezeErr = err
	o.mu.Unlock()
	if err == nil {
		o.observe(x.Observation{Kind: x.ObserveFreeze, AdmissionClosed: true})
	}
	o.mu.Lock()
	close(o.freezeJob)
	o.mu.Unlock()
}

func (f *frozen) Export() (x.WorkerState, []handoff.Resource, error) {
	// Export borrows descriptors but copies all mutable metadata. A caller
	// editing its codec input must not rewrite the owner's rollback state.
	o := f.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.closed || o.stopped || o.freezeJob == nil || f.generation != o.state.Lineage.Generation || f.epoch != o.epoch {
		return x.WorkerState{}, nil, workerError(x.CodeOwnerRecovering)
	}
	select {
	case <-o.freezeJob:
	default:
		return x.WorkerState{}, nil, workerError(x.CodeOwnerRecovering)
	}
	if o.freezeErr != nil {
		return x.WorkerState{}, nil, o.freezeErr
	}
	return xferimage.CloneWorkerState(o.state), cloneResources(o.resources), nil
}

func (f *frozen) DetachPristine() (x.Salvage, error) {
	// Pristine detach is ownership transfer, not shutdown: closing these files
	// would destroy the originals needed by the previous image's importer.
	o := f.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.pristine || o.stopped || !o.closed || f.generation != o.state.Lineage.Generation || f.epoch != o.epoch {
		return x.Salvage{}, workerError(x.CodeOwnerRecovering)
	}
	o.stopped = true
	resources := o.resources
	o.resources = nil
	o.signalLocked()
	return x.Salvage{Resources: resources}, nil
}

// Thaw is also the timeout rollback path. Rebinding behind an unjoined child
// callback would give that old callback access to a replacement transport.
func (o *owner) Thaw(bindings Bindings, deadline x.Deadline) error {
	if err := o.lockEntry(&entry{token: o.lifecycle}, deadline); err != nil {
		return err
	}
	defer func() { <-o.lifecycle }()
	if _, err := o.remaining(deadline); err != nil {
		return err
	}
	o.mu.Lock()
	if !o.closed || o.stopped || o.stopping {
		o.mu.Unlock()
		return workerError(x.CodeOwnerRecovering)
	}
	done := o.freezeJob
	o.mu.Unlock()
	if err := o.join(deadline); err != nil {
		return err
	}
	if done != nil {
		if err := o.wait(done, deadline); err != nil {
			return err
		}
	}
	o.mu.Lock()
	invalid := o.stopped || o.stopping || !o.closed || bindings.Authority == nil || bindings.OwnerGeneration < o.state.Lineage.Generation
	o.mu.Unlock()
	if invalid {
		return workerError(x.CodeOwnerRecovering)
	}
	// P00 supplies creation factories only, with no frozen actor input.
	// Refuse to fabricate resumed actors or drop their cursors.
	// Prepared-only imports are fully representable without actor restoration.
	if o.pristine && !o.actorsImported && (len(o.state.Senders) > 0 || len(o.state.Receivers) > 0 || len(o.state.Couriers) > 0) {
		return workerError(x.CodeOwnerRecovering)
	}
	// Rebind detached duties before any reader or registry admission reopens.
	// Bind joins the old management attempt outside the registry mutex.
	if err := RebindConsumerCleanup(o, bindings.JS, deadline); err != nil {
		return err
	}
	// Actors capture this stable port, not a control-session context or client.
	// Swap its backend only after every old callback and child Freeze joined.
	o.transport.mu.Lock()
	o.transport.current = bindings.Transport
	o.transport.mu.Unlock()
	o.cfg.Authority, o.cfg.Transport = bindings.Authority, bindings.Transport
	// A current-cut rollback reopens what each live actor's Freeze fenced
	// (writer, atomic sink, reader, receiver admission) through the actor's
	// own optional Thaw. Retained actors kept their identity and cursors;
	// without this every refused or failed cut stranded them all.
	// origin: hot-update S7 plan §8.2 / P21 / T37 — S7 production activation
	if !o.pristine || o.actorsImported {
		for _, e := range o.entries {
			if e.value.Status.Phase == x.PhaseSettled {
				continue
			}
			for _, actor := range []any{e.sender, e.receiver} {
				if err := thawActor(actor, deadline); err != nil {
					return err
				}
			}
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.state.Lineage.Generation = bindings.OwnerGeneration
	o.state.Admission = x.AdmissionState{Generation: bindings.OwnerGeneration}
	for _, e := range o.entries {
		e.value.OwnerGeneration = bindings.OwnerGeneration
	}
	restart := o.pristine
	o.closed, o.pristine = false, false
	o.epoch++
	o.freezeJob = nil
	o.freezeErr = nil
	o.signalLocked()
	if restart {
		for _, e := range o.entries {
			if (e.value.Started || e.courier != nil) && e.value.Status.Phase != x.PhaseSettled && !e.running {
				e.running, e.done = true, make(chan struct{})
				go o.run(e)
			}
		}
	}
	return nil
}

// Stop owns actor joins and resource release. A partial failure retains the
// reservation and resource references for a later Stop; TTL cannot erase them.
func (o *owner) Stop(deadline x.Deadline) error {
	if err := o.lockEntry(&entry{token: o.lifecycle}, deadline); err != nil {
		return err
	}
	defer func() { <-o.lifecycle }()
	o.mu.Lock()
	if o.stopped {
		o.mu.Unlock()
		return nil
	}
	if o.stopping {
		o.mu.Unlock()
		return workerError(x.CodeOwnerRecovering)
	}
	o.closed, o.stopping = true, true
	o.signalLocked()
	done := o.freezeJob
	var stopping []*entry
	for _, e := range o.entries {
		stopping = append(stopping, e)
	}
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.stopping = false; o.mu.Unlock() }()
	// origin: S7 internal review rv-lifecycle F4
	// Stop signals interruptible byte IO before its owning callback is joined.
	for _, e := range stopping {
		if err := o.stopBytes(e, deadline); err != nil {
			return err
		}
	}
	if err := o.join(deadline); err != nil {
		return err
	}
	if done != nil {
		if err := o.wait(done, deadline); err != nil {
			return err
		}
	}
	var errs []error
	for _, e := range o.entries {
		if e.done != nil {
			if err := o.wait(e.done, deadline); err != nil {
				return err
			}
		}
	}
	for _, e := range o.entries {
		errs = append(errs, o.stopBytes(e, deadline))
		if e.courier != nil {
			errs = append(errs, e.courier.Stop(deadline))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if o.pristine && !o.actorsImported {
		for _, resource := range o.resources {
			for _, file := range resource.Files {
				errs = append(errs, file.Close())
			}
		}
	}
	o.mu.Lock()
	duties := append([]*jsstream.ConsumerCleanupOwner(nil), o.cleanup...)
	o.mu.Unlock()
	for _, duty := range duties {
		if err := duty.Wait(deadline); err != nil {
			return err
		}
	}
	o.cleanup = nil
	o.resources = nil
	if err := errors.Join(errs...); err != nil {
		return err
	}
	for _, e := range o.entries {
		if e.value.Reservation != "" {
			if err := o.cfg.Capacity.Release(e.value.Reservation); err != nil {
				return err
			}
			e.value.Reservation = ""
		}
	}
	o.mu.Lock()
	o.stopped = true
	o.signalLocked()
	o.mu.Unlock()
	return nil
}

// A control connection is replaceable; the subactor's lifetime port is not.
// Fetch the backend once per operation, then release the mutex before network
// work. The enclosing worker fence joins operations before changing it.
type transportBinding struct {
	mu      sync.Mutex
	current x.Transport
}

var _ x.Transport = (*transportBinding)(nil)

func (b *transportBinding) target() (x.Transport, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.current == nil {
		return nil, workerError(x.CodeOwnerRecovering)
	}
	return b.current, nil
}
func (b *transportBinding) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	target, err := b.target()
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	return target.Request(r, d)
}
func (b *transportBinding) OpenData(id x.Identity, d x.Deadline) (jetstream.JetStream, error) {
	target, err := b.target()
	if err != nil {
		return nil, err
	}
	return target.OpenData(id, d)
}
func (b *transportBinding) CloseData(id x.Identity, d x.Deadline) error {
	target, err := b.target()
	if err != nil {
		return err
	}
	return target.CloseData(id, d)
}

func (o *owner) checkResourceClosure(state x.WorkerState) error {
	// Actor snapshots currently expose references without an FD-export port.
	// A reference alone is insufficient: refuse a cut that cannot carry every
	// original descriptor instead of producing a broken but plausible image.
	files := make(map[x.ResourceRef]bool)
	for _, res := range o.resources {
		for name, file := range res.Files {
			if file == nil {
				return workerError(x.CodeOwnerRecovering)
			}
			files[x.ResourceRef{ResourceID: res.ID, Name: name, Kind: res.Kind}] = true
		}
	}
	refs := xferimage.WorkerResourceRefs(state)
	if len(files) != len(refs) {
		return ErrActorCustody
	}
	for _, ref := range refs {
		if !files[ref] {
			return ErrActorCustody
		}
	}
	return nil
}

// Retained includes settled results until their original retention expires.
// A repeated request must see the same receipt on either side of an image cut.
// origin: hot-update S7 plan §4 / §8.3 / T14
func Retained(m Manager) bool {
	o, ok := m.(*owner)
	if !ok {
		return m != nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.entries) != 0 || len(o.cleanup) != 0
}

// ErrActorCustody refuses a cut whose live actors name descriptors this
// owner cannot hand over (no FD-export port yet). It keeps the owner_recovering
// code; the agent's image adapter reports it as a transfers-in-flight deferral
// and the rollback thaws every actor the snapshot froze.
// origin: hot-update S7 plan §8.2 / §8.4 / P21 — S7 production activation
var ErrActorCustody = workerError(x.CodeOwnerRecovering)

func validateImport(o *owner, state x.WorkerState) error {
	// Validate the entire closure before the first TakeFile. This distinguishes
	// a refused image (no ownership side effect) from a partial take (salvage).
	if state.Schema != x.StateSchema || state.Lineage.BootID != o.state.Lineage.BootID || state.Lineage.InstanceID != o.state.Lineage.InstanceID || state.Lineage.Generation == 0 || !state.Admission.Closed || state.Admission.Inflight != 0 || state.Admission.Generation != state.Lineage.Generation || len(state.Entries) > registrySlots {
		return workerError(x.CodeOwnerRecovering)
	}
	if !xferimage.ValidWorkerConsumerCleanup(state) {
		return workerError(x.CodeIntentConflict)
	}
	for _, e := range state.Entries {
		if err := o.prepareValid(e.Prepare); err != nil {
			return err
		}
	}
	if code := xferimage.WorkerRegistryClosureCode(state); code != "" {
		return workerError(code)
	}
	return nil
}

func key(id x.Identity) x.Key { return x.Key{SID: id.SID, TransferID: id.TransferID} }

// The owner census enumerates the worker's retained responsibility by
// identity for the S8 upgrade census. A transaction's original scope must name
// every object a cut carries (hot-update S8 plan §4.2: "transfer worker/
// terminal/result/grant/GC permits"), and a domain that cannot be enumerated
// refuses rather than reading as zero. Retained only answers whether anything
// is held; this answers what, without changing any of it. (It lives in this
// file, beside the owner whose registry it reads, so the package keeps its S7
// file cap and every file stays within its code ceiling.)
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census)

// CensusEntry is one registry entry: the identity it was prepared with (the
// business key plus the EntryNonce that distinguishes a replacement slot) and
// whether it already settled. A settled entry holds only its retained result
// until its original retention expires (Retained).
type CensusEntry struct {
	Identity x.Identity
	Settled  bool
}

// Census lists every registry entry and every retained consumer-cleanup duty.
// It takes only the registry mutex, never the lifecycle token or an entry
// token: a cut's census must not wait behind a parked actor or a blocked
// filesystem callback, and it neither admits nor retires anything. During a
// frozen cut admission is closed, so the list is exactly the frozen image's.
// ok is false for a manager this package did not build: its registry cannot
// be enumerated, and the caller must refuse instead of reporting none.
func Census(m Manager) (entries []CensusEntry, cleanup []string, ok bool) {
	o, isOwner := m.(*owner)
	if !isOwner || o == nil {
		return nil, nil, false
	}
	o.mu.Lock()
	for _, e := range o.entries {
		entries = append(entries, CensusEntry{Identity: e.value.Prepare.Identity, Settled: e.value.Status.Phase == x.PhaseSettled})
	}
	for _, duty := range o.cleanup {
		cleanup = append(cleanup, duty.ID())
	}
	o.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].Identity, entries[j].Identity
		if a.SID != b.SID {
			return a.SID < b.SID
		}
		return a.TransferID < b.TransferID
	})
	sort.Strings(cleanup)
	return entries, cleanup, true
}
