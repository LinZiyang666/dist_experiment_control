package brokerimage

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"sort"
	"sync"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

var (
	ErrCutChanged = errors.New("broker image: cut changed; final export required")
	ErrCutSpent   = errors.New("broker image: cut already thawed or discarded")
	ErrStoreClose = errors.New("broker image: store_close_unknown")
)

// DeferredError keeps a busy transfer census distinct from an unreadable census.
// It retains the transfer fence until rollback, never stops live transfers to
// manufacture a zero count. The broker adapter maps Reason to the existing code.
type DeferredError struct{ Reason string }

func (e *DeferredError) Error() string { return "broker image: deferred (" + e.Reason + ")" }

// mu is the cut/revoke/ExecCommit latch, not a server table mutex. Every worker
// and callback has joined before the cut reaches Export; none may wait for this
// latch while Freeze is joining it. Prepared aliases are owned as one indivisible
// group because handoff has no safe RemoveResource operation.
// origin: hot-update S6 plan §3.2 / §6.2 / §12.1 P01
type frozenCut struct {
	mu               sync.Mutex
	cfg              Config
	baseline         storage.StoreStamp
	server           tunnel.ServerFrozen
	paused           []listenerimage.Keeper
	transfers        bool
	fenced           bool
	storeQuiesced    bool
	closeAttempted   bool
	seal             *storage.StoreSeal
	graph            resourceGraph
	prepared         *handoff.Prepared
	preparedBundle   Bundle
	revoked          map[string]bool
	lockID           string
	slot             RuntimeSlot
	reopened         bool
	controlBuilt     bool
	transportStopped bool
	authStarted      bool
	serverThawed     bool
	transferFrozen   bool
	transferBound    bool
	resumed          map[listenerimage.Keeper]bool
	business         bool
	done             bool
	blocked          bool
}

// Freeze reaches safe points without closing the store: Run must still call
// Node.Shutdown explicitly before CloseForImage and Export. No preliminary
// StoreSeal or tunnel export exists at this stage.
// On failure a non-nil Cut means rollback did not converge; the caller must keep
// that owner, fenced, and retry Thaw rather than discard it or open another store.
// origin: hot-update S6 plan §3.2 / T09
func Freeze(cfg Config) (cut Cut, retErr error) {
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if !ownerPointer(cfg.Store) || !ownerPointer(cfg.Control) || !validMode(cfg.Mode) || cfg.Transfers != nil && !ownerPointer(cfg.Transfers) ||
		cfg.Server != nil && cfg.FreezeServer == nil {
		return nil, errors.New("broker image: complete cut owners required")
	}
	if err := cutDeadline(cfg.Deadline); err != nil {
		return nil, err
	}
	// Validate all keepers before fencing anything; typed nils must not panic
	// halfway through a cut and leave an invisible owner.
	for _, keeper := range cfg.Listeners {
		if !ownerPointer(keeper) {
			return nil, errors.New("broker image: invalid listener owner")
		}
	}
	stamp, err := cfg.Store.Stamp()
	if err != nil {
		return nil, err
	}
	attempt, err := beginObservation(cfg.Observe, controlbudget.PhaseFreeze)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, finishObservation(cfg.Observe, attempt, retErr)) }()
	c := &frozenCut{cfg: cfg, baseline: cloneStamp(stamp), revoked: make(map[string]bool),
		resumed: make(map[listenerimage.Keeper]bool)}
	c.cfg.Listeners = append([]listenerimage.Keeper(nil), cfg.Listeners...)
	c.transfers = true // Even a failing fence may have closed admission.
	count, err := cfg.Control.FenceTransfers(cfg.Deadline)
	if err != nil || count != 0 {
		if err == nil {
			err = &DeferredError{Reason: "transfers_in_flight"}
		}
		return failedFreeze(c, err)
	}
	if cfg.Transfers != nil {
		c.transferFrozen = true // Freeze may already have fenced on refusal.
		if err := cfg.Transfers.FreezeImage(cfg.Deadline); err != nil {
			return failedFreeze(c, err)
		}
	}
	c.fenced = true
	if err := cfg.Control.Fence(cfg.Deadline); err != nil {
		return failedFreeze(c, err)
	}
	for _, keeper := range c.cfg.Listeners {
		c.paused = append(c.paused, keeper)
		if err := keeper.Pause(cfg.Deadline); err != nil {
			return failedFreeze(c, err)
		}
	}
	c.storeQuiesced = true // A refused quiesce can already have fenced admission.
	if err := cfg.Store.Quiesce(cfg.Deadline); err != nil {
		return failedFreeze(c, err)
	}
	if err := cfg.Control.JoinStoreUsers(cfg.Deadline); err != nil {
		return failedFreeze(c, err)
	}
	if cfg.Server != nil {
		// Register a returned partial owner before inspecting its error.
		c.server, err = cfg.FreezeServer(cfg.Server, cfg.Deadline)
		if err != nil || c.server == nil {
			if err == nil {
				err = errors.New("broker image: server freeze returned no owner")
			}
			return failedFreeze(c, err)
		}
	}
	return c, nil
}

func failedFreeze(c *frozenCut, cause error) (Cut, error) {
	if err := c.Thaw(c.cfg.Deadline); err != nil {
		return c, errors.Join(cause, err)
	}
	return nil, cause
}

// BindRuntime is required before a closed-store Thaw. The frozen signature has
// no slot parameter, so Run binds its sole slot once instead of capturing old
// DB/NC owners in an exec-error cleanup closure.
// origin: hot-update S6 plan §1.2 / §4.2 / T02
func BindRuntime(cut Cut, slot RuntimeSlot) error {
	c, ok := unwrapCut(cut).(*frozenCut)
	if !ok || !ownerPointer(slot) {
		return errors.New("broker image: current runtime slot required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.slot != nil && c.slot != slot {
		return errors.New("broker image: runtime slot already bound or cut spent")
	}
	c.slot = slot
	return nil
}

// unwrapCut preserves the one leaf latch when Run adds metadata projection.
// A wrapper must never create a second closing or exec responsibility.
// origin: hot-update S6 plan §3.2 / T01
func unwrapCut(cut Cut) Cut {
	if wrapper, ok := cut.(interface{ ImageCut() Cut }); ok {
		return wrapper.ImageCut()
	}
	return cut
}

// MarkStoreClosing records the irreversible boundary before Run invokes Node
// shutdown or any store close. Until Export validates a final receipt, rollback
// must retain this fenced owner rather than resume a possibly closed writer.
// origin: hot-update S6 plan §3.2 / §4.2 / T10; internal review rv-writer-6
func MarkStoreClosing(cut Cut) error {
	c, ok := unwrapCut(cut).(*frozenCut)
	if !ok {
		return errors.New("broker image: closing cut owner required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.reopened {
		return ErrCutSpent
	}
	if c.blocked {
		return ErrStoreClose
	}
	c.closeAttempted = true
	return nil
}

// Export accepts only a real final receipt. It never invokes CloseForImage or
// claims that an object will close later. Identity/schema/journal remain those
// of the current disk; the final SQL cursor may advance while producers drain.
func (c *frozenCut) Export(seal storage.StoreSeal) (Bundle, []handoff.Resource, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.reopened {
		return Bundle{}, nil, ErrCutSpent
	}
	if c.blocked {
		return Bundle{}, nil, ErrStoreClose
	}
	c.closeAttempted = true
	if err := validateCutSeal(c.cfg.Mode, c.baseline, seal); err != nil {
		c.blocked = true
		return Bundle{}, nil, err
	}
	if c.seal != nil && !reflect.DeepEqual(*c.seal, seal) {
		return Bundle{}, nil, errors.New("broker image: closed store receipt changed")
	}
	copy := cloneSeal(seal)
	c.seal, c.blocked = &copy, false
	if err := c.closePrepared(); err != nil {
		return Bundle{}, nil, err
	}
	return c.exportLocked()
}

func (c *frozenCut) exportLocked() (bundle Bundle, resources []handoff.Resource, retErr error) {
	if c.seal == nil || c.blocked {
		return Bundle{}, nil, ErrStoreClose
	}
	if err := cutDeadline(c.cfg.Deadline); err != nil {
		return Bundle{}, nil, err
	}
	bundle = Bundle{Schema: Domain, SchemaVersion: BundleVersion, Role: ImageRole, Mode: c.cfg.Mode,
		Store: cloneStamp(c.seal.Stamp), Seal: cloneSeal(*c.seal), RequiredDomains: []string{Domain},
		Args: []string{}, Listeners: []listenerimage.Descriptor{}, Maintenance: []brokermaint.LocalLease{}}
	if c.server != nil {
		var err error
		bundle.Tunnel, resources, err = c.server.Export()
		// The tunnel and flow leaves own replacement and terminal snapshot close.
		// Check capacity without claiming their borrowed sealed files as ours.
		// origin: hot-update S6 plan §2.2 / §4.2; internal review rv-fd-4
		_, privateErr := privateFiles(resources)
		if err != nil || privateErr != nil {
			return Bundle{}, nil, errors.Join(err, privateErr)
		}
	}
	// Required domains follow every retained flow responsibility, including
	// idle listeners, retirement and cleanup metadata. ABI 1 alone does not
	// prove that previous can read flow state admitted after pure preflight.
	// origin: hot-update S6 plan §9.1 / hot-update S5 plan §5.2
	if retainedFlow(bundle.Tunnel) {
		bundle.RequiredDomains = append(bundle.RequiredDomains, "flow-v1")
	}
	// Transfer resources participate in exactly the same union as lock/raw/flow.
	// They are leaf-owned originals, never extras secretly appended at Prepare.
	// origin: hot-update S7 plan §8.2 / §8.3 / P24
	if c.cfg.Transfers != nil {
		ref, nested, err := c.cfg.Transfers.ExportImage()
		if err != nil {
			return Bundle{}, nil, err
		}
		bundle.Transfers = &ref
		bundle.RequiredDomains = append(bundle.RequiredDomains, "transfer-v1")
		resources = append(resources, nested...)
	}
	lock, err := c.cfg.Store.LockResource()
	if err != nil {
		return Bundle{}, nil, err
	}
	if len(lock.Files) != 1 || c.revoked[lock.ID] {
		return Bundle{}, nil, errors.New("broker image: one original lock resource required")
	}
	bundle.LockResource = lock.ID
	c.lockID = lock.ID
	resources = append(resources, lock)
	for _, keeper := range c.paused {
		if c.revoked[keeper.Descriptor().ID] {
			continue
		}
		descriptor, resource, err := keeper.Export()
		if err != nil {
			return Bundle{}, nil, err
		}
		if descriptor.ID != resource.ID || len(resource.Files) != 1 {
			return Bundle{}, nil, errors.New("broker image: listener resource identity mismatch")
		}
		bundle.Listeners = append(bundle.Listeners, descriptor)
		resources = append(resources, resource)
	}
	for _, resource := range resources {
		if c.revoked[resource.ID] {
			return Bundle{}, nil, ErrCutChanged // The leaf must omit every revoked owner.
		}
	}
	if err := validateResourceGraph(resources); err != nil {
		return Bundle{}, nil, err
	}
	if _, err := privateFiles(resources); err != nil {
		return Bundle{}, nil, err
	}
	sort.Slice(bundle.Listeners, func(i, j int) bool { return bundle.Listeners[i].ID < bundle.Listeners[j].ID })
	resources = copyResources(resources)
	previous := c.graph.private
	c.graph = resourceGraph{resources: resources}
	if err := closePrivate(previous); err != nil {
		c.blocked = true
		return Bundle{}, nil, err
	}
	return bundle, copyResources(resources), nil
}

// Prepare re-exports fenced leaf decisions. A changed projection requires the
// caller's reader/domain check and broker private encoding to run again; it is
// never silently replaced under an already validated preliminary bundle.
// Caller metadata fills the identity/config fields not present in frozen Config.
// origin: hot-update S6 plan §3.2 / §6.2 / T09 / T25
func (c *frozenCut) Prepare(bundle Bundle, resources []handoff.Resource) (*handoff.Prepared, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepareLocked(bundle, resources, nil)
}

// PrepareFinal projects broker metadata only after the last leaf export under
// the same cut latch. Private provenance must describe these exact memfds, not
// the preliminary export whose snapshots the leaves have already replaced.
// origin: hot-update S6 plan §3.2 / §6.2 / T09 / T25
func PrepareFinal(cut Cut, finalize func(Bundle, []handoff.Resource) (Bundle, []handoff.Resource, error)) (*handoff.Prepared, error) {
	c, ok := unwrapCut(cut).(*frozenCut)
	if !ok || finalize == nil {
		return nil, ErrImage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepareLocked(Bundle{}, nil, finalize)
}

func (c *frozenCut) prepareLocked(bundle Bundle, resources []handoff.Resource, finalize func(Bundle, []handoff.Resource) (Bundle, []handoff.Resource, error)) (*handoff.Prepared, error) {
	if finalize != nil {
		if c.done || c.reopened {
			return nil, ErrCutSpent
		}
		if c.blocked || c.seal == nil {
			return nil, ErrStoreClose
		}
		if err := c.closePrepared(); err != nil {
			return nil, err
		}
		fresh, current, err := c.exportLocked()
		if err != nil {
			return nil, err
		}
		bundle, resources, err = finalize(fresh, current)
		if err != nil {
			return nil, err
		}
		// A successful finalizer hands us its new private files even when a
		// subsequent projection/codec guard refuses. Claim only new pointers:
		// borrowed leaf files retain their leaf closer on every error path.
		// origin: hot-update S6 plan §4.2 / T09 / T25
		borrowed := map[*os.File]bool{}
		for _, resource := range current {
			for _, file := range resource.Files {
				borrowed[file] = true
			}
		}
		if c.graph.private == nil {
			c.graph.private = map[*os.File]bool{}
		}
		for _, resource := range resources {
			if resource.Kind == "sealed-state-v1" {
				for _, file := range resource.Files {
					if file != nil && !borrowed[file] {
						c.graph.private[file] = true
					}
				}
			}
		}
		if !sameProjection(bundle, fresh) {
			return nil, ErrCutChanged
		}
	}

	if c.done || c.reopened {
		return nil, ErrCutSpent
	}
	if c.blocked || c.seal == nil {
		return nil, ErrStoreClose
	}
	if err := Validate(bundle); err != nil {
		return nil, err
	}
	if err := c.closePrepared(); err != nil {
		return nil, err
	}
	// Only broker private state is added by the P02 codec. Every other supplied
	// resource must describe the last export exactly; swapping a lock or raw FD
	// here would silently give the receiver a second terminal owner.
	extra, err := prepareExtras(bundle, resources, c.graph.resources)
	if err != nil {
		return nil, err
	}
	// A repeated Prepare may reuse the broker codec's immutable state file.
	// Preserve that owner while replacing only the server export's private FDs.
	extraOwned, err := privateFiles(extra)
	if err != nil {
		return nil, err
	}
	for file := range extraOwned {
		delete(c.graph.private, file)
	}
	defer func() {
		if c.graph.private == nil {
			c.graph.private = make(map[*os.File]bool)
		}
		for file := range extraOwned {
			c.graph.private[file] = true
		}
	}()
	fresh, current := bundle, c.graph.resources
	if finalize == nil {
		fresh, current, err = c.exportLocked()
		if err != nil {
			return nil, err
		}
	}
	if !sameProjection(bundle, fresh) {
		return nil, ErrCutChanged
	}
	merged, err := handoff.MergeResources(current, extra)
	if err != nil {
		return nil, errors.New("broker image: resource merge failed")
	}
	if err := validateResourceGraph(merged); err != nil {
		return nil, err
	}
	// Include broker, flow and transfer memfds in the one 32 MiB cut ceiling.
	// Per-leaf success must not authorize an oversized combined handoff.
	// origin: hot-update S7 plan §8.3 / P24
	if _, err := privateFiles(merged); err != nil {
		return nil, err
	}
	// Only extraOwned (broker state) transfers to this cut. Leaf state in merged
	// stays borrowed; claiming it here would give each memfd two closers.
	// The exact receiver codec rejects adapter slips before any exec side effect.
	// origin: hot-update S6 plan §4.1; internal review rv-writer-10
	data, err := Encode(bundle)
	if err != nil {
		return nil, err
	}
	p, err := handoff.Prepare(handoff.State{Role: ImageRole, Instance: bundle.InstanceID,
		Generation: bundle.Generation, Data: data}, merged)
	if err != nil {
		return nil, err
	}
	c.prepared, c.preparedBundle = p, cloneBundle(bundle)
	return p, nil
}

// RevokeResources serializes the leaf's full-identity hard revoke with Prepare
// and ExecCommit. The callback must record its fence, SHUT_RDWR raw OFDs, and
// close original/salvage aliases without joining callbacks under this latch.
// Closing the entire Prepared removes every inherited victim alias, including
// duplicates that a leaf cannot see; no sealed snapshot is patched in place.
// An error leaves the cut blocked so the failed revoke cannot be ACKed as safe.
// origin: hot-update S6 plan §6.2 / T25
func RevokeResources(cut Cut, ids []string, revoke func() error) error {
	c, ok := unwrapCut(cut).(*frozenCut)
	if !ok || revoke == nil || len(ids) == 0 {
		return errors.New("broker image: revoke owner and resource IDs required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.reopened {
		return ErrCutSpent
	}
	if c.lockID == "" {
		lock, err := c.cfg.Store.LockResource()
		if err != nil {
			return err
		}
		c.lockID = lock.ID
	}
	for _, id := range ids {
		if !resourceName(id, 256) || id == c.lockID {
			return errors.New("broker image: invalid revoke resource")
		}
	}
	c.blocked = true
	// Remove prepared copies even if the leaf callback subsequently fails.
	closeErr := c.closePrepared()
	for _, id := range ids {
		c.revoked[id] = true
	}
	revokeErr := revoke()
	privateErr := closePrivate(c.graph.private)
	if err := errors.Join(closeErr, revokeErr, privateErr); err != nil {
		return err
	}
	c.graph = resourceGraph{}
	c.blocked = false
	return nil
}

// ExecCommit is the only supported exec entry for a broker cut. Run performs the
// DB-free armed reply, checked flush/sender join and StopTransport first. Validate
// rechecks SHA-bound reader domains, role, maintenance and actual FD whitelist
// under this same latch; it must not join work that can acquire the latch.
// origin: hot-update S6 plan §3.2 / §4.1 / §6.2 / T25
func ExecCommit(cut Cut, prepared *handoff.Prepared, validate func(Bundle) error,
	path string, args, env []string) (retErr error) {
	c, ok := unwrapCut(cut).(*frozenCut)
	if !ok || validate == nil {
		return errors.New("broker image: final exec validation required")
	}
	c.mu.Lock()
	if c.done || c.reopened || c.blocked || prepared == nil || prepared != c.prepared {
		c.mu.Unlock()
		return ErrCutChanged
	}
	control, deadline := c.cfg.Control, c.cfg.Deadline
	c.mu.Unlock()
	// A transport join can wait for an admitted local revoke to acquire the
	// cut latch. Join without that latch, then recheck the exact Prepared; a
	// revoke that wins this interval closes it and forces a fresh final export.
	// origin: hot-update S6 plan §3.2 / §6.2 / T25
	if err := control.StopTransport(deadline); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || c.reopened || c.blocked || prepared == nil || prepared != c.prepared {
		return ErrCutChanged
	}
	c.transportStopped = true
	if err := cutDeadline(c.cfg.Deadline); err != nil {
		return err
	}
	attempt, err := beginObservation(c.cfg.Observe, controlbudget.PhaseExec)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, finishObservation(c.cfg.Observe, attempt, retErr)) }()
	if !reflect.DeepEqual(args, c.preparedBundle.Args) {
		return errors.New("broker image: exec arguments differ from sealed arguments")
	}
	if err := Validate(c.preparedBundle); err != nil {
		return err
	}
	if err := validate(cloneBundle(c.preparedBundle)); err != nil {
		return err
	}
	return prepared.Exec(path, args, handoff.CleanEnvironment(env))
}

func (c *frozenCut) closePrepared() error {
	p := c.prepared
	c.prepared = nil
	if p != nil {
		if err := p.Close(); err != nil {
			c.blocked = true
			return err
		}
	}
	return nil
}

// Thaw reopens only a validated closed current store. A non-nil failed opener
// is installed immediately as a partial current owner, then kept fenced; it is
// never mistaken for a pristine import or used to start another opener.
// origin: hot-update S6 plan §4.2 / T02 / T49
func (c *frozenCut) Thaw(deadline controlbudget.Deadline) error {
	c.mu.Lock()
	if c.done {
		c.mu.Unlock()
		return ErrCutSpent
	}
	if deadline <= 0 {
		c.mu.Unlock()
		return errors.New("broker image: thaw deadline required")
	}
	if c.blocked || c.closeAttempted && c.seal == nil {
		c.mu.Unlock()
		return ErrStoreClose
	}
	control := c.cfg.Control
	stopTransport := c.seal != nil && !c.reopened && !c.transportStopped && !c.blocked
	c.mu.Unlock()
	// Prepare can fail after the store closed but before ExecCommit. Retire the
	// old NC on that path too, before constructing a new control owner; joins
	// stay outside the cut latch so admitted local revokes can finish.
	// origin: hot-update S6 plan §3.2 / §4.2 / T02
	if stopTransport {
		if err := control.StopTransport(deadline); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stopTransport && c.cfg.Control == control {
		c.transportStopped = true
	}
	if c.done {
		return ErrCutSpent
	}
	if deadline <= 0 {
		return errors.New("broker image: thaw deadline required")
	}
	if err := c.closePrepared(); err != nil {
		return err
	}
	if c.blocked || c.closeAttempted && c.seal == nil {
		return ErrStoreClose
	}
	if c.seal != nil {
		if c.slot == nil || c.cfg.NewControl == nil {
			return errors.New("broker image: closed store requires current runtime slot and control constructor")
		}
		// Serialize the entire potential open with normal stop, not just the
		// eventual pointer assignment. The old lock is still the new writer's
		// lock; releasing it during construction would create a second-writer gap.
		// origin: hot-update S6 plan §1.2 / §3.2 / T02
		r, err := lockRuntimeForThaw(c.slot)
		if err != nil {
			return err
		}
		defer r.mu.Unlock()
		if !c.reopened {
			store, err := c.cfg.Store.ReopenCurrent(cloneStamp(c.seal.Stamp), deadline)
			if ownerPointer(store) {
				c.cfg.Store, c.reopened = store, true
				if installErr := r.installLocked(store, c.cfg.Control, r.server); installErr != nil {
					c.blocked = true
					return errors.Join(err, installErr)
				}
			}
			if err != nil || !ownerPointer(store) {
				c.blocked = true
				return errors.Join(err, ErrStoreClose)
			}
		}
		if !c.controlBuilt {
			control, err := c.cfg.NewControl(c.cfg.Store, deadline)
			if ownerPointer(control) {
				c.cfg.Control, c.controlBuilt = control, true
				if installErr := r.installLocked(c.cfg.Store, control, r.server); installErr != nil {
					c.blocked = true
					return errors.Join(err, installErr)
				}
			}
			if err != nil || !ownerPointer(control) {
				c.blocked = true
				return errors.Join(err, ErrStoreClose)
			}
		}
	}
	if c.storeQuiesced && !c.closeAttempted {
		// Store admission must be live before auth/business regain access to SQL.
		// A resume failure retains the same cut for retry; it cannot report ready.
		// origin: hot-update S6 plan §4.2; internal review rv-writer-4
		if err := resumeQuiescedStore(c.cfg.Store); err != nil {
			return err
		}
		c.storeQuiesced = false
	}
	if c.fenced && !c.authStarted {
		if err := observedCall(c.cfg.Observe, controlbudget.PhaseAuth, func() error {
			return c.cfg.Control.StartAuth(deadline)
		}); err != nil {
			return err
		}
		c.authStarted = true
	}
	if c.transferFrozen && !c.transferBound {
		if err := c.cfg.Transfers.Rebind(deadline); err != nil {
			return err
		}
		c.transferBound = true
	}
	if c.server != nil && !c.serverThawed {
		if err := c.server.Thaw(deadline); err != nil {
			return err
		}
		c.serverThawed = true
	}
	for _, keeper := range c.paused {
		if !c.revoked[keeper.Descriptor().ID] && !c.resumed[keeper] {
			if err := keeper.Resume(); err != nil {
				return err
			}
			c.resumed[keeper] = true
		}
	}
	if c.fenced && !c.business {
		if err := observedCall(c.cfg.Observe, controlbudget.PhaseReady, func() error {
			return startBusinessWithTransferUsers(c.cfg.Control, deadline)
		}); err != nil {
			return err
		}
		c.business = true
	}
	if c.transfers {
		if err := c.cfg.Control.ResumeTransfers(); err != nil {
			return err
		}
		c.transfers = false
	}
	if err := closePrivate(c.graph.private); err != nil {
		return err
	}
	c.done = true
	return nil
}

// Discard is pristine relinquishment after another owner has taken its copies.
// It neither releases the flock nor shuts down borrowed raw/listener OFDs.
func (c *frozenCut) Discard() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return ErrCutSpent
	}
	if c.reopened || c.blocked || c.closeAttempted && c.seal == nil {
		return errors.New("broker image: active or unknown owner cannot be discarded")
	}
	if err := c.closePrepared(); err != nil {
		return err
	}
	if c.server != nil {
		if err := c.server.Discard(); err != nil {
			return err
		}
	}
	if err := closePrivate(c.graph.private); err != nil {
		return err
	}
	c.done = true
	return nil
}

// resumeQuiescedStore keeps the frozen StoreOwner contract unchanged. Each
// concrete owner reverses only its own admission fence, never a shutdown.
func resumeQuiescedStore(owner StoreOwner) error {
	switch s := owner.(type) {
	case *clusterStore:
		return ResumeClusterStore(s)
	case *singleStoreOwner:
		return resumeSingleStore(s)
	default:
		if resume, ok := owner.(interface{ Resume() error }); ok {
			return resume.Resume()
		}
		return errors.New("broker image: store admission resume owner required")
	}
}

func cutDeadline(deadline controlbudget.Deadline) error {
	now, err := procrequest.BootNow()
	if err != nil {
		return err
	}
	if deadline <= 0 || int64(deadline) <= now {
		return errors.New("broker image: cut deadline expired")
	}
	return nil
}

func validMode(mode Mode) bool {
	switch mode {
	case ModeSingle, ModeSoleVoter, ModeQuorum:
		return true
	default:
		return false
	}
}

// validateCutSeal checks the live cut's seal against its mode and opening
// baseline; codec.go's validateSeal checks only an encoded seal's shape.
func validateCutSeal(mode Mode, baseline storage.StoreStamp, seal storage.StoreSeal) error {
	// Stopped/unknown phases and a nonzero real connection census forbid a
	// second writer even when some Close calls returned success.
	// origin: hot-update S6 plan §3.2 / T09
	if !StoreSealClosed(seal) ||
		!reflect.DeepEqual(baseline.Identity, seal.Stamp.Identity) ||
		!reflect.DeepEqual(baseline.Schema, seal.Stamp.Schema) || baseline.Journal != seal.Stamp.Journal ||
		seal.Stamp.Synchronous != 2 || baseline.Synchronous != seal.Stamp.Synchronous ||
		seal.Stamp.AppliedIndex < baseline.AppliedIndex || seal.Stamp.AppliedTerm < baseline.AppliedTerm ||
		seal.Stamp.RaftLastIndex < baseline.RaftLastIndex || seal.Stamp.RaftConfigIndex < baseline.RaftConfigIndex {
		return ErrStoreClose
	}
	if mode != ModeSingle && (!seal.Role.ObserverClosed || seal.Role.Term == 0 || seal.Role.ConfigIndex == 0 ||
		seal.Role.VoterDigest == "" || mode == ModeQuorum && seal.Role.LastRunningIsLeader) {
		return ErrStoreClose
	}
	return nil
}

func beginObservation(observer controlbudget.Observer, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if observer == nil {
		return controlbudget.Attempt{}, nil
	}
	return observer.Begin("broker-cut", phase)
}

func finishObservation(observer controlbudget.Observer, attempt controlbudget.Attempt, cause error) error {
	if observer == nil {
		return nil
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return err
	}
	attempt.FinishedMonoNS = now
	attempt.Class = "ok"
	if cause != nil {
		attempt.Class = "error" // Never stringify private errors into observations.
	}
	return observer.Record(attempt)
}

func observedCall(observer controlbudget.Observer, phase controlbudget.AttemptPhase, fn func() error) error {
	attempt, err := beginObservation(observer, phase)
	if err != nil {
		return err
	}
	err = fn()
	return errors.Join(err, finishObservation(observer, attempt, err))
}
