package brokerimage

import (
	"errors"
	"reflect"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// RuntimeSlot deliberately has no captured old DB/NC/server in an exit defer.
// Its callbacks receive the current owners while replacement and stop serialize.
// The adapters own the actual Node shutdown, listener join and final lock release;
// StoreOwner.CloseForImage alone cannot express a terminal lock release.
// origin: hot-update S6 plan §1.2 / §4.2 / T02
type currentRuntime struct {
	mu           sync.Mutex
	stopMu       sync.Mutex
	store        StoreOwner
	control      ControlOwner
	server       tunnel.ServerImported
	transfer     xfertrack.Manager
	retired      map[any]bool
	stopping     bool
	stopped      bool
	controlDone  bool
	serverDone   bool
	storeDone    bool
	transferDone bool
	stopStore    func(StoreOwner, controlbudget.Deadline) error
	stopServer   func(tunnel.ServerImported, controlbudget.Deadline) error
}

// InstallTransferOwner gives even a nil+error store constructor a terminal
// tracker owner. It runs before ACTIVE and does not open a backend or start work.
// Retired generations cannot reinstall themselves through an old callback.
// origin: hot-update S7 plan §8.2 / P24
func InstallTransferOwner(slot RuntimeSlot, owner xfertrack.Manager) error {
	r, ok := slot.(*currentRuntime)
	if !ok || r == nil || !ownerPointer(owner) {
		return ErrImage
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping || r.stopped || r.retired[owner] {
		return ErrActive
	}
	if r.transfer != nil && r.transfer != owner {
		r.retired[r.transfer] = true
	}
	r.transfer = owner
	return nil
}

// NewRuntimeSlot creates the Run's sole cleanup owner. Callbacks must join actual
// work and report close failures; neither may launch unowned background shutdown.
// The store callback releases the original flock only on ordinary terminal stop.
func NewRuntimeSlot(stopStore func(StoreOwner, controlbudget.Deadline) error,
	stopServer func(tunnel.ServerImported, controlbudget.Deadline) error) (RuntimeSlot, error) {
	if stopStore == nil || stopServer == nil {
		return nil, errors.New("broker image: terminal owner callbacks required")
	}
	return &currentRuntime{retired: make(map[any]bool), stopStore: stopStore, stopServer: stopServer}, nil
}

// Partial constructors can install a non-nil store before inspecting their error,
// then attach control/server to that same current store. Once replaced, any of
// those owners is retired permanently; an old defer cannot reinstall generation A.
// origin: hot-update S6 plan §3.2 / §4.2 / T49
func (r *currentRuntime) Install(store StoreOwner, control ControlOwner, server tunnel.ServerImported) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.installLocked(store, control, server)
}

func (r *currentRuntime) installLocked(store StoreOwner, control ControlOwner, server tunnel.ServerImported) error {
	if r.stopping || r.stopped || !ownerPointer(store) ||
		control != nil && !ownerPointer(control) || server != nil && !ownerPointer(server) {
		return errors.New("broker image: invalid or stopping current runtime")
	}
	for _, owner := range []any{store, control, server} {
		if owner != nil && r.retired[owner] {
			return errors.New("broker image: retired runtime cannot become current")
		}
	}
	// Replacing one member while retaining others is legal (exec-error reopen
	// keeps the frozen server), but dropping a live member loses its owner.
	if r.control != nil && control == nil || r.server != nil && server == nil {
		return errors.New("broker image: current runtime owner cannot be dropped")
	}
	for _, pair := range [][2]any{{r.store, store}, {r.control, control}, {r.server, server}} {
		if pair[0] != nil && pair[0] != pair[1] {
			r.retired[pair[0]] = true
		}
	}
	r.store, r.control, r.server = store, control, server
	return nil
}

// Cut.Thaw holds mu from before the first potential opener until recovery ends.
// Thus normal stop cannot release the old OFD between open and Install, and each
// non-nil failed constructor is recorded before terminal cleanup can snapshot it.
// Constructors/start callbacks must not reenter the slot; they receive owners
// directly and install is performed by the cut itself.
// origin: hot-update S6 plan §1.2 / §4.2 / T02
func lockRuntimeForThaw(slot RuntimeSlot) (*currentRuntime, error) {
	r, ok := slot.(*currentRuntime)
	if !ok || r == nil {
		return nil, errors.New("broker image: reopen requires the Run runtime slot")
	}
	r.mu.Lock()
	if r.stopping || r.stopped {
		r.mu.Unlock()
		return nil, errors.New("broker image: current runtime stopping")
	}
	return r, nil
}

func ownerPointer(owner any) bool {
	value := reflect.ValueOf(owner)
	return value.IsValid() && value.Kind() == reflect.Pointer && !value.IsNil()
}

// A failed join keeps the database and lock owned and fenced. Stop is retryable,
// but never retries an already completed close or accepts a replacement while
// partial stop is in progress. This also protects a store-only failed constructor.
// origin: hot-update S6 plan §3.2 / §4.2 / T02
func (r *currentRuntime) StopCurrent(deadline controlbudget.Deadline) error {
	// Only stopMu is held through joins. A joined callback may acquire the cut
	// latch while Thaw acquires cut -> slot; holding mu here would invert that
	// order. stopping forbids replacement once the snapshot has been taken.
	// origin: hot-update S6 plan §1.2 / §6.2 / T02
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	if deadline <= 0 {
		r.mu.Unlock()
		return errors.New("broker image: stop deadline required")
	}
	r.stopping = true
	store, control, server, transfer := r.store, r.control, r.server, r.transfer
	r.mu.Unlock()
	// Tracker callbacks still need today's NC/ledger/store while joining. A
	// failed join retains every backend and remains retryable in the same slot.
	// origin: hot-update S7 plan §8.2 / P24
	if !r.transferDone {
		if transfer != nil {
			if err := transfer.Stop(deadline); err != nil {
				return err
			}
		}
		r.transferDone = true
	}
	if !r.controlDone {
		if control != nil {
			if err := control.StopCurrent(deadline); err != nil {
				return err
			}
		}
		r.controlDone = true
	}
	if !r.serverDone {
		if server != nil {
			if err := r.stopServer(server, deadline); err != nil {
				return err
			}
		}
		r.serverDone = true
	}
	if !r.storeDone {
		if store != nil {
			if err := r.stopStore(store, deadline); err != nil {
				return err
			}
		}
		r.storeDone = true
	}
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	return nil
}

// Reopen retained callback admission only after Thaw has rebound the transfer
// owner to the current backend. A refused cut never discards that admission.
// origin: hot-update S7 plan §8.2 / T39
func startBusinessWithTransferUsers(control ControlOwner, deadline controlbudget.Deadline) error {
	if retained, ok := control.(interface{ ResumeStoreUsers() error }); ok {
		if err := retained.ResumeStoreUsers(); err != nil {
			return err
		}
	}
	return control.StartBusiness(deadline)
}
