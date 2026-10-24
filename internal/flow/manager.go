package flow

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

var ErrClosed = errors.New("flow: owner closed")
var ErrFrozen = errors.New("flow: owner frozen")

// Failure carries a public reason only. Neither raw errors nor target addresses
// escape through the carrier or image diagnostics.
type Failure struct{ Reason Reason }

func (e *Failure) Error() string { return fmt.Sprintf("flow: rejected (%d)", e.Reason) }
func (e *Failure) Is(target error) bool {
	other, ok := target.(*Failure)
	return ok && e.Reason == other.Reason
}
func failure(reason Reason) error { return &Failure{Reason: reason} }

type managerState struct {
	mu  sync.Mutex
	cfg Config
	// The journal retains bounded terminal receipts; retirement.live is the
	// separate admission set. Completed history must not enter live RESUME.
	records           map[ID]*record
	retirement        map[EndpointID]*retirement
	frozen            bool
	closed            bool
	change            chan struct{}
	pristine          bool // Never activated or changed since import.
	importedResources []handoff.Resource
}

// Each direction has one raw worker. All byte/FIN decisions commit under mu,
// after the actual syscall result, before a worker announces its safe point.
// No carrier IO or user callback runs under either owner lock.
// origin: hot-update S5 plan §3.1 / §3.5
// Lock order: manager -> record -> replay; workers never acquire manager.mu.
type record struct {
	settle                                         func(bool)
	mu                                             sync.Mutex
	connectDeadlineBootNS                          uint64
	open                                           OpenRequest
	phase                                          Phase
	raw                                            *rawsocket.Socket
	replay                                         *workload.Replay
	replayFile                                     *os.File
	terminalTx                                     workload.ReplayState
	rxApplied, rxReceived                          uint64
	rxPending, txPending                           []byte
	txError, writerError                           string
	sourceEnded                                    bool
	txFIN, rxFIN                                   uint64
	txFINSet, rxFINSet, rxCloseWrite, txFINReceipt bool
	finalEnd, finalApplied                         [2]uint64
	closeMode                                      CloseMode
	reason                                         Reason
	openStatus                                     uint8
	openReason                                     Reason
	carrierGen                                     uint64
	ownerGen                                       uint64
	sent                                           uint64
	revoked                                        bool
	cleanupPending                                 bool
	closeReceipt                                   bool
	change                                         chan struct{}
	stop                                           chan struct{}
	done                                           chan struct{}
	running                                        bool
	desired                                        bool
	closed                                         bool
	dial                                           func(ID, uint16, time.Time, <-chan struct{}) (*rawsocket.Socket, error)
}

func New(cfg Config) (*Manager, error) {
	if cfg.Budget == nil {
		return nil, failure(ReasonCapacity)
	}
	if cfg.MaxFlows == 0 {
		cfg.MaxFlows = MaxFlows
	}
	if cfg.MaxPendingFDs == 0 {
		cfg.MaxPendingFDs = MaxPendingFDs
	}
	if cfg.MaxFlows < 1 || cfg.MaxFlows > MaxFlows || cfg.MaxPendingFDs < 1 || cfg.MaxPendingFDs > MaxPendingFDs {
		return nil, failure(ReasonCapacity)
	}
	return &Manager{state: &managerState{cfg: cfg, records: make(map[ID]*record), retirement: make(map[EndpointID]*retirement), change: make(chan struct{})}}, nil
}
func changed(r *record) {
	if drillobserve.Hook != nil {
		observeRecordLocked(r)
	}
	close(r.change)
	r.change = make(chan struct{})
}
func managerChanged(st *managerState) {
	// Observe actual open/retirement decisions while their owner state is stable.
	// origin: hot-update S5 plan §3.2 / §9.1 / DRILL-OBSERVER
	if drillobserve.Hook != nil {
		for id, r := range st.retirement {
			observeRetirement(id, r)
		}
	}
	close(st.change)
	st.change = make(chan struct{})
}
func lookup(m *Manager, id ID) (*record, error) {
	if m == nil || m.state == nil {
		return nil, ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return nil, ErrClosed
	}
	if st.frozen {
		return nil, ErrFrozen
	}
	r := st.records[id]
	if r == nil {
		return nil, failure(ReasonUnknownFlow)
	}
	return r, nil
}

// RestoreDirection selects the enclosing image owner's wire direction before
// activation. S5 agent images use FlagDirection; S6 broker images use zero.
// The role is validated by the enclosing importer, never inferred from a frame.
// No worker may run or record change after this pristine-only boundary.
// origin: hot-update S6 plan §6.3 / T24
func RestoreDirection(m *Manager, direction uint16) error {
	if m == nil || m.state == nil || direction & ^FlagDirection != 0 {
		return failure(ReasonStateInvalid)
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || !st.frozen || !st.pristine {
		return ErrFrozen
	}
	for _, r := range st.records {
		r.mu.Lock()
		r.open.Direction = direction
		r.mu.Unlock()
	}
	return nil
}

func (m *Manager) ActivateRaw() error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return ErrClosed
	}
	// The first pump ends pristine rollback responsibility; the original sealed
	// metadata must be released before workers can advance the imported cut.
	// origin: hot-update S5 internal review rv-image-1
	releaseImportMetadataLocked(st)
	st.pristine = false
	st.frozen = false
	for _, r := range st.records {
		r.mu.Lock()
		if !r.closed {
			r.desired = true
			startPumpsLocked(r)
		}
		r.mu.Unlock()
	}
	managerChanged(st)
	return nil
}
func (m *Manager) RevokeFlow(id ID, reason Reason) error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	if reason < ReasonMalformed || reason > ReasonStateInvalid {
		return failure(ReasonMalformed)
	}
	st := m.state
	st.mu.Lock()
	r := st.records[id]
	// Revoke changes the original image before shutting down every OFD copy.
	// It cannot leave that image eligible for a pristine redispatch.
	// origin: hot-update S5 internal review rv-image-1
	if r != nil {
		st.pristine = false
		releaseImportMetadataLocked(st)
	}
	st.mu.Unlock()
	if r == nil {
		return failure(ReasonUnknownFlow)
	}
	r.mu.Lock()
	r.revoked = true
	r.cleanupPending = true
	r.reason = reason
	r.openStatus = 3
	r.openReason = reason
	r.closeMode = CloseModeRevoke
	raw := r.raw
	r.mu.Unlock()
	// SHUT_RDWR reaches Prepared and inherited duplicates too. Close alone does not.
	if raw != nil {
		_ = raw.Shutdown()
	}
	closeRecord(r)
	st.mu.Lock()
	rt := st.retirement[r.open.EndpointID]
	err := retire(rt, r.open.OpenSeq)
	managerChanged(st)
	st.mu.Unlock()
	if st.cfg.OnTerminal != nil {
		st.cfg.OnTerminal(id, reason)
	}
	return err
}
func closeRecord(r *record) { closeRecordJoining(r, true) }

// Pump retirement runs after its syscalls have joined but before its final done
// receipt. It must not join itself; external close still joins that callback.
// origin: hot-update S6 plan §6.2 / T23 / T56
func closeRecordJoining(r *record, join bool) bool {
	r.mu.Lock()
	r.desired = false
	stopPumpsLocked(r)
	done := r.done
	r.mu.Unlock()
	if join && done != nil {
		<-done
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	r.closed = true
	r.phase = FlowPhaseTerminal
	if r.replay != nil {
		tx := replayStateLocked(r)
		r.finalEnd = [2]uint64{tx.End, r.rxReceived}
		if r.reason != 0 && r.rxFINSet {
			r.finalEnd[1] = r.rxFIN
		}
		r.finalApplied = [2]uint64{tx.Base, r.rxApplied}
	}
	raw, replay := r.raw, r.replay
	r.cleanupPending = false
	// Workers have already joined, so these descriptor closes cannot join IO.
	// Release replay capacity before publishing Terminal to Controls/Observe;
	// a final receipt must prove that this owner has no remaining reservation.
	// origin: hot-update S5 internal review SS-REVOKE-1 / plan §6.5
	if raw != nil {
		_ = raw.Close()
	}
	if replay != nil {
		_ = replay.Close()
	}
	changed(r)
	r.mu.Unlock()
	return true
}

func (m *Manager) Close() error {
	if m == nil || m.state == nil {
		return ErrClosed
	}
	st := m.state
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return nil
	}
	st.closed = true
	releaseImportMetadataLocked(st)
	st.pristine = false
	records := make([]*record, 0, len(st.records))
	for _, r := range st.records {
		records = append(records, r)
	}
	managerChanged(st)
	st.mu.Unlock()
	for _, r := range records {
		closeRecord(r)
	}
	return nil
}

// State is a coherent carrier receipt. Payload never appears in this view.
type State struct {
	ID                                                 ID
	EndpointID                                         EndpointID
	OpenSeq                                            uint64
	Intent                                             [32]byte
	TargetPort                                         uint16
	Phase                                              Phase
	OpenStatus                                         uint8
	OpenReason                                         Reason
	TxBase, TxEnd, RxApplied, RxReceived, TxFIN, RxFIN uint64
	FINFlags                                           uint8
	FinalEnd, FinalApplied                             [2]uint64
	Reason                                             Reason
}

func replayStateLocked(r *record) workload.ReplayState {
	if r.replay == nil {
		return r.terminalTx
	}
	return r.replay.Snapshot()
}
func stateLocked(r *record) State {
	tx := replayStateLocked(r)
	flags := uint8(0)
	if r.txFINSet {
		flags |= 1
	}
	if r.rxFINSet {
		flags |= 2
	}
	if r.rxCloseWrite {
		flags |= 4
	}
	if r.txFINReceipt {
		flags |= 8
	}
	return State{ID: r.open.ID, EndpointID: r.open.EndpointID, OpenSeq: r.open.OpenSeq, Intent: r.open.Intent, TargetPort: r.open.TargetPort, Phase: r.phase, OpenStatus: r.openStatus, OpenReason: r.openReason, TxBase: tx.Base, TxEnd: tx.End, RxApplied: r.rxApplied, RxReceived: r.rxReceived, TxFIN: r.txFIN, RxFIN: r.rxFIN, FINFlags: flags, FinalEnd: r.finalEnd, FinalApplied: r.finalApplied, Reason: r.reason}
}
func States(m *Manager, endpoint EndpointID) []State {
	if m == nil || m.state == nil {
		return nil
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]State, 0, len(st.records))
	for _, r := range st.records {
		if r.open.EndpointID == endpoint {
			r.mu.Lock()
			// Settled receipts own no socket/replay and cannot consume a live
			// wire entry. Keep their final outcome in the bounded journal for
			// lost-receipt reconciliation and image export.
			// origin: hot-update S5 internal review rv-flow-1
			if !r.closed {
				out = append(out, stateLocked(r))
			}
			r.mu.Unlock()
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenSeq < out[j].OpenSeq })
	return out
}
func Observe(m *Manager, id ID) (State, <-chan struct{}, error) {
	r, err := lookup(m, id)
	if err != nil {
		return State{}, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return stateLocked(r), r.change, nil
}

// Restore is split from the public codec adapter. Every taken resource is
// returned on failure; constructing a wrapper never starts its pumps.
func restoreCutState(cfg Config, state sealedstate.CutState) (*Manager, []handoff.Resource, error) {
	return restoreFlowRecords(cfg, state)
}

// RestoreSnapshot is the cross-package private-record adapter; all resources
// remain frozen and are returned intact on refusal.
func RestoreSnapshot(cfg Config, state sealedstate.CutState) (*Manager, []handoff.Resource, error) {
	return restoreCutState(cfg, state)
}

func settleRecordJoining(m *Manager, r *record, join bool) {
	r.mu.Lock()
	normal := r.txFINSet && r.txFINReceipt && r.rxCloseWrite && r.reason == 0
	abort := r.reason != 0 && r.closeReceipt && !r.running && len(r.txPending) == 0 && (r.rxCloseWrite || r.writerError != "")
	ready := (normal || abort) && !r.closed
	closed := r.closed
	reason := r.reason
	r.mu.Unlock()
	if !closed && (!ready || !closeRecordJoining(r, join)) {
		return
	}
	st := m.state
	st.mu.Lock()
	rt := st.retirement[r.open.EndpointID]
	// Failed connectors close themselves after joining; they still retire the
	// admitted sequence, exactly once, just like a FIN/abort settlement.
	// origin: hot-update S5 internal review rv-flow-1 / rv-flow-5
	if rt == nil || rt.live[r.open.OpenSeq] != r.open.ID {
		st.mu.Unlock()
		return
	}
	if err := retire(rt, r.open.OpenSeq); err != nil {
		st.mu.Unlock()
		return
	}
	managerChanged(st)
	st.mu.Unlock()
	if st.cfg.OnTerminal != nil {
		st.cfg.OnTerminal(r.open.ID, reason)
	}
}
