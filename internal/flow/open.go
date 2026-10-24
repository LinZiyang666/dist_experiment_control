package flow

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/workload"
)

// OpenRequest is private in-memory state, never a public image or NATS message.
// Deadline belongs to the first journal admission and is not renewed on retry.
type OpenRequest struct {
	ID                   ID
	EndpointID           EndpointID
	OpenSeq              uint64
	Intent               [32]byte
	BindNonce            [16]byte
	TargetPort           uint16
	OwnerGen, CarrierGen uint64
	Direction            uint16
	Deadline             time.Time
}

func OpenIntent(endpoint EndpointID, id ID, seq uint64, port uint16) [32]byte {
	b := append([]byte("tether-flow-open-v1\n"), endpoint[:]...)
	b = append(b, id[:]...)
	b = binary.BigEndian.AppendUint64(b, seq)
	b = binary.BigEndian.AppendUint16(b, port)
	return sha256.Sum256(b)
}

// Open journals identity and reserves capacity before the connector can run.
// A repeated OPEN returns the original pending/success/failure receipt.
// origin: hot-update S5 plan §3.2 / T04
func Open(m *Manager, req OpenRequest, raw *rawsocket.Socket) (State, error) {
	if m == nil || m.state == nil {
		return State{}, ErrClosed
	}
	if req.ID == (ID{}) || req.EndpointID == (EndpointID{}) || req.OpenSeq == 0 || req.OpenSeq == math.MaxUint64 || req.TargetPort == 0 || req.OwnerGen == 0 || req.Direction & ^FlagDirection != 0 || req.Intent != OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort) {
		return State{}, failure(ReasonMalformed)
	}
	st := m.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return State{}, ErrClosed
	}
	if st.frozen {
		return State{}, ErrFrozen
	}
	rt := st.retirement[req.EndpointID]
	if rt == nil {
		rt = &retirement{next: 1, holes: make(map[uint64]bool), live: make(map[uint64]ID)}
		st.retirement[req.EndpointID] = rt
	}
	if retired(rt, req.OpenSeq) {
		return State{}, failure(ReasonRetired)
	}
	if existing := st.records[req.ID]; existing != nil {
		if existing.open.EndpointID != req.EndpointID || existing.open.OpenSeq != req.OpenSeq || existing.open.Intent != req.Intent || existing.open.TargetPort != req.TargetPort {
			return State{}, failure(ReasonIntentConflict)
		}
		existing.mu.Lock()
		defer existing.mu.Unlock()
		return stateLocked(existing), nil
	}
	if prior, ok := rt.live[req.OpenSeq]; ok && prior != req.ID {
		return State{}, failure(ReasonIntentConflict)
	}
	active := 0
	for _, r := range st.records {
		r.mu.Lock()
		if !r.closed {
			active++
		}
		r.mu.Unlock()
	}
	// The terminal bound is checked against ALL records on purpose: every
	// admitted flow eventually settles into a terminal receipt that stays until
	// CollectReceipt/ForgetEndpoint, so len(records) is the terminal count once
	// the currently active flows settle. Checking only today's terminal count
	// would let a cut taken after those settles exceed MaxTerminalMetadata,
	// which the image validators refuse.
	if active >= st.cfg.MaxFlows || len(st.records) >= MaxTerminalMetadata || len(rt.holes) >= MaxRetirementHoles {
		return State{}, failure(ReasonCapacity)
	}
	if raw == nil && st.cfg.Dial == nil {
		return State{}, failure(ReasonUnsupported)
	}
	replay, err := workload.NewReplay(st.cfg.Budget, workload.DefaultReplayCapacity)
	if err != nil {
		return State{}, err
	}
	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(2 * time.Second)
	}
	boot, err := deadlineBoot(req.Deadline)
	if err != nil {
		_ = replay.Close()
		return State{}, err
	}
	r := &record{connectDeadlineBootNS: boot, open: req, phase: FlowPhaseOpening, raw: raw, replay: replay, ownerGen: req.OwnerGen, carrierGen: req.CarrierGen, openStatus: 1, change: make(chan struct{}), desired: true, dial: st.cfg.Dial}
	r.settle = func(join bool) { settleRecordJoining(m, r, join) }
	st.records[req.ID] = r
	rt.live[req.OpenSeq] = req.ID
	if req.OpenSeq >= rt.next {
		rt.next = req.OpenSeq + 1
	}
	r.mu.Lock()
	if drillobserve.Hook != nil {
		observeRecordLocked(r)
	}
	startPumpsLocked(r)
	result := stateLocked(r)
	r.mu.Unlock()
	managerChanged(st)
	return result, nil
}

// Bind only names an existing journal entry. It cannot allocate a socket or
// infer a missing OPEN from a RESUME request, even when its offsets look valid.
func Bind(m *Manager, id ID, endpoint EndpointID, intent [32]byte, owner, carrier, applied uint64) error {
	r, err := lookup(m, id)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.open.EndpointID != endpoint || r.open.Intent != intent {
		return failure(ReasonIntentConflict)
	}
	if r.closed || r.revoked {
		return failure(ReasonRetired)
	}
	if owner < r.ownerGen || carrier < r.carrierGen || owner == 0 || carrier == 0 {
		return failure(ReasonOwnerConflict)
	}
	if err = ackLocked(r, applied, false); err != nil {
		return err
	}
	r.ownerGen = owner
	r.carrierGen = carrier
	r.sent = applied
	changed(r)
	return nil
}
