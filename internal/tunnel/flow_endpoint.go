package tunnel

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

var errFlowAuth = errors.New("tunnel: flow authorization refused")

type serverFlowState struct {
	resumeBytes  atomic.Int64
	controlBytes atomic.Int64
	cfg          FlowConfig
	endpoints    map[int]*flowEndpoint
	manager      *flow.Manager
	ticketKey    [32]byte
	pending      chan struct{}
	waiters      []*flowEndpoint
}
type flowEndpoint struct {
	id                   flow.EndpointID
	allocation           Allocation
	fence                fenceSnap
	phase                EndpointPhase
	listener             net.Listener
	ownerGen, carrierGen uint64
	targetPort           uint16
	ticket               []byte
	attachment           *flowCarrier
	detachDeadline       time.Time
	timer                *time.Timer
	permit               *flowPermit
	receipt              *resumeReceipt
	stop                 chan struct{}
	done                 chan struct{}
	closeOnce            sync.Once
	pendingRaw           *rawsocket.Socket
	lineage              [32]byte
	createdAt            time.Time
	nextSeq              uint64
	// Pause is independent of phase: it never consumes pendingRaw or its slot.
	// origin: hot-update S6 plan §6.2 / T21
	paused         bool
	pause          chan struct{}
	keeper         listenerimage.Keeper
	timerWork      *endpointTimer
	fired          bool
	cleanupPending bool
	detachBootNS   uint64
}
type flowPermit struct {
	requestID, cutID [16]byte
	owner, next      uint64
	deadline         time.Time
	digest           [32]byte
	consumed         bool
	abort            bool
}
type resumeReceipt struct {
	request  resumeRequest
	response resumeResponse
}

func validateFlowConfig(cfg FlowConfig, server bool) error {
	if runtime.GOOS != "linux" {
		return rawsocket.ErrUnsupported
	}
	if cfg.Budget == nil || cfg.InstanceID == "" || cfg.BootID == "" || cfg.OwnerGen == 0 {
		return errFlowState
	}
	if cfg.MaxEndpoints != 0 && cfg.MaxEndpoints != MaxEndpoints || cfg.MaxPendingFDs != 0 && cfg.MaxPendingFDs != flow.MaxPendingFDs {
		return errFlowState
	}
	if server && (cfg.TokenLookup == nil || cfg.Authorize == nil) {
		return errFlowState
	}
	return nil
}
func (s *Server) SetFlowConfig(cfg FlowConfig) error {
	if err := validateFlowConfig(cfg, true); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil || s.closed || s.flowState != nil {
		return errFlowState
	}
	m, err := flow.New(flow.Config{Budget: cfg.Budget})
	if err != nil {
		return err
	}
	st := &serverFlowState{cfg: cfg, endpoints: make(map[int]*flowEndpoint), manager: m, pending: make(chan struct{}, flow.MaxPendingFDs)}
	if _, err = rand.Read(st.ticketKey[:]); err != nil {
		_ = m.Close()
		return err
	}
	s.flowState = st
	return nil
}
func allocationKey(a Allocation) sessionFenceKey {
	return sessionFenceKeyFor(a.Port, a.SID, a.NID, a.TokenHash)
}
func endpointInfo(e *flowEndpoint) SessionInfo {
	a := e.allocation
	return SessionInfo{PublicPort: a.Port, SID: a.SID, NID: a.NID, TokenHash: a.TokenHash, Epoch: a.Epoch}
}
func endpointLiveLocked(s *Server, e *flowEndpoint) bool {
	return !s.closed && s.flowState != nil && s.flowState.endpoints[e.allocation.Port] == e && e.phase != EndpointPhaseClosing && e.phase != EndpointPhaseClosed && !s.forgotten[e.allocation.SID] && !s.closedAllocation[allocationKey(e.allocation)] && !s.fenceChangedLocked(e.fence, e.allocation.Port, e.allocation.SID, allocationKey(e.allocation))
}
func endpointRefLocked(s *Server, sid string, key *sessionFenceKey) bool {
	if s.flowState == nil {
		return false
	}
	for _, e := range s.flowState.endpoints {
		if e.allocation.SID == sid && (key == nil || allocationKey(e.allocation) == *key) {
			return true
		}
	}
	return false
}

// reserveEndpoint precedes bind. Re-registering the same allocation borrows its
// existing listener; authentication alone never kicks out the old attachment.
// origin: hot-update S5 plan §4.2 / T12
func reserveEndpoint(s *Server, a Allocation, snap fenceSnap) (*flowEndpoint, error) {
	s.mu.Lock()
	if s.closed || s.imagePaused || s.flowState == nil || s.forgotten[a.SID] || s.closedAllocation[allocationKey(a)] || s.fenceChangedLocked(snap, a.Port, a.SID, allocationKey(a)) {
		s.mu.Unlock()
		return nil, errFlowAuth
	}
	if e := s.flowState.endpoints[a.Port]; e != nil {
		if e.allocation != a || e.phase == EndpointPhaseBinding || !endpointLiveLocked(s, e) {
			s.mu.Unlock()
			return nil, errFlowState
		}
		s.mu.Unlock()
		return e, nil
	}
	if len(s.flowState.endpoints) >= MaxEndpoints {
		s.mu.Unlock()
		return nil, errFlowState
	}
	e := &flowEndpoint{allocation: a, fence: snap, phase: EndpointPhaseBinding, stop: make(chan struct{}), done: make(chan struct{}), pause: make(chan struct{}), nextSeq: 1, createdAt: time.Now()}
	if _, err := rand.Read(e.id[:]); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.flowState.endpoints[a.Port] = e
	s.mu.Unlock()
	ln, err := net.Listen("tcp", net.JoinHostPort(s.publicHost, strconv.Itoa(a.Port)))
	s.mu.Lock()
	if err != nil || s.imagePaused || !endpointLiveLocked(s, e) {
		delete(s.flowState.endpoints, a.Port)
		s.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		close(e.done)
		if err == nil {
			err = errFlowAuth
		}
		return nil, err
	}
	e.listener = ln
	e.phase = EndpointPhaseDetached
	// A REGISTER which never finishes RESUME also has a bounded owner lifetime.
	armDetachLocked(s, e, time.Now())
	s.mu.Unlock()
	go endpointAccept(s, e)
	return e, nil
}
func armDetachLocked(s *Server, e *flowEndpoint, now time.Time) {
	if e.detachDeadline.IsZero() {
		e.detachDeadline = now.Add(DetachedGrace)
	}
	if e.detachBootNS == 0 {
		e.detachBootNS, _ = clientDeadlineBoot(e.detachDeadline)
	}
	if e.paused || e.timer != nil {
		return
	}
	// Stop=false leaves this completion record live even after timer=nil.
	// The callback records its decision before cleanup; pause keeps that duty
	// exportable rather than letting terminal cleanup destroy a pending socket.
	// origin: hot-update S6 plan §6.2 / T22
	work := &endpointTimer{done: make(chan struct{})}
	e.timerWork = work
	if s.callbacks == nil {
		s.callbacks = make(map[*endpointTimer]struct{})
	}
	s.callbacks[work] = struct{}{}
	e.timer = time.AfterFunc(max(time.Until(e.detachDeadline), 0), func() {
		defer finishEndpointTimer(s, work)
		s.mu.Lock()
		e.fired = true
		expired := endpointLiveLocked(s, e) && e.attachment == nil && !time.Now().Before(e.detachDeadline)
		if expired {
			e.cleanupPending = true
		}
		paused := e.paused
		s.mu.Unlock()
		if expired && !paused {
			closeEndpoint(s, e)
		}
	})
}
func detachEndpoint(s *Server, e *flowEndpoint, carrier *flowCarrier) {
	s.mu.Lock()
	if e.attachment == carrier && endpointLiveLocked(s, e) {
		e.attachment = nil
		e.phase = EndpointPhaseDetached
		if e.permit != nil && !e.permit.consumed {
			e.phase = EndpointPhasePrepared
		}
		armDetachLocked(s, e, time.Now())
	}
	s.mu.Unlock()
}
func closeEndpoint(s *Server, e *flowEndpoint) {
	e.closeOnce.Do(func() {
		s.mu.Lock()
		e.phase = EndpointPhaseClosing
		close(e.stop)
		ln, a, done := e.listener, e.attachment, e.done
		e.attachment = nil
		stopEndpointTimerLocked(s, e)
		s.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		if a != nil {
			a.Close()
			a.Join()
		}
		<-done
		// A paused accept owner left this slot deliberately intact. Terminal close
		// alone consumes it; ordinary pause must never run this branch.
		s.mu.Lock()
		pending := e.pendingRaw
		if pending != nil {
			e.pendingRaw = nil
			removePendingEndpoint(s.flowState, e)
			<-s.flowState.pending
		}
		keeper := e.keeper
		retiring := s.retiringImage
		s.mu.Unlock()
		if pending != nil {
			// Hard revoke reaches every same-OFD prepared/salvage duplicate. Retiring
			// old wrappers after ownership transfer must preserve the successor socket.
			// origin: hot-update S6 plan §6.2 / T21
			if !retiring {
				_ = pending.Shutdown()
			}
			_ = pending.Close()
		}
		if keeper != nil {
			_ = keeper.Close()
		}
		for _, r := range flow.States(s.flowState.manager, e.id) {
			_ = s.flowState.manager.RevokeFlow(r.ID, flow.ReasonRevoked)
			_ = flow.CollectReceipt(s.flowState.manager, r.ID)
		}
		_ = flow.ForgetEndpoint(s.flowState.manager, e.id)
		s.mu.Lock()
		e.phase = EndpointPhaseClosed
		delete(s.flowState.endpoints, e.allocation.Port)
		s.maybePruneSessionLocked(e.allocation.SID)
		s.maybePruneAllocationLocked(allocationKey(e.allocation))
		s.mu.Unlock()
	})
}
func closeMatchingEndpoints(s *Server, match func(*flowEndpoint) bool) []int {
	s.mu.Lock()
	var owners []*flowEndpoint
	if s.flowState != nil {
		for _, e := range s.flowState.endpoints {
			if match(e) {
				owners = append(owners, e)
			}
		}
	}
	s.mu.Unlock()
	ports := make([]int, 0, len(owners))
	for _, e := range owners {
		ports = append(ports, e.allocation.Port)
		closeEndpoint(s, e)
	}
	return ports
}

// Only the original accept owner touches this listener. Pending FDs reserve a
// node-wide slot before Accept; a full replay budget parks at most one FD per
// endpoint here, and never creates unbounded reserve goroutines.
func endpointAccept(s *Server, e *flowEndpoint) {
	s.mu.Lock()
	done, pause, listener := e.done, e.pause, e.listener
	s.mu.Unlock()
	defer close(done)
	st := s.flowState
	for {
		s.mu.Lock()
		paused, raw := e.paused, e.pendingRaw
		s.mu.Unlock()
		if paused {
			return
		}
		if raw == nil {
			select {
			case st.pending <- struct{}{}:
			case <-pause:
				return
			case <-e.stop:
				return
			}
			// An idle listener must yield its slot to other listeners. The same
			// bounded FIFO channel admits both idle polls and accepted FD owners.
			// origin: hot-update S5 plan §3.4 / T51
			// Polling yields idle reservations; the resumed native view is explicitly
			// owned by this endpoint and closed before the keeper is exported.
			if tcp, ok := listener.(*net.TCPListener); ok {
				_ = tcp.SetDeadline(time.Now().Add(20 * time.Millisecond))
			}
			conn, err := listener.Accept()
			if err != nil {
				<-st.pending
				s.mu.Lock()
				paused = e.paused
				s.mu.Unlock()
				if paused {
					return
				}
				var timeout net.Error
				if errors.As(err, &timeout) && timeout.Timeout() {
					continue
				}
				return
			}
			tcp, ok := conn.(*net.TCPConn)
			if !ok {
				_ = conn.Close()
				<-st.pending
				continue
			}
			raw, err = rawsocket.TakeTCP(tcp)
			if err != nil {
				_ = conn.Close()
				<-st.pending
				continue
			}
			s.mu.Lock()
			e.pendingRaw = raw
			st.waiters = append(st.waiters, e)
			s.mu.Unlock()
		}
		admitted, capacityObserved := false, false
		for !admitted {
			s.mu.Lock()
			paused = e.paused
			live := endpointLiveLocked(s, e)
			target, gen, owner, seq := e.targetPort, e.carrierGen, e.ownerGen, e.nextSeq
			turn := len(st.waiters) > 0 && st.waiters[0] == e
			if !paused && turn && (target == 0 || owner == 0) {
				st.waiters = append(st.waiters[1:], e)
			}
			s.mu.Unlock()
			// A deadline wake must not retry Accept or run terminal cleanup. The raw
			// owner, reserved slot and FIFO position belong to the retained endpoint.
			// origin: hot-update S6 plan §6.2 / T21
			if paused {
				return
			}
			if !live || seq == math.MaxUint64 {
				releaseEndpointPending(s, e, raw)
				return
			}
			if turn && target != 0 && owner != 0 {
				var id flow.ID
				var nonce [16]byte
				if _, err := rand.Read(id[:]); err != nil {
					releaseEndpointPending(s, e, raw)
					return
				}
				if _, err := rand.Read(nonce[:]); err != nil {
					releaseEndpointPending(s, e, raw)
					return
				}
				req := flow.OpenRequest{ID: id, EndpointID: e.id, OpenSeq: seq, Intent: flow.OpenIntent(e.id, id, seq, target), BindNonce: nonce, TargetPort: target, OwnerGen: owner, CarrierGen: gen, Deadline: time.Now().Add(ResumeAttempt)}
				// Record the already accepted original socket before admission.
				// The first native refusal belongs to this pending original FD.
				// Observe backpressure without closing it or changing FIFO admission.
				// origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
				_, _, tuple, _, _ := rawsocket.ConnectReceipt(raw)
				observeFlowSocket(id, e.id, tuple)
				_, err := flow.Open(st.manager, req, raw)
				if !capacityObserved && drillobserve.Hook != nil && (errors.Is(err, &flow.Failure{Reason: flow.ReasonCapacity}) || errors.Is(err, workload.ErrBackpressure)) {
					fp := raw.Fingerprint()
					limit := st.cfg.Budget.Limit
					if limit == 0 {
						limit = workload.DefaultReplayBudget
					}
					drillobserve.Hook(drillobserve.Event{Kind: "flow-capacity", Domain: "flow", Endpoint: [16]byte(e.id), Flow: [16]byte(id), Values: [16]uint64{uint64(flow.ReasonCapacity), uint64(st.cfg.Budget.Used()), uint64(limit), fp.Dev, fp.Ino}})
					capacityObserved = true
				}
				if err == nil {
					s.mu.Lock()
					e.nextSeq++
					a := e.attachment
					s.mu.Unlock()
					admitted = true
					if a != nil {
						a.BindServerFlow(req)
					}
				}
			}
			if !admitted {
				timer := time.NewTimer(20 * time.Millisecond)
				select {
				case <-pause:
					timer.Stop()
					return
				case <-e.stop:
					timer.Stop()
					releaseEndpointPending(s, e, raw)
					return
				case <-timer.C:
				}
			}
		}
		s.mu.Lock()
		e.pendingRaw = nil
		removePendingEndpoint(st, e)
		s.mu.Unlock()
		<-st.pending
	}
}

func releaseEndpointPending(s *Server, e *flowEndpoint, raw *rawsocket.Socket) {
	_ = raw.Close()
	s.mu.Lock()
	e.pendingRaw = nil
	removePendingEndpoint(s.flowState, e)
	s.mu.Unlock()
	<-s.flowState.pending
}

func removePendingEndpoint(st *serverFlowState, e *flowEndpoint) {
	for i, pending := range st.waiters {
		if pending == e {
			st.waiters = slices.Delete(st.waiters, i, i+1)
			return
		}
	}
}

func ticketFor(s *Server, e *flowEndpoint, owner uint64) []byte {
	// The allocation digest, issuer boot and endpoint incarnation prevent a
	// recycled public port or another broker boot from accepting this proof.
	h := hmac.New(sha256.New, s.flowState.ticketKey[:])
	a := e.allocation
	for _, v := range []string{a.SID, a.NID, a.TokenHash, s.flowState.cfg.BootID, s.flowState.cfg.InstanceID} {
		_ = binary.Write(h, binary.BigEndian, uint32(len(v)))
		_, _ = h.Write([]byte(v))
	}
	_, _ = h.Write(e.id[:])
	_, _ = h.Write(e.lineage[:])
	_ = binary.Write(h, binary.BigEndian, uint64(a.Port))
	_ = binary.Write(h, binary.BigEndian, a.Epoch)
	_ = binary.Write(h, binary.BigEndian, owner)
	return h.Sum(nil)
}

// authorizeEndpoint always calls both bounded operations, even for a cached
// receipt. Inflight ownership pins fences across the unlocked database read.
// origin: hot-update S5 plan §4.3 / T13
func authorizeEndpoint(s *Server, e *flowEndpoint, deadline time.Time, stop <-chan struct{}) (Authority, error) {
	s.mu.Lock()
	if !endpointLiveLocked(s, e) {
		s.mu.Unlock()
		return Authority{}, errFlowAuth
	}
	a := e.allocation
	snap := s.fenceSnapLocked(a.Port, a.SID, allocationKey(a))
	s.inflightBySID[a.SID]++
	s.inflightByAllocation[allocationKey(a)]++
	cfg := s.flowState.cfg
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.releaseInflightLocked(a.SID, allocationKey(a)); s.mu.Unlock() }()
	limit := time.Now().Add(LookupBudget)
	if deadline.IsZero() || limit.Before(deadline) {
		deadline = limit
	}
	req := AuthRequest{Allocation: a, Deadline: deadline, Stop: stop}
	if err := cfg.TokenLookup(req); err != nil {
		return Authority{}, errFlowAuth
	}
	grant, err := cfg.Authorize(req)
	if err != nil {
		return Authority{}, errFlowAuth
	}
	select {
	case <-stop:
		return Authority{}, errFlowAuth
	default:
	}
	if !time.Now().Before(deadline) || !grant.Enabled || grant.Allocation != a || grant.Generation < 0 || grant.ProxyEpoch < 0 {
		return Authority{}, errFlowAuth
	}
	s.mu.Lock()
	valid := endpointLiveLocked(s, e) && !s.fenceChangedLocked(snap, a.Port, a.SID, allocationKey(a))
	s.mu.Unlock()
	if !valid {
		return Authority{}, errFlowAuth
	}
	return grant, nil
}

// resumeEndpoint is the single ownership CAS. PREPARE keeps the old owner
// active; ACTIVATE consumes exactly one permit. Replays reauthorize above the
// receipt check, so a cached success cannot revive a revoked allocation.
// origin: hot-update S5 plan §4.4 / T13
func resumeEndpoint(s *Server, e *flowEndpoint, req resumeRequest, a *flowCarrier, deadline time.Time, stop <-chan struct{}) (resumeResponse, *flowCarrier, error) {
	grant, err := authorizeEndpoint(s, e, deadline, stop)
	if err != nil {
		return resumeResponse{}, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.imagePaused || e.paused || !endpointLiveLocked(s, e) || req.Scope != flow.ResumeScopeEndpoint || req.Owner == 0 {
		return resumeResponse{}, nil, errFlowAuth
	}
	if !e.detachDeadline.IsZero() && e.attachment == nil && !time.Now().Before(e.detachDeadline) {
		return resumeResponse{}, nil, errFlowAuth
	}
	if e.receipt != nil && e.receipt.request.RequestID == req.RequestID {
		if !bytes.Equal(encodeResumeRequest(e.receipt.request), encodeResumeRequest(req)) {
			return resumeResponse{}, nil, errFlowState
		}
		resp := e.receipt.response
		resp.Authority = grant
		resp.GrantDigest = responseDigest(resp)
		var old *flowCarrier
		if a != nil {
			old = e.attachment
			e.attachment = a
			a.Owner = e.ownerGen
			a.Gen = e.carrierGen
			e.phase = EndpointPhaseAttached
			stopEndpointTimerLocked(s, e)
			e.detachDeadline = time.Time{}
			e.detachBootNS = 0
		}
		return resp, old, nil
	}
	// A fresh broker has no retained owner to resume. Report that fact only
	// after current authority and fences passed; it grants no attachment and
	// never turns a stale ticket into ownership of another live endpoint.
	// origin: hot-update S5 plan §0 / §4.2 / P13-REGRESS
	if req.Mode == flow.ResumeModeReconnect && e.ownerGen == 0 && req.Endpoint != (flow.EndpointID{}) {
		return resumeResponse{}, nil, &flow.Failure{Reason: flow.ReasonUnknownFlow}
	}
	if req.Mode != flow.ResumeModeInitial && (!hmac.Equal(req.Ticket, ticketFor(s, e, req.Owner)) || req.Endpoint != e.id || req.Target != e.targetPort) {
		return resumeResponse{}, nil, errFlowAuth
	}
	// Refuse before consuming a permit or changing its owner.
	// origin: hot-update S5 plan §4.4 / T13
	if a != nil && e.carrierGen == math.MaxUint64 {
		return resumeResponse{}, nil, errFlowState
	}
	switch req.Mode {
	case flow.ResumeModeInitial:
		if e.ownerGen != 0 || req.Target == 0 || len(req.Ticket) != 0 || req.Endpoint != (flow.EndpointID{}) {
			return resumeResponse{}, nil, errFlowState
		}
		e.ownerGen = req.Owner
		e.lineage = req.Digest
		e.targetPort = req.Target
	case flow.ResumeModeReconnect:
		if req.Owner != e.ownerGen {
			return resumeResponse{}, nil, errFlowAuth
		}
	case flow.ResumeModePrepare:
		if req.Owner != e.ownerGen || req.Next != req.Owner+1 || req.Next == 0 || req.Cut == ([16]byte{}) || e.attachment == nil {
			return resumeResponse{}, nil, errFlowState
		}
		// A consumed predecessor is history, not the current owner's pending
		// permit. Only its installed successor may replace it with a new cut.
		// origin: hot-update S5 internal review rv-flow-3
		if e.permit != nil && e.permit.consumed && e.permit.next == req.Owner {
			e.permit = nil
		}
		if e.permit != nil {
			if e.permit.cutID != req.Cut || e.permit.next != req.Next || e.permit.consumed || !time.Now().Before(e.permit.deadline) {
				return resumeResponse{}, nil, errFlowState
			}
		} else {
			e.permit = &flowPermit{requestID: req.RequestID, cutID: req.Cut, owner: req.Owner, next: req.Next, deadline: time.Now().Add(PermitTTL)}
		}
		e.phase = EndpointPhasePrepared
	case flow.ResumeModeActivate:
		p := e.permit
		if p == nil || p.consumed || req.Owner != p.owner || req.Next != p.next || req.Cut != p.cutID || req.Digest == ([32]byte{}) || !time.Now().Before(p.deadline) {
			return resumeResponse{}, nil, errFlowAuth
		}
		p.consumed = true
		p.digest = req.Digest
		e.ownerGen = p.next
	case flow.ResumeModeAbort:
		if req.Owner != e.ownerGen || e.permit != nil && e.permit.consumed {
			return resumeResponse{}, nil, errFlowAuth
		}
		e.permit = nil
	// Bind resumes a data stream; it cannot change an Endpoint attachment.
	// origin: hot-update S5 plan §2.2 / §10.2
	case flow.ResumeModeBind:
		return resumeResponse{}, nil, errFlowState
	default:
		return resumeResponse{}, nil, errFlowState
	}
	var old *flowCarrier
	if a != nil {
		e.carrierGen++
		old = e.attachment
		e.attachment = a
		a.Gen = e.carrierGen
		a.Owner = e.ownerGen
		e.phase = EndpointPhaseAttached
		stopEndpointTimerLocked(s, e)
		e.detachDeadline = time.Time{}
		e.detachBootNS = 0
	}
	e.ticket = ticketFor(s, e, e.ownerGen)
	resp := resumeResponse{RequestID: req.RequestID, Endpoint: e.id, Owner: e.ownerGen, Carrier: e.carrierGen, Digest: req.Digest, Verdict: flow.ResumeVerdictAccepted, Pages: 1, Ticket: append([]byte(nil), e.ticket...), Authority: grant, Retirement: flow.Retirement(s.flowState.manager, e.id), Entries: flow.States(s.flowState.manager, e.id)}
	resp.GrantDigest = responseDigest(resp)
	e.receipt = &resumeReceipt{request: req, response: resp}
	return resp, old, nil
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
var errFlowState = flowstate.ErrFlowState

func splitResumeResponse(full flowstate.ResumeResponse) ([]flowstate.ResumeResponse, error) {
	return flowstate.SplitResumeResponse(full)
}
func readResumePages(first flow.Frame, next func() (flow.Frame, error), budget *atomic.Int64) (flowstate.ResumeResponse, error) {
	return flowstate.ReadResumePages(first, next, budget)
}
func responseDigest(r flowstate.ResumeResponse) [32]byte { return flowstate.ResponseDigest(r) }
