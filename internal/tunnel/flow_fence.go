package tunnel

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/hashicorp/yamux"
)

func handleFlowRegister(s *Server, conn net.Conn, br *bufio.Reader, a Allocation, snap fenceSnap, stop <-chan struct{}) {
	failed := true
	defer func() {
		if failed {
			_ = conn.Close()
		}
	}()
	deadline := time.Now().Add(ResumeAttempt)
	req := AuthRequest{Allocation: a, Deadline: time.Now().Add(LookupBudget), Stop: stop}
	lookupErr := s.flowState.cfg.TokenLookup(req)
	lookupExpired := !time.Now().Before(req.Deadline)
	if lookupErr != nil || lookupExpired {
		// A transient store fault or unapplied home directive must retain the
		// client's retry path. Only known codes leave this boundary, never SQL
		// error text; an expired positive lookup is also a retryable refusal.
		// origin: hot-update S5 internal review rv-authority-3
		reason := "session_closed"
		if lookupExpired {
			reason = "try_again"
		} else if code := lookupErr.Error(); code == "try_again" || code == proto.ReasonHomeCatchingUp {
			reason = code
		}
		_, _ = conn.Write([]byte("DENY " + reason + "\n"))
		if reason == "session_closed" {
			// A terminal refusal revokes the exact retained allocation too.
			// Waiting for detached grace would leave its public socket usable;
			// mismatched credentials must never close a successor's endpoint.
			// origin: hot-update S5 internal review HOTUPDATE-T40
			s.mu.Lock()
			e := s.flowState.endpoints[a.Port]
			matched := e != nil && e.allocation == a
			s.mu.Unlock()
			if matched {
				closeEndpoint(s, e)
			}
		}
		return
	}
	e, err := reserveEndpoint(s, a, snap)
	if err != nil {
		// A failed public bind is not revocation. Preserve the legacy typed
		// refusal so initial expose can choose another port and an existing
		// supervisor can retry; all ownership/fence refusals remain terminal.
		// origin: hot-update S6 plan §6.3 / T28 / TESTFIX-2
		reason := "session_closed"
		var listenErr *net.OpError
		if errors.As(err, &listenErr) && listenErr.Op == "listen" {
			reason = "public_port_bind_failed"
		}
		_, _ = conn.Write([]byte("DENY " + reason + "\n"))
		return
	}
	if err = writeRegisterOK(conn); err != nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	mux, err := yamux.Server(retainRegisterReader(conn, br), flowMuxConfig(io.Discard))
	if err != nil {
		return
	}
	timer := time.AfterFunc(max(time.Until(deadline), 0), func() { _ = conn.Close() })
	stream, err := mux.AcceptStream()
	if err != nil {
		timer.Stop()
		_ = mux.Close()
		return
	}
	_ = stream.SetDeadline(deadline)
	first, err := flow.DecodeFrame(stream)
	if err != nil || first.Kind != flow.KindResume || first.FlowID != (flow.ID{}) || first.CarrierGen != 0 {
		timer.Stop()
		_ = mux.Close()
		return
	}
	request, err := decodeResumeRequest(first.Body)
	if err != nil {
		timer.Stop()
		_ = mux.Close()
		return
	}
	carrier := newFlowCarrier(conn, mux, stream, s.flowState.manager, e.id, 0, 0, 0)
	carrier.ResumeBytes = &s.flowState.resumeBytes
	carrier.ControlBudget = &s.flowState.controlBytes
	carrier.AuthorizeBind = func() error {
		_, err := authorizeEndpoint(s, e, time.Now().Add(LookupBudget), carrier.Stop)
		return err
	}
	response, old, err := resumeEndpoint(s, e, request, carrier, deadline, stop)
	if err != nil {
		// A typed missing-owner receipt lets ordinary supervisors restore
		// future requests after broker restart without claiming old flows survived.
		// origin: hot-update S5 plan §2.2 / §4.2 / P13-REGRESS
		var refusal *flow.Failure
		if errors.As(err, &refusal) {
			body := binary.BigEndian.AppendUint16(append([]byte(nil), request.RequestID[:]...), uint16(refusal.Reason))
			_ = carrier.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindReject, BodyLen: uint32(len(body)), Body: body})
		}
		timer.Stop()
		carrier.Close()
		return
	}
	if old != nil {
		old.Close()
		old.Join()
	}
	carrier.OnRequest = func(req resumeRequest) ([]resumeResponse, error) {
		r, _, e2 := resumeEndpoint(s, e, req, nil, time.Now().Add(ResumeAttempt), carrier.Stop)
		if e2 != nil {
			return nil, e2
		}
		return splitResumeResponse(r)
	}
	pages, pageErr := splitResumeResponse(response)
	if pageErr != nil {
		timer.Stop()
		carrier.Close()
		detachEndpoint(s, e, carrier)
		return
	}
	for _, page := range pages {
		body := encodeResumeResponse(page)
		if err = carrier.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, CarrierGen: response.Carrier, BodyLen: uint32(len(body)), Body: body}); err != nil {
			timer.Stop()
			detachEndpoint(s, e, carrier)
			return
		}
	}
	timer.Stop()
	_ = stream.SetDeadline(time.Time{})
	carrier.Start()
	failed = false
	startServerCarrierMonitor(s, e, carrier)
	// OPEN absent at the peer is resent with the original identity. A known
	// journal entry binds to its original FD and peer-applied offset instead.
	peers := make(map[flow.ID]flow.State)
	for _, p := range request.Entries {
		peers[p.ID] = p
		// A normally terminal peer may have lost its reciprocal FIN receipt.
		// Replay retained control evidence on this generation without creating
		// another data stream or reserving another replay log.
		// origin: hot-update S5 internal review rv-flow-V1
		if local, _, err := flow.Observe(s.flowState.manager, p.ID); err == nil && local.Phase == flow.FlowPhaseTerminal {
			if local.OpenSeq != p.OpenSeq || flow.RebindReceipt(s.flowState.manager, p.ID, e.id, p.Intent, carrier.Owner, carrier.Gen) != nil {
				carrier.Close()
				return
			}
			carrier.WatchControls(p.ID)
		}
	}
	for _, state := range flow.States(s.flowState.manager, e.id) {
		if state.Phase == flow.FlowPhaseTerminal {
			continue
		}
		if peer, ok := peers[state.ID]; ok {
			if peer.OpenStatus == 3 {
				_ = s.flowState.manager.RevokeFlow(state.ID, flow.ReasonRevoked)
				continue
			}
			carrier.RebindServerFlow(state, peer)
		} else {
			carrier.BindServerFlow(flow.OpenRequest{ID: state.ID, EndpointID: e.id, OpenSeq: state.OpenSeq, Intent: state.Intent, TargetPort: state.TargetPort, OwnerGen: e.ownerGen, CarrierGen: e.carrierGen})
		}
	}
}

func (c *Client) FreezeFlows(deadline time.Time) (*ClientFrozen, error) {
	st := c.flowState
	if st == nil {
		return nil, errFlowState
	}
	if !flowLockBefore(&st.mu, deadline) {
		return nil, os.ErrDeadlineExceeded
	}
	if st.frozen || st.admission != 0 {
		st.mu.Unlock()
		return nil, errFlowState
	}
	st.frozen = true
	var carriers []*flowCarrier
	for _, e := range st.endpoints {
		if e.attachment != nil {
			if e.detachDeadline.IsZero() {
				e.detachDeadline = time.Now().Add(DetachedGrace)
			}
			carriers = append(carriers, e.attachment)
			e.attachment = nil
		}
	}
	st.mu.Unlock()
	for _, a := range carriers {
		a.Close()
	}
	for _, a := range carriers {
		a.Join()
	}
	frozen, err := st.manager.Freeze(deadline)
	if err != nil {
		st.mu.Lock()
		st.frozen = false
		for _, e := range st.endpoints {
			if e.permit != nil && !e.permit.consumed {
				e.permit.abort = true
			}
		}
		st.mu.Unlock()
		return nil, err
	}
	return &ClientFrozen{state: &clientFrozenState{client: c, flows: frozen}}, nil
}

func (f *ClientFrozen) Thaw() error {
	if f == nil || f.state == nil {
		return errFlowState
	}
	st := f.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.released {
		return errFlowState
	}
	err := st.flows.Thaw()
	if err != nil {
		return err
	}
	st.client.flowState.mu.Lock()
	st.client.flowState.frozen = false
	for _, e := range st.client.flowState.endpoints {
		if e.permit != nil && !e.permit.consumed {
			e.permit.abort = true
		}
		if e.request.Mode == flow.ResumeModeActivate {
			e.request = resumeRequest{}
		}
	}
	st.client.flowState.mu.Unlock()
	st.released = true
	_ = closeClientImageSnapshot(f)
	return nil
}
func (f *ClientFrozen) Discard() error {
	if f == nil || f.state == nil {
		return errFlowState
	}
	st := f.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.released {
		return errFlowState
	}
	st.released = true
	_ = closeClientImageSnapshot(f)
	return st.flows.Discard()
}
func endpointRecord(values map[string]sealedstate.Field) (sealedstate.Record, error) {
	return clientPrivateRecord(sealedstate.KindEndpoint, values)
}

func (f *ClientFrozen) SnapshotState() (sealedstate.CutState, error) {
	if f == nil || f.state == nil {
		return sealedstate.CutState{}, errFlowState
	}
	fs := f.state
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return snapshotClientCutLocked(fs)
}

// Export holds this same lock through codec, sealing and snapshot replacement.
func snapshotClientCutLocked(fs *clientFrozenState) (sealedstate.CutState, error) {
	if fs.released {
		return sealedstate.CutState{}, errFlowState
	}
	out, err := fs.flows.SnapshotState()
	if err != nil {
		return out, err
	}
	st := fs.client.flowState
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, e := range st.endpoints {
		// Route credentials and the retry request are descriptor-owned fields in
		// the aggregate sealed state; no opaque secondary schema escapes inventory.
		// origin: hot-update S5 plan §5.1 / §12.1 / T32
		if p := e.permit; p != nil && !p.consumed && !p.abort {
			e.request = resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeActivate, RequestID: p.requestID, Endpoint: e.id, Owner: p.owner, Next: p.next, Cut: p.cutID, Target: e.target, Ticket: append([]byte(nil), e.ticket...), Entries: flow.States(st.manager, e.id)}
		}
		var pending []byte
		if e.request.RequestID != ([16]byte{}) {
			pending = encodeResumeRequest(e.request)
		}
		until := uint64(0)
		if e.pins.ValidUntil != nil {
			until = uint64(e.pins.ValidUntil.UnixNano())
		}
		var ids, receiptIDs []byte
		// The image owns every retained receipt, while RESUME entries above
		// contain live flows only. Build the resource closure from the actual
		// frozen journal so settlement cannot silently omit terminal metadata.
		// origin: hot-update S5 internal review rv-flow-1
		for _, rec := range out.Records {
			if rec.Kind == sealedstate.KindFlow {
				fields := clientFields(rec)
				if bytes.Equal(fields["endpoint_id"].Bytes, e.id[:]) {
					ids = append(ids, fields["id"].Bytes...)
					if fields["phase"].U64 == uint64(flow.FlowPhaseTerminal) {
						receipt, err := clientTerminalReceipt(e, fields)
						if err != nil {
							return sealedstate.CutState{}, err
						}
						out.Records = append(out.Records, receipt)
						receiptIDs = append(receiptIDs, fields["id"].Bytes...)
					}
				}
			}
		}
		deadline, err := clientDeadlineBoot(e.detachDeadline)
		if err != nil {
			return sealedstate.CutState{}, err
		}
		zero := [16]byte{}
		permitID := zero
		if e.permit != nil {
			// A node cut shares CutID across endpoints. The record identity must
			// still be unique per endpoint, including consumed permit history.
			// origin: hot-update S5 plan §12.1 / T32
			permitID = [16]byte(e.id)
		}
		rec, err := endpointRecord(map[string]sealedstate.Field{
			"id": endpointBytes(e.id[:]), "allocation_sid": endpointText(e.allocation.SID), "allocation_nid": endpointText(e.allocation.NID), "allocation_port": endpointNum(uint64(e.allocation.Port)), "allocation_token_hash": endpointText(e.allocation.TokenHash), "allocation_epoch": endpointNum(uint64(e.allocation.Epoch)),
			"register_token": endpointText(e.token), "default_broker_addr": endpointText(fs.client.brokerAddr), "broker_addr": endpointText(e.brokerAddr), "pin_current": endpointText(e.pins.Current), "pin_previous": endpointText(e.pins.Previous), "pin_valid_until_unix_ns": endpointNum(until), "pending_resume": endpointBytes(pending),
			"boot_id": endpointText(st.cfg.BootID), "instance_id": endpointText(st.cfg.InstanceID), "owner_gen": endpointNum(e.owner), "carrier_gen": endpointNum(e.carrier), "target_port": endpointNum(uint64(e.target)), "phase": endpointNum(uint64(EndpointPhaseDetached)), "ticket": endpointBytes(e.ticket), "state_ref": endpointText(""), "listener_ref": endpointText(""), "flow_ids": endpointBytes(ids), "pending_raw_refs": endpointBytes(nil), "permit_id": endpointBytes(permitID[:]), "receipt_ids": endpointBytes(receiptIDs),
			"first_detach_deadline_boot_ns": endpointNum(deadline), "ready_latch_deadline_boot_ns": endpointNum(deadline), "ready_latch": endpointNum(1), "cleanup_pending": endpointNum(0), "closed_allocation": endpointNum(0), "fence_allocation_gen": endpointNum(0), "fence_port_gen": endpointNum(0), "fence_session_gen": endpointNum(0), "forgotten": endpointNum(0),
		})
		if err != nil {
			return sealedstate.CutState{}, err
		}
		out.Records = append(out.Records, rec)
		if p := e.permit; p != nil {
			boot, err := clientDeadlineBoot(p.deadline)
			if err != nil {
				return sealedstate.CutState{}, err
			}
			permit, err := clientPrivateRecord(sealedstate.KindPermit, map[string]sealedstate.Field{
				"id": endpointBytes(e.id[:]), "endpoint_id": endpointBytes(e.id[:]), "cut_id": endpointBytes(p.cutID[:]), "request_id": endpointBytes(p.requestID[:]),
				"owner_gen": endpointNum(p.owner), "next_owner_gen": endpointNum(p.next), "purpose": endpointNum(uint64(TicketPurposeHandoff)), "deadline_boot_ns": endpointNum(boot),
				"consumed": endpointNum(boolNumber(p.consumed)), "revoked": endpointNum(boolNumber(p.abort)), "ticket": endpointBytes(e.ticket), "image_digest": endpointBytes(p.digest[:]), "resources_digest": endpointBytes(make([]byte, 32)),
			})
			if err != nil {
				return sealedstate.CutState{}, err
			}
			out.Records = append(out.Records, permit)
		}
	}
	return out, nil
}

// restoreClientPermits binds each saved permit to exactly one retained endpoint.
// origin: hot-update S5 plan §4.4 / T32
func restoreClientPermits(st *clientFlowState, permits []sealedstate.Record, digest [32]byte) error {
	for _, rec := range permits {
		v := clientFields(rec)
		var id flow.EndpointID
		copy(id[:], v["endpoint_id"].Bytes)
		var e *clientEndpoint
		for _, endpoint := range st.endpoints {
			if endpoint.id == id {
				e = endpoint
				break
			}
		}
		// The descriptor's required permit fields are validated, not ignored.
		// origin: hot-update S5 internal review PRIVATE-STATE-1
		if e == nil || e.permit != nil || v["purpose"].U64 != uint64(TicketPurposeHandoff) ||
			!bytes.Equal(v["resources_digest"].Bytes, make([]byte, 32)) || !bytes.Equal(v["id"].Bytes, e.id[:]) ||
			!bytes.Equal(v["ticket"].Bytes, e.ticket) {
			return errFlowState
		}
		p := &flowPermit{owner: v["owner_gen"].U64, next: v["next_owner_gen"].U64, consumed: v["consumed"].U64 != 0, abort: v["revoked"].U64 != 0}
		copy(p.cutID[:], v["cut_id"].Bytes)
		copy(p.requestID[:], v["request_id"].Bytes)
		copy(p.digest[:], v["image_digest"].Bytes)
		deadline, err := clientBootDeadline(v["deadline_boot_ns"].U64)
		if err != nil {
			return err
		}
		p.deadline = deadline
		e.permit = p
		if !p.consumed && !p.abort {
			e.request = resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeActivate, RequestID: p.requestID, Endpoint: e.id, Owner: p.owner, Next: p.next, Cut: p.cutID, Digest: digest, Target: e.target, Ticket: e.ticket}
		}
	}
	return nil
}

func restoreClientRecords(cfg FlowConfig, cut sealedstate.CutState) (*Client, []handoff.Resource, error) {
	if _, err := clientImageFromCut(cut, ""); err != nil {
		return nil, cut.Resources, err
	}
	if err := validateFlowConfig(cfg, false); err != nil {
		return nil, cut.Resources, err
	}
	digest, digestErr := clientCutDigest(cut)
	if digestErr != nil {
		return nil, cut.Resources, digestErr
	}
	c := NewClient("", "", "", nil, nil)
	if err := c.SetFlowConfig(cfg); err != nil {
		return nil, cut.Resources, err
	}
	st := c.flowState
	st.frozen = true
	flowCut := sealedstate.CutState{}
	seenResources := make(map[string]bool)
	var permits, receipts []sealedstate.Record
	endpoints := make(map[flow.EndpointID]map[string]sealedstate.Field)
	for _, res := range cut.Resources {
		if seenResources[res.ID] {
			return nil, cut.Resources, errFlowState
		}
		seenResources[res.ID] = true
		if len(res.ID) >= 5 && res.ID[:5] == "flow." {
			flowCut.Resources = append(flowCut.Resources, res)
		}
	}
	for _, rec := range cut.Records {
		if rec.Kind == sealedstate.KindFlow || rec.Kind == sealedstate.KindRetirement {
			flowCut.Records = append(flowCut.Records, rec)
			continue
		}
		if rec.Kind == sealedstate.KindPermit {
			permits = append(permits, rec)
			continue
		}
		if rec.Kind == sealedstate.KindReceipt {
			receipts = append(receipts, rec)
			continue
		}
		if rec.Kind != sealedstate.KindEndpoint {
			return nil, cut.Resources, errFlowState
		}
		if err := restoreClientEndpoint(c, cfg, rec, endpoints); err != nil {
			return nil, cut.Resources, err
		}
	}
	if err := restoreClientPermits(st, permits, digest); err != nil {
		return nil, cut.Resources, err
	}
	if err := validateClientPrivateClosure(st, endpoints, flowCut.Records, receipts); err != nil {
		return nil, cut.Resources, err
	}
	for _, res := range cut.Resources {
		if !strings.HasPrefix(res.ID, "flow.") {
			return nil, cut.Resources, errFlowState
		}
	}
	manager, salvage, err := flow.RestoreSnapshot(flow.Config{Budget: cfg.Budget, Dial: st.managerDial()}, flowCut)
	if err != nil {
		_ = salvage
		return nil, cut.Resources, err
	}
	_ = st.manager.Close()
	st.manager = manager
	for _, e := range st.endpoints {
		if e.request.Mode == flow.ResumeModeActivate {
			e.request.Entries = flow.States(manager, e.id)
		}
	}
	c.localPortLookup = func(port int) (int, error) {
		st.mu.Lock()
		defer st.mu.Unlock()
		e := st.endpoints[port]
		if e == nil {
			return 0, errFlowState
		}
		return int(e.target), nil
	}
	return c, nil, nil
}

// Validate each client-owned endpoint before it can join the restored flow manager.
// Broker listener/fence obligations must never enter a client cut.
// origin: hot-update S5 plan §5.1 / §5.4 / T14.
func restoreClientEndpoint(c *Client, cfg FlowConfig, rec sealedstate.Record, endpoints map[flow.EndpointID]map[string]sealedstate.Field) error {
	st := c.flowState
	var err error
	v := make(map[string]sealedstate.Field)
	for _, f := range rec.Fields {
		if _, ok := v[f.Name]; ok {
			return errFlowState
		}
		v[f.Name] = f
	}
	checked, err := endpointRecord(v)
	if err != nil {
		return err
	}
	for _, f := range checked.Fields {
		if f.Type != v[f.Name].Type {
			return errFlowState
		}
	}
	e := &clientEndpoint{stop: make(chan struct{}), allocation: Allocation{SID: string(v["allocation_sid"].Bytes), NID: string(v["allocation_nid"].Bytes), Port: int(v["allocation_port"].U64), TokenHash: string(v["allocation_token_hash"].Bytes), Epoch: int64(v["allocation_epoch"].U64)}, owner: v["owner_gen"].U64, carrier: v["carrier_gen"].U64, target: uint16(v["target_port"].U64), ticket: append([]byte(nil), v["ticket"].Bytes...)}
	e.detachDeadline, err = clientBootDeadline(v["first_detach_deadline_boot_ns"].U64)
	if err != nil {
		return err
	}
	copy(e.id[:], v["id"].Bytes)
	// These are broker-owned fields. A client cut must explicitly prove it
	// owns no listener, pending accept or broker fence/cleanup obligation.
	// origin: hot-update S5 internal review PRIVATE-STATE-1
	if v["phase"].U64 != uint64(EndpointPhaseDetached) || v["ready_latch"].U64 != 1 ||
		v["ready_latch_deadline_boot_ns"].U64 != v["first_detach_deadline_boot_ns"].U64 ||
		v["cleanup_pending"].U64 != 0 || v["closed_allocation"].U64 != 0 || v["forgotten"].U64 != 0 ||
		v["fence_allocation_gen"].U64 != 0 || v["fence_port_gen"].U64 != 0 || v["fence_session_gen"].U64 != 0 ||
		len(v["listener_ref"].Bytes) != 0 || len(v["pending_raw_refs"].Bytes) != 0 || endpoints[e.id] != nil {
		return errFlowState
	}
	endpoints[e.id] = v
	if e.id == (flow.EndpointID{}) || e.owner == 0 || e.target == 0 || v["target_port"].U64 > 65535 || e.allocation.Port < 1 || e.allocation.Port > 65535 || e.allocation.Epoch < 0 || string(v["instance_id"].Bytes) != cfg.InstanceID || string(v["boot_id"].Bytes) != cfg.BootID || st.endpoints[e.allocation.Port] != nil {
		return errFlowState
	}
	if len(v["state_ref"].Bytes) != 0 || proto.ValidateSID(e.allocation.SID) != nil || proto.ValidateNID(e.allocation.NID) != nil {
		return errFlowState
	}
	addr := string(v["default_broker_addr"].Bytes)
	if c.sid != "" && (c.sid != e.allocation.SID || c.nidValue() != e.allocation.NID || c.brokerAddr != addr) {
		return errFlowState
	}
	c.sid, c.brokerAddr = e.allocation.SID, addr
	nid := e.allocation.NID
	c.nid.Store(&nid)
	e.token, e.brokerAddr = string(v["register_token"].Bytes), string(v["broker_addr"].Bytes)
	e.pins.Current, e.pins.Previous = string(v["pin_current"].Bytes), string(v["pin_previous"].Bytes)
	if until := int64(v["pin_valid_until_unix_ns"].U64); until != 0 {
		deadline := time.Unix(0, until)
		e.pins.ValidUntil = &deadline
	}
	if hashToken(e.token) != e.allocation.TokenHash {
		return errFlowState
	}
	if pending := v["pending_resume"].Bytes; len(pending) != 0 {
		e.request, err = decodeResumeRequest(pending)
		if err != nil || e.request.Endpoint != e.id || e.request.Owner != e.owner || e.request.Target != e.target || e.request.Scope != flow.ResumeScopeEndpoint {
			return errFlowState
		}
	}
	st.endpoints[e.allocation.Port] = e
	return nil
}

// A failed exec returns to the current owner. After fresh reconnect authority,
// release its unconsumed permit before a later cut can choose another CutID.
// origin: hot-update S5 plan §4.4 / T13
func abortClientPermit(st *clientFlowState, e *clientEndpoint, a *flowCarrier) error {
	st.mu.Lock()
	p := e.permit
	if p == nil || !p.abort {
		st.mu.Unlock()
		return nil
	}
	req := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeAbort, Endpoint: e.id, Owner: e.owner, Cut: p.cutID, Target: e.target, Ticket: append([]byte(nil), e.ticket...)}
	st.mu.Unlock()
	if _, err := rand.Read(req.RequestID[:]); err != nil {
		return err
	}
	if err := exchangeResume(a, req, time.Now().Add(ResumeAttempt)); err != nil {
		a.Close()
		return err
	}
	st.mu.Lock()
	if e.permit == p {
		e.permit = nil
	}
	st.mu.Unlock()
	return nil
}

// A terminal receipt is the logical owner's final decision, not a new owner.
// Its descriptor duplicates the retained flow journal deliberately: restore
// proves the two agree before that journal can drive ACK/CLOSE replay. Client
// cuts own no broker RESUME cache; its grant digest is therefore explicitly zero.
// origin: hot-update S5 internal review PRIVATE-STATE-1
func clientTerminalReceipt(e *clientEndpoint, v map[string]sealedstate.Field) (sealedstate.Record, error) {
	zero := [16]byte{}
	digest := [32]byte{}
	cut, request, image := zero, zero, digest
	next := uint64(0)
	if p := e.permit; p != nil {
		cut, request, image, next = p.cutID, p.requestID, p.digest, p.next
	}
	verdict := flow.ResumeVerdictAccepted
	reason := v["close_reason"].U64
	if reason == 0 {
		reason = v["open_reason"].U64
	}
	if v["revoked"].U64 != 0 || v["open_status"].U64 == 3 {
		verdict = flow.ResumeVerdictRejected
	}
	return clientPrivateRecord(sealedstate.KindReceipt, map[string]sealedstate.Field{
		"id": endpointBytes(v["id"].Bytes), "flow_id": endpointBytes(v["id"].Bytes),
		"endpoint_id": endpointBytes(e.id[:]), "intent": endpointBytes(v["intent"].Bytes),
		"owner_gen": endpointNum(v["owner_gen"].U64), "carrier_gen": endpointNum(v["carrier_gen"].U64),
		"cut_id": endpointBytes(cut[:]), "request_id": endpointBytes(request[:]),
		"image_digest": endpointBytes(image[:]), "grant_digest": endpointBytes(digest[:]),
		"next_owner_gen": endpointNum(next), "ticket": endpointBytes(e.ticket),
		"verdict": endpointNum(uint64(verdict)), "reason": endpointNum(reason),
		"close_mode": endpointNum(v["close_mode"].U64), "fin_flags": endpointNum(v["fin_flags"].U64),
		"final_applied": endpointBytes(v["final_applied"].Bytes), "final_end": endpointBytes(v["final_end"].Bytes),
		"open_status": endpointNum(v["open_status"].U64), "revoked": endpointNum(v["revoked"].U64),
	})
}

// Lists and duplicate receipt decisions are exact sets, not merely counts.
// Missing/extra IDs must fail before raw or replay ownership is transferred.
// origin: hot-update S5 internal review PRIVATE-STATE-1
func validateClientPrivateClosure(st *clientFlowState, endpoints map[flow.EndpointID]map[string]sealedstate.Field, records, receipts []sealedstate.Record) error {
	flows := make(map[flow.ID]map[string]sealedstate.Field)
	seenReceipts := make(map[flow.ID]bool)
	for _, rec := range records {
		if rec.Kind == sealedstate.KindFlow {
			v := clientFields(rec)
			id := flow.ID(v["id"].Bytes)
			if flows[id] != nil {
				return errFlowState
			}
			flows[id] = v
		}
	}
	for _, rec := range receipts {
		v := clientFields(rec)
		id := flow.ID(v["flow_id"].Bytes)
		journal := flows[id]
		var e *clientEndpoint
		for _, candidate := range st.endpoints {
			if bytes.Equal(v["endpoint_id"].Bytes, candidate.id[:]) {
				e = candidate
				break
			}
		}
		if e == nil || journal == nil || journal["phase"].U64 != uint64(flow.FlowPhaseTerminal) || seenReceipts[id] {
			return errFlowState
		}
		want, err := clientTerminalReceipt(e, journal)
		if err != nil || !reflect.DeepEqual(clientFields(want), v) {
			return errFlowState
		}
		seenReceipts[id] = true
	}
	listedFlows, listedReceipts := make(map[flow.ID]bool), make(map[flow.ID]bool)
	for _, e := range st.endpoints {
		v := endpoints[e.id]
		permitID := [16]byte{}
		if e.permit != nil {
			permitID = [16]byte(e.id)
		}
		if !bytes.Equal(v["permit_id"].Bytes, permitID[:]) {
			return errFlowState
		}
		for _, list := range []struct {
			name string
			seen map[flow.ID]bool
		}{{"flow_ids", listedFlows}, {"receipt_ids", listedReceipts}} {
			b := v[list.name].Bytes
			if len(b)%16 != 0 {
				return errFlowState
			}
			for len(b) > 0 {
				id := flow.ID(b[:16])
				b = b[16:]
				journal := flows[id]
				if journal == nil || list.seen[id] || !bytes.Equal(journal["endpoint_id"].Bytes, e.id[:]) || list.name == "receipt_ids" && !seenReceipts[id] {
					return errFlowState
				}
				list.seen[id] = true
			}
		}
	}
	if len(listedFlows) != len(flows) || len(listedReceipts) != len(seenReceipts) {
		return errFlowState
	}
	for id, v := range flows {
		if v["phase"].U64 == uint64(flow.FlowPhaseTerminal) && !seenReceipts[id] {
			return errFlowState
		}
	}
	return nil
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func clientPrivateRecord(kind string, values map[string]sealedstate.Field) (sealedstate.Record, error) {
	return flowstate.ClientPrivateRecord(kind, values)
}
func endpointNum(v uint64) sealedstate.Field   { return flowstate.EndpointNum(v) }
func endpointBytes(v []byte) sealedstate.Field { return flowstate.EndpointBytes(v) }
func endpointText(v string) sealedstate.Field  { return flowstate.EndpointText(v) }
func clientCutDigest(cut sealedstate.CutState) ([32]byte, error) {
	return flowstate.ClientCutDigest(cut)
}

// Pure value operations stay below the runtime owner.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func flowLockBefore(mu interface {
	TryLock() bool
	Unlock()
}, deadline time.Time) bool {
	return flowstate.FlowLockBefore(mu, deadline)
}
func boolNumber(value bool) uint64 { return flowstate.BoolNumber(value) }
