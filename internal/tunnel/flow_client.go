package tunnel

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/hashicorp/yamux"
)

type clientFlowState struct {
	resumeBytes   atomic.Int64
	controlBytes  atomic.Int64
	mu            sync.Mutex
	cfg           FlowConfig
	manager       *flow.Manager
	endpoints     map[int]*clientEndpoint
	allocationOps map[int]chan struct{}
	carrierOps    map[int]chan struct{}
	frozen        bool
	admission     int
	generation    uint64
}

// Explicit image recovery and the ordinary port supervisor share one carrier
// operation. Waiting consumes the caller's deadline; it never holds an owner
// lock across TLS or RESUME. A successor uses the completed attachment rather
// than fencing it with a second concurrent reconnect.
// origin: hot-update S5 internal review HOTUPDATE-T40
func lockClientCarrier(ctx context.Context, c *Client, port int) (func(), error) {
	st := c.flowState
	for {
		st.mu.Lock()
		if st.frozen {
			st.mu.Unlock()
			return nil, errFlowState
		}
		if st.carrierOps == nil {
			st.carrierOps = make(map[int]chan struct{})
		}
		if done := st.carrierOps[port]; done != nil {
			st.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		st.carrierOps[port] = done
		st.mu.Unlock()
		return func() {
			st.mu.Lock()
			delete(st.carrierOps, port)
			close(done)
			st.mu.Unlock()
		}, nil
	}
}

// Only one control operation may replace a port's allocation. Other endpoints
// remain independent; a stale directive cannot tear down a newer allocation.
// origin: hot-update S5 plan §4.2 / §7.3 / T12
func lockClientAllocation(ctx context.Context, c *Client, port int) (func(), error) {
	st := c.flowState
	for {
		st.mu.Lock()
		if st.frozen {
			st.mu.Unlock()
			return nil, errFlowState
		}
		if st.allocationOps == nil {
			st.allocationOps = make(map[int]chan struct{})
		}
		if done := st.allocationOps[port]; done != nil {
			st.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.ctx.Done():
				return nil, c.ctx.Err()
			case <-done:
				continue
			}
		}
		done := make(chan struct{})
		st.allocationOps[port] = done
		st.mu.Unlock()
		return func() {
			st.mu.Lock()
			delete(st.allocationOps, port)
			close(done)
			st.mu.Unlock()
		}, nil
	}
}

// S5 cannot migrate a business socket across allocation/home epochs. Retire
// the original flows and supervisor, then admit a fresh endpoint for new TCP.
// The session is removed first so its supervisor cannot recreate the old owner.
// origin: hot-update S5 plan §0 / §4.2 / T12
func retireClientAllocation(c *Client, port, localPort int, token string, epoch int64, nid string) bool {
	st := c.flowState
	c.mu.Lock()
	st.mu.Lock()
	e := st.endpoints[port]
	if e == nil {
		st.mu.Unlock()
		c.mu.Unlock()
		return false
	}
	if epoch < e.allocation.Epoch {
		st.mu.Unlock()
		c.mu.Unlock()
		return true
	}
	changed := e.allocation.Epoch != epoch || e.allocation.NID != nid || e.allocation.TokenHash != hashToken(token) || e.target != uint16(localPort)
	st.mu.Unlock()
	if !changed {
		c.mu.Unlock()
		return false
	}
	old := c.sessions[port]
	delete(c.sessions, port)
	c.mu.Unlock()
	if old != nil {
		old.cancel()
	}
	closeClientEndpoint(c, port)
	if old != nil {
		if secure, ok := old.conn.(*tls.Conn); ok {
			_ = secure.NetConn().Close()
		}
		_ = old.conn.Close()
		_ = old.yamuxSess.Close()
	}
	return false
}

type clientEndpoint struct {
	stop              chan struct{}
	stopOnce          sync.Once
	supervisorDone    chan struct{}
	id                flow.EndpointID
	allocation        Allocation
	owner, carrier    uint64
	target            uint16
	ticket            []byte
	request           resumeRequest
	permit            *flowPermit
	attachment        *flowCarrier
	token, brokerAddr string
	pins              proto.CertPins
	authority         Authority
	detachDeadline    time.Time
}
type clientFrozenState struct {
	mu       sync.Mutex
	client   *Client
	flows    *flow.Frozen
	released bool
}

func (c *Client) FlowCensus() Census {
	c.mu.Lock()
	legacy := len(c.sessions)
	sessionPorts := make(map[int]bool, len(c.sessions))
	for port := range c.sessions {
		sessionPorts[port] = true
	}
	st := c.flowState
	c.mu.Unlock()
	if st == nil {
		return Census{Legacy: legacy, Unsupported: true}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	out := Census{Admission: st.admission, Retained: len(st.endpoints)}
	for port, e := range st.endpoints {
		out.Owned += len(flow.States(st.manager, e.id))
		if sessionPorts[port] {
			legacy--
		}
	}
	out.Legacy = max(legacy, 0)
	return out
}
func (c *Client) SetFlowConfig(cfg FlowConfig) error {
	if err := validateFlowConfig(cfg, false); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Start only anchors lifetime. Existing callers start an idle adapter before
	// Agent.New installs its shared budget; no carrier mode has been chosen yet.
	// Once any open/dial begins, or cancellation owns cleanup, configuration is
	// immutable so no legacy transport or retired manager can become a v1 owner.
	// origin: hot-update S5 plan §4.1 / §7.1 / P13-REGRESS
	if c.flowConfigUsed || c.flowState != nil || c.ctx != nil && c.ctx.Err() != nil {
		return errFlowState
	}
	st := &clientFlowState{cfg: cfg, endpoints: make(map[int]*clientEndpoint)}
	m, err := flow.New(flow.Config{Budget: cfg.Budget, Dial: func(id flow.ID, port uint16, deadline time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		return rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port), deadline, stop, func(tuple rawsocket.Tuple) error {
			observeFlowSocket(id, flow.EndpointID{}, tuple)
			if cfg.BeforeLocalConnect != nil {
				return cfg.BeforeLocalConnect(id, tuple)
			}
			return nil
		})
	}})
	if err != nil {
		return err
	}
	st.manager = m
	c.flowState = st
	return nil
}
func selectedFlow(conn net.Conn) bool {
	tlsConn, ok := conn.(*tls.Conn)
	return ok && tlsConn.ConnectionState().NegotiatedProtocol == FlowALPN
}

// attachClientFlow runs before the new transport is published to the legacy
// supervisor. Its first stream is always endpoint control, never user DATA.
func attachClientFlow(c *Client, conn net.Conn, mux *yamux.Session, port int, token, addr string, epoch int64, pins proto.CertPins, generation uint64) error {
	st := c.flowState
	if st == nil {
		return errFlowState
	}
	e, req, err := beginClientFlowResume(c, port, token, addr, epoch, pins, generation)
	if err != nil {
		return err
	}
	defer func() { st.mu.Lock(); st.admission--; st.mu.Unlock() }()
	stream, resp, err := exchangeClientFlowResume(st, mux, req)
	if err != nil {
		return err
	}
	resp.Authority.Allocation = e.allocation
	a := newFlowCarrier(conn, mux, stream, st.manager, resp.Endpoint, resp.Owner, resp.Carrier, flow.FlagDirection)
	a.ResumeBytes = &st.resumeBytes
	a.ControlBudget = &st.controlBytes
	st.mu.Lock()
	if st.frozen || st.generation != generation || st.endpoints[port] != e || e.allocation.NID != c.nidValue() {
		st.mu.Unlock()
		a.Close()
		return errFlowState
	}
	old := e.attachment
	e.id = resp.Endpoint
	e.owner = resp.Owner
	e.carrier = resp.Carrier
	e.ticket = resp.Ticket
	e.authority = resp.Authority
	if req.Mode == flow.ResumeModeActivate && e.permit != nil {
		e.permit.consumed = true
		e.permit.digest = req.Digest
		st.cfg.OwnerGen = resp.Owner
	}
	e.request = resumeRequest{}
	e.attachment = a
	e.detachDeadline = time.Time{}
	st.mu.Unlock()
	if old != nil {
		old.Close()
		old.Join()
	}
	if err := bindClientFlowResume(st, e, resp, a); err != nil {
		return err
	}
	_ = stream.SetDeadline(time.Time{})
	a.Start()
	a.AcceptClient(e.target)
	if err := abortClientPermit(st, e, a); err != nil {
		return err
	}
	// origin: hot-update S5 plan §9.1. A completed REGISTER cannot prove
	// fresh authority or data binding; observe the accepted RESUME here.
	observeAttachment(conn, e.id, resp.Owner, resp.Carrier, uint64(req.Mode), e.allocation.Port, "resume")
	c.logger.Info("tunnel: flow carrier resumed", "owner_generation", resp.Owner,
		"carrier_generation", resp.Carrier, "mode", uint8(req.Mode))
	return nil
}

// Reserve attachment admission and retain one request identity across retries.
// The caller releases admission only after exchange, publication and binding.
// origin: hot-update S5 plan §4.2 / §4.4 / T13
func beginClientFlowResume(c *Client, port int, token, addr string, epoch int64, pins proto.CertPins, generation uint64) (*clientEndpoint, resumeRequest, error) {
	st := c.flowState
	if c.localPortLookup == nil {
		return nil, resumeRequest{}, errFlowState
	}
	target, err := c.localPortLookup(port)
	if err != nil || target <= 0 || target > 65535 {
		return nil, resumeRequest{}, errFlowState
	}
	st.mu.Lock()
	if st.frozen || st.generation != generation {
		st.mu.Unlock()
		return nil, resumeRequest{}, errFlowState
	}
	st.admission++
	e := st.endpoints[port]
	if e == nil {
		if len(st.endpoints) >= MaxEndpoints {
			st.admission--
			st.mu.Unlock()
			return nil, resumeRequest{}, errFlowState
		}
		e = &clientEndpoint{stop: make(chan struct{}), allocation: Allocation{SID: c.sid, NID: c.nidValue(), Port: port, TokenHash: hashToken(token), Epoch: epoch}, owner: st.cfg.OwnerGen, target: uint16(target), token: token, brokerAddr: addr, pins: pins}
		st.endpoints[port] = e
	} else if e.allocation.TokenHash != hashToken(token) || e.allocation.Epoch != epoch || e.target != uint16(target) {
		st.admission--
		st.mu.Unlock()
		return nil, resumeRequest{}, errFlowState
	}
	req := e.request
	if req.RequestID == ([16]byte{}) {
		req = resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeReconnect, Endpoint: e.id, Owner: e.owner, Target: e.target, Ticket: append([]byte(nil), e.ticket...), Entries: flow.States(st.manager, e.id)}
		if e.id == (flow.EndpointID{}) {
			req.Mode = flow.ResumeModeInitial
			req.Digest = sha256.Sum256([]byte(st.cfg.InstanceID + "\x00" + st.cfg.BootID))
		}
		if _, err = rand.Read(req.RequestID[:]); err != nil {
			st.admission--
			st.mu.Unlock()
			return nil, resumeRequest{}, err
		}
		e.request = req
	}
	st.mu.Unlock()
	return e, req, nil
}

// Exchange the retained request on the new carrier before publishing it.
// Only its correlated, fully validated reply can decide the logical owner.
// origin: hot-update S5 plan §2.2 / §4.3 / T13
func exchangeClientFlowResume(st *clientFlowState, mux *yamux.Session, req resumeRequest) (*yamux.Stream, resumeResponse, error) {
	stream, err := mux.OpenStream()
	if err != nil {
		return nil, resumeResponse{}, clientResumeTransportError(err)
	}
	_ = stream.SetDeadline(time.Now().Add(ResumeAttempt))
	body := encodeResumeRequest(req)
	if err = flow.EncodeFrame(stream, flow.Frame{Version: flow.Version, Kind: flow.KindResume, BodyLen: uint32(len(body)), Body: body}); err != nil {
		return nil, resumeResponse{}, clientResumeTransportError(err)
	}
	frame, err := flow.DecodeFrame(stream)
	// Correlate the refusal before allowing a supervisor to retire its owner.
	// An unrelated or malformed control frame is never restart evidence.
	// origin: hot-update S5 plan §2.2 / §4.2 / P13-REGRESS
	if err == nil && frame.Kind == flow.KindReject && frame.FlowID == (flow.ID{}) && frame.Flags == 0 && frame.CarrierGen == 0 && bytes.Equal(frame.Body[:16], req.RequestID[:]) {
		return nil, resumeResponse{}, &flow.Failure{Reason: flow.Reason(binary.BigEndian.Uint16(frame.Body[16:]))}
	}
	if err != nil {
		var deny *DenyError
		if retry := clientResumeTransportError(err); errors.As(retry, &deny) {
			return nil, resumeResponse{}, retry
		}
		return nil, resumeResponse{}, errFlowState
	}
	if frame.Kind != flow.KindResumeOK {
		return nil, resumeResponse{}, errFlowState
	}
	resp, err := readResumePages(frame, func() (flow.Frame, error) { return flow.DecodeFrame(stream) }, &st.resumeBytes)
	if err != nil || resp.RequestID != req.RequestID || resp.Verdict != flow.ResumeVerdictAccepted || resp.Carrier != frame.CarrierGen || resp.Owner == 0 || resp.GrantDigest != responseDigest(resp) {
		return nil, resumeResponse{}, errFlowState
	}
	return stream, resp, nil
}

func clientResumeTransportError(err error) error {
	// REGISTER can pass before the RESUME authority recheck observes lag.
	// A closed transport carries no accepted owner decision. Preserve it as
	// a bounded retry for the caller; a new REGISTER rechecks terminal denial.
	// Malformed frames still fail instead of being accepted or downgraded.
	// origin: hot-update S5 plan §4.3 / T13 / DRILL-RUN 75.A
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, yamux.ErrSessionShutdown) {
		return &DenyError{Reason: "try_again"}
	}
	return err
}

func bindClientFlowResume(st *clientFlowState, e *clientEndpoint, resp resumeResponse, a *flowCarrier) error {
	// Reconcile carrier generations before reading terminal control evidence.
	// Missing live entries can represent a retained peer terminal receipt; its
	// ACK/CLOSE (rather than retirement alone) supplies the final byte outcome.
	// origin: hot-update S5 internal review rv-flow-V1
	peers := make(map[flow.ID]flow.State, len(resp.Entries))
	for _, peer := range resp.Entries {
		peers[peer.ID] = peer
	}
	for _, local := range flow.States(st.manager, e.id) {
		applied := local.TxBase
		if peer, ok := peers[local.ID]; ok {
			if peer.OpenSeq != local.OpenSeq || peer.Intent != local.Intent {
				a.Close()
				return errFlowState
			}
			applied = peer.RxApplied
		}
		if err := flow.Bind(st.manager, local.ID, e.id, local.Intent, resp.Owner, resp.Carrier, applied); err != nil {
			a.Close()
			return err
		}
	}
	return nil
}

func (c *Client) PrepareHandoff(nextOwner uint64, cutID [16]byte, deadline time.Time) error {
	st := c.flowState
	if st == nil {
		return errFlowState
	}
	st.mu.Lock()
	if st.frozen || nextOwner != st.cfg.OwnerGen+1 || nextOwner == 0 || cutID == ([16]byte{}) {
		st.mu.Unlock()
		return errFlowState
	}
	endpoints := make([]*clientEndpoint, 0, len(st.endpoints))
	for _, e := range st.endpoints {
		endpoints = append(endpoints, e)
	}
	st.mu.Unlock()
	// All admitted active endpoints start together; a slow authority operation
	// consumes only its own worker and the shared entry deadline.
	var wg sync.WaitGroup
	errs := make(chan error, len(endpoints))
	for _, e := range endpoints {
		wg.Add(1)
		go func(e *clientEndpoint) {
			defer wg.Done()
			st.mu.Lock()
			a := e.attachment
			req := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModePrepare, Endpoint: e.id, Owner: e.owner, Next: nextOwner, Cut: cutID, Target: e.target, Ticket: append([]byte(nil), e.ticket...)}
			st.mu.Unlock()
			if a == nil {
				errs <- errFlowState
				return
			}
			if _, err := rand.Read(req.RequestID[:]); err != nil {
				errs <- err
				return
			}
			err := prepareClientEndpoint(c, e, &a, req, deadline)
			if err != nil {
				errs <- err
				return
			}
			st.mu.Lock()
			// Preserve retries of a live cut, but replace consumed predecessor
			// history after the broker accepts this current-owner PREPARE.
			// origin: hot-update S5 internal review rv-flow-3
			if e.permit == nil || e.permit.consumed && e.permit.next == e.owner {
				var activateID [16]byte
				if _, err := rand.Read(activateID[:]); err != nil {
					st.mu.Unlock()
					errs <- err
					return
				}
				e.permit = &flowPermit{requestID: activateID, cutID: cutID, owner: e.owner, next: nextOwner, deadline: time.Now().Add(PermitTTL)}
			}
			st.mu.Unlock()
			// origin: hot-update S5 plan §4.4 / §9.1. This observes the
			// accepted permit while the old business owner is still running.
			observeAttachment(a.Conn, e.id, req.Owner, a.Gen, nextOwner, e.allocation.Port, "prepare")
			c.logger.Info("tunnel: flow handoff prepared", "owner_generation", req.Owner,
				"carrier_generation", a.Gen, "generation", nextOwner)
		}(e)
	}
	wg.Wait()
	close(errs)
	return errors.Join(channelErrors(errs)...)
}

// origin: hot-update S5 plan §4.3 / §4.4 / T13 / DRILL-RUN. A fresh
// authority read may refuse while the local command cursor catches up. That
// closes its attachment, but the original logical owner remains resumable.
// Spend the original entry deadline recovering that owner and reauthorizing
// PREPARE; no stale grant or new business socket can satisfy this operation.
func prepareClientEndpoint(c *Client, e *clientEndpoint, attachment **flowCarrier, req resumeRequest, deadline time.Time) error {
	ctx, cancel := context.WithDeadline(context.Background(), deadline) // ctx-none: PREPARE has an entry deadline and retained endpoint ownership, including before Client.Start.
	defer cancel()
	for {
		if *attachment != nil {
			if err := exchangeResume(*attachment, req, deadline); err == nil {
				return nil
			}
		}
		if ctx.Err() != nil {
			return errFlowState
		}
		st := c.flowState
		st.mu.Lock()
		valid := st.endpoints[e.allocation.Port] == e && e.owner == req.Owner
		st.mu.Unlock()
		if !valid {
			return errFlowState
		}
		// Use the existing transient/terminal authority classification and
		// backoff, bounded by this PREPARE's deadline rather than a new grace.
		if err := resumeClientEndpoint(c, e, deadline); err != nil {
			return err
		}
		st.mu.Lock()
		*attachment = e.attachment
		req.Ticket = append([]byte(nil), e.ticket...)
		st.mu.Unlock()
	}
}

func exchangeResume(a *flowCarrier, req resumeRequest, deadline time.Time) error {
	ch := make(chan resumeResponse, flow.MaxResumePages)
	a.Mu.Lock()
	if a.Closed {
		a.Mu.Unlock()
		return errFlowState
	}
	a.Requests[req.RequestID] = ch
	a.Mu.Unlock()
	defer func() { a.Mu.Lock(); delete(a.Requests, req.RequestID); a.Mu.Unlock() }()
	body := encodeResumeRequest(req)
	a.Enqueue(flow.Frame{Version: flow.Version, Kind: flow.KindResume, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body})
	if deadline.IsZero() {
		deadline = time.Now().Add(ResumeAttempt)
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	next := func() (flow.Frame, error) {
		select {
		case <-timer.C:
			return flow.Frame{}, errFlowState
		case <-a.Stop:
			return flow.Frame{}, errFlowState
		case resp := <-ch:
			body := encodeResumeResponse(resp)
			return flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, CarrierGen: resp.Carrier, BodyLen: uint32(len(body)), Body: body}, nil
		}
	}
	first, err := next()
	if err != nil {
		return err
	}
	resp, err := readResumePages(first, next, a.ResumeBytes)
	if err != nil || resp.RequestID != req.RequestID {
		return errFlowState
	}
	return nil
}
func (c *Client) ActivateRaw() error {
	if c.flowState == nil {
		return errFlowState
	}
	return c.flowState.manager.ActivateRaw()
}

// Preserve the refused owner as a fenced endpoint until explicit teardown.
// Recovery must report its refusal rather than an empty successful census.
// This function can run inside a supervisor: it never joins itself.
// origin: hot-update S5 internal review HOTUPDATE-T40
func rejectClientEndpoint(c *Client, port int) {
	st := c.flowState
	if st == nil {
		return
	}
	st.mu.Lock()
	e := st.endpoints[port]
	var attachment *flowCarrier
	if e != nil {
		e.stopOnce.Do(func() { close(e.stop) })
		attachment = e.attachment
	}
	st.mu.Unlock()
	if e == nil {
		return
	}
	if attachment != nil {
		attachment.Close()
	}
	for _, r := range flow.States(st.manager, e.id) {
		_ = st.manager.RevokeFlow(r.ID, flow.ReasonRevoked)
	}
}

func (c *Client) RevokeFlow(id flow.ID, reason flow.Reason) error {
	st := c.flowState
	if st == nil {
		return errFlowState
	}
	// Publish an explicit CLOSE while the peer is still attached. The local
	// revoke also shuts down duplicates, so a silent SS victim cannot linger.
	st.mu.Lock()
	var carrier *flowCarrier
	// SS shutdown may settle local IO before its callback submits the revoke.
	// The retained receipt still identifies the carrier; States lists only live IO.
	// origin: hot-update S5 internal review SS-REVOKE-1
	if r, _, err := flow.Observe(st.manager, id); err == nil {
		for _, e := range st.endpoints {
			if r.EndpointID == e.id {
				carrier = e.attachment
			}
		}
	}
	st.mu.Unlock()
	err := st.manager.RevokeFlow(id, reason)
	if carrier != nil {
		frames, _, e := flow.Controls(st.manager, id, carrier.Gen)
		if e == nil {
			for _, f := range frames {
				if f.Kind == flow.KindClose {
					carrier.Enqueue(f)
				}
			}
		}
	}
	return err
}
func closeClientEndpoint(c *Client, port int) {
	st := c.flowState
	if st == nil {
		return
	}
	st.mu.Lock()
	e := st.endpoints[port]
	var attachment *flowCarrier
	var supervisor <-chan struct{}
	if e != nil {
		attachment, supervisor = e.attachment, e.supervisorDone
		e.stopOnce.Do(func() { close(e.stop) })
	}
	delete(st.endpoints, port)
	st.generation++
	st.mu.Unlock()
	releaseClientEndpoint(st, e, attachment, supervisor)
}

func releaseClientEndpoint(st *clientFlowState, e *clientEndpoint, attachment *flowCarrier, supervisor <-chan struct{}) {
	if e == nil {
		return
	}
	if attachment != nil {
		attachment.Close()
		attachment.Join()
	}
	if supervisor != nil {
		<-supervisor
	}
	for _, r := range flow.States(st.manager, e.id) {
		_ = st.manager.RevokeFlow(r.ID, flow.ReasonRevoked)
		_ = flow.CollectReceipt(st.manager, r.ID)
	}
	_ = flow.ForgetEndpoint(st.manager, e.id)
}

// Only the ordinary, still-current port supervisor may abandon a missing peer
// owner. Explicit image recovery keeps its refusal and current-cut responsibility;
// pending handoffs and concurrent replacement/teardown cannot reset an owner.
// origin: hot-update S5 plan §0 / §4.2 / §5.4 / P13-REGRESS
func retireLostClientEndpoint(c *Client, port int, gen uint64) bool {
	st := c.flowState
	if st == nil {
		return false
	}
	c.mu.Lock()
	st.mu.Lock()
	e, session := st.endpoints[port], c.sessions[port]
	if st.frozen || e == nil || session == nil || session.gen != gen || c.ctx.Err() != nil ||
		e.request.Mode != flow.ResumeModeReconnect || e.permit != nil && !e.permit.consumed {
		st.mu.Unlock()
		c.mu.Unlock()
		return false
	}
	attachment, supervisor := e.attachment, e.supervisorDone
	e.stopOnce.Do(func() { close(e.stop) })
	delete(st.endpoints, port)
	st.generation++
	st.mu.Unlock()
	c.mu.Unlock()
	releaseClientEndpoint(st, e, attachment, supervisor)
	return true
}
func clientFlowWait(c *Client, port int, mux *yamux.Session) bool {
	st := c.flowState
	if st == nil {
		return false
	}
	st.mu.Lock()
	e := st.endpoints[port]
	if e == nil || e.attachment == nil || e.attachment.Mux != mux {
		st.mu.Unlock()
		return false
	}
	a := e.attachment
	st.mu.Unlock()
	<-a.Stop
	a.Join()
	st.mu.Lock()
	if e.attachment == a {
		e.attachment = nil
		if e.detachDeadline.IsZero() {
			e.detachDeadline = time.Now().Add(DetachedGrace)
		}
	}
	st.mu.Unlock()
	return true
}
func restoreCutState(cfg FlowConfig, state sealedstate.CutState) (*Client, []handoff.Resource, error) {
	return restoreClientRecords(cfg, state)
}

func (st *clientFlowState) managerDial() func(flow.ID, uint16, time.Time, <-chan struct{}) (*rawsocket.Socket, error) {
	return func(id flow.ID, port uint16, deadline time.Time, stop <-chan struct{}) (*rawsocket.Socket, error) {
		return rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port), deadline, stop, func(tuple rawsocket.Tuple) error {
			if st.cfg.BeforeLocalConnect != nil {
				return st.cfg.BeforeLocalConnect(id, tuple)
			}
			return nil
		})
	}
}

// FlowAuthorities returns only grants obtained by a completed, verified
// RESUME. Callers install these before opening SS's AuthorityPending gate.
func FlowAuthorities(c *Client) []Authority {
	if c == nil || c.flowState == nil {
		return nil
	}
	st := c.flowState
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []Authority
	for _, e := range st.endpoints {
		if e.attachment == nil {
			continue
		}
		grant := e.authority
		grant.Keys = append([]AuthorizedKey(nil), grant.Keys...)
		for i := range grant.Keys {
			grant.Keys[i].Secret = append([]byte(nil), grant.Keys[i].Secret...)
		}
		out = append(out, grant)
	}
	return out
}

// Pure value operations stay below the runtime owner.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func channelErrors(ch <-chan error) []error { return flowstate.ChannelErrors(ch) }
