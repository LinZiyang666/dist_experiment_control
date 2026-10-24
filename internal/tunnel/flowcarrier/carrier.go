package flowcarrier

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/yamux"
)

type Carrier struct {
	Mu             sync.Mutex
	ControlWriteMu sync.Mutex
	RequestBusy    atomic.Bool
	ResumeBytes    *atomic.Int64
	ControlBudget  *atomic.Int64
	ControlBytes   int64
	AuthorizeBind  func() error
	Conn           net.Conn
	Mux            *yamux.Session
	Control        net.Conn
	Manager        *flow.Manager
	Endpoint       flow.EndpointID
	Owner, Gen     uint64
	Direction      uint16
	Stop           chan struct{}
	Once           sync.Once
	Workers        sync.WaitGroup
	Bound          map[flow.ID]bool
	Streams        map[flow.ID]net.Conn
	BoundChanged   chan struct{}
	// One coalesced frame per (flow,kind) caps control memory independently of
	// a slow TLS writer. At most 32 flows * 3 small control receipts are retained.
	Queued    map[ControlKey]flow.Frame
	Wake      chan struct{}
	Closed    bool
	OnRequest func(flowstate.ResumeRequest) ([]flowstate.ResumeResponse, error)
	Requests  map[[16]byte]chan flowstate.ResumeResponse
}

type ControlKey struct {
	ID      flow.ID
	Kind    flow.Kind
	Request [16]byte
}

func NewFlowCarrier(conn net.Conn, mux *yamux.Session, control net.Conn, m *flow.Manager, id flow.EndpointID, owner, gen uint64, direction uint16) *Carrier {
	return &Carrier{ControlBudget: new(atomic.Int64), Conn: conn, Mux: mux, Control: control, Manager: m, Endpoint: id, Owner: owner, Gen: gen, Direction: direction, Stop: make(chan struct{}), Bound: make(map[flow.ID]bool), Streams: make(map[flow.ID]net.Conn), BoundChanged: make(chan struct{}), Queued: make(map[ControlKey]flow.Frame), Wake: make(chan struct{}, 1), Requests: make(map[[16]byte]chan flowstate.ResumeResponse)}
}

func (a *Carrier) Launch(fn func()) bool {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	if a.Closed {
		return false
	}
	a.Workers.Add(1)
	go func() { defer a.Workers.Done(); fn() }()
	return true
}

func (a *Carrier) Close() {
	a.Once.Do(func() {
		a.Mu.Lock()
		a.Closed = true
		releaseControlLocked(a, controlBatchBytes(a.Queued))
		a.Queued = nil
		close(a.Stop)
		a.Mu.Unlock()
		// A stream close cannot wake a yamux Read blocked in sendWindowUpdate.
		// Close the shared TLS transport FIRST, with no owner lock held, then join.
		// origin: hot-update S5 plan §3.5 / T11
		if a.Conn != nil {
			if tc, ok := a.Conn.(*tls.Conn); ok {
				_ = tc.NetConn().Close()
			}
			_ = a.Conn.Close()
		}
		if a.Mux != nil {
			_ = a.Mux.Close()
		}
	})
}

func (a *Carrier) Join() { a.Workers.Wait() }

func FlowMuxConfig(loggerWriter io.Writer) *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = loggerWriter
	cfg.ConnectionWriteTimeout = flowstate.CarrierWriteBudget
	cfg.MaxStreamWindowSize = 256 << 10
	return cfg
}

func (a *Carrier) Write(stream net.Conn, f flow.Frame) error {
	return writeCarrierFrame(a, stream, f, false)
}

func writeCarrierFrame(a *Carrier, stream net.Conn, f flow.Frame, reserved bool) error {
	if stream == a.Control && !reserved {
		a.Mu.Lock()
		ok := reserveControlLocked(a, int64(flow.HeaderBytes+len(f.Body)))
		a.Mu.Unlock()
		if !ok {
			a.Close()
			return flowstate.ErrFlowState
		}
		defer releaseControl(a, int64(flow.HeaderBytes+len(f.Body)))
	}
	if stream == a.Control {
		// origin: hot-update S5 plan §3.5 / T11 / DRILL-RUN. Only the
		// control health probe supervises the shared transport. A DATA
		// stream waiting for its own receive credit is normal backpressure;
		// fencing it on this timer repeatedly interrupts healthy neighbors
		// and races PREPARE. Yamux still bounds shared writer/sendCh waits.
		timer := time.AfterFunc(flowstate.CarrierWriteBudget, a.Close)
		defer timer.Stop()
		a.ControlWriteMu.Lock()
		defer a.ControlWriteMu.Unlock()
		_ = stream.SetWriteDeadline(time.Now().Add(flowstate.CarrierWriteBudget))
	}
	err := flow.EncodeFrame(stream, f)
	if err != nil {
		a.Close()
	}
	return err
}

func (a *Carrier) Enqueue(f flow.Frame) {
	rejected := false
	a.Mu.Lock()
	if !a.Closed {
		key := ControlKey{ID: f.FlowID, Kind: f.Kind}
		if f.Kind == flow.KindResume && len(f.Body) >= 22 {
			copy(key.Request[:], f.Body[6:22])
		}
		if f.Kind == flow.KindResumeOK && len(f.Body) >= 16 {
			copy(key.Request[:], f.Body[:16])
		}
		old, ok := a.Queued[key]
		if !ok || f.Kind != flow.KindACK || f.Offset >= old.Offset {
			delta := int64(flow.HeaderBytes + len(f.Body))
			if ok {
				delta -= int64(flow.HeaderBytes + len(old.Body))
			}
			if reserveControlLocked(a, delta) {
				a.Queued[key] = f
			} else {
				rejected = true
			}
		}
	}
	a.Mu.Unlock()
	if rejected {
		a.Close()
		return
	}
	select {
	case a.Wake <- struct{}{}:
	default:
	}
}

func (a *Carrier) Start() {
	a.Launch(func() {
		defer a.Close()
		for {
			select {
			case <-a.Stop:
				return
			case <-a.Wake:
			}
			a.Mu.Lock()
			batch := a.Queued
			a.Queued = make(map[ControlKey]flow.Frame)
			a.Mu.Unlock()
			if FlushControls(a, batch) != nil {
				return
			}
		}
	})
	a.Launch(func() {
		defer a.Close()
		for {
			f, err := flow.DecodeFrame(a.Control)
			if err != nil {
				return
			}
			if f.CarrierGen != a.Gen {
				return
			}
			switch f.Kind {
			case flow.KindACK, flow.KindFIN, flow.KindClose:
				if err = flow.Receive(a.Manager, f); err != nil {
					return
				}
				if f.Kind == flow.KindClose {
					if len(f.Body) == 35 && flow.CloseMode(f.Body[0]) == flow.CloseModeRevoke {
						_ = a.Manager.RevokeFlow(f.FlowID, flow.ReasonRevoked)
					}
				}
				// CLOSE may overtake DATA. Only a settled outcome proves the
				// retained source tail was drained or explicitly discarded.
				// origin: hot-update S5 internal review rv-flow-2 / rv-flow-V1
				// An ACK/FIN arriving after local revoke says nothing about whether
				// the peer has processed our CLOSE (it may still be waiting on the
				// shared writer). Only the peer's CLOSE proves it knows the terminal
				// decision and can safely observe stream EOF.
				// origin: hot-update S5 internal review SS-REVOKE-1
				// origin: hot-update S5 internal review HOTUPDATE-T40
				if f.Kind == flow.KindClose {
					if state, _, e := flow.Observe(a.Manager, f.FlowID); e == nil && state.Phase == flow.FlowPhaseTerminal {
						a.ReleaseFlowStream(f.FlowID)
					}
				}
			case flow.KindResume:
				if a.OnRequest == nil || !a.RequestBusy.CompareAndSwap(false, true) {
					return
				}
				req, e := flowstate.DecodeResumeRequest(f.Body)
				if e != nil {
					return
				}
				a.Launch(func() {
					defer a.RequestBusy.Store(false)
					responses, e := a.OnRequest(req)
					if e != nil {
						a.Close()
						return
					}
					for _, resp := range responses {
						body := flowstate.EncodeResumeResponse(resp)
						if a.Write(a.Control, flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}) != nil {
							return
						}
					}
				})
			case flow.KindResumeOK:
				response, e := flowstate.DecodeResumeResponse(f.Body)
				if e != nil {
					return
				}
				a.Mu.Lock()
				ch := a.Requests[response.RequestID]
				a.Mu.Unlock()
				if ch == nil {
					return
				}
				select {
				case ch <- response:
				case <-a.Stop:
					return
				}
			// OPEN/bind and DATA are confined to their per-flow streams.
			// origin: hot-update S5 plan §2.2 / §10.2
			case flow.KindOpen, flow.KindOpenACK, flow.KindReject, flow.KindData:
				return
			default:
				return
			}
		}
	})
}

func (a *Carrier) WatchControls(id flow.ID) {
	a.Launch(func() {
		for {
			frames, ch, err := flow.Controls(a.Manager, id, a.Gen)
			if err != nil {
				return
			}
			for _, f := range frames {
				a.Enqueue(f)
			}
			if ch == nil {
				// The control writer retires a normal/abort stream after sending
				// CLOSE (flushControls); a hard revoke keeps DATA open until the
				// peer closes it after that decision. An earlier stream EOF can
				// fence the peer's whole carrier before it learns that this
				// particular original socket was revoked, stranding its replay.
				// origin: hot-update S5 internal review SS-REVOKE-1 / plan §6.5
				// origin: hot-update S5 internal review HOTUPDATE-T40
				return
			}
			select {
			case <-a.Stop:
				return
			case <-ch:
			}
		}
	})
}

func (a *Carrier) PumpData(id flow.ID, stream net.Conn, start uint64) {
	a.Mu.Lock()
	a.Streams[id] = stream
	close(a.BoundChanged)
	a.BoundChanged = make(chan struct{})
	a.Mu.Unlock()
	a.WatchControls(id)
	a.Launch(func() {
		offset := start
		for {
			frame, ch, err := flow.NextData(a.Manager, id, a.Gen, offset)
			if err != nil {
				if !errors.Is(err, flow.ErrClosed) {
					a.Close()
				}
				return
			}
			if len(frame.Body) > 0 {
				if a.Write(stream, frame) != nil {
					return
				}
				offset += uint64(len(frame.Body))
				continue
			}
			select {
			case <-a.Stop:
				return
			case <-ch:
			}
		}
	})
	a.Launch(func() {
		for {
			frame, err := flow.DecodeFrame(stream)
			if err != nil {
				// A terminal sender may close its data direction before the last
				// control receipt reaches us. Applied inbound FIN already proves
				// this EOF safe, even while our reverse FIN receipt is in flight.
				// origin: hot-update S5 internal review rv-flow-1 / rv-flow-V1
				if !AwaitFlowDataEnd(a, id, err) {
					a.Close()
				}
				// The peer's stream close acknowledges our hard revoke. Both raw
				// owners are already terminal, so release this last stream reference.
				// origin: hot-update S5 internal review SS-REVOKE-1
				if state, _, lookupErr := flow.Observe(a.Manager, id); lookupErr == nil && state.Phase == flow.FlowPhaseTerminal {
					a.ReleaseFlowStream(id)
				}
				return
			}
			if frame.Kind != flow.KindData || frame.FlowID != id {
				a.Close()
				return
			}
			for {
				_, ch, e := flow.Observe(a.Manager, id)
				if e != nil {
					return
				}
				err = flow.Receive(a.Manager, frame)
				if !errors.Is(err, workload.ErrBackpressure) {
					break
				}
				select {
				case <-a.Stop:
					return
				case <-ch:
				}
			}
			if err != nil {
				if !errors.Is(err, &flow.Failure{Reason: flow.ReasonRetired}) {
					a.Close()
				}
				return
			}
		}
	})
}

// The control and DATA readers run independently. A correctly ordered CLOSE
// on the wire may still be applied after the data reader observes its EOF.
// Bound that scheduling gap by the carrier's own health budget; a silent or
// malformed EOF cannot leave an original socket waiting indefinitely.
// origin: hot-update S5 internal review HOTUPDATE-T40
func AwaitFlowDataEnd(a *Carrier, id flow.ID, readErr error) bool {
	timer := time.NewTimer(flowstate.CarrierWriteBudget)
	defer timer.Stop()
	for {
		state, changed, err := flow.Observe(a.Manager, id)
		if err != nil || state.Phase == flow.FlowPhaseTerminal || state.OpenStatus == 3 && state.Reason != 0 {
			return true
		}
		if !errors.Is(readErr, io.EOF) {
			return false
		}
		if state.FINFlags&6 == 6 && state.RxApplied == state.RxFIN {
			return true
		}
		select {
		case <-a.Stop:
			return true
		case <-changed:
		case <-timer.C:
			return false
		}
	}
}

func (a *Carrier) ReserveBind(id flow.ID) bool {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	if a.Closed || a.Bound[id] {
		return false
	}
	a.Bound[id] = true
	return true
}

func (a *Carrier) BindServerFlow(req flow.OpenRequest) {
	if !a.ReserveBind(req.ID) {
		return
	}
	a.Launch(func() {
		stream, err := a.Mux.OpenStream()
		if err != nil {
			a.Close()
			return
		}
		body := append([]byte(nil), req.EndpointID[:]...)
		body = binary.BigEndian.AppendUint64(body, req.OpenSeq)
		body = append(body, req.Intent[:]...)
		body = append(body, req.BindNonce[:]...)
		body = binary.BigEndian.AppendUint16(body, req.TargetPort)
		if a.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindOpen, FlowID: req.ID, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}) != nil {
			return
		}
		_ = stream.SetReadDeadline(time.Now().Add(flowstate.ResumeAttempt))
		ack, err := flow.DecodeFrame(stream)
		if err != nil || ack.Kind != flow.KindOpenACK || ack.FlowID != req.ID || ack.CarrierGen != a.Gen || len(ack.Body) != 75 || !bytes.Equal(ack.Body[:72], body[:72]) {
			a.Close()
			return
		}
		if ack.Body[72] != 2 {
			// The codec validates the refusal reason. Preserve a retained revoke
			// instead of turning its irreversible decision into generic IO loss.
			// origin: hot-update S5 plan §6.5 / T21 / DRILL-RUN 75.R
			_ = a.Manager.RevokeFlow(req.ID, flow.Reason(binary.BigEndian.Uint16(ack.Body[73:75])))
			a.ReleaseFlowStream(req.ID)
			_ = stream.Close()
			return
		}
		_ = stream.SetReadDeadline(time.Time{})
		if err = flow.Bind(a.Manager, req.ID, a.Endpoint, req.Intent, a.Owner, a.Gen, 0); err != nil {
			a.Close()
			return
		}
		a.PumpData(req.ID, stream, 0)
	})
}

func (a *Carrier) RebindServerFlow(state flow.State, peer flow.State) {
	if !a.ReserveBind(state.ID) {
		return
	}
	a.Launch(func() {
		if a.AuthorizeBind != nil {
			if err := a.AuthorizeBind(); err != nil {
				a.Close()
				return
			}
		}
		if err := flow.Bind(a.Manager, state.ID, a.Endpoint, state.Intent, a.Owner, a.Gen, peer.RxApplied); err != nil {
			a.Close()
			return
		}
		stream, err := a.Mux.OpenStream()
		if err != nil {
			a.Close()
			return
		}
		var rid [16]byte
		if _, err = rand.Read(rid[:]); err != nil {
			a.Close()
			return
		}
		// Bind carries the same approved local target as the endpoint grant;
		// omitting it produces a malformed RESUME before either data pump starts.
		// origin: hot-update S5 plan §2.2 / T12
		body := flowstate.EncodeResumeRequest(flowstate.ResumeRequest{Scope: flow.ResumeScopeFlow, Mode: flow.ResumeModeBind, RequestID: rid, Endpoint: a.Endpoint, Owner: a.Owner, Target: state.TargetPort, Entries: []flow.State{state}})
		if a.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindResume, FlowID: state.ID, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}) != nil {
			return
		}
		_ = stream.SetReadDeadline(time.Now().Add(flowstate.ResumeAttempt))
		frame, err := flow.DecodeFrame(stream)
		if err != nil || frame.Kind != flow.KindResumeOK || frame.FlowID != state.ID || frame.CarrierGen != a.Gen {
			a.Close()
			return
		}
		response, err := flowstate.DecodeResumeResponse(frame.Body)
		if err != nil || response.RequestID != rid || response.Verdict != flow.ResumeVerdictAccepted {
			a.Close()
			return
		}
		_ = stream.SetReadDeadline(time.Time{})
		a.PumpData(state.ID, stream, peer.RxApplied)
	})
}

func (a *Carrier) AcceptClient(localPort uint16) {
	a.Launch(func() {
		defer a.Close()
		for {
			stream, err := a.Mux.AcceptStream()
			if err != nil {
				return
			}
			a.Mu.Lock()
			full := len(a.Bound) >= flow.MaxFlows
			a.Mu.Unlock()
			if full {
				_ = stream.Close()
				return
			}
			if !a.Launch(func() { a.BindClientFlow(stream, localPort) }) {
				_ = stream.Close()
				return
			}
		}
	})
}
