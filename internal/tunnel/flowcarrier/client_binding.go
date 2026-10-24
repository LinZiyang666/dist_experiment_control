package flowcarrier

import (
	"encoding/binary"
	"errors"
	"net"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
)

// Stream retirement is independent of the carrier. The peer's terminal
// receipt proves it can observe this stream ending without losing another flow.
func (a *Carrier) ReleaseFlowStream(id flow.ID) {
	a.Mu.Lock()
	stream := a.Streams[id]
	delete(a.Streams, id)
	delete(a.Bound, id)
	// A terminal reconciliation may have no DATA stream at all. Wake recovery
	// waiters when that flow leaves the live set, not only on stream binding.
	// origin: hot-update S5 internal review rv-flow-V1
	close(a.BoundChanged)
	a.BoundChanged = make(chan struct{})
	a.Mu.Unlock()
	if stream != nil {
		a.Launch(func() { _ = stream.Close() })
	}
}

func (a *Carrier) BindClientFlow(stream net.Conn, localPort uint16) {
	_ = stream.SetReadDeadline(time.Now().Add(flowstate.ResumeAttempt))
	first, err := flow.DecodeFrame(stream)
	if err != nil || first.CarrierGen != a.Gen || !a.ReserveBind(first.FlowID) {
		a.Close()
		return
	}
	var state flow.State
	var offset uint64
	switch first.Kind {
	case flow.KindOpen:
		b := first.Body
		if len(b) != 74 {
			a.Close()
			return
		}
		req := flow.OpenRequest{ID: first.FlowID, OpenSeq: binary.BigEndian.Uint64(b[16:24]), TargetPort: binary.BigEndian.Uint16(b[72:74]), OwnerGen: a.Owner, CarrierGen: a.Gen, Direction: flow.FlagDirection, Deadline: time.Now().Add(flowstate.ResumeAttempt)}
		copy(req.EndpointID[:], b[:16])
		copy(req.Intent[:], b[24:56])
		copy(req.BindNonce[:], b[56:72])
		if req.EndpointID != a.Endpoint || req.TargetPort != localPort {
			a.Close()
			return
		}
		state, err = flow.Open(a.Manager, req, nil)
		if errors.Is(err, &flow.Failure{Reason: flow.ReasonRetired}) {
			state, err = RetainedClientOpenReceipt(a.Manager, req)
		}
		if err != nil {
			a.Close()
			return
		}
		for state.OpenStatus == 1 {
			var ch <-chan struct{}
			state, ch, err = flow.Observe(a.Manager, req.ID)
			if err != nil {
				a.Close()
				return
			}
			if state.OpenStatus != 1 {
				break
			}
			select {
			case <-a.Stop:
				return
			case <-ch:
			}
		}
		body := append([]byte(nil), b[:72]...)
		body = append(body, state.OpenStatus)
		body = binary.BigEndian.AppendUint16(body, uint16(state.OpenReason))
		if a.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindOpenACK, FlowID: req.ID, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}) != nil {
			return
		}
		if state.OpenStatus != 2 {
			a.ReleaseFlowStream(req.ID)
			// pumpData has not registered this accepted stream yet. Close its
			// actual owner on refusal, outside carrier/flow locks.
			// origin: hot-update S5 internal review rv-flow-5
			_ = stream.Close()
			return
		}
	case flow.KindResume:
		req, e := flowstate.DecodeResumeRequest(first.Body)
		if e != nil || req.Scope != flow.ResumeScopeFlow || req.Mode != flow.ResumeModeBind || req.Endpoint != a.Endpoint || req.Owner != a.Owner || len(req.Entries) != 1 || req.Entries[0].ID != first.FlowID {
			a.Close()
			return
		}
		peer := req.Entries[0]
		if err = flow.Bind(a.Manager, first.FlowID, a.Endpoint, peer.Intent, a.Owner, a.Gen, peer.RxApplied); err != nil {
			a.Close()
			return
		}
		state, _, err = flow.Observe(a.Manager, first.FlowID)
		if err != nil {
			a.Close()
			return
		}
		offset = peer.RxApplied
		body := flowstate.EncodeResumeResponse(flowstate.ResumeResponse{RequestID: req.RequestID, Endpoint: a.Endpoint, Owner: a.Owner, Carrier: a.Gen, Verdict: flow.ResumeVerdictAccepted, Pages: 1, Retirement: flow.Retirement(a.Manager, a.Endpoint), Entries: []flow.State{state}})
		if a.Write(stream, flow.Frame{Version: flow.Version, Kind: flow.KindResumeOK, FlowID: first.FlowID, CarrierGen: a.Gen, BodyLen: uint32(len(body)), Body: body}) != nil {
			return
		}
	// No other frame can establish a flow stream before OPEN or RESUME bind.
	// origin: hot-update S5 plan §2.2 / §10.2
	case flow.KindOpenACK, flow.KindResumeOK, flow.KindReject, flow.KindData, flow.KindACK, flow.KindFIN, flow.KindClose:
		a.Close()
		return
	default:
		a.Close()
		return
	}
	_ = stream.SetReadDeadline(time.Time{})
	a.PumpData(first.FlowID, stream, offset)
}

// A cut can lose revoke CLOSE after the local socket has already retired.
// The peer then repeats OPEN because RESUME lists only live IO. Return the
// exact journal's irreversible refusal; a retired watermark alone cannot
// authorize an identity/intent, and no new socket or DATA binding is allowed.
// origin: hot-update S5 plan §3.2 / §6.5 / T21 / DRILL-RUN 75.R
func RetainedClientOpenReceipt(manager *flow.Manager, req flow.OpenRequest) (flow.State, error) {
	state, _, err := flow.Observe(manager, req.ID)
	if err != nil || state.Phase != flow.FlowPhaseTerminal || state.EndpointID != req.EndpointID || state.OpenSeq != req.OpenSeq || state.Intent != req.Intent || state.TargetPort != req.TargetPort {
		return flow.State{}, flowstate.ErrFlowState
	}
	state.OpenStatus = 3
	if state.OpenReason == 0 {
		state.OpenReason = flow.ReasonRetired
	}
	return state, nil
}
