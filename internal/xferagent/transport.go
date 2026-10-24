package xferagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferproof"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var _ x.Transport = (*Runtime)(nil)

// Request sends an agent-role continuation operation on the agent's own
// authenticated subtree (tether.s.<sid>.ev.node.<nid>.transfer.<tid>.<op>.req).
// Only the operations the agent ACL grants exist here; the broker binds the
// principal from that subject, never from this body.
//
// A sender that lost an OpSource reply reconciles with OpStatus. The agent role
// has no status verb, so the original source request is replayed byte-for-byte
// under its original CallID: the tracker's seal is idempotent and answers with
// the retained status, never a second seal.
// origin: hot-update S7 plan §6.3 / §7 / P20
func (r *Runtime) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	key := x.Key{SID: req.Identity.SID, TransferID: req.Identity.TransferID}
	if req.Origin == (x.Origin{}) {
		// The frozen SendConfig carries no Origin, so a sender's request names
		// none; the retained entry's original origin is the only valid route.
		// origin: S7 production activation — pull seals were unroutable
		if worker, _ := r.boundWorker(); worker != nil {
			if status, err := worker.Status(req.Identity); err == nil {
				req.Origin = status.Origin
			}
		}
	}
	op := req.Op
	if op == x.OpStatus {
		r.mu.Lock()
		original, ok := r.sources[key]
		r.mu.Unlock()
		if !ok {
			return x.Response{}, x.Attempt{}, fmt.Errorf("%s: no agent status verb for this entry", x.CodeReconcileRequired)
		}
		req, op = original, x.OpSource
	}
	var body any
	id, origin := proto.Identity(req.Identity), proto.Origin(req.Origin)
	switch op {
	case x.OpSource:
		if req.Source == nil {
			return x.Response{}, x.Attempt{}, errors.New("xferagent: source request without seal")
		}
		r.mu.Lock()
		r.sources[key] = req
		r.mu.Unlock()
		body = proto.TransferSourceReq{CallID: req.CallID, Identity: id, Origin: origin, Source: proto.SourceSeal(*req.Source)}
	case x.OpReserve:
		if req.Reserve == nil {
			return x.Response{}, x.Attempt{}, errors.New("xferagent: reserve request without receiver")
		}
		body = proto.TransferReserveReq{CallID: req.CallID, Identity: id, Origin: origin, ReceiverInstance: req.Reserve.ReceiverInstance}
	case x.OpTerminal:
		if req.Outcome == nil {
			return x.Response{}, x.Attempt{}, errors.New("xferagent: terminal request without outcome")
		}
		o := req.Outcome
		body = proto.TransferTerminalReq{CallID: req.CallID, Identity: id, Origin: origin, Outcome: proto.ReceiverOutcome{
			Grant:     proto.CommitGrant{Identity: proto.Identity(o.Grant.Identity), GrantNonce: o.Grant.GrantNonce, ReceiverInstance: o.Grant.ReceiverInstance, TerminalID: o.Grant.TerminalID},
			Primitive: proto.CommitPrimitive(o.Primitive), Outcome: proto.PrimitiveOutcome(o.Outcome), Bytes: o.Bytes, SHA256: o.SHA256,
			Code: o.Code, Error: o.Error, ParentSynced: o.ParentSynced, TmpCleanupPending: o.TmpCleanupPending}}
	case x.OpCancel:
		if req.Cancel == nil {
			return x.Response{}, x.Attempt{}, errors.New("xferagent: cancel request without reason")
		}
		body = proto.TransferCancelReq{CallID: req.CallID, Identity: id, Origin: origin, Reason: proto.CancelReason(req.Cancel.Reason)}
	// Name every unsupported operation so future enum members still require review.
	// origin: S7 internal review GATE-CONVERGE-contracts F1
	case x.OpProbe, x.OpPermit, x.OpPrepare, x.OpStatus, x.OpCommit, x.OpFinalize, x.OpCensus, x.OpBucketConfig:
		return x.Response{}, x.Attempt{}, fmt.Errorf("%s: %s is not an agent operation", x.CodePayloadUnsupported, op)
	default:
		return x.Response{}, x.Attempt{}, fmt.Errorf("%s: unknown agent operation %s", x.CodePayloadUnsupported, op)
	}
	subject, err := proto.SubjEvTransferRequest(req.Identity.SID, req.Identity.NID, req.Identity.TransferID, op)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	data, err := json.Marshal(body)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	reply, attempt, err := r.request(subject, data, req.CallID, d)
	if err != nil {
		return x.Response{}, attempt, err
	}
	resp, err := proto.DecodeTransferResponse(op, "", reply)
	if err != nil {
		return x.Response{}, attempt, errors.Join(errors.New(x.CodeReconcileRequired), err)
	}
	if op == x.OpTerminal {
		var terminal proto.TransferTerminalResp
		if json.Unmarshal(reply, &terminal) == nil && terminal.Canonical != nil {
			r.mu.Lock()
			r.canons[key] = x.CanonicalRecord{Identity: req.Identity, Kind: terminal.Canonical.Kind, ReqID: terminal.Canonical.ReqID,
				Digest: terminal.Canonical.Digest, Bytes: append([]byte(nil), terminal.Canonical.Bytes...)}
			r.mu.Unlock()
		}
	}
	return resp, attempt, nil
}

// request is one bounded attempt. A publish that never left reports Sent=false;
// a timeout after the publish is sent-unknown and must be reconciled.
func (r *Runtime) request(subject string, data []byte, callID string, d x.Deadline) (_ []byte, attempt x.Attempt, _ error) {
	start, _ := controlbudget.Now()
	attempt = x.Attempt{LogicalID: callID, Ordinal: 1, Phase: controlbudget.PhaseRequest, StartedMonoNS: start}
	// Every return ends this attempt. One refused before or at publish (the
	// control session still reconnecting after a JetStream restart, a SUB or
	// PUB refusal) ended at a real instant too. A zero finish time claims the
	// attempt is still in flight; the sender emits it as its seal evidence,
	// and the drill observer's custody rightly refused it (SETUP-RED2).
	// origin: S7 internal review FIX-TIERA2 F1 / hot-update S7 plan §10.4 / T41 / T46
	defer func() {
		if attempt.FinishedMonoNS == 0 {
			attempt.FinishedMonoNS, _ = controlbudget.Now()
		}
	}()
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, attempt, err
	}
	js := r.host.JetStream()
	if js == nil || js.Conn() == nil || !js.Conn().IsConnected() {
		return nil, attempt, fmt.Errorf("%s: agent control session is not connected", x.CodeOwnerRecovering)
	}
	nc := js.Conn()
	// ctx-none: the frozen Transport port has an absolute deadline, no caller ctx.
	ctx, cancel := context.WithTimeout(context.Background(), min(remaining, 5*time.Second))
	defer cancel()
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, attempt, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	pub := r.receiverKey(nc.Opts.SignatureCB)
	r.mu.Lock()
	signature, signErr := xferproof.Sign(r.receiver.Sign, xferproof.Message(subject, inbox, data))
	r.mu.Unlock()
	if signErr != nil {
		return nil, attempt, signErr
	}
	msg := nats.NewMsg(subject)
	msg.Reply, msg.Data = inbox, data
	msg.Header.Set(xferproof.KeyHeader, pub)
	msg.Header.Set(xferproof.SignatureHeader, signature)
	if err = nc.PublishMsg(msg); err != nil {
		attempt.Class = "publish"
		return nil, attempt, err
	}
	attempt.Sent = true
	msg, err = sub.NextMsgWithContext(ctx)
	attempt.FinishedMonoNS, _ = controlbudget.Now()
	if err != nil {
		attempt.Class = "reply"
		return nil, attempt, errors.Join(errors.New(x.CodeReconcileRequired), err)
	}
	if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
		attempt.Sent, attempt.Class = false, "no_responders"
		return nil, attempt, nats.ErrNoResponders
	}
	return msg.Data, attempt, nil
}

// sealOf returns the source seal this entry's sender submitted. The sender
// steps its writer only after the broker acknowledged that same seal.
func (r *Runtime) sealOf(id x.Identity) (x.SourceSeal, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, ok := r.sources[x.Key{SID: id.SID, TransferID: id.TransferID}]
	if !ok || req.Source == nil || req.Identity != id {
		return x.SourceSeal{}, false
	}
	return *req.Source, true
}

// takeCanonical hands a courier the exact broker TERMINAL that arrived with
// its latest receipt. The record is removed once bound.
func (r *Runtime) takeCanonical(id x.Identity) (x.CanonicalRecord, bool) {
	key := x.Key{SID: id.SID, TransferID: id.TransferID}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.canons[key]
	if ok && record.Identity == id {
		delete(r.canons, key)
		return record, true
	}
	return x.CanonicalRecord{}, false
}

// attachment is one dedicated data connection. Writers require a connection
// with reconnect buffering disabled; the agent's control session buffers, so
// bytes never share it (late replays after reconnect would forge progress).
type attachment struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// OpenData dials a dedicated connection from the current session's own
// authenticated options (name, nkey signer, token, private inbox), with every
// session callback removed: the data plane must not drive session lifecycle.
// origin: hot-update S7 plan §2.3 / §7 / M15
func (r *Runtime) OpenData(id x.Identity, d x.Deadline) (jetstream.JetStream, error) {
	key := x.Key{SID: id.SID, TransferID: id.TransferID}
	r.mu.Lock()
	old, worker := r.data[key], r.worker
	if old != nil && !old.nc.IsClosed() {
		r.mu.Unlock()
		return old.js, nil
	}
	r.mu.Unlock()
	if old != nil {
		if err := xferworker.RetainConsumerCleanup(worker, jsstream.ConsumerCleanupFor(old.nc)); err != nil {
			return nil, err
		}
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, err
	}
	control := r.host.JetStream()
	if control == nil || control.Conn() == nil || control.Conn().ConnectedUrl() == "" {
		return nil, fmt.Errorf("%s: agent control session is not connected", x.CodeOwnerRecovering)
	}
	opts := control.Conn().Opts
	opts.Url, opts.Servers = control.Conn().ConnectedUrl(), nil
	opts.ReconnectBufSize = -1
	opts.Timeout = min(remaining, 5*time.Second)
	opts.RetryOnFailedConnect = false
	opts.ClosedCB, opts.DisconnectedCB, opts.DisconnectedErrCB, opts.ConnectedCB = nil, nil, nil, nil
	opts.ReconnectedCB, opts.DiscoveredServersCB, opts.AsyncErrorCB, opts.ReconnectErrCB = nil, nil, nil, nil
	opts.ReconnectToServerCB, opts.LameDuckModeHandler, opts.CustomReconnectDelayCB = nil, nil, nil
	// The session's CustomDialer is the agent's per-session teardown tracker;
	// dialing through it registers this data socket in the CONTROL session's
	// census, and its bounded registry evicts (closes) the control socket.
	// A data attachment dials directly (nats.go handles ws/wss URLs itself).
	// origin: S7 production activation — the first pull closed the agent's session
	opts.CustomDialer, opts.Dialer = nil, nil
	nc, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if a := r.data[key]; a != nil && !a.nc.IsClosed() {
		nc.Close()
		return a.js, nil
	}
	r.data[key] = &attachment{nc: nc, js: js}
	return js, nil
}

// CloseData retires the entry's dedicated connection after its owners stopped.
func (r *Runtime) CloseData(id x.Identity, _ x.Deadline) error {
	key := x.Key{SID: id.SID, TransferID: id.TransferID}
	r.mu.Lock()
	a, worker := r.data[key], r.worker
	delete(r.data, key)
	r.mu.Unlock()
	if a != nil {
		if err := xferworker.RetainConsumerCleanup(worker, jsstream.ConsumerCleanupFor(a.nc)); err != nil {
			return err
		}
		a.nc.Close()
	}
	return nil
}

// RetainConsumerCleanup records this runtime's attachments before management
// replacement or terminal Stop; binding/join belongs outside the host mutex.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1
func (r *Runtime) RetainConsumerCleanup() error {
	r.mu.Lock()
	worker := r.worker
	var attachments []*attachment
	for _, a := range r.data {
		attachments = append(attachments, a)
	}
	r.mu.Unlock()
	for _, a := range attachments {
		if err := xferworker.RetainConsumerCleanup(worker, jsstream.ConsumerCleanupFor(a.nc)); err != nil {
			return err
		}
	}
	return nil
}

// RebindConsumerCleanup uses the authenticated current management connection
// only after capturing the runtime's own attachment custody.
func RebindConsumerCleanup(r *Runtime, js jetstream.JetStream) error {
	if r == nil {
		return nil
	}
	if err := r.RetainConsumerCleanup(); err != nil {
		return err
	}
	return xferworker.RebindCurrentConsumerCleanup(r.BoundWorker(), js)
}

// StopWithConsumerCleanup joins the same worker and its retained server duties.
// An injected/legacy runtime has no production data attachments to rebind.
func StopWithConsumerCleanup(r *Runtime, manager x.WorkerManager, stop func() error, js jetstream.JetStream) error {
	if r == nil {
		return stop()
	}
	if err := r.RetainConsumerCleanup(); err != nil {
		return err
	}
	return xferworker.StopWithConsumerCleanup(manager, stop, js)
}
