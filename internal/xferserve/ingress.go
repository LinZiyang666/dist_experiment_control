package xferserve

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/nats-io/nats.go"
)

// Graph is the installed controller view the ingress borrows for one call.
type Graph struct {
	Controller x.Controller
	Authority  xferauth.ControlAuthority
	Tracker    x.TrackerManager
	Rows       Rows
	Link       *Link
	Decisions  x.DecisionPort
}

// AgentRequest answers one agent-role operation on the agent's authenticated
// subtree. The subject's (sid,nid) is proven by the agent's NATS ACL; the
// principal's instance is the RETAINED entry's original agent instance, never
// a body claim, so a request naming another instance is refused by the
// controller's principal check. silent=true means another origin owns it.
// origin: hot-update S7 plan §6.3 / T39 / INTEG-A trusted agent binding
func AgentRequest(g Graph, gate func(x.Origin) bool, sid, nid, tid string, op x.Operation, msg *nats.Msg, d x.Deadline) (reply []byte, silent bool) {
	req, err := xfercontrol.DecodeWire(op, msg.Data, x.Identity{SID: sid, NID: nid, TransferID: tid})
	if err != nil || !gate(req.Origin) {
		return nil, true // malformed scope, or another origin, is not response authority
	}
	if g.Controller == nil || g.Tracker == nil {
		return wire(op, x.Response{Code: x.CodeOwnerRecovering, Error: x.CodeOwnerRecovering}, nil), false
	}
	status, err := g.Tracker.Status(x.StatusRequest{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}, d)
	if err != nil || status.State == x.StatusUnknown {
		resp := x.Response{Code: x.CodeNotOwner, Error: x.CodeNotOwner}
		if err != nil {
			resp = x.Response{Code: codeOf(err), Error: codeOf(err)}
		}
		return wire(op, resp, nil), false
	}
	retained := status.Identity
	if retained.SID != sid || retained.NID != nid || retained.TransferID != tid {
		return wire(op, x.Response{Code: x.CodeNotOwner, Error: x.CodeNotOwner}, nil), false
	}
	who, authErr := g.Link.AgentPrincipal(retained, msg, d)
	if authErr != nil {
		return wire(op, x.Response{Code: x.CodeNotOwner, Error: x.CodeNotOwner}, nil), false
	}
	resp, err := g.Controller.Handle(req, who, d)
	if errors.Is(err, xfercontrol.ErrNotOwner) {
		return nil, true
	}
	var canonical *proto.TransferCanonical
	if op == x.OpTerminal && resp.Receipt != nil {
		if row, rowErr := g.Rows(retained, d); rowErr == nil && row.CanonicalTerminal != nil &&
			row.CanonicalTerminal.Digest == resp.Receipt.CanonicalDigest {
			rec := row.CanonicalTerminal
			canonical = &proto.TransferCanonical{Kind: rec.Kind, ReqID: rec.ReqID, Digest: rec.Digest, Bytes: append([]byte(nil), rec.Bytes...)}
		}
	}
	return wire(op, resp, canonical), false
}

// Carrier answers a capable push/pull prepare or push-commit arriving on the
// legacy command subjects (proto.EncodeTransferRequest's carriers). The actor
// is the NATS-proven subject token. A commit is the receiver reservation for
// the ORIGINAL receiving agent; its durable grant is delivered to that agent
// before the reply, and is never a licence to run a legacy primitive.
// handled=false means the body carries no continuation (legacy path).
// origin: hot-update S7 plan §6.2 / §7 / INTEG-D carrier request
func Carrier(g Graph, gate func(x.Origin) bool, actor, sid, nid, verb string, body []byte, d x.Deadline) (reply []byte, handled, silent bool) {
	var marker struct {
		TransferID   string          `json:"transfer_id"`
		Continuation json.RawMessage `json:"continuation"`
	}
	if json.Unmarshal(body, &marker) != nil || len(marker.Continuation) == 0 || string(marker.Continuation) == "null" {
		return nil, false, false
	}
	op := x.OpPrepare
	if verb == "push-commit" {
		op = x.OpCommit
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return nil, true, true
	}
	req, err := proto.DecodeTransferRequest(fp, sid, nid, marker.TransferID, op, verb, body)
	if err != nil {
		return carrierRefusal(verb, x.CodeIntentConflict), true, false
	}
	if !gate(req.Origin) {
		return nil, true, true // the retained origin answers (plain broadcast)
	}
	// A capable body never falls through to legacy byte work, even while the
	// graph is recovering: the ctl must reconcile, not re-upload.
	// origin: hot-update S7 plan §7 / M01
	if g.Controller == nil {
		return encodeCarrier(req, x.Response{Code: x.CodeOwnerRecovering, Error: x.CodeOwnerRecovering}), true, false
	}
	who, err := g.Authority.Principal(actor, req.Identity, d)
	if err != nil {
		return encodeCarrier(req, x.Response{Code: x.CodeNotOwner, Error: x.CodeNotOwner}), true, false
	}
	// The authenticated subject key names a ctl receiver (the broker's ctl
	// subtree binds the same way); a body string never does.
	if who.Role == x.RoleCTL {
		who.InstanceID = actor
	}
	if op == x.OpCommit {
		req.Reserve = &x.ReserveRequest{CallID: req.CallID, Origin: req.Origin, ReceiverInstance: req.Identity.AgentInstance}
	}
	resp, err := g.Controller.Handle(req, who, d)
	if errors.Is(err, xfercontrol.ErrNotOwner) {
		return nil, true, true
	}
	if op == x.OpCommit && err == nil && resp.Grant != nil {
		resp = deliverGrant(g, req, *resp.Grant, d)
	}
	if op == x.OpPrepare && err == nil && req.Intent != nil && req.Intent.Verb == "pull" && g.Link != nil {
		capsule, capErr := g.Link.ReceiverCapsule(req.Identity, req.Origin, actor, d)
		if capErr != nil {
			return encodeCarrier(req, x.Response{Code: codeOf(capErr), Error: codeOf(capErr)}), true, false
		}
		resp.Settlement = &capsule
	}
	return encodeCarrier(req, resp), true, false
}

func deliverGrant(g Graph, req x.Request, grant x.CommitGrant, d x.Deadline) x.Response {
	row, err := g.Rows(req.Identity, d)
	if err != nil {
		return x.Response{Code: codeOf(err), Error: codeOf(err)}
	}
	status, _, err := g.Link.DeliverGrant(req.Identity, req.Origin, row.Intent, grant, d)
	if err != nil {
		// The grant is durable at the tracker; a retry of the same CallID gets
		// the same grant and redelivers it. Never report this as success.
		return x.Response{Code: codeOf(err), Error: codeOf(err)}
	}
	if status.Identity != req.Identity || status.Origin != req.Origin {
		return x.Response{Code: x.CodeResultUnknown, Error: x.CodeResultUnknown}
	}
	return x.Response{Status: &status}
}

func encodeCarrier(req x.Request, resp x.Response) []byte {
	b, err := proto.EncodeTransferResponse(req, resp)
	if err != nil {
		return carrierRefusal(map[x.Operation]string{x.OpCommit: "push-commit"}[req.Op], x.CodeResultUnknown)
	}
	return b
}

func carrierRefusal(verb, code string) []byte {
	if verb == "push-commit" {
		return marshal(proto.TransferCommitResp{Code: code, Error: code})
	}
	return marshal(proto.PushPrepareResp{Code: code, Error: code})
}

func wire(op x.Operation, resp x.Response, canonical *proto.TransferCanonical) []byte {
	out, err := xfercontrol.WireResponse(op, resp)
	if err != nil {
		return nil
	}
	if terminal, ok := out.(proto.TransferTerminalResp); ok {
		terminal.Canonical = canonical
		out = terminal
	}
	return marshal(out)
}

func codeOf(err error) string { return proto.TransferCodeOf(err, x.CodeResultUnknown) }

// DispatchAgent binds the subject and one deadline before authenticating the retained instance.
// origin: hot-update S7 plan §6.3 / T39
func DispatchAgent(g Graph, gate func(x.Origin) bool, reg xfercontrol.Registration, msg *nats.Msg) []byte {
	sid, nid, tid, op, ok := proto.ParseEvTransferRequest(msg.Subject)
	if !ok || op != reg.Operation || msg.Reply == "" {
		return nil
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return nil
	}
	reply, silent := AgentRequest(g, gate, sid, nid, tid, op, msg, d)
	if silent {
		return nil
	}
	return reply
}
