package xferagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferproof"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go"
)

// HandleLink answers one broker→agent continuation control carried on the
// push-commit intake (proto.TransferLinkVerb). It reports false when the
// message carries no link, so the caller keeps the legacy commit path. A link
// never falls through to legacy byte work: every failure is an explicit code.
// origin: hot-update S7 plan §6.3 / §7 / P20
func (r *Runtime) HandleLink(peer LinkPeer, msg *nats.Msg) bool {
	var carrier proto.TransferCommitReq
	if json.Unmarshal(msg.Data, &carrier) != nil || carrier.Link == nil {
		return false
	}
	if msg.Reply == "" {
		return true
	}
	body, err := encodeLinkReply(r.answerLink(&peer, *carrier.Link), peer.MaxPayload)
	if err == nil {
		_ = peer.Publish(msg.Reply, body)
	}
	return true
}

// LinkPeer borrows just this reply's attachment and identity. The NATS owner's
// dialer and resolver contexts cannot become a worker's lifetime through it.
// origin: S7 internal review GATES-PROD F1
type LinkPeer struct {
	MaxPayload int64
	Publish    func(string, []byte) error
	Nkey       string
	Sign       func([]byte) ([]byte, error)
}

// encodeLinkReply bounds an inline answer by the agent connection's payload
// ceiling. The server drops an oversized publish, which the broker could only
// read as sent-unknown; refuse the inline route explicitly instead.
// origin: hot-update S7 plan §2.1 / §8.1 / FIX-INLINE
func encodeLinkReply(result proto.TransferLinkResult, maxPayload int64) ([]byte, error) {
	body, err := json.Marshal(proto.TransferCommitResp{OK: result.Code == "", Code: result.Code, Error: result.Error, Link: &result})
	if err == nil && result.Inline != nil && int64(len(body)) > maxPayload {
		result = proto.TransferLinkResult{Version: result.Version, InstanceID: result.InstanceID, Code: x.CodePayloadUnsupported}
		body, err = json.Marshal(proto.TransferCommitResp{Code: result.Code, Link: &result})
	}
	return body, err
}

func (r *Runtime) answerLink(peer *LinkPeer, link proto.TransferLink) proto.TransferLinkResult {
	worker, version := r.boundWorker()
	result := proto.TransferLinkResult{Version: version, InstanceID: r.host.Instance()}
	if link.Op == proto.TransferLinkProbe {
		result.ReceiverNkey = r.receiverKey(peer.Sign)
		result.MaxPayload = peer.MaxPayload
		if peer.Sign != nil {
			result.AgentNkey = peer.Nkey
			signature, err := xferproof.Sign(peer.Sign, xferproof.Binding(link.CallID, link.Identity.SID, link.Identity.NID, result.InstanceID, result.ReceiverNkey))
			if err != nil {
				result.Code = x.CodeNotOwner
			} else {
				result.BindingSignature = signature
			}
		}
		// A path query is only selection evidence: no worker entry, permit,
		// pinned source ownership or byte publication is created by a probe.
		// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
		if link.Intent != nil && version == x.Version {
			if link.Identity.SID != r.host.SID || link.Identity.NID != r.host.NID() ||
				*link.Intent != (proto.Intent{Verb: "pull", Path: link.Intent.Path}) || link.Intent.Path == "" ||
				link.Permit != nil || link.Budget != nil || link.Grant != nil || link.Settlement != nil ||
				link.Receipt != nil || link.Decision != nil || len(link.Inline) != 0 {
				result.Code = x.CodeIntentConflict
				return result
			}
			vp, err := transferpath.ValidateForRead(link.Intent.Path, r.host.Roots, r.host.Mode)
			if err != nil {
				result.Code = codeOf(err, x.CodeSourceChanged)
				return result
			}
			source, err := transferpath.OpenForReadAtomic(vp)
			if err != nil {
				result.Code = codeOf(err, x.CodeSourceChanged)
				return result
			}
			info, statErr := source.Stat()
			closeErr := source.Close()
			if statErr != nil || closeErr != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > proto.XferMaxBytes {
				result.Code = x.CodeSourceChanged
				return result
			}
			size := info.Size()
			result.SourceSize = &size
		}
		return result
	}
	fail := func(code string, err error) proto.TransferLinkResult {
		result.Code = code
		if err != nil {
			result.Error = code + ": continuation refused at the original agent"
			r.host.Logger.Warn("agent: transfer link refused", "op", link.Op, "transfer_id", link.Identity.TransferID, "code", code, "err", err)
		}
		return result
	}
	id := x.Identity(link.Identity)
	if version != x.Version || worker == nil {
		return fail(x.CodeOwnerRecovering, errors.New("transfer worker unavailable"))
	}
	if id.SID != r.host.SID || id.NID != r.host.NID() || id.AgentInstance != r.host.Instance() || link.CallID == "" {
		return fail(x.CodeIntentConflict, errors.New("link identity is not this agent instance"))
	}
	d, err := controlbudget.After(linkDeadline)
	if err != nil {
		return fail(x.CodeOwnerRecovering, err)
	}
	if link.Intent != nil {
		r.recordIntent(id, x.Intent(*link.Intent))
	}
	var status x.Status
	switch link.Op {
	case proto.TransferLinkPrepare:
		status, err = r.linkPrepare(worker, link, d)
		// A Tier A pull answers with the sender's retained copy and the seal
		// the broker durably acknowledged; a replay of the same prepare gets
		// the same bytes. Waiting happens after linkPrepare released the
		// creation admission, so an image cut is never held behind it.
		// origin: hot-update S7 plan §8.1 / FIX-INLINE (FIX-TIERA report item 3)
		if err == nil && link.Intent.Verb == "pull" && link.Intent.Tier == "a" && status.Terminal == nil {
			result.Source, result.Inline = r.awaitInline(id, d)
		}
	case proto.TransferLinkGrant:
		if link.Grant == nil {
			return fail(x.CodeIntentConflict, errors.New("grant link without grant"))
		}
		// The receiver is created by Start; the durable grant then reaches the
		// original receiver gate; a second Start resumes a runner that parked
		// on "awaits commit grant".
		if err = worker.Start(id); err == nil {
			grant := x.CommitGrant{Identity: x.Identity(link.Grant.Identity), GrantNonce: link.Grant.GrantNonce,
				ReceiverInstance: link.Grant.ReceiverInstance, TerminalID: link.Grant.TerminalID}
			status, err = worker.Apply(x.Control{Kind: x.ControlGrant, Identity: id, Grant: &grant}, d)
		}
		if err == nil {
			err = worker.Start(id)
		}
		if err == nil {
			status, err = worker.Status(id)
		}
	case proto.TransferLinkReceipt:
		if link.Receipt == nil {
			return fail(x.CodeIntentConflict, errors.New("receipt link without receipt"))
		}
		if link.Decision == nil {
			return fail(x.CodeTerminalConflict, errors.New("receipt lacks canonical evidence"))
		}
		if err = xferworker.BindOutcome(worker, x.CanonicalOutcome(*link.Decision), d); err != nil {
			break
		}
		receipt := x.TerminalReceipt(*link.Receipt)
		status, err = worker.Apply(x.Control{Kind: x.ControlReceipt, Identity: id, Receipt: &receipt}, d)
	default:
		return fail(x.CodePayloadUnsupported, fmt.Errorf("unknown link operation %q", link.Op))
	}
	if err != nil {
		return fail(codeOf(err, x.CodeResultUnknown), err)
	}
	result.Status = wireStatus(status)
	return result
}

func (r *Runtime) linkPrepare(worker x.WorkerManager, link proto.TransferLink, d x.Deadline) (x.Status, error) {
	if link.Intent == nil || link.Permit == nil || link.Budget == nil {
		return x.Status{}, errors.New(x.CodeIntentConflict + ": incomplete prepare link")
	}
	id := x.Identity(link.Identity)
	intent := x.Intent(*link.Intent)
	permit := x.CreationPermit{Identity: x.Identity(link.Permit.Identity), Origin: x.Origin(link.Permit.Origin), IssuedAt: link.Permit.IssuedAt,
		CreateUntil: link.Permit.CreateUntil, PermitNonce: link.Permit.PermitNonce, Signature: link.Permit.Signature}
	// A retained preparation owns its original descriptor even after the
	// pathname changes; only first creation validates the path.
	// origin: hot-update S7 plan §4 / P20
	if _, err := worker.Status(id); err != nil {
		if codeOf(err, "") != x.CodeResultUnknown {
			return x.Status{}, err
		}
		var pathErr error
		if intent.Verb == "push" {
			_, pathErr = transferpath.ValidateForWrite(intent.Path, r.host.Roots, r.host.Mode)
		} else {
			_, pathErr = transferpath.ValidateForRead(intent.Path, r.host.Roots, r.host.Mode)
		}
		if pathErr != nil {
			return x.Status{}, pathErr
		}
		// Only first creation consults the agent's upgrade gate; a retained
		// entry's continuation, status and settlement never do. A refusal is
		// retryable and never selects a legacy fallback.
		// origin: hot-update S7 plan §8.4 / P21
		if r.host.Admit != nil {
			release, ok := r.host.Admit()
			if !ok {
				return x.Status{}, errors.New(x.CodeOwnerRecovering + ": agent_upgrading: image switch in progress; retry")
			}
			defer release()
		}
	}
	prepare := x.Prepare{CallID: link.CallID, Identity: id, Origin: x.Origin(link.Origin), Intent: intent, Permit: permit,
		Budget: x.BudgetSeal(*link.Budget), Inline: link.Inline}
	if link.Settlement != nil {
		capsule := x.SettlementCapsule{Identity: x.Identity(link.Settlement.Identity), Origin: x.Origin(link.Settlement.Origin),
			ReceiverNkey: link.Settlement.ReceiverNkey, Issuer: link.Settlement.Issuer, Nonce: link.Settlement.Nonce, Signature: link.Settlement.Signature}
		prepare.Settlement = &capsule
	}
	status, err := worker.Prepare(prepare, d)
	if err != nil {
		return x.Status{}, err
	}
	// A pull sender starts at once; a push receiver waits for the commit grant.
	if intent.Verb == "pull" {
		if err = worker.Start(id); err != nil {
			return x.Status{}, err
		}
		return worker.Status(id)
	}
	return status, nil
}

// receiverKey names the settlement receiver: the agent's own CONNECT nkey, or
// a process-local key in no-auth deployments (where no settlement-only
// CONNECT exists to authenticate and the capsule only binds the courier).
func (r *Runtime) receiverKey(sign func([]byte) ([]byte, error)) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sign != nil {
		key, err := xferproof.InstanceKey(r.host.Instance(), sign)
		if err != nil {
			return ""
		}
		r.receiver.Wipe()
		r.receiver = key
	}
	pub, _ := r.receiver.PublicKey()
	return pub
}

const linkDeadline = 5 * time.Second

// inlineReplyReserve is the part of a prepare link's deadline the agent never
// spends waiting for its inline copy. The broker's send shares a 5s ceiling
// from slightly earlier; replying while it still has time lets it re-ask the
// original agent with the same prepare instead of reading a timeout.
// origin: hot-update S7 plan §6.1 / §8.1 / FIX-INLINE
const inlineReplyReserve = 3 * time.Second

// awaitInline returns the original sender's retained copy and its durable
// seal. The sender seals and copies right after Start; normally the copy
// exists within milliseconds. Absence is reported as absence (the broker
// re-asks), never as an empty or re-read source.
// origin: hot-update S7 plan §8.1 / FIX-INLINE
func (r *Runtime) awaitInline(id x.Identity, d x.Deadline) (*proto.SourceSeal, []byte) {
	for {
		remaining, err := controlbudget.Remaining(d)
		if err != nil || remaining <= inlineReplyReserve {
			return nil, nil
		}
		poll, err := controlbudget.After(min(remaining-inlineReplyReserve, 100*time.Millisecond))
		if err != nil {
			return nil, nil
		}
		if seal, data, ok := r.inlineOf(id, poll); ok {
			wire := proto.SourceSeal(seal)
			return &wire, data
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// codeOf recovers a stable transfer code from an owner error; the agent never
// forwards a dependency's diagnostic text (paths, capsules) to the broker.
func codeOf(err error, fallback string) string {
	var pathErr *transferpath.PathValidationError
	if errors.As(err, &pathErr) {
		return pathErr.Code
	}
	return proto.TransferCodeOf(err, fallback)
}

func wireStatus(s x.Status) *proto.TransferStatusResp {
	out := &proto.TransferStatusResp{State: proto.StatusState(s.State), Phase: proto.Phase(s.Phase), Identity: proto.Identity(s.Identity),
		Origin: proto.Origin(s.Origin), Budget: proto.BudgetSeal(s.Budget), DurableOffset: s.DurableOffset, ChunkIndex: s.ChunkIndex,
		LastConsumedStreamSeq: s.LastConsumedStreamSeq, CleanupPending: s.CleanupPending, Code: s.Code, Error: s.Error}
	if s.Terminal != nil {
		receipt := proto.TerminalReceipt(*s.Terminal)
		out.Terminal = &receipt
	}
	return out
}
