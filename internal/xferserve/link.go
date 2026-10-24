package xferserve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferproof"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// LegacyAgentInstance names an agent that authentically answered cap0 without
// an instance identity. It satisfies the probe's token shape and is never
// admitted as a continuation owner (no permit is minted for cap0).
const LegacyAgentInstance = "legacy-agent"

// Link is the broker→agent side of the continuation link and the controller's
// Transport port. The agent's reply on its directed node intake is the only
// capability evidence: a timeout is never cap0, and an authentic legacy
// transfer_unknown refusal is the only cap0 answer.
// origin: hot-update S7 plan §6.3 / §7 / P16 / P22
type Link struct {
	Capsules          *CapsuleStore
	ClusterProbe      func(x.Deadline) error
	Retirement        *RetirementJournal
	JS                jetstream.JetStream
	Conn              func() *nats.Conn
	Status            func(x.StatusRequest, x.Deadline) (x.Status, error)
	Rows              Rows
	Origin            x.Origin
	Issuer            nkeys.KeyPair
	AuthenticateAgent func(string, string, string) error

	mu   sync.Mutex
	keys map[string]string // sid/nid/instance → agent settlement key from the last probe
}

var _ x.Transport = (*Link)(nil)

func (l *Link) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	switch req.Op {
	case x.OpProbe:
		return l.probe(req, d)
	case x.OpPrepare:
		return l.prepare(req, d)
	// Explicit refusals keep the closed operation family exhaustive.
	// origin: S7 internal review GATE-CONVERGE-contracts F1
	case x.OpPermit, x.OpStatus, x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpCancel, x.OpFinalize, x.OpCensus, x.OpBucketConfig:
		return x.Response{}, x.Attempt{}, fmt.Errorf("%s: broker link has no %s operation", x.CodePayloadUnsupported, req.Op)
	default:
		return x.Response{}, x.Attempt{}, fmt.Errorf("%s: unknown broker link operation %s", x.CodePayloadUnsupported, req.Op)
	}
}

// The broker never owns a data attachment.
func (l *Link) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New(x.CodePayloadUnsupported)
}
func (l *Link) CloseData(x.Identity, x.Deadline) error { return nil }

func (l *Link) probe(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	query := proto.TransferLink{Op: proto.TransferLinkProbe, CallID: probeCallID(req.CallID),
		Identity: proto.Identity{SID: req.Identity.SID, NID: req.Identity.NID, TransferID: req.Identity.TransferID}}
	if req.Intent != nil {
		// The controller admitted only a path-only pull query; forward exactly
		// that read-only inspection to the original agent.
		// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
		intent := proto.Intent(*req.Intent)
		query.Intent = &intent
	}
	result, legacy, attempt, err := l.send(req.Identity.SID, req.Identity.NID, query, d)
	if err != nil {
		return x.Response{}, attempt, err
	}
	if legacy {
		return x.Response{Probe: &x.ProbeResult{InstanceID: LegacyAgentInstance}}, attempt, nil
	}
	if result.Code != "" {
		return x.Response{Code: result.Code}, attempt, nil
	}
	if result.Version != x.Version {
		// A current-tree agent without a working worker is an explicit cap0.
		instance := result.InstanceID
		if instance == "" {
			instance = LegacyAgentInstance
		}
		return x.Response{Probe: &x.ProbeResult{InstanceID: instance}}, attempt, nil
	}
	if l.AuthenticateAgent != nil {
		if err := l.AuthenticateAgent(req.Identity.SID, req.Identity.NID, result.AgentNkey); err != nil {
			return x.Response{}, attempt, err
		}
		if err := xferproof.Verify(result.AgentNkey, result.BindingSignature, xferproof.Binding(probeCallID(req.CallID), req.Identity.SID, req.Identity.NID, result.InstanceID, result.ReceiverNkey)); err != nil {
			return x.Response{}, attempt, errors.New(x.CodeNotOwner)
		}
	}
	if l.ClusterProbe != nil {
		if err := l.ClusterProbe(d); err != nil {
			return x.Response{}, attempt, err
		}
	}
	l.mu.Lock()
	if l.keys == nil {
		l.keys = map[string]string{}
	}
	l.keys[req.Identity.SID+"/"+req.Identity.NID+"/"+result.InstanceID] = result.ReceiverNkey
	l.mu.Unlock()
	limit := int64(0)
	if nc := l.Conn(); nc != nil {
		limit = nc.MaxPayload()
	}
	if result.MaxPayload > 0 && (limit <= 0 || result.MaxPayload < limit) {
		limit = result.MaxPayload
	}
	response := x.Response{Probe: &x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version,
		InstanceID: result.InstanceID, MaxPayload: limit, MaxMsgSize: limit}}
	// Inspection is advisory and precedes permit/worker creation. The actual
	// preparation independently pins its source and seals the original budget.
	// Without this answer every automatic capable pull refused at selection.
	// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
	if req.Intent != nil {
		if result.SourceSize == nil || *result.SourceSize < 0 || *result.SourceSize > proto.XferMaxBytes {
			return x.Response{}, attempt, errors.New(x.CodePayloadUnsupported)
		}
		response.Source = &x.SourceSeal{Size: *result.SourceSize}
	}
	return response, attempt, nil
}

// prepare forwards the admitted entry with its durable original budget and a
// broker-signed settlement capsule naming the receiving agent's key (push).
func (l *Link) prepare(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	if req.Identity.ObjectVersion != "" && l.Retirement != nil {
		if err := l.Retirement.Pin(req.Identity, func() (x.ObjectSpec, error) {
			return retiredSpec(l.JS, x.LedgerRow{Identity: req.Identity, Intent: *req.Intent}, d)
		}); err != nil {
			return x.Response{}, x.Attempt{}, err
		}
	}
	if req.Intent == nil || req.Permit == nil {
		return x.Response{}, x.Attempt{}, errors.New(x.CodeIntentConflict)
	}
	status, err := l.Status(x.StatusRequest{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}, d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	intent, permit, budget := proto.Intent(*req.Intent), wirePermit(*req.Permit), proto.BudgetSeal(status.Budget)
	link := proto.TransferLink{Op: proto.TransferLinkPrepare, CallID: req.CallID, Identity: proto.Identity(req.Identity), Origin: proto.Origin(req.Origin),
		Intent: &intent, Permit: &permit, Budget: &budget, Inline: req.Inline}
	if req.Intent.Verb == "push" {
		capsule, err := l.capsule(req.Identity, req.Origin, d)
		if err != nil {
			return x.Response{}, x.Attempt{}, err
		}
		link.Settlement = &capsule
	}
	result, legacy, attempt, err := l.send(req.Identity.SID, req.Identity.NID, link, d)
	if err != nil {
		return x.Response{}, attempt, err
	}
	if legacy {
		return x.Response{Code: x.CodePayloadUnsupported}, attempt, nil
	}
	if result.Code != "" || result.Status == nil {
		code := result.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		return x.Response{Code: code}, attempt, nil
	}
	s := contractStatus(result.Status)
	inline := req.Intent.Verb == "pull" && req.Intent.Tier == "a"
	if !inline && (result.Inline != nil || result.Source != nil) {
		// Only a Tier A pull's prepare carries inline evidence.
		return x.Response{Code: x.CodeResultUnknown}, attempt, nil
	}
	if req.Intent.Verb != "pull" {
		return x.Response{Status: &s}, attempt, nil
	}
	if inline {
		return l.inlinePrepare(req, link, result, attempt, d)
	}
	// A pull prepare answers with the original agent's durable source seal
	// (the ctl binds it independently of object metadata). The sender seals
	// right after Start; wait for the broker's own sealed row within this
	// request's deadline. Without it the ctl reconciles and replays the same
	// prepare, which re-forwards here while the row is still open.
	// origin: hot-update S7 plan §1.2 / §6.1 / INTEG-D acceptPrepared
	for {
		sealed, err := l.Status(x.StatusRequest{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}, d)
		if err == nil && sealed.Budget.SourceSealed && l.Rows != nil {
			if row, rowErr := l.Rows(req.Identity, d); rowErr == nil && row.SourceSeal != nil {
				seal := *row.SourceSeal
				return x.Response{Status: &sealed, Source: &seal}, attempt, nil
			}
		}
		remaining, err := controlbudget.Remaining(d)
		if err != nil || remaining < 300*time.Millisecond {
			return x.Response{Status: &s}, attempt, nil //nolint:nilerr // The admitted status remains valid after source-seal polling expires; the caller reconciles its pending seal.
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// inlineReask is the least remaining deadline for asking the original agent
// again: its reply reserve (xferagent inlineReplyReserve) leaves it at most
// two seconds of waiting inside its own five-second link deadline.
// origin: hot-update S7 plan §6.1 / §8.1 / FIX-INLINE
const inlineReask = 2500 * time.Millisecond

// inlinePrepare answers a Tier A pull with the original agent's retained copy
// and the source seal the tracker durably recorded from that same sender. The
// ctl can land nothing else: a copy that differs from the durable seal is
// source_changed, and a copy not yet retained is asked for again with the
// same prepare (the agent's replay path returns the same bytes, never a
// second worker) until this request's deadline, after which the sent prepare
// stays unknown for the ctl to reconcile. A status-only answer would make the
// ctl refuse the route as an unsealed source instead.
// origin: hot-update S7 plan §6.1 / §8.1 / FIX-INLINE (FIX-TIERA report item 3)
func (l *Link) inlinePrepare(req x.Request, link proto.TransferLink, result proto.TransferLinkResult, attempt x.Attempt, d x.Deadline) (x.Response, x.Attempt, error) {
	for {
		if result.Source != nil {
			seal, sum := x.SourceSeal(*result.Source), sha256.Sum256(result.Inline)
			// An empty source's copy is omitted on the wire: its length and
			// digest, not the slice's presence, bind it to the seal.
			if seal.Size > proto.XferTierAMaxBytes || seal.ChunkSize != 0 || seal.Chunks != 0 || int64(len(result.Inline)) != seal.Size ||
				hex.EncodeToString(sum[:]) != seal.SHA256 {
				return x.Response{Code: x.CodeSourceChanged}, attempt, nil
			}
			sealed, err := l.Status(x.StatusRequest{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin}, d)
			if err == nil && sealed.Budget.SourceSealed && l.Rows != nil {
				if row, rowErr := l.Rows(req.Identity, d); rowErr == nil && row.SourceSeal != nil {
					if *row.SourceSeal != seal {
						return x.Response{Code: x.CodeSourceChanged}, attempt, nil
					}
					return x.Response{Status: &sealed, Source: &seal, Inline: result.Inline}, attempt, nil
				}
			}
		} else if result.Inline != nil {
			return x.Response{Code: x.CodeSourceChanged}, attempt, nil
		}
		remaining, err := controlbudget.Remaining(d)
		if err != nil || remaining < 300*time.Millisecond || result.Source == nil && remaining < inlineReask {
			attempt.Sent = true
			return x.Response{}, attempt, fmt.Errorf("%s: original agent has not retained the inline source", x.CodeResultUnknown)
		}
		time.Sleep(50 * time.Millisecond)
		if result.Source != nil {
			continue // the copy is present; only the durable row read lags
		}
		next, legacy, nextAttempt, err := l.send(req.Identity.SID, req.Identity.NID, link, d)
		if err != nil {
			return x.Response{}, nextAttempt, err
		}
		if legacy {
			return x.Response{Code: x.CodePayloadUnsupported}, nextAttempt, nil
		}
		if next.Code != "" || next.Status == nil {
			code := next.Code
			if code == "" {
				code = x.CodeResultUnknown
			}
			return x.Response{Code: code}, nextAttempt, nil
		}
		result, attempt = next, nextAttempt
	}
}

// capsule signs the receiver binding with the broker's retained issuer. The
// key is the one the original agent instance itself reported on its probe.
func (l *Link) capsule(id x.Identity, origin x.Origin, d x.Deadline) (proto.SettlementCapsule, error) {
	if l.Capsules != nil {
		if old, err := l.Capsules.load(id); err == nil {
			checked, err := xferauth.SignCapsule(l.Issuer, old)
			if err != nil || checked != old || old.Origin != origin {
				return proto.SettlementCapsule{}, xferauth.ErrDenied
			}
			return wireCapsule(old), nil
		} else if !os.IsNotExist(err) {
			return proto.SettlementCapsule{}, err
		}
	}
	key := id.SID + "/" + id.NID + "/" + id.AgentInstance
	l.mu.Lock()
	receiver := l.keys[key]
	l.mu.Unlock()
	if receiver == "" {
		if _, _, err := l.probe(x.Request{Op: x.OpProbe, CallID: id.TransferID + "-capsule", Identity: id}, d); err != nil {
			return proto.SettlementCapsule{}, err
		}
		l.mu.Lock()
		receiver = l.keys[key]
		l.mu.Unlock()
	}
	signed, err := l.ReceiverCapsule(id, origin, receiver, d)
	if err != nil {
		return proto.SettlementCapsule{}, fmt.Errorf("%s: settlement capsule: %w", x.CodeOwnerRecovering, err)
	}
	return wireCapsule(signed), nil
}

func wireCapsule(c x.SettlementCapsule) proto.SettlementCapsule {
	return proto.SettlementCapsule{Identity: proto.Identity(c.Identity), Origin: proto.Origin(c.Origin), ReceiverNkey: c.ReceiverNkey, Issuer: c.Issuer, Nonce: c.Nonce, Signature: c.Signature}
}

func (l *Link) ReceiverCapsule(id x.Identity, origin x.Origin, receiver string, d x.Deadline) (x.SettlementCapsule, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return x.SettlementCapsule{}, err
	}
	c, err := xferauth.SignCapsule(l.Issuer, x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: receiver, Nonce: nuid.Next()})
	if err != nil || l.Capsules == nil {
		return c, err
	}
	return l.Capsules.retain(c)
}

// DeliverGrant hands the durable reservation to the original receiving agent
// and returns that agent's retained status. A lost reply is reconciled by the
// caller replaying the SAME grant: the receiver accepts only the first one.
func (l *Link) DeliverGrant(id x.Identity, origin x.Origin, intent x.Intent, grant x.CommitGrant, d x.Deadline) (x.Status, x.Attempt, error) {
	wi, wg := proto.Intent(intent), proto.CommitGrant{Identity: proto.Identity(grant.Identity), GrantNonce: grant.GrantNonce,
		ReceiverInstance: grant.ReceiverInstance, TerminalID: grant.TerminalID}
	return l.control(proto.TransferLink{Op: proto.TransferLinkGrant, CallID: "xfer-grant-" + id.TransferID, Identity: proto.Identity(id),
		Origin: proto.Origin(origin), Intent: &wi, Grant: &wg}, d)
}

// DeliverReceipt tells an original sending agent that the exact history
// receipt exists, so it retires its writer and pinned source.
func (l *Link) DeliverReceipt(id x.Identity, origin x.Origin, receipt x.TerminalReceipt, d x.Deadline) (x.Status, x.Attempt, error) {
	wr := proto.TerminalReceipt(receipt)
	if l.Rows == nil {
		return x.Status{}, x.Attempt{}, errors.New(x.CodeOwnerRecovering)
	}
	row, err := l.Rows(id, d)
	if err != nil {
		return x.Status{}, x.Attempt{}, err
	}
	if row.ReceiverOutcome == nil || row.CanonicalTerminal == nil {
		return x.Status{}, x.Attempt{}, errors.New(x.CodeOwnerRecovering)
	}
	decision := proto.TransferDecision{Identity: id, Outcome: *row.ReceiverOutcome, Canonical: *row.CanonicalTerminal, Receipt: &receipt}
	return l.control(proto.TransferLink{Op: proto.TransferLinkReceipt, CallID: "xfer-receipt-" + id.TransferID, Identity: proto.Identity(id),
		Origin: proto.Origin(origin), Receipt: &wr, Decision: &decision}, d)
}

func (l *Link) control(link proto.TransferLink, d x.Deadline) (x.Status, x.Attempt, error) {
	result, legacy, attempt, err := l.send(link.Identity.SID, link.Identity.NID, link, d)
	if err != nil {
		return x.Status{}, attempt, err
	}
	if legacy {
		return x.Status{}, attempt, errors.New(x.CodePayloadUnsupported)
	}
	if result.Code != "" || result.Status == nil {
		code := result.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		return x.Status{}, attempt, xferauth.ControlRefusal(code)
	}
	return contractStatus(result.Status), attempt, nil
}

func (l *Link) send(sid, nid string, link proto.TransferLink, d x.Deadline) (proto.TransferLinkResult, bool, x.Attempt, error) {
	start, _ := controlbudget.Now()
	attempt := x.Attempt{LogicalID: link.CallID, Ordinal: 1, Phase: controlbudget.PhaseRequest, StartedMonoNS: start}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return proto.TransferLinkResult{}, false, attempt, err
	}
	nc := l.Conn()
	if nc == nil || !nc.IsConnected() {
		return proto.TransferLinkResult{}, false, attempt, fmt.Errorf("%s: broker connection unavailable", x.CodeOwnerRecovering)
	}
	body, err := proto.EncodeTransferLink(link)
	if err != nil {
		return proto.TransferLinkResult{}, false, attempt, err
	}
	// ctx-none: the frozen Transport port has an absolute deadline, no caller ctx.
	ctx, cancel := context.WithTimeout(context.Background(), min(remaining, 5*time.Second))
	defer cancel()
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return proto.TransferLinkResult{}, false, attempt, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = nc.PublishRequest(proto.SubjCmdForwarded(sid, nid, proto.TransferLinkVerb), inbox, body); err != nil {
		attempt.Class = "publish"
		return proto.TransferLinkResult{}, false, attempt, err
	}
	attempt.Sent = true
	msg, err := sub.NextMsgWithContext(ctx)
	attempt.FinishedMonoNS, _ = controlbudget.Now()
	if err != nil {
		attempt.Class = "reply"
		return proto.TransferLinkResult{}, false, attempt, errors.Join(errors.New(x.CodeResultUnknown), err)
	}
	if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
		attempt.Sent, attempt.Class = false, "no_responders"
		return proto.TransferLinkResult{}, false, attempt, fmt.Errorf("%s: agent is not subscribed", x.CodeOwnerRecovering)
	}
	result, legacy, err := proto.DecodeTransferLinkResult(msg.Data)
	return result, legacy, attempt, err
}

func probeCallID(callID string) string {
	if callID == "" {
		return "xfer-probe"
	}
	return callID
}

func wirePermit(p x.CreationPermit) proto.CreationPermit {
	return proto.CreationPermit{Identity: proto.Identity(p.Identity), Origin: proto.Origin(p.Origin), IssuedAt: p.IssuedAt,
		CreateUntil: p.CreateUntil, PermitNonce: p.PermitNonce, Signature: p.Signature}
}

func contractStatus(s *proto.TransferStatusResp) x.Status {
	out := x.Status{State: x.StatusState(s.State), Phase: x.Phase(s.Phase), Identity: x.Identity(s.Identity), Origin: x.Origin(s.Origin),
		Budget: x.BudgetSeal(s.Budget), DurableOffset: s.DurableOffset, ChunkIndex: s.ChunkIndex, LastConsumedStreamSeq: s.LastConsumedStreamSeq,
		CleanupPending: s.CleanupPending, Code: s.Code, Error: s.Error}
	if s.Terminal != nil {
		receipt := x.TerminalReceipt(*s.Terminal)
		out.Terminal = &receipt
	}
	return out
}

// marshal is a small helper so callers can embed raw JSON without a second encoder.
func marshal(v any) []byte { b, _ := json.Marshal(v); return b }

// AgentPrincipal accepts only possession of the key bound by the node's
// authenticated probe. A body claim and a forged inbox carry no authority.
func (l *Link) AgentPrincipal(id x.Identity, msg *nats.Msg, d x.Deadline) (x.Principal, error) {
	if l == nil || msg == nil {
		return x.Principal{}, errors.New(x.CodeNotOwner)
	}
	k := id.SID + "/" + id.NID + "/" + id.AgentInstance
	l.mu.Lock()
	pub := l.keys[k]
	l.mu.Unlock()
	if pub == "" {
		if _, _, err := l.probe(x.Request{CallID: "auth-" + id.TransferID, Identity: id}, d); err != nil {
			return x.Principal{}, err
		}
		l.mu.Lock()
		pub = l.keys[k]
		l.mu.Unlock()
	}
	if pub == "" || msg.Header.Get(xferproof.KeyHeader) != pub || xferproof.Verify(pub, msg.Header.Get(xferproof.SignatureHeader), xferproof.Message(msg.Subject, msg.Reply, msg.Data)) != nil {
		return x.Principal{}, errors.New(x.CodeNotOwner)
	}
	return x.Principal{NKey: pub, FP: id.ActorFP, SID: id.SID, NID: id.NID, InstanceID: id.AgentInstance, Role: x.RoleAgent}, nil
}
