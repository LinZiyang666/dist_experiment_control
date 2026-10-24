package proto

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// EncodeAgentTransferCarrier builds the deciding broker's directed delivery to
// the original agent on its existing node CMD subjects (push/pull/push-commit
// .req.forwarded). Only that broker can publish there, so the stamped actor,
// the admitted budget, the sealed source and the durable Grant are broker
// authority; a ctl request carrying any of them is refused by
// DecodeTransferRequest. A prepare carries the original permit and intent; a
// commit carries the Grant the broker already persisted for this receiver,
// plus the sealed source geometry the receiver must read.
// origin: hot-update S7 plan §6.3 (broker-to-agent reuses the targeted node command) / INTEG-D
func EncodeAgentTransferCarrier(r xfervalue.Request, budget xfervalue.BudgetSeal, source *xfervalue.SourceSeal, grant *xfervalue.CommitGrant) (verb string, body []byte, err error) {
	if err = validateTransferRequestUnion(r); err != nil {
		return "", nil, err
	}
	if budget.StartedAt.IsZero() || budget.EffectiveBudgetNS <= 0 || budget.RemainingNS <= 0 {
		return "", nil, errors.New("agent transfer carrier has no admitted budget")
	}
	id, origin := Identity(r.Identity), Origin(r.Origin)
	cont := &TransferContinuation{Version: TransferVersion, Identity: id, Origin: origin, Budget: BudgetSeal(budget)}
	var value any
	switch r.Op {
	case xfervalue.OpPrepare:
		if source != nil || grant != nil {
			return "", nil, errors.New("agent prepare carrier has commit-only fields")
		}
		intent := Intent(*r.Intent)
		cont.Permit, cont.Intent = wirePermit(r.Permit), &intent
		verb = intent.Verb
		if verb == "push" {
			value = PushPrepareReq{CallID: r.CallID, TransferID: id.TransferID, Path: intent.Path, Size: intent.Size, SHA256: intent.SHA256,
				Force: intent.Force, Tier: intent.Tier, ChunkSize: int(intent.ChunkSize), InlineData: bytes.Clone(r.Inline),
				ActorFP: id.ActorFP, TransferVersion: TransferVersion, Continuation: cont}
		} else {
			inline := int64(0)
			if intent.Tier == "a" {
				inline = XferTierAMaxBytes
			}
			value = PullPrepareReq{CallID: r.CallID, TransferID: id.TransferID, Path: intent.Path, Force: intent.Force, MaxInline: inline,
				ActorFP: id.ActorFP, TransferVersion: TransferVersion, Continuation: cont}
		}
	case xfervalue.OpCommit:
		if grant == nil || grant.Identity != r.Identity {
			return "", nil, errors.New("agent commit carrier has no matching durable grant")
		}
		g := wireGrant(*grant)
		cont.Grant = &g
		if source != nil {
			s := SourceSeal(*source)
			cont.Source = &s
		}
		verb = "push-commit"
		value = TransferCommitReq{CallID: r.CallID, TransferID: id.TransferID, Bucket: XferBucketName(id.SID), ObjectKey: id.TransferID,
			ActorFP: id.ActorFP, Continuation: cont}
	case xfervalue.OpProbe, xfervalue.OpPermit, xfervalue.OpStatus, xfervalue.OpSource, xfervalue.OpReserve, xfervalue.OpTerminal, xfervalue.OpCancel, xfervalue.OpFinalize, xfervalue.OpCensus, xfervalue.OpBucketConfig:
		// Only prepare and the Grant-bearing commit are directed deliveries.
		return "", nil, errors.New("unsupported agent transfer carrier")
	default:
		return "", nil, errors.New("unsupported agent transfer carrier")
	}
	if err = ValidateTransferWire(value); err != nil {
		return "", nil, err
	}
	body, err = json.Marshal(value)
	return verb, body, err
}

// DecodeAgentTransferCarrier runs at the agent. sid/nid come from the agent's
// own subscription scope; the actor is the broker-stamped field, which must
// equal the retained identity's actor. A ctl cannot reach this subject.
func DecodeAgentTransferCarrier(sid, nid, verb string, body []byte) (r xfervalue.Request, budget xfervalue.BudgetSeal, source *xfervalue.SourceSeal, grant *xfervalue.CommitGrant, err error) {
	switch verb {
	case "push", "pull":
		var peek struct {
			TransferID   string                `json:"transfer_id"`
			ActorFP      string                `json:"actor_fp"`
			Continuation *TransferContinuation `json:"continuation"`
		}
		if json.Unmarshal(body, &peek) != nil || peek.Continuation == nil || peek.ActorFP == "" {
			return r, budget, nil, nil, errors.New(CodeTransferIntentConflict)
		}
		if r, err = DecodeTransferRequest(peek.ActorFP, sid, nid, peek.TransferID, xfervalue.OpPrepare, verb, body); err != nil {
			return xfervalue.Request{}, budget, nil, nil, err
		}
		budget = xfervalue.BudgetSeal(peek.Continuation.Budget)
	case "push-commit":
		var p TransferCommitReq
		if err = DecodeTransferWire(body, &p); err != nil {
			return r, budget, nil, nil, err
		}
		c := p.Continuation
		if c == nil || c.Grant == nil || c.Intent != nil || c.Permit != nil || c.Settlement != nil || p.ActorFP != c.Identity.ActorFP ||
			c.Identity.SID != sid || c.Identity.NID != nid || p.TransferID != c.Identity.TransferID ||
			p.Bucket != XferBucketName(sid) || p.ObjectKey != p.TransferID || !validTransferToken(p.CallID) {
			return r, budget, nil, nil, errors.New(CodeTransferIntentConflict)
		}
		g := contractGrant(*c.Grant)
		r = xfervalue.Request{Op: xfervalue.OpCommit, CallID: p.CallID, Identity: xfervalue.Identity(c.Identity), Origin: xfervalue.Origin(c.Origin)}
		budget, grant = xfervalue.BudgetSeal(c.Budget), &g
		if c.Source != nil {
			s := xfervalue.SourceSeal(*c.Source)
			source = &s
		}
	default:
		return r, budget, nil, nil, errors.New("unknown agent transfer carrier verb")
	}
	if budget.StartedAt.IsZero() || budget.EffectiveBudgetNS <= 0 || budget.RemainingNS <= 0 || budget.RemainingNS > budget.EffectiveBudgetNS {
		return xfervalue.Request{}, xfervalue.BudgetSeal{}, nil, nil, errors.New(CodeTransferBudgetExceeded)
	}
	return r, budget, source, grant, nil
}

// EncodeAgentTransferRequest builds an agent-originated continuation request on
// the authenticated agent subtree. Only source/reserve/terminal/cancel exist
// there; the subject carries no actor, so the broker binds the principal from
// the node's registered instance, never from this body.
// origin: hot-update S7 plan §6.3 / INTEG-A agent verbs
func EncodeAgentTransferRequest(r xfervalue.Request) (string, []byte, error) {
	if err := validateTransferRequestUnion(r); err != nil {
		return "", nil, err
	}
	id, origin := Identity(r.Identity), Origin(r.Origin)
	var body any
	switch r.Op {
	case xfervalue.OpSource:
		body = TransferSourceReq{CallID: r.CallID, Identity: id, Origin: origin, Source: SourceSeal(*r.Source)}
	case xfervalue.OpReserve:
		body = TransferReserveReq{CallID: r.CallID, Identity: id, Origin: origin, ReceiverInstance: r.Reserve.ReceiverInstance}
	case xfervalue.OpTerminal:
		body = TransferTerminalReq{CallID: r.CallID, Identity: id, Origin: origin, Outcome: wireOutcome(*r.Outcome)}
	case xfervalue.OpCancel:
		body = TransferCancelReq{CallID: r.CallID, Identity: id, Origin: origin, Reason: CancelReason(r.Cancel.Reason)}
	case xfervalue.OpProbe, xfervalue.OpPermit, xfervalue.OpPrepare, xfervalue.OpStatus, xfervalue.OpCommit, xfervalue.OpFinalize, xfervalue.OpCensus, xfervalue.OpBucketConfig:
		// The agent subtree has no subject for these operations.
		return "", nil, errors.New("operation is not an agent transfer verb")
	default:
		return "", nil, errors.New("operation is not an agent transfer verb")
	}
	subject, err := SubjEvTransferRequest(id.SID, id.NID, id.TransferID, r.Op)
	if err != nil {
		return "", nil, err
	}
	if err = ValidateTransferWire(body); err != nil {
		return "", nil, err
	}
	b, err := json.Marshal(body)
	return subject, b, err
}

// TransferCanonicalFrom projects a ledger decision onto the receipt carrier.
func TransferCanonicalFrom(record *xfervalue.CanonicalRecord) *TransferCanonical {
	if record == nil {
		return nil
	}
	return &TransferCanonical{Kind: record.Kind, ReqID: record.ReqID, Digest: record.Digest, Bytes: bytes.Clone(record.Bytes)}
}

// DecodeTransferTerminalCanonical returns the exact decision bytes delivered
// with a terminal receipt, bound to the request identity, or nil when absent.
func DecodeTransferTerminalCanonical(identity xfervalue.Identity, body []byte) (*xfervalue.CanonicalRecord, error) {
	var p TransferTerminalResp
	if err := DecodeTransferWire(body, &p); err != nil {
		return nil, err
	}
	if p.Canonical == nil {
		return nil, nil
	}
	return &xfervalue.CanonicalRecord{Identity: identity, Kind: p.Canonical.Kind, ReqID: p.Canonical.ReqID,
		Digest: p.Canonical.Digest, Bytes: bytes.Clone(p.Canonical.Bytes)}, nil
}

// The broker→agent continuation link rides the agent's existing directed
// push-commit intake (SubjCmdForwarded(sid, nid, "push-commit")). An agent
// built before S7 decodes the envelope as a TransferCommitReq with no
// transfer_id and answers its ordinary transfer_unknown refusal: that reply is
// authenticated (only the (sid,nid) agent subscribes there) and is the explicit
// cap0 answer a probe needs, whereas a brand-new verb would only time out and
// timeout must never select legacy. A capable agent recognises TransferLink
// before any legacy lookup, so the link can never run legacy byte work.
// origin: hot-update S7 plan §6.3 ("broker→agent复用定向node CMD") / §7 / P20 / P22
const TransferLinkVerb = "push-commit"

// Link operations. probe returns the agent's capability, instance and settlement
// key; prepare installs the broker-admitted entry; grant delivers the durable
// commit reservation to the original receiver; receipt delivers the exact
// history receipt to a sender that has no courier.
const (
	TransferLinkProbe   = "probe"
	TransferLinkPrepare = "prepare"
	TransferLinkGrant   = "grant"
	TransferLinkReceipt = "receipt"
)

// TransferLink is the broker-signed control. Identity/Origin are always the
// retained original values; the agent re-validates them against its worker.
type TransferLink struct {
	Op         string             `json:"op"`
	CallID     string             `json:"call_id"`
	Identity   Identity           `json:"identity"`
	Origin     Origin             `json:"origin"`
	Intent     *Intent            `json:"intent,omitempty"`
	Permit     *CreationPermit    `json:"permit,omitempty"`
	Budget     *BudgetSeal        `json:"budget,omitempty"`
	Settlement *SettlementCapsule `json:"settlement,omitempty"`
	Grant      *CommitGrant       `json:"grant,omitempty"`
	Decision   *TransferDecision  `json:"decision,omitempty"`
	Receipt    *TerminalReceipt   `json:"receipt,omitempty"`
	Inline     []byte             `json:"inline,omitempty"`
}

// TransferLinkResult is the agent's answer. Version is empty for an agent
// without a working continuation worker: that is an explicit cap0 answer.
type TransferLinkResult struct {
	AgentNkey        string              `json:"agent_nkey,omitempty"`
	BindingSignature string              `json:"binding_signature,omitempty"`
	Version          string              `json:"version"`
	InstanceID       string              `json:"instance_id"`
	ReceiverNkey     string              `json:"receiver_nkey,omitempty"`
	MaxPayload       int64               `json:"max_payload,omitempty"`
	Status           *TransferStatusResp `json:"status,omitempty"`
	Code             string              `json:"code,omitempty"`
	Error            string              `json:"error,omitempty"`
	// Read-only precreation source inspection; nil keeps capability-only probes.
	// origin: hot-update S7 plan §7 / S7 internal review INTEG-O F4
	SourceSize *int64 `json:"source_size,omitempty"`
	// Prepare-only pull projection of a Tier A sender: the exact retained
	// inline copy together with the durable source seal it was verified
	// against. Both are absent on every other operation and on Tier B, so an
	// older peer's bytes are unchanged; a probe or status never carries them.
	// origin: hot-update S7 plan §8.1 / FIX-INLINE (FIX-TIERA report item 3)
	Inline []byte      `json:"inline,omitempty"`
	Source *SourceSeal `json:"source,omitempty"`
}

// TransferCodeOf recovers a stable transfer code from an owner's error so a
// dependency's diagnostic text (paths, capsules) never crosses the link.
func TransferCodeOf(err error, fallback string) string {
	if err == nil {
		return ""
	}
	var coded interface{ TransferCode() string }
	if errors.As(err, &coded) {
		return coded.TransferCode()
	}
	text := err.Error()
	for _, code := range []string{xfervalue.CodeIntentConflict, xfervalue.CodePermitExpired, xfervalue.CodeOwnerRecovering,
		xfervalue.CodeResultUnknown, xfervalue.CodeObjectConflict, xfervalue.CodeObjectVersionMismatch, xfervalue.CodeSourceChanged,
		xfervalue.CodeStorageHorizon, xfervalue.CodePayloadUnsupported, xfervalue.CodeCapacityExceeded, xfervalue.CodeTerminalConflict,
		xfervalue.CodeCommitInflight, xfervalue.CodeDurabilityUnknown, xfervalue.CodeReconcileRequired, xfervalue.CodeBudgetExceeded,
		xfervalue.CodeCanceled, xfervalue.CodeSessionDeleting, xfervalue.CodeNotMember, xfervalue.CodeNotOwner, xfervalue.CodeUnknown} {
		if strings.Contains(text, code) {
			return code
		}
	}
	return fallback
}

// EncodeTransferLink builds the legacy-shaped carrier around one link control.
func EncodeTransferLink(link TransferLink) ([]byte, error) {
	if link.Op == "" || link.CallID == "" {
		return nil, errors.New("incomplete transfer link")
	}
	return json.Marshal(TransferCommitReq{Link: &link})
}

// DecodeTransferLinkResult distinguishes a capable answer from the legacy
// agent's refusal. legacy reports that the reply is an authentic pre-S7 commit
// refusal (no link result, transfer_unknown); any other shape is an error.
func DecodeTransferLinkResult(data []byte) (result TransferLinkResult, legacy bool, err error) {
	var resp TransferCommitResp
	if err = json.Unmarshal(data, &resp); err != nil {
		return result, false, errors.New("invalid transfer link reply")
	}
	if resp.Link != nil {
		return *resp.Link, false, nil
	}
	if !resp.OK && resp.Code == "transfer_unknown" {
		return result, true, nil
	}
	return result, false, errors.New("transfer link reply has no result: " + resp.Code)
}

// TransferDecision is the public projection of a retained canonical outcome.
// It keeps proto NATS-free without moving the frozen private contract.
// origin: hot-update S7 plan §6.3 / §8.2 / T39
type TransferDecision struct {
	Identity  xfervalue.Identity
	Outcome   xfervalue.ReceiverOutcome
	Canonical xfervalue.CanonicalRecord
	Receipt   *xfervalue.TerminalReceipt
}
