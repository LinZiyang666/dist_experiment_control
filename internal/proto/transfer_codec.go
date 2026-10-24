package proto

import (
	"bytes"
	"encoding/json"
	"errors"

	x "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// EncodeTransferRequest is the shared capable carrier mapping. The subject uses
// the authenticated NKey; the retained Origin remains in the body so a current
// home cannot silently replace the original decision owner.
// origin: hot-update S7 plan §6.2–§6.3 / T32 / INTEG-D
func EncodeTransferRequest(actor string, r x.Request) (string, []byte, error) {
	if err := validateTransferRequestUnion(r); err != nil {
		return "", nil, err
	}
	id, origin := Identity(r.Identity), Origin(r.Origin)
	var body any
	var subject string
	var err error
	switch r.Op {
	case x.OpProbe:
		body = TransferProbeReq{CallID: r.CallID, SID: id.SID, NID: id.NID}
	case x.OpPermit:
		body = TransferPermitReq{CallID: r.CallID, Intent: Intent(*r.Intent), TransferID: id.TransferID, AgentInstance: id.AgentInstance}
	case x.OpStatus:
		// Standalone status discovers the original entry; full identities are checked
		// by the controller after authenticated subject scope is resolved.
		body = TransferStatusReq{CallID: r.CallID, Identity: id, Origin: origin}
	case x.OpSource:
		body = TransferSourceReq{CallID: r.CallID, Identity: id, Origin: origin, Source: SourceSeal(*r.Source)}
	case x.OpReserve:
		body = TransferReserveReq{CallID: r.CallID, Identity: id, Origin: origin, ReceiverInstance: r.Reserve.ReceiverInstance}
	case x.OpTerminal, x.OpFinalize:
		body = TransferTerminalReq{CallID: r.CallID, Identity: id, Origin: origin, Outcome: wireOutcome(*r.Outcome)}
	case x.OpCancel:
		body = TransferCancelReq{CallID: r.CallID, Identity: id, Origin: origin, Reason: CancelReason(r.Cancel.Reason)}
	case x.OpPrepare:
		intent := Intent(*r.Intent)
		c := &TransferContinuation{Version: TransferVersion, Identity: id, Origin: origin, Permit: wirePermit(r.Permit), Intent: &intent}
		if intent.Verb == "push" {
			subject = SubjCmdBy(id.SID, actor, id.NID, "push")
			body = PushPrepareReq{CallID: r.CallID, TransferID: id.TransferID, Path: intent.Path, Size: intent.Size, SHA256: intent.SHA256,
				Force: intent.Force, Tier: intent.Tier, ChunkSize: int(intent.ChunkSize), InlineData: append([]byte(nil), r.Inline...), TransferVersion: TransferVersion, Continuation: c}
		} else {
			subject = SubjCmdBy(id.SID, actor, id.NID, "pull")
			inline := int64(0)
			if intent.Tier == "a" {
				inline = XferTierAMaxBytes
			}
			body = PullPrepareReq{CallID: r.CallID, TransferID: id.TransferID, Path: intent.Path, Force: intent.Force, MaxInline: inline, TransferVersion: TransferVersion, Continuation: c}
		}
	case x.OpCommit:
		subject = SubjCmdBy(id.SID, actor, id.NID, "push-commit")
		body = TransferCommitReq{CallID: r.CallID, TransferID: id.TransferID, Bucket: XferBucketName(id.SID), ObjectKey: id.TransferID,
			Continuation: &TransferContinuation{Version: TransferVersion, Identity: id, Origin: origin}}
	case x.OpCensus, x.OpBucketConfig:
		// Broker-internal routed verbs have no public ctl request carrier.
		return "", nil, errors.New("unsupported public transfer operation")
	default:
		return "", nil, errors.New("unsupported public transfer operation")
	}
	if subject == "" {
		op := r.Op
		if op == x.OpFinalize {
			op = x.OpTerminal
		}
		subject, err = SubjCtrlTransferRequest(actor, id.SID, id.NID, id.TransferID, op)
		if err != nil {
			return "", nil, err
		}
	}
	// Early probe/status can have a partial identity. All other operations carry
	// the complete wire authority and are validated before any publish.
	if r.Op != x.OpStatus {
		if err = ValidateTransferWire(body); err != nil {
			return "", nil, err
		}
	}
	b, err := json.Marshal(body)
	return subject, b, err
}

func validateTransferRequestUnion(r x.Request) error {
	if !validTransferToken(r.CallID) || ValidateSID(r.Identity.SID) != nil || ValidateNID(r.Identity.NID) != nil || !validTransferToken(r.Identity.TransferID) {
		return errors.New("invalid transfer request scope")
	}
	intent := r.Op == x.OpPermit || r.Op == x.OpPrepare
	if (r.Intent != nil) != intent || (r.Permit != nil) != (r.Op == x.OpPrepare) || (r.Source != nil) != (r.Op == x.OpSource) ||
		(r.Reserve != nil) != (r.Op == x.OpReserve) || (r.Outcome != nil) != (r.Op == x.OpTerminal || r.Op == x.OpFinalize) || (r.Cancel != nil) != (r.Op == x.OpCancel) {
		return errors.New("invalid transfer request union")
	}
	if len(r.Inline) > 0 && (r.Op != x.OpPrepare || r.Intent.Tier != "a" || r.Intent.Verb != "push") {
		return errors.New("unexpected inline transfer payload")
	}
	if r.Op == x.OpPrepare && r.Intent.Verb == "push" && r.Intent.Tier == "a" && (int64(len(r.Inline)) != r.Intent.Size || len(r.Inline) > XferTierAMaxBytes) {
		return errors.New("invalid inline transfer size")
	}
	if r.Reserve != nil && (r.Reserve.CallID != r.CallID || r.Reserve.Origin != r.Origin) || r.Cancel != nil && (r.Cancel.CallID != r.CallID || r.Cancel.Origin != r.Origin) {
		return errors.New("transfer nested request binding mismatch")
	}
	return nil
}

// DecodeTransferResponse is strict even on read-only replies. In particular a
// capsule/inline/source field in status is rejected rather than discarded.
// Prepare verb is retained by the invocation, never guessed from a response.
// origin: hot-update S7 plan §6.4 / T32 / INTEG-D
func DecodeTransferResponse(op x.Operation, verb string, body []byte) (x.Response, error) {
	var out x.Response
	switch op {
	case x.OpProbe:
		var p TransferProbeResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		out.Probe = &x.ProbeResult{Version: p.Version, HistoryGuardVersion: p.HistoryGuardVersion, BucketConfigVersion: p.BucketConfigVersion, InstanceID: p.InstanceID, MaxPayload: p.MaxPayload, MaxMsgSize: p.MaxMsgSize, Code: p.Code, Error: p.Error}
		out.Code, out.Error = p.Code, p.Error
	case x.OpPermit:
		var p TransferPermitResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		out.Permit = contractPermit(p.Permit)
		out.Code, out.Error = p.Code, p.Error
	case x.OpPrepare:
		var c *TransferContinuation
		var status *TransferStatusResp
		var ok bool
		switch verb {
		case "push":
			var p PushPrepareResp
			if err := DecodeTransferWire(body, &p); err != nil {
				return out, err
			}
			out.Code, out.Error = p.Code, p.Error
			c, status, ok = p.Continuation, p.Status, p.OK
		case "pull":
			var p PullPrepareResp
			if err := DecodeTransferWire(body, &p); err != nil {
				return out, err
			}
			out.Code, out.Error = p.Code, p.Error
			c, status, ok = p.Continuation, p.Status, p.OK
			out.Inline = append([]byte(nil), p.InlineData...)
		default:
			return out, errors.New("missing prepare verb")
		}
		if out.Code != "" || out.Error != "" {
			return out, nil
		}
		if !ok || c == nil || status == nil || status.Identity != c.Identity || status.Origin != c.Origin || c.Grant != nil {
			return x.Response{}, errors.New("prepare has no matching retained state")
		}
		out.Status = contractStatus(status)
		if c.Source != nil {
			v := x.SourceSeal(*c.Source)
			out.Source = &v
		}
		out.Settlement = contractCapsule(c.Settlement)
	case x.OpReserve:
		var p TransferReserveResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		if p.Grant != nil {
			v := contractGrant(*p.Grant)
			out.Grant = &v
		}
		if p.Source != nil {
			v := x.SourceSeal(*p.Source)
			out.Source = &v
		}
		out.Code, out.Error = p.Code, p.Error
	case x.OpTerminal, x.OpFinalize:
		var p TransferTerminalResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		if p.Receipt != nil {
			v := x.TerminalReceipt(*p.Receipt)
			out.Receipt = &v
		}
		out.Code, out.Error = p.Code, p.Error
	case x.OpCommit:
		var p TransferCommitResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		out.Code, out.Error = p.Code, p.Error
		if out.Code != "" || out.Error != "" {
			return out, nil
		}
		if !p.OK || p.Continuation == nil || p.Continuation.Source != nil || p.Continuation.Settlement != nil || p.Continuation.Intent != nil || p.Continuation.Grant != nil {
			return out, errors.New("commit has no retained binding")
		}
		c := p.Continuation
		out.Status = &x.Status{State: x.StatusLive, Phase: x.PhaseReceiving, Identity: x.Identity(c.Identity), Origin: x.Origin(c.Origin), Budget: x.BudgetSeal(c.Budget)}
	case x.OpStatus, x.OpSource, x.OpCancel:
		var p TransferStatusResp
		if err := DecodeTransferWire(body, &p); err != nil {
			return out, err
		}
		out.Status = contractStatus(&p)
		out.Code, out.Error = p.Code, p.Error
	case x.OpCensus, x.OpBucketConfig:
		// Broker-internal routed verbs never answer a public ctl request.
		return out, errors.New("unsupported public transfer response")
	default:
		return out, errors.New("unsupported public transfer response")
	}
	return out, nil
}

func wirePermit(p *x.CreationPermit) *CreationPermit {
	if p == nil {
		return nil
	}
	return &CreationPermit{Identity: Identity(p.Identity), Origin: Origin(p.Origin), IssuedAt: p.IssuedAt, CreateUntil: p.CreateUntil, PermitNonce: p.PermitNonce, Signature: p.Signature}
}
func contractPermit(p *CreationPermit) *x.CreationPermit {
	if p == nil {
		return nil
	}
	return &x.CreationPermit{Identity: x.Identity(p.Identity), Origin: x.Origin(p.Origin), IssuedAt: p.IssuedAt, CreateUntil: p.CreateUntil, PermitNonce: p.PermitNonce, Signature: p.Signature}
}
func wireGrant(g x.CommitGrant) CommitGrant {
	return CommitGrant{Identity: Identity(g.Identity), GrantNonce: g.GrantNonce, ReceiverInstance: g.ReceiverInstance, TerminalID: g.TerminalID}
}
func contractGrant(g CommitGrant) x.CommitGrant {
	return x.CommitGrant{Identity: x.Identity(g.Identity), GrantNonce: g.GrantNonce, ReceiverInstance: g.ReceiverInstance, TerminalID: g.TerminalID}
}
func wireOutcome(o x.ReceiverOutcome) ReceiverOutcome {
	return ReceiverOutcome{Grant: wireGrant(o.Grant), Primitive: CommitPrimitive(o.Primitive), Outcome: PrimitiveOutcome(o.Outcome), Bytes: o.Bytes, SHA256: o.SHA256, Code: o.Code, Error: o.Error, ParentSynced: o.ParentSynced, TmpCleanupPending: o.TmpCleanupPending}
}
func contractCapsule(c *SettlementCapsule) *x.SettlementCapsule {
	if c == nil {
		return nil
	}
	return &x.SettlementCapsule{Identity: x.Identity(c.Identity), Origin: x.Origin(c.Origin), ReceiverNkey: c.ReceiverNkey, Issuer: c.Issuer, Nonce: c.Nonce, Signature: c.Signature}
}
func contractStatus(s *TransferStatusResp) *x.Status {
	out := &x.Status{State: x.StatusState(s.State), Phase: x.Phase(s.Phase), Identity: x.Identity(s.Identity), Origin: x.Origin(s.Origin), Budget: x.BudgetSeal(s.Budget), DurableOffset: s.DurableOffset, ChunkIndex: s.ChunkIndex, LastConsumedStreamSeq: s.LastConsumedStreamSeq, CleanupPending: s.CleanupPending, Code: s.Code, Error: s.Error}
	if s.Terminal != nil {
		v := x.TerminalReceipt(*s.Terminal)
		out.Terminal = &v
	}
	return out
}

// TransferWriterRetirementMarked keeps refused evidence on its capable path;
// malformed/unknown markers must not become a legacy finalize operation.
// origin: hot-update S7 plan §2.3 / §7 / FIX-TERM request 3
func TransferWriterRetirementMarked(body []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	_, present := fields["writer_retirement_version"]
	return present
}

// This finalize-entry extension carries only public retirement evidence. It
// never transports pending chunk bytes, source paths or settlement credentials.
// Coalescing all original stream expectations preserves the fence's strict
// ordering requirement without copying the writer's bulk private state.
// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
type TransferWriterRetirement struct {
	Bucket               string `json:"bucket"`
	Stream               string `json:"stream"`
	StreamCreated        string `json:"stream_created"`
	SHA256               string `json:"sha256"`
	Size                 int64  `json:"size"`
	ChunkSize            uint32 `json:"chunk_size"`
	Chunks               uint32 `json:"chunks"`
	AttachmentGeneration uint64 `json:"attachment_generation"`
	WriterRetired        bool   `json:"writer_retired"`
	ExpectedStreamSeq    uint64 `json:"expected_stream_seq"`
	PreviousMetadataSeq  uint64 `json:"previous_metadata_seq"`
}

// The marker prevents this payload from falling into legacy finalization, even
// when its version is refused. It uses the original authenticated actor's
// finalize subject and cannot admit bytes or decide a receiver's outcome.
type TransferWriterRetirementReq struct {
	Version  string                   `json:"writer_retirement_version"`
	CallID   string                   `json:"call_id"`
	Identity Identity                 `json:"identity"`
	Origin   Origin                   `json:"origin"`
	Writer   TransferWriterRetirement `json:"writer"`
}

// An exact digest plus a durable revision acknowledges the submitted witness;
// EOF, a generic OK or an unrelated terminal receipt cannot discharge it.
type TransferWriterRetirementResp struct {
	CallID        string   `json:"call_id,omitempty"`
	Identity      Identity `json:"identity,omitempty"`
	Origin        Origin   `json:"origin,omitempty"`
	Revision      uint64   `json:"revision,omitempty"`
	WitnessDigest string   `json:"witness_digest,omitempty"`
	Code          string   `json:"code,omitempty"`
	Error         string   `json:"error,omitempty"`
}

// DecodeTransferRequest receives scope from the authenticated subject adapter.
// It never trusts a payload actor. Only status may start with an unknown Origin;
// the controller resolves that read through its retained original-owner census.
// origin: hot-update S7 plan §6.1–§6.3 / INTEG-D
func DecodeTransferRequest(actorFP, sid, nid, tid string, op x.Operation, verb string, body []byte) (x.Request, error) {
	r := x.Request{Op: op, Identity: x.Identity{SID: sid, NID: nid, ActorFP: actorFP, TransferID: tid}}
	switch op {
	case x.OpProbe:
		var p TransferProbeReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		if p.SID != sid || p.NID != nid {
			return r, errors.New(CodeTransferIntentConflict)
		}
		r.CallID = p.CallID
	case x.OpPermit:
		var err error
		r, err = decodeTransferPermit(r, tid, body)
		if err != nil {
			return r, err
		}
	case x.OpStatus:
		var p TransferStatusReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, r.Identity, r.Origin = p.CallID, x.Identity(p.Identity), x.Origin(p.Origin)
	case x.OpSource:
		var p TransferSourceReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		s := x.SourceSeal(p.Source)
		r.CallID, r.Identity, r.Origin, r.Source = p.CallID, x.Identity(p.Identity), x.Origin(p.Origin), &s
	case x.OpReserve:
		var p TransferReserveReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, r.Identity, r.Origin = p.CallID, x.Identity(p.Identity), x.Origin(p.Origin)
		r.Reserve = &x.ReserveRequest{CallID: r.CallID, Origin: r.Origin, ReceiverInstance: p.ReceiverInstance}
	case x.OpTerminal, x.OpFinalize:
		var p TransferTerminalReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, r.Identity, r.Origin = p.CallID, x.Identity(p.Identity), x.Origin(p.Origin)
		o := p.Outcome
		r.Outcome = &x.ReceiverOutcome{Grant: contractGrant(o.Grant), Primitive: x.CommitPrimitive(o.Primitive), Outcome: x.PrimitiveOutcome(o.Outcome), Bytes: o.Bytes, SHA256: o.SHA256, Code: o.Code, Error: o.Error, ParentSynced: o.ParentSynced, TmpCleanupPending: o.TmpCleanupPending}
	case x.OpCancel:
		var p TransferCancelReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, r.Identity, r.Origin = p.CallID, x.Identity(p.Identity), x.Origin(p.Origin)
		r.Cancel = &x.CancelRequest{CallID: r.CallID, Origin: r.Origin, Reason: x.CancelReason(p.Reason)}
	case x.OpPrepare:
		var err error
		r, err = decodeTransferPrepare(r, tid, verb, body)
		if err != nil {
			return r, err
		}
	case x.OpCommit:
		var err error
		r, err = decodeTransferCommit(r, sid, tid, body)
		if err != nil {
			return r, err
		}
	case x.OpCensus, x.OpBucketConfig:
		// Routed broker verbs are never accepted from a ctl public subject.
		return r, errors.New("unknown public transfer operation")
	default:
		return r, errors.New("unknown public transfer operation")
	}
	if r.Identity.SID != sid || r.Identity.NID != nid || r.Identity.TransferID != tid || r.Identity.ActorFP != actorFP {
		return x.Request{}, errors.New(CodeTransferIntentConflict)
	}
	if err := validateTransferRequestUnion(r); err != nil {
		return x.Request{}, err
	}
	return r, nil
}

// EncodeTransferResponse prevents a status or error response from leaking a
// prepare-only capsule. The adapter must pass the actual request, including its
// original verb; it cannot manufacture completion from a pickup ACK.
// origin: hot-update S7 plan §6.4 / INTEG-D
func EncodeTransferResponse(request x.Request, r x.Response) ([]byte, error) {
	op := request.Op
	// A reserve reply may name the sealed source its Grant authorizes; inline
	// bytes and settlement capsules remain prepare-only.
	// origin: hot-update S7 plan §4 / INTEG-D pull source seal
	if op != x.OpPrepare && (len(r.Inline) != 0 || (r.Source != nil && op != x.OpReserve) || r.Settlement != nil) {
		return nil, errors.New("prepare-only transfer response fields")
	}
	var body any
	switch op {
	case x.OpProbe:
		p := r.Probe
		if p == nil {
			p = &x.ProbeResult{}
		}
		body = TransferProbeResp{Version: p.Version, HistoryGuardVersion: p.HistoryGuardVersion, BucketConfigVersion: p.BucketConfigVersion, InstanceID: p.InstanceID, MaxPayload: p.MaxPayload, MaxMsgSize: p.MaxMsgSize, Code: r.Code, Error: r.Error}
	case x.OpPermit:
		body = TransferPermitResp{Permit: wirePermit(r.Permit), Code: r.Code, Error: r.Error}
	case x.OpPrepare:
		if request.Intent == nil {
			return nil, errors.New("missing original prepare intent")
		}
		var c *TransferContinuation
		if r.Status != nil {
			s := r.Status
			c = &TransferContinuation{Version: TransferVersion, Identity: Identity(s.Identity), Origin: Origin(s.Origin), Budget: BudgetSeal(s.Budget)}
			if r.Source != nil {
				source := SourceSeal(*r.Source)
				c.Source = &source
			}
			if r.Settlement != nil {
				a := r.Settlement
				c.Settlement = &SettlementCapsule{Identity: Identity(a.Identity), Origin: Origin(a.Origin), ReceiverNkey: a.ReceiverNkey, Issuer: a.Issuer, Nonce: a.Nonce, Signature: a.Signature}
			}
		}
		version := ""
		if c != nil {
			version = c.Identity.ObjectVersion
		}
		if request.Intent.Verb == "push" {
			body = PushPrepareResp{OK: r.Code == "" && r.Error == "" && r.Status != nil, Tier: request.Intent.Tier, Status: wireStatus(r.Status), Code: r.Code, Error: r.Error, TransferVersion: TransferVersion, ObjectVersion: version, Continuation: c}
		} else {
			p := PullPrepareResp{OK: r.Code == "" && r.Error == "" && r.Status != nil, Tier: request.Intent.Tier, Status: wireStatus(r.Status), Code: r.Code, Error: r.Error, TransferVersion: TransferVersion, ObjectVersion: version, Continuation: c, InlineData: bytes.Clone(r.Inline)}
			if r.Source != nil {
				p.Size, p.SHA256 = r.Source.Size, r.Source.SHA256
			}
			body = p
		}
	case x.OpCommit:
		var c *TransferContinuation
		if r.Status != nil {
			s := r.Status
			c = &TransferContinuation{Version: TransferVersion, Identity: Identity(s.Identity), Origin: Origin(s.Origin), Budget: BudgetSeal(s.Budget)}
		}
		body = TransferCommitResp{OK: r.Code == "" && r.Error == "" && r.Status != nil, Code: r.Code, Error: r.Error, Continuation: c}
	case x.OpReserve:
		var g *CommitGrant
		if r.Grant != nil {
			v := wireGrant(*r.Grant)
			g = &v
		}
		reserve := TransferReserveResp{Grant: g, Code: r.Code, Error: r.Error}
		if r.Source != nil && g != nil {
			s := SourceSeal(*r.Source)
			reserve.Source = &s
		}
		body = reserve
	case x.OpTerminal, x.OpFinalize:
		var receipt *TerminalReceipt
		if r.Receipt != nil {
			v := TerminalReceipt(*r.Receipt)
			receipt = &v
		}
		if receipt == nil && r.Status != nil && r.Status.Terminal != nil {
			v := TerminalReceipt(*r.Status.Terminal)
			receipt = &v
		}
		body = TransferTerminalResp{Receipt: receipt, Code: r.Code, Error: r.Error}
	case x.OpStatus, x.OpSource, x.OpCancel:
		status := wireStatus(r.Status)
		if status == nil {
			status = &TransferStatusResp{Code: r.Code, Error: r.Error}
		}
		if r.Code != "" || r.Error != "" {
			status.Code, status.Error = r.Code, r.Error
		}
		body = status
	case x.OpCensus, x.OpBucketConfig:
		// Routed broker verbs have no public response carrier.
		return nil, errors.New("unknown transfer response operation")
	default:
		return nil, errors.New("unknown transfer response operation")
	}
	if err := ValidateTransferWire(body); err != nil {
		return nil, err
	}
	return json.Marshal(body)
}
func wireStatus(s *x.Status) *TransferStatusResp {
	if s == nil {
		return nil
	}
	p := &TransferStatusResp{State: StatusState(s.State), Phase: Phase(s.Phase), Identity: Identity(s.Identity), Origin: Origin(s.Origin), Budget: BudgetSeal(s.Budget), DurableOffset: s.DurableOffset, ChunkIndex: s.ChunkIndex, LastConsumedStreamSeq: s.LastConsumedStreamSeq, CleanupPending: s.CleanupPending, Code: s.Code, Error: s.Error}
	if s.Terminal != nil {
		v := TerminalReceipt(*s.Terminal)
		p.Terminal = &v
	}
	return p
}

// Creation authority, prepare envelopes and commit carriers each retain their
// own validation before the shared authenticated-scope and union checks.
// origin: hot-update S10 plan §0 / LINT-L2 maintidx
func decodeTransferPermit(r x.Request, tid string, body []byte) (x.Request, error) {
	var p TransferPermitReq
	if err := DecodeTransferWire(body, &p); err != nil {
		return r, err
	}
	if p.TransferID != tid {
		return r, errors.New(CodeTransferIntentConflict)
	}
	intent := x.Intent(p.Intent)
	r.CallID, r.Intent = p.CallID, &intent
	r.Identity.AgentInstance = p.AgentInstance
	digest, err := TransferIntentDigest(p.Intent)
	if err != nil {
		return r, err
	}
	r.Identity.IntentDigest = digest
	return r, nil
}

func decodeTransferPrepare(r x.Request, tid, verb string, body []byte) (x.Request, error) {
	var c *TransferContinuation
	var legacyID string
	switch verb {
	case "push":
		var p PushPrepareReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, c, legacyID = p.CallID, p.Continuation, p.TransferID
		r.Inline = bytes.Clone(p.InlineData)
		if c == nil || c.Intent == nil || p.TransferVersion != TransferVersion || p.Path != c.Intent.Path || p.Size != c.Intent.Size || p.SHA256 != c.Intent.SHA256 || p.Force != c.Intent.Force || p.Tier != c.Intent.Tier || p.ChunkSize != int(c.Intent.ChunkSize) {
			return r, errors.New(CodeTransferIntentConflict)
		}
	case "pull":
		var p PullPrepareReq
		if err := DecodeTransferWire(body, &p); err != nil {
			return r, err
		}
		r.CallID, c, legacyID = p.CallID, p.Continuation, p.TransferID
		if c == nil || c.Intent == nil || p.TransferVersion != TransferVersion || p.Path != c.Intent.Path || p.Force != c.Intent.Force || (p.MaxInline > 0) != (c.Intent.Tier == "a") {
			return r, errors.New(CodeTransferIntentConflict)
		}
	default:
		return r, errors.New("unknown prepare verb")
	}
	if c.Intent.Verb != verb || legacyID != tid || c.Settlement != nil || c.Source != nil || c.Grant != nil {
		return r, errors.New(CodeTransferIntentConflict)
	}
	intent := x.Intent(*c.Intent)
	r.Identity, r.Origin, r.Intent, r.Permit = x.Identity(c.Identity), x.Origin(c.Origin), &intent, contractPermit(c.Permit)
	digest, err := TransferIntentDigest(*c.Intent)
	if err != nil || digest != r.Identity.IntentDigest {
		return r, errors.New(CodeTransferIntentConflict)
	}
	return r, nil
}

func decodeTransferCommit(r x.Request, sid, tid string, body []byte) (x.Request, error) {
	var p TransferCommitReq
	if err := DecodeTransferWire(body, &p); err != nil {
		return r, err
	}
	c := p.Continuation
	if c == nil || c.Settlement != nil || c.Source != nil || c.Intent != nil || c.Grant != nil || p.TransferID != tid || p.Bucket != XferBucketName(sid) || p.ObjectKey != tid {
		return r, errors.New(CodeTransferIntentConflict)
	}
	r.CallID, r.Identity, r.Origin = p.CallID, x.Identity(c.Identity), x.Origin(c.Origin)
	return r, nil
}
