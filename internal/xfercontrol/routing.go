package xfercontrol

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Registration is consumed by both cold Run and hot wireLate. Subject is a
// subscription pattern, not a second subject parser. P01 owns concrete wire
// builders/parsers; the adapter must bind the parsed actor to Principal.NKey.
// ClusterVerb names the existing signed broker forwarding envelope. Management
// census/config use their typed adapters, not the public Request union.
// origin: hot-update S7 plan §6.3 / §12 P16
type Registration struct {
	Operation      x.Operation
	Subject        string
	Role           x.PrincipalRole
	ClusterVerb    string
	PlainBroadcast bool
	OriginalOwner  bool
	CurrentHome    bool
	LegacyCarrier  bool
}

// Registrations returns a fresh closed table. A caller cannot mutate the hot
// table by adjusting its cold subscriptions. No original-owner operation may
// be queue-grouped: a random nonowner must not consume its only delivery.
func Registrations() []Registration {
	ctl := proto.SubjectPrefix + ".ctrl.by.*.s.*.node.*.transfer.*."
	agent := proto.SubjectPrefix + ".s.*.ev.node.*.transfer.*."
	cmd := proto.SubjectPrefix + ".s.*.cmd.by.*.node.*."
	return []Registration{
		{Operation: x.OpProbe, Subject: ctl + "probe.req", Role: x.RoleCTL, PlainBroadcast: true, CurrentHome: true},
		{Operation: x.OpPermit, Subject: ctl + "permit.req", Role: x.RoleCTL, PlainBroadcast: true, CurrentHome: true},
		{Operation: x.OpStatus, Subject: ctl + "status.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpSource, Subject: ctl + "source.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpReserve, Subject: ctl + "reserve.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpTerminal, Subject: ctl + "terminal.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpCancel, Subject: ctl + "cancel.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpSource, Subject: agent + "source.req", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpReserve, Subject: agent + "reserve.req", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpTerminal, Subject: agent + "terminal.req", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpCancel, Subject: agent + "cancel.req", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpCensus, Subject: proto.SubjClusterApply("transfer-census"), Role: x.RoleBroker, ClusterVerb: "transfer-census", PlainBroadcast: true, OriginalOwner: true},
		{Operation: x.OpBucketConfig, Subject: proto.SubjClusterApply("transfer-bucket-config"), Role: x.RoleBroker, ClusterVerb: "transfer-bucket-config", PlainBroadcast: true},
		{Operation: x.OpPrepare, Subject: cmd + "push.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
		{Operation: x.OpPrepare, Subject: cmd + "pull.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
		{Operation: x.OpCommit, Subject: cmd + "push-commit.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
		{Operation: x.OpFinalize, Subject: proto.SubjectPrefix + ".ctrl.by.*.s.*.transfer.*.finalize.req", Role: x.RoleCTL, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
		{Operation: x.OpTerminal, Subject: agent + "complete", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
		{Operation: x.OpTerminal, Subject: agent + "failed", Role: x.RoleAgent, PlainBroadcast: true, OriginalOwner: true, LegacyCarrier: true},
	}
}

func validToken(s string) bool {
	return s != "" && !strings.ContainsAny(s, ".*>\\/\x00") && strings.IndexFunc(s, unicode.IsSpace) < 0 &&
		strings.IndexFunc(s, unicode.IsControl) < 0
}

// Fingerprints are canonical base64 text and may contain a slash. They are
// compared to the authenticated principal and never used as subject tokens.
// origin: hot-update S7 plan §6.2 / T39
func validIdentity(id x.Identity, full bool) bool {
	if !validToken(id.SID) || !validToken(id.NID) || (!validToken(id.ActorFP) && !auth.ValidFingerprint(id.ActorFP)) || !validToken(id.TransferID) {
		return false
	}
	for _, s := range []string{id.AgentInstance, id.EntryNonce, id.IntentDigest, id.ObjectVersion} {
		if s != "" && !validToken(s) {
			return false
		}
	}
	return !full || (id.AgentInstance != "" && id.EntryNonce != "" && id.IntentDigest != "")
}

func validOrigin(o x.Origin) bool {
	return validToken(o.BrokerID) && validToken(o.InstanceID) && validToken(o.BootID) && o.Generation > 0
}

func validSHA(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && strings.ToLower(s) == s
}

func validateIntent(i x.Intent) error {
	if (i.Verb != "push" && i.Verb != "pull") || (i.Tier != "a" && i.Tier != "b") ||
		i.Path == "" || strings.ContainsAny(i.Path, "\x00\r\n") || i.Size < 0 || i.Size > proto.XferMaxBytes || i.RateLimitBPS < 0 ||
		(i.SHA256 != "" && !validSHA(i.SHA256)) || (i.Verb == "push" && !validSHA(i.SHA256)) {
		return refusal(x.CodeIntentConflict)
	}
	if (i.Tier == "a" && (i.Size > proto.XferTierAMaxBytes || i.ChunkSize != 0)) ||
		(i.Tier == "b" && (i.ChunkSize == 0 || i.ChunkSize > 1024*1024)) {
		return refusal(x.CodePayloadUnsupported)
	}
	// Rate is fixed before Issue/Admit. This overflow-free refusal is also
	// enforced by P01's shared budget seal; it never clips an excessive budget.
	if i.Tier == "b" {
		rate := int64(proto.XferMinThroughput)
		if i.RateLimitBPS > 0 {
			rate = min(rate, i.RateLimitBPS)
		}
		seconds := (i.Size + rate - 1) / rate
		if 2*seconds+60 > int64(proto.XferTierBMaxBudget/time.Second) {
			return refusal(x.CodeBudgetExceeded)
		}
	}
	return nil
}

// The union is closed before ANY port call. Checking just the selected pointer
// lets a signed request smuggle another operation's grant/outcome into adapters.
// origin: hot-update S7 plan §6.2 / §8.1 / P16
func validateRequest(r x.Request, p x.Principal) error {
	if !validToken(r.CallID) || !validIdentity(r.Identity, false) || !validToken(p.NKey) ||
		p.SID != r.Identity.SID || p.NID != r.Identity.NID {
		return refusal(x.CodeNotMember)
	}
	if r.Origin != (x.Origin{}) && !validOrigin(r.Origin) {
		return refusal(x.CodeIntentConflict)
	}
	if err := validatePrincipal(r, p); err != nil {
		return err
	}
	wantIntent, wantPermit, wantSource, wantReserve, wantOutcome, wantCancel := false, false, false, false, false, false
	switch r.Op {
	case x.OpProbe:
		wantIntent = r.Intent != nil
		// A source inspection is a read-only path query, never a partially
		// specified creation intent. Force/rate/geometry cannot hide here.
		// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
		if wantIntent && (*r.Intent != (x.Intent{Verb: "pull", Path: r.Intent.Path}) || r.Intent.Path == "" || strings.IndexFunc(r.Intent.Path, unicode.IsControl) >= 0) {
			return refusal(x.CodeIntentConflict)
		}
	case x.OpStatus, x.OpCensus, x.OpBucketConfig:
	case x.OpPermit:
		wantIntent = true
		if r.Origin != (x.Origin{}) || r.Identity.AgentInstance == "" {
			return refusal(x.CodeIntentConflict)
		}
	case x.OpPrepare:
		wantIntent, wantPermit = true, true
	case x.OpSource:
		wantSource = true
	case x.OpReserve, x.OpCommit:
		wantReserve = true
	case x.OpTerminal, x.OpFinalize:
		wantOutcome = true
	case x.OpCancel:
		wantCancel = true
	default:
		return refusal(x.CodePayloadUnsupported)
	}
	if (r.Intent != nil) != wantIntent || (r.Permit != nil) != wantPermit || (r.Source != nil) != wantSource ||
		(r.Reserve != nil) != wantReserve || (r.Outcome != nil) != wantOutcome || (r.Cancel != nil) != wantCancel {
		return refusal(x.CodePayloadUnsupported)
	}
	if r.Op != x.OpProbe && r.Op != x.OpPermit && r.Op != x.OpStatus && r.Op != x.OpCensus && r.Op != x.OpBucketConfig {
		if !validIdentity(r.Identity, true) || !validOrigin(r.Origin) {
			return refusal(x.CodeIntentConflict)
		}
	}
	if r.Intent != nil && r.Op != x.OpProbe {
		if err := validateIntent(*r.Intent); err != nil {
			return err
		}
	}
	if r.Permit != nil && (!validToken(r.Permit.PermitNonce) || r.Permit.Signature == "") {
		return refusal(x.CodeIntentConflict)
	}
	if r.Reserve != nil && (r.Reserve.CallID != r.CallID || r.Reserve.Origin != r.Origin || !validToken(r.Reserve.ReceiverInstance)) {
		return refusal(x.CodeIntentConflict)
	}
	if r.Cancel != nil && (r.Cancel.CallID != r.CallID || r.Cancel.Origin != r.Origin || !validReason(r.Cancel.Reason)) {
		return refusal(x.CodeIntentConflict)
	}
	if r.Source != nil {
		s := r.Source
		if s.Size < 0 || s.Size > proto.XferMaxBytes || !validSHA(s.SHA256) || s.RateLimitBPS < 0 ||
			!validToken(s.SourceFingerprint) || (r.Identity.ObjectVersion == "" && (s.Chunks != 0 || s.ChunkSize != 0)) ||
			(r.Identity.ObjectVersion != "" && (s.ChunkSize == 0 || s.ChunkSize > 1024*1024 ||
				uint64(s.Chunks) != (uint64(s.Size)+uint64(s.ChunkSize)-1)/uint64(s.ChunkSize))) {
			return refusal(x.CodeIntentConflict)
		}
	}
	if r.Outcome != nil {
		return validateOutcome(*r.Outcome, r.Identity)
	}
	return nil
}

func validatePrincipal(r x.Request, p x.Principal) error {
	switch p.Role {
	case x.RoleCTL:
		if p.FP != r.Identity.ActorFP || r.Op == x.OpCensus || r.Op == x.OpBucketConfig {
			return refusal(x.CodeNotOwner)
		}
	case x.RoleAgent:
		if p.InstanceID != r.Identity.AgentInstance || p.InstanceID == "" ||
			(r.Op != x.OpSource && r.Op != x.OpReserve && r.Op != x.OpTerminal && r.Op != x.OpCancel) {
			return refusal(x.CodeNotOwner)
		}
		if r.Reserve != nil && r.Reserve.ReceiverInstance != p.InstanceID {
			return refusal(x.CodeNotOwner)
		}
	case x.RoleSettlement:
		// P19 authenticates this principal against the exact capsule/NKey and
		// retained entry. This narrow role cannot reacquire bytes or a grant.
		if !validIdentity(r.Identity, true) || !validToken(p.InstanceID) ||
			(r.Op != x.OpStatus && r.Op != x.OpTerminal && r.Op != x.OpCancel) {
			return refusal(x.CodeNotOwner)
		}
	case x.RoleBroker:
		if r.Op != x.OpProbe && r.Op != x.OpCensus && r.Op != x.OpBucketConfig {
			return refusal(x.CodeNotOwner)
		}
	default:
		return refusal(x.CodeNotMember)
	}
	if r.Outcome != nil && r.Outcome.Grant != (x.CommitGrant{}) &&
		(!validToken(p.InstanceID) || r.Outcome.Grant.ReceiverInstance != p.InstanceID) {
		return refusal(x.CodeNotOwner)
	}
	return nil
}

func validReason(reason x.CancelReason) bool {
	switch reason {
	case x.CancelUser, x.CancelDeadline, x.CancelRevoked, x.CancelStopped:
		return true
	default:
		return false
	}
}

func validateOutcome(o x.ReceiverOutcome, id x.Identity) error {
	if o.Bytes < 0 || o.Bytes > proto.XferMaxBytes || (o.SHA256 != "" && !validSHA(o.SHA256)) {
		return refusal(x.CodeIntentConflict)
	}
	if o.Grant != (x.CommitGrant{}) && (o.Grant.Identity != id || !validToken(o.Grant.GrantNonce) ||
		!validToken(o.Grant.ReceiverInstance) || o.Grant.TerminalID != terminalID(id)) {
		return refusal(x.CodeIntentConflict)
	}
	switch o.Outcome {
	case x.OutcomeNotEntered:
		if o.ParentSynced {
			return refusal(x.CodeIntentConflict)
		}
	case x.OutcomeInflight, x.OutcomeLanded:
		if o.Grant == (x.CommitGrant{}) || (o.Primitive != x.PrimitiveRename && o.Primitive != x.PrimitiveLink) ||
			(o.Outcome == x.OutcomeInflight && o.ParentSynced) {
			return refusal(x.CodeIntentConflict)
		}
	default:
		return refusal(x.CodeIntentConflict)
	}
	return nil
}

// DecodeWire validates the public, closed DTO before constructing the neutral
// request. Subject scope is supplied by the authenticated outer adapter; body
// fields cannot replace its actor, node or transfer identity.
// origin: hot-update S7 plan §6.2 / T39
func DecodeWire(op x.Operation, data []byte, scope x.Identity) (x.Request, error) {
	r := x.Request{Op: op, Identity: scope}
	var wire any
	switch op {
	case x.OpProbe:
		wire = &PullSelectionProbeReq{}
	case x.OpPermit:
		wire = &proto.TransferPermitReq{}
	case x.OpStatus:
		wire = &proto.TransferStatusReq{}
	case x.OpSource:
		wire = &proto.TransferSourceReq{}
	case x.OpReserve:
		wire = &proto.TransferReserveReq{}
	case x.OpTerminal:
		wire = &proto.TransferTerminalReq{}
	case x.OpCancel:
		wire = &proto.TransferCancelReq{}
	case x.OpPrepare, x.OpCommit, x.OpFinalize, x.OpCensus, x.OpBucketConfig:
		// Prepare/commit arrive on the legacy carriers; finalize shares the
		// terminal subject; census/bucket-config are routed broker verbs.
		return r, refusal(x.CodePayloadUnsupported)
	default:
		return r, refusal(x.CodePayloadUnsupported)
	}
	if err := proto.DecodeTransferWire(data, wire); err != nil {
		return r, refusal(x.CodeIntentConflict)
	}
	switch v := wire.(type) {
	case *PullSelectionProbeReq:
		r.CallID = v.CallID
		if v.SID != scope.SID || v.NID != scope.NID {
			return r, refusal(x.CodeIntentConflict)
		}
		if v.SourcePath != "" {
			r.Intent = &x.Intent{Verb: "pull", Path: v.SourcePath}
		}
	case *proto.TransferPermitReq:
		r.CallID = v.CallID
		r.Intent = new(x.Intent)
		*r.Intent = x.Intent(v.Intent)
		r.Identity.AgentInstance = v.AgentInstance
		if v.TransferID != scope.TransferID {
			return r, refusal(x.CodeIntentConflict)
		}
	case *proto.TransferStatusReq:
		r.CallID = v.CallID
		r.Identity = x.Identity(v.Identity)
		r.Origin = x.Origin(v.Origin)
	case *proto.TransferSourceReq:
		r.CallID = v.CallID
		r.Identity = x.Identity(v.Identity)
		r.Origin = x.Origin(v.Origin)
		r.Source = new(x.SourceSeal)
		*r.Source = x.SourceSeal(v.Source)
	case *proto.TransferReserveReq:
		r.CallID = v.CallID
		r.Identity = x.Identity(v.Identity)
		r.Origin = x.Origin(v.Origin)
		r.Reserve = &x.ReserveRequest{CallID: v.CallID, Origin: r.Origin, ReceiverInstance: v.ReceiverInstance}
	case *proto.TransferTerminalReq:
		r.CallID = v.CallID
		r.Identity = x.Identity(v.Identity)
		r.Origin = x.Origin(v.Origin)
		r.Outcome = new(x.ReceiverOutcome)
		if err := projectWire(v.Outcome, r.Outcome); err != nil {
			return r, err
		}
	case *proto.TransferCancelReq:
		r.CallID = v.CallID
		r.Identity = x.Identity(v.Identity)
		r.Origin = x.Origin(v.Origin)
		r.Cancel = &x.CancelRequest{CallID: v.CallID, Origin: r.Origin, Reason: v.Reason}
	}
	if r.Identity.SID != scope.SID || r.Identity.NID != scope.NID || r.Identity.TransferID != scope.TransferID || (scope.ActorFP != "" && r.Identity.ActorFP != scope.ActorFP) {
		return r, refusal(x.CodeNotOwner)
	}
	return r, nil
}

// WireResponse preserves the operation-specific DTO, including exact receipt
// and origin fields. The neutral Response is never itself put on the wire.
func WireResponse(op x.Operation, r x.Response) (any, error) {
	switch op {
	case x.OpProbe:
		out := proto.TransferProbeResp{Code: r.Code, Error: r.Error}
		if r.Probe != nil {
			out = proto.TransferProbeResp(*r.Probe)
		}
		if r.Source != nil {
			size := r.Source.Size
			return PullSelectionProbeResp{Version: out.Version, HistoryGuardVersion: out.HistoryGuardVersion, BucketConfigVersion: out.BucketConfigVersion,
				InstanceID: out.InstanceID, MaxPayload: out.MaxPayload, MaxMsgSize: out.MaxMsgSize, Code: out.Code, Error: out.Error, SourceSize: &size}, nil
		}
		return out, nil
	case x.OpPermit:
		out := proto.TransferPermitResp{Code: r.Code, Error: r.Error}
		if r.Permit != nil {
			out.Permit = new(proto.CreationPermit)
			if err := projectWire(r.Permit, out.Permit); err != nil {
				return nil, err
			}
		}
		return out, nil
	case x.OpReserve, x.OpCommit:
		out := proto.TransferReserveResp{Code: r.Code, Error: r.Error}
		if r.Grant != nil {
			out.Grant = new(proto.CommitGrant)
			if err := projectWire(r.Grant, out.Grant); err != nil {
				return nil, err
			}
		}
		return out, nil
	case x.OpTerminal, x.OpFinalize:
		out := proto.TransferTerminalResp{Code: r.Code, Error: r.Error}
		if r.Receipt != nil {
			out.Receipt = new(proto.TerminalReceipt)
			if err := projectWire(r.Receipt, out.Receipt); err != nil {
				return nil, err
			}
		}
		return out, nil
	case x.OpStatus, x.OpSource, x.OpCancel, x.OpPrepare:
		out := proto.TransferStatusResp{Code: r.Code, Error: r.Error}
		if r.Status != nil {
			s := r.Status
			out.State, out.Phase, out.Identity, out.Origin = s.State, s.Phase, proto.Identity(s.Identity), proto.Origin(s.Origin)
			out.Budget = proto.BudgetSeal(s.Budget)
			out.DurableOffset, out.ChunkIndex, out.LastConsumedStreamSeq = s.DurableOffset, s.ChunkIndex, s.LastConsumedStreamSeq
			out.CleanupPending, out.Code, out.Error = s.CleanupPending, s.Code, s.Error
			if s.Terminal != nil {
				out.Terminal = new(proto.TerminalReceipt)
				*out.Terminal = proto.TerminalReceipt(*s.Terminal)
			}
		}
		return out, nil
	case x.OpCensus, x.OpBucketConfig:
		// Routed broker verbs answer on the cluster forwarding envelope.
		return nil, errors.New("transfer operation has no public response")
	default:
		return nil, errors.New("transfer operation has no public response")
	}
}

// EncodePullSelectionProbe keeps the optional query under the same closed
// wire validation as capability-only probes, before any request can escape.
func EncodePullSelectionProbe(actor string, r x.Request) (string, []byte, error) {
	// This extension is a path-only query. Dropping creation or settlement
	// fields here would make different readers disagree on the operation.
	// origin: hot-update S7 plan §6.2 / S7 internal review INTEG-O F4
	if r.Op != x.OpProbe || r.Intent == nil || *r.Intent != (x.Intent{Verb: "pull", Path: r.Intent.Path}) || r.Intent.Path == "" || r.Permit != nil || r.Source != nil || r.Reserve != nil || r.Outcome != nil || r.Cancel != nil || len(r.Inline) != 0 {
		return "", nil, errors.New(x.CodeIntentConflict)
	}
	subject, err := proto.SubjCtrlTransferRequest(actor, r.Identity.SID, r.Identity.NID, r.Identity.TransferID, r.Op)
	if err != nil {
		return "", nil, err
	}
	body, err := json.Marshal(PullSelectionProbeReq{
		CallID: r.CallID, SID: r.Identity.SID, NID: r.Identity.NID, SourcePath: r.Intent.Path})
	return subject, body, err
}

// DecodePullSelectionProbe distinguishes absent inspection evidence from a
// real zero-sized file without relaxing duplicate/unknown-field validation.
func DecodePullSelectionProbe(b []byte) (x.Response, error) {
	var reply PullSelectionProbeResp
	if err := proto.DecodeTransferWire(b, &reply); err != nil {
		return x.Response{}, err
	}
	p := x.ProbeResult{Version: reply.Version, HistoryGuardVersion: reply.HistoryGuardVersion, BucketConfigVersion: reply.BucketConfigVersion,
		InstanceID: reply.InstanceID, MaxPayload: reply.MaxPayload, MaxMsgSize: reply.MaxMsgSize, Code: reply.Code, Error: reply.Error}
	response := x.Response{Probe: &p, Code: p.Code, Error: p.Error}
	if reply.SourceSize != nil {
		response.Source = &x.SourceSeal{Size: *reply.SourceSize}
	}
	return response, nil
}

// These are already validated typed values with identical frozen field names.
// Using their JSON tags avoids a second hand-maintained nested-field mapper.
func projectWire(from, to any) error {
	b, err := json.Marshal(from)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, to)
}

// AgentCapability is what a node's last NORMAL register advertised: its run
// instance and transfer version. An empty version from a complete register is
// the explicit, authenticated cap0 answer; the farewell path never installs it.
// origin: hot-update S7 plan §6.2 / §8.4 / §9
type AgentCapability struct {
	InstanceID, Version string
}

// AgentCapabilities is the broker-side register record. It is keyed by the
// authenticated register subject (sid, nid), so a request body cannot name a
// different instance. A missing record is unknown, never cap0.
// origin: hot-update S7 plan §6.2 / §9 / INTEG-D capability compatibility
type AgentCapabilities struct {
	mu      sync.Mutex
	records map[[2]string]AgentCapability
}

func (c *AgentCapabilities) Record(sid, nid, instance, version string) {
	if c == nil || sid == "" || nid == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.records == nil {
		c.records = make(map[[2]string]AgentCapability)
	}
	c.records[[2]string{sid, nid}] = AgentCapability{InstanceID: instance, Version: version}
}

func (c *AgentCapabilities) Lookup(sid, nid string) (AgentCapability, bool) {
	if c == nil {
		return AgentCapability{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.records[[2]string{sid, nid}]
	return v, ok
}

// AgentTransport is the controller's broker→agent port. Probe is answered from
// the register record and today's payload limit; prepare and the commit Grant
// travel to the original agent instance on its directed node CMD subjects. It
// owns no data attachment: the broker never writes or reads transfer bytes.
// origin: hot-update S7 plan §6.3 / §8.1 / INTEG-D outer adapter
type AgentTransport struct {
	Conn   func() *nats.Conn
	Agents func(sid, nid string) (AgentCapability, bool)
	// Status reads the admitted entry so a forwarded prepare carries the sealed
	// original budget instead of letting the agent renew its own deadline.
	Status func(x.StatusRequest, x.Deadline) (x.Status, error)
}

var _ x.Transport = (*AgentTransport)(nil)

// legacyAgentInstance names a cap0 agent that predates run instances. It is
// returned only beside all-empty versions, so it can never admit creation.
const legacyAgentInstance = "legacy-agent"

func (t *AgentTransport) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	switch r.Op {
	case x.OpProbe:
		return t.probe(r)
	case x.OpPrepare:
		return t.prepare(r, d)
	case x.OpPermit, x.OpStatus, x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpCancel, x.OpFinalize, x.OpCensus, x.OpBucketConfig:
		// The commit carrier is Deliver, not Request: it needs the Grant the
		// broker persisted, which a generic controller request does not carry.
		return x.Response{}, x.Attempt{}, refusal(x.CodePayloadUnsupported)
	default:
		return x.Response{}, x.Attempt{}, refusal(x.CodePayloadUnsupported)
	}
}

func (t *AgentTransport) probe(r x.Request) (x.Response, x.Attempt, error) {
	if t.Agents == nil || t.Conn == nil {
		return x.Response{}, x.Attempt{}, refusal(x.CodeOwnerRecovering)
	}
	capability, ok := t.Agents(r.Identity.SID, r.Identity.NID)
	nc := t.Conn()
	if !ok || nc == nil || nc.IsClosed() {
		// No normal register has been seen by this broker since it started.
		// That is not evidence of cap0; the ctl must retry, not downgrade.
		return x.Response{}, x.Attempt{}, refusal(x.CodeOwnerRecovering)
	}
	probe := &x.ProbeResult{InstanceID: capability.InstanceID, MaxPayload: nc.MaxPayload(), MaxMsgSize: nc.MaxPayload()}
	switch capability.Version {
	case x.Version:
		if capability.InstanceID == "" {
			return x.Response{}, x.Attempt{}, refusal(x.CodeOwnerRecovering)
		}
		// This broker answers with its own installed history guard and bucket
		// coordinator; the controller runs only inside an installed graph.
		probe.Version, probe.HistoryGuardVersion, probe.BucketConfigVersion = x.Version, x.Version, x.Version
	case "":
		if probe.InstanceID == "" {
			probe.InstanceID = legacyAgentInstance
		}
	default:
		return x.Response{}, x.Attempt{}, refusal(x.CodePayloadUnsupported)
	}
	return x.Response{Probe: probe}, x.Attempt{}, nil
}

// prepare forwards only to the instance named by the permit. A restarted agent
// is a different instance and refuses the old permit itself.
func (t *AgentTransport) prepare(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	if t.Status == nil {
		return x.Response{}, x.Attempt{}, refusal(x.CodeOwnerRecovering)
	}
	status, err := t.Status(x.StatusRequest{CallID: r.CallID, Identity: r.Identity, Origin: r.Origin}, d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	if status.State != x.StatusLive || status.Identity != r.Identity || status.Origin != r.Origin {
		return x.Response{}, x.Attempt{}, refusal(x.CodeResultUnknown)
	}
	verb, body, err := proto.EncodeAgentTransferCarrier(r, status.Budget, nil, nil)
	if err != nil {
		return x.Response{}, x.Attempt{}, refusal(x.CodeIntentConflict)
	}
	return t.forward(r, verb, body, d)
}

// Deliver sends the Grant the tracker already persisted to its original
// receiver. Losing this reply loses nothing: the Grant is durable and the ctl
// reconciles by status before replaying the same commit CallID, which reaches
// the same Grant and an idempotent receiver Reserve.
// origin: hot-update S7 plan §4 / §7 / INTEG-D outer commit adapter
func (t *AgentTransport) Deliver(r x.Request, grant x.CommitGrant, source *x.SourceSeal, d x.Deadline) (x.Response, x.Attempt, error) {
	if t.Status == nil {
		return x.Response{}, x.Attempt{}, refusal(x.CodeOwnerRecovering)
	}
	status, err := t.Status(x.StatusRequest{CallID: r.CallID, Identity: r.Identity, Origin: r.Origin}, d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	if status.State != x.StatusLive || status.Identity != r.Identity || status.Origin != r.Origin {
		return x.Response{}, x.Attempt{}, refusal(x.CodeResultUnknown)
	}
	commit := x.Request{Op: x.OpCommit, CallID: r.CallID, Identity: r.Identity, Origin: r.Origin}
	verb, body, err := proto.EncodeAgentTransferCarrier(commit, status.Budget, source, &grant)
	if err != nil {
		return x.Response{}, x.Attempt{}, refusal(x.CodeIntentConflict)
	}
	return t.forward(commit, verb, body, d)
}

func (t *AgentTransport) forward(r x.Request, verb string, body []byte, d x.Deadline) (x.Response, x.Attempt, error) {
	var attempt x.Attempt
	nc := t.Conn()
	if nc == nil || nc.IsClosed() {
		return x.Response{}, attempt, refusal(x.CodeOwnerRecovering)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return x.Response{}, attempt, err
	}
	// ctx-none: the frozen Transport port supplies an absolute deadline, no caller ctx.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	reply, err := nc.RequestWithContext(ctx, proto.SubjCmdForwarded(r.Identity.SID, r.Identity.NID, verb), body)
	if err != nil {
		// No responders proves no agent consumed it; every other failure after
		// the publish may have reached the agent and is reconciled by status.
		attempt.Sent = !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrConnectionClosed) &&
			!errors.Is(err, nats.ErrInvalidConnection)
		return x.Response{}, attempt, err
	}
	attempt.Sent = true
	response, err := proto.DecodeTransferResponse(r.Op, verb, reply.Data)
	if err != nil {
		return x.Response{}, attempt, refusal(x.CodeResultUnknown)
	}
	return response, attempt, nil
}

func (t *AgentTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, refusal(x.CodePayloadUnsupported)
}

func (t *AgentTransport) CloseData(x.Identity, x.Deadline) error {
	return refusal(x.CodePayloadUnsupported)
}

// CommitCarrier is the outer adapter for the legacy push-commit carrier. The
// frozen OpCommit has no Reserve branch on the ctl wire: derive the ORIGINAL
// receiver from the retained identity, reserve its durable Grant once through
// the controller, and deliver that Grant. It never executes a legacy primitive
// or invents a receiver, and a lost reply is resolved by status-first replay.
// origin: hot-update S7 plan §4 / §6.3 / INTEG-D outer commit adapter
func CommitCarrier(controller x.Controller, agents *AgentTransport, source func(x.Identity, x.Deadline) (*x.SourceSeal, error),
	r x.Request, who x.Principal, d x.Deadline) (x.Response, error) {
	if controller == nil || agents == nil || source == nil || r.Op != x.OpCommit || r.Reserve != nil {
		return x.Response{Code: x.CodeOwnerRecovering, Error: x.CodeOwnerRecovering}, refusal(x.CodeOwnerRecovering)
	}
	r.Reserve = &x.ReserveRequest{CallID: r.CallID, Origin: r.Origin, ReceiverInstance: r.Identity.AgentInstance}
	response, err := controller.Handle(r, who, d)
	if err != nil || response.Code != "" || response.Error != "" {
		return response, err
	}
	if response.Grant == nil {
		return x.Response{Code: x.CodeResultUnknown, Error: x.CodeResultUnknown}, refusal(x.CodeResultUnknown)
	}
	seal, err := source(r.Identity, d)
	if err != nil {
		return errorResponse(err)
	}
	delivered, attempt, err := agents.Deliver(r, *response.Grant, seal, d)
	if err != nil {
		if attempt.Sent {
			return errorResponse(&Error{Code: x.CodeReconcileRequired, cause: err})
		}
		return errorResponse(err)
	}
	if delivered.Code != "" {
		return errorResponse(refusal(delivered.Code))
	}
	if delivered.Status == nil || delivered.Status.Identity != r.Identity || delivered.Status.Origin != r.Origin {
		return errorResponse(refusal(x.CodeResultUnknown))
	}
	return x.Response{Status: delivered.Status}, nil
}
