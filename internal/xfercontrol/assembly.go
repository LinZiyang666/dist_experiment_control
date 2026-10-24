package xfercontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/nats-io/nkeys"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// AssemblyConfig supplies environment authority, never replacement leaf
// implementations. The constructors below are the landed production owners.
// Original-origin discovery and writer retirement are explicit ports: an empty
// local tracker cannot invent either proof for a cluster.
// origin: hot-update S7 plan §5 / §6.4 / §8.2 / P22
type AssemblyConfig struct {
	x.Base
	Origin             x.Origin
	Root, FallbackRoot string
	JS                 jetstream.JetStream
	Authority          x.Authority
	Permits            x.PermitSigner
	Transport          x.Transport
	Buckets            x.BucketPort
	Receiver           x.ReceiverResolver
	Objects            x.ObjectRetirer
	Owners             x.OwnerResolver
	SessionOwners      xferauth.SessionOwners
}

type Runtime struct {
	Tracker    x.TrackerManager
	Ledger     x.LedgerPort
	Controller x.Controller
	Finalizer  x.FinalizerPort
	Collector  x.Collector
	Buckets    x.BucketPort
	Settlement xferauth.SessionBarrier
}

// Assemble returns a sleeping tracker. Its first Bind reconciles the actual
// primary/outbox census before admission, subscriptions or destructive passes.
// On partial construction the returned Runtime still owns every acquired leaf.
func Assemble(cfg AssemblyConfig) (*Runtime, error) {
	r := &Runtime{Buckets: cfg.Buckets}
	if cfg.JS == nil || cfg.Authority == nil || cfg.Permits == nil || cfg.Transport == nil || cfg.Buckets == nil || cfg.Receiver == nil || cfg.Objects == nil || cfg.Owners == nil || cfg.SessionOwners == nil {
		return r, refusal(x.CodeOwnerRecovering)
	}
	var err error
	r.Ledger, err = xferledger.New(x.LedgerConfig{Base: cfg.Base, Root: cfg.Root, FallbackRoot: cfg.FallbackRoot})
	if err != nil {
		return r, err
	}
	history := sessionHistory{base: cfg.Base, js: cfg.JS}
	final := &finalizerBinding{}
	r.Tracker, err = xfertrack.Import(x.TrackerConfig{Base: cfg.Base, Origin: cfg.Origin, Ledger: r.Ledger, History: history, ResolveReceiver: cfg.Receiver, Finalizer: final}, x.TrackerState{
		Schema: x.StateSchema, Origin: cfg.Origin, Generation: cfg.Origin.Generation, Admission: x.AdmissionState{Closed: true}, ProvisioningFence: x.AdmissionState{Closed: true},
	})
	if err != nil {
		return r, err
	}
	decisions, err := xfertrack.Decisions(r.Tracker)
	if err != nil {
		return r, err
	}
	r.Finalizer, err = xferfinal.New(x.FinalConfig{Base: cfg.Base, LegacyOrphans: &x.LegacyOrphanConfig{JS: cfg.JS, Owners: cfg.Owners}, Decisions: decisions, Ledger: r.Ledger, History: history, Objects: cfg.Objects, ResolveReceiver: cfg.Receiver})
	if err != nil {
		return r, err
	}
	final.port = r.Finalizer
	r.Collector, err = xfergc.New(x.GCConfig{Base: cfg.Base, JS: cfg.JS, Owners: cfg.Owners, Finalizer: r.Finalizer})
	if err != nil {
		return r, err
	}
	r.Controller, err = New(Config{Base: cfg.Base, Tracker: r.Tracker, Authority: cfg.Authority, Permits: cfg.Permits, Buckets: cfg.Buckets, Transport: cfg.Transport})
	if err != nil {
		return r, err
	}
	r.Settlement, err = xferauth.NewSettlement(xferauth.SettlementConfig{Owners: cfg.SessionOwners, Tracker: r.Tracker, Ledger: r.Ledger, Finalizer: r.Finalizer, UTC: cfg.UTC})
	return r, err
}

// This binding closes the tracker/finalizer cycle before the sleeping tracker
// can run. A missing target is an error, never a successful no-op finalizer.
type finalizerBinding struct{ port x.FinalizerPort }

func (f *finalizerBinding) Advance(id x.Identity, d x.Deadline) (x.Status, error) {
	if f.port == nil {
		return x.Status{}, refusal(x.CodeOwnerRecovering)
	}
	return f.port.Advance(id, d)
}
func (f *finalizerBinding) Stop(d x.Deadline) error {
	if f.port == nil {
		return errors.New("transfer finalizer not installed")
	}
	return f.port.Stop(d)
}

type sessionHistory struct {
	base x.Base
	js   jetstream.JetStream
}

func (h sessionHistory) Confirm(r x.CanonicalRecord, d x.Deadline) (x.HistoryReceipt, error) {
	p, err := xferhistory.New(x.HistoryConfig{Base: h.base, JS: h.js, SID: r.Identity.SID})
	if err != nil {
		return x.HistoryReceipt{}, err
	}
	return p.Confirm(r, d)
}
func (h sessionHistory) Lookup(k x.Key, d x.Deadline) (x.HistorySet, error) {
	p, err := xferhistory.New(x.HistoryConfig{Base: h.base, JS: h.js, SID: k.SID})
	if err != nil {
		return x.HistorySet{}, err
	}
	return p.Lookup(k, d)
}

// Subscription carries an already-authenticating outer adapter. Cold and hot
// callers consume this same closed descriptor and never queue original owners.
// origin: hot-update S7 plan §6.3 / §8.2 / T39
type Subscription struct {
	Subject string
	Handler nats.MsgHandler
}

// ValidateSubscriptions rejects incomplete, duplicate or foreign descriptors.
func ValidateSubscriptions(subscriptions []Subscription) error {
	expected := make(map[string]bool)
	for _, registration := range Registrations() {
		if registration.LegacyCarrier || registration.ClusterVerb != "" {
			continue
		}
		if !registration.PlainBroadcast {
			return fmt.Errorf("%s: continuation registration is not broadcast", x.CodeOwnerRecovering)
		}
		expected[registration.Subject] = true
	}
	if len(expected) == 0 || len(subscriptions) != len(expected) {
		return fmt.Errorf("%s: incomplete transfer registrations", x.CodeOwnerRecovering)
	}
	seen := make(map[string]bool, len(subscriptions))
	for _, registration := range subscriptions {
		if !expected[registration.Subject] || registration.Handler == nil || seen[registration.Subject] {
			return fmt.Errorf("%s: invalid transfer registration", x.CodeOwnerRecovering)
		}
		seen[registration.Subject] = true
	}
	return nil
}

func OrderSubscriptions(subscriptions []Subscription) []Subscription {
	bySubject := make(map[string]nats.MsgHandler, len(subscriptions))
	for _, registration := range subscriptions {
		bySubject[registration.Subject] = registration.Handler
	}
	var result []Subscription
	for _, registration := range Registrations() {
		if registration.LegacyCarrier || registration.ClusterVerb != "" {
			continue
		}
		result = append(result, Subscription{Subject: registration.Subject, Handler: bySubject[registration.Subject]})
	}
	return result
}

// BindRuntime validates the real durable census before activating a sleeping
// tracker. The caller retains and stops every partial owner on error.
// origin: hot-update S7 plan §5.3 / §8.2 / M11
func BindRuntime(current Runtime, bindings x.Bindings, deadline x.Deadline) error {
	if current.Tracker == nil || current.Ledger == nil || current.Controller == nil ||
		current.Finalizer == nil || current.Collector == nil || current.Buckets == nil {
		return fmt.Errorf("%s: incomplete broker transfer graph", x.CodeOwnerRecovering)
	}
	if bindings.Ledger == nil || bindings.JS == nil ||
		bindings.Authority == nil || bindings.Transport == nil || bindings.OwnerGeneration == 0 {
		return fmt.Errorf("%s: incomplete current transfer bindings", x.CodeOwnerRecovering)
	}
	// Bind owns conflict reconciliation and retries retained dirty proposals
	// while admission is closed. An outer ready-only scan would prevent those
	// exact writes from ever reaching their Sync after storage recovers.
	// origin: S7 internal review rv-recovery F05
	bindings.Ledger = current.Ledger
	if err := current.Tracker.Bind(bindings, deadline); err != nil {
		return err
	}
	census, err := current.Ledger.Scan(deadline)
	if err != nil {
		return err
	}
	if !census.Ready || !census.Complete {
		return fmt.Errorf("%s: transfer ledger census is incomplete", x.CodeOwnerRecovering)
	}
	// A factory that accidentally constructs New instead of Import must not
	// discard retained grants or renew their original byte budget. Check actual
	// owner answers, rather than trusting the factory's successful return.
	for _, row := range census.Rows {
		status, err := current.Tracker.Status(x.StatusRequest{Identity: row.Identity, Origin: row.Origin}, deadline)
		if err != nil {
			return err
		}
		if (status.State != x.StatusLive && status.State != x.StatusTerminal) ||
			status.Identity != row.Identity || status.Origin != row.Origin || !status.Budget.StartedAt.Equal(row.BudgetSeal.StartedAt) ||
			status.Budget.EffectiveBudgetNS != row.BudgetSeal.EffectiveBudgetNS {
			return fmt.Errorf("%s: retained transfer identity or original budget was not restored", x.CodeOwnerRecovering)
		}
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return err
	}
	return nil
}

// StopRuntime closes producers before their borrowed finalizer/config ports.
// A failed join is retained responsibility; all remaining owners are attempted.
func StopRuntime(current Runtime, deadline x.Deadline) error {
	var result error
	// Close producers before the finalizer and configuration owner. Every
	// nonnil partial constructor is joined, even after an earlier Stop fails.
	if current.Controller != nil {
		result = errors.Join(result, current.Controller.Stop(deadline))
	}
	if current.Collector != nil {
		result = errors.Join(result, current.Collector.Stop(deadline))
	}
	if current.Tracker != nil {
		result = errors.Join(result, current.Tracker.Stop(deadline))
	}
	if current.Finalizer != nil {
		result = errors.Join(result, current.Finalizer.Stop(deadline))
	}
	if current.Buckets != nil {
		result = errors.Join(result, current.Buckets.Stop(deadline))
	}
	return result
}

// AdvanceRuntime schedules original rows. The finalizer alone decides exact
// history confirmation and primary-before-outbox retirement.
// origin: hot-update S7 plan §5.2 / §5.3 / P22
func AdvanceRuntime(current Runtime, deadline x.Deadline) (int, error) {
	census, err := current.Ledger.Scan(deadline)
	if err != nil {
		return 0, err
	}
	if !census.Ready || !census.Complete {
		return 0, fmt.Errorf("%s: transfer ledger census is incomplete", x.CodeOwnerRecovering)
	}
	var result error
	settled := 0
	for _, row := range census.Rows {
		status, err := current.Finalizer.Advance(row.Identity, deadline)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if status.Identity == row.Identity && status.State == x.StatusTerminal && status.Phase == x.PhaseSettled {
			settled++
		}
	}
	return settled, result
}

// LocalOwners is the single-broker authority set: the one retained origin is
// this broker's, and its durable ledger census is the complete census. A
// request for any other origin is an error, never an empty set; clustered
// brokers must supply the routed original-owner resolver instead.
// origin: hot-update S7 plan §5.3 / §6.4 / INTEG-A production ports
type LocalOwners struct {
	Origin x.Origin
	Ledger func() x.LedgerPort
}

func (o LocalOwners) Census(origin x.Origin, d x.Deadline) (x.LedgerCensus, error) {
	if origin != (x.Origin{}) && origin != o.Origin {
		return x.LedgerCensus{}, refusal(x.CodeOwnerRecovering)
	}
	ledger := o.ledger()
	if ledger == nil {
		return x.LedgerCensus{}, refusal(x.CodeOwnerRecovering)
	}
	census, err := ledger.Scan(d)
	if err != nil {
		return x.LedgerCensus{}, err
	}
	if census.Origin != (x.Origin{}) && census.Origin != o.Origin {
		return x.LedgerCensus{}, refusal(x.CodeOwnerRecovering)
	}
	census.Origin = o.Origin
	return census, nil
}

func (o LocalOwners) SessionCensus(sid string, d x.Deadline) ([]x.LedgerCensus, error) {
	census, err := o.Census(x.Origin{}, d)
	if err != nil {
		return nil, err
	}
	return []x.LedgerCensus{census}, nil
}

func (o LocalOwners) ledger() x.LedgerPort {
	if o.Ledger == nil {
		return nil
	}
	return o.Ledger()
}

// LedgerRow returns the retained durable row for exactly this identity. The
// adapters below read sealed facts from it; they never reconstruct them.
func LedgerRow(ledger x.LedgerPort, id x.Identity, d x.Deadline) (x.LedgerRow, error) {
	if ledger == nil {
		return x.LedgerRow{}, refusal(x.CodeOwnerRecovering)
	}
	census, err := ledger.Scan(d)
	if err != nil {
		return x.LedgerRow{}, err
	}
	if !census.Ready || !census.Complete {
		return x.LedgerRow{}, refusal(x.CodeOwnerRecovering)
	}
	for _, row := range census.Rows {
		if row.Identity == id {
			return row, nil
		}
	}
	return x.LedgerRow{}, refusal(x.CodeResultUnknown)
}

// LedgerSource is the commit adapter's source port: the push receiver reads
// exactly the geometry the ctl sealed into the original row. Tier A has none.
// origin: hot-update S7 plan §2.2 / §4 / INTEG-D outer commit adapter
func LedgerSource(ledger func() x.LedgerPort) func(x.Identity, x.Deadline) (*x.SourceSeal, error) {
	return func(id x.Identity, d x.Deadline) (*x.SourceSeal, error) {
		var port x.LedgerPort
		if ledger != nil {
			port = ledger()
		}
		row, err := LedgerRow(port, id, d)
		if err != nil {
			return nil, err
		}
		if id.ObjectVersion != "" && row.SourceSeal == nil {
			// Object bytes cannot be committed before the ctl's source seal.
			return nil, refusal(x.CodeSourceChanged)
		}
		if row.SourceSeal == nil {
			return nil, nil
		}
		seal := *row.SourceSeal
		return &seal, nil
	}
}

// LedgerCanonical returns the exact durable TERMINAL decision for a receipt the
// controller has just confirmed, so a receiver courier can bind those bytes
// instead of guessing them. A missing or mismatched record is not attached.
// origin: hot-update S7 plan §4 / §5.1 / INTEG-D courier canonical carrier
func LedgerCanonical(ledger x.LedgerPort, id x.Identity, receipt x.TerminalReceipt, d x.Deadline) *x.CanonicalRecord {
	row, err := LedgerRow(ledger, id, d)
	if err != nil || row.CanonicalTerminal == nil {
		return nil
	}
	record := *row.CanonicalTerminal
	if record.Identity != id || record.Kind != receipt.Kind || record.ReqID != receipt.ReqID || record.Digest != receipt.CanonicalDigest {
		return nil
	}
	record.Bytes = append([]byte(nil), record.Bytes...)
	return &record
}

// UnavailableReceiverPorts is the honest default until the original receiver
// can be asked for its primitive evidence and writer/consumer retirement is
// proven: cleanup stays pending and retains its responsibility rather than
// accepting a guessed outcome or purging an object still owned by a writer.
// origin: hot-update S7 plan §2.3 / §4 / INTEG-A production ports
type UnavailableReceiverPorts struct{}

func (UnavailableReceiverPorts) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, refusal(x.CodeOwnerRecovering)
}

func (UnavailableReceiverPorts) Retire(x.Identity, x.RetirementProof, x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{}, refusal(x.CodeOwnerRecovering)
}

// CarrierRuntime is an immutable view of the currently installed broker graph.
// The broker takes one atomic snapshot; these adapters never read mutable Config.
// origin: hot-update S7 plan §6.3 / INTEG-G broker responsibility split
type CarrierRuntime struct {
	Origin     x.Origin
	Bindings   x.Bindings
	Controller x.Controller
	Ledger     x.LedgerPort
}

// handleBrokerTransferCarrier routes a capable prepare/commit carried on the
// legacy push/pull/push-commit subjects to the original-owner controller, and
// reports false only for a body without a continuation (the one-shot legacy
// path). A malformed capable body gets no reply: it proves no authority to
// answer, and the sent-unknown client reconciles by status before replaying.
// origin: hot-update S7 plan §6.3 / §7 / INTEG-D outer adapter
func HandleCarrier(msg *nats.Msg, sid, actor, fp, nid, verb string, privateReply func(string, string) bool, originGate func(x.Origin) bool, current *CarrierRuntime) ([]byte, bool) {
	var peek struct {
		TransferID   string          `json:"transfer_id"`
		Continuation json.RawMessage `json:"continuation"`
	}
	if json.Unmarshal(msg.Data, &peek) != nil || len(peek.Continuation) == 0 || string(peek.Continuation) == "null" {
		return nil, false
	}
	if !privateReply(actor, msg.Reply) {
		return nil, true
	}
	op := x.OpPrepare
	if verb == "push-commit" {
		op = x.OpCommit
	}
	req, err := proto.DecodeTransferRequest(fp, sid, nid, peek.TransferID, op, verb, msg.Data)
	if err != nil || !originGate(req.Origin) {
		return nil, true
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return nil, true
	}
	resp := x.Response{Code: x.CodeOwnerRecovering, Error: x.CodeOwnerRecovering}
	if current != nil {
		authority, ok := current.Bindings.Authority.(xferauth.ControlAuthority)
		if !ok {
			return nil, true
		}
		who, authErr := authority.Principal(actor, req.Identity, d)
		switch {
		case authErr != nil:
			resp.Code, resp.Error = x.CodeNotOwner, x.CodeNotOwner
		case op == x.OpCommit:
			agents, _ := current.Bindings.Transport.(*AgentTransport)
			ledger := func() x.LedgerPort { return current.Ledger }
			resp, err = CommitCarrier(current.Controller, agents, LedgerSource(ledger), req, who, d)
		default:
			resp, err = current.Controller.Handle(req, who, d)
		}
		if errors.Is(err, ErrNotOwner) {
			return nil, true
		}
	}
	body, _ := proto.EncodeTransferResponse(req, resp)
	return body, true
}

// handleAgentTransferRequest serves the agent subtree. The subject's (sid, nid)
// is the node's own ACL scope; the principal's instance is the node's last
// normal register, never a body field, and the controller requires it to equal
// both the retained identity and any Grant's receiver.
// origin: hot-update S7 plan §6.3 / INTEG-A agent verbs
func HandleAgentRequest(reg Registration, msg *nats.Msg, originGate func(x.Origin) bool, lookup func(string, string) (AgentCapability, bool), current *CarrierRuntime) any {
	sid, nid, tid, op, ok := proto.ParseEvTransferRequest(msg.Subject)
	if !ok || op != reg.Operation || msg.Reply == "" {
		return nil
	}
	req, err := DecodeWire(op, msg.Data, x.Identity{SID: sid, NID: nid, TransferID: tid})
	if err != nil || !originGate(req.Origin) {
		return nil
	}
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return nil
	}
	resp := x.Response{Code: x.CodeOwnerRecovering, Error: x.CodeOwnerRecovering}
	capability, registered := lookup(sid, nid)
	if current != nil && registered && capability.Version == x.Version && capability.InstanceID != "" {
		who := x.Principal{NKey: "node:" + nid, SID: sid, NID: nid, InstanceID: capability.InstanceID, Role: x.RoleAgent}
		resp, err = current.Controller.Handle(req, who, d)
		if errors.Is(err, ErrNotOwner) {
			return nil
		}
	}
	if wire, err := WireResponse(op, resp); err == nil {
		return AttachEvidence(current, req, resp, wire, d)
	}
	return nil
}

// resolveTransferStatusOrigin fills the retained owner of a standalone status
// query (no Origin). A single broker owns its whole authority set: its
// complete durable census names the entry, supplying the exact identity and
// Origin, or proves absence at this origin. A clustered broker needs the
// routed origin census and stays silent, so it can never answer "unknown" for
// another broker's entry; an incomplete census is likewise not absence.
// origin: hot-update S7 plan §6.1 / INTEG-D standalone status
func ResolveStatusOrigin(clustered bool, current *CarrierRuntime, req x.Request, d controlbudget.Deadline) (x.Request, bool) {
	if req.Op != x.OpStatus || req.Origin != (x.Origin{}) {
		return req, true
	}
	if clustered || current == nil {
		return req, false
	}
	census, err := current.Ledger.Scan(d)
	if err != nil || !census.Ready || !census.Complete {
		return req, false
	}
	for _, row := range census.Rows {
		if row.Identity.SID == req.Identity.SID && row.Identity.TransferID == req.Identity.TransferID {
			if row.Identity.NID != req.Identity.NID {
				return req, false
			}
			req.Identity, req.Origin = row.Identity, row.Origin
			return req, true
		}
	}
	req.Origin = current.Origin
	return req, true
}

// attachTransferEvidence adds durable row evidence to a reply: the exact
// TERMINAL bytes beside a terminal receipt, so the receiver's courier binds
// them before its ACK instead of reconstructing a broker timestamp or ReqID;
// and the sealed source beside a Grant, so a pull receiver lands only bytes
// matching what the original sender sealed.
// origin: hot-update S7 plan §4 / §5.1 / INTEG-D courier canonical carrier
func AttachEvidence(current *CarrierRuntime, req x.Request, resp x.Response, wire any, d controlbudget.Deadline) any {
	if current == nil {
		return wire
	}
	switch out := wire.(type) {
	case proto.TransferTerminalResp:
		if resp.Receipt != nil && out.Receipt != nil {
			out.Canonical = proto.TransferCanonicalFrom(LedgerCanonical(current.Ledger, req.Identity, *resp.Receipt, d))
		}
		return out
	case proto.TransferReserveResp:
		if resp.Grant != nil && out.Grant != nil {
			if row, err := LedgerRow(current.Ledger, req.Identity, d); err == nil && row.SourceSeal != nil {
				source := proto.SourceSeal(*row.SourceSeal)
				out.Source = &source
			}
		}
		return out
	default:
		return wire
	}
}

// SingleConfig is one broker's transfer environment: a durable
// root (v1 ledger, outbox, permits), its retained Origin, the borrowed key that
// signs creation permits, and the shared capacity account in Base. A clustered
// broker needs routed census/receiver/config ports and is refused here.
// origin: hot-update S7 plan §5 / §6 / §8.2 / INTEG-A production ports
type SingleConfig struct {
	Base      x.Base
	Origin    x.Origin
	Root      string
	Issuer    nkeys.KeyPair
	DiskBytes int64
}

// SingleAssembly binds durable, current backend owners. Clustered brokers need
// routed census/receiver/config authority and cannot use this local constructor.
// origin: hot-update S7 plan §5 / §6 / INTEG-G responsibility move
func SingleAssembly(cfg SingleConfig, js jetstream.JetStream, clustered bool, transport *AgentTransport, ledger func() x.LedgerPort) (AssemblyConfig, error) {
	nc := transport.Conn()
	if clustered || js == nil || nc == nil || cfg.Root == "" || cfg.Issuer == nil || cfg.DiskBytes <= 0 {
		return AssemblyConfig{}, fmt.Errorf("%s: single-broker transfer environment is incomplete", x.CodeOwnerRecovering)
	}
	permits, err := xferauth.NewPermitAuthority(filepath.Join(cfg.Root, "permits"), cfg.Origin, cfg.Issuer, cfg.Base.UTC)
	if err != nil {
		return AssemblyConfig{}, err
	}
	authority, err := xferbucket.NewAuthority(x.ConfigWriterProof{BrokerID: cfg.Origin.BrokerID, InstanceID: cfg.Origin.InstanceID,
		BootID: cfg.Origin.BootID, Generation: 1, Nonce: cfg.Origin.InstanceID}, nil)
	if err != nil {
		return AssemblyConfig{}, err
	}
	buckets, err := xferbucket.New(x.BucketConfig{Base: cfg.Base, JS: js, Authority: authority,
		Limits: x.StorageLimits{DiskBytes: cfg.DiskBytes, MaxPayload: nc.MaxPayload(), MaxMsgSize: nc.MaxPayload()}})
	if err != nil {
		return AssemblyConfig{}, err
	}
	owners := LocalOwners{Origin: cfg.Origin, Ledger: ledger}
	unavailable := UnavailableReceiverPorts{}
	return AssemblyConfig{Base: cfg.Base, Origin: cfg.Origin, Root: filepath.Join(cfg.Root, "v1"), FallbackRoot: filepath.Join(cfg.Root, "outbox"),
		Permits: permits, Buckets: buckets, Receiver: unavailable, Objects: unavailable, Owners: owners, SessionOwners: owners,
		Transport: transport}, nil
}

// PassDeadline retains the caller's earlier deadline. A pass receives
// one five-second ceiling, not five seconds for each entry or directory.
// origin: hot-update S7 plan §1.2 / §5.3 / LQ3
func PassDeadline(until time.Time, stop <-chan struct{}) (controlbudget.Deadline, error) {
	select {
	case <-stop:
		return 0, context.Canceled
	default:
	}
	budget := 5 * time.Second
	if !until.IsZero() && time.Until(until) < budget {
		budget = time.Until(until)
	}
	return controlbudget.After(budget)
}
