package xfercontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// These ports are adversarial controller-boundary instruments, not substitute
// implementations of tracker persistence, N3 routing or worker effects. In
// particular Admit deliberately has no expiry/origin guard: those defects must
// be caught by the production controller rather than hidden by a second guard.
// P11/P19/P23 integration must additionally exercise the real owners and wire.
type controlFixture struct {
	now        time.Time
	boot       string
	id         x.Identity
	origin     x.Origin
	intent     x.Intent
	permit     x.CreationPermit
	principal  x.Principal
	status     x.Status
	statusErr  error
	createErr  error
	settleErr  error
	verifyErr  error
	issueErr   error
	probeErr   error
	forwardErr error
	sent       bool
	probe      x.ProbeResult
	seal       x.BucketSeal
	grant      x.CommitGrant
	receipt    x.TerminalReceipt
	calls      []string
	deadlines  []x.Deadline
	requests   []x.Request
	issues     int
	admits     int
	buckets    int
	forwards   int
	sources    int
	reserves   int
	finishes   int
	cancels    int
	ctl        Controller
}

func newControlFixture(t *testing.T) *controlFixture {
	t.Helper()
	f := &controlFixture{now: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC), boot: "boot-original"}
	f.intent = x.Intent{Verb: "push", Tier: "b", Path: "dir/file", Size: 1024 * 1024, SHA256: strings.Repeat("a", 64), ChunkSize: 32768}
	f.id = x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent-original", TransferID: "transfer", EntryNonce: "entry", IntentDigest: IntentDigest(f.intent), ObjectVersion: "original-NUID"}
	f.origin = x.Origin{BrokerID: "original-follower", InstanceID: "broker-process", BootID: "broker-boot", Generation: 7}
	f.permit = x.CreationPermit{Identity: f.id, Origin: f.origin, IssuedAt: f.now, CreateUntil: f.now.Add(30 * time.Second), PermitNonce: "permit", Signature: "authenticated-permit"}
	f.principal = x.Principal{NKey: "ctl-key", FP: f.id.ActorFP, SID: f.id.SID, NID: f.id.NID, InstanceID: "ctl-instance", Role: x.RoleCTL}
	f.status = x.Status{State: x.StatusUnknown, Identity: f.id, Origin: f.origin}
	f.probe = x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: f.id.AgentInstance, MaxPayload: 2 << 20, MaxMsgSize: -1}
	f.seal = x.BucketSeal{Bucket: proto.XferBucketName(f.id.SID), Stream: "OBJ_xfer-session", StreamCreated: "original-stream", ConfigDigest: "full-config", MaxBytes: 4 << 20, Duplicates: 2228 * time.Second, MaxAge: 48 * time.Hour, MaxPayload: 2 << 20, MaxMsgSize: -1}
	f.grant = x.CommitGrant{Identity: f.id, GrantNonce: "grant", ReceiverInstance: f.principal.InstanceID, TerminalID: terminalID(f.id)}
	f.receipt = x.TerminalReceipt{TerminalID: terminalID(f.id), Kind: "complete", CanonicalDigest: "canonical", ReqID: "req", HistoryStream: "history-session", HistorySeq: 5, Bytes: f.intent.Size, SHA256: f.intent.SHA256}
	cfg := f.config()
	var err error
	f.ctl, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.ctl.Stop(testDeadline(t)); err != nil {
			t.Errorf("stop: %v", err)
		}
	})
	return f
}

func (f *controlFixture) config() Config {
	return Config{
		Base:    x.Base{Clock: func() (string, int64, error) { n, err := controlbudget.Now(); return f.boot, n, err }, UTC: func() time.Time { return f.now }},
		Tracker: f, Authority: testAuthority{f}, Permits: testPermits{f}, Buckets: testBuckets{f}, Transport: testTransport{f},
	}
}

func testDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (f *controlFixture) record(name string, d x.Deadline) {
	f.calls = append(f.calls, name)
	f.deadlines = append(f.deadlines, d)
}

func (f *controlFixture) request(op x.Operation) x.Request {
	r := x.Request{Op: op, CallID: "logical-call", Identity: f.id, Origin: f.origin}
	switch op {
	case x.OpPrepare:
		i, p := f.intent, f.permit
		r.Intent, r.Permit = &i, &p
	case x.OpPermit:
		i := f.intent
		r.Intent, r.Origin = &i, x.Origin{}
		r.Identity.EntryNonce, r.Identity.IntentDigest, r.Identity.ObjectVersion = "", "", ""
	case x.OpSource:
		r.Source = &x.SourceSeal{Size: f.intent.Size, SHA256: f.intent.SHA256, ChunkSize: f.intent.ChunkSize, Chunks: 32, SourceFingerprint: "source-stat"}
	case x.OpReserve, x.OpCommit:
		r.Reserve = &x.ReserveRequest{CallID: r.CallID, Origin: r.Origin, ReceiverInstance: f.principal.InstanceID}
	case x.OpTerminal, x.OpFinalize:
		r.Outcome = &x.ReceiverOutcome{Grant: f.grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded, Bytes: f.intent.Size, SHA256: f.intent.SHA256, ParentSynced: true}
	case x.OpCancel:
		r.Cancel = &x.CancelRequest{CallID: r.CallID, Origin: r.Origin, Reason: x.CancelUser}
	}
	return r
}

func (f *controlFixture) live() { f.status.State, f.status.Phase = x.StatusLive, x.PhasePrepared }

func (f *controlFixture) Status(_ x.StatusRequest, d x.Deadline) (x.Status, error) {
	f.record("status", d)
	return f.status, f.statusErr
}
func (f *controlFixture) Admit(p x.CreationPermit, i x.Intent, d x.Deadline) (x.Status, error) {
	f.record("admit", d)
	f.admits++
	f.status = x.Status{State: x.StatusLive, Phase: x.PhaseOpen, Identity: p.Identity, Origin: p.Origin}
	return f.status, nil
}
func (f *controlFixture) SourceSeal(_ x.Identity, _ x.SourceSeal, d x.Deadline) (x.Status, error) {
	f.record("source", d)
	f.sources++
	return f.status, nil
}
func (f *controlFixture) Reserve(_ x.Identity, _ x.ReserveRequest, d x.Deadline) (x.CommitGrant, error) {
	f.record("reserve", d)
	f.reserves++
	return f.grant, nil
}
func (f *controlFixture) Finish(_ x.Identity, _ x.ReceiverOutcome, d x.Deadline) (x.TerminalReceipt, error) {
	f.record("finish", d)
	f.finishes++
	return f.receipt, nil
}
func (f *controlFixture) Cancel(_ x.Identity, _ x.CancelRequest, d x.Deadline) (x.Status, error) {
	f.record("cancel", d)
	f.cancels++
	return f.status, nil
}

type testAuthority struct{ f *controlFixture }

func (a testAuthority) Check(_ x.Identity, access x.AccessKind, d x.Deadline) error {
	a.f.record("authority-"+string(access), d)
	if access == x.AccessCreate {
		return a.f.createErr
	}
	if access == x.AccessSettle {
		return a.f.settleErr
	}
	return nil
}

type testPermits struct{ f *controlFixture }

func (p testPermits) Issue(_ x.Intent, _, _ string, d x.Deadline) (x.CreationPermit, error) {
	p.f.record("issue", d)
	p.f.issues++
	return p.f.permit, p.f.issueErr
}
func (p testPermits) Verify(x.CreationPermit) error { return p.f.verifyErr }

type testBuckets struct{ f *controlFixture }

func (b testBuckets) Apply(_ x.ConfigPatch, d x.Deadline) (x.BucketSeal, error) {
	b.f.record("bucket", d)
	b.f.buckets++
	return b.f.seal, nil
}
func (testBuckets) Freeze(x.Deadline) (x.ConfigWriterState, error) {
	return x.ConfigWriterState{}, errors.New("unexpected bucket freeze")
}
func (testBuckets) Stop(x.Deadline) error {
	return errors.New("controller must not stop borrowed bucket")
}

type testTransport struct{ f *controlFixture }

func (p testTransport) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	f := p.f
	f.record("transport-"+string(r.Op), d)
	f.requests = append(f.requests, r)
	if r.Op == x.OpProbe {
		return x.Response{Probe: &f.probe}, x.Attempt{}, f.probeErr
	}
	f.forwards++
	f.live()
	return x.Response{Status: &f.status}, x.Attempt{Sent: f.sent}, f.forwardErr
}
func (testTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("controller must not open data")
}
func (testTransport) CloseData(x.Identity, x.Deadline) error {
	return errors.New("controller must not close retained data")
}

func requireCode(t *testing.T, r x.Response, err error, code string) {
	t.Helper()
	if err == nil || r.Code != code || r.Error != errorMessage(code) || r.Status != nil || r.Permit != nil || r.Grant != nil || r.Receipt != nil || r.Probe != nil {
		t.Fatalf("response=%+v err=%v; want refusal %s without success evidence", r, err, code)
	}
}

// origin: hot-update S7 plan §6.1 / T26 / M07
func TestTransferPrepareReplayCannotCreateOnNewHome(t *testing.T) {
	f := newControlFixture(t)
	req := f.request(x.OpPrepare)
	resp, err := f.ctl.Handle(req, f.principal, testDeadline(t))
	if err != nil || resp.Status == nil || resp.Status.State != x.StatusLive || f.admits != 1 || f.forwards != 1 {
		t.Fatalf("initial original-follower admission: %+v %v, admits=%d forwards=%d", resp, err, f.admits, f.forwards)
	}
	// Rehome removes new-creation authority at the old follower, but preserves
	// the original entry. Even an expired permit must still read that entry.
	f.createErr = ErrNotOwner
	f.now = f.now.Add(6 * time.Minute)
	resp, err = f.ctl.Handle(req, f.principal, testDeadline(t))
	if err != nil || resp.Status == nil || resp.Status.Identity != req.Identity || f.admits != 1 || f.forwards != 1 || f.buckets != 1 {
		t.Fatalf("retained replay: %+v %v, admits=%d forwards=%d buckets=%d", resp, err, f.admits, f.forwards, f.buckets)
	}
	newHome := newControlFixture(t)
	newHome.statusErr = ErrNotOwner
	resp, err = newHome.ctl.Handle(req, newHome.principal, testDeadline(t))
	if !errors.Is(err, ErrNotOwner) || !reflect.DeepEqual(resp, x.Response{}) || newHome.admits != 0 || newHome.buckets != 0 || newHome.forwards != 0 {
		t.Fatalf("new home answered or created original transfer: %+v %v %+v", resp, err, newHome.calls)
	}
	// After result GC, expiry must stop a late original request before Admit.
	// Deliberately restore ONLINE/current-home authority: expiry is independent.
	f.createErr = nil
	f.status = x.Status{State: x.StatusUnknown, Identity: f.id, Origin: f.origin}
	resp, err = f.ctl.Handle(req, f.principal, testDeadline(t))
	requireCode(t, resp, err, x.CodePermitExpired)
	if f.admits != 1 || f.forwards != 1 || f.buckets != 1 || f.issues != 0 {
		t.Fatalf("expired replay regained side effects: admits=%d forwards=%d buckets=%d issues=%d", f.admits, f.forwards, f.buckets, f.issues)
	}
	for _, change := range []func(*x.Request){
		func(r *x.Request) { r.Origin.BrokerID = "new-home" },
		func(r *x.Request) { r.Intent.Force = !r.Intent.Force },
		func(r *x.Request) { r.Intent.RateLimitBPS = 1024 * 1024 },
		func(r *x.Request) { r.Identity.EntryNonce = "replacement" },
	} {
		r := f.request(x.OpPrepare)
		change(&r)
		resp, err = f.ctl.Handle(r, f.principal, testDeadline(t))
		requireCode(t, resp, err, x.CodeIntentConflict)
	}
}

// origin: hot-update S7 plan §6.1 / T28
func TestTransferStatusRequiresReadyOriginalOrigin(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"cold_census", refusal(x.CodeOwnerRecovering), x.CodeOwnerRecovering},
		{"corrupt_private_state", errors.New("unreadable private state with sensitive diagnostic"), x.CodeResultUnknown},
		{"original_origin_offline", controlbudget.ErrExpired, x.CodeResultUnknown},
		{"nonowner", ErrNotOwner, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newControlFixture(t)
			f.statusErr = tc.err
			r, err := f.ctl.Handle(f.request(x.OpStatus), f.principal, testDeadline(t))
			if tc.code == "" {
				if !errors.Is(err, ErrNotOwner) || !reflect.DeepEqual(r, x.Response{}) {
					t.Fatalf("nonowner must be silent: %+v %v", r, err)
				}
			} else {
				requireCode(t, r, err, tc.code)
			}
			if f.issues != 0 || f.admits != 0 || f.buckets != 0 || len(f.requests) != 0 {
				t.Fatal("read-only status caused a mutation or capability request")
			}
		})
	}
	t.Run("ready_absence_and_stale_continuation", func(t *testing.T) {
		f := newControlFixture(t)
		r, err := f.ctl.Handle(f.request(x.OpStatus), f.principal, testDeadline(t))
		if err != nil || r.Status == nil || r.Status.State != x.StatusUnknown {
			t.Fatalf("complete original-origin census: %+v %v", r, err)
		}
		f.createErr = refusal(x.CodeOwnerRecovering) // STALE: new admission is closed.
		f.live()
		for _, op := range []x.Operation{x.OpStatus, x.OpPrepare, x.OpSource, x.OpReserve, x.OpCancel} {
			if r, err := f.ctl.Handle(f.request(op), f.principal, testDeadline(t)); err != nil {
				t.Fatalf("STALE retained %s refused: %+v %v", op, r, err)
			}
		}
		f.status = x.Status{State: x.StatusUnknown, Identity: f.id, Origin: f.origin}
		r, err = f.ctl.Handle(f.request(x.OpPrepare), f.principal, testDeadline(t))
		requireCode(t, r, err, x.CodeOwnerRecovering)
		if f.admits != 0 || f.buckets != 0 || f.issues != 0 {
			t.Fatal("STALE new prepare crossed the create fence")
		}
	})
	t.Run("malformed_evidence", func(t *testing.T) {
		for _, damage := range []func(*x.Status){
			func(s *x.Status) { *s = x.Status{} },
			func(s *x.Status) { s.Origin.BrokerID = "foreign" },
			func(s *x.Status) { s.Origin.BootID = "rebooted" },
			func(s *x.Status) { s.Identity.EntryNonce = "replacement" },
			func(s *x.Status) { s.Phase = x.PhaseCommitReserved },
			func(s *x.Status) { s.CleanupPending = true },
		} {
			f := newControlFixture(t)
			damage(&f.status)
			r, err := f.ctl.Handle(f.request(x.OpStatus), f.principal, testDeadline(t))
			requireCode(t, r, err, x.CodeResultUnknown)
		}
	})
	t.Run("missing_origin_is_not_local_absence", func(t *testing.T) {
		f := newControlFixture(t)
		req := f.request(x.OpStatus)
		req.Origin = x.Origin{}
		r, err := f.ctl.Handle(req, f.principal, testDeadline(t))
		requireCode(t, r, err, x.CodeResultUnknown)
	})
}

// origin: hot-update S7 plan §6.3 / §7 / P16 capability-timeout mutation
func TestTransferCapabilityTimeoutNeverSelectsLegacy(t *testing.T) {
	for _, op := range []x.Operation{x.OpProbe, x.OpPermit, x.OpPrepare} {
		t.Run(string(op), func(t *testing.T) {
			f := newControlFixture(t)
			f.probeErr = controlbudget.ErrExpired
			r, err := f.ctl.Handle(f.request(op), f.principal, testDeadline(t))
			requireCode(t, r, err, x.CodeResultUnknown)
			if !errors.Is(err, controlbudget.ErrExpired) || f.issues != 0 || f.admits != 0 || f.buckets != 0 || f.forwards != 0 {
				t.Fatalf("timeout crossed creation fence: %v %+v", err, f.calls)
			}
		})
	}
	for _, field := range []string{"version", "history", "bucket", "instance"} {
		t.Run(field, func(t *testing.T) {
			f := newControlFixture(t)
			code := x.CodePayloadUnsupported
			switch field {
			case "version":
				f.probe.Version = ""
			case "history":
				f.probe.HistoryGuardVersion = ""
			case "bucket":
				f.probe.BucketConfigVersion = ""
			case "instance":
				f.probe.InstanceID = "replacement-agent"
				code = x.CodeResultUnknown
			}
			r, err := f.ctl.Handle(f.request(x.OpPermit), f.principal, testDeadline(t))
			requireCode(t, r, err, code)
			if f.issues != 0 {
				t.Fatal("permit issued without full current capability evidence")
			}
		})
	}
}

// origin: hot-update S7 plan §6.2 / §8.1 / P16
func TestTransferControllerRejectsCrossActorAndMixedPayloads(t *testing.T) {
	ops := []x.Operation{x.OpProbe, x.OpPermit, x.OpPrepare, x.OpStatus, x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpCancel, x.OpFinalize}
	for _, op := range ops {
		t.Run(string(op), func(t *testing.T) {
			f := newControlFixture(t)
			r := f.request(op)
			// Every branch gets an extra payload it does not own.
			if r.Cancel == nil {
				r.Cancel = &x.CancelRequest{}
			} else {
				r.Intent = &f.intent
			}
			resp, err := f.ctl.Handle(r, f.principal, testDeadline(t))
			requireCode(t, resp, err, x.CodePayloadUnsupported)
			if len(f.calls) != 0 {
				t.Fatal("mixed payload reached a port")
			}
		})
	}
	for _, mutate := range []func(*x.Request, *x.Principal){
		func(r *x.Request, _ *x.Principal) { r.Identity.ActorFP = "other" },
		func(_ *x.Request, p *x.Principal) { p.NID = "other" },
		func(r *x.Request, _ *x.Principal) { r.CallID = "call\nforged" },
		func(r *x.Request, _ *x.Principal) { r.Identity.TransferID = "*" },
		func(_ *x.Request, p *x.Principal) { p.Role = x.RoleSettlement },
		func(_ *x.Request, p *x.Principal) { p.Role = x.RoleAgent; p.InstanceID = "foreign" },
	} {
		f := newControlFixture(t)
		r, p := f.request(x.OpReserve), f.principal
		mutate(&r, &p)
		resp, err := f.ctl.Handle(r, p, testDeadline(t))
		if err == nil || resp.Grant != nil || len(f.calls) != 0 {
			t.Fatalf("unauthenticated grant crossed boundary: %+v %v %+v", resp, err, f.calls)
		}
	}
}

// origin: hot-update S7 plan §4 / §6.4 / P16
func TestTransferControllerRequiresExactTerminalEvidence(t *testing.T) {
	for _, op := range []x.Operation{x.OpTerminal, x.OpFinalize} {
		for _, damage := range []string{"none", "history", "opposite_identity", "abandoned_complete", "opposite_kind", "bytes"} {
			t.Run(string(op)+"/"+damage, func(t *testing.T) {
				f := newControlFixture(t)
				f.live()
				switch damage {
				case "history":
					f.receipt.HistorySeq = 0
				case "opposite_identity":
					f.receipt.TerminalID = "other-terminal"
				case "abandoned_complete":
					f.receipt.OutcomeUnknown = true
				case "opposite_kind":
					f.receipt.Kind = "failed"
				case "bytes":
					f.receipt.Bytes--
				}
				r, err := f.ctl.Handle(f.request(op), f.principal, testDeadline(t))
				if damage == "none" {
					if err != nil || r.Receipt == nil || *r.Receipt != f.receipt {
						t.Fatalf("exact terminal lost: %+v %v", r, err)
					}
				} else {
					requireCode(t, r, err, x.CodeTerminalConflict)
				}
			})
		}
	}
	f := newControlFixture(t)
	f.live()
	f.receipt.Kind, f.receipt.OutcomeUnknown, f.receipt.Code = "failed", true, x.ReasonSettlementAbandoned
	f.status.State, f.status.Phase, f.status.Terminal = x.StatusTerminal, x.PhaseCleanupPending, &f.receipt
	f.status.CleanupPending = true
	p := f.principal
	p.Role = x.RoleSettlement
	r, err := f.ctl.Handle(f.request(x.OpStatus), p, testDeadline(t))
	if err != nil || r.Status == nil || r.Status.Terminal == nil || !r.Status.Terminal.OutcomeUnknown || r.Status.Terminal.Kind != "failed" {
		t.Fatalf("abandonment was concealed: %+v %v", r, err)
	}
}

// origin: hot-update S7 plan §1.2 / §6.1 / T26 / lead LQ2
func TestTransferPermitBindsIntentAndCreationWindow(t *testing.T) {
	for _, change := range []string{"valid", "expired", "future", "excessive_window", "actor", "intent", "rate", "bad_signature"} {
		t.Run(change, func(t *testing.T) {
			f := newControlFixture(t)
			r := f.request(x.OpPermit)
			code := x.CodePermitExpired
			switch change {
			case "expired":
				f.now = f.permit.CreateUntil
			case "future":
				f.now = f.permit.IssuedAt.Add(-time.Second)
			case "excessive_window":
				f.permit.CreateUntil = f.permit.IssuedAt.Add(5*time.Minute + time.Nanosecond)
			case "actor":
				f.permit.Identity.ActorFP = "other"
				code = x.CodeIntentConflict
			case "intent":
				f.permit.Identity.IntentDigest = "different-digest"
				code = x.CodeIntentConflict
			case "rate":
				r.Intent.RateLimitBPS = 1
				code = x.CodeBudgetExceeded
			case "bad_signature":
				f.verifyErr = refusal(x.CodeNotOwner)
				code = x.CodeNotOwner
			}
			resp, err := f.ctl.Handle(r, f.principal, testDeadline(t))
			if change == "valid" {
				if err != nil || resp.Permit == nil || *resp.Permit != f.permit || f.issues != 1 {
					t.Fatalf("valid permit not bound: %+v %v issues=%d", resp, err, f.issues)
				}
			} else {
				requireCode(t, resp, err, code)
				if change == "rate" && (f.issues != 0 || len(f.calls) != 0) {
					t.Fatal("unachievable rate admitted before refusal")
				}
			}
		})
	}
}

// origin: hot-update S7 plan §2.1 / lead LQ3 / P16
func TestTransferPrepareRequiresStorageProofBeforeForward(t *testing.T) {
	for _, change := range []string{"duplicates", "ttl", "stream", "foreign_stream", "digest", "max_bytes", "capacity", "payload", "inline"} {
		t.Run(change, func(t *testing.T) {
			f := newControlFixture(t)
			r := f.request(x.OpPrepare)
			code := x.CodeStorageHorizon
			switch change {
			case "duplicates":
				f.seal.Duplicates = 120 * time.Second
			case "ttl":
				f.seal.MaxAge = time.Minute
			case "stream":
				f.seal.StreamCreated = ""
			case "foreign_stream":
				f.seal.Stream = "OBJ_xfer-foreign"
			case "digest":
				f.seal.ConfigDigest = ""
			case "max_bytes":
				f.seal.MaxBytes = 0
			case "capacity":
				f.seal.MaxBytes = f.intent.Size - 1
				code = x.CodeCapacityExceeded
			case "payload":
				f.seal.MaxPayload = int64(f.intent.ChunkSize - 1)
				code = x.CodePayloadUnsupported
			case "inline":
				// Tier A has no bucket proof; its proof is the exact inline
				// bytes. A carrier whose bytes disagree with the intent must be
				// refused before Admit, the bucket owner or the agent sees it.
				// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline path
				r.Intent.Tier, r.Intent.ChunkSize = "a", 0
				r.Identity.IntentDigest, r.Identity.ObjectVersion = IntentDigest(*r.Intent), ""
				r.Permit.Identity = r.Identity
				r.Inline = make([]byte, r.Intent.Size)
				f.status.Identity = r.Identity
				code = x.CodeIntentConflict
			}
			resp, err := f.ctl.Handle(r, f.principal, testDeadline(t))
			requireCode(t, resp, err, code)
			if f.forwards != 0 || (change == "inline" && (f.admits != 0 || f.buckets != 0)) {
				t.Fatalf("first-byte permission escaped without proof: %+v", f.calls)
			}
			if code == x.CodeStorageHorizon && (!strings.Contains(resp.Error, "Duplicates >= 2228s") || !strings.Contains(resp.Error, "MaxAge = 0 or >= 2228s")) {
				t.Fatalf("missing actionable horizon requirements: %q", resp.Error)
			}
		})
	}
}

// TestTransferInlinePrepareForwardsOnlyExactBytes: Tier A admits without any
// bucket proof, but only for the exact intended push bytes (and none for a
// pull); Tier B never carries inline bytes. The forwarded request is the
// original one, bytes included, so the agent receives exactly what was sealed.
// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline path
func TestTransferInlinePrepareForwardsOnlyExactBytes(t *testing.T) {
	payload := []byte("inline payload")
	sum := sha256.Sum256(payload)
	for _, tc := range []struct {
		name, verb, tier string
		inline           []byte
		code             string
	}{
		{"push-exact", "push", "a", payload, ""},
		{"push-one-byte-changed", "push", "a", append(append([]byte(nil), payload[:len(payload)-1]...), 'X'), x.CodeIntentConflict},
		{"push-truncated", "push", "a", payload[:3], x.CodeIntentConflict},
		{"pull-with-bytes", "pull", "a", payload, x.CodeIntentConflict},
		{"pull-empty", "pull", "a", nil, ""},
		{"tier-b-with-bytes", "push", "b", payload, x.CodeIntentConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newControlFixture(t)
			r := f.request(x.OpPrepare)
			r.Intent.Verb, r.Intent.Tier = tc.verb, tc.tier
			if tc.tier == "a" {
				r.Intent.ChunkSize, r.Identity.ObjectVersion = 0, ""
				r.Intent.Size, r.Intent.SHA256 = int64(len(payload)), hex.EncodeToString(sum[:])
				if tc.verb == "pull" {
					r.Intent.Size, r.Intent.SHA256 = 0, ""
				}
			}
			r.Identity.IntentDigest = IntentDigest(*r.Intent)
			r.Permit.Identity = r.Identity
			f.status.Identity = r.Identity
			r.Inline = tc.inline
			resp, err := f.ctl.Handle(r, f.principal, testDeadline(t))
			if tc.code != "" {
				requireCode(t, resp, err, tc.code)
				if f.admits != 0 || f.buckets != 0 || f.forwards != 0 {
					t.Fatalf("refused inline carrier reached an owner: %v", f.calls)
				}
				return
			}
			if err != nil || resp.Status == nil || f.admits != 1 || f.buckets != 0 || f.forwards != 1 {
				t.Fatalf("exact inline prepare: resp=%+v err=%v calls=%v", resp, err, f.calls)
			}
			forwarded := f.requests[len(f.requests)-1]
			if !bytes.Equal(forwarded.Inline, tc.inline) || forwarded.Intent.Tier != "a" {
				t.Fatalf("forwarded inline carrier differs: %+v", forwarded)
			}
		})
	}
}

// origin: hot-update S7 plan §1.2 / §6 / §7 / P16
func TestTransferControllerKeepsCallIdentityAndDeadline(t *testing.T) {
	f := newControlFixture(t)
	f.sent, f.forwardErr = true, errors.New("lost prepare reply")
	req := f.request(x.OpPrepare)
	d := testDeadline(t)
	r, err := f.ctl.Handle(req, f.principal, d)
	requireCode(t, r, err, x.CodeReconcileRequired)
	if f.forwards != 1 || len(f.requests) != 2 || !reflect.DeepEqual(f.requests[1], req) {
		t.Fatalf("forward changed identity or secretly retried: %+v", f.requests)
	}
	for _, got := range f.deadlines {
		if got != d {
			t.Fatalf("phase renewed deadline: got=%d want=%d calls=%v", got, d, f.calls)
		}
	}
	// Controller restart has no permit/result cache to accidentally consult.
	// Its borrowed tracker remains the sole status truth across generations.
	if err := f.ctl.Stop(d); err != nil {
		t.Fatal(err)
	}
	f.ctl, err = New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ctl.Handle(req, f.principal, testDeadline(t)); err != nil || f.admits != 1 || f.forwards != 1 {
		t.Fatalf("new controller generation recreated retained work: %v admits=%d forwards=%d", err, f.admits, f.forwards)
	}
}
