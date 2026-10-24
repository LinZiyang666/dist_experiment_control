package brokerupgrade

// These tests run the real engine with adversarial Store/Runtime/Registry ports.
// The ports model journal CAS and observations; they do not certify P02 fsync,
// P03 admission locking, P15 broker wiring or P17 same-PID exec/lease transfer.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

type journalPort struct {
	mu                                 sync.Mutex
	grant                              upgradetx.Grant
	scope                              upgradetx.Scope
	record                             upgradetx.Record
	history                            map[string]upgradetx.Record
	events                             []upgradetx.Event
	fail                               func(upgradetx.Event) bool
	starts, reverses, archives, closes int
	reverseAttempts                    int
}

func (s *journalPort) Prepare(_ upgradetx.Key, _ upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	return s.grant, nil
}

func (s *journalPort) Start(g upgradetx.Grant, f upgradetx.Fence) (upgradetx.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g != s.grant || f.RunningSHA != g.SourceSHA {
		return upgradetx.Record{}, refusal(upgradetx.CodeGrantUnknown, "test grant mismatch")
	}
	s.starts++
	a := upgradetx.Attempt{Handle: g.Handle, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, SourceGeneration: g.Handle.Generation - 1,
		Direction: upgradetx.DirectionForward, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: s.scope}
	r := upgradetx.Record{Version: upgradetx.RecordVersion, Key: g.Handle.Key, Revision: g.Handle.Revision, Grant: g, RecordedPreviousSHA: g.SourceSHA, EffectiveSHA: g.TargetSHA}
	if s.reverses != 0 {
		a.Direction = upgradetx.DirectionReverse
		r.Reverse = a
	} else {
		r.Forward = a
	}
	if s.record.Grant.Handle.TxID != "" {
		s.history[s.record.Grant.Handle.TxID] = s.record
	}
	s.record = r
	return cloneRecord(r), nil
}

func cloneRecord(r upgradetx.Record) upgradetx.Record {
	r.Actions = append([]upgradetx.Action(nil), r.Actions...)
	return r
}

func (s *journalPort) Load(_ upgradetx.Key, tx string) (upgradetx.View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.record
	if r.Grant.Handle.TxID != tx {
		r = s.history[tx]
	}
	if r.Grant.Handle.TxID == "" {
		return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "test transaction unknown")
	}
	return upgradetx.View{Historical: cloneRecord(r), Metadata: upgradetx.Metadata{TxID: tx, Window: upgradetx.WindowOpen}}, nil
}

func (s *journalPort) Apply(h upgradetx.Handle, ev upgradetx.Event) (upgradetx.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h != handle(s.record) {
		return upgradetx.Record{}, refusal(upgradetx.CodeDeferred, "test stale CAS")
	}
	if s.fail != nil && s.fail(ev) {
		return upgradetx.Record{}, errors.New("test journal sync failure")
	}
	s.events = append(s.events, ev)
	a := &s.record.Forward
	if s.record.Reverse.Handle.TxID != "" {
		a = &s.record.Reverse
	}
	if ev.Phase != "" {
		a.Phase = ev.Phase
	}
	if ev.CutStage != "" {
		a.CutStage = ev.CutStage
	}
	if ev.Outcome != "" {
		a.Outcome = ev.Outcome
	}
	if ev.Receipt.Kind == upgradetx.ReceiptControlReady {
		a.Control = ev.Receipt
	}
	if ev.Receipt.Kind == upgradetx.ReceiptResourcesMigrated {
		a.Resources = ev.Receipt
	}
	if ev.Receipt.Kind == upgradetx.ReceiptOldGenerationReleased {
		a.Released = ev.Receipt
	}
	if ev.Action.ID != "" {
		found := false
		for i := range s.record.Actions {
			if s.record.Actions[i].ID == ev.Action.ID {
				s.record.Actions[i], found = ev.Action, true
			}
		}
		if !found {
			s.record.Actions = append(s.record.Actions, ev.Action)
		}
	}
	s.record.Revision++
	a.Handle.Revision = s.record.Revision
	return cloneRecord(s.record), nil
}

func (s *journalPort) PrepareReverse(f upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reverseAttempts++
	if f.Current != handle(s.record) || f.PreviousSHA != s.record.RecordedPreviousSHA {
		return upgradetx.Grant{}, refusal(upgradetx.CodeDeferred, "test stale reverse")
	}
	s.reverses++
	g := s.grant
	g.Handle = upgradetx.Handle{Key: f.Current.Key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: f.Current.Generation + 1, Revision: 1}
	g.Issuer, g.SourceSHA, g.TargetSHA, g.ClientNonce = f.Owner, f.RunningSHA, req.TargetSHA, req.ClientNonce
	s.grant, s.scope = g, req.Scope
	return g, nil
}

func (s *journalPort) Archive(upgradetx.Handle, upgradetx.DeathProof) (upgradetx.View, error) {
	s.archives++
	return upgradetx.View{}, errors.New("test archive must not be called without a death proof")
}

func (s *journalPort) CloseWindow(h upgradetx.Handle) (upgradetx.Metadata, error) {
	if h != handle(s.record) {
		return upgradetx.Metadata{}, refusal(upgradetx.CodeDeferred, "test stale window")
	}
	s.closes++
	return upgradetx.Metadata{TxID: h.TxID, Window: upgradetx.WindowClosed}, nil
}

type runtimePort struct {
	journal                                                                       *journalPort
	owner                                                                         upgradetx.Owner
	running                                                                       string
	effective                                                                     string
	last                                                                          upgradetx.Stamp
	seq                                                                           uint64
	activated                                                                     bool
	freezeCalls, exportCalls, activateCalls, retireCalls, thawCalls, observeCalls int
	preflightErr, activateErr, thawErr                                            error
	observe                                                                       func(*upgradetx.Observation)
	receipt                                                                       func(*upgradetx.Receipt)
	beforeFreeze                                                                  func()
	beforeActivate                                                                func(upgradeport.Cut)
	currentDescriptor                                                             []byte
	frozenDescriptor                                                              []byte
	file                                                                          *os.File
	oldOwners                                                                     []upgradetx.Owner
	preflight                                                                     func(*upgradeport.PreflightProof)
	thawNoRestore                                                                 bool
	export                                                                        func(*upgradeport.Cut)
	observeErr                                                                    error
}

func (r *runtimePort) census(s upgradetx.Stamp) upgradetx.Census {
	s.Producer, s.Sequence, s.At = r.owner, r.seq, time.Now().UnixNano()
	rows := append([]upgradetx.Resource(nil), r.journal.scope.Expected...)
	if r.activated {
		for i := range rows {
			rows[i].Owner, rows[i].Generation, rows[i].Custody = r.owner, active(r.journal.record).Handle.Generation, "target"
		}
	}
	return upgradetx.Census{Stamp: s, Known: true, Complete: true, Resources: rows, ExpectedDigest: func() string { d, _ := upgradetx.ExpectedDigest(r.journal.scope); return d }(), OldOwners: r.oldOwners}
}

func (r *runtimePort) Preflight(_ context.Context, g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	if r.preflightErr != nil {
		return upgradeport.PreflightProof{}, r.preflightErr
	}
	s := makeStamp(g.Handle, g.Issuer, g.ScopeDigest, nonce())
	proof := upgradeport.PreflightProof{Grant: g, ReadersDigest: "readers", ConfigDigest: "config", Census: r.census(s)}
	if r.preflight != nil {
		r.preflight(&proof)
	}
	return proof, nil
}

func (r *runtimePort) cut(h upgradetx.Handle) upgradeport.Cut {
	c := upgradeport.Cut{Attempt: h, Owner: r.owner, Stage: upgradetx.CutPristine, Descriptor: []byte("stale bootstrap database"), Scope: r.journal.scope, Sequence: r.seq}
	if r.frozenDescriptor != nil {
		c.Descriptor = r.frozenDescriptor
	}
	if r.file != nil {
		c.Auxiliary = []upgradeport.FD{{Index: 1, File: r.file, Kind: "lease", Owner: r.owner}}
	}
	return c
}

func (r *runtimePort) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	r.freezeCalls++
	if r.beforeFreeze != nil {
		r.beforeFreeze()
	}
	return r.cut(h), nil
}

func (r *runtimePort) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) {
	r.exportCalls++
	c := r.cut(h)
	c.Descriptor = r.currentDescriptor
	if r.export != nil {
		r.export(&c)
	}
	return c, nil
}

func (r *runtimePort) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	r.activateCalls++
	if r.beforeActivate != nil {
		r.beforeActivate(c)
	}
	r.activated, r.running = true, r.journal.grant.TargetSHA
	r.effective = r.running
	r.owner.InstanceID = fmt.Sprintf("generation-%d", c.Attempt.Generation)
	return upgradeport.Activation{Attempt: c.Attempt, Owner: r.owner, Stage: upgradetx.CutActive, RunningSHA: r.running}, r.activateErr
}

func (r *runtimePort) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	r.observeCalls++
	if r.observeErr != nil {
		return upgradetx.Observation{}, r.observeErr
	}
	r.last = s
	// The requested transaction may be archived. Runtime health always refers
	// to the current owner; replaying the requested history would hide M12.
	r.journal.mu.Lock()
	a := active(cloneRecord(r.journal.record))
	r.journal.mu.Unlock()
	if r.activated {
		a.CutStage = upgradetx.CutActive
	}
	c := r.census(s)
	a.Control = r.makeReceipt(upgradetx.ReceiptControlReady, c)
	o := upgradetx.Observation{Nonce: s.Nonce, At: c.Stamp.At, Current: a, Census: c, OwnerKnown: true, RunningSHA: r.running, EffectiveSHA: r.effective}
	if r.observe != nil {
		r.observe(&o)
	}
	return o, nil
}

func (r *runtimePort) makeReceipt(kind upgradetx.ReceiptKind, c upgradetx.Census) upgradetx.Receipt {
	proof := upgradetx.Receipt{Kind: kind, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: r.running, Census: c, EvidenceDigest: strings.Repeat("e", 64)}
	if r.receipt != nil {
		r.receipt(&proof)
	}
	return proof
}

func (r *runtimePort) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	r.retireCalls++
	r.oldOwners = nil
	s := r.last
	s.Attempt = h
	return r.makeReceipt(upgradetx.ReceiptOldGenerationReleased, r.census(s)), nil
}

func (r *runtimePort) Thaw(upgradetx.Handle) error {
	r.thawCalls++
	if r.thawErr == nil && !r.thawNoRestore {
		r.effective = r.running
	}
	return r.thawErr
}

type registryPort struct {
	runtime  *runtimePort
	snapshot func(*upgradetx.Census)
}

func (*registryPort) Begin(upgradetx.Resource) (uint64, error) {
	return 0, errors.New("business admissions belong to the runtime")
}
func (*registryPort) Finish(uint64, upgradetx.Resource) error {
	return errors.New("business admissions belong to the runtime")
}
func (r *registryPort) Snapshot(s upgradetx.Stamp) (upgradetx.Census, error) {
	c := r.runtime.census(s)
	c.Stamp = s
	if r.snapshot != nil {
		r.snapshot(&c)
	}
	return c, nil
}

func (r *registryPort) Reconcile(stamp upgradetx.Stamp, reconcile func(upgradetx.Census) error) error {
	c, err := r.Snapshot(stamp)
	if err != nil {
		return err
	}
	return reconcile(c)
}
func (r *registryPort) Seal(a upgradetx.Attempt, n string) (upgradetx.Receipt, error) {
	s := r.runtime.last
	s.Attempt, s.Nonce = a.Handle, n
	return r.runtime.makeReceipt(upgradetx.ReceiptResourcesMigrated, r.runtime.census(s)), nil
}

type engineFixture struct {
	endpoint upgradeport.Endpoint
	store    *journalPort
	runtime  *runtimePort
	registry *registryPort
	artifact upgradeartifact.Receipt
}

func newEngineFixture() *engineFixture {
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "node", DomainID: strings.Repeat("d", 32), ConfigPath: "/fixture/broker.yaml"}
	owner := upgradetx.Owner{BootID: "boot", PID: 42, StartTicks: 99, InstanceID: strings.Repeat("f", 32)}
	g := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 1}, Issuer: owner,
		Mode: upgradetx.ModeHot, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), ScopeDigest: "scope", CapabilityDigest: "capabilities", ClientNonce: "prepare", ActorFP: "actor", ExpiresAt: time.Now().Add(time.Minute).UnixNano()}
	scope := upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}}
	for _, domain := range scope.Domains {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: domain, ID: string(domain), Origin: "original", Owner: owner, Generation: 1, State: "active", Custody: "owned"})
	}
	g.ScopeDigest, _ = upgradetx.ScopeDigest(scope)
	s := &journalPort{grant: g, scope: scope, history: make(map[string]upgradetx.Record)}
	r := &runtimePort{journal: s, owner: owner, running: g.SourceSHA, effective: g.SourceSHA, seq: 10, currentDescriptor: []byte("current database")}
	registry := &registryPort{runtime: r}
	return &engineFixture{endpoint: New(s, r, registry), store: s, runtime: r, registry: registry, artifact: upgradeartifact.Receipt{Grant: g, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA}}
}

func (f *engineFixture) start(t *testing.T) upgradetx.View {
	t.Helper()
	v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
	if err != nil {
		t.Fatalf("Start: %v (phase %s)", err, active(v.Historical).Phase)
	}
	return v
}

func requireAcceptedFailure(t *testing.T, v upgradetx.View, err error) {
	t.Helper()
	var txErr *upgradetx.Error
	if !errors.As(err, &txErr) || !txErr.Accepted || active(v.Historical).Phase == upgradetx.PhaseCompleted || active(v.Historical).Phase == upgradetx.PhaseRolledBack {
		t.Fatalf("accepted failure became success/refusal: phase=%s err=%v", active(v.Historical).Phase, err)
	}
}

// origin: hot-update S8 plan §3.2 / §4 / §11.2 P10
func TestBrokerUpgradeJournalsBeforeEffects(t *testing.T) {
	f := newEngineFixture()
	f.runtime.beforeFreeze = func() {
		a := active(f.store.record)
		if a.Phase != upgradetx.PhaseFreezing || a.CutStage != upgradetx.CutPristine || len(f.store.events) == 0 {
			t.Fatal("freeze preceded durable intent")
		}
	}
	f.runtime.beforeActivate = func(c upgradeport.Cut) {
		a := active(f.store.record)
		if a.Phase != upgradetx.PhaseActivating || a.CutStage != upgradetx.CutPartialActive {
			t.Fatal("activation preceded partial-active intent")
		}
		if string(c.Descriptor) != "current database" {
			t.Fatal("activation used the bootstrap database")
		}
		if c.Attempt != handle(f.store.record) {
			t.Fatal("exported boot handle predates the activation intent")
		}
	}
	v := f.start(t)
	a := active(v.Historical)
	if a.Phase != upgradetx.PhaseCompleted || a.CutStage != upgradetx.CutReleased || a.Outcome != upgradetx.OutcomeSuccess {
		t.Fatalf("completion = %+v", a)
	}
	for _, action := range v.Historical.Actions {
		if action.State != upgradetx.ActionDurable || action.ResultDigest == "" {
			t.Fatalf("unsettled action %+v", action)
		}
	}
	if f.runtime.freezeCalls != 1 || f.runtime.exportCalls != 1 || f.runtime.activateCalls != 1 || f.runtime.retireCalls != 1 {
		t.Fatal("cut effects were skipped or duplicated")
	}
}

// origin: hot-update S8 plan §4.2 / §8.3 M02, M05, M06, M15 / §11.2 P10
func TestBrokerUpgradeRequiresIndependentCompletionProofs(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*engineFixture)
	}{
		{"disk_sha_only", func(f *engineFixture) {
			f.runtime.observe = func(o *upgradetx.Observation) {
				if f.runtime.activated {
					o.RunningSHA = f.artifact.SourceSHA
				}
			}
		}},
		{"unknown_owner", func(f *engineFixture) {
			f.runtime.observe = func(o *upgradetx.Observation) {
				if f.runtime.activated {
					o.OwnerKnown = false
				}
			}
		}},
		{"stale_nonce", func(f *engineFixture) {
			f.runtime.observe = func(o *upgradetx.Observation) {
				if f.runtime.activated {
					o.Nonce = "old"
				}
			}
		}},
		{"wrong_cut", func(f *engineFixture) {
			f.runtime.observe = func(o *upgradetx.Observation) {
				if f.runtime.activated {
					o.Current.Handle.CutID = "wrong"
				}
			}
		}},
		{"new_admission", func(f *engineFixture) { f.registry.snapshot = func(c *upgradetx.Census) { c.Stamp.Sequence++ } }},
		{"unknown_zero", func(f *engineFixture) {
			f.registry.snapshot = func(c *upgradetx.Census) { c.Known = false; c.Resources = nil }
		}},
		{"missing_domain", func(f *engineFixture) {
			f.registry.snapshot = func(c *upgradetx.Census) { c.Resources = c.Resources[1:] }
		}},
		{"missing_original_id", func(f *engineFixture) {
			f.registry.snapshot = func(c *upgradetx.Census) { c.Resources[0].ID = "replacement" }
		}},
		{"retained_owner", func(f *engineFixture) {
			f.registry.snapshot = func(c *upgradetx.Census) { c.OldOwners = []upgradetx.Owner{f.artifact.Grant.Issuer} }
		}},
	}
	for _, kind := range []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased} {
		cases = append(cases, struct {
			name    string
			corrupt func(*engineFixture)
		}{string(kind), func(f *engineFixture) {
			f.runtime.receipt = func(r *upgradetx.Receipt) {
				if r.Kind == kind {
					r.Known = false
				}
			}
		}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEngineFixture()
			tc.corrupt(f)
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			requireAcceptedFailure(t, v, err)
		})
	}
}

// origin: hot-update S8 plan §3.1 / §3.2 / §8.2
func TestBrokerUpgradeIntentFailureAndRetryNeverRedispatch(t *testing.T) {
	for _, failKind := range []upgradetx.ActionKind{upgradetx.ActionFreeze, upgradetx.ActionActivate} {
		t.Run(string(failKind), func(t *testing.T) {
			f := newEngineFixture()
			f.store.fail = func(ev upgradetx.Event) bool {
				return ev.Action.Kind == failKind && ev.Action.State == upgradetx.ActionIntent
			}
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			requireAcceptedFailure(t, v, err)
			if f.runtime.activateCalls != 0 || (failKind == upgradetx.ActionFreeze && f.runtime.freezeCalls != 0) {
				t.Fatal("effect ran after intent sync failed")
			}
			before := f.runtime.freezeCalls
			f.store.fail = nil
			if _, err = f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact); err != nil {
				t.Fatal(err)
			}
			if f.store.starts != 1 || f.runtime.freezeCalls != before || f.runtime.activateCalls != 0 {
				t.Fatal("accepted retry redispatched")
			}
		})
	}
}

// origin: hot-update S8 plan §3.3 / §4.2
func TestBrokerUpgradeQuerySeparatesHistoryFromCurrent(t *testing.T) {
	// origin: hot-update S8 plan §3.3 / §4.2 / LINT-s8 — failed live
	// observation/readback returns an unsettled view with immutable history.
	for _, defect := range []string{"cached proof", "observer failure", "cancelled observation", "changed journal", "failed readback"} {
		t.Run(defect, func(t *testing.T) {
			f := newEngineFixture()
			completed := f.start(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch defect {
			case "cached proof":
				f.runtime.observe = func(o *upgradetx.Observation) { o.Nonce = "cached" }
			case "observer failure":
				f.runtime.observeErr = errors.New("live observer unavailable")
			case "cancelled observation":
				f.runtime.observe = func(*upgradetx.Observation) { cancel() }
			case "changed journal":
				f.runtime.observe = func(*upgradetx.Observation) {
					f.store.mu.Lock()
					defer f.store.mu.Unlock()
					f.store.record.Revision++
				}
			case "failed readback":
				f.runtime.observe = func(*upgradetx.Observation) {
					f.store.mu.Lock()
					defer f.store.mu.Unlock()
					f.store.record = upgradetx.Record{}
				}
			}
			v, err := f.endpoint.Query(ctx, f.artifact.Grant.Handle.Key, f.artifact.Grant.Handle.TxID, "fresh-query")
			if err != nil || v.Code != upgradetx.CodeUnsettled || v.Observation.OwnerKnown || !reflect.DeepEqual(v.Historical, completed.Historical) {
				t.Fatalf("query reused cached proof or mutated history: %+v %v", v, err)
			}
		})
	}
}

// A waiting ctl ends at the first unknown answer (74), so a query must not
// call a cut window unknown health: a runtime's retryable deferral (the armed
// source, the importer not yet able to enumerate) is returned as that
// refusal, and one completion write racing the sample is sampled again
// rather than reported as a changed transaction.
// origin: hot-update S8 plan §7.1; S8 BROKER-EXPORTER R-11 (real-cut finding)
func TestBrokerUpgradeQueryRetriesCutWindowsInsteadOfUnknown(t *testing.T) {
	f := newEngineFixture()
	f.start(t)
	key, tx := f.artifact.Grant.Handle.Key, f.artifact.Grant.Handle.TxID
	f.runtime.observeErr = &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonUpgradeInProgress}
	v, err := f.endpoint.Query(t.Context(), key, tx, "window-query")
	var typed *upgradetx.Error
	if !errors.As(err, &typed) || typed.Code != upgradetx.CodeDeferred || typed.Accepted || v.Code != "" {
		t.Fatalf("cut window became a view: %+v %v", v, err)
	}
	// An accepted unknown from the runtime is not a retryable window.
	f.runtime.observeErr = &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonUpgradeInProgress, Accepted: true}
	if v, err := f.endpoint.Query(t.Context(), key, tx, "accepted-query"); err != nil || v.Code != upgradetx.CodeUnsettled {
		t.Fatalf("accepted refusal was retried as a window: %+v %v", v, err)
	}
	f.runtime.observeErr = nil
	moved := false
	f.runtime.observe = func(*upgradetx.Observation) {
		f.store.mu.Lock()
		defer f.store.mu.Unlock()
		if !moved {
			moved = true
			f.store.record.Revision++
		}
	}
	v, err = f.endpoint.Query(t.Context(), key, tx, "raced-query")
	if err != nil || v.Code != "" || !v.Observation.OwnerKnown || v.Historical.Revision != f.store.record.Revision || f.runtime.observeCalls < 2 {
		t.Fatalf("one raced completion write ended the query: %+v %v", v, err)
	}
}

// origin: hot-update S8 plan §3.3 / §4 / §8.1
func TestBrokerUpgradeResumeUsesCurrentProofWithoutRedispatch(t *testing.T) {
	f := newEngineFixture()
	f.store.fail = func(ev upgradetx.Event) bool {
		return ev.Action.Kind == upgradetx.ActionActivate && ev.Action.State == upgradetx.ActionObserved
	}
	v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
	requireAcceptedFailure(t, v, err)
	f.store.fail = nil
	v, err = Resume(t.Context(), f.endpoint, handle(f.store.record))
	if err != nil || active(v.Historical).Phase != upgradetx.PhaseCompleted || f.runtime.activateCalls != 1 || f.runtime.exportCalls != 1 {
		t.Fatalf("boot redispatched or failed to reconcile: %v", err)
	}
	late := handle(v.Historical)
	late.Revision--
	if _, err = Resume(t.Context(), f.endpoint, late); err == nil {
		t.Fatal("late boot writer claimed a newer revision")
	}
}

// origin: hot-update S8 plan §5.1 / §11.2 P10
func TestBrokerUpgradeCompetingMutationDoesNotBlockQuery(t *testing.T) {
	f := newEngineFixture()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	f.runtime.beforeFreeze = func() { close(entered); <-release }
	go func() { _, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("Start returned before the freeze barrier: %v", err)
	}
	_, err := f.endpoint.Prepare(t.Context(), f.artifact.Grant.Handle.Key, upgradetx.PrepareRequest{Mode: upgradetx.ModeHot})
	if !hasCode(err, upgradetx.CodeDeferred) {
		t.Errorf("competing Prepare: %v", err)
	}
	if _, err = f.endpoint.Query(t.Context(), f.artifact.Grant.Handle.Key, f.artifact.Grant.Handle.TxID, "concurrent-query"); err != nil {
		t.Error(err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §2.2 / §11.2 P10
func TestBrokerUpgradeMissingDependenciesFailClosed(t *testing.T) {
	f := newEngineFixture()
	ep := New(contractstub.Store{}, contractstub.Runtime{}, contractstub.Registry{})
	if _, err := ep.Start(t.Context(), f.artifact.Grant, f.artifact); !hasCode(err, upgradetx.CodeNotWired) {
		t.Fatalf("stub dependency accepted execution: %v", err)
	}
	if _, err := New(nil, nil, nil).Prepare(t.Context(), f.artifact.Grant.Handle.Key, upgradetx.PrepareRequest{Mode: upgradetx.ModeHot}); !hasCode(err, upgradetx.CodeNotWired) {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §11.2 P10 / §12.2
func TestBrokerUpgradeEngineLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerUpgradeEngine(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker upgrade engine", beforeG)
	testharness.AssertNoFDLeak(t, "broker upgrade engine", beforeFD, 0)
}

func exerciseBrokerUpgradeEngine(t *testing.T) {
	t.Helper()
	f := newEngineFixture()
	file, err := os.CreateTemp(t.TempDir(), "custody-")
	if err != nil {
		t.Fatal(err)
	}
	f.runtime.file = file
	f.start(t)
	if _, err = file.WriteString("lease still held by runtime"); err != nil {
		t.Fatal("engine closed adapter custody", err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func (r *registryPort) Commit(stamp upgradetx.Stamp, commit func(upgradetx.Census) error) error {
	c, err := r.Snapshot(stamp)
	if err != nil {
		return err
	}
	return commit(c)
}
