package agentupgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
)

// These ports are adversarial boundary fixtures, not a substitute for P02's
// fsync journal, P03's shared admission gate, or P17's real image implementation.
// They record the engine's calls and deliberately let invalid evidence through
// so a test cannot pass merely because a dependency rejected the mutation.
type engineFixture struct {
	t           *testing.T
	store       *recordingStore
	runtime     *recordingRuntime
	registry    *recordingRegistry
	endpoint    upgradeport.Endpoint
	grant       upgradetx.Grant
	artifact    upgradeartifact.Receipt
	scope       upgradetx.Scope
	sequence    uint64
	owner       upgradetx.Owner
	running     string
	effective   string
	calls       []string
	preflight   func(*upgradeport.PreflightProof)
	observation func(*upgradetx.Observation)
	sealed      func(*upgradetx.Receipt)
	retirement  func(*upgradetx.Receipt)
	activation  func(upgradeport.Cut) error
	freezeError error
	applyError  func(upgradetx.Event) error
	death       func(*upgradetx.DeathProof)
	file        *os.File
	startError  error
	loadError   error
}

type recordingStore struct {
	fixture   *engineFixture
	mu        sync.Mutex
	records   map[string]upgradetx.Record
	grants    map[string]upgradetx.Grant
	currentID string
	events    []upgradetx.Event
	starts    int
	prepares  int
	reverses  int
	archives  int
	closes    int
}

type recordingRuntime struct{ fixture *engineFixture }
type recordingRegistry struct{ fixture *engineFixture }

func fixture(t *testing.T) *engineFixture {
	t.Helper()
	key := upgradetx.Key{DomainID: "canonical-domain", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "configured-node", ConfigPath: "/sandbox/agent.yaml", UID: 1000}
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 456, InstanceID: strings.Repeat("1", 32)}
	resource := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "original-child", Origin: "source", Owner: owner, Generation: 1, State: "active", Custody: "current"}
	scope := upgradetx.Scope{ID: strings.Repeat("2", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{resource}}
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1}, ClientNonce: strings.Repeat("5", 32), Issuer: owner, ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "A", TargetRelease: "B", SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), ScopeDigest: strings.Repeat("c", 64), CapabilityDigest: strings.Repeat("d", 64), ExpiresAt: time.Now().Add(120 * time.Second).UnixNano()}
	grant.ScopeDigest, _ = upgradetx.ScopeDigest(scope)
	f := &engineFixture{t: t, grant: grant, scope: scope, sequence: 7, owner: owner, running: grant.SourceSHA, effective: grant.SourceSHA}
	f.artifact = upgradeartifact.Receipt{Grant: grant, CanonicalPath: "/sandbox/canonical", SourcePath: "/sandbox/A", TargetPath: "/sandbox/B", SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("e", 64)}
	f.store = &recordingStore{fixture: f, records: make(map[string]upgradetx.Record), grants: map[string]upgradetx.Grant{grant.Handle.TxID: grant}}
	f.runtime, f.registry = &recordingRuntime{f}, &recordingRegistry{f}
	f.endpoint = New(f.store, f.runtime, f.registry)
	return f
}

func (f *engineFixture) census(handle upgradetx.Handle, nonce string) upgradetx.Census {
	resources := append([]upgradetx.Resource(nil), f.scope.Expected...)
	for i := range resources {
		resources[i].Owner, resources[i].Generation = f.owner, handle.Generation
		if record, ok := f.store.records[handle.TxID]; ok && f.running == current(record).SourceSHA {
			resources[i].Generation = current(record).SourceGeneration
		}
	}
	scope := f.scope
	if record, ok := f.store.records[handle.TxID]; ok {
		scope = current(record).Scope
	}
	expectedDigest, _ := upgradetx.ExpectedDigest(scope)
	scopeDigest, _ := upgradetx.ScopeDigest(scope)
	return upgradetx.Census{Stamp: upgradetx.Stamp{Attempt: handle, Producer: f.owner, Nonce: nonce, Sequence: f.sequence, At: time.Now().UnixNano(), ScopeDigest: scopeDigest}, Known: true, Complete: true, Resources: resources, ExpectedDigest: expectedDigest}
}

func (f *engineFixture) receipt(kind upgradetx.ReceiptKind, handle upgradetx.Handle, nonce string) upgradetx.Receipt {
	c := f.census(handle, nonce)
	return upgradetx.Receipt{Kind: kind, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: f.running, Census: c, EvidenceDigest: strings.Repeat("e", 64)}
}

func (s *recordingStore) Prepare(key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	s.prepares++
	return s.fixture.grant, nil
}

func (s *recordingStore) Start(g upgradetx.Grant, fence upgradetx.Fence) (upgradetx.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fixture.startError != nil {
		return upgradetx.Record{}, s.fixture.startError
	}
	if _, ok := s.grants[g.Handle.TxID]; !ok {
		return upgradetx.Record{}, refusal(upgradetx.CodeGrantUnknown, "unknown grant", false)
	}
	if r, ok := s.records[g.Handle.TxID]; ok {
		return r, nil
	}
	direction := upgradetx.DirectionForward
	if s.reverses > 0 {
		direction = upgradetx.DirectionReverse
	}
	a := upgradetx.Attempt{Handle: g.Handle, Direction: direction, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, SourceGeneration: g.Handle.Generation - 1, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: s.fixture.scope}
	r := upgradetx.Record{Version: upgradetx.RecordVersion, Key: g.Handle.Key, Revision: g.Handle.Revision, Grant: g, Forward: a, RecordedPreviousSHA: g.SourceSHA, EffectiveSHA: g.SourceSHA}
	s.records[g.Handle.TxID], s.currentID = r, g.Handle.TxID
	s.starts++
	return r, nil
}

func (s *recordingStore) Load(key upgradetx.Key, id string) (upgradetx.View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fixture.loadError != nil {
		return upgradetx.View{}, s.fixture.loadError
	}
	r, ok := s.records[id]
	if !ok {
		return upgradetx.View{}, refusal(upgradetx.CodeGrantUnknown, "no execution", false)
	}
	return upgradetx.View{Historical: r, Metadata: upgradetx.Metadata{TxID: id, Window: upgradetx.WindowOpen}}, nil
}

func (s *recordingStore) Apply(expected upgradetx.Handle, event upgradetx.Event) (upgradetx.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[expected.TxID]
	if !ok || !sameAttempt(current(r).Handle, expected) || r.Revision != expected.Revision || s.currentID != expected.TxID {
		return r, refusal(upgradetx.CodeUnsettled, "write CAS lost", true)
	}
	if s.fixture.applyError != nil {
		if err := s.fixture.applyError(event); err != nil {
			return r, err
		}
	}
	a := &r.Forward
	if r.Reverse.Handle.TxID != "" {
		a = &r.Reverse
	}
	if event.Action.ID != "" {
		found := false
		for i := range r.Actions {
			if r.Actions[i].ID == event.Action.ID {
				r.Actions[i] = event.Action
				found = true
			}
		}
		if !found {
			r.Actions = append(r.Actions, event.Action)
		}
	}
	if event.Phase != "" {
		a.Phase = event.Phase
	}
	if event.CutStage != "" {
		a.CutStage = event.CutStage
	}
	if event.Outcome != "" {
		a.Outcome = event.Outcome
	}
	switch event.Receipt.Kind {
	case upgradetx.ReceiptControlReady:
		a.Control = event.Receipt
	case upgradetx.ReceiptResourcesMigrated:
		a.Resources = event.Receipt
	case upgradetx.ReceiptOldGenerationReleased:
		a.Released = event.Receipt
	case "":
	default:
		s.fixture.t.Fatalf("unexpected receipt kind %s", event.Receipt.Kind)
	}
	r.Revision++
	a.Handle.Revision = r.Revision
	r.Reason = event.Reason
	s.records[expected.TxID] = r
	s.events = append(s.events, event)
	return r, nil
}

func (s *recordingStore) PrepareReverse(expected upgradetx.Fence, req upgradetx.ReverseRequest) (upgradetx.Grant, error) {
	s.reverses++
	g := s.fixture.grant
	g.Handle.TxID, g.Handle.CutID, g.Handle.Generation, g.Handle.Revision = newID(), newID(), expected.Current.Generation+1, 1
	g.SourceSHA, g.TargetSHA, g.Issuer = expected.RunningSHA, expected.PreviousSHA, expected.Owner
	g.ScopeDigest, _ = upgradetx.ScopeDigest(req.Scope)
	s.grants[g.Handle.TxID] = g
	return g, nil
}

func (s *recordingStore) Archive(h upgradetx.Handle, death upgradetx.DeathProof) (upgradetx.View, error) {
	s.archives++
	r := s.records[h.TxID]
	r.Forward.Phase = upgradetx.PhaseAbandoned
	r.Forward.Outcome = upgradetx.OutcomeUnknown
	s.records[h.TxID] = r
	return upgradetx.View{Historical: r}, nil
}

func (s *recordingStore) CloseWindow(h upgradetx.Handle) (upgradetx.Metadata, error) {
	s.closes++
	return upgradetx.Metadata{TxID: h.TxID, Window: upgradetx.WindowClosed}, nil
}

func (r *recordingRuntime) Preflight(ctx context.Context, g upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	f := r.fixture
	f.calls = append(f.calls, "preflight")
	proof := upgradeport.PreflightProof{Grant: g, ReadersDigest: strings.Repeat("1", 64), ConfigDigest: strings.Repeat("2", 64), Census: f.census(g.Handle, newID())}
	if f.preflight != nil {
		f.preflight(&proof)
	}
	return proof, nil
}

func (r *recordingRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	f := r.fixture
	f.calls = append(f.calls, "freeze")
	return upgradeport.Cut{Attempt: h, Owner: f.owner, Stage: upgradetx.CutPristine, Descriptor: []byte("pristine-input"), Scope: f.scope, Sequence: f.sequence, Auxiliary: []upgradeport.FD{{Index: 1, File: f.file, Kind: "lease", Owner: f.owner}}}, f.freezeError
}

func (r *recordingRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) {
	f := r.fixture
	f.calls = append(f.calls, "export-current")
	return upgradeport.Cut{Attempt: h, Owner: f.owner, Stage: upgradetx.CutActive, Descriptor: []byte("current-latest-ACK-and-new-child"), Scope: f.scope, Sequence: f.sequence, Auxiliary: []upgradeport.FD{{Index: 1, File: f.file, Kind: "lease", Owner: f.owner}}}, nil
}

func (r *recordingRuntime) Activate(cut upgradeport.Cut) (upgradeport.Activation, error) {
	f := r.fixture
	f.calls = append(f.calls, "activate")
	record := f.store.records[cut.Attempt.TxID]
	if current(record).CutStage != upgradetx.CutPartialActive {
		f.t.Fatal("activation ran before durable partial_active intent")
	}
	for _, kind := range []upgradetx.ActionKind{upgradetx.ActionSelect, upgradetx.ActionDispatch, upgradetx.ActionActivate} {
		found := false
		for _, action := range record.Actions {
			if action.Kind == kind && action.State == upgradetx.ActionIntent {
				found = true
			}
		}
		if !found {
			f.t.Fatalf("activation preceded %s intent", kind)
		}
	}
	f.running, f.effective = current(record).TargetSHA, current(record).TargetSHA
	if f.activation != nil {
		if err := f.activation(cut); err != nil {
			return upgradeport.Activation{}, err
		}
	}
	return upgradeport.Activation{Attempt: cut.Attempt, Owner: f.owner, Stage: upgradetx.CutActive, RunningSHA: f.running}, nil
}

func (r *recordingRuntime) Observe(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	f := r.fixture
	a := current(f.store.records[f.store.currentID])
	if a.Handle.TxID == "" {
		a = upgradetx.Attempt{SourceSHA: f.running, TargetSHA: f.running, Direction: upgradetx.DirectionForward, Scope: f.scope}
	}
	a.Control = f.receipt(upgradetx.ReceiptControlReady, a.Handle, stamp.Nonce)
	o := upgradetx.Observation{Nonce: stamp.Nonce, At: time.Now().UnixNano(), Current: a, Census: f.census(a.Handle, stamp.Nonce), OwnerKnown: true, RunningSHA: f.running, EffectiveSHA: f.effective}
	o.At = o.Census.Stamp.At
	if f.observation != nil {
		f.observation(&o)
	}
	return o, nil
}

func (r *recordingRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	f := r.fixture
	f.calls = append(f.calls, "retire")
	receipt := f.receipt(upgradetx.ReceiptOldGenerationReleased, h, newID())
	if f.retirement != nil {
		f.retirement(&receipt)
	}
	return receipt, nil
}

func (r *recordingRuntime) Thaw(h upgradetx.Handle) error {
	r.fixture.calls = append(r.fixture.calls, "thaw")
	return nil
}

func (r *recordingRuntime) ObserveDeath(stamp upgradetx.Stamp) (upgradetx.DeathProof, error) {
	f := r.fixture
	c := f.census(stamp.Attempt, stamp.Nonce)
	for i := range c.Resources {
		c.Resources[i].State, c.Resources[i].Custody = "terminal", "released"
	}
	c.Stamp.Producer.InstanceID = strings.Repeat("9", 32)
	d := upgradetx.DeathProof{Owner: f.owner, Stamp: c.Stamp, Custody: c, Reaped: true, NoOwnedFDs: true}
	if f.death != nil {
		f.death(&d)
	}
	return d, nil
}

// origin: hot-update S8 plan §2.2 / §7.1 / P09
func TestAgentEndpointRequiresRoleAndHonorsCancellation(t *testing.T) {
	f := fixture(t)
	key := f.grant.Handle.Key
	key.Component = upgradetx.ComponentBroker
	_, err := f.endpoint.Prepare(testContext(t), key, upgradetx.PrepareRequest{Mode: upgradetx.ModeHot})
	requireError(t, err, upgradetx.CodeRejected, false)
	if f.store.prepares != 0 {
		t.Fatal("agent endpoint issued a broker grant")
	}
	_, err = f.endpoint.Prepare(testContext(t), f.grant.Handle.Key, upgradetx.PrepareRequest{Mode: upgradetx.ModeHot})
	if err != nil || f.store.prepares != 1 || len(f.calls) != 0 {
		t.Fatal("prepare did not remain inert")
	}
	ctx, cancel := context.WithCancel(testContext(t))
	cancel()
	_, err = f.endpoint.Start(ctx, f.grant, f.artifact)
	if !errors.Is(err, context.Canceled) || f.store.starts != 0 || len(f.calls) != 0 {
		t.Fatal("cancelled caller began execution")
	}
	view := startFixture(t, f)
	view, err = f.endpoint.CloseWindow(testContext(t), current(view.Historical).Handle)
	if err != nil || f.store.closes != 1 || view.Metadata.Window != upgradetx.WindowClosed {
		t.Fatal("window was not closed by the unique store owner")
	}
}

func (r *recordingRegistry) Begin(resource upgradetx.Resource) (uint64, error) {
	r.fixture.sequence++
	return r.fixture.sequence, nil
}
func (r *recordingRegistry) Finish(sequence uint64, resource upgradetx.Resource) error { return nil }
func (r *recordingRegistry) Snapshot(stamp upgradetx.Stamp) (upgradetx.Census, error) {
	c := r.fixture.census(stamp.Attempt, stamp.Nonce)
	c.Stamp = stamp
	c.Stamp.Sequence = r.fixture.sequence
	return c, nil
}
func (r *recordingRegistry) Seal(a upgradetx.Attempt, nonce string) (upgradetx.Receipt, error) {
	f := r.fixture
	receipt := f.receipt(upgradetx.ReceiptResourcesMigrated, a.Handle, nonce)
	if f.sealed != nil {
		f.sealed(&receipt)
	}
	return receipt, nil
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: bounded engine test lifetime.
	t.Cleanup(cancel)
	return ctx
}

func requireError(t *testing.T, err error, code string, accepted bool) {
	t.Helper()
	var e *upgradetx.Error
	if !errors.As(err, &e) || e.Code != code || e.Accepted != accepted {
		t.Fatalf("error = %v; want %s accepted=%t", err, code, accepted)
	}
}

func startFixture(t *testing.T, f *engineFixture) upgradetx.View {
	t.Helper()
	view, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
	if err != nil {
		t.Fatalf("start: %v; record=%+v", err, view.Historical.Forward)
	}
	return view
}

// origin: hot-update S8 plan §3.2 / §4.2 / P09
func TestAgentEnginePersistsIntentBeforeEffects(t *testing.T) {
	f := fixture(t)
	view := startFixture(t, f)
	if current(view.Historical).Phase != upgradetx.PhaseCompleted || current(view.Historical).Outcome != upgradetx.OutcomeSuccess {
		t.Fatalf("completion was not durable: %+v", view)
	}
	if got := strings.Join(f.calls, ","); got != "preflight,preflight,freeze,preflight,activate,retire" {
		t.Fatalf("runtime gates = %s", got)
	}
	for _, action := range view.Historical.Actions {
		if action.State != upgradetx.ActionDurable || action.IntentDigest == "" || action.ResultDigest == "" {
			t.Fatalf("incomplete action: %+v", action)
		}
	}
	before := len(f.calls)
	if _, err := f.endpoint.Start(testContext(t), f.grant, f.artifact); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != before || f.store.starts != 1 {
		t.Fatal("accepted retry repeated a runtime effect")
	}
}

// origin: hot-update S8 plan §2.2 / §3.1 / P09
func TestAgentEngineRefusesUnwiredAndForeignArtifacts(t *testing.T) {
	ctx := testContext(t)
	stub := New(contractstub.Store{}, contractstub.Runtime{}, contractstub.Registry{})
	f := fixture(t)
	_, err := stub.Start(ctx, f.grant, f.artifact)
	requireError(t, err, upgradetx.CodeNotWired, false)
	f.artifact.Grant.ClientNonce = newID()
	_, err = f.endpoint.Start(ctx, f.grant, f.artifact)
	requireError(t, err, upgradetx.CodeRejected, false)
	if len(f.calls) != 0 || f.store.starts != 0 {
		t.Fatal("foreign publication reached execution")
	}
	_, err = New(nil, nil, nil).Query(ctx, f.grant.Handle.Key, f.grant.Handle.TxID, newID())
	requireError(t, err, upgradetx.CodeNotWired, false)
	var missing *recordingRuntime
	_, err = New(f.store, missing, f.registry).Prepare(ctx, f.grant.Handle.Key, upgradetx.PrepareRequest{Mode: upgradetx.ModeHot})
	requireError(t, err, upgradetx.CodeNotWired, false)
}

// origin: hot-update S8 plan §3.1 / §7.1 / P09
func TestAgentAcceptedStoreFailuresNeverBecomeRetryable(t *testing.T) {
	f := fixture(t)
	f.startError = &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "secret-storage-error", Accepted: true}
	_, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
	requireError(t, err, upgradetx.CodeUnsettled, true)
	if strings.Contains(err.Error(), "secret-storage-error") || strings.Contains(strings.Join(f.calls, ","), "activate") {
		t.Fatal("accepted store failure leaked text or repeated execution")
	}
	f = fixture(t)
	f.loadError = &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Accepted: true}
	_, err = f.endpoint.Start(testContext(t), f.grant, f.artifact)
	requireError(t, err, upgradetx.CodeStateCorrupt, true)
	if len(f.calls) != 0 || f.store.starts != 0 {
		t.Fatal("corrupt journal became an absent execution")
	}
}

// origin: hot-update S8 plan §4.2 / M05 / M06 / M15
func TestAgentEngineRequiresIndependentCompletionProofs(t *testing.T) {
	cases := []struct {
		name   string
		inject func(*engineFixture)
	}{
		{"control_missing", func(f *engineFixture) {
			f.observation = func(o *upgradetx.Observation) { o.Current.Control = upgradetx.Receipt{} }
		}},
		{"control_unknown", func(f *engineFixture) {
			f.observation = func(o *upgradetx.Observation) { o.Current.Control.Known = false }
		}},
		{"control_empty", func(f *engineFixture) {
			f.observation = func(o *upgradetx.Observation) { o.Current.Control.Census.Resources = nil }
		}},
		{"resources_unknown", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Known = false } }},
		{"resources_incomplete", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Complete = false } }},
		{"census_unknown", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Census.Known = false } }},
		{"admission_live", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Census.Admitting = 1 } }},
		{"resource_omitted", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Census.Resources = nil } }},
		{"domain_omitted", func(f *engineFixture) {
			f.sealed = func(r *upgradetx.Receipt) { r.Census.Resources[0].Domain = upgradetx.DomainFlow }
		}},
		{"wrong_original", func(f *engineFixture) {
			f.sealed = func(r *upgradetx.Receipt) { r.Census.Resources[0].Origin = "replacement" }
		}},
		{"old_custody", func(f *engineFixture) { f.sealed = func(r *upgradetx.Receipt) { r.Census.Resources[0].Generation-- } }},
		{"released_unknown", func(f *engineFixture) { f.retirement = func(r *upgradetx.Receipt) { r.Known = false } }},
		{"released_incomplete", func(f *engineFixture) { f.retirement = func(r *upgradetx.Receipt) { r.Complete = false } }},
		{"released_empty", func(f *engineFixture) { f.retirement = func(r *upgradetx.Receipt) { r.Census.Resources = nil } }},
		{"wrong_producer", func(f *engineFixture) {
			f.sealed = func(r *upgradetx.Receipt) { r.Stamp.Producer.PID++; r.Census.Stamp = r.Stamp }
		}},
		{"retained_owner", func(f *engineFixture) {
			f.retirement = func(r *upgradetx.Receipt) { r.Census.OldOwners = []upgradetx.Owner{f.owner} }
		}},
		{"wrong_cut", func(f *engineFixture) {
			f.sealed = func(r *upgradetx.Receipt) { r.Stamp.Attempt.CutID = newID(); r.Census.Stamp = r.Stamp }
		}},
		{"wrong_direction", func(f *engineFixture) {
			f.observation = func(o *upgradetx.Observation) { o.Current.Direction = upgradetx.DirectionReverse }
		}},
		{"wrong_running", func(f *engineFixture) {
			f.activation = func(c upgradeport.Cut) error { f.running = f.grant.SourceSHA; return nil }
		}},
		{"wrong_effective", func(f *engineFixture) {
			f.activation = func(c upgradeport.Cut) error { f.effective = f.grant.SourceSHA; return nil }
		}},
		{"late_admission", func(f *engineFixture) { f.retirement = func(r *upgradetx.Receipt) { f.sequence++ } }},
		{"stale_nonce", func(f *engineFixture) { f.observation = func(o *upgradetx.Observation) { o.Nonce = newID() } }},
		{"stale_sample", func(f *engineFixture) { f.observation = func(o *upgradetx.Observation) { o.At = 1 } }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture(t)
			tc.inject(f)
			_, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
			if err == nil {
				t.Fatal("invalid independent completion proof was accepted")
			}
			for _, r := range f.store.records {
				if r.Forward.Direction == upgradetx.DirectionForward && r.Forward.Phase == upgradetx.PhaseCompleted {
					t.Fatal("invalid proof signed forward completion")
				}
			}
		})
	}
}

// origin: hot-update S8 plan §6.1 / §6.3 / P09
func TestAgentEngineRechecksFinalPreflight(t *testing.T) {
	for _, gate := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(gate), func(t *testing.T) {
			f := fixture(t)
			calls := 0
			f.preflight = func(p *upgradeport.PreflightProof) {
				calls++
				if calls == gate {
					p.ReadersDigest = ""
				}
			}
			_, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
			if err == nil {
				t.Fatal("missing reader proof reached success")
			}
			if strings.Contains(strings.Join(f.calls, ","), "activate") {
				t.Fatal("failed preflight reached activation")
			}
		})
	}
}

// origin: hot-update S8 plan §3.2 / §8.2 F3/F4 / P09
func TestAgentEngineDurabilityFailureNeverRepeatsEffects(t *testing.T) {
	for _, phase := range []upgradetx.Phase{upgradetx.PhaseActivating, upgradetx.PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			f := fixture(t)
			f.applyError = func(e upgradetx.Event) error {
				if e.Phase == phase {
					return errors.New("private-state-secret: disk sync failed")
				}
				return nil
			}
			_, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
			requireError(t, err, upgradetx.CodeUnsettled, true)
			if strings.Contains(err.Error(), "private-state-secret") {
				t.Fatal("adapter error leaked private material")
			}
			if f.store.reverses != 0 {
				t.Fatal("durability uncertainty authorized another execution")
			}
			if phase == upgradetx.PhaseActivating && strings.Contains(strings.Join(f.calls, ","), "activate") {
				t.Fatal("failed intent write reached activation")
			}
			if current(f.store.records[f.grant.Handle.TxID]).Phase == upgradetx.PhaseCompleted {
				t.Fatal("failed durable completion returned success")
			}
		})
	}
}

// origin: hot-update S8 plan §7.1 / M15 / P09
func TestAgentQueryReturnsFreshUnknownWithoutRewritingHistory(t *testing.T) {
	f := fixture(t)
	original := startFixture(t, f).Historical
	f.observation = func(o *upgradetx.Observation) {
		o.OwnerKnown = false
		o.Census.Known = false
		o.Census.Complete = false
	}
	nonce := newID()
	view, err := f.endpoint.Query(testContext(t), f.grant.Handle.Key, f.grant.Handle.TxID, nonce)
	if err != nil || view.Code != upgradetx.CodeUnsettled || view.Observation.Nonce != nonce {
		t.Fatalf("unknown query = %+v, %v", view, err)
	}
	if !reflect.DeepEqual(original, view.Historical) {
		t.Fatal("observation rewrote historical execution")
	}
}

// origin: hot-update S8 plan §6.3 / P09
func TestAgentPreflightCountsLiveCustodyAndRejectsUnknownOwners(t *testing.T) {
	for _, state := range []string{"third_live", "unknown_live", "released_history"} {
		t.Run(state, func(t *testing.T) {
			f := fixture(t)
			f.preflight = func(p *upgradeport.PreflightProof) {
				if state == "unknown_live" {
					p.Census.Resources[0].Generation = 0
					return
				}
				for gen := uint64(3); gen <= 4; gen++ {
					resource := p.Census.Resources[0]
					resource.ID = fmt.Sprintf("retained-%d", gen)
					resource.Generation = gen
					if state == "released_history" {
						resource.State, resource.Custody = "terminal", "released"
					}
					p.Census.Resources = append(p.Census.Resources, resource)
				}
			}
			_, err := f.endpoint.Start(testContext(t), f.grant, f.artifact)
			if state == "released_history" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || f.store.starts != 0 {
				t.Fatal("unknown or third live generation reached acceptance")
			}
		})
	}
}

func (r *recordingRegistry) Commit(stamp upgradetx.Stamp, commit func(upgradetx.Census) error) error {
	c, err := r.Snapshot(stamp)
	if err != nil {
		return err
	}
	return commit(c)
}

func (r *recordingRuntime) Incarnation(upgradetx.Handle) (string, error) {
	return strings.Repeat("f", 64), nil
}
func (s *recordingStore) RecordForwardRelease(h upgradetx.Handle, r upgradetx.Receipt) (upgradetx.Record, error) {
	return s.Apply(h, upgradetx.Event{})
}
