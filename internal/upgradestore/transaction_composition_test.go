// Copy into internal/upgradestore. These tests compose real S8 leaf packages.
// No child processes or daemon state are used. Runtime observations are explicit DTOs.
package upgradestore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func txnHex(c string, n int) string { return strings.Repeat(c, n) }
func txnDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func txnDomains() []upgradetx.Domain {
	return []upgradetx.Domain{
		upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore,
		upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY,
		upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade,
	}
}

type txnFixture struct {
	store  *upgradestore.Store
	opts   upgradestore.Options
	req    upgradetx.PrepareRequest
	record upgradetx.Record
}

func txnNew(t *testing.T, idle bool) *txnFixture {
	t.Helper()
	dir := t.TempDir()
	k := upgradetx.Key{DomainID: txnHex("a", 32), Component: upgradetx.ComponentBroker, ConfiguredNID: "broker", ConfigPath: "/test/broker.yaml", UID: uint32(os.Getuid())}
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 100, InstanceID: txnHex("a", 32)}
	scope := upgradetx.Scope{ID: txnHex("b", 32), Participants: []upgradetx.Key{k}, Domains: txnDomains(), Expected: []upgradetx.Resource{}}
	if !idle {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: upgradetx.DomainControl, ID: "control", Origin: "original", Owner: owner, Generation: 1, State: "running", Custody: "source"})
	}
	opts := upgradestore.Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, k.DomainID+".guard"), Key: k, Owner: owner, Transition: upgradetx.Transition}
	s, err := upgradestore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	req := upgradetx.PrepareRequest{ClientNonce: txnHex("c", 32), ActorFP: "actor", SourceRelease: "A", TargetRelease: "B", SourceSHA: txnHex("a", 64), TargetSHA: txnHex("b", 64), Mode: upgradetx.ModeHot, Scope: scope, CapabilityDigest: txnHex("c", 64)}
	g, err := s.Prepare(k, req)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Start(g, upgradetx.Fence{Owner: owner, RunningSHA: g.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	return &txnFixture{s, opts, req, r}
}
func (f *txnFixture) apply(t *testing.T, e upgradetx.Event) {
	t.Helper()
	a := f.record.Forward
	if f.record.Reverse.Handle.TxID != "" {
		a = f.record.Reverse
	}
	r, err := f.store.Apply(a.Handle, e)
	if err != nil {
		t.Fatal(err)
	}
	f.record = r
}
func (f *txnFixture) active(t *testing.T) {
	t.Helper()
	for _, e := range []upgradetx.Event{
		{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady},
		{Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine},
		{Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive},
	} {
		f.apply(t, e)
	}
}
func (f *txnFixture) receipt(kind upgradetx.ReceiptKind) upgradetx.Receipt {
	a := f.record.Forward
	stamp := upgradetx.Stamp{Attempt: a.Handle, Producer: f.opts.Owner, Nonce: txnHex("d", 32), Sequence: 1, At: time.Now().UnixNano(), ScopeDigest: f.record.Grant.ScopeDigest}
	resources := append([]upgradetx.Resource{}, a.Scope.Expected...)
	for i := range resources {
		resources[i].Owner = f.opts.Owner
		resources[i].Generation = a.Handle.Generation
		resources[i].Custody = "target"
	}
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: txnDigest(a.Scope.Expected), Resources: resources}
	return upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: census, EvidenceDigest: txnHex("e", 64)}
}
func (f *txnFixture) receipts(t *testing.T) {
	t.Helper()
	f.apply(t, upgradetx.Event{Receipt: f.receipt(upgradetx.ReceiptControlReady), Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive})
	f.apply(t, upgradetx.Event{Receipt: f.receipt(upgradetx.ReceiptResourcesMigrated), Phase: upgradetx.PhaseMigrating})
	f.apply(t, upgradetx.Event{Receipt: f.receipt(upgradetx.ReceiptOldGenerationReleased), CutStage: upgradetx.CutRetiring})
}
func (f *txnFixture) complete(t *testing.T) {
	t.Helper()
	f.active(t)
	f.receipts(t)
	f.apply(t, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
}

// origin: S8 adversarial review; the real domain and journal must share one Key.
// origin: S8 internal review rv-txn F07
func TestAgentKeyAcceptedByDomainAndJournal(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "tether")
	if err := os.WriteFile(bin, []byte("image"), 0755); err != nil {
		t.Fatal(err)
	}
	k := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "agent", UID: uint32(os.Getuid())}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: bin, ControlDir: dir, DeploymentID: "deployment", Participants: []upgradelock.Participant{{Key: k, ServiceID: "agent"}}})
	if err != nil {
		t.Fatal(err)
	}
	k.DomainID = id
	// ctx-root: hermetic transaction test entry point.
	lease, err := upgradelock.Acquire(context.Background(), k, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lease.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	_, err = upgradestore.Open(upgradestore.Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, id+".guard"), Key: k, Owner: upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 1, InstanceID: txnHex("a", 32)}, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatalf("registered agent key rejected by durable store: %v", err)
	}
}

// origin: S8 internal review rv-txn F10
func TestIdleGrantStartsAtUsableGeneration(t *testing.T) {
	f := txnNew(t, true)
	if _, err := f.store.Apply(f.record.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseStaged}); err != nil {
		t.Fatalf("idle accepted grant generation=%d source=%d cannot transition: %v", f.record.Forward.Handle.Generation, f.record.Forward.SourceGeneration, err)
	}
}

// origin: S8 internal review rv-txn F04
func TestDurableActionAgreesWithTransitionContract(t *testing.T) {
	t.Run("original_action_revision", func(t *testing.T) {
		f := txnNew(t, false)
		action := upgradetx.Action{ID: txnHex("e", 32), Handle: f.record.Forward.Handle, Kind: upgradetx.ActionPublish, IntentDigest: txnHex("d", 64), Nonce: txnHex("e", 32), State: upgradetx.ActionIntent}
		f.apply(t, upgradetx.Event{Action: action})
		action.State = upgradetx.ActionObserved
		action.ResultDigest = txnHex("f", 64)
		if _, err := upgradetx.Transition(f.record, upgradetx.Event{Action: action}); err != nil {
			t.Fatalf("positive predicate premise: %v", err)
		}
		if _, err := f.store.Apply(f.record.Forward.Handle, upgradetx.Event{Action: action}); err != nil {
			t.Fatalf("real store rejects engine result with immutable action revision: %v", err)
		}
	})
	t.Run("phase_and_intent", func(t *testing.T) {
		f := txnNew(t, false)
		f.apply(t, upgradetx.Event{Phase: upgradetx.PhaseStaged})
		f.apply(t, upgradetx.Event{Phase: upgradetx.PhaseReady})
		action := upgradetx.Action{ID: txnHex("e", 32), Handle: f.record.Forward.Handle, Kind: upgradetx.ActionFreeze, IntentDigest: txnHex("d", 64), Nonce: txnHex("e", 32), State: upgradetx.ActionIntent}
		ev := upgradetx.Event{Action: action, Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine, Outcome: upgradetx.OutcomePending}
		if _, err := upgradetx.Transition(f.record, ev); err != nil {
			t.Fatalf("positive predicate premise: %v", err)
		}
		if _, err := f.store.Apply(f.record.Forward.Handle, ev); err != nil {
			t.Fatalf("real store rejects engine atomic phase+intent: %v", err)
		}
	})
}

// origin: S8 internal review rv-txn F06
func TestCompletedJournalSelectsTargetOnNextStart(t *testing.T) {
	f := txnNew(t, false)
	f.complete(t)
	reopened, err := upgradestore.Open(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	req := f.req
	req.ClientNonce = txnHex("f", 32)
	req.SourceSHA = f.record.Forward.TargetSHA
	req.TargetSHA = txnHex("c", 64)
	req.SourceRelease = "B"
	req.TargetRelease = "C"
	g, err := reopened.Prepare(f.opts.Key, req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reopened.Start(g, upgradetx.Fence{Current: f.record.Forward.Handle, Owner: f.opts.Owner, RunningSHA: g.SourceSHA, PreviousSHA: f.record.RecordedPreviousSHA})
	if err != nil {
		t.Fatalf("completed target B cannot start B->C: effective=%s: %v", f.record.EffectiveSHA, err)
	}
}

// origin: S8 internal review rv-txn F05
func TestReverseStartProducesTransitionableRecord(t *testing.T) {
	for _, manual := range []bool{false, true} {
		name := "automatic"
		if manual {
			name = "manual"
		}
		t.Run(name, func(t *testing.T) {
			f := txnNew(t, false)
			if manual {
				f.complete(t)
			} else {
				f.active(t)
			}
			a := f.record.Forward
			fence := upgradetx.Fence{Current: a.Handle, Owner: f.opts.Owner, RunningSHA: a.TargetSHA, PreviousSHA: f.record.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
			g, err := f.store.PrepareReverse(fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: a.Scope})
			if err != nil {
				t.Fatal(err)
			}
			r, err := f.store.Start(g, fence)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.Apply(r.Reverse.Handle, upgradetx.Event{Phase: upgradetx.PhaseStaged}); err != nil {
				t.Fatalf("%s reverse accepted but its first transition is corrupt: %v", name, err)
			}
		})
	}
}

// Only issuer/decoder attestations are supplied by this test port. Reservation,
// immutable file publication, journal fsync and cold selector resolution are real.
type txnPublication struct{ location upgradeartifact.Location }

func (p txnPublication) Authorize(context.Context, upgradetx.Grant) (upgradeartifact.Location, error) {
	return p.location, nil
}
func (p txnPublication) Reserve(ctx context.Context, g upgradetx.Grant) (upgradelock.Lease, error) {
	return upgradelock.Acquire(ctx, g.Handle.Key, "")
}
func (p txnPublication) CheckImage(context.Context, string, string, string, string) (string, error) {
	return txnHex("e", 64), nil
}

// origin: S8 internal review rv-txn F06
func TestJournalPinsAreConsumableByColdBootResolver(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "tether")
	slots := filepath.Join(dir, "artifacts")
	if err := os.Mkdir(slots, 0700); err != nil {
		t.Fatal(err)
	}
	source, target := []byte("source image"), []byte("target image")
	if err := os.WriteFile(canonical, source, 0755); err != nil {
		t.Fatal(err)
	}
	sha := func(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
	k := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "broker", ConfigPath: "/test/broker.yaml", UID: uint32(os.Getuid())}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir, DeploymentID: "publication", Participants: []upgradelock.Participant{{Key: k, ServiceID: "broker"}}})
	if err != nil {
		t.Fatal(err)
	}
	k.DomainID = id
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 1, InstanceID: txnHex("a", 32)}
	scope := upgradetx.Scope{ID: txnHex("b", 32), Participants: []upgradetx.Key{k}, Domains: txnDomains(), Expected: []upgradetx.Resource{{Domain: upgradetx.DomainControl, ID: "control", Origin: "original", Owner: owner, Generation: 1, State: "running", Custody: "source"}}}
	opts := upgradestore.Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, id+".guard"), Key: k, Owner: owner, Transition: upgradetx.Transition}
	store, err := upgradestore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	g, err := store.Prepare(k, upgradetx.PrepareRequest{ClientNonce: txnHex("c", 32), ActorFP: "actor", SourceRelease: "A", TargetRelease: "B", SourceSHA: sha(source), TargetSHA: sha(target), Mode: upgradetx.ModeHot, Scope: scope, CapabilityDigest: txnHex("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: hermetic transaction test entry point.
	ctx := upgradeartifact.WithPublicationPort(context.Background(), txnPublication{upgradeartifact.Location{CanonicalPath: canonical, Directory: slots}})
	receipt, err := upgradeartifact.Publish(ctx, g, bytes.NewReader(source), bytes.NewReader(target))
	if err != nil {
		t.Fatal(err)
	}
	if err := upgradeartifact.Verify(receipt, g); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Start(g, upgradetx.Fence{Owner: owner, RunningSHA: g.SourceSHA}); err != nil {
		t.Fatal(err)
	}
	reopened, err := upgradestore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	view, err := reopened.Load(k, g.Handle.TxID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := upgradeartifact.Resolve(k, view)
	if err != nil {
		t.Fatalf("published two real immutable artifacts, but cold boot cannot resolve journal pins %v: %v", view.Metadata.Pins, err)
	}
	if got != receipt.SourcePath {
		t.Fatalf("got %s, want known source %s", got, receipt.SourcePath)
	}
}

func txnRegistry(t *testing.T, a upgradetx.Attempt, owner upgradetx.Owner) upgradeobserve.Registry {
	t.Helper()
	expected, err := upgradeobserve.ExpectedDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	observers := make([]upgradeobserve.DomainObserver, 0, len(a.Scope.Domains))
	for _, d := range a.Scope.Domains {
		observers = append(observers, upgradeobserve.DomainObserver{Domain: d, CapabilityDigest: txnHex("c", 64), Observe: func(s upgradetx.Stamp) (upgradetx.Census, error) {
			c := upgradetx.Census{Stamp: s, Known: true, Complete: true, ExpectedDigest: expected}
			for _, r := range a.Scope.Expected {
				if r.Domain == d {
					r.Owner = owner
					r.Generation = a.Handle.Generation
					r.Custody = "target"
					c.Resources = append(c.Resources, r)
				}
			}
			return c, nil
		}})
	}
	registry, err := upgradeobserve.New(upgradeobserve.Config{Attempt: a, Producer: owner, Observers: observers, Probe: func(s upgradetx.Stamp) (upgradetx.Observation, error) {
		return upgradetx.Observation{Current: a, Nonce: s.Nonce, At: s.At, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA, Census: upgradetx.Census{Stamp: s}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

// origin: S8 internal review rv-txn F08
func TestRegistrySealSurvivesJournalRevision(t *testing.T) {
	f := txnNew(t, false)
	r := txnRegistry(t, f.record.Forward, f.opts.Owner)
	if _, err := r.Seal(f.record.Forward, txnHex("e", 32)); err != nil {
		t.Fatalf("initial seal premise: %v", err)
	}
	f.apply(t, upgradetx.Event{Phase: upgradetx.PhaseStaged})
	if _, err := r.Seal(f.record.Forward, txnHex("f", 32)); err != nil {
		t.Fatalf("same cut after one durable write cannot be sealed: %v", err)
	}
}

// origin: S8 internal review rv-txn F03
func TestCompletionCASClosesAdmissionBarrier(t *testing.T) {
	f := txnNew(t, false)
	f.active(t)
	f.receipts(t)
	r := txnRegistry(t, f.record.Forward, f.opts.Owner)
	sealed, err := r.Seal(f.record.Forward, txnHex("e", 32))
	if err != nil {
		t.Fatal(err)
	}
	// This is the engine's last read; a new target request enters before Apply.
	if _, err = r.Snapshot(sealed.Stamp); err != nil {
		t.Fatal(err)
	}
	late := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "late-job", Origin: "late-request", Owner: f.opts.Owner, Generation: f.record.Forward.Handle.Generation, State: "starting", Custody: "target"}
	if _, err = r.Begin(late); err != nil {
		return
	} // Holding admission through CAS is a safe fix.
	completed, err := f.store.Apply(f.record.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
	if err == nil {
		t.Fatalf("completed while late Begin remains unfinished: phase=%s, sealed_seq=%d", completed.Forward.Phase, sealed.Stamp.Sequence)
	}
}

// origin: S8 internal review rv-txn F11
func TestReleasedTerminalDoesNotConsumeLiveGeneration(t *testing.T) {
	f := txnNew(t, false)
	resource := f.record.Forward.Scope.Expected[0]
	resource.State = "terminal"
	resource.Custody = "released"
	census := upgradetx.Census{Known: true, Complete: true, ExpectedDigest: txnHex("e", 64), Stamp: upgradetx.Stamp{Producer: f.opts.Owner}, Resources: []upgradetx.Resource{resource}}
	if err := upgradecompat.CheckGenerations(census, 2, 3); err != nil {
		t.Fatalf("retired generation 1 prevents current 2 -> target 3: %v", err)
	}
}

// origin: S8 internal review rv-txn F10
func TestStoreAndObserverCanonicalScopeAgree(t *testing.T) {
	f := txnNew(t, true)
	want, err := upgradeobserve.ScopeDigest(f.req.Scope)
	if err != nil {
		t.Fatal(err)
	}
	if want != f.record.Grant.ScopeDigest {
		t.Fatalf("same idle scope has different store/observer digests: %s != %s", f.record.Grant.ScopeDigest, want)
	}
}

// origin: S8 internal review rv-txn F09
func TestLateIdentityReceiptSurvivesDurableStore(t *testing.T) {
	f := txnNew(t, false)
	f.active(t)
	receipt := f.receipt(upgradetx.ReceiptControlReady)
	late := receipt.Census.Resources[0]
	late.ID = "late-job"
	late.Domain = upgradetx.DomainExec
	receipt.Census.Resources = append(receipt.Census.Resources, late)
	sort.Slice(receipt.Census.Resources, func(i, j int) bool { return receipt.Census.Resources[i].Domain < receipt.Census.Resources[j].Domain })
	event := upgradetx.Event{Receipt: receipt, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive}
	if _, err := upgradetx.Transition(f.record, event); err != nil {
		t.Fatalf("predicate premise: %v", err)
	}
	if _, err := f.store.Apply(f.record.Forward.Handle, event); err != nil {
		t.Fatalf("new request is allowed by predicate but rejected by journal: %v", err)
	}
}

// origin: S8 internal review rv-txn F09
func TestTerminalReceiptVocabularyIsShared(t *testing.T) {
	f := txnNew(t, false)
	f.active(t)
	a := f.record.Forward
	census := f.receipt(upgradetx.ReceiptResourcesMigrated).Census
	census.Resources[0] = a.Scope.Expected[0]
	census.Resources[0].State = "terminal"
	census.Resources[0].Custody = "durable"
	observation := upgradetx.Observation{Nonce: census.Stamp.Nonce, At: census.Stamp.At, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
	receipt, err := upgradeobserve.ResourcesProof(a, observation, census.Stamp.Nonce)
	if err != nil {
		t.Fatalf("real observer premise: %v", err)
	}
	if _, err := upgradetx.Transition(f.record, upgradetx.Event{Receipt: receipt}); err != nil {
		t.Fatalf("real collector terminal receipt rejected by transaction validator: %v", err)
	}
}
