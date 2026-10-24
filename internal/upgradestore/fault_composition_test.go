// Copy into internal/upgradestore; standalone public-API reproductions.
package upgradestore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type faultFixture struct {
	store   *upgradestore.Store
	options upgradestore.Options
	req     upgradetx.PrepareRequest
	grant   upgradetx.Grant
}

func faultNew(t *testing.T, component upgradetx.Component) *faultFixture {
	t.Helper()
	canonical := filepath.Join(t.TempDir(), "canonical")
	if err := os.WriteFile(canonical, []byte("canonical executable bytes"), 0555); err != nil {
		t.Fatal(err)
	}
	return faultNewForInstallation(t, component, canonical)
}

func faultNewForInstallation(t *testing.T, component upgradetx.Component, canonical string) *faultFixture {
	t.Helper()
	dir := t.TempDir()
	key := upgradetx.Key{Component: component, SID: "session", ConfiguredNID: "node", ConfigPath: "/sandbox/config", UID: uint32(os.Getuid())}
	if component == upgradetx.ComponentBroker {
		key.SID = ""
	}
	// Boot registers the installation before preparing grants, so publication
	// and the journal share the same durable control directory and namespace.
	// origin: S8 internal review rv-fault F3
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir, DeploymentID: "rv-fault", Participants: []upgradelock.Participant{{Key: key, ServiceID: "service"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 456, InstanceID: strings.Repeat("1", 32)}
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	scope := upgradetx.Scope{ID: strings.Repeat("2", 32), Participants: []upgradetx.Key{key}, Domains: domains}
	for _, d := range domains {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: d, ID: string(d) + "-original", Origin: "original", Owner: owner, Generation: 1, State: "active", Custody: "source"})
	}
	options := upgradestore.Options{Path: filepath.Join(dir, "upgrade-state.json"), GuardPath: filepath.Join(dir, key.DomainID+".guard"), Key: key, Owner: owner, Transition: upgradetx.Transition}
	store, err := upgradestore.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	req := upgradetx.PrepareRequest{ClientNonce: strings.Repeat("3", 32), ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "A", TargetRelease: "B", SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), CapabilityDigest: strings.Repeat("c", 64), Scope: scope}
	grant, err := store.Prepare(key, req)
	if err != nil {
		t.Fatal(err)
	}
	return &faultFixture{store, options, req, grant}
}
func faultStart(t *testing.T, f *faultFixture) upgradetx.Record {
	t.Helper()
	r, e := f.store.Start(f.grant, upgradetx.Fence{Owner: f.options.Owner, RunningSHA: f.grant.SourceSHA})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func faultHandle(r upgradetx.Record) upgradetx.Handle {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse.Handle
	}
	return r.Forward.Handle
}
func faultApply(t *testing.T, f *faultFixture, r upgradetx.Record, e upgradetx.Event) upgradetx.Record {
	t.Helper()
	out, err := f.store.Apply(faultHandle(r), e)
	if err != nil {
		t.Fatalf("setup event %+v: %v", e, err)
	}
	return out
}
func faultActive(t *testing.T, f *faultFixture) upgradetx.Record {
	t.Helper()
	r := faultStart(t, f)
	for _, e := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}, {Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive}} {
		r = faultApply(t, f, r, e)
	}
	return r
}
func faultDigest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func faultReceipt(f *faultFixture, r upgradetx.Record, kind upgradetx.ReceiptKind, n string) upgradetx.Receipt {
	stamp := upgradetx.Stamp{Attempt: faultHandle(r), Producer: f.options.Owner, Nonce: strings.Repeat(n, 32), Sequence: 1, At: time.Now().UnixNano(), ScopeDigest: f.grant.ScopeDigest}
	resources := append([]upgradetx.Resource(nil), r.Forward.Scope.Expected...)
	for i := range resources {
		resources[i].Generation = r.Forward.Handle.Generation
		resources[i].Custody = "target"
	}
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: faultDigest(r.Forward.Scope.Expected)}
	return upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: f.grant.TargetSHA, Census: census, EvidenceDigest: strings.Repeat(n, 64)}
}
func faultComplete(t *testing.T, f *faultFixture) upgradetx.Record {
	t.Helper()
	r := faultActive(t, f)
	r = faultApply(t, f, r, upgradetx.Event{Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive, Receipt: faultReceipt(f, r, upgradetx.ReceiptControlReady, "4")})
	r = faultApply(t, f, r, upgradetx.Event{Phase: upgradetx.PhaseMigrating, Receipt: faultReceipt(f, r, upgradetx.ReceiptResourcesMigrated, "5")})
	r = faultApply(t, f, r, upgradetx.Event{Receipt: faultReceipt(f, r, upgradetx.ReceiptOldGenerationReleased, "6")})
	return faultApply(t, f, r, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
}

// R0: a safely cancelled action history must not permanently occupy Current.
// origin: S8 internal review rv-fault F4
func TestFaultCancelledJournalAllowsNextStart(t *testing.T) {
	f := faultNew(t, upgradetx.ComponentAgent)
	r := faultStart(t, f)
	action := upgradetx.Action{ID: strings.Repeat("7", 32), Handle: r.Forward.Handle, Kind: upgradetx.ActionPublish, IntentDigest: strings.Repeat("7", 64), State: upgradetx.ActionIntent, Nonce: strings.Repeat("8", 32)}
	r = faultApply(t, f, r, upgradetx.Event{Action: action})
	for _, state := range []upgradetx.ActionState{upgradetx.ActionObserved, upgradetx.ActionDurable} {
		action.Handle = r.Forward.Handle
		action.State = state
		action.ResultDigest = strings.Repeat("9", 64)
		r = faultApply(t, f, r, upgradetx.Event{Action: action})
	}
	r = faultApply(t, f, r, upgradetx.Event{Phase: upgradetx.PhaseCancelled, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomeCancelled})
	reopened, e := upgradestore.Open(f.options)
	if e != nil {
		t.Fatal(e)
	}
	req := f.req
	req.ClientNonce = strings.Repeat("f", 32)
	g, e := reopened.Prepare(f.options.Key, req)
	if e != nil {
		t.Fatal(e)
	}
	_, e = reopened.Start(g, upgradetx.Fence{Current: r.Forward.Handle, Owner: f.options.Owner, RunningSHA: f.grant.SourceSHA, PreviousSHA: r.RecordedPreviousSHA})
	if e != nil {
		t.Fatalf("R0 durable cancellation cannot be followed by another upgrade: %v", e)
	}
}

// A successful forward must leave the next cold boot selecting the target.
// origin: S8 internal review rv-fault F3
func TestFaultCompletedJournalPersistsEffectiveSHA(t *testing.T) {
	f := faultNew(t, upgradetx.ComponentAgent)
	r := faultComplete(t, f)
	reopened, e := upgradestore.Open(f.options)
	if e != nil {
		t.Fatal(e)
	}
	v, e := reopened.Load(f.options.Key, r.Grant.Handle.TxID)
	if e != nil {
		t.Fatal(e)
	}
	if v.Historical.EffectiveSHA != f.grant.TargetSHA {
		t.Fatalf("completed target=%s but durable effective=%s", f.grant.TargetSHA, v.Historical.EffectiveSHA)
	}
}

// The real journal's reverse records must be consumable by its real validator.
// origin: S8 internal review rv-fault F2
func TestFaultReverseRecordSurvivesFirstTransition(t *testing.T) {
	for _, lane := range []string{"partial_active", "completed"} {
		t.Run(lane, func(t *testing.T) {
			f := faultNew(t, upgradetx.ComponentAgent)
			var r upgradetx.Record
			if lane == "completed" {
				r = faultComplete(t, f)
			} else {
				r = faultActive(t, f)
			}
			fence := upgradetx.Fence{Current: faultHandle(r), Owner: f.options.Owner, RunningSHA: f.grant.TargetSHA, PreviousSHA: r.RecordedPreviousSHA, Incarnation: "incarnation"}
			request := upgradetx.ReverseRequest{ClientNonce: strings.Repeat("f", 32), ActorFP: "actor", HistoricalTxID: f.grant.Handle.TxID, TargetSHA: r.RecordedPreviousSHA, Scope: f.req.Scope}
			g, e := f.store.PrepareReverse(fence, request)
			if e != nil {
				t.Fatal(e)
			}
			reverse, e := f.store.Start(g, fence)
			if e != nil {
				t.Fatal(e)
			}
			_, e = f.store.Apply(faultHandle(reverse), upgradetx.Event{Phase: upgradetx.PhaseStaged})
			if e != nil {
				t.Fatalf("%s reverse accepted but its first valid phase fails: %v", lane, e)
			}
		})
	}
}

type faultRuntime struct {
	upgradeport.RuntimePort
	f           *faultFixture
	freezeCalls int
}

func (p *faultRuntime) census(s upgradetx.Stamp) upgradetx.Census {
	s.Producer = p.f.options.Owner
	s.Sequence = 1
	s.At = time.Now().UnixNano()
	return upgradetx.Census{Stamp: s, Known: true, Complete: true, Resources: p.f.req.Scope.Expected, ExpectedDigest: faultDigest(p.f.req.Scope.Expected)}
}
func (p *faultRuntime) Preflight(_ context.Context, g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	return upgradeport.PreflightProof{Grant: g, ReadersDigest: strings.Repeat("1", 64), ConfigDigest: strings.Repeat("2", 64), Census: p.census(upgradetx.Stamp{Attempt: g.Handle, Nonce: strings.Repeat("3", 32), ScopeDigest: g.ScopeDigest})}, nil
}
func (p *faultRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	c := p.census(s)
	o := upgradetx.Observation{Nonce: s.Nonce, At: c.Stamp.At, Census: c, OwnerKnown: true, RunningSHA: p.f.grant.SourceSHA, EffectiveSHA: p.f.grant.SourceSHA}
	if v, e := p.f.store.Load(p.f.options.Key, p.f.grant.Handle.TxID); e == nil {
		o.Current = v.Historical.Forward
	}
	return o, nil
}
func (p *faultRuntime) Freeze(upgradetx.Handle) (upgradeport.Cut, error) {
	p.freezeCalls++
	return upgradeport.Cut{}, errors.New("intentional stop at Freeze")
}
func (p *faultRuntime) Thaw(upgradetx.Handle) error { return nil }

type faultUnusedRegistry struct{ upgradeobserve.Registry }

// No process or IO faults are injected: these engines fail before Freeze with a real store.
// origin: S8 internal review rv-fault F1
func TestFaultEnginesReachFreezeWithRealJournal(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			f := faultNew(t, role)
			runtime := &faultRuntime{f: f}
			var endpoint upgradeport.Endpoint
			if role == upgradetx.ComponentAgent {
				endpoint = agentupgrade.New(f.store, runtime, faultUnusedRegistry{})
			} else {
				endpoint = brokerupgrade.New(f.store, runtime, faultUnusedRegistry{})
			}
			v, e := endpoint.Start(t.Context(), f.grant, upgradeartifact.Receipt{Grant: f.grant, SourceSHA: f.grant.SourceSHA, TargetSHA: f.grant.TargetSHA})
			if runtime.freezeCalls == 0 {
				t.Fatalf("engine never reached Freeze with real Store; phase=%s reason=%s err=%v", v.Historical.Forward.Phase, v.Historical.Reason, e)
			}
		})
	}
}

// A final receipt must include requests admitted after the original scope was sealed.
// origin: S8 internal review rv-fault F5
func TestFaultJournalAcceptsLateAdmissionReceipt(t *testing.T) {
	f := faultNew(t, upgradetx.ComponentAgent)
	r := faultActive(t, f)
	receipt := faultReceipt(f, r, upgradetx.ReceiptControlReady, "4")
	receipt.Census.Resources = append(receipt.Census.Resources, upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "late-child", Origin: "request-after-ACTIVE", Owner: f.options.Owner, Generation: r.Forward.Handle.Generation, State: "active", Custody: "target"})
	event := upgradetx.Event{Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive, Receipt: receipt}
	if _, e := upgradetx.Transition(r, event); e != nil {
		t.Fatalf("pure validator must accept the correct superset first: %v", e)
	}
	if _, e := f.store.Apply(r.Forward.Handle, event); e != nil {
		t.Fatalf("real journal rejects a valid receipt containing the new request: %v", e)
	}
}

// Files are real immutable publications; only peer authorization/decoder facts are test ports.
type faultPublication struct {
	grant    upgradetx.Grant
	location upgradeartifact.Location
}
type faultPublicationLease struct{ domain string }

func (l faultPublicationLease) FD() uintptr      { return 0 }
func (l faultPublicationLease) DomainID() string { return l.domain }
func (l faultPublicationLease) Close() error     { return nil }
func (p faultPublication) Authorize(context.Context, upgradetx.Grant) (upgradeartifact.Location, error) {
	return p.location, nil
}
func (p faultPublication) Reserve(context.Context, upgradetx.Grant) (upgradelock.Lease, error) {
	return faultPublicationLease{p.grant.Handle.Key.DomainID}, nil
}
func (p faultPublication) CheckImage(_ context.Context, path, want, release, cap string) (string, error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		return "", errors.New("SHA mismatch")
	}
	return strings.Repeat("a", 64), nil
}

// origin: S8 internal review rv-fault F3
func TestFaultPublishedArtifactsResolveFromDurableJournal(t *testing.T) {
	source, target := []byte("source executable bytes"), []byte("target executable bytes")
	base := t.TempDir()
	loc := upgradeartifact.Location{CanonicalPath: filepath.Join(base, "canonical"), Directory: filepath.Join(base, "artifacts")}
	if e := os.WriteFile(loc.CanonicalPath, target, 0555); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(loc.Directory, 0755); e != nil {
		t.Fatal(e)
	}
	f := faultNewForInstallation(t, upgradetx.ComponentAgent, loc.CanonicalPath)
	a, b := sha256.Sum256(source), sha256.Sum256(target)
	f.req.SourceSHA, f.req.TargetSHA = hex.EncodeToString(a[:]), hex.EncodeToString(b[:])
	f.req.ClientNonce = strings.Repeat("e", 32)
	var e error
	f.grant, e = f.store.Prepare(f.options.Key, f.req)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := upgradeartifact.Publish(upgradeartifact.WithPublicationPort(t.Context(), faultPublication{f.grant, loc}), f.grant, bytes.NewReader(source), bytes.NewReader(target))
	if e != nil {
		t.Fatal(e)
	}
	if e = upgradeartifact.Verify(receipt, f.grant); e != nil {
		t.Fatal(e)
	}
	faultStart(t, f)
	reopened, e := upgradestore.Open(f.options)
	if e != nil {
		t.Fatal(e)
	}
	view, e := reopened.Load(f.options.Key, f.grant.Handle.TxID)
	if e != nil {
		t.Fatal(e)
	}
	path, e := upgradeartifact.Resolve(f.options.Key, view)
	if e != nil || path != receipt.SourcePath {
		t.Fatalf("accepted journal cannot resolve its safe source on cold boot; pins=%v path=%s err=%v", view.Metadata.Pins, path, e)
	}
}

// Independent observer supplies the terminal census of the dead source owner.
// No process is spawned; this checks the two production layers' proof contract.
type faultDeathRuntime struct{ *faultRuntime }

func (p *faultDeathRuntime) ObserveDeath(s upgradetx.Stamp) (upgradetx.DeathProof, error) {
	dead := p.f.options.Owner
	s.Producer = dead
	s.Producer.PID++
	s.Producer.InstanceID = strings.Repeat("9", 32)
	s.Sequence = 1
	s.At = time.Now().UnixNano()
	resources := append([]upgradetx.Resource(nil), p.f.req.Scope.Expected...)
	for i := range resources {
		resources[i].State = "terminal"
		resources[i].Custody = "released"
	}
	census := upgradetx.Census{Stamp: s, Known: true, Complete: true, Resources: resources, ExpectedDigest: faultDigest(p.f.req.Scope.Expected)}
	return upgradetx.DeathProof{Stamp: s, Owner: dead, Reaped: true, NoOwnedFDs: true, Custody: census}, nil
}

// origin: S8 internal review rv-fault F6
func TestFaultIndependentDeathProofCanReleaseCurrent(t *testing.T) {
	for _, throughEngine := range []bool{false, true} {
		name := "store-control"
		if throughEngine {
			name = "agent-endpoint"
		}
		t.Run(name, func(t *testing.T) {
			f := faultNew(t, upgradetx.ComponentAgent)
			r := faultActive(t, f)
			runtime := &faultDeathRuntime{&faultRuntime{f: f}}
			var e error
			if throughEngine {
				_, e = agentupgrade.New(f.store, runtime, faultUnusedRegistry{}).Archive(t.Context(), faultHandle(r))
			} else {
				proof, _ := runtime.ObserveDeath(upgradetx.Stamp{Attempt: faultHandle(r), Nonce: strings.Repeat("9", 32), ScopeDigest: f.grant.ScopeDigest})
				_, e = f.store.Archive(faultHandle(r), proof)
			}
			if e != nil {
				t.Fatalf("valid independent death/custody proof cannot archive abandoned: %v", e)
			}
		})
	}
}
