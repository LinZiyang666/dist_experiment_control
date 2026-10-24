package upgradestore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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

// This runtime supplies independent local facts only. Journal, transitions,
// publication, registry and both endpoints are the production implementations.
// It does not certify production agent/broker adapters or same-PID exec.
type composedRuntime struct {
	t               *testing.T
	store           *upgradestore.Store
	options         upgradestore.Options
	registry        upgradeobserve.Registry
	scope           upgradetx.Scope
	owner           upgradetx.Owner
	grant           upgradetx.Grant
	receipt         upgradeartifact.Receipt
	location        upgradeartifact.Location
	running         string
	resources       []upgradetx.Resource
	sequence        uint64
	effects         int
	failActivate    bool
	failPreflightAt int
	preflights      int
}

func composedAttempt(r upgradetx.Record) upgradetx.Attempt {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse
	}
	return r.Forward
}
func (r *composedRuntime) current() upgradetx.Attempt {
	v, err := r.store.Load(r.options.Key, "")
	if err != nil {
		return upgradetx.Attempt{}
	}
	return composedAttempt(v.Historical)
}
func (r *composedRuntime) census(s upgradetx.Stamp, scope upgradetx.Scope) upgradetx.Census {
	s.Producer = r.owner
	s.Sequence = r.sequence
	s.At = time.Now().UnixNano()
	s.ScopeDigest, _ = upgradetx.ScopeDigest(scope)
	expected, _ := upgradetx.ExpectedDigest(scope)
	return upgradetx.Census{Stamp: s, Known: true, Complete: true, ExpectedDigest: expected, Resources: append([]upgradetx.Resource{}, r.resources...)}
}
func (r *composedRuntime) Preflight(_ context.Context, g upgradetx.Grant, _ upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	r.preflights++
	if r.failPreflightAt == r.preflights {
		return upgradeport.PreflightProof{}, errors.New("reader withdrawn")
	}
	r.grant = g
	c := r.census(upgradetx.Stamp{Attempt: g.Handle, Nonce: txnHex("e", 32)}, r.scope)
	return upgradeport.PreflightProof{Grant: g, ReadersDigest: txnHex("d", 64), ConfigDigest: txnHex("e", 64), Census: c}, nil
}
func (r *composedRuntime) cut(h upgradetx.Handle) (upgradeport.Cut, error) {
	a := r.current()
	return upgradeport.Cut{Attempt: h, Owner: r.owner, Stage: upgradetx.CutPristine, Descriptor: []byte("current state"), Scope: a.Scope, Sequence: r.sequence}, nil
}
func (r *composedRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error)        { return r.cut(h) }
func (r *composedRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) { return r.cut(h) }
func (r *composedRuntime) Activate(c upgradeport.Cut) (upgradeport.Activation, error) {
	r.effects++
	a := r.current()
	r.running = a.TargetSHA
	for i := range r.resources {
		if !upgradetx.ResourceNotApplicable(r.resources[i]) && !upgradetx.ResourceReleased(r.resources[i]) {
			r.resources[i].Owner = r.owner
			r.resources[i].Generation = a.Handle.Generation
			r.resources[i].Custody = "target"
		}
	}
	if r.failActivate {
		r.failActivate = false
		return upgradeport.Activation{}, errors.New("partial activation")
	}
	return upgradeport.Activation{Attempt: c.Attempt, Owner: r.owner, Stage: upgradetx.CutActive, RunningSHA: r.running}, nil
}
func (r *composedRuntime) Observe(s upgradetx.Stamp) (upgradetx.Observation, error) {
	a := r.current()
	scope := r.scope
	if a.Handle.TxID != "" {
		scope = a.Scope
	}
	c := r.census(s, scope)
	if a.Handle.TxID != "" && r.running == a.TargetSHA && a.CutStage == upgradetx.CutPartialActive {
		a.CutStage = upgradetx.CutActive
	}
	if r.running == a.TargetSHA {
		a.Control = upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: r.running, Census: c, EvidenceDigest: txnHex("e", 64)}
	}
	return upgradetx.Observation{Current: a, Nonce: s.Nonce, At: c.Stamp.At, OwnerKnown: true, RunningSHA: r.running, EffectiveSHA: r.running, Census: c}, nil
}
func (r *composedRuntime) Retire(h upgradetx.Handle) (upgradetx.Receipt, error) {
	a := r.current()
	if !upgradetx.SameCut(a.Handle, h) {
		v, err := r.store.Load(h.Key, h.TxID)
		if err != nil {
			return upgradetx.Receipt{}, err
		}
		a = v.Historical.Forward
	}
	c := r.census(upgradetx.Stamp{Attempt: h, Nonce: txnHex("f", 32)}, a.Scope)
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: c.Stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: c, EvidenceDigest: txnHex("e", 64)}, nil
}
func (r *composedRuntime) Thaw(upgradetx.Handle) error                  { return nil }
func (r *composedRuntime) Incarnation(upgradetx.Handle) (string, error) { return txnHex("f", 64), nil }
func (r *composedRuntime) reopen() {
	s, err := upgradestore.Open(r.options)
	if err != nil {
		r.t.Fatal(err)
	}
	r.store = s
}
func (r *composedRuntime) endpoint() upgradeport.Endpoint {
	if r.options.Key.Component == upgradetx.ComponentAgent {
		return agentupgrade.New(r.store, r, r.registry)
	}
	return brokerupgrade.New(r.store, r, r.registry)
}
func (r *composedRuntime) prepare(source, target []byte) upgradetx.Grant {
	r.t.Helper()
	hash := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	g, err := r.store.Prepare(r.options.Key, upgradetx.PrepareRequest{ClientNonce: hex.EncodeToString([]byte(time.Now().Format("150405.000000000")))[:32], ActorFP: "actor", SourceRelease: "source", TargetRelease: "target", SourceSHA: hash(source), TargetSHA: hash(target), Mode: upgradetx.ModeHot, Scope: r.scope, CapabilityDigest: txnHex("c", 64)})
	if err != nil {
		r.t.Fatal(err)
	}
	r.grant = g
	// ctx-root: isolated publication and endpoint exercise.
	ctx := upgradeartifact.WithPublicationPort(context.Background(), txnPublication{r.location})
	r.receipt, err = upgradeartifact.Publish(ctx, g, bytes.NewReader(source), bytes.NewReader(target))
	if err != nil {
		r.t.Fatal(err)
	}
	a := upgradetx.Attempt{Handle: g.Handle, SourceGeneration: g.Handle.Generation - 1, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, Scope: r.scope, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}
	// The actual source survives a cancelled candidate allocation unchanged.
	if prior, err := r.store.Load(r.options.Key, ""); err == nil && upgradetx.CancellationSettled(prior.Historical) {
		a.SourceGeneration = composedAttempt(prior.Historical).SourceGeneration
	}
	r.installRegistry(a)
	return g
}
func (r *composedRuntime) installRegistry(a upgradetx.Attempt) {
	r.t.Helper()
	r.sequence = 1
	var observers []upgradeobserve.DomainObserver
	for _, d := range a.Scope.Domains {
		na := false
		for _, e := range a.Scope.Expected {
			if e.Domain == d && upgradetx.ResourceNotApplicable(e) {
				na = true
			}
		}
		b := upgradeobserve.DomainObserver{Domain: d, CapabilityDigest: txnHex("c", 64), NotApplicable: na}
		if !na {
			b.Observe = func(s upgradetx.Stamp) (upgradetx.Census, error) {
				scope := r.current().Scope
				if scope.ID == "" {
					scope = a.Scope
				}
				c := r.census(s, scope)
				c.Stamp = s
				c.Resources = nil
				for _, row := range r.resources {
					if row.Domain == d {
						c.Resources = append(c.Resources, row)
					}
				}
				return c, nil
			}
		}
		observers = append(observers, b)
	}
	var err error
	r.registry, err = upgradeobserve.New(upgradeobserve.Config{Attempt: a, Producer: r.owner, Observers: observers, Probe: func(s upgradetx.Stamp) (upgradetx.Observation, error) {
		o, err := r.Observe(s)
		o.Census.Stamp = s
		o.At = s.At
		return o, err
	}})
	if err != nil {
		r.t.Fatal(err)
	}
}
func newComposedRuntime(t *testing.T, component upgradetx.Component) *composedRuntime {
	t.Helper()
	dir := t.TempDir()
	canonical := filepath.Join(dir, "entry")
	slots := filepath.Join(dir, "artifacts")
	if err := os.WriteFile(canonical, []byte("A"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(slots, 0700); err != nil {
		t.Fatal(err)
	}
	key := upgradetx.Key{Component: component, ConfiguredNID: "node", UID: uint32(os.Getuid())}
	if component == upgradetx.ComponentAgent {
		key.SID = "session"
	} else {
		key.ConfigPath = filepath.Join(dir, "broker.yaml")
	}
	id, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: dir, DeploymentID: "composition", Participants: []upgradelock.Participant{{Key: key, ServiceID: "service"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = id
	owner := upgradetx.Owner{BootID: txnHex("b", 32), PID: 123, StartTicks: 9, InstanceID: txnHex("a", 32)}
	scope := upgradetx.Scope{ID: txnHex("d", 32), Participants: []upgradetx.Key{key}, Domains: txnDomains(), Expected: []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "job", Origin: "original", Generation: 1, Owner: owner, State: "active", Custody: "source"}}}
	r := &composedRuntime{t: t, owner: owner, scope: scope, resources: append([]upgradetx.Resource{}, scope.Expected...), sequence: 1, location: upgradeartifact.Location{CanonicalPath: canonical, Directory: slots}}
	r.options = upgradestore.Options{Path: filepath.Join(dir, "state.json"), GuardPath: filepath.Join(dir, id+".guard"), Key: key, Owner: owner, Transition: upgradetx.Transition}
	r.reopen()
	r.prepare([]byte("A"), []byte("B"))
	r.running = r.grant.SourceSHA
	return r
}

// origin: S8 internal review rv-txn F03 F04 F06 F07 F08 F09 F10
func TestEnginesComposeWithDurableStoreAndRegistry(t *testing.T) {
	for _, component := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(component), func(t *testing.T) {
			r := newComposedRuntime(t, component)
			// ctx-root: hermetic engine transaction entry point.
			v, err := r.endpoint().Start(context.Background(), r.grant, r.receipt)
			if err != nil {
				t.Fatalf("real engine/store/registry forward failed in %s: %v", composedAttempt(v.Historical).Phase, err)
			}
			if composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
				t.Fatal("forward did not complete exactly once")
			}
			r.reopen()
			v, err = r.store.Load(r.options.Key, r.grant.Handle.TxID)
			if err != nil {
				t.Fatal(err)
			}
			path, err := upgradeartifact.Resolve(r.options.Key, v)
			if err != nil || path != r.receipt.TargetPath {
				t.Fatalf("cold target resolution: %q %v", path, err)
			}
			if _, err = r.store.CloseWindow(composedAttempt(v.Historical).Handle); err != nil {
				t.Fatal(err)
			}
			r.reopen()
			v, err = r.store.Load(r.options.Key, r.grant.Handle.TxID)
			if err != nil {
				t.Fatal(err)
			}
			if path, err = upgradeartifact.Resolve(r.options.Key, v); err != nil || path != r.receipt.TargetPath {
				t.Fatalf("closed-window cold boot: %q %v", path, err)
			}
			r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
			r.prepare([]byte("B"), []byte("C"))
			// ctx-root: the second isolated upgrade uses the cold selected source.
			v, err = r.endpoint().Start(context.Background(), r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 2 {
				t.Fatalf("second Start failed: %v", err)
			}
		})
	}
}

// origin: S8 internal review rv-txn F05 F06 F08
func TestEngineReverseSurvivesReopenColdBootAndSecondStart(t *testing.T) {
	for _, component := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(component), func(t *testing.T) {
			r := newComposedRuntime(t, component)
			// ctx-root: isolated forward and reverse lifecycle.
			ctx := context.Background()
			v, err := r.endpoint().Start(ctx, r.grant, r.receipt)
			if err != nil {
				t.Fatal(err)
			}
			original := v.Historical
			r.reopen()
			a := r.current()
			fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: original.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
			v, err = r.endpoint().Rollback(ctx, fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: a.Scope})
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("manual reverse: %v", err)
			}
			r.reopen()
			old, err := r.store.Load(r.options.Key, original.Grant.Handle.TxID)
			if err != nil || !reflect.DeepEqual(old.Historical, original) {
				t.Fatal("manual reverse rewrote forward history")
			}
			v, err = r.store.Load(r.options.Key, "")
			if err != nil {
				t.Fatal(err)
			}
			path, err := upgradeartifact.Resolve(r.options.Key, v)
			if err != nil || path != r.receipt.SourcePath {
				t.Fatalf("reverse cold resolve: %q %v", path, err)
			}
			r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
			r.prepare([]byte("A"), []byte("C"))
			v, err = r.endpoint().Start(ctx, r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("post reverse Start: %v", err)
			}
		})
	}
}
