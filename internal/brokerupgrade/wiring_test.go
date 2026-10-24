package brokerupgrade

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// The cut role is a live fact. Each gated step must ask the broker again:
// a sample taken at Prepare or at the first preflight is not authority for a
// later Freeze/Activate after an election, a new voter or a membership op.
// origin: hot-update S8 plan §6.3 / §8.2 F8 / §11.2 P15 (cached-role mutant)
func TestBrokerUpgradeCutRoleIsResampledAtEveryGatedStep(t *testing.T) {
	// Gated steps in engine order: Start preflight, drive preflight, Freeze,
	// post-export preflight, Activate. A completed cut asks exactly five times.
	for failAt := 0; failAt <= 5; failAt++ {
		t.Run(fmt.Sprintf("role_lost_at_step_%d", failAt), func(t *testing.T) {
			f := newEngineFixture()
			samples := 0
			role := func() error {
				samples++
				if samples == failAt {
					return ErrLegacyReloadLeader
				}
				return nil
			}
			f.endpoint = New(f.store, GateRuntime(f.runtime, role), f.registry)
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			switch failAt {
			case 0:
				if err != nil || active(v.Historical).Phase != upgradetx.PhaseCompleted || samples != 5 {
					t.Fatalf("healthy role: phase=%s samples=%d err=%v", active(v.Historical).Phase, samples, err)
				}
				return
			case 1:
				// Before acceptance the refusal is retryable and nothing is journaled.
				var refused *upgradetx.Error
				if !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred || refused.Accepted || f.store.record.Grant.Handle.TxID != "" {
					t.Fatalf("pre-accept role loss = %+v record=%+v", err, f.store.record.Grant.Handle)
				}
			default:
				requireAcceptedFailure(t, v, err)
			}
			if samples != failAt {
				t.Fatalf("role sampled %d times before refusing at step %d", samples, failAt)
			}
			// Every effect after the refused gate stayed unexecuted.
			wantFreeze, wantExport, wantActivate := 0, 0, 0
			if failAt > 3 {
				wantFreeze, wantExport = 1, 1
			}
			if f.runtime.freezeCalls != wantFreeze || f.runtime.exportCalls != wantExport || f.runtime.activateCalls != wantActivate {
				t.Fatalf("effects after lost role: freeze=%d export=%d activate=%d", f.runtime.freezeCalls, f.runtime.exportCalls, f.runtime.activateCalls)
			}
		})
	}
}

type deathPort struct {
	*runtimePort
	calls int
}

func (d *deathPort) ObserveDeath(upgradetx.Stamp) (upgradetx.DeathProof, error) {
	d.calls++
	return upgradetx.DeathProof{}, errors.New("not dead")
}

// Bind is the broker's only endpoint constructor: a foreign or incomplete
// binding is refused outright, and the gate keeps Archive's optional death
// observer reachable instead of turning every archive into a missing port.
// origin: hot-update S8 plan §2.2 / §8.1 / §11.2 P15
func TestBrokerUpgradeBindRefusesForeignOrIncompletePorts(t *testing.T) {
	f := newEngineFixture()
	key := f.store.grant.Handle.Key
	role := func() error { return nil }
	if endpoint, bound, err := Bind(nil, role); endpoint != nil || bound != (upgradetx.Key{}) || err != nil {
		t.Fatalf("absent binding must stay unwired: %v %v %v", endpoint, bound, err)
	}
	agentKey := upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: key.DomainID, SID: "lab", ConfiguredNID: "node"}
	for name, deps := range map[string]*Dependencies{
		"agent_key":    {Key: agentKey, Store: f.store, Runtime: f.runtime, Registry: f.registry},
		"invalid_key":  {Key: upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: key.DomainID, ConfigPath: "relative.yaml"}, Store: f.store, Runtime: f.runtime, Registry: f.registry},
		"no_store":     {Key: key, Runtime: f.runtime, Registry: f.registry},
		"no_runtime":   {Key: key, Store: f.store, Registry: f.registry},
		"no_registry":  {Key: key, Store: f.store, Runtime: f.runtime},
		"nil_role_ptr": nil,
	} {
		if deps == nil {
			if endpoint, _, err := Bind(&Dependencies{Key: key, Store: f.store, Runtime: f.runtime, Registry: f.registry}, nil); endpoint != nil || err == nil {
				t.Fatalf("%s: a missing role sampler bound an endpoint", name)
			}
			continue
		}
		if endpoint, _, err := Bind(deps, role); endpoint != nil || err == nil {
			t.Fatalf("%s: bound %v, %v", name, endpoint, err)
		}
	}
	endpoint, bound, err := Bind(&Dependencies{Key: key, Store: f.store, Runtime: f.runtime, Registry: f.registry}, role)
	if err != nil || endpoint == nil || bound != key {
		t.Fatalf("valid binding: %v %v %v", endpoint, bound, err)
	}
	// The bound endpoint is always the role-gated one: a broker that has lost
	// its cut role refuses before the journal accepts anything.
	gatedEndpoint, _, err := Bind(&Dependencies{Key: key, Store: f.store, Runtime: f.runtime, Registry: f.registry}, func() error { return ErrLegacyReloadLeader })
	if err != nil {
		t.Fatal(err)
	}
	var refused *upgradetx.Error
	if _, err := gatedEndpoint.Start(t.Context(), f.artifact.Grant, f.artifact); !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred || f.store.record.Grant.Handle.TxID != "" {
		t.Fatalf("Bind served an ungated runtime: %v", err)
	}
	if _, ok := GateRuntime(f.runtime, role).(deathObserver); ok {
		t.Fatal("the gate invented a death observer the runtime does not have")
	}
	death := &deathPort{runtimePort: f.runtime}
	gated, ok := GateRuntime(death, role).(deathObserver)
	if !ok {
		t.Fatal("the gate hid the runtime's independent death observer")
	}
	if _, err := gated.ObserveDeath(upgradetx.Stamp{}); err == nil || death.calls != 1 {
		t.Fatal("death observation was not delegated to the runtime")
	}
}

// FreshRole has no cached state: a nil Node (single mode) is always eligible,
// and the refusal for a multi-voter leader names the transfer remedy.
// origin: hot-update S8 plan §6.3 / §11.2 P15
func TestBrokerUpgradeRoleRefusalNamesTheRemedy(t *testing.T) {
	if err := FreshRole(nil); err != nil {
		t.Fatalf("single broker has no cluster role to refuse: %v", err)
	}
	g := gatedRuntime{RuntimePort: newEngineFixture().runtime, role: func() error { return ErrLegacyReloadLeader }}
	var refused *upgradetx.Error
	if err := g.admit(); !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred || !strings.Contains(refused.Detail, "transfer leadership") {
		t.Fatalf("leader refusal = %v", err)
	}
	g.role = func() error { return errMembershipInFlight }
	if err := g.admit(); !errors.As(err, &refused) || refused.Code != upgradetx.CodeDeferred || strings.Contains(refused.Detail, "membership operation is in flight") {
		t.Fatalf("membership refusal must stay a closed detail: %v", err)
	}
}
