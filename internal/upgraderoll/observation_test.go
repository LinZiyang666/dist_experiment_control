package upgraderoll

import (
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"testing"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2 / IL14
func TestObservationOnlyGrantRequiresFreshUnchangedCustody(t *testing.T) {
	model, _ := newRollModel(t)
	g := model.plan.Steps[0].Grant
	g.TargetSHA = g.SourceSHA
	a := model.children[g.Handle.TxID]
	a.TargetSHA = g.TargetSHA
	o := fixtureObservation(a, testID(990))
	o.At = o.Census.Stamp.At
	v := upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g, Forward: a}, Observation: o}
	if err := ObserveUnchanged(v, g, o.Nonce); err != nil {
		t.Fatal(err)
	}
	// An independently owned retained helper remains part of the exact source
	// census; observing it is not a migration into the coordinator process.
	helper := v
	helperScope := a.Scope
	helperScope.Expected = append(append([]upgradetx.Resource(nil), a.Scope.Expected...), upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "z-helper", Origin: "fixture", Owner: testOwner(2), Generation: 1, State: "active", Custody: "owned"})
	helper.Historical.Forward.Scope = helperScope
	helper.Historical.Grant.ScopeDigest = ScopeDigest(helperScope)
	helper.Observation.Current = helper.Historical.Forward
	helper.Observation.Census.Resources = append([]upgradetx.Resource(nil), helperScope.Expected...)
	helper.Observation.Census.Stamp.ScopeDigest = helper.Historical.Grant.ScopeDigest
	helper.Observation.Census.ExpectedDigest = digest(helperScope.Expected)
	if err := ObserveUnchanged(helper, helper.Historical.Grant, o.Nonce); err != nil {
		t.Fatalf("known original helper was omitted: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*upgradetx.View)
	}{
		{"unknown owner", func(v *upgradetx.View) { v.Observation.OwnerKnown = false }},
		{"stale challenge", func(v *upgradetx.View) { v.Observation.Nonce = testID(991) }},
		{"old running bytes", func(v *upgradetx.View) { v.Observation.RunningSHA = testSHA(999) }},
		{"disk only", func(v *upgradetx.View) { v.Observation.EffectiveSHA = testSHA(999) }},
		{"missing resource", func(v *upgradetx.View) { v.Observation.Census.Resources = nil }},
		{"admitting", func(v *upgradetx.View) { v.Observation.Census.Admitting = 1 }},
		{"retained generation", func(v *upgradetx.View) { v.Observation.Census.OldOwners = []upgradetx.Owner{testOwner(2)} }},
		{"another live generation", func(v *upgradetx.View) { v.Observation.Census.Resources[0].Generation++ }},
		{"another producer", func(v *upgradetx.View) { v.Observation.Census.Stamp.Producer = testOwner(2) }},
		{"unresolved cut", func(v *upgradetx.View) { v.Historical.Forward.Phase = upgradetx.PhaseActivating }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := v
			bad.Observation.Census.Resources = append([]upgradetx.Resource(nil), o.Census.Resources...)
			tc.change(&bad)
			if err := ObserveUnchanged(bad, g, o.Nonce); err == nil {
				t.Fatal("unsafe observation-only participant was accepted")
			}
		})
	}
}
