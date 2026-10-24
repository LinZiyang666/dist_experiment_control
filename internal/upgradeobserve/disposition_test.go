package upgradeobserve

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv-txn F09
func TestCollectorRejectsCustodyInExcludedDomain(t *testing.T) {
	f := fixture()
	o := freshObservation(t, f)
	na := upgradetx.Resource{Domain: upgradetx.DomainSS, ID: "unsupported", Origin: "capability", State: "not_applicable", Custody: "not_applicable"}
	f.attempt.Scope.Expected = append(f.attempt.Scope.Expected, na)
	var err error
	f.attempt.Scope, err = upgradetx.CanonicalScope(f.attempt.Scope)
	if err != nil {
		t.Fatal(err)
	}
	o.Current.Scope = f.attempt.Scope
	o.Census.Resources = append(o.Census.Resources, na)
	o.Census.Stamp.ScopeDigest, _ = upgradetx.ScopeDigest(f.attempt.Scope)
	o.Census.ExpectedDigest, _ = upgradetx.ExpectedDigest(f.attempt.Scope)
	if _, err = ResourcesProof(f.attempt, o, o.Nonce); err != nil {
		t.Fatal(err)
	}
	late := o.Census.Resources[0]
	late.Domain, late.ID, late.Origin = upgradetx.DomainSS, "late", "unexpected-create"
	o.Census.Resources = append(o.Census.Resources, late)
	if _, err = ResourcesProof(f.attempt, o, o.Nonce); err == nil {
		t.Fatal("collector certified custody in an excluded domain")
	}
}
