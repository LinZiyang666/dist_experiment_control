package upgradetx

import "testing"

// origin: S8 internal review rv-txn F10
func TestCanonicalIdleCommitmentUsesExplicitEmptySet(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	a.Scope.Expected = nil
	canonical, err := CanonicalScope(a.Scope)
	if err != nil || canonical.Expected == nil || len(canonical.Expected) != 0 {
		t.Fatal("idle commitment is not an explicit empty set")
	}
	digest, err := ScopeDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	a.Scope.Expected = []Resource{}
	other, err := ScopeDigest(a.Scope)
	if err != nil || digest != other {
		t.Fatal("nil and empty scope commitments differ")
	}
}

// origin: S8 internal review rv-txn F09 F10
func TestSharedEvidenceRejectsForeignCommitmentAndExcludedCustody(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	for _, defect := range []string{"scope_digest", "expected_digest", "excluded_custody"} {
		t.Run(defect, func(t *testing.T) {
			c := cloneReceipt(a.Resources).Census
			scope := a.Scope
			switch defect {
			case "scope_digest":
				c.Stamp.ScopeDigest = testSHA("0")
			case "expected_digest":
				c.ExpectedDigest = testSHA("0")
			case "excluded_custody":
				na := Resource{Domain: DomainSS, ID: "unsupported", Origin: "capability", State: "not_applicable", Custody: "not_applicable"}
				scope.Expected = append(scope.Expected, na)
				c.Resources = append(c.Resources, na, Resource{Domain: DomainSS, ID: "unexpected", Origin: "late", State: "active", Custody: "target", Owner: targetOwner(), Generation: a.Handle.Generation})
				c.Stamp.ScopeDigest, _ = ScopeDigest(scope)
				c.ExpectedDigest, _ = ExpectedDigest(scope)
			}
			attempt := a
			attempt.Scope = scope
			if ValidateCensus(attempt, c, true) == nil {
				t.Fatal("foreign commitment or excluded domain acquired custody")
			}
		})
	}
}

// origin: S8 internal review rv-fault F5
func TestCurrentExportScopeRetainsOriginalAuthority(t *testing.T) {
	a, _ := completionFixture(DirectionForward)
	original := a.Scope
	for _, defect := range []string{"none", "namespace", "participant", "domains", "missing", "origin"} {
		t.Run(defect, func(t *testing.T) {
			exported, err := CanonicalScope(original)
			if err != nil {
				t.Fatal(err)
			}
			late := exported.Expected[0]
			late.ID, late.Origin = "new-exec", "after-prepare"
			exported.Expected = append(exported.Expected, late)
			switch defect {
			case "namespace":
				exported.ID = testID(900)
			case "participant":
				exported.Participants[0].ConfiguredNID = "foreign"
			case "domains":
				exported.Domains = exported.Domains[:len(exported.Domains)-1]
			case "missing":
				exported.Expected = exported.Expected[1:]
			case "origin":
				exported.Expected[0].Origin = "recreated"
			}
			err = ValidateExportScope(original, exported)
			if defect == "none" && err != nil {
				t.Fatal(err)
			}
			if defect != "none" && err == nil {
				t.Fatal("current export changed original authority or identity")
			}
		})
	}
}
