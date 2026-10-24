package acceptance

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: S10 internal review rv-honesty F2
func TestCompletionRejectsOldGenerationCustody(t *testing.T) {
	e := rolloutEvidence(t)
	if r, err := Judge(e); err != nil || r.RC != 0 {
		t.Fatalf("baseline: %s %v", r.Verdict, err)
	}
	for i := range e.Views {
		if e.Views[i].Stamp.Generation < 2 {
			continue
		}
		editCurrent(t, &e.Views[i], func(o *transactionObservation) {
			for _, census := range []*transactionCensus{&o.Census, &o.Current.Resources.Census, &o.Current.Released.Census} {
				census.Resources[0].Generation = o.Current.Handle.Generation - 1
				census.Resources[0].Owner.PID += 999
				census.Resources[0].State = "live"
				census.Resources[0].Custody = "owned"
			}
		})
		break
	}
	r, err := Judge(e)
	if err != nil {
		t.Fatal(err)
	}
	if r.RC == 0 {
		t.Fatalf("old-generation live resource accepted: verdict=%s rc=%d", r.Verdict, r.RC)
	}
}

// origin: S10 internal review rv-honesty F3
func TestCompletionAcceptsUpstreamFreshObservation(t *testing.T) {
	// Construct a valid upstream completion with three historical seal nonces
	// and one distinct current-query nonce, then feed its unchanged raw DTO to P01.
	k := upgradetx.Key{DomainID: "domain", Component: upgradetx.ComponentAgent, SID: "sid", ConfiguredNID: "node", UID: 1000}
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 100, InstanceID: "instance"}
	a := upgradetx.Attempt{
		Handle:    upgradetx.Handle{Key: k, TxID: fmt.Sprintf("%032x", 1), CutID: fmt.Sprintf("%032x", 2), Generation: 2, Revision: 20},
		Direction: upgradetx.DirectionForward, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), SourceGeneration: 1,
		Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess,
		Scope: upgradetx.Scope{ID: fmt.Sprintf("%032x", 3), Participants: []upgradetx.Key{k}, Domains: []upgradetx.Domain{
			upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl,
			upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS,
			upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade},
			Expected: []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "job", Origin: "node", Owner: owner, Generation: 1, State: "running", Custody: "owned"}}},
	}
	// The live S8 validator commits the original scope using its shared codec.
	// origin: S10 internal review MAKETEST-GATES F1
	scopeDigest, err := upgradetx.ScopeDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := upgradetx.ExpectedDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
		s := upgradetx.Stamp{Attempt: a.Handle, Producer: owner, Nonce: fmt.Sprintf("%032x", 110+i), Sequence: uint64(40 + i), At: int64(100 + i), ScopeDigest: scopeDigest}
		resource := a.Scope.Expected[0]
		resource.Generation = 2
		*r = upgradetx.Receipt{Kind: []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}[i], Stamp: s,
			Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: strings.Repeat("c", 64),
			Census: upgradetx.Census{Stamp: s, Known: true, Complete: true, Resources: []upgradetx.Resource{resource}, ExpectedDigest: expectedDigest}}
	}
	census := a.Released.Census
	census.Stamp.Nonce = fmt.Sprintf("%032x", 201)
	census.Stamp.At = 200
	census.Stamp.Sequence = 50
	o := upgradetx.Observation{Nonce: census.Stamp.Nonce, At: 200, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
	if err := upgradetx.Completed(a, o); err != nil {
		t.Fatalf("upstream baseline rejected: %v", err)
	}
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateJSON("s8_observation", raw); err != nil {
		t.Fatal(err)
	}
	var projected transactionObservation
	if err = json.Unmarshal(raw, &projected); err != nil {
		t.Fatal(err)
	}
	stamp := c.Stamp{Process: c.Process{Key: c.Key{Role: "agent", Host: "host", Unit: "unit", SID: k.SID, NID: k.ConfiguredNID, Domain: k.DomainID, UID: k.UID}, BootID: owner.BootID, PIDNS: "pidns", InstanceID: owner.InstanceID, PID: owner.PID, StartTicks: owner.StartTicks},
		TxID: a.Handle.TxID, CutID: a.Handle.CutID, Direction: "forward", QueryNonce: o.Nonce, ScopeDigest: census.Stamp.ScopeDigest, Generation: 2, Revision: 20, Sequence: 50, AtBootNS: 200}
	var target c.Digest
	for i := range target {
		target[i] = 0xbb
	}
	v := c.View{Stamp: stamp, Current: raw}
	refs := map[c.RawRef]bool{}
	for i, r := range []*c.Receipt{&v.Control, &v.Resources, &v.Released} {
		u := []upgradetx.Receipt{a.Control, a.Resources, a.Released}[i]
		s := stamp
		s.QueryNonce = u.Stamp.Nonce
		s.Sequence = u.Stamp.Sequence
		s.AtBootNS = u.Stamp.At
		*r = c.Receipt{Kind: string(u.Kind), Stamp: s, Known: true, Complete: true, RunningSHA: target, ResourceIDs: []string{"job"}, Evidence: fixtureRaw(fmt.Sprint(i), "s8_receipt")}
		refs[r.Evidence] = true
	}
	sample := c.Sample{Stamp: stamp, ExeSHA: target, Complete: true, Raw: fixtureRaw("fresh", "sample")}
	refs[sample.Raw] = true
	result := c.Result{}
	judgeCompletion(&result, c.Evidence{Samples: []c.Sample{sample}}, v, projected, target, refs)
	if len(result.Failures)+len(result.Missing) != 0 {
		t.Fatalf("valid upgradetx.Completed proof rejected: failures=%v missing=%v", result.Failures, result.Missing)
	}

	// Later journal writes do not invalidate an otherwise unchanged historical seal.
	a.Control.Stamp.Attempt.Revision--
	a.Control.Census.Stamp = a.Control.Stamp
	o.Current = a
	if err := upgradetx.Completed(a, o); err != nil {
		t.Fatalf("upstream older seal rejected: %v", err)
	}
	raw, err = json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateJSON("s8_observation", raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &projected); err != nil {
		t.Fatal(err)
	}
	v.Current = raw
	v.Control.Stamp.Revision--
	result = c.Result{}
	judgeCompletion(&result, c.Evidence{Samples: []c.Sample{sample}}, v, projected, target, refs)
	if len(result.Failures)+len(result.Missing) != 0 {
		t.Fatalf("valid older S8 seal rejected: %+v", result)
	}
}

// Resources, release and the fresh query each carry their own responsibility
// proof; a sound final query cannot repair a false historical migration seal.
// origin: S10 internal review rv-honesty F2
func TestCompletionRejectsUnsettledCustody(t *testing.T) {
	e := rolloutEvidence(t)
	v := e.Views[7]
	raw := make(map[c.RawRef]bool)
	for _, ref := range e.Raw {
		raw[ref] = true
	}
	for _, which := range []string{"resources", "released", "fresh"} {
		for _, defect := range []string{"old generation", "same SHA two owners", "terminal execution rights", "admitting", "undeclared not applicable", "missing owner", "old owner responsibility"} {
			t.Run(which+"/"+defect, func(t *testing.T) {
				v := v
				var o transactionObservation
				if err := json.Unmarshal(v.Current, &o); err != nil {
					t.Fatal(err)
				}
				census := &o.Census
				switch which {
				case "resources":
					census = &o.Current.Resources.Census
				case "released":
					census = &o.Current.Released.Census
				}
				resource := &census.Resources[0]
				switch defect {
				case "old generation":
					resource.Generation--
				case "same SHA two owners":
					other := *resource
					other.Owner.PID++
					census.Resources = append(census.Resources, other)
					if which == "resources" {
						v.Resources.ResourceIDs = append(append([]string{}, v.Resources.ResourceIDs...), other.ID)
					}
					if which == "released" {
						v.Released.ResourceIDs = append(append([]string{}, v.Released.ResourceIDs...), other.ID)
					}
				case "terminal execution rights":
					resource.Generation--
					resource.State, resource.Custody = "terminal", "retained"
				case "admitting":
					census.Admitting = 1
				case "undeclared not applicable":
					resource.State, resource.Custody, resource.Generation, resource.Owner = "not_applicable", "not_applicable", 0, transactionOwner{}
				case "missing owner":
					resource.Owner = transactionOwner{}
				case "old owner responsibility":
					census.OldOwners = []transactionOwner{resource.Owner}
				}
				result := c.Result{}
				judgeCompletion(&result, e, v, o, v.Control.RunningSHA, raw)
				if len(result.Failures) == 0 {
					t.Fatalf("unsettled custody accepted: %+v", result)
				}
			})
		}
	}
	for _, disposition := range []string{"terminal released", "scoped not applicable", "late admission"} {
		t.Run(disposition, func(t *testing.T) {
			var o transactionObservation
			if err := json.Unmarshal(v.Current, &o); err != nil {
				t.Fatal(err)
			}
			view := v
			switch disposition {
			case "terminal released":
				for _, census := range []*transactionCensus{&o.Current.Resources.Census, &o.Current.Released.Census, &o.Census} {
					census.Resources[0].Generation--
					census.Resources[0].State, census.Resources[0].Custody = "terminal", "released"
				}
			case "scoped not applicable":
				for _, resources := range [][]transactionResource{o.Current.Scope.Expected, o.Current.Control.Census.Resources, o.Current.Resources.Census.Resources, o.Current.Released.Census.Resources, o.Census.Resources} {
					resources[0].State, resources[0].Custody, resources[0].Generation, resources[0].Owner = "not_applicable", "not_applicable", 0, transactionOwner{}
				}
			case "late admission":
				late := o.Census.Resources[0]
				late.ID = "late-resource"
				for _, census := range []*transactionCensus{&o.Current.Resources.Census, &o.Current.Released.Census, &o.Census} {
					census.Resources = append(census.Resources, late)
				}
				view.Resources.ResourceIDs = append(append([]string{}, view.Resources.ResourceIDs...), late.ID)
				view.Released.ResourceIDs = append(append([]string{}, view.Released.ResourceIDs...), late.ID)
			}
			result := c.Result{}
			judgeCompletion(&result, e, view, o, v.Control.RunningSHA, raw)
			if len(result.Failures)+len(result.Missing) != 0 {
				t.Fatalf("valid custody rejected: %+v", result)
			}
		})
	}
}

// Historical revisions can lag journal writes, but the current census must
// answer this query. A saved seal cannot become fresh merely by relabeling it.
// origin: S10 internal review rv-honesty F3
func TestCompletionPreservesHistoricalBarriers(t *testing.T) {
	e := rolloutEvidence(t)
	baseline := e.Views[7]
	raw := make(map[c.RawRef]bool)
	for _, ref := range e.Raw {
		raw[ref] = true
	}
	for _, defect := range []string{"older revision", "future revision", "historical nonce replay", "future seal time", "release before control time", "lost late identity", "future seal sequence"} {
		t.Run(defect, func(t *testing.T) {
			v := baseline
			var o transactionObservation
			if err := json.Unmarshal(v.Current, &o); err != nil {
				t.Fatal(err)
			}
			switch defect {
			case "older revision":
				v.Control.Stamp.Revision--
				o.Current.Control.Stamp.Attempt.Revision--
				o.Current.Control.Census.Stamp = o.Current.Control.Stamp
			case "future revision":
				v.Control.Stamp.Revision++
				o.Current.Control.Stamp.Attempt.Revision++
				o.Current.Control.Census.Stamp = o.Current.Control.Stamp
			case "historical nonce replay":
				// Keep the historical receipt untouched; replay it as the current query.
				v.Stamp.QueryNonce = v.Control.Stamp.QueryNonce
				o.Nonce, o.Census.Stamp.Nonce = v.Stamp.QueryNonce, v.Stamp.QueryNonce
			case "future seal time":
				v.Control.Stamp.AtBootNS++
				o.Current.Control.Stamp.At++
				o.Current.Control.Census.Stamp = o.Current.Control.Stamp
			case "release before control time":
				v.Released.Stamp.AtBootNS--
				o.Current.Released.Stamp.At--
				o.Current.Released.Census.Stamp = o.Current.Released.Stamp
			case "lost late identity":
				late := o.Current.Released.Census.Resources[0]
				late.ID = "late-resource"
				o.Current.Released.Census.Resources = append(o.Current.Released.Census.Resources, late)
				v.Released.ResourceIDs = append(append([]string{}, v.Released.ResourceIDs...), late.ID)
			case "future seal sequence":
				v.Control.Stamp.Sequence = v.Stamp.Sequence + 1
				o.Current.Control.Stamp.Sequence = v.Control.Stamp.Sequence
				o.Current.Control.Census.Stamp = o.Current.Control.Stamp
			}
			// A matching independent sample isolates the historical barrier from the
			// separate executable-proof guard, including the nonce replay case.
			sample := e.Samples[7]
			sample.Stamp = v.Stamp
			result := c.Result{}
			judgeCompletion(&result, c.Evidence{Samples: []c.Sample{sample}}, v, o, v.Control.RunningSHA, raw)
			if defect == "older revision" {
				if len(result.Failures)+len(result.Missing) != 0 {
					t.Fatalf("historical revision rejected: %+v", result)
				}
			} else if len(result.Failures) == 0 {
				t.Fatalf("invalid historical barrier accepted: %+v", result)
			}
		})
	}
}

// origin: S10 internal review rv-harness F2
func TestCompletionRejectsForeignResourceCustody(t *testing.T) {
	e := rolloutEvidence(t)
	editCurrent(t, &e.Views[0], func(o *transactionObservation) {
		all := []*transactionCensus{&o.Census, &o.Current.Control.Census, &o.Current.Resources.Census, &o.Current.Released.Census}
		for _, census := range all {
			census.Resources[0].Owner.PID += 9999
			census.Resources[0].Generation += 99
			census.Resources[0].Custody = "none"
		}
	})
	result, err := Judge(e)
	if err != nil {
		t.Fatal(err)
	}
	if result.RC == 0 {
		t.Fatalf("foreign owner and missing custody accepted: verdict=%s rc=%d", result.Verdict, result.RC)
	}
}
