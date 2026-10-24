package rollcontrol

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func evidenceID(i int) string { return fmt.Sprintf("%032x", i) }
func evidenceOwner(i int) upgradetx.Owner {
	return upgradetx.Owner{BootID: "fixture-boot", PID: 100 + i, StartTicks: uint64(1000 + i), InstanceID: evidenceID(100 + i)}
}
func signSample(t *testing.T, seed []byte, s *Sample) {
	t.Helper()
	s.Signature = ""
	b, _ := json.Marshal(s)
	sig, err := auth.SignWithSeed(seed, b)
	if err != nil {
		t.Fatal(err)
	}
	s.Signature = hex.EncodeToString(sig)
}
func evidenceFixture(t *testing.T) ([]byte, upgradetx.Plan, Request) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	parentKey := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: "cluster", ConfiguredNID: "root@roll", ConfigPath: "/fixture/root.yaml"}
	p := upgradetx.Plan{Parent: upgradetx.Handle{Key: parentKey, TxID: evidenceID(1), CutID: evidenceID(2), Generation: 2, Revision: 1}, Scope: upgradetx.Scope{ID: evidenceID(3), Domains: domains}, SourceManifestDigest: strings.Repeat("a", 64), TargetManifestDigest: strings.Repeat("b", 64), Deadline: time.Now().Add(time.Minute).UnixNano()}
	r := Request{Op: "roll.observe", Nonce: evidenceID(42)}
	for index := 0; index < 2; index++ {
		key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: fmt.Sprint("host-", index), ConfigPath: fmt.Sprintf("/fixture/%d.yaml", index)}
		scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: evidenceID(20 + index), Participants: []upgradetx.Key{key}, Domains: domains, Expected: []upgradetx.Resource{{Domain: upgradetx.DomainBrokerRuntime, ID: "tunnel:server", Origin: "listener:server", Owner: evidenceOwner(index), Generation: 1, State: "active", Custody: "source"}}})
		if err != nil {
			t.Fatal(err)
		}
		g := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: evidenceID(100 + index*10), CutID: evidenceID(200 + index*10), Generation: 2, Revision: 1}, ParentTxID: p.Parent.TxID, ParentSlot: fmt.Sprintf("node-%d:", index) + strings.Repeat("c", 64), ClientNonce: evidenceID(300 + index*10), Issuer: evidenceOwner(index), ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64), ScopeDigest: upgraderoll.ScopeDigest(scope), CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: p.Deadline}
		h := g.Handle
		h.Revision = 5
		a := upgradetx.Attempt{Handle: h, Direction: upgradetx.DirectionForward, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, SourceGeneration: 1, Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess, Scope: scope}
		expected, _ := upgradetx.ExpectedDigest(scope)
		row := scope.Expected[0]
		row.Owner, row.Generation, row.Custody = evidenceOwner(index+10), 2, "target"
		stamp := upgradetx.Stamp{Attempt: h, Producer: row.Owner, Sequence: 7, At: time.Now().Add(-time.Millisecond).UnixNano(), ScopeDigest: g.ScopeDigest}
		kinds := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}
		for i, receipt := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
			stamp.Nonce = evidenceID(70 + i)
			stamp.At++
			census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: []upgradetx.Resource{row}, ExpectedDigest: expected}
			*receipt = upgradetx.Receipt{Kind: kinds[i], Stamp: stamp, Known: true, Complete: true, RunningSHA: g.TargetSHA, Census: census, EvidenceDigest: strings.Repeat("f", 64)}
		}
		stamp.Nonce, stamp.At = r.Nonce, time.Now().UnixNano()
		o := upgradetx.Observation{Nonce: r.Nonce, At: stamp.At, Current: a, Census: upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: []upgradetx.Resource{row}, ExpectedDigest: expected}, OwnerKnown: true, RunningSHA: g.TargetSHA, EffectiveSHA: g.TargetSHA}
		if err := upgradetx.Completed(a, o); err != nil {
			t.Fatal("invalid fixture completion:", err)
		}
		sample := Sample{ParentTxID: p.Parent.TxID, Nonce: r.Nonce, At: time.Now().UnixNano(), View: upgradetx.View{Historical: upgradetx.Record{Version: 2, Key: key, Revision: 5, Grant: g, Forward: a, EffectiveSHA: g.TargetSHA, RecordedPreviousSHA: g.SourceSHA}, Observation: o}, Release: g.TargetRelease, Holder: true, Released: true}
		signSample(t, seed, &sample)
		r.Samples = append(r.Samples, sample)
		p.Scope.Participants = append(p.Scope.Participants, key)
		p.Scope.Expected = append(p.Scope.Expected, upgraderoll.ParentResource(key, scope.Expected[0]))
		var deps []uint32
		if index > 0 {
			deps = []uint32{uint32(len(p.Steps))}
		}
		for i, kind := range []upgradetx.StepKind{upgradetx.StepBrokerCut, upgradetx.StepHomeConverge, upgradetx.StepRelease} {
			child := g
			child.Handle.TxID = evidenceID(100 + index*10 + i)
			child.Handle.CutID = evidenceID(200 + index*10 + i)
			child.ClientNonce = evidenceID(300 + index*10 + i)
			ordinal := uint32(len(p.Steps) + 1)
			p.Steps = append(p.Steps, upgradetx.Step{Ordinal: ordinal, Participant: key, Grant: child, Kind: kind, DependsOn: deps})
			deps = []uint32{ordinal}
		}
	}
	p.Scope, err = upgradetx.CanonicalScope(p.Scope)
	if err != nil {
		t.Fatal(err)
	}
	g := p.Steps[0].Grant
	g.Handle.Key = parentKey
	g.Handle.TxID = evidenceID(999)
	g.Handle.CutID = evidenceID(998)
	g.ClientNonce = evidenceID(997)
	g.ParentSlot = "root-slot"
	g.ScopeDigest = upgraderoll.ScopeDigest(p.Scope)
	p.Steps = append(p.Steps, upgradetx.Step{Ordinal: 7, Participant: parentKey, Grant: g, Kind: upgradetx.StepRelease, DependsOn: []uint32{3, 6}})
	if err := upgraderoll.ValidatePlan(p); err != nil {
		t.Fatal(err)
	}
	return seed, p, r
}

// origin: S8 internal review cluster-roll F6 F7
func TestParentCollectorRequiresEveryOriginalSignedCompletion(t *testing.T) {
	// Empty resources still have a real source generation. Equal-byte grants
	// do not activate their proposed next generation just by being sampled.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
	t.Run("observation_without_resources", func(t *testing.T) {
		seed, p, r := evidenceFixture(t)
		sample := &r.Samples[0]
		scope := sample.View.Historical.Forward.Scope
		scope.Expected = []upgradetx.Resource{}
		grant := p.Steps[0].Grant
		grant.SourceSHA, grant.SourceRelease = grant.TargetSHA, grant.TargetRelease
		grant.ScopeDigest = upgraderoll.ScopeDigest(scope)
		a := upgradetx.Attempt{Handle: grant.Handle, Direction: upgradetx.DirectionForward, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, SourceGeneration: 1, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: scope}
		at := time.Now().UnixNano()
		stamp := upgradetx.Stamp{Attempt: grant.Handle, Producer: grant.Issuer, Nonce: r.Nonce, Sequence: 1, At: at, ScopeDigest: grant.ScopeDigest}
		expected, _ := upgradetx.ExpectedDigest(scope)
		census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: []upgradetx.Resource{}, ExpectedDigest: expected}
		sample.View = upgradetx.View{Historical: upgradetx.Record{Version: 2, Key: grant.Handle.Key, Revision: grant.Handle.Revision, Grant: grant, Forward: a}, Observation: upgradetx.Observation{Nonce: r.Nonce, At: at, Current: a, Census: census, OwnerKnown: true, RunningSHA: grant.TargetSHA, EffectiveSHA: grant.TargetSHA}}
		sample.Release, sample.At = grant.TargetRelease, at
		p.Steps[0].Grant = grant
		p.Scope.Expected = p.Scope.Expected[1:]
		signSample(t, seed, sample)
		_, members, err := collect(seed, p, r, true)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, member := range members {
			if member.Participant == grant.Handle.Key {
				found = true
				if member.Generation != a.SourceGeneration {
					t.Fatalf("observation-only participant invented generation %d, want %d", member.Generation, a.SourceGeneration)
				}
			}
		}
		if !found {
			t.Fatal("empty source participant disappeared")
		}
	})

	for _, defect := range []string{"none", "signature", "nonce", "stale", "missing", "extra", "grant", "receipt", "host_release", "owner", "census_nonce", "release"} {
		t.Run(defect, func(t *testing.T) {
			seed, p, r := evidenceFixture(t)
			s := &r.Samples[0]
			resign := true
			switch defect {
			case "signature":
				s.Holder = !s.Holder
				resign = false
			case "nonce":
				s.Nonce = evidenceID(800)
			case "stale":
				s.At = time.Now().Add(-time.Minute).UnixNano()
			case "missing":
				r.Samples = r.Samples[:1]
				resign = false
			case "extra":
				r.Samples = append(r.Samples, r.Samples[0])
				resign = false
			case "grant":
				s.View.Historical.Grant.TargetSHA = strings.Repeat("3", 64)
			case "receipt":
				s.View.Historical.Forward.Released.Known = false
			case "host_release":
				s.Released = false
			case "owner":
				s.View.Observation.OwnerKnown = false
			case "census_nonce":
				s.View.Observation.Census.Stamp.Nonce = evidenceID(800)
			case "release":
				s.Release = "unrelated-release"
			}
			if resign {
				signSample(t, seed, s)
			}
			rows, members, err := collect(seed, p, r, true)
			if defect == "none" {
				if err != nil || len(rows) != 2 || len(members) != 2 || rows[0].ID == rows[1].ID {
					t.Fatalf("valid distributed custody was lost: rows=%+v members=%+v err=%v", rows, members, err)
				}
			} else if err == nil {
				t.Fatalf("%s was certified complete", defect)
			}
		})
	}
}
