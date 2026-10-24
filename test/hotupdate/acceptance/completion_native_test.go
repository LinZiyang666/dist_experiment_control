// Compare the actual S8 and S10 completion predicates using unchanged native evidence.
package acceptance

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	tx "github.com/LinZiyang666/tether/internal/upgradetx"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: S10 internal review rv-harness F1
func TestCompletionAcceptsNativeFreshQuery(t *testing.T) {
	id := func(n int) string { return fmt.Sprintf("%032x", n) }
	key := tx.Key{DomainID: "install", Component: tx.ComponentAgent, SID: "sid", ConfiguredNID: "nid", UID: 1000}
	owner := tx.Owner{BootID: "boot", PID: 100, StartTicks: 100, InstanceID: id(4)}
	a := tx.Attempt{
		Handle:    tx.Handle{Key: key, TxID: id(1), CutID: id(2), Generation: 2, Revision: 20},
		Direction: tx.DirectionForward, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), SourceGeneration: 1,
		Phase: tx.PhaseCompleted, CutStage: tx.CutReleased, Outcome: tx.OutcomeSuccess,
		Scope: tx.Scope{ID: id(3), Participants: []tx.Key{key}, Domains: []tx.Domain{
			tx.DomainAuthority, tx.DomainBrokerRuntime, tx.DomainBrokerStore, tx.DomainControl, tx.DomainExec,
			tx.DomainFlow, tx.DomainPTY, tx.DomainSS, tx.DomainTransferTracker, tx.DomainTransferWorker, tx.DomainUpgrade},
			Expected: []tx.Resource{{Domain: tx.DomainExec, ID: "job", Origin: "nid", Owner: owner, Generation: 1, State: "running", Custody: "owned"}}},
	}
	// The real S8 predicate binds both commitments to the canonical scope;
	// placeholder hex strings no longer describe valid upstream evidence.
	// origin: S10 internal review MAKETEST-GATES F1
	scopeDigest, err := tx.ScopeDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := tx.ExpectedDigest(a.Scope)
	if err != nil {
		t.Fatal(err)
	}
	for i, receipt := range []*tx.Receipt{&a.Control, &a.Resources, &a.Released} {
		stamp := tx.Stamp{Attempt: a.Handle, Producer: owner, Nonce: id(110 + i), Sequence: uint64(40 + i), At: int64(100 + i), ScopeDigest: scopeDigest}
		resources := append([]tx.Resource(nil), a.Scope.Expected...)
		resources[0].Generation = 2
		*receipt = tx.Receipt{Kind: []tx.ReceiptKind{tx.ReceiptControlReady, tx.ReceiptResourcesMigrated, tx.ReceiptOldGenerationReleased}[i],
			Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: strings.Repeat("c", 64),
			Census: tx.Census{Stamp: stamp, Known: true, Complete: true, Resources: resources, ExpectedDigest: expectedDigest}}
	}
	census := a.Released.Census
	census.Stamp.Nonce, census.Stamp.Sequence, census.Stamp.At = id(201), 43, 200
	observation := tx.Observation{Nonce: id(201), At: 200, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}
	if err := tx.Completed(a, observation); err != nil {
		t.Fatalf("invalid native positive control: %v", err)
	}
	native, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ValidateJSON("s8_observation", native); err != nil {
		t.Fatal(err)
	}
	var projection transactionObservation
	if err := json.Unmarshal(native, &projection); err != nil {
		t.Fatal(err)
	}
	var target c.Digest
	decoded, _ := hex.DecodeString(a.TargetSHA)
	copy(target[:], decoded)
	projectStamp := func(s tx.Stamp) c.Stamp {
		return c.Stamp{Process: c.Process{Key: c.Key{Role: "agent", Host: "host", Unit: "agent.service", SID: key.SID, NID: key.ConfiguredNID, Domain: key.DomainID, UID: key.UID},
			BootID: owner.BootID, PIDNS: "pidns", InstanceID: owner.InstanceID, PID: owner.PID, StartTicks: owner.StartTicks},
			TxID: s.Attempt.TxID, CutID: s.Attempt.CutID, Direction: "forward", QueryNonce: s.Nonce, ScopeDigest: s.ScopeDigest,
			Generation: s.Attempt.Generation, Revision: s.Attempt.Revision, Sequence: s.Sequence, AtBootNS: s.At}
	}
	v := c.View{Stamp: projectStamp(census.Stamp), Current: native}
	raw := fixtureRaw("native", "s8_receipt")
	for i, receipt := range []*c.Receipt{&v.Control, &v.Resources, &v.Released} {
		original := []tx.Receipt{a.Control, a.Resources, a.Released}[i]
		*receipt = c.Receipt{Kind: string(original.Kind), Stamp: projectStamp(original.Stamp), Known: true, Complete: true, RunningSHA: target, ResourceIDs: []string{"job"}, Evidence: raw}
	}
	e := c.Evidence{Samples: []c.Sample{{Stamp: v.Stamp, ExeSHA: target, Complete: true, Raw: raw}}}
	result := c.Result{}
	judgeCompletion(&result, e, v, projection, target, map[c.RawRef]bool{raw: true})
	if len(result.Failures)+len(result.Missing) != 0 {
		t.Fatalf("S8 Completed accepted the raw observation but S10 rejected it: failures=%v missing=%v", result.Failures, result.Missing)
	}
}
