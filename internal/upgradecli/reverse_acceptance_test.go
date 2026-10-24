package upgradecli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type acceptedReverseEndpoint struct {
	*verbEndpoint
	announced upgradetx.Grant
	malformed bool
}

func (e *acceptedReverseEndpoint) Rollback(_ context.Context, f upgradetx.Fence, r upgradetx.ReverseRequest) (upgradetx.View, error) {
	g := e.view.Historical.Grant
	g.Handle.TxID, g.Handle.CutID, g.Handle.Generation = strings.Repeat("7", 32), strings.Repeat("8", 32), f.Current.Generation+1
	g.ClientNonce, g.ActorFP, g.SourceSHA, g.TargetSHA, g.Issuer = r.ClientNonce, r.ActorFP, f.RunningSHA, r.TargetSHA, f.Owner
	g.ScopeDigest, _ = upgradetx.ScopeDigest(r.Scope)
	if e.malformed {
		g.ActorFP = "foreign"
	}
	e.announced = g
	a := upgradetx.Attempt{Handle: g.Handle, Direction: upgradetx.DirectionReverse, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, SourceGeneration: f.Current.Generation, Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive, Outcome: upgradetx.OutcomePending, Scope: r.Scope}
	e.rollbackReply = upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g, Forward: a}}
	return e.rollbackReply, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true, Detail: "start accepted and activating; wait for or query the original transaction"}
}

// Accepted asynchronous reverse is queried by its issued ID; it is neither
// repeated nor mistaken for a completed reverse. Foreign replies stay refused.
// origin: hot-update S8 plan §7.1 / §8.1; S8 internal review upgrade-maketest F8
func TestRollbackWaitsForItsAnnouncedAcceptedReverse(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		v := reverseView()
		v.Historical.Forward.Scope = acceptedReverseScope(v.Historical.Key)
		v.Observation.Current.Scope = v.Historical.Forward.Scope
		e := &acceptedReverseEndpoint{verbEndpoint: &verbEndpoint{view: v}, malformed: malformed}
		r, _ := verbRequest(t, e.verbEndpoint, VerbRollback)
		r.Endpoint = e
		if err := RunVerb(t.Context(), r); ExitCode(err) != 74 {
			t.Fatalf("pending reverse falsely completed: %v", err)
		}
		got, err := RecallGrant(r.Home, e.announced.Handle.TxID)
		if malformed {
			if err == nil || len(e.queries) != 1 {
				t.Fatal("foreign reverse reply gained query authority")
			}
		} else if err != nil || got != e.announced || len(e.queries) < 2 || e.queries[len(e.queries)-1] != e.announced.Handle.TxID {
			t.Fatalf("accepted reverse identity lost before Wait: queries=%v err=%v", e.queries, err)
		}
	}
}

// A manual reverse seals the latest observed work, including tasks admitted
// by the target after the forward completed. Reusing the forward scope loses
// the registry/adjunct commitment for that new work during import.
// origin: hot-update S8 plan §8.1 R2; S8 internal review upgrade-maketest F9
func TestReverseRequestSealsNewlyAdmittedResources(t *testing.T) {
	v := reverseView()
	v.Observation.Census.Known, v.Observation.Census.Complete = true, true
	late := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "new-child", Origin: "request:new-child", Owner: v.Observation.Census.Stamp.Producer, Generation: v.Observation.Current.Handle.Generation, State: "running", Custody: "target"}
	v.Observation.Census.Resources = append(v.Observation.Census.Resources, late)
	_, r, err := ReverseFor(v, "actor", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range r.Scope.Expected {
		if row == late {
			found = true
		}
	}
	if !found {
		t.Fatal("reverse reused the forward snapshot and omitted the new child")
	}
	v.Observation.Census.Known = false
	if _, _, err := ReverseFor(v, "actor", time.Now()); err == nil {
		t.Fatal("unknown census authorized a reverse scope")
	}
	v.Observation.Census.Known, v.Observation.Census.Complete = true, false
	if _, _, err := ReverseFor(v, "actor", time.Now()); err == nil {
		t.Fatal("incomplete census authorized a reverse scope")
	}
}

func acceptedReverseScope(key upgradetx.Key) upgradetx.Scope {
	return upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{}}
}
