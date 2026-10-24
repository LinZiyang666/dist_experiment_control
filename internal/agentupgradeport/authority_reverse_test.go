package agentupgradeport_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

type censusJournal struct {
	contractstub.Store
	view upgradetx.View
}

func (s censusJournal) Load(upgradetx.Key, string) (upgradetx.View, error) { return s.view, nil }

// origin: S8 internal review remaining-orchestration F2 (R2)
func TestAgentFreshCensusAuthorizesTheOriginalReverse(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("2", 32), CutID: strings.Repeat("3", 32), Generation: 2, Revision: 4}
	source, target := strings.Repeat("a", 64), strings.Repeat("b", 64)
	incarnation := strings.Repeat("c", 64)
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{{Domain: upgradetx.DomainAuthority, ID: incarnation, Origin: "session:lab", Owner: owner, Generation: 1, State: "active", Custody: "source"}}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	attempt := upgradetx.Attempt{Handle: handle, Direction: upgradetx.DirectionForward, Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess, Scope: scope, SourceSHA: source, TargetSHA: target, SourceGeneration: 1}
	view := upgradetx.View{Historical: upgradetx.Record{Version: 2, Key: key, Revision: handle.Revision, Forward: attempt, Grant: upgradetx.Grant{Handle: handle}, RecordedPreviousSHA: source, EffectiveSHA: target}}
	port := agentupgradeport.NewRuntime(agentupgradeport.Host{Key: key, Journal: censusJournal{view: view},
		Owner: func() (upgradetx.Owner, error) { return owner, nil }, RunningSHA: func() (string, error) { return target, nil }, Incarnation: func() string { return incarnation },
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			return execsession.Image{}, agentimage.CourierState{}, nil
		}, Transfers: func() ([]agentupgradeport.TransferEntry, error) { return nil, nil },
	})
	view.Observation, err = port.Observe(upgradetx.Stamp{Attempt: handle, Nonce: strings.Repeat("6", 32), ScopeDigest: digest})
	if err != nil || !view.Observation.Census.Known || !view.Observation.Census.Complete {
		t.Fatalf("fresh census: %+v %v", view.Observation.Census, err)
	}
	fence, reverse, err := upgradecli.ReverseFor(view, "actor", time.Now())
	if err != nil || fence.Incarnation != incarnation || reverse.TargetSHA != source {
		t.Fatalf("fresh agent reverse is unusable: %+v %+v %v", fence, reverse, err)
	}
	if _, err := upgradewire.EncodeRequest(upgradewire.Request{Op: upgradewire.OpRollback, Key: key, Expected: fence, Reverse: reverse}); err != nil {
		t.Fatalf("incarnation fence cannot cross the strict wire: %v", err)
	}
}
