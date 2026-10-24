package agentupgradeport

import (
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type pendingRetirementImage struct {
	Image
	bundle agentimage.Bundle
}

func (i pendingRetirementImage) Bundle() agentimage.Bundle { return i.bundle }

// origin: S8 internal review activation-and-route-migration F10
func TestPendingReverseRetiresItsOriginalForwardCommitment(t *testing.T) {
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	forward := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 9}
	source, target, incarnation := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("7", 64)
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: strings.Repeat("3", 32), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), registryDomains[:]...),
		Expected: []upgradetx.Resource{{Domain: upgradetx.DomainAuthority, ID: incarnation, Origin: "session:lab", Owner: owner, Generation: 1, State: "active", Custody: "source"}}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	reverse := forward
	reverse.CutID, reverse.Generation, reverse.Revision = strings.Repeat("4", 32), 3, 12
	view := &upgradetx.View{Historical: upgradetx.Record{Key: key, Revision: reverse.Revision, Grant: upgradetx.Grant{Handle: forward, ScopeDigest: digest},
		Forward: upgradetx.Attempt{Handle: forward, SourceSHA: source, TargetSHA: target, Scope: scope},
		Reverse: upgradetx.Attempt{Handle: reverse, Direction: upgradetx.DirectionReverse, SourceSHA: target, TargetSHA: source, SourceGeneration: 2, Scope: scope}}}
	p := NewRuntime(Host{Key: key, Journal: recordJournal{view: view}, Inherited: &upgradecut.State{Attempt: forward, Owner: owner, SourceSHA: source, TargetSHA: target},
		Owner: func() (upgradetx.Owner, error) { return owner, nil }, RunningSHA: func() (string, error) { return target, nil },
		Incarnation: func() string { return incarnation }, Transfers: noTransfers,
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			t.Fatal("retirement sampled thawed owners instead of the held export")
			return execsession.Image{}, agentimage.CourierState{}, nil
		},
	}).(*runtimePort)
	p.held = &heldCut{handle: reverse, sequence: 11, image: pendingRetirementImage{bundle: agentimage.Bundle{}}}
	receipt, err := p.Retire(forward)
	if err != nil {
		t.Fatalf("accepted same-transaction reverse lost its forward retirement: %v", err)
	}
	if !receipt.Known || !receipt.Complete || receipt.Kind != upgradetx.ReceiptOldGenerationReleased || receipt.Stamp != receipt.Census.Stamp ||
		receipt.Stamp.Attempt != forward || receipt.Stamp.ScopeDigest != digest || receipt.RunningSHA != target || !upgradetx.CensusCovers(scope, receipt.Census) {
		t.Fatal("reverse borrowed another cut's release receipt")
	}
	for _, mutate := range []func(){
		func() { p.held = nil },
		func() { p.held.activating = true },
		func() { p.held.handle = forward },
		func() { view.Historical.Reverse.SourceSHA = source },
		func() { view.Historical.Forward.Handle.CutID = reverse.CutID },
		func() {
			later := *view
			later.Historical.Reverse.Handle.CutID = strings.Repeat("5", 32)
			p.host.Journal = retirementJournal{records: map[string]upgradetx.View{forward.TxID: *view, "": later}}
		},
		func() { p.host.RunningSHA = func() (string, error) { return source, nil } },
		func() {
			p.host.Owner = func() (upgradetx.Owner, error) {
				replacement := owner
				replacement.StartTicks++
				return replacement, nil
			}
		},
	} {
		goodHeld, goodView, goodSHA, goodOwner, goodJournal := *p.held, *view, p.host.RunningSHA, p.host.Owner, p.host.Journal
		mutate()
		if _, err := p.Retire(forward); err == nil {
			t.Fatal("unheld, stale or unrelated cut certified source retirement")
		}
		p.held, *view, p.host.RunningSHA, p.host.Owner = &goodHeld, goodView, goodSHA, goodOwner
		p.host.Journal = goodJournal
	}
}
