package agentupgradeport

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type retirementJournal struct {
	upgradetx.Store
	records map[string]upgradetx.View
}

func (j retirementJournal) Load(_ upgradetx.Key, tx string) (upgradetx.View, error) {
	v, ok := j.records[tx]
	if !ok {
		return v, errors.New("unknown retirement transaction")
	}
	return v, nil
}

type retirementImage struct {
	Image
	bundle agentimage.Bundle
}

func (i retirementImage) Bundle() agentimage.Bundle { return i.bundle }

// A reverse has already frozen the current runtime when it retires the
// inherited forward generation. Re-snapshotting that runtime is unavailable;
// the held current export includes the work admitted after the forward cut.
// origin: hot-update S8 plan §8.1; S8 internal review upgrade-maketest F6
// origin: S8 internal review TAGGED-SUITE-SWEEP-A F4
func TestReverseRetirementUsesItsCurrentFrozenExport(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: strings.Repeat("d", 32), SID: "lab", ConfiguredNID: "node"}
	forward := upgradetx.Handle{Key: key, TxID: strings.Repeat("2", 32), CutID: strings.Repeat("3", 32), Generation: 2, Revision: 9}
	reverse := upgradetx.Handle{Key: key, TxID: strings.Repeat("4", 32), CutID: strings.Repeat("5", 32), Generation: 3, Revision: 3}
	source, target := strings.Repeat("a", 64), strings.Repeat("b", 64)
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), registryDomains[:]...), Expected: []upgradetx.Resource{}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	a := upgradetx.Attempt{Handle: forward, Direction: upgradetx.DirectionForward, SourceSHA: source, TargetSHA: target, SourceGeneration: 1, Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess, Scope: scope}
	b := upgradetx.Attempt{Handle: reverse, Direction: upgradetx.DirectionReverse, SourceSHA: target, TargetSHA: source, SourceGeneration: 2, Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine, Outcome: upgradetx.OutcomePending, Scope: scope}
	old := upgradetx.View{Historical: upgradetx.Record{Key: key, Grant: upgradetx.Grant{Handle: forward, ScopeDigest: digest}, Forward: a}}
	next := upgradetx.View{Historical: upgradetx.Record{Key: key, Grant: upgradetx.Grant{Handle: reverse, ScopeDigest: digest}, Forward: b}}
	snapshots := 0
	p := NewRuntime(Host{Key: key, Journal: retirementJournal{records: map[string]upgradetx.View{forward.TxID: old, reverse.TxID: next}}, Owner: func() (upgradetx.Owner, error) { return owner, nil }, RunningSHA: func() (string, error) { return target, nil }, Incarnation: func() string { return strings.Repeat("7", 64) }, Transfers: noTransfers,
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			snapshots++
			return execsession.Image{}, agentimage.CourierState{}, errors.New("current runtime is frozen")
		},
		Inherited: &upgradecut.State{Attempt: forward, Owner: owner, SourceSHA: source, TargetSHA: target, Scope: scope}}).(*runtimePort)
	bundle := agentimage.Bundle{Courier: agentimage.CourierState{Events: []agentimage.CourierEvent{{PID: "new-work", Kind: "exit", EnqueuedAtUnixNano: 1}}}}
	p.held = &heldCut{handle: reverse, image: retirementImage{bundle: bundle}, sequence: 1}
	receipt, err := p.Retire(forward)
	if err != nil || !receipt.Census.Known || !receipt.Census.Complete || snapshots != 0 {
		t.Fatalf("current frozen retirement: known=%t snapshots=%d err=%v", receipt.Census.Known, snapshots, err)
	}
	found := false
	for _, r := range receipt.Census.Resources {
		if r.ID == "courier:new-work/exit" {
			found = true
			if r.Owner != owner || r.Generation != forward.Generation {
				t.Fatal("retirement lost current custody")
			}
		}
	}
	if !found {
		t.Fatal("retirement omitted work admitted after the forward cut")
	}
	// A pending forward and its reverse share one durable Record. The store
	// resolves the original TxID to that combined record, whose current attempt
	// is already the reverse. It must still retire the inherited source cut.
	combined := next
	combined.Historical.Forward = a
	combined.Historical.Reverse = b
	combined.Historical.Forward.Phase, combined.Historical.Forward.CutStage, combined.Historical.Forward.Outcome = upgradetx.PhaseRollbackPending, upgradetx.CutRetiring, upgradetx.OutcomePending
	combined.Historical.Reverse.Handle.TxID = forward.TxID
	combined.Historical.Grant = old.Historical.Grant
	p.held.handle = combined.Historical.Reverse.Handle
	// The store resolves the empty TxID to the domain's latest record, which is
	// this combined record; the fallback now checks the reverse is still current.
	p.host.Journal = retirementJournal{records: map[string]upgradetx.View{forward.TxID: combined, "": combined}}
	if receipt, err := p.Retire(forward); err != nil || !receipt.Census.Known || snapshots != 0 || receipt.Stamp.Attempt != forward {
		t.Fatalf("combined pending-forward/reverse record lost inherited custody: known=%t snapshots=%d err=%v", receipt.Census.Known, snapshots, err)
	}
	// Another held cut cannot certify this inherited generation.
	p.held.handle.Generation++
	if _, err := p.Retire(forward); err == nil {
		t.Fatal("unrelated held generation signed retirement")
	}
}
