package agentupgradeport

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// recordJournal answers every Load with the current record.
type recordJournal struct {
	upgradetx.Store
	view *upgradetx.View
}

func (j recordJournal) Load(upgradetx.Key, string) (upgradetx.View, error) { return *j.view, nil }

// origin: hot-update S8 plan §4.2 / M04 / M15; S8 FIX-DESCRIBE (real-cut
// findings). Every sample the importing target takes of its cut must line up
// with the completion registry: it names the registry's admission sequence
// (never a private counter), has one sampling time (Census.Stamp.At == At),
// and the registry's own probe echoes its stamp so Seal can bind the running
// image. A still-activating cut gets a fresh Control receipt and the image's
// active stage; a completed cut is reported exactly as journaled — released,
// with its durable receipts, never re-stamped by the query's nonce.
func TestImportedCutSamplesShareTheRegistryBarrier(t *testing.T) {
	owner := upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	source, target := strings.Repeat("a", 64), strings.Repeat("c", 64)
	scope, err := upgradetx.CanonicalScope(upgradetx.Scope{ID: strings.Repeat("5", 32), Participants: []upgradetx.Key{key},
		Domains: append([]upgradetx.Domain(nil), registryDomains[:]...), Expected: []upgradetx.Resource{{Domain: upgradetx.DomainAuthority, ID: strings.Repeat("7", 64), Origin: "session:lab", Owner: owner, Generation: 1, State: "active", Custody: "source"}}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 9}
	attempt := upgradetx.Attempt{Handle: handle, Direction: upgradetx.DirectionForward, SourceSHA: source, TargetSHA: target, SourceGeneration: 1,
		Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive, Outcome: upgradetx.OutcomePending, Scope: scope}
	view := &upgradetx.View{Historical: upgradetx.Record{Version: upgradetx.RecordVersion, Key: key, Revision: 9,
		Grant: upgradetx.Grant{Handle: handle, ScopeDigest: digest}, Forward: attempt}}
	controls := 0
	port := NewRuntime(Host{Key: key, Journal: recordJournal{view: view}, Incarnation: func() string { return strings.Repeat("7", 64) },
		Owner:      func() (upgradetx.Owner, error) { return owner, nil },
		RunningSHA: func() (string, error) { return target, nil },
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			return execsession.Image{}, agentimage.CourierState{}, nil
		},
		Transfers: noTransfers,
		Inherited: &upgradecut.State{Attempt: handle, Owner: owner, SourceSHA: source, TargetSHA: target, Scope: scope},
		Stage:     func() upgradetx.CutStage { return upgradetx.CutActive },
		Control:   func(string) error { controls++; return nil },
	}).(*runtimePort)
	registry, err := CutRegistry(port, port.host.Journal, *port.host.Inherited)
	if err != nil {
		t.Fatalf("imported cut registry: %v", err)
	}
	barrier, ok := upgradeobserve.Sequence(registry)
	if !ok || barrier == 0 {
		t.Fatal("registry exposes no admission barrier")
	}
	for round := 0; round < 3; round++ { // advance any private sample counter
		o, err := port.Observe(upgradetx.Stamp{Attempt: handle, Nonce: newID(), ScopeDigest: digest})
		if err != nil {
			t.Fatal(err)
		}
		if o.Census.Stamp.Sequence != barrier {
			t.Fatalf("target sample %d names sequence %d, registry barrier %d", round, o.Census.Stamp.Sequence, barrier)
		}
		if o.At != o.Census.Stamp.At || o.At == 0 {
			t.Fatalf("one observation carries two sampling times: at=%d census=%d", o.At, o.Census.Stamp.At)
		}
		if o.Current.CutStage != upgradetx.CutActive || o.Current.Control.Kind != upgradetx.ReceiptControlReady || o.Current.Control.Stamp != o.Census.Stamp {
			t.Fatalf("activating cut lost its fresh control or active stage: %+v", o.Current)
		}
	}
	echo := upgradetx.Stamp{Attempt: handle, Producer: owner, Nonce: newID(), Sequence: 77, At: 123, ScopeDigest: digest}
	if o, err := port.Observe(echo); err != nil || o.Census.Stamp.Sequence != 77 {
		t.Fatalf("a requested registry stamp was not echoed: %+v %v", o.Census.Stamp, err)
	}
	// The eleven domain observers share one census sample at the seal's stamp
	// and probe no control; only the registry's own probe does, once.
	// origin: S8 FIX-DESCRIBE (real-cut finding: completion took a broker
	// round trip and a runtime sample per frozen domain)
	snapshots, probes := 0, controls
	port.host.Snapshot = func() (execsession.Image, agentimage.CourierState, error) {
		snapshots++
		return execsession.Image{}, agentimage.CourierState{}, nil
	}
	if _, err := registry.Seal(attempt, newID()); err != nil {
		t.Fatalf("seal could not bind the running image through the probe: %v", err)
	}
	if controls-probes != 1 || snapshots != 2 {
		t.Fatalf("seal probed control %d times and sampled the runtime %d times, want 1 and 2 (census, probe)", controls-probes, snapshots)
	}
	// Old-generation release reads the census and running bytes only.
	probes = controls
	if released, err := port.Retire(handle); err != nil || released.Kind != upgradetx.ReceiptOldGenerationReleased || controls != probes {
		t.Fatalf("retire probed control or failed: probes=%d err=%v", controls-probes, err)
	}
	// Completed: report the journal exactly; no fresh control, no downgrade.
	durable := upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: upgradetx.Stamp{Attempt: handle, Producer: owner, Nonce: strings.Repeat("4", 32), Sequence: barrier, At: 5, ScopeDigest: digest}, Known: true, Complete: true, RunningSHA: target}
	settled := attempt
	settled.Phase, settled.CutStage, settled.Outcome, settled.Control = upgradetx.PhaseCompleted, upgradetx.CutReleased, upgradetx.OutcomeSuccess, durable
	view.Historical.Forward = settled
	before := controls
	o, err := port.Observe(upgradetx.Stamp{Attempt: handle, Nonce: newID(), ScopeDigest: digest})
	if err != nil || o.Current.CutStage != upgradetx.CutReleased || !reflect.DeepEqual(o.Current.Control, durable) || controls != before {
		t.Fatalf("completed cut was re-stamped or downgraded: stage=%s control=%+v probes=%d err=%v", o.Current.CutStage, o.Current.Control.Stamp, controls-before, err)
	}
}
