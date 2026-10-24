package upgradestore_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv-txn F09 F10
func TestMixedDomainProofSurvivesCollectorTransitionAndReopen(t *testing.T) {
	// Keep the original grant immutable; a second fixture starts with explicit
	// inapplicability, a terminal responsibility and idle applicable domains.
	r := newComposedRuntime(t, upgradetx.ComponentBroker)
	// No accepted execution exists yet. Mint a distinct authorization for the
	// expanded original set, exactly as a runtime census would before acceptance.
	terminal := r.scope.Expected[0]
	terminal.State = "terminal"
	terminal.Custody = "durable"
	r.scope.Expected = []upgradetx.Resource{terminal, {Domain: upgradetx.DomainSS, ID: "unsupported", Origin: "capability", State: "not_applicable", Custody: "not_applicable"}}
	r.resources = append([]upgradetx.Resource{}, r.scope.Expected...)
	r.prepare([]byte("A"), []byte("B"))
	rec, err := r.store.Start(r.grant, upgradetx.Fence{Owner: r.owner, RunningSHA: r.grant.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(e upgradetx.Event) {
		t.Helper()
		rec, err = r.store.Apply(composedAttempt(rec).Handle, e)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}, {Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive}} {
		apply(e)
	}
	r.running = r.grant.TargetSHA
	late := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "late", Origin: "new-call", Owner: r.owner, Generation: r.grant.Handle.Generation, State: "active", Custody: "target"}
	seq, err := r.registry.Begin(late)
	if err != nil {
		t.Fatal(err)
	}
	r.sequence = seq
	r.resources = append(r.resources, late)
	if err = r.registry.Finish(seq, late); err != nil {
		t.Fatal(err)
	}
	a := composedAttempt(rec)
	o, err := r.Observe(upgradetx.Stamp{Attempt: a.Handle, Nonce: txnHex("1", 32)})
	if err != nil {
		t.Fatal(err)
	}
	apply(upgradetx.Event{Receipt: o.Current.Control, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive})
	receipt, err := r.registry.Seal(composedAttempt(rec), txnHex("2", 32))
	if err != nil {
		t.Fatal(err)
	}
	apply(upgradetx.Event{Receipt: receipt, Phase: upgradetx.PhaseMigrating})
	released, err := r.Retire(composedAttempt(rec).Handle)
	if err != nil {
		t.Fatal(err)
	}
	apply(upgradetx.Event{Receipt: released, CutStage: upgradetx.CutRetiring})
	r.reopen()
	v, err := r.store.Load(r.options.Key, "")
	if err != nil {
		t.Fatal(err)
	}
	rec = v.Historical
	stamp := released.Stamp
	stamp.Attempt = composedAttempt(rec).Handle
	stamp.Nonce = txnHex("3", 32)
	stamp.At = time.Now().UnixNano()
	err = upgradeobserve.Commit(r.registry, stamp, func(upgradetx.Census) error {
		next, e := r.store.Apply(stamp.Attempt, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
		if e == nil {
			rec = next
		}
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	r.reopen()
	v, err = r.store.Load(r.options.Key, "")
	if err != nil {
		t.Fatal(err)
	}
	o, err = r.Observe(upgradetx.Stamp{Attempt: composedAttempt(v.Historical).Handle, Nonce: txnHex("4", 32)})
	if err != nil {
		t.Fatal(err)
	}
	// Observation's independent control probe need not replace historical receipts.
	o.Current = composedAttempt(v.Historical)
	if err = upgradetx.Completed(composedAttempt(v.Historical), o); err != nil {
		t.Fatal(err)
	}
	if len(o.Census.Resources) != 3 {
		t.Fatal("late or terminal identity disappeared")
	}
}

// origin: S8 internal review rv-txn F03
func TestCompletionCommitFencesBeginUntilDurableCAS(t *testing.T) {
	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCompletionCommit(t)
	}
	testharness.AssertNoGoroutineLeak(t, "completion commit", before)
	testharness.AssertNoFDLeak(t, "completion commit", fds, 0)
}

func exerciseCompletionCommit(t *testing.T) {
	t.Helper()
	f := txnNew(t, false)
	f.active(t)
	f.receipts(t)
	registry := txnRegistry(t, f.record.Forward, f.opts.Owner)
	sealed, err := registry.Seal(f.record.Forward, txnHex("5", 32))
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var joined sync.WaitGroup
	joined.Go(func() {
		done <- upgradeobserve.Commit(registry, sealed.Stamp, func(upgradetx.Census) error {
			close(entered)
			<-release
			_, e := f.store.Apply(f.record.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess})
			return e
		})
	})
	<-entered
	beginStarted, beginDone := make(chan struct{}), make(chan error, 1)
	late := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "racing", Origin: "new-call", Owner: f.opts.Owner, Generation: f.record.Forward.Handle.Generation, State: "starting", Custody: "target"}
	joined.Go(func() { close(beginStarted); _, e := registry.Begin(late); beginDone <- e })
	<-beginStarted
	select {
	case err := <-beginDone:
		close(release)
		joined.Wait()
		t.Fatalf("admission escaped durable callback: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	joined.Wait()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = <-beginDone; err != nil {
		t.Fatal(err)
	}
	v, err := f.store.Load(f.opts.Key, "")
	if err != nil || v.Historical.Forward.Phase != upgradetx.PhaseCompleted {
		t.Fatal("new admission preceded durable completion")
	}
}

// origin: S8 internal review rv-txn F04 F05
func TestEnginesRecoverOriginalActionAcrossJournalReopen(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		for _, kind := range []upgradetx.ActionKind{upgradetx.ActionActivate, upgradetx.ActionControl, upgradetx.ActionMigrate, upgradetx.ActionRetire} {
			for _, state := range []upgradetx.ActionState{upgradetx.ActionIntent, upgradetx.ActionObserved, upgradetx.ActionDurable} {
				t.Run(string(role)+"/"+string(kind)+"/"+string(state), func(t *testing.T) {
					r := newComposedRuntime(t, role)
					rec, err := r.store.Start(r.grant, upgradetx.Fence{Owner: r.owner, RunningSHA: r.running})
					if err != nil {
						t.Fatal(err)
					}
					for _, e := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}, {Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive}} {
						rec, err = r.store.Apply(composedAttempt(rec).Handle, e)
						if err != nil {
							t.Fatal(err)
						}
					}
					_, err = r.Activate(upgradeCutFor(rec))
					if err != nil {
						t.Fatal(err)
					}
					if kind == upgradetx.ActionMigrate || kind == upgradetx.ActionRetire {
						o, observeErr := r.Observe(upgradetx.Stamp{Attempt: composedAttempt(rec).Handle, Nonce: txnHex("3", 32)})
						if observeErr != nil {
							t.Fatal(observeErr)
						}
						rec, err = r.store.Apply(composedAttempt(rec).Handle, upgradetx.Event{Receipt: o.Current.Control, Phase: upgradetx.PhaseServingNew, CutStage: upgradetx.CutActive})
						if err != nil {
							t.Fatal(err)
						}
					}
					if kind == upgradetx.ActionRetire {
						proof, sealErr := r.registry.Seal(composedAttempt(rec), txnHex("4", 32))
						if sealErr != nil {
							t.Fatal(sealErr)
						}
						rec, err = r.store.Apply(composedAttempt(rec).Handle, upgradetx.Event{Receipt: proof, Phase: upgradetx.PhaseMigrating})
						if err != nil {
							t.Fatal(err)
						}
					}
					action := upgradetx.Action{ID: txnHex("a", 32), Handle: composedAttempt(rec).Handle, Kind: kind, Nonce: txnHex("b", 32), IntentDigest: txnHex("c", 64), State: upgradetx.ActionIntent}
					rec, err = r.store.Apply(composedAttempt(rec).Handle, upgradetx.Event{Action: action})
					if err != nil {
						t.Fatal(err)
					}
					if state != upgradetx.ActionIntent {
						action.State = upgradetx.ActionObserved
						action.ResultDigest = txnHex("d", 64)
						rec, err = r.store.Apply(composedAttempt(rec).Handle, upgradetx.Event{Action: action})
						if err != nil {
							t.Fatal(err)
						}
					}
					if state == upgradetx.ActionDurable {
						action.State = state
						rec, err = r.store.Apply(composedAttempt(rec).Handle, upgradetx.Event{Action: action})
						if err != nil {
							t.Fatal(err)
						}
					}
					r.reopen()
					var v upgradetx.View
					// ctx-root: simulate boot after the effect and a lost result write.
					ctx := context.Background()
					if role == upgradetx.ComponentAgent {
						v, err = agentupgrade.Boot(ctx, r.endpoint(), composedAttempt(rec).Handle)
					} else {
						v, err = brokerupgrade.Resume(ctx, r.endpoint(), composedAttempt(rec).Handle)
					}
					if err != nil {
						t.Fatal(err)
					}
					if composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted || r.effects != 1 {
						t.Fatal("boot did not finish the original effect exactly once")
					}
					found := false
					for _, a := range v.Historical.Actions {
						if a.ID == action.ID {
							found = true
							if a.State != upgradetx.ActionDurable || a.Handle != action.Handle {
								t.Fatalf("original action not reconciled: %+v", a)
							}
						}
					}
					if !found {
						t.Fatal("lost action")
					}
				})
			}
		}
	}

}

// origin: S8 internal review rv-txn F05
func TestAutomaticReversePersistsForwardRetirementAcrossReopen(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentAgent)
	r.failActivate = true
	// ctx-root: partial activation triggers real automatic engine recovery.
	v, err := r.endpoint().Start(context.Background(), r.grant, r.receipt)
	var txErr *upgradetx.Error
	if !errors.As(err, &txErr) || txErr.Code != upgradetx.CodeRejected || v.Historical.Forward.Phase != upgradetx.PhaseRolledBack || v.Historical.Reverse.Phase != upgradetx.PhaseCompleted {
		t.Fatalf("automatic reverse failed: %v phase=%s/%s", err, v.Historical.Forward.Phase, v.Historical.Reverse.Phase)
	}
	retirements := 0
	for _, action := range v.Historical.Actions {
		if upgradetx.SameCut(action.Handle, v.Historical.Reverse.Handle) && action.Kind == upgradetx.ActionRetire {
			retirements++
		}
	}
	if retirements != 2 {
		t.Fatal("source salvage and reverse retirement shared one effect identity")
	}
	if v.Historical.Forward.Released.Kind != upgradetx.ReceiptOldGenerationReleased {
		t.Fatal("forward retirement was only an action digest")
	}
	r.reopen()
	got, err := r.store.Load(r.options.Key, "")
	if err != nil || !reflect.DeepEqual(got.Historical, v.Historical) {
		t.Fatalf("reverse cannot reopen: %v", err)
	}
	if _, err = upgradeartifact.Resolve(r.options.Key, got); err != nil {
		t.Fatal(err)
	}
}

// origin: S8 internal review rv-txn F05 F10
func TestReverseActivationRequiresOriginalRetirementAndStaleWritesFail(t *testing.T) {
	f := txnNew(t, false)
	f.active(t)
	old := f.record.Forward
	fence := upgradetx.Fence{Current: old.Handle, Owner: f.opts.Owner, RunningSHA: old.TargetSHA, PreviousSHA: old.SourceSHA, Incarnation: txnHex("f", 64)}
	g, err := f.store.PrepareReverse(fence, upgradetx.ReverseRequest{ClientNonce: txnHex("1", 32), ActorFP: "actor", HistoricalTxID: old.Handle.TxID, TargetSHA: old.SourceSHA, Scope: old.Scope})
	if err != nil {
		t.Fatal(err)
	}
	f.record, err = f.store.Start(g, fence)
	if err != nil {
		t.Fatal(err)
	}
	f.store, err = upgradestore.Open(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []upgradetx.Event{{Phase: upgradetx.PhaseStaged}, {Phase: upgradetx.PhaseReady}, {Phase: upgradetx.PhaseFreezing, CutStage: upgradetx.CutPristine}} {
		f.apply(t, e)
	}
	before := f.record
	if _, err = f.store.Apply(f.record.Reverse.Handle, upgradetx.Event{Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive}); err == nil {
		t.Fatal("G+2 effect allowed before G retirement")
	}
	if _, err = f.store.Apply(old.Handle, upgradetx.Event{Reason: "late forward"}); err == nil {
		t.Fatal("late forward acquired reverse CAS")
	}
	got, err := f.store.Load(f.opts.Key, "")
	if err != nil || !reflect.DeepEqual(got.Historical, before) {
		t.Fatal("refused writes changed reverse")
	}
}

func upgradeCutFor(r upgradetx.Record) upgradeport.Cut {
	return upgradeport.Cut{Attempt: composedAttempt(r).Handle}
}
