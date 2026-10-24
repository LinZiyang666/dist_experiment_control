package upgradestore_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review rv-txn F05 F06
func TestBrokerPendingReverseSurvivesReopenAndColdResolve(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentBroker)
	r.failActivate = true
	// ctx-root: interrupted activation is recovered by the broker watchdog port.
	ctx := context.Background()
	v, err := r.endpoint().Start(ctx, r.grant, r.receipt)
	if err == nil || composedAttempt(v.Historical).CutStage != upgradetx.CutPartialActive {
		t.Fatal("partial effect was not retained for recovery")
	}
	r.reopen()
	a := r.current()
	fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: r.grant.SourceSHA, Incarnation: txnHex("f", 32)}
	v, err = r.endpoint().Rollback(ctx, fence, upgradetx.ReverseRequest{ClientNonce: txnHex("e", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: a.Scope})
	if err != nil || v.Historical.Forward.Phase != upgradetx.PhaseRolledBack || v.Historical.Reverse.Phase != upgradetx.PhaseCompleted {
		t.Fatalf("pending broker reverse: %v", err)
	}
	if v.Historical.Forward.Released.Kind != upgradetx.ReceiptOldGenerationReleased {
		t.Fatal("reverse effect preceded durable forward retirement")
	}
	r.reopen()
	got, err := r.store.Load(r.options.Key, "")
	if err != nil || !reflect.DeepEqual(got.Historical, v.Historical) {
		t.Fatalf("pending reverse changed on reopen: %v", err)
	}
	path, err := upgradeartifact.Resolve(r.options.Key, got)
	if err != nil || path != r.receipt.SourcePath {
		t.Fatalf("pending reverse cold resolve: %q %v", path, err)
	}
}

// origin: S8 internal review rv-txn F03 F08 F10
func TestIdleEngineCreatesFirstResourceAtDurableGeneration(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			r.scope.Expected, r.resources = nil, nil
			r.prepare([]byte("A"), []byte("B"))
			// ctx-root: no business work exists before this isolated upgrade.
			ctx := context.Background()
			v, err := r.endpoint().Start(ctx, r.grant, r.receipt)
			if err != nil {
				t.Fatal(err)
			}
			a := composedAttempt(v.Historical)
			row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "first", Origin: "after-idle", Owner: r.owner, Generation: a.Handle.Generation, State: "active", Custody: "target"}
			sequence, err := r.registry.Begin(row)
			if err != nil {
				t.Fatal(err)
			}
			r.sequence = sequence
			r.resources = append(r.resources, row)
			if err = r.registry.Finish(sequence, row); err != nil {
				t.Fatal(err)
			}
			r.reopen()
			r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
			r.prepare([]byte("B"), []byte("C"))
			v, err = r.endpoint().Start(ctx, r.grant, r.receipt)
			if err != nil || composedAttempt(v.Historical).Handle.Generation != a.Handle.Generation+1 {
				t.Fatalf("first business resource lost durable generation: %v", err)
			}
		})
	}
}

// origin: S8 internal review rv-txn F05 F08 F09
func TestReverseKeepsAdmissionsAfterForwardCompletion(t *testing.T) {
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		t.Run(string(role), func(t *testing.T) {
			r := newComposedRuntime(t, role)
			// ctx-root: the same admission owner survives forward and reverse.
			ctx := context.Background()
			v, err := r.endpoint().Start(ctx, r.grant, r.receipt)
			if err != nil {
				t.Fatal(err)
			}
			original := v.Historical
			a := composedAttempt(original)
			row := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "after-completion", Origin: "late-request", Owner: r.owner, Generation: a.Handle.Generation, State: "active", Custody: "target"}
			seq, err := r.registry.Begin(row)
			if err != nil {
				t.Fatal(err)
			}
			r.sequence = seq
			r.resources = append(r.resources, row)
			if err = r.registry.Finish(seq, row); err != nil {
				t.Fatal(err)
			}
			r.scope.Expected = append([]upgradetx.Resource{}, r.resources...)
			r.scope, err = upgradetx.CanonicalScope(r.scope)
			if err != nil {
				t.Fatal(err)
			}
			r.reopen()
			fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: original.RecordedPreviousSHA, Incarnation: txnHex("f", 64)}
			v, err = r.endpoint().Rollback(ctx, fence, upgradetx.ReverseRequest{ClientNonce: txnHex("f", 32), ActorFP: "actor", HistoricalTxID: a.Handle.TxID, TargetSHA: a.SourceSHA, Scope: r.scope})
			if err != nil || composedAttempt(v.Historical).Phase != upgradetx.PhaseCompleted {
				t.Fatalf("late custody blocked reverse: %v", err)
			}
			if len(composedAttempt(v.Historical).Released.Census.Resources) != 2 {
				t.Fatal("reverse discarded late custody")
			}
			r.reopen()
			old, err := r.store.Load(r.options.Key, original.Grant.Handle.TxID)
			if err != nil || !reflect.DeepEqual(old.Historical, original) {
				t.Fatal("late reverse rewrote completed history")
			}
		})
	}
}
