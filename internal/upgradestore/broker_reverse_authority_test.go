package upgradestore_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F9
func TestBrokerReverseUsesAuthorityMapWithoutAgentIncarnation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerReverseAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker reverse authority map x5", g)
	testharness.AssertNoFDLeak(t, "broker reverse authority map x5", fd, 0)
}

func exerciseBrokerReverseAuthority(t *testing.T) {
	t.Helper()
	r := newComposedRuntime(t, upgradetx.ComponentBroker)
	view, err := brokerupgrade.New(r.store, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err != nil || composedAttempt(view.Historical).Phase != upgradetx.PhaseCompleted {
		t.Fatalf("forward did not complete: %v", err)
	}
	r.reopen()
	a := composedAttempt(view.Historical)
	fence := upgradetx.Fence{Current: a.Handle, Owner: r.owner, RunningSHA: r.running, PreviousSHA: view.Historical.RecordedPreviousSHA}
	request := upgradetx.ReverseRequest{ClientNonce: "manual-broker-reverse", ActorFP: r.grant.ActorFP, HistoricalTxID: a.Handle.TxID, TargetSHA: fence.PreviousSHA, Scope: a.Scope, ExpiresAt: time.Now().Add(time.Minute).UnixNano()}
	grant, err := r.store.PrepareReverse(fence, request)
	if err != nil {
		t.Fatalf("broker authority map incorrectly requires an agent incarnation scalar: %v", err)
	}
	digest, err := upgradetx.ScopeDigest(a.Scope)
	if err != nil || grant.ScopeDigest != digest || grant.Handle.CutID == a.Handle.CutID || grant.SourceSHA != fence.RunningSHA || grant.TargetSHA != fence.PreviousSHA {
		t.Fatal("broker reverse lost its exact current scope/owner binding")
	}
}
