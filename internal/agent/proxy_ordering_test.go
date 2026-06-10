package agent

import (
	"context"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// Self-review: lock the (generation, epoch) lexicographic ordering contract.
func TestProxyNewerLexicographic(t *testing.T) {
	cases := []struct {
		name                                   string
		dGen, dEpoch, appliedGen, appliedEpoch int64
		want                                   bool
	}{
		{"higher gen lower epoch applies (restore)", 200, 1, 100, 9, true},
		{"same gen higher epoch applies", 100, 6, 100, 5, true},
		{"same gen same epoch is stale", 100, 5, 100, 5, false},
		{"same gen lower epoch is stale", 100, 4, 100, 5, false},
		{"lower gen higher epoch is stale", 99, 100, 100, 1, false},
		{"from zero applies", 1, 1, 0, 0, true},
		{"zero against applied is stale", 0, 0, 100, 5, false},
	}
	for _, c := range cases {
		if got := proxyNewer(c.dGen, c.dEpoch, c.appliedGen, c.appliedEpoch); got != c.want {
			t.Errorf("%s: proxyNewer(%d,%d,%d,%d)=%v want %v",
				c.name, c.dGen, c.dEpoch, c.appliedGen, c.appliedEpoch, got, c.want)
		}
	}
}

// Self-review (rejecting a suggested but WRONG fix): after a directive tears the
// proxy down, the applied (gen,epoch) must be RETAINED — NOT reset to (0,0).
// Resetting would let a stale OLD enable re-apply and resurrect a killed exit.
// This guards proxyFailCleanupLocked/disable against that regression.
func TestStaleEnableDroppedAfterDisableTeardown(t *testing.T) {
	a := newProxyTestAgent(t)
	// Apply enable (gen 100, epoch 5) then an authoritative disable (gen 100, epoch 6).
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 100, Epoch: 5,
	})
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: false, Generation: 100, Epoch: 6,
	})
	if runningSrv(a) {
		t.Fatal("precondition: disable should have torn the proxy down")
	}
	// A LATE stale enable at the OLD pair (gen 100, epoch 5) must be DROPPED —
	// the applied pair is retained at (100,6), so this is not newer.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 100, Epoch: 5,
	})
	if runningSrv(a) {
		t.Fatal("stale enable resurrected a killed proxy after disable (applied pair must be retained, not reset)")
	}
}
