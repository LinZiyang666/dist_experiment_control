//go:build linux

package agent

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// origin: hot-update S5 internal review rv-authority-4
func TestRestoredFreshAuthorityDominatesDelayedKeysetPush(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRestoredFreshAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "restored proxy authority", g0)
	testharness.AssertNoFDLeak(t, "restored proxy authority", fd0, 0)
}
func exerciseRestoredFreshAuthority(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	d := &proto.ProxyDirective{Enabled: true, Generation: 7, Epoch: 1, PublicPort: 14017, Token: "private-allocation", Keys: []proto.ProxyKey{{SubID: "alice", Secret: "alice-private-material"}, {SubID: "bob", Secret: "bob-private-material"}}}
	a.applyProxyDirective(nil, d)
	owner := a.proxy.srv
	if owner == nil || !a.proxyBound() {
		t.Fatal("real SS owner required")
	}
	state := &flowRestoreState{server: owner}
	fresh := tunnel.Authority{Enabled: true, Allocation: tunnel.Allocation{SID: a.cfg.SID, NID: nidOf(a), Port: d.PublicPort}, Generation: 7, ProxyEpoch: 3, AppliedRevision: 103, Keys: []tunnel.AuthorizedKey{{ID: "bob", Secret: []byte(d.Keys[1].Secret)}}}
	if err := authorizeRestoredProxy(a, state, fresh); err != nil {
		t.Fatal(err)
	}
	delayed := *d
	delayed.Epoch = 2
	delayed.Token = ""
	a.applyProxyDirective(nil, &delayed)
	if a.proxy.srv != owner {
		t.Fatal("delayed push replaced current owner")
	}
	f, err := owner.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(f.Discard)
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range cut.Records {
		if r.Kind == "ss-server" {
			for _, field := range r.Fields {
				if field.Name == "keys" && bytes.Contains(field.Bytes, []byte("alice")) {
					t.Fatal("delayed keyset push resurrected a freshly revoked key")
				}
			}
		}
	}
	if a.proxy.gen != 7 || a.proxy.epoch != 3 {
		t.Fatal("fresh RESUME watermark was not applied to push authority")
	}
	// A stale RESUME must likewise leave the currently authorized receipt intact.
	if err := authorizeRestoredProxy(a, state, tunnel.Authority{Enabled: true, Allocation: fresh.Allocation, Generation: 7, ProxyEpoch: 2, AppliedRevision: 102, Keys: fresh.Keys}); err == nil {
		t.Fatal("stale RESUME accepted")
	}
	if !state.authorized.Load() {
		t.Fatal("rejected RESUME cleared current authorization")
	}
	revoked := fresh
	revoked.Enabled, revoked.ProxyEpoch, revoked.AppliedRevision, revoked.Keys = false, 4, 104, nil
	if err := authorizeRestoredProxy(a, state, revoked); err != nil {
		t.Fatal(err)
	}
	if state.authorized.Load() || a.proxyTunnelUp.Load() {
		t.Fatal("fresh RESUME revocation retained readiness")
	}
}
