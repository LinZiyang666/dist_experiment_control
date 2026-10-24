package agent

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real SS listeners and keyset operations; the tunnel adapter records lifecycle
// calls. This unit oracle covers authority application, not crypto continuity.
// origin: hot-update S5 plan §6.4 / §8
func TestProxyPairDigestIsNotSuppressed(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxyPairDigest(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy pair digest", beforeG)
	testharness.AssertNoFDLeak(t, "proxy pair digest", beforeFD, 0)
}

func exerciseProxyPairDigest(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	d := &proto.ProxyDirective{Enabled: true, Generation: 7, Epoch: 11,
		PublicPort: 14015, Token: "opaque-allocation-token",
		Keys: []proto.ProxyKey{{SubID: "alice", Secret: "alice-material"}, {SubID: "bob", Secret: "bob-material"}}}
	a.applyProxyDirective(nil, d)
	if !a.proxyBound() {
		t.Fatal("precondition: real SS listener and tunnel must be ready")
	}
	original := a.proxy.srv
	digest := a.proxy.keysetDigest
	reverse := *d
	reverse.Keys = []proto.ProxyKey{d.Keys[1], d.Keys[0]}
	a.applyProxyDirective(nil, &reverse)
	if a.proxy.srv != original || a.proxy.keysetDigest != digest || a.proxy.authorityConflicts != 0 {
		t.Fatal("equal complete keyset must re-ACK without changing owner or digest")
	}
	// Same pair, same ID, different material must not disappear at the old
	// exact-pair shortcut. Nor may that conflicting directive rehome first.
	conflict := *d
	conflict.Keys = []proto.ProxyKey{{SubID: "alice", Secret: "replaced-material"}, d.Keys[1]}
	conflict.Home = &proto.HomeDirective{PublicPort: d.PublicPort, Epoch: 99, BrokerAddr: "conflicting-home"}
	a.applyProxyDirective(nil, &conflict)
	if a.proxyBound() || original.Serving() || a.proxy.authorityConflicts != 1 {
		t.Fatal("same-pair different digest was suppressed instead of failing closed")
	}
	if a.proxy.gen != 7 || a.proxy.epoch != 11 || a.proxy.keysetDigest != digest || a.proxy.homeEpoch != 0 {
		t.Fatal("conflicting authority advanced the applied pair, digest or home")
	}
	if !a.proxy.needsReestablish {
		t.Fatal("local conflict must require authoritative reestablishment")
	}
	a.applyProxyDirective(nil, d)
	if !a.proxyBound() {
		t.Fatal("original authoritative complete snapshot could not reestablish")
	}
	for _, invalid := range []*proto.ProxyDirective{
		{Enabled: true, Generation: -1, Epoch: 12},
		{Enabled: true, Generation: 7, Epoch: -1},
		{Enabled: true, Generation: 7, Epoch: 12, Keys: []proto.ProxyKey{{SubID: "same", Secret: "a"}, {SubID: "same", Secret: "b"}}},
	} {
		if _, err := proxyDirectiveDigest(invalid); err == nil {
			t.Fatal("invalid authority snapshot passed the digest validator")
		}
		owner := a.proxy.srv
		a.applyProxyDirective(nil, invalid)
		if a.proxy.srv != owner || a.proxy.epoch != 11 {
			t.Fatal("invalid snapshot changed a live owner")
		}
	}
	st, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(verb, st), d.Token) {
			t.Fatal("private proxy cut leaked an allocation token")
		}
	}
}
