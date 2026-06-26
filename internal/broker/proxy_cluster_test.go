package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/session"
)

// proxy_cluster_test.go (C5) — the pure degraded-state machine + readiness-reason precedence + the
// event change-gate (the load-bearing C5 semantics: no-quorum fail-closed write, freeze-keeps-vending,
// disable-404, render-equivalence).

func TestProxyClusterState(t *testing.T) {
	cases := []struct {
		name                                  string
		clusterMode, forceSingle, leaderStale bool
		policy                                string
		wantState                             string
		wantWritable, wantVendable            bool
	}{
		{"single mode", false, false, false, session.ProxyHAFreeze, proxyStateActive, true, true},
		{"cluster active", true, false, false, session.ProxyHAFreeze, proxyStateActive, true, true},
		{"force single", true, true, true, session.ProxyHAFreeze, proxyStateForceSingle, true, true},
		{"no quorum freeze: writes refused, /sub vends", true, false, true, session.ProxyHAFreeze, proxyStateFrozenReadonly, false, true},
		{"no quorum disable: writes refused, /sub 404", true, false, true, session.ProxyHADisable, proxyStateDisabledNoQuorum, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := proxyClusterState(c.clusterMode, c.forceSingle, c.leaderStale, c.policy)
			if got.State != c.wantState || got.Writable != c.wantWritable || got.Vendable != c.wantVendable {
				t.Fatalf("got %+v, want state=%s writable=%v vendable=%v", got, c.wantState, c.wantWritable, c.wantVendable)
			}
		})
	}
}

func TestProxyReadyReasonPrecedence(t *testing.T) {
	base := proxyAgentFacts{HasHome: true, NodeOnline: true, ProxyReady: true, AgentEpoch: 5, SessionEpoch: 5}
	if r := proxyReadyReason(base); r != proxyReasonReady {
		t.Fatalf("a fully-ready agent: %s", r)
	}
	// no_home wins over everything.
	if r := proxyReadyReason(proxyAgentFacts{HasHome: false, NodeOnline: true, ProxyReady: true}); r != proxyReasonNoHome {
		t.Fatalf("no_home precedence: %s", r)
	}
	// catching_up wins over tunnel/keyset.
	f := base
	f.HomeCatchingUp = true
	f.ProxyReady = false
	if r := proxyReadyReason(f); r != proxyReasonCatchingUp {
		t.Fatalf("catching_up precedence: %s", r)
	}
	// tunnel_down (offline or !proxy_ready) wins over keyset_stale.
	f = base
	f.ProxyReady = false
	f.AgentEpoch = 4
	if r := proxyReadyReason(f); r != proxyReasonTunnelDown {
		t.Fatalf("tunnel_down precedence: %s", r)
	}
	// keyset_stale: ready but agent epoch behind.
	f = base
	f.AgentEpoch = 4
	if r := proxyReadyReason(f); r != proxyReasonKeysetStale {
		t.Fatalf("keyset_stale: %s", r)
	}
	// render-equivalence: in /sub iff reason ∈ {ready, keyset_stale}.
	for r, want := range map[string]bool{
		proxyReasonReady: true, proxyReasonKeysetStale: true,
		proxyReasonNoHome: false, proxyReasonCatchingUp: false, proxyReasonTunnelDown: false,
	} {
		if proxyInSubRender(r) != want {
			t.Fatalf("render-equivalence for %s: got %v want %v", r, proxyInSubRender(r), want)
		}
	}
}

func TestDecideProxyEventsChangeGated(t *testing.T) {
	// disabled session → no events.
	if ev := decideProxyEvents(proxyEventCounts{}, proxyEventCounts{Enabled: false}); ev != nil {
		t.Fatalf("disabled session must emit nothing: %v", ev)
	}
	// enabled, 3 capable, 0 ready → sub_render_empty + proxy_no_ready_nodes (newly).
	ev := decideProxyEvents(proxyEventCounts{Enabled: false}, proxyEventCounts{Enabled: true, CapableNodes: 3, ReadyNodes: 0})
	if !contains(ev, "sub_render_empty") || !contains(ev, "proxy_no_ready_nodes") {
		t.Fatalf("0-ready enabled session must emit sub_render_empty + proxy_no_ready_nodes: %v", ev)
	}
	// steady 0-ready → no re-emit (change-gated).
	prev := proxyEventCounts{Enabled: true, CapableNodes: 3, ReadyNodes: 0}
	if ev := decideProxyEvents(prev, prev); ev != nil {
		t.Fatalf("a steady state must NOT re-emit: %v", ev)
	}
	// partial (2 of 3 ready) newly → proxy_partial.
	ev = decideProxyEvents(prev, proxyEventCounts{Enabled: true, CapableNodes: 3, ReadyNodes: 2})
	if !contains(ev, "proxy_partial") {
		t.Fatalf("a newly-partial render must emit proxy_partial: %v", ev)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
