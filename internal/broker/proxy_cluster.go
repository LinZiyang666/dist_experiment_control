package broker

import "github.com/LinZiyang666/tether/internal/session"

// proxy_cluster.go (C5) — the PURE decision functions for the proxy degraded state machine + per-agent
// readiness reason + the observability-event change-gate. No I/O, no locks — the broker feeds them
// replicated/liveness facts. Kept pure so the load-bearing degraded semantics are unit-testable.

// Proxy cluster states (derived, never stored).
const (
	proxyStateActive           = "ACTIVE"
	proxyStateFrozenReadonly   = "FROZEN_READONLY"
	proxyStateDisabledNoQuorum = "DISABLED_NO_QUORUM"
	proxyStateForceSingle      = "FORCE_SINGLE"
)

// proxyClusterStatus is the derived proxy control/data capability for the current cluster condition.
type proxyClusterStatus struct {
	State    string // one of proxyState*
	Writable bool   // advisory: may a control write be ATTEMPTED (Raft is the real authority)
	Vendable bool   // does /sub serve (the read gate)
}

// proxyClusterState derives the degraded state from the cluster condition + the per-session HA policy.
// C5 §D-DEGRADED: the write gate is advisory (proposeOrForward is the authority); the /sub vend gate is
// `!leaderContactStale` ALONE (a routine election must NOT 404 /sub fleet-wide). The default policy is
// freeze (a fail-closed default would kill working subscriptions on any quorum blip).
func proxyClusterState(clusterMode, forceSingle, leaderContactStale bool, haPolicy string) proxyClusterStatus {
	if !clusterMode {
		return proxyClusterStatus{State: proxyStateActive, Writable: true, Vendable: true}
	}
	if forceSingle {
		// 1-voter timeline: a legitimate Raft write on the lone-voter quorum; degraded but operational.
		return proxyClusterStatus{State: proxyStateForceSingle, Writable: true, Vendable: true}
	}
	if !leaderContactStale {
		return proxyClusterStatus{State: proxyStateActive, Writable: true, Vendable: true}
	}
	// Stale leader contact ⇒ no committable quorum. Writes are refused regardless of policy.
	if haPolicy == session.ProxyHADisable {
		return proxyClusterStatus{State: proxyStateDisabledNoQuorum, Writable: false, Vendable: false}
	}
	// Default freeze: control writes refused, but /sub KEEPS vending the last committed state so
	// existing subscriber sessions survive (the data plane is leader-decoupled).
	return proxyClusterStatus{State: proxyStateFrozenReadonly, Writable: false, Vendable: true}
}

// Per-agent readiness reasons (proxy status --cluster).
const (
	proxyReasonNoHome      = "no_home"
	proxyReasonCatchingUp  = "catching_up"
	proxyReasonTunnelDown  = "tunnel_down"
	proxyReasonKeysetStale = "keyset_stale"
	proxyReasonReady       = "ready"
)

// proxyAgentFacts are the replicated + live facts about one agent's proxy footprint.
type proxyAgentFacts struct {
	HasHome        bool // an ALLOCATED __proxy__ row with a non-empty, Eligible() home
	HomeCatchingUp bool // the home VOTER is CATCHING_UP (or the row epoch advanced, agent not re-bound)
	NodeOnline     bool
	ProxyReady     bool  // liveness proxy_ready=1
	AgentEpoch     int64 // the keyset epoch the agent last reported via heartbeat
	SessionEpoch   int64 // sessions.proxy_epoch (the desired keyset epoch)
}

// proxyReadyReason returns the first-match readiness reason. Render-equivalence (C5 §4.3): a node is in
// the /sub render iff its reason ∈ {ready, keyset_stale} — a keyset_stale node still serves its OLD
// PSKs (the P13 multi-key keyset), so it stays vended but is flagged stale in status.
func proxyReadyReason(f proxyAgentFacts) string {
	switch {
	case !f.HasHome:
		return proxyReasonNoHome
	case f.HomeCatchingUp:
		return proxyReasonCatchingUp
	case !f.NodeOnline || !f.ProxyReady:
		return proxyReasonTunnelDown
	case f.AgentEpoch < f.SessionEpoch:
		return proxyReasonKeysetStale
	default:
		return proxyReasonReady
	}
}

// proxyInSubRender reports whether a node with this reason is included in the /sub render.
func proxyInSubRender(reason string) bool {
	return reason == proxyReasonReady || reason == proxyReasonKeysetStale
}

// proxyEventCounts is one session's proxy readiness snapshot, used to decide which observability events
// to emit (change-keyed → idle-zero-writes).
type proxyEventCounts struct {
	Enabled      bool
	CapableNodes int // online + proxy-capable nodes
	ReadyNodes   int // nodes in the /sub render
}

// decideProxyEvents returns the event kinds to emit for the transition prev→cur. Only emits on a real
// change (the caller change-keys per (sid,event) so a steady state is silent). Secret-free.
func decideProxyEvents(prev, cur proxyEventCounts) []string {
	if !cur.Enabled {
		return nil
	}
	var out []string
	// sub_render_empty: an enabled session renders 0 nodes (newly).
	if cur.ReadyNodes == 0 && (prev.ReadyNodes != 0 || !prev.Enabled) {
		out = append(out, "sub_render_empty")
	}
	// proxy_no_ready_nodes: enabled, ≥1 capable, 0 ready.
	if cur.CapableNodes >= 1 && cur.ReadyNodes == 0 && (prev.ReadyNodes != 0 || prev.CapableNodes == 0 || !prev.Enabled) {
		out = append(out, "proxy_no_ready_nodes")
	}
	// proxy_partial: some-but-not-all capable nodes ready (newly).
	curPartial := cur.ReadyNodes > 0 && cur.ReadyNodes < cur.CapableNodes
	prevPartial := prev.ReadyNodes > 0 && prev.ReadyNodes < prev.CapableNodes
	if curPartial && !prevPartial {
		out = append(out, "proxy_partial")
	}
	return out
}
