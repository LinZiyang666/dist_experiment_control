package broker

import (
	"database/sql"
	"errors"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// proxy_cluster_wire.go (C5) — the cluster-mode proxy CONTROL handlers. They route every write through
// Raft (majority-write: no quorum ⇒ fail-closed), gated by the derived degraded state. The DATA plane
// (the per-node __proxy__ allocation + the token-bearing directive push) is the leader-gated reaper's
// job (proxy_reconcile.go) — these handlers only flip the replicated control state. They are broadcast
// + leader-only (Stage-C N0): a follower returns silent and only the leader proposes locally.

// proxyClusterStatusFor derives the proxy degraded state for one session from the live cluster
// condition + the session's replicated HA policy.
func (b *Broker) proxyClusterStatusFor(sid string) proxyClusterStatus {
	if !b.clusterMode {
		return proxyClusterState(false, false, false, "")
	}
	forceSingle := forceSingleActive(b.cfg.DB)
	stale := b.cl != nil && b.cl.node != nil && b.cl.node.LeaderContactStale(b.cfg.Now())
	policy, _ := session.GetProxyHAPolicy(b.cfg.DB, sid)
	return proxyClusterState(true, forceSingle, stale, policy)
}

// proxyVendable is the subhttp /sub read gate (C5). Single mode (and cluster ACTIVE / FROZEN_READONLY)
// vends; only DISABLED_NO_QUORUM returns false → /sub 404. Byte-identical in single mode (always true).
func (b *Broker) proxyVendable(sid string) bool {
	return b.proxyClusterStatusFor(sid).Vendable
}

func (b *Broker) proxyDegradedCode(st proxyClusterStatus) string {
	if st.State == proxyStateDisabledNoQuorum {
		return "proxy_disabled_no_quorum"
	}
	return "proxy_frozen_readonly"
}

// proxyProposeErr maps a leader-local Propose failure to a typed code. The proxy control writes are
// broadcast + leader-only (Stage-C N0 plan amendment, consistent with expose/run/kill), so a follower
// stays silent and only the leader proposes — a no-quorum minority ctl therefore TIMES OUT (identical
// to every other broadcast+leader-only write), and a leader that loses quorum mid-Propose surfaces the
// typed frozen/disabled code here. Anything else is a store error.
func (b *Broker) proxyProposeErr(msg *nats.Msg, sid, what string, err error) {
	if errors.Is(err, cluster.ErrForwardNotLeader) || cluster.IsNotLeader(err) {
		b.proxyErr(msg, b.proxyDegradedCode(b.proxyClusterStatusFor(sid)), what+": no committable quorum (lost the leader); retry")
		return
	}
	b.proxyErr(msg, "store_error", what+": "+err.Error())
}

// handleProxySetCluster routes the proxy master switch + HA policy through Raft (leader-local Propose;
// the handler is broadcast + leader-only so a follower already returned). The data-plane allocation +
// directive push is performed by the leader-gated reaper (which is token-correct).
func (b *Broker) handleProxySetCluster(sid, fp, actor, haPolicy string, enabled bool, msg *nats.Msg) {
	if b.isClusterFollower() {
		return // broadcast + leader-only: a follower stays silent, the leader answers
	}
	if haPolicy == "" {
		haPolicy, _ = session.GetProxyHAPolicy(b.cfg.DB, sid) // keep the current policy on a plain on/off
		if haPolicy == "" {
			haPolicy = session.ProxyHAFreeze
		}
	}
	if !session.ValidProxyHAPolicy(haPolicy) {
		b.proxyErr(msg, "ha_policy_invalid", "ha policy must be freeze-on-quorum-loss or disable-on-quorum-loss")
		return
	}
	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return session.PlanProxySetEnabled(sid, enabled, haPolicy, b.cfg.Now())
	}); err != nil {
		b.proxyProposeErr(msg, sid, "proxy set", err)
		return
	}
	action, ev := "proxy.off", "proxy_disabled"
	if enabled {
		action, ev = "proxy.on", "proxy_enabled"
	}
	b.pubSysEvent(ev, map[string]any{"sid": sid})
	b.pubAuditCall(sid, fp, actor, action, "", true, "", msg.Reply, nil)
	b.replyJSON(msg, proto.ProxySetResp{OK: true, Enabled: enabled, AffectedNodes: len(b.onlineNIDs(sid))})
}

// handleProxySubRevokeCluster routes a subscriber revoke through Raft (broadcast + leader-only — no
// secret returned).
func (b *Broker) handleProxySubRevokeCluster(sid, fp, actor, name string, msg *nats.Msg) {
	if b.isClusterFollower() {
		return
	}
	// #30: emit proxy_keyset_changed on a REAL revoke. The single-mode path surfaces this
	// operator-facing event (via pushCurrentKeyset), but this cluster path bumped the keyset epoch
	// through Raft and never emitted it — so an operator watching sys.events saw create/revoke go by
	// invisibly (drill 73's "cluster-mode revoke's ABSENT proxy_keyset_changed"). Capture the epoch
	// across the Propose: the change-gated bump only advances when a row actually flipped
	// ACTIVE→REVOKED, so an idempotent no-op re-revoke stays silent (matches single mode's
	// ErrAlreadyRevoked path — no spurious event). The read-back sees the committed value because
	// Propose applies locally before returning on the leader (same contract the create read-back uses).
	epochBefore, _ := session.GetProxyEpoch(b.cfg.DB, sid)
	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return proxysub.PlanRevoke(sid, name, b.cfg.Now())
	}); err != nil {
		b.proxyProposeErr(msg, sid, "proxy sub revoke", err)
		return
	}
	if epochAfter, err := session.GetProxyEpoch(b.cfg.DB, sid); err == nil && epochAfter > epochBefore {
		// Secret-free body ({sid} only), like every other pubSysEvent — no PSK/token ever.
		b.pubSysEvent("proxy_keyset_changed", map[string]any{"sid": sid})
	}
	b.pubAuditCall(sid, fp, actor, "proxy.sub.revoke", name, true, "", msg.Reply, nil)
	b.replyJSON(msg, proto.ProxySubRevokeResp{OK: true, Name: name})
}

// handleProxySubCreateCluster mints the subscriber on the LEADER (broadcast + leader-only), commits the
// hash + PSK via Raft, and returns the raw bearer token leak-once in the SubURL. A follower stays
// silent (the broadcast reaches the leader). The PSK travels the Raft log (C5 §D-SECRET); the raw
// token does NOT — it is captured here and never persisted.
func (b *Broker) handleProxySubCreateCluster(sid, fp, actor, name string, msg *nats.Msg) {
	if b.isClusterFollower() {
		return
	}
	rawToken, tokenHash, psk, subID, err := proxysub.MintSubscriber()
	if err != nil {
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "store_error"})
		return
	}
	// Leader-local Propose (a follower already returned), so no forward verb is needed. The guarded
	// INSERT no-ops on a duplicate ACTIVE (sid,name); detect that AFTER by re-reading.
	if err := b.cl.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return proxysub.PlanCreate(sid, subID, name, tokenHash, psk, fp, b.cfg.Now())
	}); err != nil {
		if errors.Is(err, cluster.ErrForwardNotLeader) || cluster.IsNotLeader(err) {
			b.replyJSON(msg, proto.ProxySubCreateResp{Code: b.proxyDegradedCode(b.proxyClusterStatusFor(sid))})
			return
		}
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "store_error"})
		return
	}
	// Confirm the committed row is OURS (the guard may have no-op'd a duplicate name).
	got, err := proxysub.LookupActiveByName(b.cfg.DB, sid, name)
	if err != nil || got == nil || got.TokenHash != tokenHash {
		b.replyJSON(msg, proto.ProxySubCreateResp{Code: "sub_name_taken", Name: name})
		return
	}
	// #30: a confirmed create bumped the change-gated keyset epoch → surface proxy_keyset_changed,
	// same as single mode (pushCurrentKeyset) and the sibling handleProxySetCluster (proxy_enabled/
	// disabled). Reaching here proves the INSERT took (a duplicate no-op returned above), so the
	// epoch definitely advanced. Body is secret-free ({sid} only) — the minted token/PSK never here.
	b.pubSysEvent("proxy_keyset_changed", map[string]any{"sid": sid})
	// THE SAME FENCE AS SINGLE MODE, through the SAME helper — origin: prerelease audit
	// external review R2-B1. This handler used to reply with the bearer URL unconditionally
	// while `proxySubCreate` next door withheld it, so the fence covered the deployment shape
	// with the SMALLER blast radius and missed the clustered one entirely.
	resp := proxySubCreateReply(b, sid, name, rawToken, msg.Reply)
	b.pubAuditCall(sid, fp, actor, "proxy.sub.create", name, true, resp.Code, msg.Reply, nil)
	b.replyJSON(msg, resp)
}
