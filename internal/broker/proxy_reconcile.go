package broker

import (
	"database/sql"
	"slices"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// proxy_reconcile.go (C5) — the LEADER-GATED proxy data-plane reaper. It owns the per-node __proxy__
// allocation (the token-correct leader-local mint) + the home-down rehome via the D6 epoch ladder. It
// is the reason `proxy on` works in cluster mode (the control handlers only flip the switch) and the
// reason a dead proxy home auto-switches WITHOUT `proxy off/on`. Every write is a Raft Propose ⇒
// requires quorum; no quorum ⇒ no allocation/rehome (the proxy "briefly vanishes" frozen), NEVER a
// temp write-center. idle-zero-writes: it Proposes only on a real allocation/rehome transition.
//
// proxyRehomeDwell is the SUSTAINED-down threshold (in reaper ticks) before a rehome — distinct from
// the single-window alert raise — so a transient blip does not flap the proxy home.
const proxyRehomeDwell = 3

// driveProxyReconcile runs one reaper tick (leader-gated). No-op without a NATS conn / outside cluster.
func (b *Broker) driveProxyReconcile() {
	if !b.clusterMode || b.cl == nil || b.cl.node == nil || !b.cl.node.IsLeader() {
		return
	}
	nc := b.nc.Load()
	if nc == nil {
		return
	}
	sids, err := b.activeProxySessions()
	if err != nil {
		b.cfg.Logger.Warn("broker: proxy reaper list sessions", "err", err)
		return
	}
	for _, sid := range sids {
		b.reconcileProxySession(nc, sid)
	}
	// B1 (Stage-C BLOCKER): the OFF pass. A cluster `proxy off` only commits proxy_enabled=0 (the
	// control switch); this convergent pass performs the DATA-plane teardown the single-mode disableProxy
	// did inline — free the __proxy__ port (OpPortFree) + push Enabled:false so the agent stops its SS
	// server + drops the reverse tunnel (which closes the home broker's bound public listener). Covers a
	// proxy-off, a deleted/deleting session, and any orphaned __proxy__ row. idle-zero-writes once torn
	// down (PlanFree is a no-op on an already-FREED row; the disable push is harmless to a stopped agent).
	b.reconcileProxyTeardown(nc)
}

// reconcileProxyTeardown frees + disables every __proxy__ allocation whose session is no longer an
// ACTIVE proxy-enabled session (the B1 OFF/teardown convergence).
func (b *Broker) reconcileProxyTeardown(nc *nats.Conn) {
	rows, err := b.read().Query(`
		SELECT pa.sid, pa.nid, pa.port
		FROM port_allocations pa
		JOIN sessions s ON s.sid = pa.sid
		WHERE pa.name = '__proxy__' AND pa.state = 'ALLOCATED'
		  AND NOT (s.state = 'ACTIVE' AND s.proxy_enabled = 1)`)
	if err != nil {
		b.cfg.Logger.Warn("broker: proxy reaper teardown query", "err", err)
		return
	}
	type row struct {
		sid, nid string
		port     int
	}
	var stale []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.sid, &r.nid, &r.port); err != nil {
			_ = rows.Close()
			return
		}
		stale = append(stale, r)
	}
	_ = rows.Close()
	for _, r := range stale {
		// Push Enabled:false so the agent tears down its SS server + tunnel (the tunnel drop closes the
		// home broker's bound public listener). The epoch carries the post-off keyset bump → newer →
		// accepted by the agent's proxyNewer guard (epoch-only ordering in cluster mode).
		epoch, _ := session.GetProxyEpoch(b.cfg.DB, r.sid)
		b.pushProxyDirective(nc, r.sid, r.nid, &proto.ProxyDirective{Enabled: false, Epoch: epoch})
		// Close any LOCAL home listener immediately (if this broker is the home; harmless otherwise).
		if srv := b.tunnelSrv.Load(); srv != nil {
			srv.CloseProxy(r.port)
		}
		// Free the row via raft (idempotent OpPortFree; WHERE state='ALLOCATED' makes a re-run a no-op).
		freePort := r.port
		if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return port.PlanFree(db, freePort, b.cfg.Now())
		}); err != nil {
			b.cfg.Logger.Warn("broker: proxy teardown free", "sid", r.sid, "port", freePort, "err", err)
		}
		b.pubPortEvent(r.sid, r.port, port.ProxyPortName, r.nid, 0, "freed")
		// C6-EVT-3: drop this freed port's rehome change-mark + dwell so a future allocation re-announces
		// cleanly (no leaked sync.Map entries on a proxy off / session delete).
		b.clearRehomeMark(r.port)
		b.clearProxyDwell(r.port)
		b.clearProxyCountEvents(r.sid)
	}
}

func (b *Broker) activeProxySessions() ([]string, error) {
	rows, err := b.read().Query(`SELECT sid FROM sessions WHERE state='ACTIVE' AND proxy_enabled=1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, sid)
	}
	return out, rows.Err()
}

func (b *Broker) reconcileProxySession(nc *nats.Conn, sid string) {
	keys, err := b.activeProxyKeys(sid)
	if err != nil {
		return
	}
	epoch, _ := session.GetProxyEpoch(b.cfg.DB, sid)
	nids := b.onlineNIDs(sid)
	readyNodes := 0
	for _, nid := range nids {
		existing, lookErr := port.LookupProxyByNode(b.cfg.DB, sid, nid)
		if lookErr != nil || existing == nil {
			b.allocateAndPushProxy(nc, sid, nid, keys, epoch)
			continue
		}
		// Home unhealthy (drained/retired OR crashed-unreachable, M1) for proxyRehomeDwell consecutive
		// ticks (hysteresis) → rehome to a fresh deliverable VOTER.
		if !b.proxyHomeHealthy(existing.HomeBroker) {
			if b.bumpProxyDwell(existing.Port) >= proxyRehomeDwell {
				b.rehomeProxy(nc, sid, nid, existing, keys, epoch)
			}
			continue
		}
		// M3: home is healthy but the node has NEVER bound (lost token-bearing push / wiped agent
		// state.json — the cluster register reply is token-LESS for an existing row, so it can't recover
		// a tokenless agent). After proxyRehomeDwell ticks of !proxy_ready, ROTATE the allocation:
		// allocateAndPushProxy's free-then-insert mints a FRESH port+token and pushes a full token-
		// bearing directive, self-healing the strand. A still-converging rehome target also recovers here.
		if !b.nodeProxyReady(sid, nid) {
			if b.bumpProxyDwell(existing.Port) >= proxyRehomeDwell {
				// BD9: this is an ALLOCATE (epoch resets to 0), NOT a rehome — emit rehome_stalled
				// (change-keyed), never home_reassign_succeeded / a last_rehome_at stamp.
				b.emitRehomeOnce(existing.Port, "stalled:"+reasonTargetNotReady, evRehomeStalled, map[string]any{
					"kind": "proxy", "sid": sid, "nid": nid, "port": existing.Port,
					"home_broker": existing.HomeBroker, "epoch": existing.Epoch, "reason": reasonTargetNotReady,
				})
				b.clearProxyDwell(existing.Port)
				b.allocateAndPushProxy(nc, sid, nid, keys, epoch)
				b.clearRehomeMark(existing.Port) // C6-EVT-3: the old port is freed by the re-alloc — drop its leaked mark
				continue
			}
			b.pushProxyDirectiveWithHome(nc, sid, nid, existing, keys, epoch) // nudge while waiting
			continue
		}
		b.clearProxyDwell(existing.Port)
		b.clearRehomeMark(existing.Port) // healthy + ready → re-arm rehome event for a future down
		b.pushProxyDirectiveWithHome(nc, sid, nid, existing, keys, epoch)
		readyNodes++
	}
	// BD1: the C5 proxy observability events, wired LIVE here (the pure decideProxyEvents + its tests
	// existed in C5 but had no production caller). Change-keyed → idle-zero-writes.
	b.emitProxyCountEvents(sid, proxyEventCounts{Enabled: true, CapableNodes: len(nids), ReadyNodes: readyNodes})
}

// emitProxyCountEvents (BD1, C6-EVT-4) drives the C5 proxy observability events from decideProxyEvents
// (proxy_cluster.go) — the SINGLE source of truth (the pure fn + its tests) — instead of re-rolling it.
// It keeps the last counts per sid (proxyEvtCounts) and emits only the kinds decideProxyEvents returns
// for the prev→cur transition, so a steady condition is announced exactly once (idle-zero-writes).
func (b *Broker) emitProxyCountEvents(sid string, cur proxyEventCounts) {
	var prev proxyEventCounts
	if v, ok := b.proxyEvtCounts.Load(sid); ok {
		// Checked: a type mismatch leaves prev at its zero value, so the transition is announced as if
		// the sid were new. Losing one idle-suppression is the harmless direction; panicking inside a
		// reconcile pass is not.
		prev, _ = v.(proxyEventCounts)
	}
	for _, kind := range decideProxyEvents(prev, cur) {
		b.pubSysEvent(kind, map[string]any{"sid": sid, "ready": cur.ReadyNodes, "capable": cur.CapableNodes})
	}
	b.proxyEvtCounts.Store(sid, cur)
}

// clearProxyCountEvents forgets a session's last proxy counts (on teardown / session-rm, C6-EVT-3) so a
// later re-enable re-announces from a clean slate rather than suppressing on a stale prev.
func (b *Broker) clearProxyCountEvents(sid string) { b.proxyEvtCounts.Delete(sid) }

// proxyHomeHealthy reports whether a home_broker is currently a DELIVERABLE proxy home: an Eligible()
// VOTER with BOTH a usable cert AND a public host (N4: /sub gates on public_host, so the reaper/status
// must gate the same columns for render-equivalence), AND — for a remote home — currently REACHABLE per
// the §17 observe loop. M1: a CRASHED broker never mutates cluster_nodes.phase (only planned
// drain/retire does), so Eligible() alone would judge a dead home healthy forever and crash-failover
// would never fire; folding the reachability signal makes a sustained crash trip the rehome dwell.
func (b *Broker) proxyHomeHealthy(homeBroker string) bool {
	if homeBroker == "" {
		return false
	}
	home, err := clusternodes.LookupByNodeID(b.cfg.DB, homeBroker)
	if err != nil || !home.Eligible() || home.CertFP == "" || home.PublicHost == "" {
		return false
	}
	if homeBroker == b.selfID {
		return true // the leader is always reachable to itself
	}
	return b.homeReachable(homeBroker)
}

// homeReachable reads the leader's most-recent §17 per-peer reachability cache. A home not yet observed
// is treated as reachable (don't rehome before the first observe; the dwell + next tick catch a real
// down).
func (b *Broker) homeReachable(nodeID string) bool {
	b.lastObserveMu.Lock()
	defer b.lastObserveMu.Unlock()
	for _, p := range b.lastObserve {
		if p.NodeID == nodeID {
			return p.Reachable
		}
	}
	return true
}

// allocateAndPushProxy mints a fresh home-stamped __proxy__ allocation (leader-local, token-correct)
// and pushes the token-bearing directive with the resolved Home. Skips if no Eligible() home is
// available yet (the next tick / the agent's register retries — never an unsatisfiable directive).
func (b *Broker) allocateAndPushProxy(nc *nats.Conn, sid, nid string, keys []proto.ProxyKey, epoch int64) {
	home := b.resolveHomeForAgent(sid, nid)
	if home == nil {
		return
	}
	// B2 (Stage-C BLOCKER): PLAN inside the Propose closure on the live n.db (under applyMu), NOT on
	// the RODB cfg.DB handle ahead of time — findFreePort must read the committed, serialized view so
	// two concurrent allocations (e.g. a reaper proxy + a ctl expose) cannot bake the SAME port and
	// fail-stop the FSM at Apply on idx_port_alloc_unique_active. The raw token is leak-once-captured
	// from the closure (mirrors allocatePort, clusterwrite.go).
	var alloc *port.Allocation
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		a, cmd, e := port.PlanAllocateProxy(db, sid, nid, home.NodeID, b.cfg.PortAllocCfg())
		if e != nil {
			return nil, e
		}
		alloc = a
		return cmd, nil
	}); err != nil || alloc == nil {
		return
	}
	b.pubPortEvent(sid, alloc.Port, port.ProxyPortName, nid, 0, "allocated")
	b.pushProxyDirectiveWithHome(nc, sid, nid, alloc, keys, epoch)
}

// rehomeProxy re-points a __proxy__ home to a fresh Eligible() VOTER (load-spread) via the D6
// monotone-CAS OpPortReassignHome (single-winner on every replica) and re-pushes the Home directive.
func (b *Broker) rehomeProxy(nc *nats.Conn, sid, nid string, existing *port.Allocation, keys []proto.ProxyKey, epoch int64) {
	target := b.pickProxyRehomeTarget(existing.HomeBroker)
	if target == "" {
		// rehome_stalled{no_eligible_target} — no healthy VOTER to rehome to; stay frozen, loud via status.
		b.emitRehomeOnce(existing.Port, "stalled:"+reasonNoEligibleTarget, evRehomeStalled, map[string]any{
			"kind": "proxy", "sid": sid, "nid": nid, "port": existing.Port,
			"home_broker": existing.HomeBroker, "epoch": existing.Epoch, "reason": reasonNoEligibleTarget,
		})
		return
	}
	// C6-EVT-1: started + failed share ONE per-port mark so a SUSTAINED Propose failure does not flood
	// sys.events every ~5s tick (two distinct marks would perpetually invalidate each other). A fresh
	// attempt (the mark is neither this target's started nor failed) emits started once; the same
	// sustained attempt re-enters silently until it succeeds (clearRehomeMark re-arms) or the target
	// changes. HR2: PlanReassignHome returns the authoritative post-Apply epoch — use it, not the
	// re-lookup, in the succeeded event.
	startMark, failMark := "start:"+target, "fail:"+target
	prev, _ := b.rehomeEvt.Load(existing.Port)
	curMark, _ := prev.(string)
	if curMark != startMark && curMark != failMark {
		b.rehomeEvt.Store(existing.Port, startMark)
		curMark = startMark
		b.pubSysEvent(evHomeReassignStarted, map[string]any{
			"kind": "proxy", "sid": sid, "nid": nid, "port": existing.Port,
			"from_broker": existing.HomeBroker, "to_broker": target, "epoch": existing.Epoch,
		})
	}
	var newEpoch int64
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		ne, cmd, perr := port.PlanReassignHome(db, existing.Port, target, b.cfg.Now())
		newEpoch = ne
		return cmd, perr
	}); err != nil {
		if curMark != failMark {
			b.rehomeEvt.Store(existing.Port, failMark)
			b.pubSysEvent(evHomeReassignFailed, map[string]any{
				"kind": "proxy", "sid": sid, "port": existing.Port,
				"from_broker": existing.HomeBroker, "to_broker": target, "reason": classifyRehomeErr(err),
			})
		}
		return
	}
	b.clearProxyDwell(existing.Port)
	b.clearRehomeMark(existing.Port) // success → re-arm for a future down
	b.pubSysEvent("proxy_node_unready", map[string]any{"sid": sid, "nid": nid})
	b.pubSysEvent(evHomeReassignSucceeded, map[string]any{
		"kind": "proxy", "sid": sid, "nid": nid, "port": existing.Port,
		"from_broker": existing.HomeBroker, "to_broker": target, "epoch": newEpoch,
	})
	if upd, err := port.LookupProxyByNode(b.cfg.DB, sid, nid); err == nil && upd != nil {
		b.pushProxyDirectiveWithHome(nc, sid, nid, upd, keys, epoch)
	}
}

// pickProxyRehomeTarget chooses an Eligible() VOTER different from the current (failed) home, spreading
// load by the FEWEST current __proxy__ homes. "" if no healthy alternative exists.
func (b *Broker) pickProxyRehomeTarget(currentHome string) string {
	// A target must be a DELIVERABLE VOTER (cert + public_host, N4) and currently REACHABLE (M1: never
	// rehome onto a dead broker), excluding the failed current home.
	rows, err := b.read().Query(`SELECT node_id FROM cluster_nodes WHERE phase='VOTER' AND cert_fp != '' AND public_host != '' AND node_id != ? ORDER BY node_id`, currentHome)
	if err != nil {
		return ""
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return ""
		}
		ids = append(ids, id)
	}
	// A9: exclude a mid-drain broker (broker_draining marker up but phase still VOTER during DrainNode's
	// migrate window) as a rehome target, mirroring the allocatePort guard. Queried AFTER the rows above are
	// fully drained (single store connection); a read error → no-draining, proceed unfiltered.
	draining, _ := cluster.DrainingNodes(b.cfg.DB)
	best, bestLoad := "", int(^uint(0)>>1)
	for _, id := range ids {
		if slices.Contains(draining, id) {
			continue // A9: never rehome onto a draining broker
		}
		if id != b.selfID && !b.homeReachable(id) {
			continue // skip an unreachable target
		}
		var load int
		_ = b.read().QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE name='__proxy__' AND state='ALLOCATED' AND home_broker=?`, id).Scan(&load)
		if load < bestLoad {
			best, bestLoad = id, load
		}
	}
	return best
}

// pushProxyDirectiveWithHome pushes the keyset directive carrying the resolved Home (the rehome
// delivery backup; the register reply is the primary). A nil-home alloc pushes keys only.
func (b *Broker) pushProxyDirectiveWithHome(nc *nats.Conn, sid, nid string, alloc *port.Allocation, keys []proto.ProxyKey, epoch int64) {
	d := &proto.ProxyDirective{Enabled: true, Cipher: ssproxy.Cipher, Keys: keys, Epoch: epoch}
	if alloc != nil && alloc.Token != "" {
		d.PublicPort = alloc.Port
		d.Token = alloc.Token
	}
	d.Home = b.homeForExpose(alloc) // reuse the D6 HomeDirective builder (nil if home not deliverable)
	b.pushProxyDirective(nc, sid, nid, d)
}

// --- leader-local rehome dwell counter (hysteresis; reset on a leadership change is fine) ----------

func (b *Broker) bumpProxyDwell(p int) int {
	v, _ := b.proxyDwell.LoadOrStore(p, 0)
	cur, _ := v.(int) // checked: a mismatch restarts the dwell at 0, which only delays a rehome
	n := cur + 1
	b.proxyDwell.Store(p, n)
	return n
}
func (b *Broker) clearProxyDwell(p int) { b.proxyDwell.Delete(p) }
