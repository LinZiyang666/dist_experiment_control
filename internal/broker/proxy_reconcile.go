package broker

import (
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/backoff"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusternodes"
	"github.com/LinZiyang666/tether/internal/node"
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
	// stalledNow is the set of (sid/nid) this tick judges STALLED. The
	// authoritative `proxy_ready` observation retains an alert while the data
	// plane is down; a surviving rotation tracker retains it during the
	// 12-tick sustained-ready clear hysteresis.
	//
	// origin: h1 external review F2 (Major). The first version keyed off the
	// tracker, and trackers are leader-local memory: after ANY broker restart
	// they are empty while the persisted ACTIVE alert survives. A node that
	// was STILL unready then looked "not stalled" on the first tick and the
	// sweep cleared its alert — the data plane was demonstrably down
	// (`nodes.proxy_ready=0`) and we announced recovery. The absence of a
	// throttle is not evidence of health; the readiness flag is.
	//
	stalledNow := map[string]bool{}
	activeSIDs := make(map[string]bool, len(sids))
	for _, sid := range sids {
		activeSIDs[sid] = true
		b.reconcileProxySession(nc, sid)
		for _, nid := range b.onlineNIDs(sid) {
			key := proxyRotateKey(sid, nid)
			if proxyBindStalledThisTick(b, key, b.nodeProxyReady(sid, nid)) {
				stalledNow[key] = true
			}
		}
	}
	// h1 E2: level-triggered alert convergence (leader-restart-safe).
	sweepProxyBindAlerts(b, activeSIDs, stalledNow, func(key string) { clearProxyBindAlertByKey(b, key) })
	// B1 (Stage-C BLOCKER): the OFF pass. A cluster `proxy off` only commits proxy_enabled=0 (the
	// control switch); this convergent pass performs the DATA-plane teardown the single-mode disableProxy
	// did inline — free the __proxy__ port (OpPortFree) + push Enabled:false so the agent stops its SS
	// server + drops the reverse tunnel (which closes the home broker's bound public listener). Covers a
	// proxy-off, a deleted/deleting session, and any orphaned __proxy__ row. idle-zero-writes once torn
	// down (PlanFree is a no-op on an already-FREED row; the disable push is harmless to a stopped agent).
	b.reconcileProxyTeardown(nc)
}

// reconcileProxyTeardown frees + disables every __proxy__ allocation whose session is no longer an
// ACTIVE proxy-enabled session (the B1 OFF/teardown convergence) — or (#78) whose NODE registered
// as not proxy-capable (the opt-out fold): every allocate/repair/rotate gate skips a
// proxy_capable=0 node, so without this leg an allocation made BEFORE the opt-out would keep its
// public port forever — no gate would ever touch that row again (see
// staleProxyTeardownRows for the full argument).
// staleProxyRow is one __proxy__ allocation the teardown pass must free.
type staleProxyRow struct {
	sid, nid string
	port     int
}

// staleProxyTeardownRows selects every ALLOCATED __proxy__ row the teardown
// pass owns: the session is no longer ACTIVE+proxy-enabled, OR (#78) the node
// registered as not proxy-capable (the opt-out fold) — every allocate/repair/
// rotate gate skips a proxy_capable=0 node (the M3 rotation included: its only
// entry is inside the onlineNIDs(proxy_capable=1) loop), so without the second
// leg an allocation made BEFORE the opt-out would keep its public port forever
// — no gate would ever touch that row again (review N3 corrected an earlier
// claim that rotation would keep re-minting; it structurally cannot). Split
// from the teardown executor so the selection is hermetically testable without
// a raft harness.
// FREE function taking *Broker (the h1 E2 precedent): the Broker
// type-methods ratchet is deliberately not raised for plumbing.
func staleProxyTeardownRows(b *Broker) ([]staleProxyRow, error) {
	rows, err := b.read().Query(`
		SELECT pa.sid, pa.nid, pa.port
		FROM port_allocations pa
		JOIN sessions s ON s.sid = pa.sid
		WHERE pa.name = '__proxy__' AND pa.state = 'ALLOCATED'
		  AND (NOT (s.state = 'ACTIVE' AND s.proxy_enabled = 1)
		       OR EXISTS (SELECT 1 FROM nodes n
		                   WHERE n.sid = pa.sid AND n.nid = pa.nid AND n.proxy_capable = 0))`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var stale []staleProxyRow
	for rows.Next() {
		var r staleProxyRow
		if err := rows.Scan(&r.sid, &r.nid, &r.port); err != nil {
			return nil, err
		}
		stale = append(stale, r)
	}
	return stale, rows.Err()
}

func (b *Broker) reconcileProxyTeardown(nc *nats.Conn) {
	stale, err := staleProxyTeardownRows(b)
	if err != nil {
		b.cfg.Logger.Warn("broker: proxy reaper teardown query", "err", err)
		return
	}
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
			// review N4: the raft free did NOT commit — do NOT emit a `freed`
			// event nor drop the marks/dwell for a row still ALLOCATED (a
			// quorum-loss / leader-not-stepped-down window would repeat a false
			// `freed` every tick). The next tick re-does the full teardown once
			// the write can commit.
			b.cfg.Logger.Warn("broker: proxy teardown free", "sid", r.sid, "port", freePort, "err", err)
			continue
		}
		b.pubPortEvent(r.sid, r.port, port.ProxyPortName, r.nid, 0, "freed")
		// C6-EVT-3: drop this freed port's rehome change-mark + dwell so a future allocation re-announces
		// cleanly (no leaked sync.Map entries on a proxy off / session delete).
		b.clearRehomeMark(r.port)
		b.clearProxyDwell(r.port)
		b.clearProxyCountEvents(r.sid)
		// h1 E2 hygiene: a torn-down proxy takes its rotation damper and any
		// ACTIVE proxy_bind_stalled alert with it.
		b.proxyRotate.Delete(proxyRotateKey(r.sid, r.nid))
		b.proxyReadyTicks.Delete(proxyRotateKey(r.sid, r.nid))
		clearProxyBindAlertByKey(b, cluster.DedupKeyNode(cluster.AlertKindProxyBindStalled, r.sid+"/"+r.nid))
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
		//
		// h1 E2: the rotation is DAMPED. Undamped, this branch rotated every
		// 4th 5s-tick (20s) forever against a node whose network was simply
		// broken — the 2026-08-04 incident minted ~4k FREED rows/day for five
		// weeks. Rotation now consults a per-(sid,nid) backoff tracker
		// (20s→40s→…→10min); during a hold the directive nudge continues (the
		// proxy stays ON — the user's hard constraint), and the 3rd
		// consecutive rotation raises the info-severity proxy_bind_stalled
		// alert so the condition is VISIBLE instead of a silent row leak.
		if !b.nodeProxyReady(sid, nid) {
			proxyRotateUnready(b, sid, nid)
			if b.bumpProxyDwell(existing.Port) >= proxyRehomeDwell {
				if !proxyRotateDue(b, sid, nid) {
					b.pushProxyDirectiveWithHome(nc, sid, nid, existing, keys, epoch) // nudge during the hold
					continue
				}
				// BD9: this is an ALLOCATE (epoch resets to 0), NOT a rehome — emit rehome_stalled
				// (change-keyed), never home_reassign_succeeded / a last_rehome_at stamp.
				b.emitRehomeOnce(existing.Port, "stalled:"+reasonTargetNotReady, evRehomeStalled, map[string]any{
					"kind": "proxy", "sid": sid, "nid": nid, "port": existing.Port,
					"home_broker": existing.HomeBroker, "epoch": existing.Epoch, "reason": reasonTargetNotReady,
				})
				b.clearProxyDwell(existing.Port)
				b.allocateAndPushProxy(nc, sid, nid, keys, epoch)
				b.clearRehomeMark(existing.Port) // C6-EVT-3: the old port is freed by the re-alloc — drop its leaked mark
				proxyRotateFailed(b, sid, nid)
				continue
			}
			b.pushProxyDirectiveWithHome(nc, sid, nid, existing, keys, epoch) // nudge while waiting
			continue
		}
		b.clearProxyDwell(existing.Port)
		b.clearRehomeMark(existing.Port) // healthy + ready → re-arm rehome event for a future down
		proxyRotateReady(b, sid, nid)
		b.pushProxyDirectiveWithHome(nc, sid, nid, existing, keys, epoch)
		readyNodes++
	}
	// BD1: the C5 proxy observability events, wired LIVE here (the pure decideProxyEvents + its tests
	// existed in C5 but had no production caller). Change-keyed → idle-zero-writes.
	b.emitProxyCountEvents(sid, proxyEventCounts{Enabled: true, CapableNodes: len(nids), ReadyNodes: readyNodes})
}

// ---- h1 E2: M3 rotation damping + proxy_bind_stalled alert ------------------
// origin: docs/reviews/h1-plan.md workstream E2 (2026-08-04 incident).
// FREE functions taking *Broker on purpose (plan X-2: the increment's
// type-methods hand-raise budget is spent elsewhere).

// proxyRotatePolicy is the M3 rotation schedule: first re-rotate after 20s
// (the pre-h1 cadence — a genuinely transient bind failure still self-heals
// as fast as before), doubling to a 10min ceiling for a persistently-dead
// link (≤144 leaked FREED rows/day worst case vs the incident's ~4k).
var proxyRotatePolicy = backoff.Policy{Base: 20 * time.Second, Cap: 10 * time.Minute}

// proxyBindAlertAfter is the consecutive-rotation count that raises the
// proxy_bind_stalled alert (~2min of failing binds at the base cadence).
const proxyBindAlertAfter = 3

// proxyBindClearTicks is the clear-side hysteresis (plan critique-4): the
// alert clears and the tracker drops only after this many CONSECUTIVE ready
// reaper ticks (12 × 5s = 60s sustained). A shorter ready blip merely DECAYS
// the rotation backoff one step — the incident's own link "intermittently
// healed", and a reset-on-first-ready would flap the alert and restart the
// 20s rotation churn on every blip.
const proxyBindClearTicks = 12

func proxyRotateKey(sid, nid string) string { return sid + "/" + nid }

// proxyRotateDue reports whether the damper allows a rotation for (sid,nid)
// this tick. A node with no tracker (never rotated, or recovered) is due.
func proxyRotateDue(b *Broker, sid, nid string) bool {
	tr, ok := loadProxyTracker(b, proxyRotateKey(sid, nid))
	if !ok {
		return true
	}
	return tr.Due(b.cfg.Now())
}

// loadProxyTracker is the ONE checked read of the proxyRotate map. A checked
// assertion (not a bare one) on a value only this file ever stores: the cost
// is a bool, and the alternative is a panic inside a reconcile pass if some
// future code stores a different type — the same reasoning emitProxyCountEvents
// records for proxyEvtCounts.
func loadProxyTracker(b *Broker, key string) (*backoff.Tracker, bool) {
	v, ok := b.proxyRotate.Load(key)
	if !ok {
		return nil, false
	}
	tr, ok := v.(*backoff.Tracker)
	return tr, ok
}

// loadProxyReadyTicks is the same checked read for the consecutive-ready
// counter; a type mismatch reads as "no ticks yet", which merely restarts the
// clear hysteresis.
func loadProxyReadyTicks(b *Broker, key string) int {
	v, ok := b.proxyReadyTicks.Load(key)
	if !ok {
		return 0
	}
	n, _ := v.(int)
	return n
}

// proxyBindStalledThisTick combines the committed readiness observation with
// the leader-local recovery hysteresis. A ready node remains stalled until
// proxyRotateReady has observed the full sustained-ready run and removed its
// tracker; an unready node is stalled even just after a leader restart, when
// no tracker exists yet.
func proxyBindStalledThisTick(b *Broker, key string, ready bool) bool {
	if !ready {
		return true
	}
	_, trackingRecovery := loadProxyTracker(b, key)
	return trackingRecovery
}

// proxyRotateFailed records one performed rotation (= one failed bind cycle),
// schedules the next, and raises the info-severity proxy_bind_stalled alert
// at the proxyBindAlertAfter-th consecutive rotation. The already-ACTIVE gate
// lives INSIDE the Propose plan closure (the planAlertSignal shape,
// alert_forward.go): plan returns a nil command while ACTIVE → zero raft
// entries on re-assertion, no gate→Propose race, and the read rides the
// leader closure db (cfgdb-ratchet-neutral).
func proxyRotateFailed(b *Broker, sid, nid string) {
	key := proxyRotateKey(sid, nid)
	b.proxyRotate.LoadOrStore(key, backoff.New(proxyRotatePolicy))
	tr, ok := loadProxyTracker(b, key)
	if !ok {
		return // unreachable: only this file stores here (checked, not asserted)
	}
	if tr.Fail(b.cfg.Now(), "bind") {
		b.cfg.Logger.Warn("broker: proxy bind rotation entering backoff",
			"sid", sid, "nid", nid, "policy_base", proxyRotatePolicy.Base, "policy_cap", proxyRotatePolicy.Cap)
	}
	b.proxyReadyTicks.Delete(key)
	// LEVEL-triggered, not edge-triggered (internal review, concurrency lens):
	// `== proxyBindAlertAfter` would fire the raise on exactly ONE rotation,
	// and proposeProxyBindAlert's error path only logs — a single failed
	// Propose (leadership blip, no quorum) would then silence the alert for
	// the ENTIRE remaining stall. Re-asserting on every subsequent rotation
	// costs nothing while ACTIVE: the transition gate lives inside the plan
	// closure, so an already-ACTIVE alert plans a nil command and writes no
	// raft entry at all (the planAlertSignal shape).
	if tr.Fails() >= proxyBindAlertAfter {
		proposeProxyBindAlert(b, sid, nid, true)
	}
}

// proxyRotateUnready handles the unready tick housekeeping: a ready blip that
// did NOT reach the clear hysteresis decays the rotation backoff one step
// instead of resetting it (plan critique-4), then the consecutive-ready count
// restarts from zero.
func proxyRotateUnready(b *Broker, sid, nid string) {
	key := proxyRotateKey(sid, nid)
	if loadProxyReadyTicks(b, key) > 0 {
		if tr, ok := loadProxyTracker(b, key); ok {
			tr.Decay()
		}
		b.proxyReadyTicks.Store(key, 0)
	}
}

// proxyRotateReady counts consecutive ready ticks for a node with a rotation
// tracker; at proxyBindClearTicks the run is declared healed — the tracker is
// dropped (a future failure starts at the 20s base again) and the alert is
// cleared through raft. Nodes without a tracker cost nothing here (no map
// churn on the healthy path).
func proxyRotateReady(b *Broker, sid, nid string) {
	key := proxyRotateKey(sid, nid)
	tr, ok := loadProxyTracker(b, key)
	if !ok {
		return
	}
	n := loadProxyReadyTicks(b, key) + 1
	if n < proxyBindClearTicks {
		b.proxyReadyTicks.Store(key, n)
		return
	}
	if suppressed, ok := tr.Recover(b.cfg.Now()); ok {
		b.cfg.Logger.Info("broker: proxy bind recovered",
			"sid", sid, "nid", nid, "suppressed_rotations", suppressed)
	}
	b.proxyRotate.Delete(key)
	b.proxyReadyTicks.Delete(key)
	proposeProxyBindAlert(b, sid, nid, false)
}

// proposeProxyBindAlert raises (raise=true) or clears the per-(sid,nid)
// proxy_bind_stalled alert through raft. Severity is ALWAYS info — h1 plan Q7:
// severe feeds the --ack-alerts friction gate on destructive commands, and one
// node's dead laptop link must not tax the whole fleet's operations.
// Idempotent by construction: the transition gate inside the closure returns a
// nil command when there is nothing to change (nil = no raft write).
func proposeProxyBindAlert(b *Broker, sid, nid string, raise bool) {
	if b.cl == nil || b.cl.node == nil {
		return
	}
	now := b.cfg.Now()
	key := cluster.DedupKeyNode(cluster.AlertKindProxyBindStalled, sid+"/"+nid)
	err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		active, aerr := cluster.IsAlertActive(db, key)
		if aerr != nil {
			return nil, aerr
		}
		switch {
		case raise && !active:
			return cluster.PlanAlertRaise(newAlertID(key, now),
				cluster.AlertKindProxyBindStalled, cluster.AlertSeverityInfo, key,
				"proxy tunnel bind keeps failing on "+sid+"/"+nid+"; rotation is in backoff (agent-side dial problem — check that node's network path to the broker tunnel port)",
				now)
		case !raise && active:
			return cluster.PlanAlertClear(key, now)
		default:
			return nil, nil
		}
	})
	if err != nil {
		b.cfg.Logger.Warn("broker: proxy_bind_stalled alert propose", "err", err, "sid", sid, "nid", nid, "raise", raise)
	}
}

// sweepProxyBindAlerts is the LEVEL-triggered convergence for the
// proxy_bind_stalled alert. It answers one question per ACTIVE alert — "is
// this node still stalled RIGHT NOW, on this tick's own judgement?" — and
// clears when the answer is no.
//
// origin: internal review (raft + concurrency lenses). The first version
// only cleared alerts whose SESSION was gone, and delegated the healed-node
// clear to proxyRotateReady's 12-tick counter — which needs the leader-local
// in-memory tracker. Trackers die with the process, so after ANY broker
// restart (the next upgrade hop!) an alert for a node that healed in the
// meantime could never clear: no tracker → proxyRotateReady returns at its
// first line → the row stays ACTIVE forever, and `tether alert ls` reports a
// stalled proxy that is healthy. Node eviction had the same shape.
//
// stalledNow is the caller's set of (sid/nid) keys judged stalled this tick:
// either the committed bind ACK is unready, or a live rotation tracker is
// preserving the sustained-ready clear hysteresis.
// Not in that set ⇒ nothing on this broker currently believes the node is
// stalled ⇒ clear. That covers all three lost cases at once: healed node
// after a restart, evicted/departed node, and session gone. An OFFLINE node
// of an active session keeps its alert (an unreachable agent IS a stalled
// proxy) because onlineNIDs excludes it — so its key is absent from
// stalledNow, which would clear it. Hence the explicit OFFLINE guard below.
// clear is the commit half, lifted out as a parameter for the same reason
// gcDrain lifts propose: the DECISION (which alerts are no longer stalled) is
// what regressed and what needs pinning, and driving it through a real raft
// node just to observe a boolean would test the wrong thing.
func sweepProxyBindAlerts(b *Broker, activeSIDs map[string]bool, stalledNow map[string]bool, clear func(dedupKey string)) {
	alerts, err := cluster.ActiveAlerts(b.read().SQL())
	if err != nil {
		return
	}
	for _, a := range alerts {
		if a.Kind != cluster.AlertKindProxyBindStalled {
			continue
		}
		rest, ok := strings.CutPrefix(a.DedupKey, cluster.AlertKindProxyBindStalled+":")
		sid, nid, splitOK := strings.Cut(rest, "/")
		if !ok || !splitOK || sid == "" || nid == "" {
			b.cfg.Logger.Warn("broker: clearing malformed proxy_bind_stalled alert", "dedup_key", a.DedupKey)
			clear(a.DedupKey)
			continue
		}
		key := proxyRotateKey(sid, nid)
		if stalledNow[key] {
			continue // still stalled on this tick's own judgement
		}
		// Not judged stalled by the caller this tick — but before clearing,
		// RE-VERIFY against the authoritative per-node health.
		//
		// origin: h1 external review F2 (Major). This function must be correct
		// IN ISOLATION, because its most dangerous caller state is a
		// just-restarted leader whose in-memory view is empty: trusting the
		// caller's set alone made "no throttle exists" read as "the node
		// recovered", and a node with proxy_ready=0 — the data plane provably
		// still down — had its alert cleared. The readiness flag is the
		// evidence; the caller's set is only a fast path.
		if activeSIDs[sid] && b.proxySubjectStillUnhealthy(sid, nid) {
			continue
		}
		clear(a.DedupKey)
		b.proxyRotate.Delete(key)
		b.proxyReadyTicks.Delete(key)
	}
}

// proxySubjectStillUnhealthy reports whether (sid,nid) is STILL a stalled
// proxy on the authoritative committed view — the sweep's own evidence, held
// independently of any in-memory throttle state (h1 external review F2).
//
// A node is still unhealthy when its row exists AND it is either not ONLINE
// (an unreachable agent is a stalled proxy) or ONLINE with proxy_ready=0 (the
// agent has not ACKed its SS bind — the data plane is down). A MISSING row is
// not unhealthy: the node was evicted or never existed, so the alert's
// subject is gone and it must clear.
func (b *Broker) proxySubjectStillUnhealthy(sid, nid string) bool {
	st, err := node.LookupStatus(b.read().SQL(), sid, nid)
	switch {
	case errors.Is(err, node.ErrNotFound):
		return false // evicted / never existed: the SUBJECT is gone, clear it
	case err != nil:
		return true // unreadable DB → keep the alert (never lose signal on a transient)
	}
	if st != node.StateOnline {
		return true
	}
	return !b.nodeProxyReady(sid, nid)
}

// clearProxyBindAlertByKey proposes a clear for one dedup key with the same
// in-closure transition gate as proposeProxyBindAlert.
func clearProxyBindAlertByKey(b *Broker, key string) {
	if b.cl == nil || b.cl.node == nil {
		return
	}
	now := b.cfg.Now()
	_ = b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		active, aerr := cluster.IsAlertActive(db, key)
		if aerr != nil || !active {
			return nil, aerr
		}
		return cluster.PlanAlertClear(key, now)
	})
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
