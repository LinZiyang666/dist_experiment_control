package broker

import (
	"database/sql"
	"fmt"
	"slices"
	"sort"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// proxy_rebalance.go (C-rebalance) — the operator-triggered `cluster rebalance proxy` pass. The reaper
// (proxy_reconcile.go) only moves a __proxy__ home when its current home goes DOWN; nothing migrates a
// HEALTHY proxy off an over-loaded voter onto a freshly-added empty one. This leader-local pass evens
// out a lopsided distribution (e.g. after `cluster add` brings a new voter, or a drained broker rejoins)
// by reassigning homes from the most-loaded voter to the least-loaded until they differ by at most one.
//
// It reuses the SAME building blocks the reaper does — PlanReassignHome (the D6 monotone-CAS that bumps
// the home epoch, single-winner on every replica) + the home_reassign_* events + the Home directive
// re-push — so an agent re-establishes on the new home exactly as it does on a crash-failover. Every
// write is a Raft Propose ⇒ requires quorum + leadership; a non-leader never reaches here (the admin
// dispatch gates leadership before the switch).

// rebalanceProxyHomes plans + (unless dryRun) executes a single load-spreading pass over the per-node
// __proxy__ homes. It is wired into the cluster admin backend (broker.go) and invoked by the
// OpClusterRebalanceProxy admin verb.
func (b *Broker) rebalanceProxyHomes(dryRun bool) (*adminsock.ProxyRebalanceReport, error) {
	// Defensive re-check (the admin dispatch already gated leadership): rebalance MUST run on the leader
	// with a live NATS conn — it Proposes raft writes + re-pushes directives. (The CLI prefixes the verb,
	// so these messages stay bare.)
	if !b.clusterMode || b.cl == nil || b.cl.node == nil || !b.cl.node.IsLeader() {
		return nil, fmt.Errorf("not the leader; re-run on the leader host")
	}
	nc := b.nc.Load()
	if nc == nil {
		return nil, fmt.Errorf("no NATS connection (broker not ready)")
	}

	// 1. The eligible rebalance targets: deliverable (VOTER + cert + public_host, N4) AND currently
	//    reachable (M1) voters — the exact gate the reaper's pickProxyRehomeTarget applies, so a
	//    rebalance never lands a home on a broker the reaper would immediately rehome away from.
	voters := b.eligibleProxyHomes()
	rep := &adminsock.ProxyRebalanceReport{DryRun: dryRun, Voters: len(voters), Moves: []adminsock.ProxyRebalanceMove{}}
	if len(voters) < 2 {
		return rep, nil // nothing to spread across (single eligible home / none)
	}
	voterSet := make(map[string]bool, len(voters))
	for _, v := range voters {
		voterSet[v] = true
	}

	// 2. The MOVABLE proxies: ALLOCATED __proxy__ rows of ACTIVE proxy-enabled sessions whose current
	//    home is one of the eligible voters. A proxy whose home is NOT eligible (dead/draining) is left
	//    to the reaper's auto-rehome — rebalance only evens out the healthy distribution.
	allocs, err := b.movableProxyAllocs(voterSet)
	if err != nil {
		return nil, err
	}
	rep.Proxies = len(allocs)
	if len(allocs) == 0 {
		return rep, nil
	}

	// 3. Greedy balance plan (pure, so it is adversarially unit-testable without a DB/raft harness).
	plan := planProxyRebalance(voters, allocs)
	rep.Planned = len(plan)

	// 4. Materialize the report; execute unless dry-run. Per-sid keyset (keys + epoch) is gathered once
	//    and reused across that sid's moves (the directive carries the keyset epoch, not the home epoch).
	keysCache := make(map[string][]proto.ProxyKey)
	epochCache := make(map[string]int64)
	for _, m := range plan {
		entry := adminsock.ProxyRebalanceMove{SID: m.a.SID, NID: m.a.NID, Port: m.a.Port, From: m.from, To: m.to}
		if dryRun {
			rep.Moves = append(rep.Moves, entry)
			continue
		}
		keys, ok := keysCache[m.a.SID]
		if !ok {
			keys, _ = b.activeProxyKeys(m.a.SID)
			keysCache[m.a.SID] = keys
			ep, _ := session.GetProxyEpoch(b.cfg.DB, m.a.SID)
			epochCache[m.a.SID] = ep
		}
		err := b.moveProxyHomeTo(nc, m.a, m.to, keys, epochCache[m.a.SID])
		if err != nil {
			entry.Error = err.Error()
			rep.Moves = append(rep.Moves, entry)
			// Classify the failure (R1 MAJOR): a LEADERSHIP loss / quorum transition fails every remaining
			// move the same way → stop the pass (the operator re-runs once HA is restored; rep.Planned >
			// len(rep.Moves) tells the CLI it stopped early). A BENIGN per-row race — the reaper freed or
			// rotated this port between the plan-snapshot and this Propose (port.ErrNotFound) — only affects
			// THIS proxy, so record it and keep going (do NOT abort the whole rebalance for one stale row).
			if cluster.IsNotLeader(err) {
				break
			}
			continue
		}
		entry.Done = true
		rep.Moves = append(rep.Moves, entry)
	}
	return rep, nil
}

// plannedMove is one decided home reassignment in a rebalance plan.
type plannedMove struct {
	a    *port.Allocation
	from string
	to   string
}

// planProxyRebalance is the PURE greedy planner: given the eligible voter homes and the movable proxy
// allocations (each already home==one-of-voters), it returns the moves that even out the load to a
// max−min ≤ 1 spread. Deterministic — homes are tie-broken by node_id (sorted). NOTE: only the LEADER
// runs the planner; it is NOT part of the FSM (followers just apply the per-port PlanReassignHome
// commands), so the sort buys unit-testability/stability, not cross-replica consensus. Bounded by
// len(allocs) iterations (belt-and-suspenders cap; correctness rests on the ≤1-gap break, which always
// fires first — each move shrinks the global gap and no proxy is ever moved twice).
func planProxyRebalance(voters []string, allocs []*port.Allocation) []plannedMove {
	if len(voters) < 2 || len(allocs) == 0 {
		return nil
	}
	load := make(map[string]int, len(voters))
	byHome := make(map[string][]*port.Allocation, len(voters))
	for _, v := range voters {
		load[v] = 0
	}
	for _, a := range allocs {
		if _, ok := load[a.HomeBroker]; !ok {
			continue // defensive: a non-eligible home slipped in — skip (the reaper owns it)
		}
		load[a.HomeBroker]++
		byHome[a.HomeBroker] = append(byHome[a.HomeBroker], a)
	}
	var plan []plannedMove
	for i := 0; i < len(allocs); i++ {
		hi, lo := maxLoadHome(load), minLoadHome(load)
		if hi == "" || lo == "" || load[hi]-load[lo] <= 1 {
			break
		}
		src := byHome[hi]
		mv := src[len(src)-1] // pop one proxy off the most-loaded home
		byHome[hi] = src[:len(src)-1]
		plan = append(plan, plannedMove{a: mv, from: hi, to: lo})
		load[hi]--
		load[lo]++
		byHome[lo] = append(byHome[lo], mv)
	}
	return plan
}

// moveProxyHomeTo reassigns ONE __proxy__ allocation's home to an EXPLICIT target (the rebalance
// counterpart of the reaper's failure-driven rehomeProxy). Emits the home_reassign_started/{succeeded,
// failed} lifecycle events (reason=rebalance) + re-pushes the Home directive so the agent re-establishes
// on the new home. Does NOT touch the reaper's per-port dwell/event marks — a rebalance is one-shot, and
// the next reaper tick re-arms them cleanly once the new (healthy) home is observed.
func (b *Broker) moveProxyHomeTo(nc *nats.Conn, existing *port.Allocation, target string, keys []proto.ProxyKey, epoch int64) error {
	b.pubSysEvent(evHomeReassignStarted, map[string]any{
		"kind": "proxy", "sid": existing.SID, "nid": existing.NID, "port": existing.Port,
		"from_broker": existing.HomeBroker, "to_broker": target, "epoch": existing.Epoch, "reason": "rebalance",
	})
	var newEpoch int64
	if err := b.cl.node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		ne, cmd, perr := port.PlanReassignHome(db, existing.Port, target, b.cfg.Now())
		newEpoch = ne
		return cmd, perr
	}); err != nil {
		b.pubSysEvent(evHomeReassignFailed, map[string]any{
			"kind": "proxy", "sid": existing.SID, "port": existing.Port,
			"from_broker": existing.HomeBroker, "to_broker": target, "reason": classifyRehomeErr(err),
		})
		return err
	}
	// Parity with rehomeProxy (R3): announce the transient unready before succeeded, so a readiness
	// consumer sees the same event stream for an operator rebalance as for a failure-driven rehome.
	b.pubSysEvent("proxy_node_unready", map[string]any{"sid": existing.SID, "nid": existing.NID})
	b.pubSysEvent(evHomeReassignSucceeded, map[string]any{
		"kind": "proxy", "sid": existing.SID, "nid": existing.NID, "port": existing.Port,
		"from_broker": existing.HomeBroker, "to_broker": target, "epoch": newEpoch, "reason": "rebalance",
	})
	// Re-push the Home directive immediately (the reaper would also catch this on its next tick, but the
	// inline push makes the switch prompt — mirrors rehomeProxy). Re-lookup to read the committed home.
	if upd, err := port.LookupProxyByNode(b.cfg.DB, existing.SID, existing.NID); err == nil && upd != nil {
		b.pushProxyDirectiveWithHome(nc, existing.SID, existing.NID, upd, keys, epoch)
	}
	return nil
}

// eligibleProxyHomes returns every DELIVERABLE + REACHABLE voter (the valid rebalance target set),
// sorted for determinism. Mirrors pickProxyRehomeTarget's gate but returns the whole set.
func (b *Broker) eligibleProxyHomes() []string {
	// A9: fetch the draining set FIRST (before opening the rows iterator — the store is single-connection,
	// so a nested query mid-iteration deadlocks). A DrainNode marks broker_draining and migrates exposes
	// BEFORE flipping VOTER->DRAINING, so a draining node is still phase=VOTER in this window and must be
	// excluded as a rebalance target (mirrors the allocatePort guard, clusterwrite.go:550). A read error →
	// treat as no-draining and proceed unfiltered rather than stall EVERY rehome on a transient error.
	draining, _ := cluster.DrainingNodes(b.cfg.DB)
	rows, err := b.read().Query(`SELECT node_id FROM cluster_nodes WHERE phase='VOTER' AND cert_fp != '' AND public_host != '' ORDER BY node_id`)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return out
		}
		if slices.Contains(draining, id) {
			continue // A9: never rebalance a proxy home onto a draining broker
		}
		// Self (the leader) is always reachable to itself; a remote home must be REACHABLE per §17.
		if id == b.selfID || b.homeReachable(id) {
			out = append(out, id)
		}
	}
	return out
}

// movableProxyAllocs lists the ALLOCATED __proxy__ rows of ACTIVE proxy-enabled sessions whose current
// home is in voterSet AND that are currently READY (R1 MINOR: only move STABLE proxies — moving one the
// reaper is mid-rotation on, !proxy_ready, would just be undone by the reaper's M3 path on its next
// tick). Returns minimal Allocations carrying the fields moveProxyHomeTo needs (Port/SID/NID/HomeBroker/
// Epoch). The skipped not-ready / dead-homed proxies are the reaper's job, not rebalance's.
func (b *Broker) movableProxyAllocs(voterSet map[string]bool) ([]*port.Allocation, error) {
	rows, err := b.read().Query(`
		SELECT pa.sid, pa.nid, pa.port, pa.home_broker, pa.epoch
		FROM port_allocations pa
		JOIN sessions s ON s.sid = pa.sid
		WHERE pa.name = '__proxy__' AND pa.state = 'ALLOCATED'
		  AND s.state = 'ACTIVE' AND s.proxy_enabled = 1
		ORDER BY pa.port`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*port.Allocation
	for rows.Next() {
		var a port.Allocation
		var home sql.NullString
		if err := rows.Scan(&a.SID, &a.NID, &a.Port, &home, &a.Epoch); err != nil {
			return nil, err
		}
		a.HomeBroker = home.String
		if a.HomeBroker == "" || !voterSet[a.HomeBroker] {
			continue // unhomed / dead-home proxy → the reaper's auto-rehome owns it, not rebalance
		}
		if !b.nodeProxyReady(a.SID, a.NID) {
			continue // mid-establishment / strand → leave it to the reaper (don't churn an unstable proxy)
		}
		ac := a
		out = append(out, &ac)
	}
	return out, rows.Err()
}

// maxLoadHome / minLoadHome return the home with the most / fewest proxies, ties broken by lowest
// node_id (the sorted iteration makes the greedy plan stable/reproducible — leader-local, see above).
func maxLoadHome(load map[string]int) string {
	ids := sortedHomeIDs(load)
	best, bestN := "", -1
	for _, id := range ids {
		if load[id] > bestN {
			best, bestN = id, load[id]
		}
	}
	return best
}

func minLoadHome(load map[string]int) string {
	ids := sortedHomeIDs(load)
	best, bestN := "", int(^uint(0)>>1)
	for _, id := range ids {
		if load[id] < bestN {
			best, bestN = id, load[id]
		}
	}
	return best
}

func sortedHomeIDs(load map[string]int) []string {
	ids := make([]string, 0, len(load))
	for id := range load {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
