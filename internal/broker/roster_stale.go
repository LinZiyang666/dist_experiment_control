package broker

import (
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// roster_stale.go (C1 §6) — the leader-side `agent_roster_stale` OBSERVABILITY event. An agent
// reports its cached roster generation in NodeRegisterReq.RosterGen; when that is sustained-behind
// the current cluster roster, the leader emits one scrubbed sys.event so an operator can see "this
// agent is not picking up the new topology". It is an EVENT (pubSysEvent, fire-and-forget), NOT a
// D8 replicated alert: there is no durable raise/clear, a per-(sid,nid) alert row would balloon the
// alerts table, and it self-resolves (a converged agent's next register has agentGen==gen → silent).
//
// It is leader-only for free: handleRegister early-returns on a follower (isClusterFollower), so this
// is only ever reached on the leader; in single mode rosterForRegister returns nil → roster==nil →
// this returns immediately → no event, NodeRegisterResp byte-identical to pre-C1.

// rosterStaleGrace is the sustained-lag window: an agent strictly behind the current generation is
// only flagged once the cluster's LAST membership change is older than this. It exceeds the agent's
// (jittered, ≤3min) refresh interval so a normally-converging agent never trips it; it is a
// "sustained lag" signal, not a transient post-add blip. (5min SLA + 1min margin.)
const rosterStaleGrace = 6 * time.Minute

// rosterStaleDedupCap bounds the per-(sid,nid) last-warned map so a large fleet cannot grow it
// without limit. On overflow an arbitrary entry is dropped (approximate, not strict LRU — bounded
// memory is the goal; an occasional re-warn after eviction is benign, and the grace window is the
// primary throttle anyway).
const rosterStaleDedupCap = 4096

// maybeEmitRosterStale emits at most one agent_roster_stale event per (sid,nid) per generation when a
// registering agent's cached roster generation is sustained-behind the current one. roster is the
// signed roster about to be returned (its Generation is the monotone counter, §D-2). agentGen is
// req.RosterGen (0 ⇒ a pre-C1 / cold agent adopting now → never flagged).
//
// The grace window is anchored on the broker-local DERIVED last-membership-change wall-clock
// (readRosterBrokers' second return), NEVER on the generation value — so the predicate is correct
// whether the generation is a monotone counter or (legacy) a unix-nano timestamp (critique fix).
func (b *Broker) maybeEmitRosterStale(sid, nid string, agentGen uint64, roster *proto.ClusterRoster) {
	if roster == nil || agentGen == 0 {
		return
	}
	cur := roster.Generation
	_, lastChangeNano, err := readRosterBrokers(b.cfg.DB)
	if err != nil {
		return
	}
	if !rosterStaleShouldEmit(agentGen, cur, int64(lastChangeNano), b.cfg.Now()) {
		return
	}
	if !b.rosterStaleClaim(sid, nid, cur) {
		return // already warned for this (sid,nid) at >= this generation
	}
	b.pubSysEvent("agent_roster_stale", rosterStaleFields(sid, nid, agentGen, cur))
}

// rosterStaleFields builds the SCRUBBED agent_roster_stale event body: only ids + generations, no
// token/PSK/cert_fp/nats_route. (sid/nid already appear in the agent_registered event on this same
// path.) Extracted so a test can pin the exact field set against accidental secret leakage.
func rosterStaleFields(sid, nid string, agentGen, cur uint64) map[string]any {
	return map[string]any{
		"sid":         sid,
		"nid":         nid,
		"agent_gen":   agentGen,
		"current_gen": cur,
	}
}

// rosterStaleShouldEmit is the PURE staleness predicate (c1-plan.md §6), separated for testing. An
// agent is flagged iff it reports a non-zero generation (a cold/pre-C1 agent reporting 0 is adopting
// now — never flagged), is STRICTLY behind the current monotone counter, AND the cluster's last
// membership change is older than the grace window. The grace is anchored on a WALL-CLOCK
// (lastChangeNano = the broker-local derived last-change time), NEVER on the generation value — so it
// is correct whether the generation is a monotone counter or a (legacy) unix-nano timestamp.
func rosterStaleShouldEmit(agentGen, currentGen uint64, lastChangeNano int64, now time.Time) bool {
	if agentGen == 0 || agentGen >= currentGen {
		return false
	}
	return now.Sub(time.Unix(0, lastChangeNano)) > rosterStaleGrace
}

// rosterStaleClaim returns true iff (sid,nid) has not yet been warned at >= gen, recording gen as the
// new high-water mark. Bounded + mutex-guarded (concurrent agent registers). The map is created on
// first use so a directly-constructed Broker (tests) needs no New() wiring.
func (b *Broker) rosterStaleClaim(sid, nid string, gen uint64) bool {
	b.rosterStaleMu.Lock()
	defer b.rosterStaleMu.Unlock()
	if b.rosterStaleWarned == nil {
		b.rosterStaleWarned = make(map[string]uint64)
	}
	key := sid + "\x00" + nid
	if last, ok := b.rosterStaleWarned[key]; ok && last >= gen {
		return false
	}
	if len(b.rosterStaleWarned) >= rosterStaleDedupCap {
		for k := range b.rosterStaleWarned { // approximate eviction: drop one arbitrary entry
			delete(b.rosterStaleWarned, k)
			break
		}
	}
	b.rosterStaleWarned[key] = gen
	return true
}
