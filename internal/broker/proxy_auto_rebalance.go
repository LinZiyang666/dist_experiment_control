package broker

import (
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// proxy_auto_rebalance.go — G7a #18: after a broker RETURNS (its broker_down alert clears while it is
// still a current voter), the leader auto-evaluates the __proxy__ home distribution and triggers ONE
// rebalance so a failover's "stickiness" does not skew the spread permanently. The mechanism reuses the
// operator `cluster rebalance proxy` machinery verbatim (rebalanceProxyHomes) — this file only adds the
// leader-local, debounced TRIGGER.
//
// Safety (KD-3b / invariant "black-hole bounded"): a rebalance MOVES a healthy proxy home, and rehome is
// break-before-make — a client that cached the old /sub host black-holes until it refetches. So auto-fire
// is (a) DEFAULT-OFF (opt-in via TETHER_AUTO_REBALANCE=on) until the client-refresh cadence is verified /
// make-before-break lands, (b) gated on a quiet cluster, and (c) spread apart by a long cooldown. The
// mechanism + `cluster rebalance proxy --dry-run` preview ship regardless.

const (
	// autoRebalanceReturnDwellTicks is how many observe ticks a returned broker must stay eligible-up
	// before an auto-rebalance fires — long enough for the outbound crash-rehome to settle and for a
	// transient flap to cancel (must exceed proxyRehomeDwell=3). At observeTickInterval=5s, 6 ≈ 30s.
	autoRebalanceReturnDwellTicks = 6
	// autoRebalanceCooldownTicks bounds how often auto-rebalance may fire — one migration black-holes
	// cached clients until they refetch /sub, so firings are spread well apart. 60 ≈ 5min at 5s/tick.
	autoRebalanceCooldownTicks = 60
)

// autoRebalanceArm is the leader-local, in-memory debounce state for #18. It is NOT replicated: it is a
// scheduling hint, fully re-derivable from the substrate, and reset on a leadership change (a new leader
// re-observes returns and re-arms). Access is confined to the single observe loop goroutine.
type autoRebalanceArm struct {
	pending  map[string]int // returned node -> observe ticks remaining before its return-dwell is satisfied
	cooldown int            // ticks remaining before another fire is permitted (0 = ready)
}

func newAutoRebalanceArm() *autoRebalanceArm {
	return &autoRebalanceArm{pending: map[string]int{}}
}

// reset drops all armed state. Called on a leadership-lost edge so a demoted leader does not carry a
// stale pending set into a future term.
func (a *autoRebalanceArm) reset() {
	a.pending = map[string]int{}
	a.cooldown = 0
}

// tick advances the arm one observe tick and reports whether to FIRE an auto-rebalance now. Pure — no IO,
// so the debounce is adversarially table-testable without a DB/raft harness.
//
//	returned   : nodes whose broker_down cleared this tick (down→up edge), still current voters
//	stillUp    : predicate — is this armed node still eligible-up this tick? A node that flaps back down
//	             (or loses eligibility) is DROPPED, cancelling its dwell (no fire on a flapping broker).
//	gatesClear : all fire-gates satisfied THIS tick (downNow empty ∧ no in-flight op ∧ !force-single ∧
//	             no recent proxy rehome). Checked at fire time, so a mid-dwell gate change DEFERS the fire
//	             (the dwell-satisfied node stays armed) rather than cancelling it.
//
// A fire clears the whole pending set (one rebalance re-spreads everything) and enters cooldown.
func (a *autoRebalanceArm) tick(returned []string, stillUp func(string) bool, gatesClear bool) bool {
	if a.pending == nil {
		a.pending = map[string]int{} // zero-value arm (a Broker struct field) is usable without newAutoRebalanceArm
	}
	if a.cooldown > 0 {
		a.cooldown--
	}
	for _, n := range returned {
		if _, ok := a.pending[n]; !ok {
			a.pending[n] = autoRebalanceReturnDwellTicks
		}
	}
	ready := false
	for n, left := range a.pending {
		if !stillUp(n) {
			delete(a.pending, n) // flapped back down / lost eligibility → cancel its dwell
			continue
		}
		if left <= 1 {
			ready = true
			a.pending[n] = 0 // dwell satisfied; hold armed until gates clear (defer, not cancel)
		} else {
			a.pending[n] = left - 1
		}
	}
	if ready && gatesClear && a.cooldown == 0 {
		a.pending = map[string]int{}
		a.cooldown = autoRebalanceCooldownTicks
		return true
	}
	return false
}

// autoRebalanceQuietWindow: a __proxy__ home rehomed within this window means the crash-failover reaper
// is still settling the data plane — hold off so the two movers do not fight.
const autoRebalanceQuietWindow = 60 * time.Second

// autoRebalanceEnabled reports the operator opt-in. #18 auto-fire is DEFAULT-OFF (a rebalance moves a
// healthy proxy home and rehome is break-before-make, so a client that cached the old /sub host
// black-holes until it refetches); the mechanism + `cluster rebalance proxy --dry-run` preview ship
// regardless, and an operator enables auto-fire with TETHER_AUTO_REBALANCE=on once their client refresh
// cadence is known. No broker.yaml key (keeps G7 off the deploy tier).
func autoRebalanceEnabled() bool { return os.Getenv("TETHER_AUTO_REBALANCE") == "on" }

// driveAutoRebalanceOnReturn is the leader-local #18 trigger, called once per observe tick with THIS
// tick's broker_down edges. It advances the debounce arm and, on a fire with all gates clear, runs ONE
// rebalanceProxyHomes pass — the exact operator `cluster rebalance proxy` machinery (only __proxy__
// homes move; regular/rebuild-OFF exposes are structurally unmovable). Best-effort; a non-leader no-ops
// inside rebalanceProxyHomes.
func (b *Broker) driveAutoRebalanceOnReturn(returned, downNow []string) {
	if !autoRebalanceEnabled() {
		return // mechanism inert until opted in (black-hole-bounded invariant, KD-3b)
	}
	// A10: gatesClear drives ONLY the fire decision, which can only happen when a node's dwell is satisfied
	// (the arm has pending work). In the common steady state (no returns, empty pending) skip the three DB
	// reads entirely — tick never consults gatesClear then, so a false value is indistinguishable.
	gatesClear := false
	if len(returned) > 0 || len(b.autoRebalanceArm.pending) > 0 {
		gatesClear = len(downNow) == 0 && !forceSingleActive(b.cfg.DB) && b.noInflightOps() && !b.recentProxyRehome()
	}
	if !b.autoRebalanceArm.tick(returned, b.proxyHomeHealthy, gatesClear) {
		return
	}
	rep, err := b.rebalanceProxyHomes(false)
	if err != nil {
		b.cfg.Logger.Warn("broker: auto-rebalance on broker return", "err", err)
		return
	}
	if rep != nil && rep.Planned > 0 {
		// secret-free summary (scalar counts + the returned node ids); NOT a per-move splat.
		b.pubSysEvent("proxy_auto_rebalanced", map[string]any{
			"reason":   "broker_return",
			"returned": returned,
			"voters":   rep.Voters,
			"planned":  rep.Planned,
		})
	}
}

// noInflightOps reports that no cluster operation is mid-flight (a grow/retire/force-single in progress
// must not race an auto-rebalance).
func (b *Broker) noInflightOps() bool {
	ops, err := cluster.NonTerminalOperations(b.cfg.DB)
	return err == nil && len(ops) == 0
}

// recentProxyRehome reports that some __proxy__ home was reassigned within autoRebalanceQuietWindow. On a
// read error it returns true (conservative: skip the rebalance rather than fight an unknown data-plane
// state). Compares against the same cluster.LitTime(now.UTC()) format PlanReassignHome stamps.
func (b *Broker) recentProxyRehome() bool {
	cutoff := cluster.LitTime(b.cfg.Now().UTC().Add(-autoRebalanceQuietWindow))
	var n int
	if err := b.read().QueryRow(
		`SELECT COUNT(*) FROM port_allocations
		   WHERE name='__proxy__' AND state='ALLOCATED'
		     AND last_rehome_at IS NOT NULL AND last_rehome_at > ` + cutoff,
	).Scan(&n); err != nil {
		return true
	}
	return n > 0
}
