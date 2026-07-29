package natsconf

import "strings"

// topostate.go (batch C / C3) — the SINGLE classifier that turns one broker's topology reconcile
// self-report into an operator-facing verdict.
//
// WHY THIS EXISTS
// ---------------
// "Is this broker's NATS topology converged?" had FOUR implementations with FOUR different
// predicates, and the two that classify a FAILURE did it by substring-matching the free-text
// Reason — with DIFFERENT substring sets on the two ends:
//
//	broker  internal/broker/clusterstatus.go computeHealth : "unrecognized directive" / "nats-server -t" / "render"
//	ctl     cmd/tether/cluster.go topoCell                  : "unrecognized directive" / "nats-server -t"      <- no "render"
//
// Three defects followed, all of them reproduced before this file was written:
//
//	(a) computeHealth's STUCK test was NESTED inside `observed < desired`. ActionUnknownDirective
//	    returns AppliedGen/ObservedGen UNCHANGED (reconcile.go, the Preflight branch), so a broker
//	    that converged and was THEN wedged — an operator hand-editing an `include` into nats.conf —
//	    has observed == desired, the substring test never runs, and the cluster reports HEALTHY_HA
//	    while that broker's reconciler is permanently fail-closed. topoCell, which tests the
//	    substrings FIRST, renders STUCK for the same state. The two ends disagreed on more than the
//	    substring set; they disagreed on the ORDER, and the order is what hid this one.
//
//	(b) ActionAwaitingClusteredCutover's Reason contains "cutover rendeRED + validated but WITHHELD".
//	    "rendered" contains "render", so the broker classified a DELIBERATE hold as STUCK and told
//	    the operator to "fix that broker's nats.conf, or run `tether cluster reconcile nats --manual`
//	    on it" — and --manual is exactly the action ReconcileOnce's withhold branch documents as
//	    forming a clustered-alone JS meta (G4 #10) or orphaning the standalone store (#4). The
//	    product was actively steering an operator into data damage. That is why TopoHeld carries a
//	    NEGATIVE clause in its next-step rather than merely a different verb.
//
//	(c) ActionRejected has THREE producers (render failure, `nats-server -t` failure, and Apply
//	    failure whose Reason is "apply: …"). The third matches NONE of the substrings on EITHER end,
//	    so an atomic-swap failure — full disk, unwritable .bak, cross-device rename — rendered as
//	    "still catching up" everywhere.
//
// So the fix is not "add the missing substring". A substring set maintained by convention across a
// package boundary WILL diverge again the next time a Reason is reworded. The Action is a closed
// enum the producer already emits; putting it on the wire and classifying it HERE, once, is what
// removes the mechanism rather than this instance of it.
//
// WHY IN THIS PACKAGE
// -------------------
// The Action constants' SSOT is reconcile.go, in this package. Measured internal import edges:
//
//	internal/natsconf  -> internal/auth        (its only one)
//	internal/adminsock -> (none)
//	internal/proto     -> (none)
//	internal/broker    -> already imports internal/natsconf
//	cmd/tether         -> already imports internal/natsconf
//
// Both consumers already depend on this package, so the shared classifier adds ZERO import edges
// and cannot form a cycle. adminsock and proto stay at zero internal imports — they each gain one
// `string` field and NOTHING else, because a zero-internal-import leaf is the exact shape the
// refactor roadmap's §7.1 says to protect (the criterion is the import surface, not the line count).
//
// This file imports only `strings`, so internal/natsconf's L-2 property (no nats, no raft, no
// broker), pinned by test/determinism's raft-confinement gate, is unaffected.

// TopoState is one broker's topology-reconcile verdict. The ordering of the constants is NOT
// significant; severity() is the authority for folding several nodes into one cluster verdict.
type TopoState int

const (
	// TopoUnreported: this node does not report topology (a pre-C3 broker), or nothing is desired
	// yet. Never degrades — the HEALTHY-HA gate must not punish a broker for being old.
	TopoUnreported TopoState = iota
	// TopoConverged: the LIVE nats-server confirmed loading the desired generation.
	TopoConverged
	// TopoBehind: not there yet, but a documented mechanism is still driving it. Waiting is correct.
	TopoBehind
	// TopoHeld: the standalone->clustered first-grow swap is rendered + validated but DELIBERATELY
	// withheld. The conf is fine; the reconciler is SIGHUP-only and cannot cross this transition.
	// It will NEVER self-heal, so it is not Behind — but it is not broken either, so it is not Stuck.
	TopoHeld
	// TopoStuck: the reconcile is wedged and will not self-heal. Needs an operator.
	TopoStuck
	// TopoUnknownAction: the peer reported an action this binary has no classification for — i.e.
	// the reader is OLDER than the writer. Fail-closed: say so rather than guess a polarity.
	TopoUnknownAction
	// topoStateCount is the closed-enum sentinel. Keep it last: AllTopoStates iterates to it, so adding
	// a state before the sentinel automatically expands every exhaustiveness table.
	topoStateCount
)

func (s TopoState) String() string {
	switch s {
	case TopoUnreported:
		return "unreported"
	case TopoConverged:
		return "converged"
	case TopoBehind:
		return "behind"
	case TopoHeld:
		return "held"
	case TopoStuck:
		return "stuck"
	case TopoUnknownAction:
		return "unknown"
	}
	return "unknown"
}

// Cell is the TOPO-column marker appended to "applied/observed→desired". "-" is the whole cell (an
// unreported node has no generations worth printing).
func (s TopoState) Cell() string {
	switch s {
	case TopoUnreported:
		return "-"
	case TopoConverged:
		return "✓"
	case TopoBehind:
		return "…"
	case TopoHeld:
		return "HOLD"
	case TopoStuck:
		return "STUCK"
	case TopoUnknownAction:
		return "?"
	}
	return "?"
}

// Degrades reports whether this state must keep the cluster out of HEALTHY_HA.
//
// TopoHeld degrades: applied/observed stay put across a withhold, so `observed < desired` already
// held before C3 and the verdict polarity is UNCHANGED by introducing the state. What changes is
// only the banner and the next-step — which is what keeps this refactor's blast radius small.
//
// TopoUnknownAction degrades on purpose: "I cannot classify what that broker said" is not evidence
// of health, and the alternative (silently folding it into Behind) is the false-green direction —
// it would tell an operator to wait for a self-heal this binary cannot even see.
func (s TopoState) Degrades() bool {
	switch s {
	case TopoUnreported, TopoConverged:
		return false
	}
	return true
}

// severity orders the states for WorstTopoState. Stuck needs hands NOW; Held needs one specific
// command; UnknownAction means "do not trust my reading"; Behind only needs patience.
func (s TopoState) severity() int {
	switch s {
	case TopoStuck:
		return 5
	case TopoHeld:
		return 4
	case TopoUnknownAction:
		return 3
	case TopoBehind:
		return 2
	case TopoConverged:
		return 1
	}
	return 0
}

// Banner is the cluster-level status banner for this state ("" when it does not degrade).
func (s TopoState) Banner() string {
	switch s {
	case TopoStuck:
		return "a broker's NATS topology reconcile is STUCK (see the TOPO column) — its nats.conf cannot be rendered/validated"
	case TopoHeld:
		return "a broker's standalone→clustered NATS cutover is deliberately WITHHELD (see the TOPO column) — its nats.conf is fine; the coordinated restart has not been run"
	case TopoBehind:
		return "a broker's NATS topology has not caught the desired generation yet (see the TOPO column)"
	case TopoUnknownAction:
		return "a broker reported a topology reconcile action this binary does not recognize (see the TOPO column) — it is probably running a NEWER release"
	}
	return ""
}

// NextStep is the operator's next command for this state ("" when it does not degrade).
func (s TopoState) NextStep() string {
	switch s {
	case TopoStuck:
		return "fix that broker's nats.conf, or run `tether cluster reconcile nats --manual` on it"
	case TopoHeld:
		// The NEGATIVE clause is the whole point: before C3 this state was classified STUCK, whose
		// next-step recommends `--manual` — the one action ReconcileOnce's withhold branch documents
		// as forming a clustered-alone JS meta (G4 #10) or orphaning the standalone store (#4).
		// Naming the wrong command explicitly is the runtime defence against that regression.
		return "run `tether cluster add <that-broker>` to perform the coordinated restart — do NOT run `tether cluster reconcile nats --manual` on it: that would apply a clustered conf under a running standalone nats-server (G4 #10/#4)"
	case TopoBehind:
		return "tether cluster reconcile nats --all --wait"
	case TopoUnknownAction:
		return "upgrade this ctl/broker to the fleet's release, then re-read `tether cluster status`"
	}
	return ""
}

// TopoCellLegend is the one-line key for the TOPO column, and it lives HERE rather than in the
// renderer for the same reason the classifier does.
//
// origin: batch-c internal review C3-F1. The legend was written when the column had three tokens and
// was not swept when HOLD and ? were added — so an operator looking at a HOLD cell found only one
// non-converged remedy in the key ("STUCK fix conf or `cluster reconcile nats --manual`") and
// followed it. On a broker holding a standalone→clustered cutover that command applies a clustered
// conf under a running standalone nats-server, which is exactly the data-damaging action
// TopoHeld.NextStep() spends a whole negated clause preventing. The legend silently cancelled it.
// Keeping the legend next to Cell() is what stops the next added state from repeating that.
func TopoCellLegend() string {
	return "TOPO=NATS topology applied/observed→desired gen (" +
		"✓ converged · … catching up · " +
		"HOLD cutover withheld: run `cluster add`, NOT `reconcile nats --manual` · " +
		"STUCK fix that conf, or `cluster reconcile nats --manual` · " +
		"? that broker reported an action this binary does not know — upgrade)"
}

// WorstTopoState folds per-node states into the one the cluster banner should report.
func WorstTopoState(states ...TopoState) TopoState {
	worst := TopoUnreported
	for _, s := range states {
		if s.severity() > worst.severity() {
			worst = s
		}
	}
	return worst
}

// AllActions returns every Action constant this package can emit. It exists so a guard test can
// assert ClassifyTopo is EXHAUSTIVE: adding an eighth Action without classifying it must redden a
// test rather than silently fall into TopoUnknownAction on a broker that does know the value.
func AllActions() []string {
	return []string{
		ActionNoop,
		ActionReloaded,
		ActionSwappedReloadPending,
		ActionRejected,
		ActionUnresolvable,
		ActionUnknownDirective,
		ActionAwaitingClusteredCutover,
	}
}

// legacyCutoverMarker identifies ActionAwaitingClusteredCutover from its Reason alone, for the
// mixed-version window where a pre-C3 broker sends no action.
//
// It MUST be tested before the render/apply markers below, and this ordering is load-bearing: the
// withhold Reason contains the word "rendered", which is why the pre-C3 broker-side matcher
// classified a deliberate hold as STUCK. Reproducing that bug inside the fallback would leave the
// mixed-version window behaving exactly as before the fix.
const legacyCutoverMarker = "cutover rendered + validated but WITHHELD"

// classifyLegacyReason is the SINGLE mixed-version fallback, shared by both ends. Before C3 each
// end had its own substring set and they disagreed; one implementation is the only shape in which
// they cannot drift apart again.
func classifyLegacyReason(reason string, observed, desired uint64) TopoState {
	switch {
	case strings.Contains(reason, legacyCutoverMarker):
		return TopoHeld
	case strings.Contains(reason, "unrecognized directive"),
		strings.Contains(reason, "nats-server -t"),
		strings.Contains(reason, "render ("),
		strings.HasPrefix(reason, "apply: "):
		// All four are ActionRejected/ActionUnknownDirective producers. Deliberately NOT gated on
		// observed<desired — defect (a) above.
		return TopoStuck
	case reason != "":
		// origin: batch-c internal review C3-F2. ANY non-empty reason means the pass did not simply
		// converge, so the honest legacy answer is Behind — never Converged.
		//
		// Falling through to the generation comparison here made the two paths DISAGREE on the same
		// broker state: swapped_reload_pending and unresolvable carry a reason but no failure marker,
		// and at observed>=desired the legacy path called them Converged while the action path calls
		// them Behind. A pre-batch-C broker whose conf is swapped but whose live server has not loaded
		// it would have read as fully converged — which is the false-green direction, and precisely the
		// class of disagreement this classifier exists to remove.
		//
		// The converged actions (noop/reloaded on a real convergence) emit an EMPTY reason, so they
		// still reach the generation comparison below.
		return TopoBehind
	case observed >= desired:
		return TopoConverged
	}
	return TopoBehind
}

// ClassifyTopo maps one broker's self-report to its verdict.
//
// action is proto.ClusterHealthResp.TopoAction / adminsock.ClusterNodeStatus.TopoAction; "" means
// the reporting broker predates C3 and the reason-substring fallback applies.
//
// STUCK and HELD are returned UNCONDITIONALLY — never gated on observed<desired. That gating is
// defect (a): a reconcile can be permanently wedged AT the converged generation, and the pre-C3
// broker-side predicate reported HEALTHY_HA for exactly that state.
func ClassifyTopo(action, reason string, observed, desired uint64, reported bool) TopoState {
	if !reported || desired == 0 {
		return TopoUnreported
	}
	switch action {
	case "":
		return classifyLegacyReason(reason, observed, desired)
	case ActionRejected, ActionUnknownDirective:
		return TopoStuck
	case ActionAwaitingClusteredCutover:
		return TopoHeld
	case ActionSwappedReloadPending, ActionUnresolvable:
		// Both have a documented driver still working: swapped_reload_pending escalates to the
		// staggered hard restart, and unresolvable is "a peer's identity has not replicated yet",
		// which the reconciler's own sys.event change-gate already treats as steady-state noise.
		return TopoBehind
	case ActionNoop, ActionReloaded:
		if observed >= desired {
			return TopoConverged
		}
		return TopoBehind
	}
	return TopoUnknownAction
}

// AllTopoStates returns every TopoState. It exists so a CONSUMER can be tested for exhaustiveness:
// the states are a closed set, and a consumer that silently drops one produces a verdict opposite to
// the one the classifier reached.
//
// origin: batch-c EXTERNAL review M2. `cluster doctor` collected only Stuck and Held and reported
// everything else as PASS — so a Behind or UnknownAction voter got "every reached voter's NATS
// topology reconcile is converging" printed underneath a DEGRADED health banner about that same
// voter. `reconcile nats --wait` had the mirror bug: it polled UnknownAction to the deadline and then
// exited EX_TEMPFAIL, telling automation to retry a condition no amount of retrying resolves.
func AllTopoStates() []TopoState {
	out := make([]TopoState, 0, int(topoStateCount))
	for state := TopoUnreported; state < topoStateCount; state++ {
		out = append(out, state)
	}
	return out
}
