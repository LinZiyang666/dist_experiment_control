package broker

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// home_convergence.go (R8a) — the rc-semantics gate shared by drain / retire /
// upgrade.
//
// THE CONTRACT
// ------------
//	A verb that mutated the control plane but whose data plane has NOT converged
//	MUST NOT return rc=0.
//
// `cluster drain` used to violate this in the loudest possible way: it wrote
// port_allocations.home_broker through raft, notified nobody, and printed
// "drain <node> ok". The operator's script saw success; the tunnel stayed pinned to
// the drained broker until something unrelated made the agent reconnect.
//
// WHAT COUNTS AS "CONVERGED"
// --------------------------
// Only an AGENT-CONFIRMED applied home epoch. Specifically NOT:
//   - "the row says home_broker=X"  — that is the control-plane write itself, so
//     using it as the oracle makes the check a tautology;
//   - "we published the directive"  — a publish that lands on an islanded agent
//     converges nothing, and treating it as success would recreate the original bug
//     with an extra step.
//
// The agent acks from the SUCCESS path of its rehome (ApplyHome returned nil AND a
// tunnel session exists AND the new home was persisted), so an ack means the data
// plane genuinely moved.

// rehomedExpose is one expose the drain re-pointed, plus the epoch its agent must
// now reach. Produced by migrateExposes, consumed by awaitHomeConvergence.
type rehomedExpose struct {
	sid, nid, name string
	port           int
	epoch          int64
	toBroker       string
}

// codeDataplaneNotConverged is the adminsock reply Code for "control plane wrote,
// data plane has not followed yet".
//
// It is a WIRE literal shared with cmd/tether (which maps it to exit 75,
// EX_TEMPFAIL — retry-later, because re-running the verb is exactly the right
// response). The R7b review flagged cross-package wire literals as a silent-failure
// hazard: renaming one side would quietly downgrade this terminal signal. There is
// no compile-time link available across those packages, so
// TestDataplaneNotConvergedCodeIsWireStable (cmd/tether) pins the literal from the
// other end.
const codeDataplaneNotConverged = "dataplane_not_converged"

// ErrDataPlaneNotConverged is returned when the affected agents have not confirmed
// the new home within the operator's deadline. It names every port still behind, so
// the operator sees WHICH expose is stranded rather than a bare timeout.
type ErrDataPlaneNotConverged struct {
	Verb    string
	NodeID  string
	Pending []rehomedExpose
}

func (e *ErrDataPlaneNotConverged) Error() string {
	parts := make([]string, 0, len(e.Pending))
	for _, r := range e.Pending {
		parts = append(parts, fmt.Sprintf("%s(port %d, sid %s, nid %s, want epoch %d, home %s)",
			r.name, r.port, r.sid, r.nid, r.epoch, r.toBroker))
	}
	sort.Strings(parts)
	return fmt.Sprintf("cluster %s %s: the control plane committed the rehome but %d expose(s) have NOT confirmed the new home yet: %s"+
		" — the CONTROL plane is done, the DATA plane is not; re-run to keep waiting (the broker keeps re-delivering)",
		e.Verb, e.NodeID, len(e.Pending), strings.Join(parts, ", "))
}

// homeConvergencePollInterval is how often the gate re-checks the applied acks. It
// is only a polling granularity: the delivery itself is driven by the R7a
// home-delivery pass, never by this loop.
const homeConvergencePollInterval = 200 * time.Millisecond

// pendingHomeConvergence returns the subset of migrated exposes whose agent has NOT
// confirmed the target epoch. nil homeAppliedFn ⇒ nothing pending (see the field
// comment on ClusterAdmin: the unit paths construct a bare admin).
func (a *ClusterAdmin) pendingHomeConvergence(migrated []rehomedExpose) []rehomedExpose {
	if a.homeAppliedFn == nil {
		return nil
	}
	var pending []rehomedExpose
	for _, r := range migrated {
		if a.homeAppliedFn(r.port) < r.epoch {
			pending = append(pending, r)
		}
	}
	return pending
}

// kickHomeDelivery issues one immediate delivery per distinct owning agent. Used by
// the RESUMABLE retire path, which holds-and-retries instead of blocking.
func (a *ClusterAdmin) kickHomeDelivery(migrated []rehomedExpose) {
	if a.homeDeliverFn == nil {
		return
	}
	kicked := map[string]struct{}{}
	for _, r := range migrated {
		k := r.sid + "|" + r.nid
		if _, dup := kicked[k]; dup {
			continue
		}
		kicked[k] = struct{}{}
		a.homeDeliverFn(r.sid, r.nid)
	}
}

// awaitHomeConvergence blocks until every migrated expose's agent has confirmed the
// new home epoch, or the deadline passes. It kicks an immediate delivery first so
// convergence starts in milliseconds instead of on the next pass tick — but the KICK
// IS NOT THE MECHANISM: if it is lost, the home-delivery pass re-delivers on its own
// cadence and this loop still observes the ack. Correctness lives in the pass;
// latency lives here.
func (a *ClusterAdmin) awaitHomeConvergence(verb, nodeID string, migrated []rehomedExpose, deadline time.Time) error {
	if len(migrated) == 0 || a.homeAppliedFn == nil {
		return nil
	}
	// Kick once per distinct owning agent (a node with 20 exposes gets one push
	// carrying all 20 directives — homeForRegister builds the whole set).
	a.kickHomeDelivery(migrated)
	for {
		pending := a.pendingHomeConvergence(migrated)
		if len(pending) == 0 {
			a.logger.Info("cluster "+verb+": data plane converged onto the new home",
				"node_id", nodeID, "exposes", len(migrated))
			return nil
		}
		if !a.now().Before(deadline) {
			a.logger.Warn("cluster "+verb+": data plane has NOT converged within the deadline",
				"node_id", nodeID, "pending", len(pending), "of", len(migrated))
			return &ErrDataPlaneNotConverged{Verb: verb, NodeID: nodeID, Pending: pending}
		}
		time.Sleep(homeConvergencePollInterval)
	}
}
