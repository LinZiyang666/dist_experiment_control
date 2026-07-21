package broker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// reconcile_grow_lock.go (R7a, defect #31) — release a cluster_grow_active
// marker whose grow has already finished.
//
// THE DEFECT
// ----------
// `tether cluster add` acquires the replicated cluster_grow_active marker before
// the join operation exists, and releases it at P9 on clean completion. The
// release is BEST-EFFORT: releaseGrowLock printed a warning and moved on if the
// leader could not be resolved or the trigger did not confirm. There was no
// retry, no alert, and — critically — nothing that would ever notice afterwards.
// A single dropped release therefore left the marker set FOREVER, and because
// grow/retire/upgrade are mutually fenced on it, every subsequent membership
// operation on that cluster was refused until an operator hand-cleared a row in
// a replicated SQLite table. That is a permanent, cluster-wide outage of the
// membership control plane caused by one lost NATS reply.
//
// WHY THIS PASS IS THE INTERFACE'S THIRD-SHAPE SAMPLE
// ---------------------------------------------------
// The four passes moved out of the pre-R7 loop only prove the registry can hold
// what already existed. This one proves it generalizes: it is leader-only (like
// `ports`) but on its OWN cadence (like `proc-gc`), and — unlike any pre-R7 body
// — it can genuinely FAIL, because it proposes through raft. Propose returns an
// error on leadership loss, so the pass returns that error and earns the
// registry's exponential backoff instead of hammering raft every 30s during an
// election. Nothing about the four rewritten passes exercises that path; without
// this sample the error/backoff half of the interface would be frozen untested.
//
// THE ONE-VOTE-VETO INVARIANT, DISCHARGED
// ---------------------------------------
// Expected state: no grow marker, once the grow is over.
// Actual state:   growActiveJoiner(ro).
// Existing idempotent command path: cluster.PlanClearGrowActive(joiner) — the
// SAME plan the CLI's release-lock trigger proposes, value-bound to this joiner
// so it can never wipe a different grow's live marker.
//
// The pass invents no policy. In particular it does NOT decide "this grow has
// taken too long, kill it" — that is a lease/TTL mechanism and it belongs to
// R7b together with the upgrade lock (P3). This pass only recognizes a grow that
// is ALREADY OVER by the product's own two completion criteria, both lifted
// verbatim from code that already ships:
//
//	(a) a TERMINAL operation exists for the joiner — the same evidence the CLI's
//	    P9 release acts on; or
//	(b) the joiner is already a raft VOTER with no live join op — byte-for-byte
//	    the CLI's own P0 shortcut ("already a VOTER with no live join op — grow
//	    complete", cluster_add_drive.go), which releases the lock and exits.
//
// R7b — THE SCOPE LIMIT R7a DECLARED, NOW CLOSED
// -----------------------------------------------
// R7a stopped at the acquire-lock window: marker set between P1 (acquire) and P4
// (approve-join), with NO operation row to judge it by, because a HALT at P2/P3
// (init, verify-seam) leaves nothing to read. R7a's note said inventing a timeout
// there would be the new policy the invariant forbids — and it would have been,
// because a timeout on its own cannot tell "halted three minutes ago" from
// "operator is mid-init right now" and would yank the mutex out from under a live
// grow.
//
// A LEASE can tell them apart, because the holder keeps saying so. `cluster add`
// renews cluster_grow_lease every LockLeaseRenewInterval for the whole grow; when
// the orchestrator dies, the renewals stop and the lease expires. So the clause
// added below is not "this grow has taken too long" (a policy) — it is "the holder
// is gone" (a fact the holder itself established by falling silent).
//
// It is evaluated FIRST, ahead of the completion evidence, and it deliberately
// applies even when a live operation row EXISTS. A join op stuck non-terminal
// (BLOCKED, say) is still serialized by the entry-point NonTerminalOperations
// checks; the MARKER's only job is to serialize against a DRIVING orchestrator,
// and once no orchestrator is driving, holding it buys nothing and fences
// everything.
//
// FAIL-CLOSED: a marker with no lease row (acquired by a pre-R7b broker) never
// expires here — see cluster.LeaseExpired.

// growLockDecision is the whole decision, isolated from raft so it can be tested
// exhaustively against a plain SQLite fixture. isVoter reports whether a node is
// a VOTER in the committed raft configuration.
//
// Returns the marker's joiner and whether it should be cleared. It performs NO
// writes and reads nothing outside the two replicated tables.
func growLockDecision(ro *sql.DB, isVoter func(nodeID string) (bool, error), now time.Time) (joiner string, clear bool, err error) {
	// --- read ACTUAL state ---
	joiner = growActiveJoiner(ro)
	if joiner == "" {
		return "", false, nil // already converged: nothing held, nothing written
	}

	// --- R7b: the holder stopped renewing ⇒ the lock is abandoned, whatever the op log says ---
	expired, lerr := cluster.LeaseExpired(ro, cluster.MetaKeyGrowLease, now)
	if lerr != nil {
		return joiner, false, fmt.Errorf("read grow lease: %w", lerr)
	}
	if expired {
		return joiner, true, nil
	}

	// --- a live operation for this joiner means the grow IS the expected state ---
	active, err := cluster.ActiveOperationForTarget(ro, joiner)
	if err != nil {
		return joiner, false, fmt.Errorf("read active operation for %s: %w", joiner, err)
	}
	if active != nil {
		return joiner, false, nil
	}

	// --- compare against the product's own completion evidence ---
	// (a) a terminal operation for this joiner — the join ran to an end state
	// (SERVING, FAILED, ABORTED, …). Either way the grow is over and the CLI's
	// P9 release was the only thing left to do.
	latest, err := cluster.LatestOperationForTarget(ro, joiner)
	if err != nil {
		return joiner, false, fmt.Errorf("read latest operation for %s: %w", joiner, err)
	}
	if latest != nil && latest.Terminal {
		return joiner, true, nil
	}

	// (b) the joiner is already a raft VOTER and has no live join op — the CLI's
	// own "already a VOTER with no live join op — grow complete" shortcut.
	// Reached when the operation row has been pruned.
	voter, err := isVoter(joiner)
	if err != nil {
		return joiner, false, fmt.Errorf("read raft configuration: %w", err)
	}
	if voter {
		return joiner, true, nil
	}

	// No op row at all, not a voter, and the lease is still being renewed: a grow
	// really is in flight between P1 and P4. Hands off — this is the case the lease
	// exists to protect, not the case it exists to reap.
	return joiner, false, nil
}

// reconcileGrowLock is the registry pass. Leader-only (see registerCoreReconcilePasses).
func (b *Broker) reconcileGrowLock(_ context.Context, now time.Time) error {
	if !b.clusterMode || b.cl == nil || b.cl.node == nil {
		return nil
	}
	ro := b.cl.node.RODB()
	if ro == nil {
		return nil
	}
	// External review M-2: reap only when this leader has APPLIED everything committed (see the sibling
	// comment in reconcileUpgradeLock). A freshly-elected leader trailing the log could read a stale
	// EXPIRED grow lease that a renewal already refreshed, and tear out a live grow lock — re-admitting a
	// concurrent membership change. reaperCaughtUp gates the decision on applied>=commit.
	if !b.reaperCaughtUp() {
		return nil
	}

	joiner, clear, err := growLockDecision(ro, b.growLockIsVoter, now)
	if err != nil {
		return fmt.Errorf("grow-lock: %w", err)
	}
	if !clear {
		return nil
	}

	// --- call the EXISTING idempotent command path ---
	if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClearGrowActive(joiner)
	}); err != nil {
		// Propose fails on leadership loss. Returning the error earns the
		// registry's exponential backoff instead of re-proposing into a running
		// election every GrowLockReapInterval — this is the pass that exercises
		// (and therefore freezes) the registry's failure half.
		return fmt.Errorf("grow-lock: release stale cluster_grow_active for %s: %w", joiner, err)
	}
	b.cfg.Logger.Info("broker: released a stale grow lock left by an unconfirmed `cluster add` release",
		"joiner", joiner)
	return nil
}

func (b *Broker) growLockIsVoter(nodeID string) (bool, error) {
	cfgs, err := b.cl.node.RaftConfiguration()
	if err != nil {
		return false, err
	}
	for _, s := range cfgs {
		if s.NodeID == nodeID && s.Voter {
			return true, nil
		}
	}
	return false, nil
}
