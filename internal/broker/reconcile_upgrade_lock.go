package broker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// reconcile_upgrade_lock.go (R7b, defect P3) — release a cluster_upgrade_active
// marker whose holder has stopped renewing.
//
// THE DEFECT
// ----------
// `tether cluster upgrade` acquires a cluster-scoped roll lock before touching a
// single host, and releases it ONLY on clean completion. A HALT — any refusal at
// any host — deliberately leaves it held, on the theory that membership should
// stay frozen while the cluster sits in a partial-roll state, and that the
// operator will "fix and re-run", whose re-run re-acquires and eventually
// releases.
//
// That theory has one reachable branch and one unreachable one. The re-run path
// works while there is still something to roll. But once every broker host is at
// the target, `cluster upgrade` short-circuits at `plan.Upgrades() == 0`, and the
// stale-lock self-heal lives INSIDE that branch — reachable only if the planner
// agrees every host is AtTarget. On an agentless host AtTarget is structurally
// false, so `plan.Upgrades()` never reaches 0, the self-heal branch never runs,
// and the roll loop that would release the lock has nothing left to do either.
// The marker is then held FOREVER: `cluster join` and `cluster retire` are
// refused, cluster-wide, until somebody hand-edits a replicated SQLite row.
//
// WHY A LEASE AND NOT A DEADLINE
// ------------------------------
// A plain "locks older than X are stale" rule is a POLICY, and a wrong one: rolls
// legitimately run long, so any X large enough to be safe is too large to be
// useful, and any X small enough to be useful drops the mutex under a live roll —
// which is strictly worse than the leak, because it re-admits the concurrent
// join/retire the lock exists to fence. The lease inverts the question from "has
// this taken too long?" (unanswerable) to "is anybody still holding it?"
// (answered by the holder, continuously, by renewing).
//
// THE ONE-VOTE-VETO INVARIANT, DISCHARGED
// ---------------------------------------
// Expected state: no roll lock, once nobody is renewing its lease.
// Actual state:   upgradeActive(ro) + cluster_upgrade_lease.
// Existing idempotent command path: cluster.PlanClearUpgradeActive() — the SAME
// plan the CLI's release-lock trigger proposes, unchanged.
//
// The pass invents no policy beyond the lease's own expiry, which is the single
// new mechanism R7b is authorized to add. It does not decide a roll has failed,
// does not touch any host, does not abort anything: it removes a mutex whose
// holder is provably gone.
//
// FAIL-CLOSED, TWICE
// ------------------
//   - No lease row ⇒ never expire. That is what a lock acquired by a pre-R7b
//     broker looks like, and mid-rolling-upgrade is exactly when a mixed-version
//     cluster exists. `tether cluster unlock` is the operator's out for those.
//   - Unparseable/unreadable lease ⇒ never expire, and surface the error so the
//     pass earns the registry's exponential backoff instead of silently declining
//     forever.

// upgradeLockDecision is the whole decision, isolated from raft so it can be
// tested exhaustively against a plain SQLite fixture. It performs NO writes.
func upgradeLockDecision(ro *sql.DB, now time.Time) (clear bool, err error) {
	if !upgradeActive(ro) {
		return false, nil // already converged: nothing held, nothing written
	}
	expired, err := cluster.LeaseExpired(ro, cluster.MetaKeyUpgradeLease, now)
	if err != nil {
		return false, fmt.Errorf("read upgrade lease: %w", err)
	}
	return expired, nil
}

// reconcileUpgradeLock is the registry pass. Leader-only (see registerCoreReconcilePasses).
func (b *Broker) reconcileUpgradeLock(_ context.Context, now time.Time) error {
	if !b.clusterMode || b.cl == nil || b.cl.node == nil {
		return nil
	}
	ro := b.cl.node.RODB()
	if ro == nil {
		return nil
	}
	// External review M-2: reap only when this leader has APPLIED everything committed. A leader that
	// just won an election (e.g. mid rolling-upgrade) can trail the log by several entries; deciding to
	// reap off that stale view could read an EXPIRED lease that a renewal has already refreshed on the
	// committed-but-not-yet-applied tail, and tear out a LIVE roll lock — re-admitting the concurrent
	// join/retire the lock exists to fence. reaperCaughtUp gates the decision on applied>=commit.
	if !b.reaperCaughtUp() {
		return nil
	}

	clear, err := upgradeLockDecision(ro, now)
	if err != nil {
		return fmt.Errorf("upgrade-lock: %w", err)
	}
	if !clear {
		return nil
	}

	// --- call the EXISTING idempotent command path ---
	if err := b.cl.node.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClearUpgradeActive()
	}); err != nil {
		// Propose fails on leadership loss; returning the error earns the registry's
		// exponential backoff rather than re-proposing into a running election.
		return fmt.Errorf("upgrade-lock: release expired cluster_upgrade_active: %w", err)
	}
	b.cfg.Logger.Warn("broker: released an expired `cluster upgrade` roll lock — its orchestrator stopped renewing the lease. " +
		"If a roll IS still running, it HALTED; re-run `tether cluster upgrade` before starting any membership change.")
	return nil
}
