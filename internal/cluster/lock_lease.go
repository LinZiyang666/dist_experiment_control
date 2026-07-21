package cluster

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// lock_lease.go (R7b) — TTL leases for the two cluster-scoped membership locks.
//
// WHY A LEASE AT ALL (and why R7a deliberately did not build one)
// ---------------------------------------------------------------
// R7a's registry passes are IDENTIFYING: they read a fact the product itself
// already wrote (a terminal operation row, a raft configuration) and, on that
// evidence, call an existing idempotent command path. That is enough to release a
// lock whose work has demonstrably FINISHED, and it is all the one-vote-veto
// invariant permits — a pass may not invent policy.
//
// It is NOT enough for the two remaining leaks, because in both of them the
// product wrote NO evidence at all:
//
//	P3  `cluster upgrade` HALTs mid-roll. releaseUpgradeLock runs only on clean
//	    completion, so the marker stays. The one self-heal path lives inside
//	    `plan.Upgrades() == 0`, and on an agentless host AtTarget is structurally
//	    false, so that branch is unreachable — the lock is held FOREVER and every
//	    join/retire is refused.
//	#31 `cluster add` HALTs at P2/P3 (init, verify-seam). The marker was acquired
//	    at P1, BEFORE the join operation row exists. reconcile_grow_lock.go can see
//	    a finished grow and a running grow; it cannot see the difference between
//	    "halted three minutes ago" and "the operator is mid-init right now",
//	    because in both cases there is nothing to read.
//
// Distinguishing those two requires knowing whether a HOLDER IS STILL ALIVE, and
// no amount of reading existing state answers that. It needs the holder to keep
// SAYING so. That is a lease: the holder renews on a timer, and the lock expires
// on its own once the renewals stop. This is the ONE new mechanism R7b is
// authorized to add, and it is confined to lock lifetime management — no pass
// gains a business policy from it.
//
// THE LEASE IS A SEPARATE KEY, NOT A NEW VALUE FORMAT
// ---------------------------------------------------
// cluster_grow_active's VALUE is the joiner id, and PlanClearGrowActive is
// value-predicated on it (that predicate is what stops a finished grow of joiner
// A from wiping joiner B's live marker). cluster_upgrade_active's value is the
// acquire timestamp. Re-encoding either would either break that predicate or make
// an OLD broker's value indistinguishable from a NEW one — and the value written
// by an old broker is an acquire time, i.e. always in the past, so a reaper that
// misread it would expire a LIVE lock instantly. During a rolling upgrade, which
// is exactly when the upgrade lock is held.
//
// So the lease lives in its OWN key and is purely additive:
//
//	cluster_upgrade_active -> unchanged   | cluster_upgrade_lease -> expiry
//	cluster_grow_active    -> unchanged   | cluster_grow_lease    -> expiry
//
// FAIL-CLOSED: a marker with NO lease row never expires. That is the pre-R7b
// behavior, and it is what a lock acquired by an older broker looks like. The
// explicit `tether cluster unlock` verb is the out for that case — an operator
// must never be told to wait for a TTL that will not come.

// Lease keys. Values are RFC3339Nano UTC absolute expiry instants, baked as SQL
// literals by the leader so every replica applies the identical row.
const (
	MetaKeyUpgradeLease = "cluster_upgrade_lease"
	MetaKeyGrowLease    = "cluster_grow_lease"
)

// LockLeaseTTL is how long a membership lock survives its holder's SILENCE.
//
// THE DERIVATION (the number is not a guess, and both directions are dangerous)
// -----------------------------------------------------------------------------
// Getting this too SHORT is strictly worse than the bug being fixed: expiring a
// lock under a LIVE orchestrator drops the mutex mid-operation and lets a
// concurrent join/retire cross a rolling restart — the precise hazard the lock
// was introduced to prevent. Too LONG and a stuck lock is indistinguishable from
// no fix at all.
//
// The quantity to bound is NOT how long the operation takes. It is how long a
// HEALTHY holder can go without landing a renewal. Those are very different
// numbers: a `cluster add` may legitimately spend opCatchupMaxWindow (30 min)
// waiting on an InstallSnapshot, but it is awake and renewing the whole time.
// A renewal fails only when the holder cannot reach a WRITABLE LEADER.
//
//	(a) No-writable-leader window, healthy .............. 3 min
//	    A renew is a leader Propose. During a roll the leader is deliberately
//	    moved and restarted on every host. The product's own bound on how long
//	    that may take is upgradeConvergeTimeout (3 min): waitLeader/waitVersion
//	    give up and HALT the roll past it, so a healthy roll never sits leaderless
//	    longer than that.
//
//	(b) Inter-broker clock skew ......................... 5 min
//	    The expiry literal is stamped by whichever broker was leader at renew
//	    time, and compared against the clock of whichever broker is leader at reap
//	    time. Leadership MOVES during a roll, so those are different clocks. The
//	    skew tether already tolerates cluster-wide is upgradeTriggerSkew (5 min) —
//	    a signed trigger outside that window is refused — so 5 min is the largest
//	    disagreement two brokers can have while still operating at all. A TTL
//	    below it could be consumed entirely by skew and expire a fresh lease.
//
//	(c) One missed renewal slot ......................... LockLeaseRenewInterval
//	    An outage can begin one instant after a renewal was due, so the last
//	    landed renewal may already be a full interval old.
//
// With the renew interval set to TTL/3 (three chances to renew per lease, so two
// consecutive failures are survivable):
//
//	TTL >= (a) + (b) + TTL/3   =>   TTL >= 8min / (2/3) = 12 min
//
// Rounded up to 15 min, which leaves ~3 min of headroom over the derived floor.
//
// The cost of the round-up is bounded and has an escape hatch: after an abandoned
// operation, membership is fenced for at most 15 min instead of forever, and
// `tether cluster unlock` clears it immediately for an operator who will not wait.
const LockLeaseTTL = 15 * time.Minute

// LockLeaseRenewInterval is the holder's renewal cadence: TTL/3, so two
// consecutive renewal failures still leave the lease alive. Renewal is cheap (one
// leader Propose of a two-statement command) and its cost does not scale with
// cluster size.
const LockLeaseRenewInterval = LockLeaseTTL / 3

// ErrNoLease reports that the lock is held but carries NO lease row — a lock
// acquired by a pre-R7b broker. Callers MUST fail closed (never expire it).
var ErrNoLease = errors.New("cluster: lock has no lease (acquired before leases existed)")

// LeaseExpiry reads a lease key's absolute expiry. Returns ErrNoLease when the row
// is absent, and a parse error when the value is unintelligible — in BOTH cases
// the caller must decline to expire the lock, never assume it is dead.
func LeaseExpiry(ro *sql.DB, key string) (time.Time, error) {
	var raw string
	err := ro.QueryRow(`SELECT value FROM cluster_meta WHERE key=? LIMIT 1`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, ErrNoLease
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("cluster: read lease %s: %w", key, err)
	}
	t, perr := time.Parse(time.RFC3339Nano, raw)
	if perr != nil {
		return time.Time{}, fmt.Errorf("cluster: lease %s holds an unparseable expiry %q: %w", key, raw, perr)
	}
	return t, nil
}

// LeaseExpired reports whether a lease exists AND its expiry is strictly in the
// past. It is the single fail-closed predicate every reaper uses: absent lease,
// unparseable lease, or read error all return false (do not expire).
//
// The returned error is informational — a caller that wants the pass to earn the
// registry's backoff can surface it; a caller that only wants the verdict may
// ignore it, because false is already the safe answer.
func LeaseExpired(ro *sql.DB, key string, now time.Time) (bool, error) {
	exp, err := LeaseExpiry(ro, key)
	if err != nil {
		if errors.Is(err, ErrNoLease) {
			return false, nil // fail-closed, and NOT an error: a pre-R7b lock is a normal state
		}
		return false, err
	}
	return now.After(exp), nil
}

// markerAcquireStmts renders a GUARDED membership-lock acquire using the same
// unambiguous INSERT-OR-IGNORE + UPDATE idiom as leaseRenewStmts (see the note
// there on why INSERT..SELECT..WHERE..ON CONFLICT is deliberately avoided).
//
// The marker is created (if absent) or refreshed to valLit (if this holder already
// owns it) ONLY while `guard` holds. That guard is what turns grow/upgrade
// acquisition into a REPLICATED mutex instead of the check-then-write race the
// leader's preflight read cannot make atomic: two concurrent callbacks can both
// pass their preflight, but Raft serializes these commands and the SECOND one's
// guard now sees the first's marker and no-ops. When the guard is false BOTH
// statements no-op, so the caller's post-Propose read-back observes it did NOT
// acquire and refuses (external review C1/H1).
func markerAcquireStmts(markerKey, valLit, guard string) []Statement {
	key := MustLitText(markerKey)
	return []Statement{
		Stmt(`INSERT OR IGNORE INTO cluster_meta(key, value) SELECT ` + key + `, ` + valLit + ` WHERE ` + guard),
		Stmt(`UPDATE cluster_meta SET value = ` + valLit + ` WHERE key = ` + key + ` AND ` + guard),
	}
}

// leaseAcquireStmts renders the lock's lease write guarded IDENTICALLY to its
// marker, so a no-op (guard-false) acquire never leaves an ORPHAN lease. A
// marker-without-lease is the fail-closed never-expires bucket this batch removes;
// writing the lease unconditionally while the marker no-opped would land a brand-new
// lease against a marker that was never acquired.
func leaseAcquireStmts(leaseKey string, expiry time.Time, guard string) []Statement {
	val := MustLitText(expiry.UTC().Format(time.RFC3339Nano))
	key := MustLitText(leaseKey)
	return []Statement{
		Stmt(`INSERT OR IGNORE INTO cluster_meta(key, value) SELECT ` + key + `, ` + val + ` WHERE ` + guard),
		Stmt(`UPDATE cluster_meta SET value = ` + val + ` WHERE key = ` + key + ` AND ` + guard),
	}
}

// growMarkerExists is the existence predicate mirroring upgradeMarkerExists, used
// to make an upgrade acquire refuse while ANY grow marker is held.
func growMarkerExists() string {
	return `EXISTS (SELECT 1 FROM cluster_meta WHERE key=` + MustLitText(MetaKeyGrowActive) + `)`
}

// growMarkerHeldByOther renders "a grow marker exists whose joiner is NOT this one",
// so a grow acquire is idempotent for its own joiner (resume) yet mutually exclusive
// against a DIFFERENT joiner's in-flight grow at the REPLICATED layer — closing the
// same check-then-write race for grow-vs-grow that H1 flagged for grow-vs-upgrade.
func growMarkerHeldByOther(joinerID string) (string, error) {
	lit, err := LitText(joinerID)
	if err != nil {
		return "", err
	}
	return `EXISTS (SELECT 1 FROM cluster_meta WHERE key=` + MustLitText(MetaKeyGrowActive) +
		` AND value != ` + lit + `)`, nil
}

// leaseClearStmt renders "drop this lease, but ONLY once its marker is gone".
//
// The guard is what makes a value-predicated marker clear stay honest. The marker
// DELETE and this DELETE ride the SAME command, i.e. the same FSM transaction, in
// this order. If the marker clear no-opped because the marker belongs to a
// DIFFERENT joiner, the marker is still present and this statement no-ops too — so
// a finished grow of joiner A cannot strip joiner B's live lease and hand B's lock
// to the reaper on the next tick. No substring matching, no LIKE metacharacter
// hazards: the guard is an existence check on the marker the lease belongs to.
func leaseClearStmt(leaseKey, markerKey string) Statement {
	return Stmt(`DELETE FROM cluster_meta WHERE key=` + MustLitText(leaseKey) +
		` AND NOT EXISTS (SELECT 1 FROM cluster_meta WHERE key=` + MustLitText(markerKey) + `)`)
}

// leaseRenewStmts render a renewal that can REFRESH an existing lease but can
// never CREATE a lock. `exists` is the SQL predicate proving the holder still owns
// the marker.
//
// Two statements rather than one INSERT..SELECT..ON CONFLICT: SQLite's parser
// cannot unambiguously attach an upsert clause to an INSERT whose SELECT carries a
// WHERE, and the workarounds are exactly the kind of cleverness that has no place
// in a statement replayed on every replica for the lifetime of the log. INSERT OR
// IGNORE then UPDATE is unambiguous, order-independent in effect, and idempotent.
func leaseRenewStmts(leaseKey, exists string, expiry time.Time) []Statement {
	val := MustLitText(expiry.UTC().Format(time.RFC3339Nano))
	key := MustLitText(leaseKey)
	return []Statement{
		Stmt(`INSERT OR IGNORE INTO cluster_meta(key, value) SELECT ` + key + `, ` + val + ` WHERE ` + exists),
		Stmt(`UPDATE cluster_meta SET value = ` + val + ` WHERE key = ` + key + ` AND ` + exists),
	}
}

// upgradeMarkerExists / growMarkerOwnedBy are the ownership predicates a renewal
// is gated on. A renewal that could run without them would resurrect a lock the
// orchestrator already released — a keeper goroutine that outlives its command by
// one tick would re-fence the whole membership plane.
func upgradeMarkerExists() string {
	return `EXISTS (SELECT 1 FROM cluster_meta WHERE key=` + MustLitText(MetaKeyUpgradeActive) + `)`
}

func growMarkerOwnedBy(joinerID string) (string, error) {
	lit, err := LitText(joinerID)
	if err != nil {
		return "", err
	}
	return `EXISTS (SELECT 1 FROM cluster_meta WHERE key=` + MustLitText(MetaKeyGrowActive) +
		` AND value=` + lit + `)`, nil
}

// PlanRenewUpgradeLease refreshes the upgrade lock's lease to now+LockLeaseTTL,
// but ONLY while cluster_upgrade_active is still set. On a released lock every
// statement no-ops, so a late renewal from a keeper that has not stopped yet can
// never re-fence membership.
func PlanRenewUpgradeLease(now time.Time) (*Command, error) {
	return NewCommand(OpClusterMetaSet,
		leaseRenewStmts(MetaKeyUpgradeLease, upgradeMarkerExists(), now.Add(LockLeaseTTL))...), nil
}

// PlanRenewGrowLease refreshes the grow lock's lease, but ONLY while
// cluster_grow_active is still held BY THIS JOINER. A renewal for joiner A can
// therefore never extend joiner B's lock.
func PlanRenewGrowLease(joinerID string, now time.Time) (*Command, error) {
	if joinerID == "" {
		return nil, fmt.Errorf("cluster: grow-lease renew requires a joiner id")
	}
	exists, err := growMarkerOwnedBy(joinerID)
	if err != nil {
		return nil, fmt.Errorf("cluster: grow-lease renew: literal: %w", err)
	}
	return NewCommand(OpClusterMetaSet,
		leaseRenewStmts(MetaKeyGrowLease, exists, now.Add(LockLeaseTTL))...), nil
}
