package broker

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// reconcile_lock_lease_test.go (R7b) — the lease mechanism, from both ends.
//
// The lease is the ONE new mechanism this batch adds, and it is the only place in the reconcile registry
// where a pass acts on something other than evidence the product already wrote. It therefore carries the
// batch's whole safety argument, and it is dangerous in BOTH directions:
//
//	expiring too EAGERLY  → the membership mutex is dropped under a LIVE orchestrator, and two member
//	                        changes can run concurrently. Strictly worse than the leak being fixed.
//	expiring too RELUCTANTLY → the lock is held forever, which IS the leak being fixed.
//
// So every test below pins one of those two directions, and the two headline ones
// (TestLeaseHolderIsNeverRobbed / TestLeaseExpiresOnceTheHolderStops) are the required bidirectional pair.

// --------------------------------------------------------------------------
// fixtures
// --------------------------------------------------------------------------

func setUpgradeMarker(t *testing.T, db *sql.DB, at time.Time) {
	t.Helper()
	cmd, err := cluster.PlanSetUpgradeActive(at)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
}

func setGrowMarkerLeased(t *testing.T, db *sql.DB, joiner string, at time.Time) {
	t.Helper()
	cmd, err := cluster.PlanSetGrowActive(joiner, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
}

func renewUpgrade(t *testing.T, db *sql.DB, at time.Time) {
	t.Helper()
	cmd, err := cluster.PlanRenewUpgradeLease(at)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
}

func renewGrow(t *testing.T, db *sql.DB, joiner string, at time.Time) {
	t.Helper()
	cmd, err := cluster.PlanRenewGrowLease(joiner, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
}

func leaseRow(t *testing.T, db *sql.DB, key string) (string, bool) {
	t.Helper()
	var v string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return v, true
}

// --------------------------------------------------------------------------
// THE BIDIRECTIONAL PAIR (exit assertion 2)
// --------------------------------------------------------------------------

// TestLeaseHolderIsNeverRobbed is the DANGEROUS direction. A holder that keeps renewing must survive
// arbitrarily long — far past the TTL — because a live `cluster add` may legitimately run for
// opCatchupMaxWindow (30 min), twice the 15-minute TTL. If this ever fails, two membership operations can
// run concurrently, which is worse than the defect R7b fixes.
//
// The simulation is deliberately harsh: renewals land at the full production cadence (TTL/3), the reaper
// runs at its own much finer cadence (30s) in between, and the whole thing runs for 4x the TTL.
func TestLeaseHolderIsNeverRobbed(t *testing.T) {
	for _, lock := range []string{"upgrade", "grow"} {
		t.Run(lock, func(t *testing.T) {
			db := passTestDB(t)
			now := passEpoch
			if lock == "upgrade" {
				setUpgradeMarker(t, db, now)
			} else {
				setGrowMarkerLeased(t, db, "brk-b", now)
			}

			nextRenew := now.Add(cluster.LockLeaseRenewInterval)
			deadline := now.Add(4 * cluster.LockLeaseTTL)
			reaps := 0
			for now.Before(deadline) {
				now = now.Add(30 * time.Second) // the reaper's cadence
				if !now.Before(nextRenew) {
					// The holder is alive and renews on schedule.
					if lock == "upgrade" {
						renewUpgrade(t, db, now)
					} else {
						renewGrow(t, db, "brk-b", now)
					}
					nextRenew = now.Add(cluster.LockLeaseRenewInterval)
				}
				reaps++
				if lock == "upgrade" {
					clear, err := upgradeLockDecision(db, now)
					if err != nil {
						t.Fatal(err)
					}
					if clear {
						t.Fatalf("at %s (reap #%d) the reaper decided to expire a lock whose holder is STILL RENEWING — "+
							"this drops the membership mutex under a live roll and lets a concurrent join/retire cross it",
							now.Sub(passEpoch), reaps)
					}
				} else {
					_, clear, err := growLockDecision(db, noVoters, now)
					if err != nil {
						t.Fatal(err)
					}
					if clear {
						t.Fatalf("at %s (reap #%d) the reaper decided to expire a grow lock whose holder is STILL RENEWING",
							now.Sub(passEpoch), reaps)
					}
				}
			}
			if reaps < 100 {
				t.Fatalf("the simulation only reaped %d times — too weak to prove anything", reaps)
			}
		})
	}
}

// TestLeaseExpiresOnceTheHolderStops is the other direction: the actual fix. Once renewals stop, the lock
// MUST be gone within the TTL — not "eventually", not "on the next operator action".
func TestLeaseExpiresOnceTheHolderStops(t *testing.T) {
	for _, lock := range []string{"upgrade", "grow"} {
		t.Run(lock, func(t *testing.T) {
			db := passTestDB(t)
			acquired := passEpoch
			if lock == "upgrade" {
				setUpgradeMarker(t, db, acquired)
			} else {
				setGrowMarkerLeased(t, db, "brk-b", acquired)
			}

			decide := func(now time.Time) bool {
				if lock == "upgrade" {
					clear, err := upgradeLockDecision(db, now)
					if err != nil {
						t.Fatal(err)
					}
					return clear
				}
				_, clear, err := growLockDecision(db, noVoters, now)
				if err != nil {
					t.Fatal(err)
				}
				return clear
			}

			// The holder dies immediately after acquiring — no renewal ever lands. Before the TTL the lock
			// must be UNTOUCHED (an operator mid-init is indistinguishable from a corpse until then).
			for _, dt := range []time.Duration{0, time.Minute, cluster.LockLeaseRenewInterval,
				cluster.LockLeaseTTL - time.Second} {
				if decide(acquired.Add(dt)) {
					t.Fatalf("the lock was expired %s after acquire, BEFORE its %s lease ran out — "+
						"a live-but-slow orchestrator would be robbed", dt, cluster.LockLeaseTTL)
				}
			}
			// One instant past the TTL it MUST be reaped.
			if !decide(acquired.Add(cluster.LockLeaseTTL + time.Nanosecond)) {
				t.Fatalf("the lock survived its %s lease with nobody renewing — this is the permanent fence R7b exists to remove",
					cluster.LockLeaseTTL)
			}
		})
	}
}

// TestLeaseExpiryIsMeasuredFromTheLASTRenewal: a holder that renews for a while and THEN dies gets a full
// TTL from its final renewal, not from acquire. Getting this wrong would expire long-running-but-healthy
// operations on a fixed schedule — a deadline wearing a lease's clothes.
func TestLeaseExpiryIsMeasuredFromTheLASTRenewal(t *testing.T) {
	db := passTestDB(t)
	setUpgradeMarker(t, db, passEpoch)

	lastRenew := passEpoch.Add(42 * time.Minute) // long past the TTL from acquire
	renewUpgrade(t, db, lastRenew)

	if clear, _ := upgradeLockDecision(db, lastRenew.Add(cluster.LockLeaseTTL-time.Second)); clear {
		t.Fatal("expiry is being measured from ACQUIRE, not from the last renewal — every long operation would be robbed on a fixed timer")
	}
	if clear, _ := upgradeLockDecision(db, lastRenew.Add(cluster.LockLeaseTTL+time.Second)); !clear {
		t.Fatal("the lease did not expire a full TTL after the LAST renewal")
	}
}

// --------------------------------------------------------------------------
// the renewal's structural safety properties
// --------------------------------------------------------------------------

// TestRenewalCannotCreateALock is the property that makes a keeper goroutine outliving its release
// harmless — and it is the reason lock_keeper.Stop() is not required to be ordered before the release. If a
// renewal could CREATE a marker, a keeper that ticked once after `cluster add` released would re-fence the
// entire membership plane with a lock nobody holds and nobody will ever release.
func TestRenewalCannotCreateALock(t *testing.T) {
	t.Run("upgrade", func(t *testing.T) {
		db := passTestDB(t)
		renewUpgrade(t, db, passEpoch) // no marker was ever set
		if upgradeActive(db) {
			t.Fatal("a renewal CREATED the upgrade lock — a late keeper tick would permanently fence membership")
		}
		if _, ok := leaseRow(t, db, cluster.MetaKeyUpgradeLease); ok {
			t.Fatal("a renewal wrote a lease row for a lock that does not exist")
		}
	})
	t.Run("grow", func(t *testing.T) {
		db := passTestDB(t)
		renewGrow(t, db, "brk-b", passEpoch)
		if growActiveJoiner(db) != "" {
			t.Fatal("a renewal CREATED the grow lock")
		}
		if _, ok := leaseRow(t, db, cluster.MetaKeyGrowLease); ok {
			t.Fatal("a renewal wrote a lease row for a lock that does not exist")
		}
	})
	t.Run("released mid-flight then renewed", func(t *testing.T) {
		db := passTestDB(t)
		setUpgradeMarker(t, db, passEpoch)
		cmd, err := cluster.PlanClearUpgradeActive()
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
		renewUpgrade(t, db, passEpoch.Add(time.Minute)) // the keeper's last tick, after the release
		if upgradeActive(db) {
			t.Fatal("a post-release renewal resurrected the roll lock")
		}
	})
}

// TestGrowRenewalIsJoinerBound: joiner A's keeper must not be able to extend joiner B's lock. Without the
// value predicate, a stale keeper from a previous grow would keep a DIFFERENT operator's lock alive
// forever — an immortal lock created by the very mechanism meant to make locks mortal.
func TestGrowRenewalIsJoinerBound(t *testing.T) {
	db := passTestDB(t)
	setGrowMarkerLeased(t, db, "brk-b", passEpoch)
	before, _ := leaseRow(t, db, cluster.MetaKeyGrowLease)

	renewGrow(t, db, "brk-OTHER", passEpoch.Add(time.Hour))

	after, _ := leaseRow(t, db, cluster.MetaKeyGrowLease)
	if after != before {
		t.Fatalf("a renewal naming brk-OTHER extended brk-b's lease (%q → %q) — a stale keeper could hold another operator's lock open indefinitely", before, after)
	}
}

// TestAcquireStampsMarkerAndLeaseAtomically: the two writes must ride ONE command, i.e. one FSM
// transaction. If a crash could interleave them, a brand-new lock could land with no lease row — and the
// reaper is fail-closed on exactly that shape, so it would never expire. That is the permanent fence,
// reintroduced by the fix.
func TestAcquireStampsMarkerAndLeaseAtomically(t *testing.T) {
	// H1: the acquire is now GUARDED (grow⇄upgrade replicated mutex), so each of the marker and lease writes
	// is the 2-statement INSERT-OR-IGNORE+UPDATE idiom the codebase uses for a conditional write — marker(2)
	// + lease(2). The invariant this test protects is UNCHANGED: both writes ride ONE *Command (one FSM
	// transaction), so a crash can never interleave a marker without its lease into the never-expires bucket.
	up, err := cluster.PlanSetUpgradeActive(passEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(up.Body) < 2 {
		t.Fatalf("the upgrade acquire must write the marker AND its lease in one command, got %d statements", len(up.Body))
	}
	grow, err := cluster.PlanSetGrowActive("brk-b", passEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(grow.Body) < 2 {
		t.Fatalf("the grow acquire must write the marker AND its lease in one command, got %d statements", len(grow.Body))
	}
	// And executing the ONE command lands BOTH the marker and the lease (proving the atomic pair).
	db := passTestDB(t)
	if err := cluster.ExecCommand(db, up); err != nil {
		t.Fatal(err)
	}
	if !upgradeActive(db) {
		t.Fatal("acquire did not stamp the marker")
	}
	got, ok := leaseRow(t, db, cluster.MetaKeyUpgradeLease)
	if !ok {
		t.Fatal("acquire did not stamp a lease — the lock would be in the never-expires bucket from birth")
	}
	want := passEpoch.Add(cluster.LockLeaseTTL).UTC().Format(time.RFC3339Nano)
	if got != want {
		t.Fatalf("lease expiry %q, want %q (now+LockLeaseTTL)", got, want)
	}
}

// TestReleaseClearsTheLeaseButOnlyItsOwn: the lease dies with its marker, and a release aimed at joiner A
// must leave joiner B's lease alone. If A's release stripped B's lease, B's marker would become leaseless
// — which the reaper reads as pre-R7b and NEVER expires. A lock that can never be released, created by the
// release path.
func TestReleaseClearsTheLeaseButOnlyItsOwn(t *testing.T) {
	t.Run("its own lease goes", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarkerLeased(t, db, "brk-b", passEpoch)
		cmd, err := cluster.PlanClearGrowActive("brk-b")
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
		if _, ok := leaseRow(t, db, cluster.MetaKeyGrowLease); ok {
			t.Fatal("the lease outlived its marker — a later acquire would inherit a stale expiry")
		}
	})
	t.Run("another joiner's lease survives", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarkerLeased(t, db, "brk-b", passEpoch)
		// A release for a DIFFERENT joiner: the marker DELETE no-ops (value predicate), so the lease
		// DELETE must no-op too.
		cmd, err := cluster.PlanClearGrowActive("brk-OTHER")
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
		if growActiveJoiner(db) != "brk-b" {
			t.Fatal("precondition failed: brk-b's marker should have survived a clear aimed at brk-OTHER")
		}
		if _, ok := leaseRow(t, db, cluster.MetaKeyGrowLease); !ok {
			t.Fatal("brk-OTHER's release stripped brk-b's LEASE while leaving its marker — the marker is now leaseless, " +
				"which the reaper treats as pre-R7b and never expires. The permanent fence is back.")
		}
	})
}

// TestNoLeaseNeverExpires is the fail-closed rule. A marker written by a pre-R7b broker carries no lease,
// and its VALUE is an acquire timestamp — always in the past. A reaper that guessed from the value would
// expire live locks instantly, and a rolling upgrade (mixed versions, roll lock held) is exactly when that
// would happen.
func TestNoLeaseNeverExpires(t *testing.T) {
	t.Run("upgrade", func(t *testing.T) {
		db := passTestDB(t)
		// Pre-R7b shape: marker only, written the old way.
		if _, err := db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('cluster_upgrade_active',?)`,
			passEpoch.Add(-72*time.Hour).UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		clear, err := upgradeLockDecision(db, passEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if clear {
			t.Fatal("a leaseless (pre-R7b) roll lock was expired — during a rolling upgrade this rips the mutex out of a LIVE roll driven by an old broker")
		}
	})
	t.Run("grow", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarker(t, db, "brk-b") // no lease row
		_, clear, err := growLockDecision(db, noVoters, passEpoch.Add(72*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if clear {
			t.Fatal("a leaseless grow marker was expired on age alone — that is the timeout policy R7a refused, not a lease")
		}
	})
	t.Run("unparseable lease never expires and DOES surface an error", func(t *testing.T) {
		db := passTestDB(t)
		setUpgradeMarker(t, db, passEpoch)
		if _, err := db.Exec(`UPDATE cluster_meta SET value='not-a-timestamp' WHERE key=?`, cluster.MetaKeyUpgradeLease); err != nil {
			t.Fatal(err)
		}
		clear, err := upgradeLockDecision(db, passEpoch.Add(time.Hour))
		if clear {
			t.Fatal("an unintelligible lease must never authorize an expiry")
		}
		if err == nil {
			t.Fatal("an unintelligible lease must surface an error so the pass earns the registry's backoff instead of silently declining forever")
		}
	})
}

// TestGrowLockLeaseExpiry closes the ACQUIRE-LOCK WINDOW — the half of #31 that R7a explicitly could not
// judge, because between P1 (acquire) and P4 (approve-join) there is no operation row to read. Referenced
// by TestGrowLockDecision's "marker + no op at all + not a voter" row.
func TestGrowLockLeaseExpiry(t *testing.T) {
	// Live holder, no op row yet: this is `cluster add` sitting in P2/P3. Hands OFF.
	t.Run("mid-init grow is protected", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarkerLeased(t, db, "brk-b", passEpoch)
		_, clear, err := growLockDecision(db, noVoters, passEpoch.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if clear {
			t.Fatal("a grow that is mid-init (marker set, op row not created yet, lease live) was reaped")
		}
	})
	// Same shape, but the orchestrator HALTed at P2/P3 and walked away. This is the case that used to be
	// unjudgeable and therefore permanent.
	t.Run("halted grow is reaped", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarkerLeased(t, db, "brk-b", passEpoch)
		joiner, clear, err := growLockDecision(db, noVoters, passEpoch.Add(cluster.LockLeaseTTL+time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if !clear || joiner != "brk-b" {
			t.Fatal("a grow abandoned in the acquire-lock window was NOT reaped — with no op row, the lease is the only evidence that exists")
		}
	})
	// The lease outranks a live-looking op row: a join op stuck non-terminal is still serialized by the
	// entry-point NonTerminalOperations checks, so holding the MARKER once nobody is driving buys nothing.
	t.Run("an expired lease outranks a live op row", func(t *testing.T) {
		db := passTestDB(t)
		setGrowMarkerLeased(t, db, "brk-b", passEpoch)
		insertOp(t, db, "op-1", "brk-b", "CATCHING_UP", false, passEpoch)
		_, clear, err := growLockDecision(db, noVoters, passEpoch.Add(cluster.LockLeaseTTL+time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if !clear {
			t.Fatal("a lock whose holder is provably gone stayed held because an op row looked live — the marker's job is to serialize against a DRIVING orchestrator, and none is driving")
		}
	})
}

// --------------------------------------------------------------------------
// the upgrade-lock pass: the three idempotence properties (exit assertion 1)
// --------------------------------------------------------------------------

func TestUpgradeLockDecision(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(t *testing.T, db *sql.DB)
		at        time.Duration // offset from passEpoch
		wantClear bool
		why       string
	}{
		{
			name:      "no marker",
			setup:     func(*testing.T, *sql.DB) {},
			wantClear: false,
			why:       "converged: the pass must not write on a cluster with no roll lock, ever",
		},
		{
			name:      "marker + fresh lease",
			setup:     func(t *testing.T, db *sql.DB) { setUpgradeMarker(t, db, passEpoch) },
			at:        time.Minute,
			wantClear: false,
			why:       "a roll IS running; clearing here re-admits the join/retire the lock exists to fence",
		},
		{
			name:      "marker + expired lease",
			setup:     func(t *testing.T, db *sql.DB) { setUpgradeMarker(t, db, passEpoch) },
			at:        cluster.LockLeaseTTL + time.Minute,
			wantClear: true,
			why:       "P3 proper: the roll HALTed and nobody is renewing. Before R7b this lock was UNRELEASABLE on an agentless host",
		},
		{
			name: "marker + lease renewed just in time",
			setup: func(t *testing.T, db *sql.DB) {
				setUpgradeMarker(t, db, passEpoch)
				renewUpgrade(t, db, passEpoch.Add(cluster.LockLeaseTTL-time.Second))
			},
			at:        cluster.LockLeaseTTL + time.Minute,
			wantClear: false,
			why:       "a renewal landing in the last second of the lease must extend it — the TTL budgets for exactly this",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := passTestDB(t)
			tc.setup(t, db)
			clear, err := upgradeLockDecision(db, passEpoch.Add(tc.at))
			if err != nil {
				t.Fatalf("decision returned %v", err)
			}
			if clear != tc.wantClear {
				t.Fatalf("clear=%v, want %v\nwhy: %s", clear, tc.wantClear, tc.why)
			}
		})
	}
}

func TestPassUpgradeLockIdempotence(t *testing.T) {
	// (a) converged ⇒ zero side effects across consecutive ticks.
	t.Run("converged is a fixed point", func(t *testing.T) {
		db := passTestDB(t)
		for i := 0; i < 3; i++ {
			clear, err := upgradeLockDecision(db, passEpoch.Add(time.Duration(i)*30*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if clear {
				t.Fatalf("tick %d proposed a write against a cluster holding no roll lock", i+1)
			}
		}
	})
	t.Run("a live roll is a fixed point", func(t *testing.T) {
		db := passTestDB(t)
		setUpgradeMarker(t, db, passEpoch)
		for i := 0; i < 3; i++ {
			at := passEpoch.Add(time.Duration(i) * 30 * time.Second)
			renewUpgrade(t, db, at) // the orchestrator is alive
			if clear, err := upgradeLockDecision(db, at); err != nil || clear {
				t.Fatalf("tick %d would have expired a LIVE roll lock (err=%v)", i+1, err)
			}
		}
	})

	// (b) external drift ⇒ re-converges, then becomes a fixed point again.
	t.Run("re-converges when the holder dies, then settles", func(t *testing.T) {
		db := passTestDB(t)
		setUpgradeMarker(t, db, passEpoch)
		if clear, _ := upgradeLockDecision(db, passEpoch); clear {
			t.Fatal("precondition: a freshly acquired lock must not be expired")
		}
		// The holder stops renewing (the external change) — the next tick past the TTL must clear.
		dead := passEpoch.Add(cluster.LockLeaseTTL + time.Second)
		clear, err := upgradeLockDecision(db, dead)
		if err != nil || !clear {
			t.Fatalf("the pass did not notice an abandoned roll lock: clear=%v err=%v", clear, err)
		}
		// Apply the clear through the SAME idempotent command path the pass uses, then assert the pass
		// stops writing — a reaper that re-DELETEs an absent key every 30s forever is the R-a risk.
		cmd, err := cluster.PlanClearUpgradeActive()
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.ExecCommand(db, cmd); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if clear, _ := upgradeLockDecision(db, dead.Add(time.Duration(i)*30*time.Second)); clear {
				t.Fatalf("tick %d re-proposed a clear against an already-released lock", i+1)
			}
		}
		// And the lease row went with it, so a future acquire cannot inherit a stale expiry.
		if _, ok := leaseRow(t, db, cluster.MetaKeyUpgradeLease); ok {
			t.Fatal("the lease survived the clear — the next acquire would start with a foreign expiry")
		}
	})

	// (c) two brokers ⇒ exactly one writer. Structural, like grow-lock: the pass is leader-only and its
	// clear goes through raft, so a follower neither evaluates nor proposes.
	t.Run("a follower neither evaluates nor proposes", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		follower := passBrokerFollower(t, db, clk)
		for i := 0; i < 10; i++ {
			follower.reconcilers.runDue(context.Background(), clk.advance(30*time.Second))
		}
		found := false
		for _, s := range follower.reconcilers.status() {
			if s.Name != "upgrade-lock" {
				continue
			}
			found = true
			if s.Runs != 0 {
				t.Fatalf("upgrade-lock ran %d times on a follower — two brokers would race the same replicated clear", s.Runs)
			}
			if s.Skips == 0 {
				t.Fatal("upgrade-lock never came due on a follower — it must be scheduled and gated, not unscheduled")
			}
		}
		if !found {
			t.Fatal("upgrade-lock is not registered at all")
		}
	})

	t.Run("single mode is inert", func(t *testing.T) {
		clk := newFakeClock(passEpoch)
		db := passTestDB(t)
		b := passBroker(t, db, clk)
		for i := 0; i < 3; i++ {
			if err := runPass(t, b, "upgrade-lock", clk.Now()); err != nil {
				t.Fatalf("single-mode upgrade-lock returned %v", err)
			}
		}
	})
}

// TestLockNotHeldCodeIsWireStable pins the terminal reply code both trigger handlers return to a renewal
// against a lock the caller no longer holds. The CLI keeper matches this literal
// (cmd/tether/cluster_lock_keeper.go: clusterLockNotHeldCode) and there is no compile-time link between
// the two packages, so a rename on either side would silently demote the TERMINAL case to "transient" —
// and the keeper would go on renewing a lock it does not own for the rest of the operation.
func TestLockNotHeldCodeIsWireStable(t *testing.T) {
	const wire = "lock_not_held"
	if growTriggerCodeLockNotHeld != wire {
		t.Fatalf("growTriggerCodeLockNotHeld = %q, want %q (the CLI keeper matches the literal)", growTriggerCodeLockNotHeld, wire)
	}
	if upgradeTriggerCodeLockNotHeld != wire {
		t.Fatalf("upgradeTriggerCodeLockNotHeld = %q, want %q", upgradeTriggerCodeLockNotHeld, wire)
	}
}

// TestLeaseTTLDerivationHolds pins the arithmetic the TTL comment argues, so a future edit to either
// constant cannot silently break the relationship the derivation depends on.
func TestLeaseTTLDerivationHolds(t *testing.T) {
	if cluster.LockLeaseRenewInterval*3 != cluster.LockLeaseTTL {
		t.Fatalf("the renew interval must be exactly TTL/3 (three chances to renew ⇒ two consecutive failures survivable); got TTL=%s interval=%s",
			cluster.LockLeaseTTL, cluster.LockLeaseRenewInterval)
	}
	// The derived floor: no-writable-leader window (upgradeConvergeTimeout, 3 min) + inter-broker skew
	// (upgradeTriggerSkew, 5 min) + one missed renewal slot.
	floor := 3*time.Minute + 5*time.Minute + cluster.LockLeaseRenewInterval
	if cluster.LockLeaseTTL < floor {
		t.Fatalf("LockLeaseTTL=%s is below its own derived floor %s — a healthy holder could be robbed during a normal leadership transfer",
			cluster.LockLeaseTTL, floor)
	}
	// And it must stay meaningfully bounded, or the fix is not a fix.
	if cluster.LockLeaseTTL > time.Hour {
		t.Fatalf("LockLeaseTTL=%s is long enough to be indistinguishable from the permanent fence it replaces", cluster.LockLeaseTTL)
	}
}

// TestUpgradeLockClearUsesTheExistingPath pins the one-vote-veto invariant for the new pass: it calls the
// SAME plan the CLI's release-lock trigger proposes, and invents nothing.
func TestUpgradeLockClearUsesTheExistingPath(t *testing.T) {
	cmd, err := cluster.PlanClearUpgradeActive()
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.Body) != 2 {
		t.Fatalf("expected the marker clear + its guarded lease clear, got %d", len(cmd.Body))
	}
	if !strings.Contains(cmd.Body[0].SQL, cluster.MetaKeyUpgradeActive) {
		t.Fatalf("the pass's clear must target the roll-lock marker; rendered: %s", cmd.Body[0].SQL)
	}
}

// origin: prerelease audit round 2, H-2.
//
// upgradeActive AND upgradeLockHeld ARE DIFFERENT QUESTIONS, and the acquire
// preflight was asking the wrong one.
//
//   - upgradeActive: "is the cluster mid-roll" — what fences `cluster join` /
//     `cluster retire`, and it must stay TRUE across a HALT, because a partial
//     roll is exactly when a membership change must not cross a restart.
//   - upgradeLockHeld: "is a roll still DRIVING" — what excludes another roll.
//
// Using the first for the second is what made a HALTed roll's own marker refuse
// the re-run its HALT message instructs the operator to perform, and block the
// emergency `--to-version <older>` rollback with it.
func TestTheAcquirePreflightAsksWhetherARollIsStillDriving(t *testing.T) {
	db := testharness.OpenDB(t)
	at := time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)
	setUpgradeMarker(t, db, at)

	if !upgradeLockHeld(db, at.Add(time.Minute)) {
		t.Fatal("a roll that acquired a minute ago is not seen as holding its own lock")
	}
	if !upgradeActive(db) {
		t.Fatal("premise broken: the marker is not set")
	}

	// The holder stopped renewing (a HALT, a kill -9, a snapped SSH).
	past := at.Add(cluster.LockLeaseTTL + time.Minute)
	if upgradeLockHeld(db, past) {
		t.Fatal("a roll that stopped renewing is still reported as holding the lock.\n\n" +
			"The operator was told to `fix and re-run to resume`; the re-run is then refused " +
			"for the whole lease TTL plus a reap interval — and so is the rollback they reach " +
			"for while the fleet sits mixed-version.")
	}
	// ...and the MEMBERSHIP fence is untouched by that.
	if !upgradeActive(db) {
		t.Fatal("an expired lease also unfenced join/retire.\n\n" +
			"The partial-roll fence is the whole reason a HALT keeps the marker instead of " +
			"releasing the lock, and it must outlive the lease that only excludes other rolls.")
	}

	// FAIL CLOSED: a marker whose lease cannot be evaluated is HELD. That is a
	// pre-R7b lock, which never expires on its own by design.
	if _, err := db.Exec(`DELETE FROM cluster_meta WHERE key=?`, cluster.MetaKeyUpgradeLease); err != nil {
		t.Fatal(err)
	}
	if !upgradeLockHeld(db, past.Add(100*cluster.LockLeaseTTL)) {
		t.Fatal("a marker with NO lease was treated as expired.\n\n" +
			"That is what a lock acquired by an older broker looks like, and lock_lease.go's " +
			"contract is that it never expires by itself — `tether cluster unlock` is the out.")
	}
}
