package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// cluster_lock_keeper.go (R7b) — the HOLDER half of the membership-lock lease.
//
// WHY THIS FILE IS LOAD-BEARING, NOT A NICETY
// -------------------------------------------
// internal/cluster/lock_lease.go stamps a lease (now+LockLeaseTTL) into the SAME raft command that
// acquires either membership marker, and internal/broker/reconcile_{grow,upgrade}_lock.go reap a marker
// whose lease has expired. Those two halves alone are not a fix — they are a REGRESSION. Without a holder
// that keeps renewing, EVERY lock expires LockLeaseTTL after it was taken, whether or not its orchestrator
// is alive and working. A `cluster add` is allowed to spend opCatchupMaxWindow (30 min) waiting on an
// InstallSnapshot, so a perfectly healthy grow would lose its mutex halfway through and a concurrent
// join/retire/upgrade could then cross it. That is strictly worse than the leak this batch removes: the
// leak BLOCKS membership, an unrenewed lease CORRUPTS it.
//
// So the reaper and this keeper must ship together, and the invariant tying them is:
//
//	a lock is expired  ⟺  nobody is renewing it  ⟺  no orchestrator is driving the operation
//
// WHAT A RENEWAL IS ALLOWED TO DO
// -------------------------------
// Nothing except push the expiry forward. The broker-side handlers propose PlanRenew{Grow,Upgrade}Lease,
// whose statements are gated on the marker still existing (and, for grow, still naming THIS joiner), so a
// renewal is structurally incapable of CREATING a lock. That is what makes a keeper goroutine that outlives
// its release by a tick harmless, and it is why Stop() does not need to be ordered before the release.
//
// LEADERSHIP MOVES UNDER US (and that is the normal case, not the exception)
// -------------------------------------------------------------------------
// A renewal is a leader Propose, and `cluster upgrade` DELIBERATELY moves leadership off every host it
// rolls. The keeper therefore re-resolves the writable leader on every single renewal rather than caching
// the one it was started with; a cached leader would fail every renewal for the rest of the roll and the
// lock would expire mid-upgrade — the exact corruption above.
//
// FAILURE POSTURE
// ---------------
//   - transient failure (no leader visible, dropped reply, NotLeader): keep trying. The TTL is sized
//     (lock_lease.go) to survive two consecutive misses, so a single election is a non-event.
//   - TERMINAL failure (the broker replies lock_not_held): the lock we believe we hold is gone — released,
//     reaped, or taken over. Retrying cannot re-acquire it, so the keeper STOPS renewing and latches the
//     fact. The orchestrator reads that latch and refuses to exit 0, because from that instant on it was
//     driving a membership change without the mutex that serializes it.

// clusterLockNotHeldCode is the broker's terminal reply code for a renewal against a lock the caller no
// longer holds. It is a WIRE value: the broker declares the same literal as growTriggerCodeLockNotHeld /
// upgradeTriggerCodeLockNotHeld, and TestLockNotHeldCodeIsWireStable (internal/broker) pins both ends to
// this literal so the two cannot drift apart silently — a drifted code would demote the terminal case to
// "transient", and the keeper would renew a lock it does not own until the operation ended.
const clusterLockNotHeldCode = "lock_not_held"

// lockRenewFn performs ONE renewal. It reports whether the caller still HOLDS the lock; (false, nil) is the
// terminal "somebody else owns this now" answer, while a non-nil error is transient by definition — the
// renewal could not be established either way, which is not evidence the lock is gone.
type lockRenewFn func(ctx context.Context) (held bool, err error)

// lockKeeper renews one membership lock's lease until it is stopped, the lock is lost, or ctx ends.
//
// It owns exactly one goroutine and one ticker, and Stop() JOINS that goroutine before returning, so the
// keeper is invisible to the repo's NumGoroutine/fd leak gate: by the time any orchestrator returns, its
// keeper has fully exited.
type lockKeeper struct {
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	renewals atomic.Int64 // successful renewals (tests + operator-visible accounting)
	lost     atomic.Bool  // latched: the broker told us the lock is no longer ours
	lostWhy  atomic.Value // string, set exactly once alongside lost
}

// startLockKeeper launches the renewal loop. interval is a parameter rather than a read of
// cluster.LockLeaseRenewInterval so hermetic tests can compress a 5-minute cadence into microseconds;
// production always passes cluster.LockLeaseRenewInterval.
//
// The first renewal fires one full interval in, not immediately: the acquire command already stamped a
// full-TTL lease in the same transaction, so an immediate renewal would be a redundant raft write on every
// single membership operation.
func startLockKeeper(ctx context.Context, label string, interval time.Duration, renew lockRenewFn, out io.Writer) *lockKeeper {
	k := &lockKeeper{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(k.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-k.stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
			}
			held, err := renew(ctx)
			switch {
			case err != nil:
				// Transient. Say so once per occurrence — an operator watching a long grow needs to
				// know renewals are failing BEFORE the lease runs out, not after.
				_, _ = fmt.Fprintf(out, "  ⚠ %s lock lease renewal failed (transient; the lease survives ~%s more of these): %v\n",
					label, interval, err)
			case !held:
				// Terminal. Report FIRST, then latch, then stop: no future renewal can re-acquire.
				//
				// The ordering is load-bearing. lost is the only thing an observer polls, so latching it
				// last makes "Lost() is true" imply "the message has already been written" — without that
				// edge, a caller that reacts to Lost() races the Fprintf above it.
				_, _ = fmt.Fprintf(out, "  ⚠ %s lock LOST: the broker no longer records this orchestrator as the holder. "+
					"Another operation may be running concurrently — this run will NOT report success.\n", label)
				k.lostWhy.Store(fmt.Sprintf("the broker reports the %s lock is no longer held by this orchestrator", label))
				k.lost.Store(true)
				return
			default:
				k.renewals.Add(1)
			}
		}
	}()
	return k
}

// Stop ends the renewal loop and WAITS for its goroutine to exit. Safe to call more than once (the
// orchestrators call it both explicitly on the success path and via defer on every HALT path).
func (k *lockKeeper) Stop() {
	if k == nil {
		return
	}
	k.stopOnce.Do(func() { close(k.stop) })
	<-k.done
}

// Lost reports whether the lock was taken away from this orchestrator while it was still driving.
func (k *lockKeeper) Lost() bool { return k != nil && k.lost.Load() }

// LostErr is the non-zero exit an orchestrator owes its caller when Lost() is true. Finishing the work is
// NOT the same as finishing it safely: from the moment the lock left us, the mutex that serializes
// membership changes was not held, so another `cluster add`/`retire`/`upgrade` may have been interleaved
// with ours. Reporting rc=0 would tell automation a serialized operation completed when it did not.
func (k *lockKeeper) LostErr(what string) error {
	why, _ := k.lostWhy.Load().(string)
	if why == "" {
		why = "the lock was lost"
	}
	return unavailErr("%s: %s. The work itself may have completed, but it ran WITHOUT the membership mutex "+
		"for part of its duration, so it was not serialized against other membership changes. Verify with "+
		"`tether cluster status` and `tether cluster ops ls` before running any further membership operation", what, why)
}

// Renewals reports how many renewals landed (tests).
func (k *lockKeeper) Renewals() int64 { return k.renewals.Load() }
