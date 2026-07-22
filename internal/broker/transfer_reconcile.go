package broker

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// xferReapMinObjectAge is the grace period below which an xfer object is NEVER reaped, no matter what
// the in-flight tracker says (external review M-3). The per-bucket busy re-read closes most of the
// snapshot race, but an object created between that check and the Delete — or one a mid-transfer rehome
// left on a broker whose tracker no longer knows about it — must not be torn out mid-upload. A genuinely
// orphan object simply ages past this window and is reaped on a later cycle; the cost of the grace is one
// extra idle interval, the cost of skipping it is a corrupted in-flight transfer.
const xferReapMinObjectAge = 2 * time.Minute

// reconcileXferObjects scans every per-session xfer-<sid>
// bucket and deletes objects that don't correspond to an in-flight
// transfer. After a restart the in-memory tracker is empty so EVERY
// pre-existing object is considered orphan — the receiver-finalization
// invariant guarantees no useful work was in progress that could
// resume from a stale object.
//
// R7 (#58/P10) — this used to be named ...OnBoot and was called from exactly one
// place: Run's JetStream probe. That single call site is why tier-B garbage was
// immortal on a cluster. reaperMayDelete() below requires a CAUGHT-UP LEADER,
// and at the moment Run probes JetStream no cluster-mode broker has established
// raft leadership yet — so the one call it got returned 0 without scanning, and
// there was never a second one. The fix is not in this function (it was always
// correct); it is that the registry now calls it on XferReapInterval, so it runs
// again as soon as leadership and catch-up actually arrive.
//
// Running periodically is SAFER than running at boot, not riskier: the
// in-flight guard below keys off transfers.activeOBJStreams(), and a tracker
// entry (bucket already populated) is put() before the prepare is forwarded to
// the agent — i.e. before any object can exist. At boot the tracker is empty and
// everything looks orphan; at steady state live work is excluded by name.
//
// v0.2.2 change: previously this function deleted whole OBJ_xfer-*
// streams (one per transfer in the old design). Now buckets are
// per-session and survive across transfers + restarts; only stale
// OBJECTS get cleaned. The bucket itself is left alone so the next
// transfer in this session doesn't have to recreate it.
//
// Returns the count of deleted objects. Per-bucket errors are logged
// and skipped; the returned error reflects only the top-level
// ListStreams call.
func (b *Broker) reconcileXferObjects(ctx context.Context) (int, error) {
	if b.js == nil {
		return 0, nil
	}
	// audit G: a not-caught-up node's empty in-memory tracker would treat live cluster-wide objects as
	// orphan. reaperCaughtUp requires this broker's raft-domain view to be current (Node.CaughtUp:
	// synced-with-a-leader-once CommitIndex>0 AND RaftAppliedIndex >= CommitIndex) — a caught-up FOLLOWER
	// as much as a leader. It deliberately does NOT require leadership:
	// the per-bucket homeOwnsXferBucket gate below already partitions the blast radius to buckets whose
	// session is entirely homed HERE, so "am I this bucket's home, on a current view" is the correct
	// authority — not "am I the leader". Requiring leader AND home is exactly the #58/P10 bug: a session
	// homed to a non-leader broker was unreapable by any node (the leader failed the home gate, the home
	// failed the leader gate), so tier-B garbage was immortal on every cluster whose transfers were not
	// homed to the raft leader. See reaperCaughtUp for why the catch-up gate also closes the reassignment
	// split-view race.
	if !b.reaperCaughtUp() {
		return 0, nil
	}
	infos := b.js.ListStreams(ctx)
	deleted := 0
	unreapable := 0 // N-6: buckets no broker can ever reap that hold aged garbage (observability only)
	for info := range infos.Info() {
		name := info.Config.Name
		if !strings.HasPrefix(name, "OBJ_xfer-") {
			continue
		}
		// D8 §9: in clustered mode the bucket is REPLICATED across brokers — only the
		// broker that is the home of every node bound to the session may reap it, else this
		// broker's empty-tracker boot reap would delete another broker's LIVE in-flight
		// object. Inert in production (selfID==""): homeOwnsXferBucket is always true.
		sid := strings.TrimPrefix(name, "OBJ_xfer-")
		if !b.homeOwnsXferBucket(sid) {
			// N-6: a bucket NO broker can ever reap (split-home / zero-node session) that actually holds
			// AGED orphan garbage is the racknerd small-disk-fill class — count it for the gauge + warn so
			// an operator sees it before the disk fills. Observability ONLY: the reaper deletes nothing
			// extra here (that would be #58's per-transfer-owner product fix), and this never softens the
			// #58 ledger status. Counting every non-home skip would be ~always nonzero on a healthy
			// cluster (each bucket is non-home on N-1 brokers), so the orphaned-EVERYWHERE + aged-objects
			// predicate is what makes the number mean something. Cluster mode only (selfID!="").
			if b.selfID != "" && b.xferBucketOrphanedEverywhere(sid) && b.xferBucketHasAgedObjects(ctx, name) {
				unreapable++
				b.cfg.Logger.Warn("broker: xfer bucket unreapable by ANY broker (split-home or zero-node session) "+
					"holds aged orphan objects that will accumulate — the #58 per-transfer-owner refinement retires this",
					"bucket", name, "sid", sid)
			}
			continue
		}
		// M-3: re-read the in-flight set PER BUCKET rather than snapshotting it once at sweep start.
		// A transfer that put() its tracker entry AFTER the sweep began (but before this bucket is
		// processed) is otherwise invisible, and its just-completed objects would be reaped. Re-reading
		// here shields a bucket the instant a transfer registers.
		if _, busy := b.transfers.activeOBJStreams()[name]; busy {
			continue
		}
		bucket := strings.TrimPrefix(name, "OBJ_")
		store, err := b.js.ObjectStore(ctx, bucket)
		if err != nil {
			b.cfg.Logger.Warn("broker: open xfer bucket for reconcile",
				"bucket", bucket, "err", err)
			continue
		}
		objs, err := store.List(ctx)
		if err != nil {
			// Empty bucket → ErrNoObjectsFound; not an error.
			if errors.Is(err, jetstream.ErrNoObjectsFound) {
				continue
			}
			b.cfg.Logger.Warn("broker: list xfer objects for reconcile",
				"bucket", bucket, "err", err)
			continue
		}
		for _, obj := range objs {
			if obj.Deleted {
				continue
			}
			// M-3: never reap a FRESH object. The per-bucket busy re-read above closes most of the
			// snapshot race, but an object created between that check and this Delete — or one a
			// mid-transfer rehome left on a broker whose tracker no longer tracks it — must not be
			// torn out mid-upload. A genuinely orphan object ages past the grace and is reaped later.
			if b.xferReapMinAge > 0 && !obj.ModTime.IsZero() && b.now().Sub(obj.ModTime) < b.xferReapMinAge {
				continue
			}
			if err := store.Delete(ctx, obj.Name); err != nil &&
				!errors.Is(err, jetstream.ErrObjectNotFound) {
				b.cfg.Logger.Warn("broker: orphan xfer object delete",
					"bucket", bucket, "name", obj.Name, "err", err)
				continue
			}
			deleted++
		}
		b.cfg.Logger.Info("broker: orphan xfer objects reaped",
			"bucket", bucket, "deleted_so_far", deleted)
	}
	// N-6: publish the latest unreapable-bucket count for the /metrics gauge + status. Store (not Add):
	// it is a gauge that must fall back to 0 once topology heals the split-home / zero-node condition.
	b.xferUnreapableBuckets.Store(int64(unreapable))
	return deleted, infos.Err()
}

// xferBucketHasAgedObjects reports whether the OBJ_xfer bucket `name` holds at least one non-deleted
// object past the reap grace — i.e. real accumulating garbage, not an empty or all-fresh bucket. Used
// ONLY to keep the N-6 unreapable-bucket gauge meaningful (an empty split-home bucket is no disk risk).
// A missing bucket / empty list / transient error is "no aged objects" (fail-quiet: the gauge must not
// over-count on a read blip).
func (b *Broker) xferBucketHasAgedObjects(ctx context.Context, name string) bool {
	bucket := strings.TrimPrefix(name, "OBJ_")
	store, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		return false
	}
	objs, err := store.List(ctx)
	if err != nil {
		return false // ErrNoObjectsFound (empty) or a transient error — not counted
	}
	for _, obj := range objs {
		if obj.Deleted {
			continue
		}
		if b.xferReapMinAge <= 0 || obj.ModTime.IsZero() || b.now().Sub(obj.ModTime) >= b.xferReapMinAge {
			return true
		}
	}
	return false
}
