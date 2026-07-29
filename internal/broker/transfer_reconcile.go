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
	// origin: batch-c EXTERNAL review B2. The in-memory tracker consulted below is rebuilt EMPTY by a
	// restart, so "no tracker entry" has never meant "nobody owns this object" — it also means "this
	// process was not the one that started it". That was survivable while the tier-B watchdog was a
	// fixed 5 minutes against a 2-minute orphan grace. It stopped being survivable when batch C made
	// the budget size-derived (up to 35m08s): a restart mid-transfer left roughly half an hour in which
	// the reaper would delete a live object two minutes in.
	//
	// The durable evidence exists and always has — the #57 xfer-inflight ledger records the transfer
	// id, bucket, size and start time, and the object's NAME IS THE TRANSFER ID at every writer. So the
	// protection is an exact identity match rather than a heuristic. A ledger that cannot be read is
	// treated as "delete nothing this pass": acting on evidence we could not read is precisely the
	// failure this guard exists to prevent.
	protected, lerr := b.ledgerProtectedObjects(b.now())
	if lerr != nil {
		b.cfg.Logger.Warn("broker: xfer ledger unreadable — skipping the orphan reap this pass rather "+
			"than deleting objects on evidence we could not read", "err", lerr)
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
			// R16 #58 Lane C: a bucket NO home can reap (split-home / zero-node session — homeOwnsXferBucket
			// is false on EVERY broker) is the racknerd small-disk-fill class. The caught-up LEADER performs a
			// cross-home GC of its AGED orphan objects: ONE broker (the leader) does it so no two race, only
			// when orphaned-EVERYWHERE (no home owns the whole session), and with a LONGER age floor than the
			// per-home grace (a transfer still live on ANOTHER home must never be torn out across clock skew).
			// A follower cannot GC (leader-only) but still counts the gauge so accumulation stays visible.
			// Cluster mode only (selfID!=""); in production single-broker mode homeOwnsXferBucket is always true.
			if b.selfID != "" && b.xferBucketOrphanedEverywhere(sid) {
				// M6: NEVER cross-home GC a bucket with a LIVE local tracker entry — a split-home session where
				// the leader itself homes one node and holds an in-flight transfer. Its live object must not be
				// torn out even under a compressed cross-home floor (the drill sets ≥1s). A busy bucket is live
				// work, not accumulating garbage → skip the GC AND the gauge (tracker key == the OBJ_xfer-<sid> name).
				if _, busy := b.transfers.activeOBJStreams()[name]; busy {
					continue
				}
				// The caught-up LEADER GCs objects past the LONGER cross-home floor (only the leader, so no two
				// brokers race; the longer floor protects a transfer still live on another home across clock skew).
				if b.reaperMayDelete() {
					if reaped := b.reapBucketObjects(ctx, name, b.crossHomeReapAge(), true, protected); reaped > 0 {
						deleted += reaped
						b.cfg.Logger.Info("broker: cross-home GC reaped aged orphan xfer objects (split-home/zero-node bucket)",
							"bucket", name, "sid", sid, "deleted", reaped)
					}
				}
				// Gauge (N-6b): any bucket STILL holding aged garbage (per the per-home grace) AFTER the GC is
				// accumulating disk risk — a follower (leader-only GC), or objects not yet past the longer
				// cross-home floor. Computed independently of the GC so the number stays honest; it falls to 0
				// once the leader reaps everything past the floor (the drill-observable #58-fixed signal).
				if b.xferBucketHasAgedObjects(ctx, name) {
					unreapable++
					b.cfg.Logger.Warn("broker: xfer bucket unreapable by any HOME (split-home or zero-node session) holds "+
						"aged orphan objects — the leader's cross-home GC (#58) reclaims those past the cross-home age floor",
						"bucket", name, "sid", sid)
				}
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
		reaped := b.reapBucketObjects(ctx, name, b.xferReapMinAge, false, protected)
		deleted += reaped
		if reaped > 0 {
			b.cfg.Logger.Info("broker: orphan xfer objects reaped",
				"bucket", name, "deleted_so_far", deleted)
		}
	}
	// N-6: publish the latest unreapable-bucket count for the /metrics gauge + status. Store (not Add):
	// it is a gauge that must fall back to 0 once topology heals the split-home / zero-node condition.
	b.xferUnreapableBuckets.Store(int64(unreapable))
	return deleted, infos.Err()
}

// reapBucketObjects deletes every non-deleted object in the OBJ_xfer bucket `name` older than minAge and
// returns the count deleted. Shared by the home-owned reap (per-home grace b.xferReapMinAge) and the #58
// leader cross-home GC (the longer crossHomeReapAge). minAge<=0 reaps everything (the zero-value test
// broker's prompt-reap semantics).
// sizeAware (batch C) extends minAge PER OBJECT by whatever its size-derived watchdog budget exceeds
// the tier floor by. The cross-home GC passes true: it is deleting another home's objects, and since
// the watchdog became size-derived a single global floor can no longer bound "still live over there".
// The home-owned reap passes false — its short grace is protected by the per-bucket busy re-read
// instead, and widening it would make orphan garbage linger on exactly the small-disk brokers this
// sweep exists to protect.
func (b *Broker) reapBucketObjects(ctx context.Context, name string, minAge time.Duration, sizeAware bool, protected map[string]bool) (deleted int) {
	bucket := strings.TrimPrefix(name, "OBJ_")
	store, err := b.js.ObjectStore(ctx, bucket)
	if err != nil {
		b.cfg.Logger.Warn("broker: open xfer bucket for reconcile", "bucket", bucket, "err", err)
		return 0
	}
	objs, err := store.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) { // empty bucket — not an error
			return 0
		}
		b.cfg.Logger.Warn("broker: list xfer objects for reconcile", "bucket", bucket, "err", err)
		return 0
	}
	for _, obj := range objs {
		if obj.Deleted {
			continue
		}
		// Never reap a FRESH object: the per-bucket busy re-read closes most of the snapshot race, but an
		// object created between that check and this Delete — or one a mid-transfer rehome left on a broker
		// whose tracker no longer tracks it — must not be torn out mid-upload. It ages past the grace and is
		// reaped later. The cross-home GC uses a longer floor so a peer-home's live transfer is never torn out.
		// EXTERNAL review B2: the durable ledger outranks every age heuristic below. A row that is
		// still inside its own budget means a live transfer owns this object, whatever this process's
		// in-memory tracker happens to remember.
		if protected[bucket+"/"+obj.Name] {
			continue
		}
		floor := minAge
		if sizeAware && floor > 0 {
			floor += xferCrossHomeExtraFor(int64(obj.Size))
		}
		if floor > 0 && !obj.ModTime.IsZero() && b.now().Sub(obj.ModTime) < floor {
			continue
		}
		if err := store.Delete(ctx, obj.Name); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			b.cfg.Logger.Warn("broker: orphan xfer object delete", "bucket", bucket, "name", obj.Name, "err", err)
			continue
		}
		deleted++
	}
	return deleted
}

// crossHomeReapAge is the effective age floor for the #58 leader cross-home GC — the serveconf override when
// set (a deploy-tier drill compresses it), else the derived xferCrossHomeReapAge default.
func (b *Broker) crossHomeReapAge() time.Duration {
	if b.cfg.XferCrossHomeReapAge > 0 {
		return b.cfg.XferCrossHomeReapAge
	}
	return xferCrossHomeReapAge
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
