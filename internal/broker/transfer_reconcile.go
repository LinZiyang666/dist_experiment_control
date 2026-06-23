package broker

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// reconcileXferObjectsOnBoot scans every per-session xfer-<sid>
// bucket and deletes objects that don't correspond to an in-flight
// transfer. After a restart the in-memory tracker is empty so EVERY
// pre-existing object is considered orphan — the receiver-finalization
// invariant guarantees no useful work was in progress that could
// resume from a stale object.
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
func (b *Broker) reconcileXferObjectsOnBoot(ctx context.Context) (int, error) {
	if b.js == nil {
		return 0, nil
	}
	active := b.transfers.activeOBJStreams()
	infos := b.js.ListStreams(ctx)
	deleted := 0
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
			// `active` is keyed by stream name — if the bucket has
			// any in-flight transfers, the in-memory tracker would
			// have re-registered them on agent re-register. After
			// a fresh broker boot the tracker is empty, so every
			// object in any xfer bucket is orphan.
			if _, busy := active[name]; busy {
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
	return deleted, infos.Err()
}
