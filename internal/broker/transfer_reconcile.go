package broker

import (
	"context"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// reconcileXferStreamsOnBoot lists all JetStream streams with the
// "OBJ_xfer-" prefix and deletes any that the in-process tracker
// does NOT currently know about. After a broker restart the tracker
// is empty, so this effectively deletes every leftover OBJ_xfer-
// stream — which is exactly the file-transfer-plan §Object bucket
// lifecycle G.2 reconcile rule: "after a restart, no in-flight
// transfer can be honored; reap any leftover buckets so they don't
// accumulate forever."
//
// Returns the count of deletions. Errors per-stream are logged but
// not propagated; the overall function returning an error covers the
// initial ListStreams call only.
func (b *Broker) reconcileXferStreamsOnBoot(ctx context.Context) (int, error) {
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
		if _, keep := active[name]; keep {
			continue
		}
		// Translate stream name "OBJ_<bucket>" back to bucket for
		// DeleteObjectStore which takes the bucket name.
		bucket := strings.TrimPrefix(name, "OBJ_")
		if err := b.js.DeleteObjectStore(ctx, bucket); err != nil &&
			err != jetstream.ErrBucketNotFound &&
			err != jetstream.ErrStreamNotFound {
			b.cfg.Logger.Warn("broker: orphan OBJ_xfer delete",
				"stream", name, "err", err)
			continue
		}
		b.cfg.Logger.Info("broker: orphan OBJ_xfer reaped", "stream", name)
		deleted++
	}
	return deleted, infos.Err()
}
