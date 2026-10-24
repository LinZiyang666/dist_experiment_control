package xferserve

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats.go/jetstream"
)

// LegacyReaper owns cap0 object enumeration and chunk retirement. The broker
// supplies its admission guards and age policy; capable objects use Finalizer.
// origin: hot-update S7 plan §5.3 / P31
type LegacyReaper struct {
	JS     jetstream.JetStream
	Now    func() time.Time
	Logger interface {
		Warn(string, ...any)
		Info(string, ...any)
	}
}

func (r LegacyReaper) ReapBucketObjects(until time.Time, stop <-chan struct{}, name string, floorFor func(int64) time.Duration,
	protected, unresolvedRows map[string]bool) (deleted int) {
	// Direct calls share the pass deadline and synchronously join List's watcher.
	// origin: hot-update S7 plan lead LQ3 / T30; FIX-REAP rv-wiring F7
	ctx, cancel := legacyReapContext(until, stop)
	defer cancel()
	bucket := strings.TrimPrefix(name, "OBJ_")
	store, err := r.JS.ObjectStore(ctx, bucket)
	if err != nil {
		r.Logger.Warn("broker: open xfer bucket for reconcile", "bucket", bucket, "err", err)
		return 0
	}
	// Deleted objects are listed too. The chunk sweep below treats a tombstone's chunk group as garbage
	// either way — it builds its referenced set from NON-deleted objects only, so a tombstone that is
	// listed (Deleted=true) and one that is not listed at all produce the same set (round-2 review
	// R4-2-F11: the option is not what makes TOMB groups purgeable). It is kept so the object loop SEES
	// the tombstones it skips: a ctl/agent Delete writes the tombstone but its PURGE is denied by the ACL,
	// so the chunks stay (gotcha #85), and a listing that hid those objects would make that leak invisible
	// to anyone reading this loop. An empty object list is NOT an early return any more: a bucket with no
	// objects at all can still hold the chunks of an abandoned Put, which is exactly the leak.
	objs, err := store.List(ctx, jetstream.ListObjectsShowDeleted())
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		r.Logger.Warn("broker: list xfer objects for reconcile", "bucket", bucket, "err", err)
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
		// An object whose ledger row could not be READ is protected individually.
		// The object name IS the transfer id at every writer, so this is the same
		// exact identity match the protected set uses, applied to the rows whose
		// contents we do not have (prerelease audit BT-F3).
		if unresolvedRows[xferledger.LegacyFilename(obj.Name)] {
			continue
		}
		floor := floorFor(int64(obj.Size))
		if floor > 0 && !obj.ModTime.IsZero() && r.Now().Sub(obj.ModTime) < floor {
			continue
		}
		if err := store.Delete(ctx, obj.Name); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
			r.Logger.Warn("broker: orphan xfer object delete", "bucket", bucket, "name", obj.Name, "err", err)
			continue
		}
		deleted++
	}
	// The object loop above sees only what has a meta entry. The chunk sweep sees what does not.
	deleted += reapOrphanChunks(ctx, r.JS, name, objs, floorFor, r.Now(), r.Logger)
	return deleted
}

// xferChunkSize is nats.go's object-store chunk size (objDefaultChunkSize, 128 KiB); tether never sets
// ObjectMeta.Opts.ChunkSize, so a chunk group's byte size is its message count times this. It is only an
// estimate fed to the size-aware floor, never a correctness input.
const xferChunkSize = 128 << 10

// reapOrphanChunks purges the chunk groups of the OBJ_xfer bucket `name` that NO object references — the
// storage half of an abandoned tier-B Put (gotcha #85).
//
// WHY THE OBJECT REAPER CANNOT SEE THEM. nats.go's ObjectStore.Put publishes the chunks first, on ONE
// subject per object (`$O.<bucket>.C.<nuid>`), and the meta entry (`$O.<bucket>.M.<name>`) last. A Put
// that is cancelled (the #84 watchdog / bounded retry), that times out, or whose ctl dies mid-upload
// leaves the chunks with no meta; nats.go's own cleanup for that case is `STREAM.PURGE` with a subject
// filter, which the ctl's and the agent's ACLs DENY by design (file-transfer-plan Round-4 #3: bucket
// lifecycle is the broker's). That denial was harmless when a bucket was per-transfer and the broker
// deleted the whole bucket on failure. Since v0.2.2 the bucket is per-SESSION and lives for ever, the
// broker reaps per OBJECT through store.List — and a chunk group with no meta is not an object, so
// nobody has ever deleted one: every failed Put has been leaving up to its full size behind, counted
// against the bucket's MaxBytes, until the refusals turned into `insufficient storage`. A deleted object
// has the same shape: the ctl/agent Delete writes the tombstone, its PURGE is denied, the chunks stay.
//
// WHAT IS SAFE TO PURGE. A chunk group whose NUID no live object references AND whose NEWEST chunk is
// older than the caller's floor. The floor is the same one the object reaper applies (per-home grace,
// cross-home extra, the uptime term that shields a previous incarnation's live uploads) — an upload in
// flight keeps writing chunks (a 2 GiB Put at the 2 MiB/s admission floor writes one every 60 ms; one
// stalled longer than 30 s is being cancelled by the ctl's watchdog), so "newest chunk older than the
// grace" is what distinguishes abandoned from live without knowing the transfer id, which the chunk
// group does not carry. The per-bucket busy skip at the call site (any local in-flight transfer → the
// whole bucket is skipped) still applies above this function.
//
// A package-level function taking the JetStream handle, not a Broker method: the structural-budget
// ratchet pins the type's method count (see brokerUptime).
// origin: S7 internal review GATES-PROD F1
func ReapOrphanChunks(until time.Time, stop <-chan struct{}, js jetstream.JetStream, name string, objs []*jetstream.ObjectInfo,
	floorFor func(int64) time.Duration, now time.Time, logger interface {
		Warn(string, ...any)
		Info(string, ...any)
	}) int {
	ctx, cancel := legacyReapContext(until, stop)
	defer cancel()
	return reapOrphanChunks(ctx, js, name, objs, floorFor, now, logger)
}

func reapOrphanChunks(ctx context.Context, js jetstream.JetStream, name string, objs []*jetstream.ObjectInfo,
	floorFor func(size int64) time.Duration, now time.Time, logger interface {
		Warn(string, ...any)
		Info(string, ...any)
	}) (purged int) {
	if js == nil {
		return 0
	}
	bucket := strings.TrimPrefix(name, "OBJ_")
	chunkPrefix := "$O." + bucket + ".C."
	stream, err := js.Stream(ctx, name)
	if err != nil {
		logger.Warn("broker: open xfer stream for chunk sweep", "bucket", bucket, "err", err)
		return 0
	}
	si, err := stream.Info(ctx, jetstream.WithSubjectFilter(chunkPrefix+">"))
	if err != nil {
		logger.Warn("broker: xfer stream info for chunk sweep", "bucket", bucket, "err", err)
		return 0
	}
	if len(si.State.Subjects) == 0 {
		return 0
	}
	referenced := make(map[string]bool, len(objs))
	for _, o := range objs {
		if o != nil && !o.Deleted {
			referenced[o.NUID] = true
		}
	}
	for subj, count := range si.State.Subjects {
		nuid := strings.TrimPrefix(subj, chunkPrefix)
		if nuid == "" || referenced[nuid] {
			continue
		}
		last, err := stream.GetLastMsgForSubject(ctx, subj)
		if err != nil {
			logger.Warn("broker: orphan chunk group newest message", "bucket", bucket, "nuid", nuid, "err", err)
			continue
		}
		floor := floorFor(int64(count) * xferChunkSize)
		if floor > 0 && !last.Time.IsZero() && now.Sub(last.Time) < floor {
			continue
		}
		if err := stream.Purge(ctx, jetstream.WithPurgeSubject(subj)); err != nil {
			logger.Warn("broker: orphan chunk group purge", "bucket", bucket, "nuid", nuid, "err", err)
			continue
		}
		logger.Info("broker: orphan xfer chunk group purged (a Put that never wrote its meta, or a client-side delete whose purge the ACL denied)",
			"bucket", bucket, "nuid", nuid, "chunks", count)
		purged++
	}
	return purged
}

// xferBucketHasAgedObjects reports whether the OBJ_xfer bucket `name` holds at least one non-deleted
// object past the reap grace — i.e. real accumulating garbage, not an empty or all-fresh bucket. Used
// ONLY to keep the N-6 unreapable-bucket gauge meaningful (an empty split-home bucket is no disk risk).
// A missing bucket / empty list / transient error is "no aged objects" (fail-quiet: the gauge must not
// over-count on a read blip).
func (r LegacyReaper) HasAgedObjects(until time.Time, stop <-chan struct{}, name string, minAge time.Duration) bool {
	// Direct calls share the pass deadline and synchronously join List's watcher.
	// origin: hot-update S7 plan lead LQ3 / T30; FIX-REAP rv-wiring F7
	ctx, cancel := legacyReapContext(until, stop)
	defer cancel()
	bucket := strings.TrimPrefix(name, "OBJ_")
	store, err := r.JS.ObjectStore(ctx, bucket)
	if err != nil {
		return false
	}
	objs, err := store.List(ctx)
	if err != nil {
		return false // ErrNoObjectsFound (empty) or a transient error — not counted
	}
	return xfergc.LegacyHasAgedObjects(objs, minAge, r.Now())
}

// One original pass deadline bounds every SDK call. Cancellation is a signal,
// never an imported control context; cleanup joins the signal watcher before
// returning so neither it nor List can outlive the synchronous reaper call.
// origin: S7 internal review GATES-PROD F1 / hot-update S7 plan lead LQ3
func legacyReapContext(until time.Time, stop <-chan struct{}) (context.Context, context.CancelFunc) {
	ceiling := time.Now().Add(5 * time.Second)
	if until.IsZero() || until.After(ceiling) {
		until = ceiling
	}
	ctx, cancel := context.WithDeadline(context.Background(), until) // ctx-none: deadline and cancellation signal are the reaper's bounded operation port.
	select {
	case <-stop:
		cancelLegacyReapBeforeDeadline(until, cancel)
	default:
	}
	if stop == nil {
		return ctx, cancel
	}
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-stop:
			cancelLegacyReapBeforeDeadline(until, cancel)
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-joined }
}

// The parent's deadline signal can win before this operation's timer runs.
// Canceling at that point would replace DeadlineExceeded with Canceled; let
// the original deadline finish the SDK call with its authoritative reason.
// origin: S7 internal review GATES-PROD F1 / hot-update S7 plan lead LQ3
func cancelLegacyReapBeforeDeadline(until time.Time, cancel context.CancelFunc) {
	if time.Now().Before(until) {
		cancel()
	}
}
