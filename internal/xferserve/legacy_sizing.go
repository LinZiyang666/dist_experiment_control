package xferserve

import (
	"fmt"
	"math"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/nats-io/nats.go/jetstream"
)

// Sizing preserves the pre-S7 cap0 admission units and overflow checks.
// origin: hot-update S7 plan §7 / P22
const (
	xferBucketFloor       = 256 * 1024 * 1024
	xferBucketChunkMargin = 64 * 1024 * 1024
	xferBucketCap         = proto.XferMaxBytes + xferBucketChunkMargin
)

// xferReserveFor is what the NON-xfer JetStream streams reserve on this broker: the single global
// events stream plus one history stream PER SESSION, each multiplied by the replica factor.
//
// The replica multiply is not decoration. nats-server charges the account-level reservation as
// Replicas*MaxBytes (jetstream_api.go tieredReservation) while the server-level one is un-multiplied
// (jetstream.go, js.storeReserved += cfg.MaxBytes). The two levels genuinely disagree; sizing against
// the LOOSER of them would hand a 3-replica cluster a bucket the account check then refuses. Production
// is force-single N=1 so both agree there — which is precisely why this would have shipped un-noticed.
//
// This replaces a hand-copied 2 GiB constant that was correct only at exactly one session and one
// replica. The inputs are now the exported jsstream constants, so editing either one there cannot
// silently falsify the arithmetic here.
func LegacyReserveFor(sessions, replicas int) int64 {
	if sessions < 1 {
		sessions = 1
	}
	if replicas < 1 {
		replicas = 1
	}
	if int64(sessions) > (math.MaxInt64-jsstream.EventsMaxBytes)/jsstream.HistoryMaxBytesPerSession {
		return math.MaxInt64
	}
	perReplica := jsstream.EventsMaxBytes + int64(sessions)*jsstream.HistoryMaxBytesPerSession
	if int64(replicas) > math.MaxInt64/perReplica {
		return math.MaxInt64
	}
	return int64(replicas) * perReplica
}

// xferMaxBytesForCeiling is the pure sizing clamp, split out for testing.
//
// ceiling<=0 (unknown JS store limit) ⇒ the cap, which is now one max-size transfer plus margin
// rather than the legacy 8 GiB — an unknown ceiling is no longer a licence to reserve 8 GiB.
// Otherwise: take what is left after the non-xfer streams' reservation, keep three quarters of it,
// and never ask for more than one max-size transfer needs. Too small for the floor ⇒ refuse (never
// MaxBytes<=0, which nats treats as UNLIMITED → a worse silent re-brick).
//
// NOTE ON WHAT `ceiling` IS: it is the CONFIGURED JetStream store limit (AccountInfo.Limits.MaxStore),
// NOT "space still unreserved". The reserve is subtracted HERE, exactly once. Passing an
// already-reduced number in would double-count and turn a working small-disk broker into a PERMANENT
// xfercontrol.ErrLegacyStoreTooSmall refusal — on racknerd's shape that is the difference between a 2 GiB bucket and
// a hard "tier-B is impossible here". drill 21-smalldisk-tierb would catch it at deploy tier.
func LegacyMaxBytesForCeiling(ceiling int64, sessions, replicas int) (int64, error) {
	if ceiling <= 0 {
		return xferBucketCap, nil // unknown ceiling → cap (never <=0/unlimited)
	}
	reserve := LegacyReserveFor(sessions, replicas)
	avail := ceiling - reserve
	if avail < xferBucketFloor {
		need := int64(math.MaxInt64)
		if reserve <= math.MaxInt64-int64(xferBucketFloor) {
			need = reserve + int64(xferBucketFloor)
		}
		// G67: wrapped in a SENTINEL so the #67 provisioning path can prove this refusal is
		// PERMANENT via errors.Is rather than by matching prose. Without a structural handle, the
		// "permanent set" tests would pass with the entire permanent branch deleted, because the
		// classifier's default is already permanent.
		return 0, fmt.Errorf("%w: js store ceiling=%d bytes, need >= %d (reserved by events+history "+
			"for %d session(s) at %d replica(s) = %d, plus the %d floor)",
			xfercontrol.ErrLegacyStoreTooSmall,
			ceiling, need, sessions, replicas, reserve, int64(xferBucketFloor))
	}
	maxBytes := avail / 4 * 3 // 0.75 of what's left, integer-safe
	if maxBytes < xferBucketFloor {
		maxBytes = xferBucketFloor
	}
	hi := int64(xferBucketCap)
	if avail < hi {
		hi = avail
	}
	if maxBytes > hi {
		maxBytes = hi
	}
	return maxBytes, nil
}

// xferPayloadLimit converts a backing stream limit into the largest payload that can be admitted.
// MaxBytes includes existing objects and JetStream's chunk/message overhead; neither is payload the
// new transfer can use. A non-positive MaxBytes is JetStream's unlimited form, so only the global
// transfer cap applies in that case.
func LegacyPayloadLimit(bucketMax int64, used uint64) int64 {
	if bucketMax <= 0 {
		return int64(proto.XferMaxBytes)
	}
	available := bucketMax - int64(xferBucketChunkMargin)
	if available <= 0 || used >= uint64(available) {
		return 0
	}
	available -= int64(used)
	if available > int64(proto.XferMaxBytes) {
		available = int64(proto.XferMaxBytes)
	}
	return available
}

// LegacyBucketCapacity distinguishes a temporarily full shared reservation
// from a permanently oversized file. DiscardNew preserves other objects' ACKed
// chunks, so cap0 must refuse admission until the bounded reaper frees space;
// zero payload room must never become an unlimited pull ceiling.
// origin: hot-update S7 plan §2.1 / §7 / LQ3; S7 internal review FIX-CONVERGE F1
func LegacyBucketCapacity(bucket string, bucketMax int64, used uint64) (string, int64, error) {
	limit := LegacyPayloadLimit(bucketMax, used)
	if limit == 0 {
		return "", 0, &xferbucket.Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "tier-B bucket has no payload capacity after stored bytes and chunk margin; retry after transfer cleanup frees space"}
	}
	return bucket, limit, nil
}

// LegacySealedBucketCapacity preserves the finite admitted ceiling when an
// operator has changed MaxBytes to the SDK's unlimited form after the seal.
// A fresh smaller reservation still wins; an unlimited readback never grants
// more room than the coordinator authorized.
// origin: hot-update S7 plan §2.1 / LQ3; S7 internal review FIX-CONVERGE F1
func LegacySealedBucketCapacity(seal xfercontract.BucketSeal, info *jetstream.StreamInfo) (string, int64, error) {
	limit := seal.MaxBytes
	if info.Config.MaxBytes > 0 {
		limit = min(limit, info.Config.MaxBytes)
	}
	return LegacyBucketCapacity(seal.Bucket, limit, info.State.Bytes)
}

func LegacyBucketLimit(account, server int64, sessions, replicas int) (int64, error) {
	maxBytes := int64(xferBucketCap)
	if account > 0 {
		accountMax, err := LegacyMaxBytesForCeiling(account, sessions, replicas)
		if err != nil {
			return 0, fmt.Errorf("account limit: %w", err)
		}
		if accountMax < maxBytes {
			maxBytes = accountMax
		}
	}
	if server > 0 {
		serverMax, err := LegacyMaxBytesForCeiling(server, sessions, 1)
		if err != nil {
			return 0, fmt.Errorf("server store estimate: %w", err)
		}
		if serverMax < maxBytes {
			maxBytes = serverMax
		}
	}
	return maxBytes, nil
}

// LegacyObjectStoreConfig preserves every ObjectStoreConfig field represented by the backing
// stream. UpdateObjectStore treats omitted fields as zero values; rebuilding only MaxBytes and
// Replicas would silently erase operator description, TTL, placement, compression and metadata.
func LegacyObjectStoreConfig(bucket string, cfg jetstream.StreamConfig) jetstream.ObjectStoreConfig {
	return jetstream.ObjectStoreConfig{
		Bucket:      bucket,
		Description: cfg.Description,
		TTL:         cfg.MaxAge,
		MaxBytes:    cfg.MaxBytes,
		Storage:     cfg.Storage,
		Replicas:    cfg.Replicas,
		Placement:   cfg.Placement,
		Compression: cfg.Compression == jetstream.S2Compression,
		Metadata:    cfg.Metadata,
	}
}

// LegacyCrossHomeExtra is the EXTRA grace an object of this size earns on top of whatever base floor
// is in effect: exactly the amount by which its watchdog budget exceeds the fixed tier floor.
//
// It is an INCREMENT, not an absolute floor, and that is the whole design. Two things would break if
// it returned an absolute value:
//
//   - the serveconf knob (broker.cluster.xfer_cross_home_reap_age) and the deploy-tier drill that
//     COMPRESSES it would stop working — a drill that sets the floor to seconds so it can observe a
//     reap would find every small object pinned for 15 minutes instead;
//   - every small object would inherit the worst object's grace, on precisely the small-disk brokers
//     that can least afford holding garbage.
//
// As an increment, an object whose budget is the plain tier floor earns nothing (behaviour byte-for-
// byte unchanged, drill seam intact), while a 2 GiB object earns the ~29 extra minutes its own budget
// bought. size<=0 (unknown) earns nothing, which is the fail-SAFE direction only because the base
// floor already covers the tier floor with its historical margin.
func LegacyCrossHomeExtra(size int64) time.Duration {
	if size <= 0 {
		return 0
	}
	if extra := proto.XferBudget("b", size, proto.XferPushLegs) - proto.XferTimeoutTierBFloor; extra > 0 {
		return extra
	}
	return 0
}
