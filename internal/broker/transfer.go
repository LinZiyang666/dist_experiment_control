// Broker side of the file-transfer feature (file-transfer-plan v0.2.0).
//
// Subjects owned here:
//
//   IN  s.<sid>.cmd.by.<actor>.node.<nid>.push.req         → handlePushReq
//   IN  s.<sid>.cmd.by.<actor>.node.<nid>.pull.req         → handlePullReq
//   IN  s.<sid>.cmd.by.<actor>.node.<nid>.push-commit.req  → handlePushCommitReq
//   IN  s.<sid>.ev.node.<nid>.transfer.<id>.{complete,failed} → handleEvTransfer
//   IN  ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req → handleFinalizeReq (transfer_finalize.go)
//   IN  ctrl.by.<actor>.s.<sid>.caps.req                   → handleCapsReq (caps.go)
//   OUT s.<sid>.audit.transfer                             → pubAuditTransfer
//
// Bucket lifecycle invariant: the broker is the SOLE owner of the
// per-session OBJ_xfer-<sid> stream. ctl + agent only Put/Get/Watch
// per-transfer objects keyed by transfer_id via the data subjects
// ($O.xfer-<sid>.{M,C}.>). Member/agent permission templates
// explicitly omit STREAM.CREATE/DELETE/PURGE; see internal/auth/permissions.go.
//
// Receiver-finalization invariant (plan §Audit): start row is written
// here on accepted prepare; complete|failed flows from the RECEIVER
// (agent for push, ctl for pull) into audit + bucket cleanup. The
// in-memory tracker provides the join between (transfer_id ↔ actor,
// sid, nid, tier, bucket) plus the timeout guard.

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Tier budgets — broker-side timeouts for the receiver-finalization
// signal to arrive. Past these the broker writes a synthetic
// failed-audit and reclaims the bucket. file-transfer-plan §Audit
// (timeout fallback) and §Object bucket lifecycle.
// batch C: the numbers moved to internal/proto so the broker, the agent and the ctl stop each
// declaring their own copy of a bound the broker REFUSES at. These aliases keep the ~30 broker-local
// call sites readable; proto owns the values and the derivation.
//
// transferTimeoutTierB is now the FLOOR of a size-derived budget, not the whole budget — see
// proto.XferBudget. It keeps its name because everything that legitimately still wants "the fixed
// tier-B floor" (the tier-A/B split, the ledger fallback for a record written before Size existed)
// wants exactly this value.
const (
	transferTimeoutTierA  = proto.XferTimeoutTierA
	transferTimeoutTierB  = proto.XferTimeoutTierBFloor
	transferTierAMaxBytes = proto.XferTierAMaxBytes
)

// xferCrossHomeReapAge (R16 #58 Lane C) is the age floor for the LEADER-driven cross-home GC of a
// split-home / zero-node OBJ_xfer bucket that no single home broker can reap (homeOwnsXferBucket is false
// on every broker). It is deliberately LONGER than the per-home grace (xferReapMinObjectAge, 2m): the
// leader deletes another home's objects here, and it sees their ModTime across nodes with clock skew, so a
// transfer still live on ANOTHER broker's home must never be torn out. Derived from transferTimeoutTierB (a
// live tier-B transfer terminates by its watchdog within one tier-B timeout) with a 3× margin; pinned by a
// test so the relation cannot silently drift. A deploy-tier drill compresses it via the serveconf seam.
//
// batch C: this stays the FLOOR and its value is UNCHANGED. The watchdog budget is now size-derived and
// can reach proto.XferTierBMaxBudget (35m08s), so the invariant "the GC never deletes an object a live
// watchdog still covers" no longer follows from one global constant. Raising the constant to cover the
// worst case was rejected twice over: serveconf.MinXferCrossHomeReapAge is a HARD floor enforced on
// production YAML, so raising it makes a broker that explicitly set the knob REFUSE TO START after an
// upgrade — and it would punish every small object with the worst object's grace on exactly the
// small-disk brokers that can least afford it. xferCrossHomeFloorFor makes the invariant per-object
// instead, which is both exact and free of deployment surface.
const xferCrossHomeReapAge = 3 * transferTimeoutTierB

// xferCrossHomeExtraFor is the EXTRA grace an object of this size earns on top of whatever base floor
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
func xferCrossHomeExtraFor(size int64) time.Duration {
	if size <= 0 {
		return 0
	}
	if extra := proto.XferBudget("b", size, proto.XferPushLegs) - transferTimeoutTierB; extra > 0 {
		return extra
	}
	return 0
}

// transferMaxBytes is the GLOBAL hard upper bound for one tier-B transfer (2 GiB). On a small-disk
// broker the per-session bucket ceiling (xferBucketMaxBytes, G6 #21) can be LOWER, and the push
// admission gate rejects at min(transferMaxBytes, that bucket ceiling).
//
// It is ALSO what makes the size-derived watchdog budget bounded: admission refuses anything larger
// before the watchdog is armed, so proto.XferTierBMaxBudget is a compile-time ceiling.
const transferMaxBytes = proto.XferMaxBytes

// G6 #21 + smalldisk: OBJ_xfer per-session bucket sizing.
//
// WHAT THE BUCKET ACTUALLY HAS TO HOLD — and why the cap used to be 4x too big.
//
// The bucket holds IN-FLIGHT transfer objects only: both ends delete the object on completion
// (cmd/tether/transfer.go finishPushTierB, internal/agent/transfer.go), and the orphan reaper
// (transfer_reconcile.go) sweeps whatever a crash left behind. One transfer can never exceed
// proto.XferMaxBytes (2 GiB) because push admission refuses above it. So a bucket larger than one
// max-size transfer buys nothing but a RESERVATION — and a reservation is exactly what a small-disk
// broker cannot spare. The legacy 8 GiB cap reserved 77% of racknerd's 10.33 GiB JetStream store for
// a bucket holding ZERO bytes, which is how tier-B came to be permanently denied there.
//
// WHY THE CAP IS XferMaxBytes PLUS A MARGIN, NEVER EXACTLY XferMaxBytes.
//
// nats stores an object as 128 KiB chunks (nats.go objDefaultChunkSize), so a 2 GiB object is ~16384
// messages, and MaxBytes accounts STORED bytes including per-message overhead. The backing stream is
// Discard=DiscardNew, so a bucket sized exactly XferMaxBytes would let admission pass a max-size
// transfer (the gate is `size > maxBytes`, and equal is not greater) and then fail it MID-PUT, after
// the sender hashed and started streaming. That is strictly worse than refusing up front. The margin
// covers the chunk overhead with room to spare (~3.3 MB worst case at 2 GiB).
const (
	xferBucketFloor       = 256 * 1024 * 1024 // smallest useful per-session tier-B ceiling
	xferBucketChunkMargin = 64 * 1024 * 1024  // headroom for 128 KiB-chunk + per-message overhead
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
func xferReserveFor(sessions, replicas int) int64 {
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

// transferTrackerMaxEntries caps the in-memory tracker map so a fast
// attacker spamming push.req / pull.req can't OOM the broker before
// the per-tier watchdog reaps stale entries. At the worst case
// (1024 entries * ~200 bytes/entry) the cap holds memory under
// ~200 KiB. New requests past the cap are rejected with
// `too_many_in_flight` so callers get a clean error instead of silent
// slowdown. Audit shard P11 F2.
//
// batch C: the MEMORY bound above is unchanged, but the EXHAUSTION WINDOW is not. This comment used
// to derive the bound from a "5-min Tier-B timeout"; with a size-derived budget an entry can now live
// up to proto.XferTierBMaxBudget (35m08s), so a client declaring the maximum size and then sending
// nothing holds a slot ~6.8x longer than before. That is accepted rather than defended against: only
// a session MEMBER can reach this path, and a member already has unrestricted `run`/`exec` (per the
// documented boundary in docs/usage.md), so a member wanting to exhaust a broker has far more direct
// options. Recording the changed window matters more than the (unchanged) byte count.
const transferTrackerMaxEntries = 1024

// xferRecentlyReapedTTL bounds how long the tracker remembers that it reaped a transfer id. Long
// enough for the receiver's real completion to arrive after a watchdog fire (that is the whole point
// — the receiver was still working), short enough that the set cannot grow without bound.
const xferRecentlyReapedTTL = 10 * time.Minute

// transferEntry is the broker's in-memory tracking record for one
// in-flight transfer. Keyed by transfer_id in transferTracker.entries.
type transferEntry struct {
	transferID string
	sid        string
	nid        string
	actor      string // NATS user nkey public key
	actorFP    string // sha-256 fingerprint
	verb       string // push | pull
	tier       string // a | b
	bucket     string // empty for tier A
	path       string
	size       int64
	startedAt  time.Time
	cancel     context.CancelFunc // cancels the timeout watchdog
	finalized  bool               // set under tracker.mu when audit complete|failed already written
}

// transferTracker is a process-wide registry. Long-lived, attached to
// Broker. All transferEntry reads/writes hold tracker.mu.
type transferTracker struct {
	mu      sync.Mutex
	entries map[string]*transferEntry
	// reaped (batch C) remembers ids the WATCHDOG terminated, with the time it happened. It exists so
	// a receiver's genuine completion arriving after the budget expired can be LOGGED rather than
	// dropped in silence — that silence is the last step of "the file landed but the audit says
	// failed". Bounded by an opportunistic sweep on insert plus xferRecentlyReapedTTL.
	reaped map[string]time.Time
}

func newTransferTracker() *transferTracker {
	return &transferTracker{entries: map[string]*transferEntry{}, reaped: map[string]time.Time{}}
}

// markReaped records that the watchdog (not a normal finalize) ended this transfer.
func (t *transferTracker) markReaped(id string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reaped == nil {
		t.reaped = map[string]time.Time{}
	}
	for k, at := range t.reaped {
		if now.Sub(at) >= xferRecentlyReapedTTL {
			delete(t.reaped, k)
		}
	}
	t.reaped[id] = now
}

// recentlyReaped reports whether the watchdog ended this transfer within the retention window.
func (t *transferTracker) recentlyReaped(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.reaped[id]
	return ok && time.Since(at) < xferRecentlyReapedTTL
}

func (t *transferTracker) get(id string) *transferEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.entries[id]
}

// put inserts an entry. Returns "" on success, else the response Code
// the caller must reject the request with. A transfer_id already in
// flight is never overwritten: the original entry's watchdog stays
// armed on that id, so replacing the entry would let the stale
// watchdog claim and reap the replacement mid-transfer.
func (t *transferTracker) put(e *transferEntry) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.entries[e.transferID]; exists {
		return "transfer_id_in_flight"
	}
	// The bucket is per session and intentionally sized for one maximum object plus overhead.
	// Serialize tier-B use of the same bucket so two valid objects cannot race into DiscardNew.
	// Tier A (empty bucket) and different sessions remain concurrent.
	if e.bucket != "" {
		for _, live := range t.entries {
			if live.bucket == e.bucket {
				return "too_many_in_flight"
			}
		}
	}
	if len(t.entries) >= transferTrackerMaxEntries {
		return "too_many_in_flight"
	}
	t.entries[e.transferID] = e
	return ""
}

// remove deletes the entry. It used to return it; nothing ever read the result, and a returned
// value nobody reads reads like a handle somebody is holding.
func (t *transferTracker) remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, id)
}

// claimFinalize atomically marks the entry as finalized iff:
//   - the entry exists in the map, AND
//   - it hasn't already been finalized.
//
// First caller wins; later callers receive ok=false and MUST NOT write
// a second audit row / delete the transfer object twice.
//
// The returned *transferEntry MAY be non-nil even when ok=false
// (entry exists but was already claimed) — callers can use the entry
// for an idempotent OK reply, but must not mutate it. The fields used
// for caller-side validation (sid, nid, actor, verb, tier, bucket,
// path) are immutable after put(), so they may be read without
// holding tracker.mu.
//
// Audit shard P11 F1: previous markFinalized + per-handler
// `entry.finalized = false` "unclaim" pattern was racy because the
// unclaim happened outside tracker.mu. The new contract is
// validate-then-claim: callers do all validation against immutable
// fields BEFORE calling claimFinalize, so there is no path that
// claims and then needs to back out.
func (t *transferTracker) claimFinalize(id string) (e *transferEntry, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e = t.entries[id]
	if e == nil || e.finalized {
		return e, false
	}
	e.finalized = true
	return e, true
}

// activeOBJStreams returns a snapshot of the in-flight bucket names.
// Used by the boot reconciler to distinguish "stream is owned by an
// active in-process transfer" (keep) from "orphan from a previous
// broker that crashed" (delete). After a restart entries is empty so
// EVERY OBJ_xfer-* stream is considered orphan — which is correct
// because the in-memory state-machine is lost on crash.
func (t *transferTracker) activeOBJStreams() map[string]struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]struct{}{}
	for _, e := range t.entries {
		if e.bucket != "" {
			out["OBJ_"+e.bucket] = struct{}{}
		}
	}
	return out
}

// ensureXferBucket lazily creates the per-session bucket xfer-<sid>
// (backing stream OBJ_xfer-<sid>) on first use. Idempotent: returns OK
// when the bucket already exists. Tier B only. Bucket sticks around
// until the session is removed; per-transfer cleanup happens via
// deleteXferObject (object delete inside the bucket).
//
// xferStoreLimits keeps the two JetStream admission domains separate. Account reservation is
// replica-weighted; server reservation is not. Combining the raw ceilings before subtracting the
// reserve is therefore dimensionally wrong for replicas > 1.
type xferStoreLimits struct {
	account int64 // AccountInfo MaxStore; reservation is replica-weighted
	server  int64 // statfs-derived server estimate; reservation is not replica-weighted
}

func xferStoreLimitsFor(ctx context.Context, b *Broker) xferStoreLimits {
	var limits xferStoreLimits
	if b.js != nil {
		if info, err := b.js.AccountInfo(ctx); err == nil && info.Limits.MaxStore > 0 {
			limits.account = info.Limits.MaxStore
		}
	}
	if b.cfg.StoreDir != "" {
		if used, total, err := xferDiskUsage(b.cfg.StoreDir); err == nil && total > used {
			free := total - used
			if free > math.MaxInt64 {
				limits.server = math.MaxInt64 / 4 * 3
			} else {
				limits.server = int64(free) / 4 * 3
			}
		}
	}
	return limits
}

// jsStoreCeiling is retained for diagnostics and tests that need the tightest raw byte limit. Sizing
// itself uses xferStoreLimits so it can apply the correct reservation units to each limit.
func (b *Broker) jsStoreCeiling(ctx context.Context) int64 {
	limits := xferStoreLimitsFor(ctx, b)
	if limits.account > 0 && (limits.server <= 0 || limits.account < limits.server) {
		return limits.account
	}
	return limits.server
}

// xferDiskUsage is a var ONLY so tests can pin the statfs branch to a known store size.
//
// It is not gratuitous seam-making: this branch is the one production sizing path actually takes
// (tether renders no account JWT limits, so AccountInfo reports MaxStore = -1). Without a
// controllable disk reading, the server-limit regressions depend on the CI filesystem's free space.
var xferDiskUsage = diskUsage

// activeSessionCount is how many history streams this broker's JetStream is holding a reservation for
// (one per ACTIVE session; H.3 deletes the stream when a session is tombstoned). A LOCAL SQLite read —
// the sizing path already pays one NETWORK round trip for AccountInfo, and this adds no second one.
//
// On a read error it reports 1 rather than failing the transfer: 1 is exactly what the old hard-coded
// 2 GiB reserve assumed, so a DB hiccup degrades to today's behaviour instead of blocking tier-B. It
// under-reserves if there really are more sessions, and that lands on the bounded create-retry path
// with the honest "insufficient storage" refusal — the same place any other over-ask lands.
func (b *Broker) activeSessionCount(ctx context.Context) int {
	// No DB handle at all (unit fixtures that exercise only the JS side) ⇒ the arithmetic still has
	// to produce a number. Sizing must never PANIC on a missing dependency: it runs on the push path,
	// and a nil-deref there would take the broker down over a capacity question.
	//
	// Asked through read().SQL() rather than b.cfg.DB: in cluster mode Run re-points b.cfg.DB to the
	// node's READ-ONLY pool, and the cfgdb ratchet (correctly) refuses new direct touches of that
	// field so a read site cannot quietly become a write that fails only in cluster mode.
	if b.read().SQL() == nil {
		return 1
	}
	var n int
	if err := b.read().QueryRowContext(ctx, `SELECT COUNT(*) FROM sessions WHERE state='ACTIVE'`).Scan(&n); err != nil {
		if b.cfg.Logger != nil {
			b.cfg.Logger.Warn("broker: xfer sizing session count", "err", err, "assuming", 1)
		}
		return 1
	}
	if n < 1 {
		return 1
	}
	return n
}

// xferBucketMaxBytes computes the per-session OBJ_xfer bucket ceiling: what is left of the JS store
// after the non-xfer reservations, capped at one max-size transfer plus margin. REFUSES (rather than
// emit MaxBytes<=0, which nats treats as UNLIMITED) when the store cannot even fit the floor.
func (b *Broker) xferBucketMaxBytes(ctx context.Context) (int64, error) {
	limits := xferStoreLimitsFor(ctx, b)
	sessions := b.activeSessionCount(ctx)
	replicas := b.xferTargetReplicas()
	maxBytes := int64(xferBucketCap)
	if limits.account > 0 {
		accountMax, err := xferMaxBytesForCeiling(limits.account, sessions, replicas)
		if err != nil {
			return 0, fmt.Errorf("account limit: %w", err)
		}
		if accountMax < maxBytes {
			maxBytes = accountMax
		}
	}
	if limits.server > 0 {
		serverMax, err := xferMaxBytesForCeiling(limits.server, sessions, 1)
		if err != nil {
			return 0, fmt.Errorf("server store estimate: %w", err)
		}
		if serverMax < maxBytes {
			maxBytes = serverMax
		}
	}
	return maxBytes, nil
}

// errXferStoreTooSmall marks the G6 #21 sizing refusal as a PERMANENT condition: a disk that cannot
// hold the tier-B floor is not a stall, so the #67 bounded provisioning retry must make ZERO create
// attempts against it. Wrapped (never re-worded) by xferMaxBytesForCeiling.
var errXferStoreTooSmall = errors.New("js store too small for tier-B")

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
// errXferStoreTooSmall refusal — on racknerd's shape that is the difference between a 2 GiB bucket and
// a hard "tier-B is impossible here". drill 21-smalldisk-tierb would catch it at deploy tier.
func xferMaxBytesForCeiling(ceiling int64, sessions, replicas int) (int64, error) {
	if ceiling <= 0 {
		return xferBucketCap, nil // unknown ceiling → cap (never <=0/unlimited)
	}
	reserve := xferReserveFor(sessions, replicas)
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
			errXferStoreTooSmall,
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
func xferPayloadLimit(bucketMax int64, used uint64) int64 {
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

// ensureXferBucketSized is ensureXferBucket with a PRE-COMPUTED disk-aware ceiling (A11): the tier-B push
// admission path already computes the ceiling for its size check, so threading it in avoids a SECOND
// AccountInfo round-trip per transfer (each xferBucketMaxBytes issues a live AccountInfo, which nats.go
// does not cache). The too-small refusal is short-circuited by the CALLER (provisionXferBucket) and
// never reaches here — the sizeErr parameter that used to re-check it was dead and was removed.
func ensureXferBucketSizedWithLimit(ctx context.Context, b *Broker, sid string, targetReplicas int, maxBytes int64) (string, int64, error) {
	if b.js == nil {
		return "", 0, fmt.Errorf("jetstream_unavailable")
	}
	bucket := proto.XferBucketName(sid)

	// RESOLVE BEFORE CREATE.
	//
	// A STREAM.INFO lookup reaches no storage-limits path at all, whereas STREAM.CREATE is gated by
	// jsNonClusteredStreamLimitsCheck BEFORE the server ever looks at the name — and that gate's
	// server-level clause (js.storeReserved + requested > MaxStore) counts THIS BUCKET'S OWN existing
	// reservation. On a store whose reservations already reach the ceiling, asking to create a bucket
	// that is already there is therefore refused with 10047 "insufficient storage", never with
	// "already exists". Looking first turns that whole class of false failure into a no-op.
	//
	// Only an authoritative not-found reaches create. A timeout or leader error is retried by the
	// bounded provisioning loop; treating it as absence would issue a misleading create request.
	if stream, err := b.js.Stream(ctx, xferBackingStream(sid)); err == nil {
		if err := raiseXferReplicas(ctx, b.js, bucket, targetReplicas); err != nil {
			return "", 0, err
		}
		si, err := stream.Info(ctx)
		if err != nil {
			return "", 0, fmt.Errorf("xfer bucket info %s: %w", bucket, err)
		}
		b.convergeOversizedXferBucket(ctx, sid, bucket, si, maxBytes, targetReplicas)
		return bucket, xferPayloadLimit(si.Config.MaxBytes, si.State.Bytes), nil
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return "", 0, fmt.Errorf("xfer bucket lookup %s: %w", bucket, err)
	}

	cfg := jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		MaxBytes: maxBytes, // sized by need (one max transfer + margin), clamped by what the store can carry
		Storage:  jetstream.FileStorage,
		Replicas: targetReplicas, // D5 §6.4/§9: replicasFor(nVoters); live callers pass jsstream.ReplicasSingle
	}
	if _, err := b.js.CreateObjectStore(ctx, cfg); err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) ||
			errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			// Exists: RAISE-ONLY reconcile toward target (D5 §9, R-18). Idempotent +
			// retriable on a not-yet-ready meta-group (ErrMetaGroupNotReady). Still reachable when
			// the lookup above failed transiently and the bucket really does exist.
			if err := raiseXferReplicas(ctx, b.js, bucket, targetReplicas); err != nil {
				return "", 0, err
			}
			stream, serr := b.js.Stream(ctx, xferBackingStream(sid))
			if serr != nil {
				return "", 0, fmt.Errorf("xfer bucket info after create race %s: %w", bucket, serr)
			}
			si, serr := stream.Info(ctx)
			if serr != nil {
				return "", 0, fmt.Errorf("xfer bucket info after create race %s: %w", bucket, serr)
			}
			return bucket, xferPayloadLimit(si.Config.MaxBytes, si.State.Bytes), nil
		}
		return "", 0, fmt.Errorf("create_bucket: %w%s", err, b.xferStoreAccounting(ctx, err, maxBytes))
	}
	return bucket, xferPayloadLimit(maxBytes, 0), nil
}

// jsErrCodeStorageExceeded is the JetStream API code for "insufficient storage resources available"
// (jetstream.APIError.ErrorCode). nats.go exports no named constant, so the stable wire value is
// pinned here the same way jsErrCodeUnavailable is in js_health.go.
const jsErrCodeStorageExceeded = 10047

// xferStoreAccounting turns nats's four-word storage refusal into something an operator can act on.
//
// "insufficient storage resources available" says nothing about WHICH bytes are gone, and the
// answer is rarely the obvious one: on the incident broker the store was 99.5% RESERVED and 0.5%
// USED, with 77% of the whole store held by an OBJ_xfer bucket containing zero bytes. An operator
// reading only nats's text checks `df`, sees free disk, and has nowhere to go. Reserved-vs-used is
// exactly the distinction that closes that gap, so it is printed at the point of refusal.
//
// Returns "" for any other error, and for an unreadable account — an enrichment that cannot be
// computed must add nothing rather than guess.
func (b *Broker) xferStoreAccounting(ctx context.Context, err error, want int64) string {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jsErrCodeStorageExceeded || b.js == nil {
		return ""
	}
	info, ierr := b.js.AccountInfo(ctx)
	if ierr != nil || info == nil {
		return ""
	}
	// Same trap as xferStoreIsOverCommitted: the ACCOUNT limit is -1 (unlimited) on every
	// tether-rendered broker, so gating on it here would silently print nothing in exactly the
	// deployment this message exists for. jsStoreCeiling resolves the effective limit.
	ceiling := b.jsStoreCeiling(ctx)
	if ceiling <= 0 {
		return ""
	}
	return fmt.Sprintf(" (JetStream store: effective limit ~%d bytes, RESERVED %d, actually used %d; "+
		"this request wanted %d. Reserved-not-used bytes are held by stream MaxBytes settings — check "+
		"`nats stream ls --all` for a stream reserving far more than it stores)",
		ceiling, info.ReservedStore, info.Store, want)
}

// convergeOversizedXferBucket shrinks an existing bucket whose reservation the store can no longer
// honour. BEST-EFFORT by construction: every failure path leaves the bucket exactly as it was and
// returns, because an un-shrunk bucket still serves transfers while a failed transfer does not.
//
// WHY THIS IS "REPAIR ON DEMONSTRATED FAILURE", NOT "NORMALISE ALWAYS"
//
// This package states, and relies on, the rule that stream LIMITS are operator-owned:
// raiseXferReplicas raises only the replica factor "so an operator's `nats stream edit` limits
// (MaxBytes/retention/…) survive … the HA replica factor is cluster-owned; retention/limits are
// operator-owned". Shrinking on every push because tether would have chosen a different number
// would clobber exactly that. So the trigger is not "differs from target" but "the store cannot
// satisfy what is configured": only then does honouring the operator's number mean staying broken,
// and only then does tether overrule it — loudly.
//
// The repair is limited to a finite ACCOUNT limit because AccountInfo.ReservedStore is expressed in
// account (replica-weighted) units. A statfs estimate is a server-level number with different units
// and cannot safely authorize changing an operator-owned stream configuration.
func (b *Broker) convergeOversizedXferBucket(ctx context.Context, sid, bucket string,
	info *jetstream.StreamInfo, target int64, targetReplicas int,
) {
	if info == nil || target <= 0 {
		return
	}
	cur := info.Config.MaxBytes
	if cur <= 0 || cur <= target {
		return // unlimited-or-unknown, or already at/below what we would ask for: not ours to touch
	}
	if !b.xferAccountIsOverCommitted(ctx) {
		return // no EXACT proof the store is over-committed ⇒ operator-owned, hands off
	}
	// NEVER shrink while a transfer is using this bucket.
	//
	// State.Bytes alone is NOT enough, and believing it was is how the first cut of this function
	// would have cut a live upload in half: it reports what is STORED RIGHT NOW, not what an
	// admitted transfer is still going to send. A 1.4 GiB object 600 MiB into its Put looks like
	// "600 MiB used" and would sail past a bytes-only check, then hit DiscardNew at the new ceiling
	// and fail mid-stream — the exact failure mode the whole increment exists to prevent, caused by
	// the repair rather than by the disk. The tracker knows which buckets have admitted transfers,
	// so ask it.
	if b.transfers == nil {
		// No tracker wired ⇒ we cannot prove the bucket is idle, so we do not touch it. Same rule as
		// everywhere else in this function: an unreadable input means "leave the operator's
		// configuration alone", never "assume it is safe".
		return
	}
	if _, live := b.transfers.activeOBJStreams()[xferBackingStream(sid)]; live {
		b.cfg.Logger.Info("broker: deferring xfer bucket shrink (transfer in flight)",
			"sid", sid, "bucket", bucket, "current_max_bytes", cur, "target", target)
		return
	}
	// Belt: even with no tracked transfer, never shrink under the bytes already on disk —
	// UpdateObjectStore rejects that, and a rejected update here is how replication_degraded gets
	// latched (see raiseXferReplicas).
	if used := int64(info.State.Bytes); target <= used+int64(xferBucketChunkMargin) {
		b.cfg.Logger.Warn("broker: xfer bucket oversized but cannot shrink (bytes on disk)",
			"sid", sid, "bucket", bucket, "current_max_bytes", cur, "target", target, "used_bytes", used)
		return
	}
	// PRESERVE the existing replica factor. Replication is raise-only and cluster-owned
	// (raiseXferReplicas); writing targetReplicas here would let a shrink SILENTLY DOWNGRADE a
	// 3-replica bucket to 1 whenever the local target happens to be lower — losing redundancy as a
	// side effect of a capacity repair, which nothing in this function is entitled to do.
	replicas := info.Config.Replicas
	if replicas < 1 {
		replicas = targetReplicas
	}
	cfg := objectStoreConfigFromStream(bucket, info.Config)
	cfg.MaxBytes = target
	cfg.Replicas = replicas
	if _, err := b.js.UpdateObjectStore(ctx, cfg); err != nil {
		b.cfg.Logger.Warn("broker: xfer bucket shrink failed (continuing with the existing bucket)",
			"sid", sid, "bucket", bucket, "current_max_bytes", cur, "target", target, "err", err)
		return
	}
	// The caller uses this same snapshot for post-ensure admission. Reflect the successful update so
	// it does not accidentally admit against the old, larger limit.
	info.Config.MaxBytes = target
	// LOUD on success: tether just changed a limit the operator owns. Say what it was, what it is,
	// and why — an operator who finds their `nats stream edit` undone must be able to read the
	// reason out of the log rather than infer it.
	b.cfg.Logger.Info("broker: shrank an unsatisfiable xfer bucket reservation",
		"sid", sid, "bucket", bucket, "from_max_bytes", cur, "to_max_bytes", target,
		"reason", "the JetStream store cannot honour the configured reservation, which blocks every "+
			"stream create on this broker until it is reduced")
}

// xferAccountIsOverCommitted reports a demonstrated CURRENT overcommit. ReservedStore already includes the
// existing bucket, so adding a target would ask whether an imaginary second copy fits and would
// authorize needless shrink. The comparison is account-only because both operands must use the same
// replica-weighted units. Unknown/unlimited limits fail closed toward leaving operator config alone.
func (b *Broker) xferAccountIsOverCommitted(ctx context.Context) bool {
	if b.js == nil {
		return false
	}
	info, err := b.js.AccountInfo(ctx)
	if err != nil || info == nil || info.Limits.MaxStore <= 0 {
		return false
	}
	reserved := info.ReservedStore
	if info.Store > reserved {
		reserved = info.Store
	}
	return reserved > uint64(info.Limits.MaxStore)
}

// objectStoreConfigFromStream preserves every ObjectStoreConfig field represented by the backing
// stream. UpdateObjectStore treats omitted fields as zero values; rebuilding only MaxBytes and
// Replicas would silently erase operator description, TTL, placement, compression and metadata.
func objectStoreConfigFromStream(bucket string, cfg jetstream.StreamConfig) jetstream.ObjectStoreConfig {
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

// xferBackingStream is the JetStream stream that backs the per-session object-store
// bucket (nats.go names it "OBJ_<bucket>"). Reading its StreamInfo gives the per-peer
// Cluster placement (object-store Status() exposes only the configured Replicas count,
// not the caught-up peers), so the §6.4 AllAtTarget "actual" is read from it.
func xferBackingStream(sid string) string { return "OBJ_" + proto.XferBucketName(sid) }

// raiseXferReplicas raises an existing object-store bucket's replica factor toward
// target via UpdateObjectStore (D5 §9, R-18). Raise-only (the configured Replicas <
// target check); the meta-group readiness gate is the UpdateObjectStore rejection
// classified to jsstream.ErrMetaGroupNotReady (retriable), mirroring the stream path.
func raiseXferReplicas(ctx context.Context, js jetstream.JetStream, bucket string, target int) error {
	os, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return fmt.Errorf("xfer reconcile lookup %s: %w", bucket, err)
	}
	st, err := os.Status(ctx)
	if err != nil {
		return fmt.Errorf("xfer reconcile status %s: %w", bucket, err)
	}
	cur := st.Replicas()
	if cur < 1 {
		cur = 1
	}
	if cur >= target {
		return nil // raise-only
	}
	// G6 #21: PRESERVE the existing bucket's MaxBytes on a replica raise — recomputing could shrink it
	// below already-used bytes (UpdateObjectStore would reject → replication_degraded latch). Read it off
	// the backing stream; fall back to the legacy cap (never 0 = UNLIMITED) if unreadable.
	var streamCfg jetstream.StreamConfig
	if si, serr := js.Stream(ctx, "OBJ_"+bucket); serr == nil {
		if info, ierr := si.Info(ctx); ierr == nil && info.Config.MaxBytes > 0 {
			streamCfg = info.Config
		}
	}
	if streamCfg.MaxBytes <= 0 {
		// G6 #21 (internal review): could not read the existing bucket's MaxBytes → do NOT raise with a
		// guessed ceiling (a wrong 8 GiB over-provisions a small disk; a shrink-below-used latches
		// replication_degraded). Skip this raise and retry next cycle via the retriable channel the callers
		// already handle.
		return jsstream.ErrMetaGroupNotReady
	}
	cfg := objectStoreConfigFromStream(bucket, streamCfg)
	cfg.Replicas = target
	if _, err := js.UpdateObjectStore(ctx, cfg); err != nil {
		if jsstream.IsMetaGroupNotReady(err) {
			return jsstream.ErrMetaGroupNotReady
		}
		return fmt.Errorf("xfer reconcile update %s replicas->%d: %w", bucket, target, err)
	}
	return nil
}

// XferReplicaState ensures+raises the per-session object-store bucket toward target and
// returns its replica health for the §6.4 AllAtTarget predicate (read from the backing
// stream's live Cluster placement). It is the OBJ_xfer half of the publisher's reconcile
// pass (wired as auditPublisher.xferState in test/d5). A not-yet-ready meta-group
// surfaces as jsstream.ErrMetaGroupNotReady (the caller marks the tick degraded + retries).
func XferReplicaState(ctx context.Context, js jetstream.JetStream, sid string, target int) (jsstream.StreamReplicaState, error) {
	// A not-yet-ready meta-group is expected mid-expand: swallow it and still report the
	// CURRENT (degraded) state so AllAtTarget sees actual<target. A missing bucket
	// (session never transferred) propagates jetstream.ErrBucketNotFound so the reconcile
	// pass SKIPS it (nothing to replicate). Any other error is a real failure.
	if err := raiseXferReplicas(ctx, js, proto.XferBucketName(sid), target); err != nil &&
		!errors.Is(err, jsstream.ErrMetaGroupNotReady) {
		return jsstream.StreamReplicaState{}, err
	}
	return jsstream.CollectStreamState(ctx, js, xferBackingStream(sid), target)
}

// deleteXferObject removes one transfer's object from the per-session
// bucket. Missing-object is OK (idempotent). The bucket itself
// survives — it's per-session, not per-transfer.
func (b *Broker) deleteXferObject(ctx context.Context, sid, transferID string) error {
	if b.js == nil || transferID == "" {
		return nil
	}
	store, err := b.js.ObjectStore(ctx, proto.XferBucketName(sid))
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) ||
			errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil
		}
		return err
	}
	if err := store.Delete(ctx, transferID); err != nil &&
		!errors.Is(err, jetstream.ErrObjectNotFound) {
		return err
	}
	return nil
}

// watchdogBudget is the duration startTransferWatchdog ACTUALLY arms.
//
// origin: batch-c internal review tests-F5. It is a named function so a test can assert the value the
// watchdog uses rather than re-deriving proto.XferBudget(...) and comparing that to itself. The tests
// that restated the formula stayed green when the real arm site was changed to cover one leg instead
// of two — i.e. they could not see the very defect they were written for.
//
// The budget is derived from the declared size and covers BOTH object-store crossings, because the
// watchdog is armed before the prepare is even forwarded. e.size is 0 on the pull path
// (proto.PullPrepareReq carries no size — permanent decision N5), which yields the historical flat
// floor rather than a zero budget.
func watchdogBudget(e *transferEntry) time.Duration {
	return proto.XferBudget(e.tier, e.size, proto.XferPushLegs)
}

// startTransferWatchdog arms a per-tier timeout. If the receiver-side
// finalization signal does not arrive in time, the watchdog writes a
// synthetic audit failed + reclaims the bucket. The returned cancel
// is stored on the entry so a successful finalize stops the watchdog.
func (b *Broker) startTransferWatchdog(parent context.Context, e *transferEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	d := watchdogBudget(e)
	go func() {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ent, ok := b.transfers.claimFinalize(e.transferID)
		if !ok || ent == nil {
			return
		}
		// batch C: the push arm used to write "agent_no_responders", which is a WRONG ATTRIBUTION —
		// the agent may have been reachable and transferring the whole time; what ran out was the
		// broker's budget. It is also the code the CLI classifies as EX_TEMPFAIL with the hint "the
		// agent isn't reachable on NATS; check it's running and connected", sending an operator to
		// debug connectivity that was never broken. The genuine no-responders emitters (expose,
		// upgrade, the cluster upgrade trigger) keep that code; only this one changes.
		code := proto.CodeTransferBudgetExceeded
		if ent.verb == "pull" {
			code = "ctl_disconnect"
		}
		b.transfers.markReaped(ent.transferID, b.cfg.Now())
		b.finalizeTransfer(ent, schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
			Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
			ActorNkey: ent.actor, ActorFp: ent.actorFP,
			TransferID: ent.transferID, Path: ent.path, Tier: ent.tier,
			Bucket: ent.bucket, DurationMs: msSince(ent.startedAt, b.cfg.Now()),
			Code:  code,
			Error: fmt.Sprintf("broker watchdog: no %s finalization within %s", ent.verb, d),
		}, false) // false: the watchdog owns entry.cancel; calling it here is a no-op
		b.cfg.Logger.Warn("broker: transfer watchdog fired",
			"transfer_id", ent.transferID, "verb", ent.verb, "tier", ent.tier,
			"code", code)
	}()
	return cancel
}

func msSince(t0, t1 time.Time) int64 {
	if t0.IsZero() {
		return 0
	}
	d := t1.Sub(t0)
	if d < 0 {
		d = 0
	}
	return d.Milliseconds()
}

// pubAuditTransfer marshals + publishes one AuditTransfer row to
// `tether.v2.s.<sid>.audit.transfer`. Best-effort: failures log a
// warning but don't abort the surrounding handler — losing one audit
// row is preferable to flapping a successful transfer because
// JetStream momentarily glitched.
func (b *Broker) pubAuditTransfer(rec schema.AuditTransfer) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := b.publishAudit(proto.SubjAuditTransfer(rec.Session), payload); err != nil {
		b.cfg.Logger.Warn("broker: audit.transfer pub",
			"err", err, "sid", rec.Session, "tid", rec.TransferID)
	}
}

// emitTransferAudit routes one transfer-audit record. In production
// (transferAuditSink==nil, D8a build-and-prove) it is the byte-identical best-effort
// pubAuditTransfer — the nil-seam read is the only change. When the harness has attached
// the cluster sink (AttachTransferAuditSink) the record is routed through leader Apply
// (OpTransferAudit, re-derivable §9/§6.3) so any new leader re-derives committed-but-
// unpublished transfer audit after a leader death (cutover=D9).
func (b *Broker) emitTransferAudit(rec schema.AuditTransfer) {
	b.emitTransferAuditWithCommit(rec, nil)
}

// emitTransferAuditWithCommit is emitTransferAudit plus a COMMIT callback: onCommitted runs only after
// the record is durably committed (cluster mode: the raft forward returned nil; single mode: the core
// publish is the record). It never runs on a give-up.
func (b *Broker) emitTransferAuditWithCommit(rec schema.AuditTransfer, onCommitted func()) {
	if b.transferAuditSink != nil {
		b.transferAuditSink(rec, onCommitted)
		return
	}
	b.pubAuditTransfer(rec)
	if onCommitted != nil {
		onCommitted()
	}
}

// emitTerminalTransferAudit is the ONE way a TERMINAL transfer audit is written, and the only place
// allowed to drop the #57 durable in-flight ledger.
//
// EXTERNAL REVIEW F1 (Blocker). The four terminal paths used to emit the audit and then delete the
// ledger on the next line — but in cluster mode emitTransferAudit hands the record to a goroutine and
// returns immediately (transfer_audit_forward.go: "never block the NATS handler"), so "the audit was
// emitted" is not "the audit was committed". A broker exiting in that window left the ledger already
// deleted and the terminal never committed: recovery had no evidence to read and the audit kept a
// start row with no terminal forever — verbatim the #57 shape R16 exists to remove.
//
// R16's own recovery finalizer already had this right (commitSyntheticTransferTerminal, then delete).
// This applies the SAME invariant to the common paths. If the forward gives up, the ledger SURVIVES and
// the recovery finalizer synthesizes a terminal on the next boot — the outcome #57 was built for.
//
// The reviewer also noted the previous pin (TestXferInflightTerminalDropsLedger) used a no-op sink and
// therefore froze the WRONG order as green; it now drives a blocking sink.
func (b *Broker) emitTerminalTransferAudit(rec schema.AuditTransfer, transferID string) {
	// RE-REVIEW F1: STAGE the decided terminal into the ledger BEFORE forwarding it. The first fix only
	// delayed the unlink until after the commit, which left the symmetric window open — a process killed
	// after the commit but before the unlink came back to a START-ONLY ledger, and recovery then GUESSED
	// a `failed/home_broker_restart` row that can never dedup against the real terminal (the reqID hashes
	// the whole record), so the audit claimed the transfer both completed and failed.
	//
	// Staging first makes recovery a REPLAY of the same bytes instead of a guess: identical record ⇒
	// identical reqID ⇒ the replicated dedup ledger collapses it. Both windows are now closed by one
	// invariant: the ledger holds the exact row that must exist, until that row is known to exist.
	staged, applicable := b.stageXferInflightTerminal(transferID, rec)
	// ROUND-4 RE-REVIEW: what makes suppression safe is that RECOVERY STILL HOLDS THIS EXACT TERMINAL —
	// and that is now a property of the staging attempt itself, not a guess about the past. Staging falls
	// back to a sibling outbox directory, so "unstaged" no longer means "the primary directory hiccuped";
	// it means the whole data directory refused BOTH writes. Earlier rounds tried to answer this with an
	// in-memory "the start ledger was written once" bit, which is a claim about a write that may since
	// have been undone — the data directory can be replaced, unmounted or wiped while the process runs.
	// A durability decision must not rest on a bit that outlives the thing it describes.
	if applicable && !staged {
		// Nothing anywhere will hold this outcome, so the two remaining options are: forward it and risk
		// recovery synthesizing a contradictory row from a surviving start record, or stay silent and
		// lose the transfer's ONLY terminal outright. Silence is worse: a contradiction is visible and
		// repairable, a transfer that never reported an outcome is not — nothing in the audit trail even
		// says it should be there. Forward, and make the durability failure loud.
		b.cfg.Logger.Error("broker: terminal staging failed in BOTH the xfer-inflight ledger and the fallback "+
			"outbox — forwarding the terminal best-effort, because suppressing it would lose this transfer's "+
			"only terminal outright",
			"transfer_id", transferID, "kind", rec.Kind)
	}
	b.emitTransferAuditWithCommit(rec, func() { b.removeXferInflight(transferID) })
}

// handlePushReq is the broker entry for `tether push`. It runs the
// standard ctl preconditions (sid alive, actor is a member, node is
// ONLINE), ensures the per-session tier-B bucket exists if needed, registers the
// in-memory entry, forwards a tightly-shaped payload to the agent,
// and writes audit start. The agent's reply (PushPrepareResp) goes
// directly back to ctl on msg.Reply — broker isn't in the data path.
func (b *Broker) handlePushReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "push" {
		b.replyPushErr(msg, "subject_malformed", msg.Subject)
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyPushErr(msg, "actor_invalid", err.Error())
		return
	}
	if !b.transferHomeGate(sid, nid) {
		return // not this agent's home (or home unresolved): stay silent, the home answers (§9 D8)
	}
	if err := b.transferGate(sid, fp, nid); err != "" {
		b.replyPushErr(msg, err, "")
		return
	}
	var req proto.PushPrepareReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyPushErr(msg, "json_parse", err.Error())
		return
	}
	if req.TransferID == "" || req.Path == "" || req.Tier == "" {
		b.replyPushErr(msg, "request_invalid",
			fmt.Sprintf("transfer_id=%q path=%q tier=%q", req.TransferID, req.Path, req.Tier))
		return
	}
	if req.Tier != "a" && req.Tier != "b" {
		b.replyPushErr(msg, "tier_invalid", req.Tier)
		return
	}
	if req.Size < 0 {
		b.replyPushErr(msg, "request_invalid", fmt.Sprintf("size=%d must be non-negative", req.Size))
		return
	}
	if req.Size > transferMaxBytes {
		// batch C: the human-readable size is DERIVED, not hand-copied. The "(2 GiB)" that used to be
		// typed here would have kept saying 2 GiB after the constant moved — an operator reading the
		// refusal would compute the wrong limit and keep retrying at it.
		b.replyPushErr(msg, "too_large", fmt.Sprintf("size=%d > %d (%s)", req.Size, transferMaxBytes, proto.HumanBytes(transferMaxBytes)))
		return
	}
	if req.Tier == "a" && req.Size > transferTierAMaxBytes {
		b.replyPushErr(msg, "too_large",
			fmt.Sprintf("tier-a size=%d > %d (%s)", req.Size, transferTierAMaxBytes, proto.HumanBytes(transferTierAMaxBytes)))
		return
	}
	if req.Tier == "a" && int64(len(req.InlineData)) != req.Size {
		b.replyPushErr(msg, "request_invalid",
			fmt.Sprintf("tier=a but inline_data len=%d vs size=%d", len(req.InlineData), req.Size))
		return
	}

	// Tier B: broker ensures the per-session bucket exists before
	// forwarding (plan §Tier B push state machine step 2). Per-transfer
	// scoping happens via ObjectKey = TransferID.
	if req.Tier == "b" {
		if b.js == nil {
			b.replyPushErr(msg, "jetstream_unavailable", xferNoJetStreamMsg)
			return
		}
		// G6 #21 + A11: the disk-aware ceiling is computed ONCE and reused for BOTH the size-admission
		// check and the bucket sizing. G67: sizing and create no longer share a deadline, and a
		// TRANSIENT create failure is retried a bounded number of times and reported as retriable —
		// see provisionXferBucket. Runs BEFORE transfers.put(); do not move it after.
		bucket, tooLarge, perr := b.provisionXferBucket(b.runCtx, sid, req.Size)
		if tooLarge != nil {
			b.replyPushErr(msg, "too_large", fmt.Sprintf("size=%d > per-session tier-B ceiling %d on this broker (small disk)", req.Size, tooLarge.MaxBytes))
			return
		}
		if perr != nil {
			code, text := xferProvisionRefusal(perr)
			b.replyPushErr(msg, code, text)
			return
		}
		req.Bucket = bucket
		req.ObjectKey = req.TransferID
	}

	// Stamp actor fp so agent's audit attribution matches broker's.
	req.ActorFP = fp
	body, err := json.Marshal(&req)
	if err != nil {
		b.replyPushErr(msg, "marshal", err.Error())
		return
	}

	entry := &transferEntry{
		transferID: req.TransferID, sid: sid, nid: nid,
		actor: actor, actorFP: fp, verb: "push", tier: req.Tier,
		bucket: req.Bucket, path: req.Path, size: req.Size,
		startedAt: b.cfg.Now(),
	}
	if code := b.transfers.put(entry); code != "" {
		// Duplicate id or in-flight cap reached. Per-session bucket
		// survives across transfers, so no bucket cleanup needed here.
		b.replyPushErr(msg, code,
			fmt.Sprintf("transfer %s rejected (%s); retry shortly or use a fresh transfer id", req.TransferID, code))
		return
	}
	// #57 Lane B: persist the durable in-flight ledger BEFORE forwarding, so a home crash between here and a
	// terminal is recovered into a synthetic terminal audit (not a dangling start row).
	b.writeXferInflight(entry)
	entry.cancel = b.startTransferWatchdog(b.runCtx, entry)

	// Audit start (plan §Audit "start written from broker-accepted prepare").
	b.emitTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "start", Verb: "push",
		Ts: entry.startedAt, Session: sid, Node: nid,
		ActorNkey: actor, ActorFp: fp,
		TransferID: req.TransferID, Path: req.Path,
		Size: req.Size, SHA256: req.SHA256, Tier: req.Tier, Bucket: req.Bucket,
	})

	// R7a ORDERING GUARD (do not remove — it is load-bearing for the PERIODIC orphan reaper).
	// The reaper (#58) used to run once at boot; it now runs on every reconcile sweep, so "delete objects
	// this broker has no tracker entry for" changed from a one-shot cleanup into a loop. What keeps that
	// loop from eating a live upload is an ORDERING FACT: the tracker entry is put() above, BEFORE the
	// prepare reaches the agent, and the agent is the only writer of objects — so no object can exist
	// during the unprotected window. That fact was a convention enforced by nothing. Moving this forward
	// above the put(), or adding a path that writes an object before registering, would silently turn the
	// reaper into a data-loss loop. So the convention is now a PRECONDITION: refuse to forward a prepare
	// the tracker does not already know about, rather than trust that nobody reorders these lines.
	if b.transfers.get(entry.transferID) == nil {
		b.replyPushErr(msg, "internal_error",
			"transfer prepare would be forwarded to the agent before its tracker entry exists — "+
				"the periodic orphan reaper could delete the upload mid-flight (R7a ordering guard)")
		return
	}

	// Forward to agent, preserving ctl's reply inbox so the agent's
	// PushPrepareResp goes straight back to ctl (broker stays out of
	// the data path).
	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyPushErr(msg, "forward_failed", err.Error())
		// best-effort cleanup of the entry/bucket if we couldn't even forward
		b.cleanupEntry(entry, "broker_forward_failed", err.Error())
		return
	}
	b.cfg.Logger.Info("broker: push.req forwarded",
		"sid", sid, "nid", nid, "actor", actor, "tier", req.Tier,
		"transfer_id", req.TransferID, "size", req.Size)
}

// handlePullReq is the broker entry for `tether pull`. ctl is the
// receiver here; the broker still does the standard preconditions and
// optimistically pre-creates a tier-B bucket (the agent may choose
// tier A based on Size; the unused bucket is reaped by finalize.req
// or watchdog timeout). Audit start writes immediately.
func (b *Broker) handlePullReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "pull" {
		b.replyPullErr(msg, "subject_malformed", msg.Subject)
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyPullErr(msg, "actor_invalid", err.Error())
		return
	}
	if !b.transferHomeGate(sid, nid) {
		return // not this agent's home (or home unresolved): stay silent, the home answers (§9 D8)
	}
	if err := b.transferGate(sid, fp, nid); err != "" {
		b.replyPullErr(msg, err, "")
		return
	}
	var req proto.PullPrepareReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyPullErr(msg, "json_parse", err.Error())
		return
	}
	if req.TransferID == "" || req.Path == "" {
		b.replyPullErr(msg, "request_invalid",
			fmt.Sprintf("transfer_id=%q path=%q", req.TransferID, req.Path))
		return
	}

	// Ensure the per-session bucket exists (no-op if already there).
	// Cheap because it's per-session, not per-transfer. Agent uses
	// it iff Size > MaxInline; otherwise nothing is written and the
	// bucket stays untouched. Per-transfer cleanup deletes only the
	// object (deleteXferObject), never the bucket.
	// G67: same provisioning seam as the push path — separate deadlines for the best-effort sizing
	// probe and the load-bearing create, plus a bounded classified retry, so a transient JetStream
	// stall is reported as retriable instead of terminal. Pull passes size 0: the agent decides
	// tier A vs B itself, so there is no admission size to check here (tooLarge is unreachable).
	bucket := ""
	bucketPayloadMax := int64(0)
	if b.js != nil {
		var perr *xferProvisionErr
		bucket, bucketPayloadMax, _, perr = provisionXferBucketWithLimit(b.runCtx, b, sid, 0)
		if perr != nil {
			code, text := xferProvisionRefusal(perr)
			b.replyPullErr(msg, code, text)
			return
		}
	}

	req.ActorFP = fp
	// Pass bucket info to agent via additional fields on the forwarded
	// payload. The agent will read these if it elects tier B.
	body, err := json.Marshal(struct {
		proto.PullPrepareReq
		Bucket                string `json:"bucket,omitempty"`
		ObjectKey             string `json:"object_key,omitempty"`
		BucketPayloadMaxBytes *int64 `json:"bucket_payload_max_bytes,omitempty"`
	}{
		PullPrepareReq:        req,
		Bucket:                bucket,
		ObjectKey:             req.TransferID,
		BucketPayloadMaxBytes: &bucketPayloadMax,
	})
	if err != nil {
		b.replyPullErr(msg, "marshal", err.Error())
		return
	}

	entry := &transferEntry{
		transferID: req.TransferID, sid: sid, nid: nid,
		actor: actor, actorFP: fp, verb: "pull",
		tier:   "b", // optimistic; finalize.req body may say "a" — we delete the per-transfer object regardless
		bucket: bucket, path: req.Path,
		startedAt: b.cfg.Now(),
	}
	if code := b.transfers.put(entry); code != "" {
		// Per-session bucket survives across transfers; nothing to
		// reap on this rejection.
		b.replyPullErr(msg, code,
			fmt.Sprintf("transfer %s rejected (%s); retry shortly or use a fresh transfer id", req.TransferID, code))
		return
	}
	// #57 Lane B: durable in-flight ledger BEFORE forwarding (see handlePushReq).
	b.writeXferInflight(entry)
	entry.cancel = b.startTransferWatchdog(b.runCtx, entry)

	b.emitTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "start", Verb: "pull",
		Ts: entry.startedAt, Session: sid, Node: nid,
		ActorNkey: actor, ActorFp: fp,
		TransferID: req.TransferID, Path: req.Path,
		Tier: "?", Bucket: bucket,
	})

	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyPullErr(msg, "forward_failed", err.Error())
		b.cleanupEntry(entry, "broker_forward_failed", err.Error())
		return
	}
	b.cfg.Logger.Info("broker: pull.req forwarded",
		"sid", sid, "nid", nid, "actor", actor,
		"transfer_id", req.TransferID, "bucket", bucket)
}

// handlePushCommitReq is the tier-B push step where ctl signals
// "ObjectStore.Put completed; please Get + verify + rename + emit
// ev.transfer". Same gating as the other handlers; the body just
// names the transfer + bucket. Broker forwards to the agent and acks
// ctl synchronously.
func (b *Broker) handlePushCommitReq(nc *nats.Conn, msg *nats.Msg) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(msg.Subject)
	if !ok || verb != "push-commit" {
		b.replyCommitErr(msg, "subject_malformed", msg.Subject)
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyCommitErr(msg, "actor_invalid", err.Error())
		return
	}
	var req proto.TransferCommitReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyCommitErr(msg, "json_parse", err.Error())
		return
	}
	entry := b.transfers.get(req.TransferID)
	if entry == nil {
		// Tracker miss. In clustered mode (selfID!="") this broker is NOT the transfer's
		// origin — every broker receives this broadcast subject, so a non-owner MUST stay
		// SILENT (else it would race-answer transfer_unknown before the real home's OK,
		// §9 D8 continuation routing). In production (no cluster identity) the single broker
		// IS the owner, so a miss is a genuine unknown transfer → reply (byte-identical).
		if b.selfNodeID() == "" {
			b.replyCommitErr(msg, "transfer_unknown", req.TransferID)
		}
		return
	}
	// audit transfer F5: the transfer_id is a shared key — cross-check it belongs to THIS
	// subject's (sid,nid) so a valid actor cannot drive a push-commit against another session's
	// transfer (consistency with handleEvTransfer's sid/nid guard).
	if entry.sid != sid || entry.nid != nid {
		b.replyCommitErr(msg, "not_owner_or_creator", "")
		return
	}
	if err := b.transferGate(sid, fp, nid); err != "" {
		b.replyCommitErr(msg, err, "")
		return
	}
	if entry.actor != actor || entry.verb != "push" {
		b.replyCommitErr(msg, "not_owner_or_creator", "")
		return
	}
	if entry.tier != "b" {
		b.replyCommitErr(msg, "tier_invalid", "push-commit requires tier b")
		return
	}
	req.ActorFP = fp
	body, _ := json.Marshal(&req)
	fwd := &nats.Msg{
		Subject: proto.SubjCmdForwarded(sid, nid, verb),
		Reply:   msg.Reply,
		Data:    body,
	}
	if err := nc.PublishMsg(fwd); err != nil {
		b.replyCommitErr(msg, "forward_failed", err.Error())
		return
	}
}

// handleEvTransfer subscribes to s.*.ev.node.*.transfer.>. Push
// receiver-side finalization: agent has Get/SHA/rename'd (or failed)
// the file. Broker writes audit + cleans up bucket. file-transfer-plan
// §Audit table "push" row.
func (b *Broker) handleEvTransfer(msg *nats.Msg) {
	sid, nid, transferID, kind, ok := proto.ParseEvTransfer(msg.Subject)
	if !ok {
		return
	}
	var ev proto.TransferEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		b.cfg.Logger.Warn("broker: ev.transfer parse", "err", err)
		return
	}
	if kind != "complete" && kind != "failed" {
		return
	}

	// Validate-then-claim. The fields used here (sid, nid) are
	// immutable after put(), so reading them via the lookup snapshot
	// without holding tracker.mu is safe. claimFinalize is the only
	// state-mutating call.
	preview := b.transfers.get(transferID)
	if preview == nil {
		// batch C: THIS is where a late completion is dropped, not the !claimed branch below —
		// finalizeTransfer already called transfers.remove, so a watchdog-reaped transfer is simply
		// absent by the time the receiver's real event lands. That silence is the last step of "the
		// file landed on disk but the audit says failed", so say it out loud when we know we just
		// reaped this id. Unknown ids (a replay, a foreign broker's transfer) stay quiet: they are
		// routine and would drown the signal.
		if b.transfers.recentlyReaped(transferID) {
			b.cfg.Logger.Warn("broker: receiver reported a transfer AFTER the broker's budget expired and "+
				"the terminal audit was already written — the audit says failed; the file may well be on "+
				"disk at the destination",
				"transfer_id", transferID, "kind", kind, "session", sid, "node", nid)
		}
		return // unknown transfer; ignore
	}
	if preview.sid != sid || preview.nid != nid {
		b.cfg.Logger.Warn("broker: ev.transfer sid/nid mismatch",
			"want_sid", preview.sid, "got_sid", sid,
			"want_nid", preview.nid, "got_nid", nid)
		return
	}
	entry, claimed := b.transfers.claimFinalize(transferID)
	if !claimed {
		// Already finalized by watchdog or another caller. Don't
		// double-write audit / double-delete the transfer object.
		return
	}

	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: kind, Verb: entry.verb,
		Ts: b.cfg.Now(), Session: entry.sid, Node: entry.nid,
		ActorNkey: entry.actor, ActorFp: entry.actorFP,
		TransferID: entry.transferID, Path: entry.path,
		Tier: entry.tier, Bucket: entry.bucket,
		Bytes: ev.Bytes, DurationMs: ev.DurationMs,
	}
	if kind == "failed" {
		rec.Code = ev.Code
		rec.Error = ev.Error
	}
	b.emitTerminalTransferAudit(rec, entry.transferID)
	_ = b.deleteXferObject(context.Background(), entry.sid, entry.transferID)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)
	// F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here.
	b.cfg.Logger.Info("broker: ev.transfer handled",
		"transfer_id", transferID, "kind", kind, "verb", entry.verb)
}

// cleanupEntry is used on the unhappy paths inside the prepare
// handlers (forward_failed). Writes a failed audit + drops the
// bucket + cancels the watchdog. Idempotent: if a watchdog or
// finalize handler already claimed the entry, this is a no-op.
func (b *Broker) cleanupEntry(entry *transferEntry, code, errMsg string) {
	ent, claimed := b.transfers.claimFinalize(entry.transferID)
	if !claimed || ent == nil {
		return
	}
	b.finalizeTransfer(ent, schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
		Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
		ActorNkey: ent.actor, ActorFp: ent.actorFP,
		TransferID: ent.transferID, Path: ent.path,
		Tier: ent.tier, Bucket: ent.bucket,
		Code: code, Error: errMsg,
	}, true)
}

// finalizeTransfer performs the terminal disposal shared by the watchdog and
// the failure path: emit the terminal audit, delete the staged object, then drop
// the tracker entry.
//
// cancelEntry is EXPLICIT rather than inferred. The watchdog must pass false —
// it IS the owner of entry.cancel, and calling it there would be a no-op (the
// only thing that ctx guards is the watchdog's own select, and the object delete
// on this path deliberately uses context.Background()). Making the difference a
// named argument is the point: the previous shape had the two paths written out
// separately, and the ONLY record of why one of them omits cancel() was the
// absence of a line.
//
// Batch-A A10 deliberately does NOT fold in two other sites that look similar:
//   - transfer.go's rec-building blocks assemble an audit record, they are not
//     terminal disposal;
//   - xfer_inflight.go's stranded-transfer recovery must NOT delete the ledger
//     unless the synthetic terminal was durably COMMITTED (the #57 M1
//     invariant) — its delete is conditional where these two are unconditional,
//     so merging them would invert the guarantee.
func (b *Broker) finalizeTransfer(ent *transferEntry, rec schema.AuditTransfer, cancelEntry bool) {
	b.emitTerminalTransferAudit(rec, ent.transferID)
	_ = b.deleteXferObject(context.Background(), ent.sid, ent.transferID)
	if cancelEntry && ent.cancel != nil {
		ent.cancel()
	}
	b.transfers.remove(ent.transferID)
	// F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here.
}

// transferGate runs the standard precondition trio used by every
// prepare/commit handler. Pass nid="" to skip the node-online check
// (used for finalize.req / caps.req which don't carry a nid in their
// subject). Returns "" on accept, else the response Code.
func (b *Broker) transferGate(sid, fp, nid string) string {
	// Batch-A review M13: A2 established that a store_error's SQLite detail
	// belongs in the broker log rather than on the wire — but the detail was
	// simply dropped here, so it went NOWHERE. "Only in the log" was a claim
	// about a log line that did not exist; the operator lost the one string that
	// says whether the DB is locked, corrupt, or missing a table.
	logStore := func(op string, err error) string {
		if b.cfg.Logger != nil {
			b.cfg.Logger.Warn("broker: transfer gate storage error",
				"op", op, "sid", sid, "nid", nid, "err", err)
		}
		return "store_error"
	}

	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		return logStore("session.IsActive", err)
	}
	if !active {
		return "session_not_found_or_deleting"
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		return logStore("session.IsMember", err)
	}
	if !member {
		return "not_a_member"
	}
	if nid == "" {
		return "" // caller doesn't need the node check (e.g. finalize.req, caps.req)
	}
	status, err := node.LookupStatus(b.cfg.DB, sid, nid)
	if errors.Is(err, node.ErrNotFound) {
		return "node_not_found"
	}
	if err != nil {
		return logStore("node.LookupStatus", err)
	}
	if status != node.StateOnline {
		return "node_offline"
	}
	return ""
}

// replyPushErr / replyPullErr / replyCommitErr are tiny typed wrappers
// over b.replyJSON: they let call sites stay terse (`b.replyPushErr(msg,
// "tier_invalid", "")`) while keeping the response type discoverable.
func (b *Broker) replyPushErr(msg *nats.Msg, code, errMsg string) {
	b.replyJSON(msg, proto.PushPrepareResp{OK: false, Code: code, Error: errMsg})
}
func (b *Broker) replyPullErr(msg *nats.Msg, code, errMsg string) {
	b.replyJSON(msg, proto.PullPrepareResp{OK: false, Code: code, Error: errMsg})
}
func (b *Broker) replyCommitErr(msg *nats.Msg, code, errMsg string) {
	b.replyJSON(msg, proto.TransferCommitResp{OK: false, Code: code, Error: errMsg})
}

// ─── Caps probe (caps.req) ─────────────────────────────────────────────

// handleCapsReq replies to `ctrl.by.<actor>.s.<sid>.caps.req` with
// broker capabilities the CLI needs before chooseTier:
//
//   - JetStreamReady: whether tier-B (ObjectStore) is usable.
//   - MaxPayload: the server-advertised max NATS payload (bytes).
//     CLI uses this to cap tier-A inline_data to a value the actual
//     server will accept (operator may have left max_payload at the
//     1 MiB default even though the design ceiling is 8 MiB).
//   - BrokerRelease / BrokerProto: lets ctl print a useful version-
//     skew message instead of hanging on a wrong-proto agent.
//
// Membership gate mirrors handlePsReq — sid is parsed from the subject
// and validated against the session row + the actor's fp.
//
// file-transfer-plan §Wire protocol "caps".
func (b *Broker) handleCapsReq(msg *nats.Msg) {
	actor, leaf, ok := proto.ParseCtrlBy(msg.Subject)
	if !ok {
		b.replyJSON(msg, proto.CapsResp{Code: "subject_malformed"})
		return
	}
	parts := splitDot(leaf)
	// leaf = "s.<sid>.caps.req"
	if len(parts) != 4 || parts[0] != "s" || parts[2] != "caps" || parts[3] != "req" {
		b.replyJSON(msg, proto.CapsResp{Code: "subject_malformed", Error: leaf})
		return
	}
	sid := parts[1]

	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "actor_invalid", Error: err.Error()})
		return
	}
	// Batch-A A2: this used to inline transferGate's first two checks. The
	// helper lives 60 lines up and its doc comment already named this handler
	// ("used for finalize.req / caps.req"), so the copy was pure drift risk.
	//
	// The inline copy also put err.Error() — raw SQLite text: db path, table
	// and constraint names — on the wire to any session member. transferGate
	// returns the code alone, and that is the behaviour we want: store_error
	// detail belongs in the broker log, not in a reply to a non-owner.
	if code := b.transferGate(sid, fp, ""); code != "" {
		b.replyJSON(msg, proto.CapsResp{Code: code})
		return
	}

	resp := proto.CapsResp{
		OK:             true,
		JetStreamReady: b.js != nil,
		BrokerRelease:  proto.ReleaseVersion,
		BrokerProto:    proto.ProtoVersion,
	}
	if nc := b.nc.Load(); nc != nil {
		resp.MaxPayload = nc.MaxPayload()
	}
	b.replyJSON(msg, resp)
}

// ─── Pull receiver finalize (transfer.<id>.finalize.req) ──────────────

// handleFinalizeReq is invoked on
// `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req`.
//
// Pull receiver-side: ctl has finished local writing (tier A inline
// or tier B Get+rename). It tells the broker so audit complete|failed
// can be written and the bucket cleaned up. Two layers of defence:
//
//  1. NATS-layer: the JWT pub allow is scoped to (sid, actor) via
//     `ctrl.by.<actor>.s.<sid>.transfer.*.finalize.req`. A different
//     sid never reaches here. A different actor publishing for sid
//     reaches here only via subject forgery — which is impossible
//     because the broker parses the actor segment, not the body.
//  2. Application-layer (this function): the in-memory entry for
//     transfer_id is looked up; the publishing actor MUST match the
//     creator. Mismatch → not_owner_or_creator (Round-3 #1).
//
// Idempotent on duplicate finalize (Round-3 case #18): the second
// caller sees finalized=true and gets OK back without re-writing
// audit / re-deleting bucket.
//
// file-transfer-plan §Wire / §Audit / §Risk Round-4 #1.
func (b *Broker) handleFinalizeReq(msg *nats.Msg) {
	actor, sid, transferID, ok := proto.ParseTransferFinalize(msg.Subject)
	if !ok {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "subject_malformed"})
		return
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "actor_invalid", Error: err.Error()})
		return
	}
	var fin proto.TransferFinalize
	if err := json.Unmarshal(msg.Data, &fin); err != nil {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	if fin.TransferID != "" && fin.TransferID != transferID {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "request_invalid", Error: "transfer_id mismatch subject vs body"})
		return
	}
	if fin.Kind != "complete" && fin.Kind != "failed" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "request_invalid", Error: "kind must be complete|failed"})
		return
	}

	// Validate-then-claim. Read the current entry without claiming;
	// validate against immutable fields (actor, verb), then atomically
	// claim. This avoids the previous "claim, then unclaim on
	// validation failure" race where the unclaim happened outside
	// tracker.mu.
	preview := b.transfers.get(transferID)
	if preview == nil {
		// Tracker miss: SILENT in clustered mode (not this broker's transfer — a non-owner
		// must not race-answer transfer_unknown before the real home, §9 D8); reply in
		// production (no cluster identity, the single owner — genuine unknown / already reaped).
		if b.selfNodeID() == "" {
			b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "transfer_unknown"})
		}
		return
	}
	if code := b.transferGate(sid, fp, ""); code != "" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: code})
		return
	}
	// audit transfer F5: cross-check the transfer belongs to THIS subject's sid (the transfer_id
	// is a shared key) so a valid actor cannot finalize another session's transfer — consistency
	// with handleEvTransfer's sid/nid guard.
	if preview.sid != sid {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "not_owner_or_creator"})
		return
	}
	if preview.actor != actor {
		// Foreign-actor finalize: leave entry untouched so the real
		// owner / watchdog handles it.
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "not_owner_or_creator"})
		return
	}
	if preview.verb != "pull" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false,
			Code: "verb_mismatch", Error: "finalize.req is pull-only"})
		return
	}
	entry, claimed := b.transfers.claimFinalize(transferID)
	if !claimed {
		// Concurrent finalize won the race (duplicate from same actor,
		// or watchdog raced us after our preview). Reply OK so the
		// caller sees idempotent success — audit and bucket cleanup
		// are guaranteed by whoever did claim.
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: true})
		return
	}

	// Resolve tier. ctl-supplied body wins over the optimistic "b" we
	// stamped at pull.req time (tier-A pulls have an empty bucket).
	tier := entry.tier
	if fin.Tier == "a" || fin.Tier == "b" {
		tier = fin.Tier
	}

	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: fin.Kind, Verb: "pull",
		Ts: b.cfg.Now(), Session: entry.sid, Node: entry.nid,
		ActorNkey: entry.actor, ActorFp: entry.actorFP,
		TransferID: entry.transferID, Path: entry.path,
		Tier: tier, Bucket: entry.bucket,
		Bytes: fin.Bytes, DurationMs: fin.DurationMs,
	}
	if fin.Kind == "failed" {
		rec.Code = fin.Code
		rec.Error = fin.Error
	}
	b.emitTerminalTransferAudit(rec, entry.transferID)
	_ = b.deleteXferObject(context.Background(), entry.sid, entry.transferID)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)
	// F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here.

	b.replyFinalize(msg, proto.TransferFinalizeResp{OK: true})
	b.cfg.Logger.Info("broker: pull finalize handled",
		"transfer_id", transferID, "kind", fin.Kind, "tier", tier)
}

func (b *Broker) replyFinalize(msg *nats.Msg, resp proto.TransferFinalizeResp) {
	b.replyJSON(msg, resp)
}
