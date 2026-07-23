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
const (
	transferTimeoutTierA  = 30 * time.Second
	transferTimeoutTierB  = 5 * time.Minute
	transferTierAMaxBytes = 8 * 1024 * 1024
)

// xferCrossHomeReapAge (R16 #58 Lane C) is the age floor for the LEADER-driven cross-home GC of a
// split-home / zero-node OBJ_xfer bucket that no single home broker can reap (homeOwnsXferBucket is false
// on every broker). It is deliberately LONGER than the per-home grace (xferReapMinObjectAge, 2m): the
// leader deletes another home's objects here, and it sees their ModTime across nodes with clock skew, so a
// transfer still live on ANOTHER broker's home must never be torn out. Derived from transferTimeoutTierB (a
// live tier-B transfer terminates by its watchdog within one tier-B timeout) with a 3× margin; pinned by a
// test so the relation cannot silently drift. A deploy-tier drill compresses it via the serveconf seam.
const xferCrossHomeReapAge = 3 * transferTimeoutTierB

// transferMaxBytes is the GLOBAL hard upper bound for one tier-B transfer (2 GiB). On a small-disk
// broker the per-session bucket ceiling (xferBucketMaxBytes, G6 #21) can be LOWER, and the push
// admission gate rejects at min(transferMaxBytes, that bucket ceiling).
const transferMaxBytes = 2 * 1024 * 1024 * 1024

// G6 #21: OBJ_xfer per-session bucket sizing. The bucket MaxBytes is disk-aware — a fraction of the JS
// store ceiling left after the events/history reservations — clamped to [floor, cap]. Below the floor we
// REFUSE (never emit MaxBytes<=0, which nats treats as UNLIMITED → a worse silent re-brick). The cap is
// the legacy 8 GiB, so large-disk brokers keep today's behavior.
const (
	xferEventsHistoryReserve = 2 * 1024 * 1024 * 1024 // events(1 GiB)+history(1 GiB) — the other JS reservations
	xferBucketFloor          = 256 * 1024 * 1024      // smallest useful per-session tier-B ceiling
	xferBucketCap            = 8 * 1024 * 1024 * 1024 // legacy ceiling (large-disk brokers unchanged)
)

// transferTrackerMaxEntries caps the in-memory tracker map so a fast
// attacker spamming push.req / pull.req can't OOM the broker before
// the per-tier watchdog reaps stale entries. At the worst case
// (5-min Tier-B timeout * 1024 entries * ~200 bytes/entry) the cap
// holds memory under ~200 KiB. New requests past the cap are
// rejected with `too_many_in_flight` so callers get a clean error
// instead of silent slowdown. Audit shard P11 F2.
const transferTrackerMaxEntries = 1024

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
}

func newTransferTracker() *transferTracker {
	return &transferTracker{entries: map[string]*transferEntry{}}
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
	if len(t.entries) >= transferTrackerMaxEntries {
		return "too_many_in_flight"
	}
	t.entries[e.transferID] = e
	return ""
}

func (t *transferTracker) remove(id string) *transferEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[id]
	delete(t.entries, id)
	return e
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
// jsStoreCeiling returns the JetStream file-store ceiling in bytes: nats's own AccountInfo.MaxStore when
// finite (the EXACT admission number), else a statfs-derived ~0.75 of the store partition, else 0 (caller
// falls back to the legacy cap). Never negative.
func (b *Broker) jsStoreCeiling(ctx context.Context) int64 {
	if b.js != nil {
		if info, err := b.js.AccountInfo(ctx); err == nil && info.Limits.MaxStore > 0 {
			return info.Limits.MaxStore // finite configured/derived JS store limit
		}
		// MaxStore <= 0 ⇒ UNLIMITED (nats reports -1) → fall through to statfs
	}
	if b.cfg.StoreDir != "" {
		if used, total, err := diskUsage(b.cfg.StoreDir); err == nil && total > used {
			free := total - used
			return int64(free) / 4 * 3 // ~0.75 of AVAILABLE space (nats sizes its default limit off free disk, not total)
		}
	}
	return 0
}

// xferBucketMaxBytes computes the disk-aware per-session OBJ_xfer bucket ceiling (G6 #21): a fraction of
// the JS store ceiling left after the events/history reservations, clamped to [floor, min(cap, avail)]. It
// REFUSES (rather than emit MaxBytes<=0, which nats treats as UNLIMITED) when the store is too small even
// for the floor. Unknown ceiling ⇒ the legacy 8 GiB cap (preserves large-disk behavior).
func (b *Broker) xferBucketMaxBytes(ctx context.Context) (int64, error) {
	return xferMaxBytesForCeiling(b.jsStoreCeiling(ctx))
}

// errXferStoreTooSmall marks the G6 #21 sizing refusal as a PERMANENT condition: a disk that cannot
// hold the tier-B floor is not a stall, so the #67 bounded provisioning retry must make ZERO create
// attempts against it. Wrapped (never re-worded) by xferMaxBytesForCeiling.
var errXferStoreTooSmall = errors.New("js store too small for tier-B")

// xferMaxBytesForCeiling is the pure disk-aware clamp (G6 #21), split out for testing. ceiling<=0
// (unknown) ⇒ the legacy cap. Otherwise a fraction of what's left after the events/history reservation,
// clamped to [floor, min(cap, avail)]; too small for the floor ⇒ refuse (never MaxBytes<=0, which nats
// treats as UNLIMITED → a worse silent re-brick).
func xferMaxBytesForCeiling(ceiling int64) (int64, error) {
	if ceiling <= 0 {
		return xferBucketCap, nil // unknown ceiling → legacy cap (never <=0/unlimited)
	}
	avail := ceiling - xferEventsHistoryReserve
	if avail < xferBucketFloor {
		// G67: wrapped in a SENTINEL so the #67 provisioning path can prove this refusal is
		// PERMANENT via errors.Is rather than by matching prose. The rendered text is byte-identical
		// to what it was before the sentinel was introduced. Without a structural handle, the
		// "permanent set" tests would pass with the entire permanent branch deleted, because the
		// classifier's default is already permanent.
		return 0, fmt.Errorf("%w: ceiling=%d bytes, need >= %d (events+history %d + floor %d)",
			errXferStoreTooSmall,
			ceiling, int64(xferEventsHistoryReserve)+int64(xferBucketFloor), int64(xferEventsHistoryReserve), int64(xferBucketFloor))
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

// ensureXferBucketSized is ensureXferBucket with a PRE-COMPUTED disk-aware ceiling (A11): the tier-B push
// admission path already computes the ceiling for its size check, so threading it in avoids a SECOND
// AccountInfo round-trip per transfer (each xferBucketMaxBytes issues a live AccountInfo, which nats.go
// does not cache). sizeErr carries a G6 #21 too-small refusal so it surfaces identically to the old path.
func (b *Broker) ensureXferBucketSized(ctx context.Context, sid string, targetReplicas int, maxBytes int64, sizeErr error) (string, error) {
	if b.js == nil {
		return "", fmt.Errorf("jetstream_unavailable")
	}
	if sizeErr != nil {
		return "", fmt.Errorf("xfer bucket sizing: %w", sizeErr)
	}
	bucket := proto.XferBucketName(sid)
	cfg := jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		MaxBytes: maxBytes, // G6 #21: disk-aware (was a hardcoded 8 GiB that denied tier-B on small-disk brokers)
		Storage:  jetstream.FileStorage,
		Replicas: targetReplicas, // D5 §6.4/§9: replicasFor(nVoters); live callers pass jsstream.ReplicasSingle
	}
	if _, err := b.js.CreateObjectStore(ctx, cfg); err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) ||
			errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			// Exists: RAISE-ONLY reconcile toward target (D5 §9, R-18). Idempotent +
			// retriable on a not-yet-ready meta-group (ErrMetaGroupNotReady).
			if err := raiseXferReplicas(ctx, b.js, bucket, targetReplicas); err != nil {
				return "", err
			}
			return bucket, nil
		}
		return "", fmt.Errorf("create_bucket: %w", err)
	}
	return bucket, nil
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
	curMax := int64(0)
	if si, serr := js.Stream(ctx, "OBJ_"+bucket); serr == nil {
		if info, ierr := si.Info(ctx); ierr == nil && info.Config.MaxBytes > 0 {
			curMax = info.Config.MaxBytes
		}
	}
	if curMax <= 0 {
		// G6 #21 (internal review): could not read the existing bucket's MaxBytes → do NOT raise with a
		// guessed ceiling (a wrong 8 GiB over-provisions a small disk; a shrink-below-used latches
		// replication_degraded). Skip this raise and retry next cycle via the retriable channel the callers
		// already handle.
		return jsstream.ErrMetaGroupNotReady
	}
	cfg := jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		MaxBytes: curMax,
		Storage:  jetstream.FileStorage,
		Replicas: target,
	}
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

// startTransferWatchdog arms a per-tier timeout. If the receiver-side
// finalization signal does not arrive in time, the watchdog writes a
// synthetic audit failed + reclaims the bucket. The returned cancel
// is stored on the entry so a successful finalize stops the watchdog.
func (b *Broker) startTransferWatchdog(parent context.Context, e *transferEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	d := transferTimeoutTierA
	if e.tier == "b" {
		d = transferTimeoutTierB
	}
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
		var code string
		switch ent.verb {
		case "push":
			code = "agent_no_responders"
		case "pull":
			code = "ctl_disconnect"
		default:
			code = "timeout"
		}
		b.emitTerminalTransferAudit(schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
			Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
			ActorNkey: ent.actor, ActorFp: ent.actorFP,
			TransferID: ent.transferID, Path: ent.path, Tier: ent.tier,
			Bucket: ent.bucket, DurationMs: msSince(ent.startedAt, b.cfg.Now()),
			Code:  code,
			Error: fmt.Sprintf("broker watchdog: no %s finalization within %s", ent.verb, d),
		}, ent.transferID)
		_ = b.deleteXferObject(context.Background(), ent.sid, ent.transferID)
		b.transfers.remove(ent.transferID)
		// F1: the ledger is dropped by emitTerminalTransferAudit's COMMIT callback, not here.
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
		b.replyPushErr(msg, "too_large", fmt.Sprintf("size=%d > %d (2 GiB)", req.Size, transferMaxBytes))
		return
	}
	if req.Tier == "a" && req.Size > transferTierAMaxBytes {
		b.replyPushErr(msg, "too_large",
			fmt.Sprintf("tier-a size=%d > %d (8 MiB)", req.Size, transferTierAMaxBytes))
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
	if b.js != nil {
		var perr *xferProvisionErr
		bucket, _, perr = b.provisionXferBucket(b.runCtx, sid, 0)
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
		Bucket    string `json:"bucket,omitempty"`
		ObjectKey string `json:"object_key,omitempty"`
	}{
		PullPrepareReq: req,
		Bucket:         bucket,
		ObjectKey:      req.TransferID,
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
	b.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
		Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
		ActorNkey: ent.actor, ActorFp: ent.actorFP,
		TransferID: ent.transferID, Path: ent.path,
		Tier: ent.tier, Bucket: ent.bucket,
		Code: code, Error: errMsg,
	}, ent.transferID)
	_ = b.deleteXferObject(context.Background(), ent.sid, ent.transferID)
	if ent.cancel != nil {
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
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		return "store_error"
	}
	if !active {
		return "session_not_found_or_deleting"
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		return "store_error"
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
		return "store_error"
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
	active, err := session.IsActive(b.cfg.DB, sid)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !active {
		b.replyJSON(msg, proto.CapsResp{Code: "session_not_found_or_deleting"})
		return
	}
	member, err := session.IsMember(b.cfg.DB, sid, fp)
	if err != nil {
		b.replyJSON(msg, proto.CapsResp{Code: "store_error", Error: err.Error()})
		return
	}
	if !member {
		b.replyJSON(msg, proto.CapsResp{Code: "not_a_member"})
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
