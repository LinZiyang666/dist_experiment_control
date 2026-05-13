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
// Bucket lifecycle invariant (plan §Object bucket lifecycle): the
// broker is the SOLE owner of OBJ_xfer-<sid>-<transfer_id> stream
// CREATE / DELETE. ctl + agent only Put/Get/Watch via the data
// subjects ($O.xfer-<sid>-<id>.{M,C}.>). Member/agent permission
// templates explicitly omit STREAM.CREATE/DELETE/PURGE; see
// internal/auth/permissions.go.
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
	transferTimeoutTierA = 30 * time.Second
	transferTimeoutTierB = 5 * time.Minute
)

// transferMaxBytes is the hard upper bound for one tier-B transfer.
// Bumped 200 MiB → 2 GiB in v0.2.5; the per-session bucket MaxBytes
// (ensureXferBucket) was already set to 4 GiB which leaves 2 GiB
// headroom for one in-flight + one staging transfer per session.
const transferMaxBytes = 2 * 1024 * 1024 * 1024

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

// put inserts an entry. Returns true on success; false if the tracker
// is at transferTrackerMaxEntries. Callers must reject the request
// with `too_many_in_flight` on false.
func (t *transferTracker) put(e *transferEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.entries) >= transferTrackerMaxEntries {
		// Don't overwrite an existing entry with the same id — that
		// would silently drop the original transfer.
		if _, exists := t.entries[e.transferID]; !exists {
			return false
		}
	}
	t.entries[e.transferID] = e
	return true
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
// a second audit row / delete the bucket twice.
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
// MaxBytes covers many concurrent transfers + their staging space:
// ceil at 4 GiB per session. Per-transfer ObjectMeta caps individual
// objects via the chunked-message TTL on the underlying stream.
//
// v0.2.2 change: previously each transfer got its own bucket
// `xfer-<sid>-<id>`, but NATS perm wildcards can't address that
// (no partial-token wildcards), so JWTs couldn't allow per-bucket
// access. Per-session buckets allow a single literal perm entry.
func (b *Broker) ensureXferBucket(ctx context.Context, sid string) (string, error) {
	if b.js == nil {
		return "", fmt.Errorf("jetstream_unavailable")
	}
	bucket := proto.XferBucketName(sid)
	cfg := jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		MaxBytes: 8 * 1024 * 1024 * 1024, // 8 GiB per-session ceiling (allows ~3 in-flight 2-GiB transfers + headroom)
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	}
	if _, err := b.js.CreateObjectStore(ctx, cfg); err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) ||
			errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return bucket, nil
		}
		return "", fmt.Errorf("create_bucket: %w", err)
	}
	return bucket, nil
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
		b.pubAuditTransfer(schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
			Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
			ActorNkey: ent.actor, ActorFp: ent.actorFP,
			TransferID: ent.transferID, Path: ent.path, Tier: ent.tier,
			Bucket: ent.bucket, DurationMs: msSince(ent.startedAt, b.cfg.Now()),
			Code:  code,
			Error: fmt.Sprintf("broker watchdog: no %s finalization within %s", ent.verb, d),
		})
		_ = b.deleteXferObject(context.Background(), ent.sid, ent.transferID)
		b.transfers.remove(ent.transferID)
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
// `tether.v1.s.<sid>.audit.transfer`. Best-effort: failures log a
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

// handlePushReq is the broker entry for `tether push`. It runs the
// standard ctl preconditions (sid alive, actor is a member, node is
// ONLINE), pre-creates the tier-B bucket if needed, registers the
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
	if req.Size > transferMaxBytes {
		b.replyPushErr(msg, "too_large", fmt.Sprintf("size=%d > %d (2 GiB)", req.Size, transferMaxBytes))
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
			b.replyPushErr(msg, "jetstream_unavailable", "")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bucket, err := b.ensureXferBucket(ctx, sid)
		cancel()
		if err != nil {
			b.replyPushErr(msg, "bucket_create_failed", err.Error())
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
	if !b.transfers.put(entry) {
		// In-flight cap reached. Per-session bucket survives across
		// transfers, so no bucket cleanup needed here — just reject.
		b.replyPushErr(msg, "too_many_in_flight",
			fmt.Sprintf("broker has %d transfers in flight; retry shortly", transferTrackerMaxEntries))
		return
	}
	entry.cancel = b.startTransferWatchdog(b.runCtx, entry)

	// Audit start (plan §Audit "start written from broker-accepted prepare").
	b.pubAuditTransfer(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "start", Verb: "push",
		Ts: entry.startedAt, Session: sid, Node: nid,
		ActorNkey: actor, ActorFp: fp,
		TransferID: req.TransferID, Path: req.Path,
		Size: req.Size, SHA256: req.SHA256, Tier: req.Tier, Bucket: req.Bucket,
	})

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
	bucket := ""
	if b.js != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bucket, err = b.ensureXferBucket(ctx, sid)
		cancel()
		if err != nil {
			b.replyPullErr(msg, "bucket_create_failed", err.Error())
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
	if !b.transfers.put(entry) {
		// Per-session bucket survives across transfers; nothing to
		// reap on this rejection.
		b.replyPullErr(msg, "too_many_in_flight",
			fmt.Sprintf("broker has %d transfers in flight; retry shortly", transferTrackerMaxEntries))
		return
	}
	entry.cancel = b.startTransferWatchdog(b.runCtx, entry)

	b.pubAuditTransfer(schema.AuditTransfer{
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
	if err := b.transferGate(sid, fp, nid); err != "" {
		b.replyCommitErr(msg, err, "")
		return
	}
	var req proto.TransferCommitReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		b.replyCommitErr(msg, "json_parse", err.Error())
		return
	}
	entry := b.transfers.get(req.TransferID)
	if entry == nil {
		b.replyCommitErr(msg, "transfer_unknown", req.TransferID)
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
		// double-write audit / double-delete bucket.
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
	b.pubAuditTransfer(rec)
	_ = b.deleteXferObject(context.Background(), entry.sid, entry.transferID)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)
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
	b.pubAuditTransfer(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "failed", Verb: ent.verb,
		Ts: b.cfg.Now(), Session: ent.sid, Node: ent.nid,
		ActorNkey: ent.actor, ActorFp: ent.actorFP,
		TransferID: ent.transferID, Path: ent.path,
		Tier: ent.tier, Bucket: ent.bucket,
		Code: code, Error: errMsg,
	})
	_ = b.deleteXferObject(context.Background(), ent.sid, ent.transferID)
	if ent.cancel != nil {
		ent.cancel()
	}
	b.transfers.remove(ent.transferID)
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
	if b.nc != nil {
		resp.MaxPayload = b.nc.MaxPayload()
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
	if code := b.transferGate(sid, fp, ""); code != "" {
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: code})
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
		// Unknown transfer_id (or already reaped by watchdog).
		// Idempotent reply.
		b.replyFinalize(msg, proto.TransferFinalizeResp{OK: false, Code: "transfer_unknown"})
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
	b.pubAuditTransfer(rec)
	_ = b.deleteXferObject(context.Background(), entry.sid, entry.transferID)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)

	b.replyFinalize(msg, proto.TransferFinalizeResp{OK: true})
	b.cfg.Logger.Info("broker: pull finalize handled",
		"transfer_id", transferID, "kind", fin.Kind, "tier", tier)
}

func (b *Broker) replyFinalize(msg *nats.Msg, resp proto.TransferFinalizeResp) {
	b.replyJSON(msg, resp)
}
