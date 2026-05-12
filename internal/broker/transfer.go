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

// Tier-A inline ceiling. Anything above this MUST go through tier B.
const transferTierAMaxBytes = 8 * 1024 * 1024

// Tier budgets — broker-side timeouts for the receiver-finalization
// signal to arrive. Past these the broker writes a synthetic
// failed-audit and reclaims the bucket. file-transfer-plan §Audit
// (timeout fallback) and §Object bucket lifecycle.
const (
	transferTimeoutTierA = 30 * time.Second
	transferTimeoutTierB = 5 * time.Minute
)

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

func (t *transferTracker) put(e *transferEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries[e.transferID] = e
}

func (t *transferTracker) remove(id string) *transferEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[id]
	delete(t.entries, id)
	return e
}

// markFinalized claims the right to publish the final audit row. The
// first caller wins; later callers see ok=false and must not write a
// second audit (idempotent finalize + timeout race protection).
func (t *transferTracker) markFinalized(id string) (e *transferEntry, ok bool) {
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

// xferBucketName returns "xfer-<sid>-<transfer_id>". The backing stream
// is "OBJ_<bucket>" per nats.go convention.
func xferBucketName(sid, transferID string) string {
	return "xfer-" + sid + "-" + transferID
}

// createXferBucket creates the OBJ_xfer-<sid>-<id> stream via the
// nats.go ObjectStore API. Tier B only. Returns the bucket name.
// Idempotent w.r.t. existing-name (treated as success — the broker
// uses ULID transfer_ids so collisions are practically zero, but
// defending against `nats stream list` shenanigans is cheap).
func (b *Broker) createXferBucket(ctx context.Context, sid, transferID string) (string, error) {
	if b.js == nil {
		return "", fmt.Errorf("jetstream_unavailable")
	}
	bucket := xferBucketName(sid, transferID)
	cfg := jetstream.ObjectStoreConfig{
		Bucket:   bucket,
		TTL:      30 * time.Minute,
		MaxBytes: 200 * 1024 * 1024, // hard ceiling: 200 MiB (plan §Tier B)
		Storage:  jetstream.FileStorage,
		Replicas: 1,
	}
	if _, err := b.js.CreateObjectStore(ctx, cfg); err != nil {
		// "stream name already in use" is acceptable here.
		if errors.Is(err, jetstream.ErrBucketExists) ||
			errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return bucket, nil
		}
		return "", fmt.Errorf("create_bucket: %w", err)
	}
	return bucket, nil
}

// deleteXferBucket drops the bucket; missing-bucket is treated as
// success (caller may invoke twice from timeout + finalize paths).
func (b *Broker) deleteXferBucket(ctx context.Context, bucket string) error {
	if b.js == nil || bucket == "" {
		return nil
	}
	err := b.js.DeleteObjectStore(ctx, bucket)
	if err == nil ||
		errors.Is(err, jetstream.ErrBucketNotFound) ||
		errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return err
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
		ent, ok := b.transfers.markFinalized(e.transferID)
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
		_ = b.deleteXferBucket(context.Background(), ent.bucket)
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
	if req.Size > 200*1024*1024 {
		b.replyPushErr(msg, "too_large", fmt.Sprintf("size=%d > 200 MiB", req.Size))
		return
	}
	if req.Tier == "a" && int64(len(req.InlineData)) != req.Size {
		b.replyPushErr(msg, "request_invalid",
			fmt.Sprintf("tier=a but inline_data len=%d vs size=%d", len(req.InlineData), req.Size))
		return
	}

	// Tier B: broker creates bucket before forwarding (plan §Tier B push state machine step 2).
	if req.Tier == "b" {
		if b.js == nil {
			b.replyPushErr(msg, "jetstream_unavailable", "")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bucket, err := b.createXferBucket(ctx, sid, req.TransferID)
		cancel()
		if err != nil {
			b.replyPushErr(msg, "bucket_create_failed", err.Error())
			return
		}
		req.Bucket = bucket
		req.ObjectKey = "object" // single-object-per-bucket convention
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
	b.transfers.put(entry)
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

	// Optimistic tier-B bucket creation. Agent uses it iff Size > MaxInline.
	// Tier-A pulls leave the bucket empty; finalize.req or watchdog
	// reaps it.
	bucket := ""
	if b.js != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bucket, err = b.createXferBucket(ctx, sid, req.TransferID)
		cancel()
		if err != nil {
			// Non-fatal for tier-A pulls; but we can't tell the agent
			// to fall back without losing the optimization. Reject
			// the request so the operator notices a misconfigured JS
			// instead of silently degrading every pull to tier A.
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
		ObjectKey:      "object",
	})
	if err != nil {
		b.replyPullErr(msg, "marshal", err.Error())
		return
	}

	entry := &transferEntry{
		transferID: req.TransferID, sid: sid, nid: nid,
		actor: actor, actorFP: fp, verb: "pull",
		tier:   "b", // optimistic; finalize.req body may say "a" — we delete the empty bucket regardless
		bucket: bucket, path: req.Path,
		startedAt: b.cfg.Now(),
	}
	b.transfers.put(entry)
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
	entry, claimed := b.transfers.markFinalized(transferID)
	if !claimed {
		// Either unknown transfer (replay / corrupted) or already
		// finalized by the watchdog. Either way: don't double-write.
		return
	}
	if entry.sid != sid || entry.nid != nid {
		b.cfg.Logger.Warn("broker: ev.transfer sid/nid mismatch",
			"want_sid", entry.sid, "got_sid", sid,
			"want_nid", entry.nid, "got_nid", nid)
		entry.finalized = false // unclaim; let the watchdog or a correct ev handle it
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
	_ = b.deleteXferBucket(context.Background(), entry.bucket)
	if entry.cancel != nil {
		entry.cancel()
	}
	b.transfers.remove(transferID)
	b.cfg.Logger.Info("broker: ev.transfer handled",
		"transfer_id", transferID, "kind", kind, "verb", entry.verb)
}

// cleanupEntry is used on the unhappy paths inside the prepare
// handlers (forward_failed). Writes a failed audit + drops the
// bucket + cancels the watchdog.
func (b *Broker) cleanupEntry(entry *transferEntry, code, errMsg string) {
	ent, claimed := b.transfers.markFinalized(entry.transferID)
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
	_ = b.deleteXferBucket(context.Background(), ent.bucket)
	if ent.cancel != nil {
		ent.cancel()
	}
	b.transfers.remove(ent.transferID)
}

// transferGate runs the standard precondition trio used by every
// prepare/commit handler. Returns "" on accept, else the response Code.
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

func (b *Broker) replyPushErr(msg *nats.Msg, code, errMsg string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.PushPrepareResp{OK: false, Code: code, Error: errMsg})
	_ = msg.Respond(payload)
}

func (b *Broker) replyPullErr(msg *nats.Msg, code, errMsg string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.PullPrepareResp{OK: false, Code: code, Error: errMsg})
	_ = msg.Respond(payload)
}

func (b *Broker) replyCommitErr(msg *nats.Msg, code, errMsg string) {
	if msg.Reply == "" {
		return
	}
	payload, _ := json.Marshal(proto.TransferCommitResp{OK: false, Code: code, Error: errMsg})
	_ = msg.Respond(payload)
}
