// Agent side of the file-transfer feature (file-transfer-plan v0.2.0).
//
// Verbs handled here:
//
//   push.req.forwarded         → handlePushForwarded
//     Tier A: agent IS receiver; verify SHA + write tmp + rename +
//             emit ev.transfer.<id>.complete|failed. Reply OK after
//             the byte work so ctl's request/reply latency includes
//             the file actually landing.
//     Tier B: agent acks "ready for your Put"; the bytes-on-disk
//             phase happens later under push-commit.req.forwarded.
//
//   push-commit.req.forwarded  → handlePushCommitForwarded
//     Tier B push step. Agent ObjectStore.Get from broker-created
//     bucket → SHA verify → rename → emit ev.transfer.
//
//   pull.req.forwarded         → handlePullForwarded
//     Agent is the sender. Validates allow_roots, stats the file,
//     compares size against MaxInline. Tier A → reply inline.
//     Tier B → ObjectStore.Put into broker-supplied bucket → reply
//             with bucket/object_key/size/sha. ctl is the receiver
//             and emits the finalize.req.

package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// tier-A inline ceiling mirrors broker side; file-transfer-plan §Tier selection.
const agentTierAMaxBytes = 8 * 1024 * 1024

// handlePushForwarded — push.req.forwarded entrypoint.
func (a *Agent) handlePushForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: push.req.forwarded without Reply inbox")
		return
	}
	var req proto.PushPrepareReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	if req.Tier != "a" && req.Tier != "b" {
		a.replyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: "tier_invalid", Error: req.Tier})
		return
	}
	vp, err := ValidateForWrite(req.Path, a.cfg.AllowRoots)
	if err != nil {
		var pve *PathValidationError
		if errors.As(err, &pve) {
			a.replyPush(nc, msg.Reply, proto.PushPrepareResp{
				OK: false, Code: pve.Code, Error: pve.Msg})
		} else {
			a.replyPush(nc, msg.Reply, proto.PushPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
		}
		return
	}

	switch req.Tier {
	case "a":
		a.handlePushTierA(nc, msg, &req, vp)
	case "b":
		// Cache prep state so push-commit knows what bucket + path
		// + sha to use. Bucket name was stamped by the broker.
		a.rememberPushCommit(req.TransferID, pushCommitEntry{
			vp:     vp,
			path:   req.Path,
			sha256: req.SHA256,
			size:   req.Size,
			force:  req.Force,
		})
		a.replyPush(nc, msg.Reply, proto.PushPrepareResp{OK: true, Tier: "b"})
	}
}

// handlePushTierA writes the inline bytes through the
// path.go tmp+rename pipeline, sha-verifies, and emits ev.transfer.
// The ctl-bound reply happens AFTER the write so ctl's RTT includes
// the actual outcome (file in place or rejected); ev.transfer is
// emitted unconditionally so the broker's audit always flows from the
// receiver-finalization invariant for symmetry with tier B.
func (a *Agent) handlePushTierA(nc *nats.Conn, msg *nats.Msg, req *proto.PushPrepareReq, vp *ValidatedPath) {
	startedAt := time.Now().UTC()
	finalize := func(code, errMsg string, bytes int64) {
		if code == "" {
			a.replyPush(nc, msg.Reply, proto.PushPrepareResp{OK: true, Tier: "a"})
			a.pubTransferEv(nc, "complete", "push", req, "a", bytes, time.Since(startedAt))
		} else {
			a.replyPush(nc, msg.Reply, proto.PushPrepareResp{OK: false, Code: code, Error: errMsg})
			a.pubTransferEvFailed(nc, "push", req, "a", code, errMsg, time.Since(startedAt))
		}
	}

	// SHA verify the inline blob BEFORE touching disk.
	if got := hexSHA256(req.InlineData); got != req.SHA256 {
		finalize("sha_mismatch",
			fmt.Sprintf("inline_data sha256: want=%s got=%s", req.SHA256, got), 0)
		return
	}
	rnd := randSuffix()
	f, tmp, err := OpenForWriteAtomic(vp, rnd)
	if err != nil {
		var pve *PathValidationError
		if errors.As(err, &pve) {
			finalize(pve.Code, pve.Msg, 0)
		} else {
			finalize("io_error", err.Error(), 0)
		}
		return
	}
	n, werr := f.Write(req.InlineData)
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(tmp)
		finalize("io_error", firstNonNilErr(werr, cerr).Error(), int64(n))
		return
	}
	if err := RenameForWriteAtomic(vp, tmp, req.Force); err != nil {
		_ = os.Remove(tmp)
		var pve *PathValidationError
		if errors.As(err, &pve) {
			finalize(pve.Code, pve.Msg, int64(n))
		} else {
			finalize("io_error", err.Error(), int64(n))
		}
		return
	}
	finalize("", "", int64(n))
}

// handlePushCommitForwarded — tier B push receiver-side. ctl has
// finished ObjectStore.Put; we Get the object, SHA-verify, rename
// into place, and emit ev.transfer.<id>.complete|failed.
func (a *Agent) handlePushCommitForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: push-commit.req.forwarded without Reply inbox")
		return
	}
	var req proto.TransferCommitReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	if a.js == nil {
		a.replyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "jetstream_unavailable"})
		return
	}
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	store, err := a.js.ObjectStore(commitCtx, req.Bucket)
	if err != nil {
		cancel()
		a.replyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "bucket_unknown", Error: err.Error()})
		return
	}

	// Look up the expected SHA + path saved during push.req prepare.
	// We piggyback on the broker's bucket-object Metadata, set by the
	// agent itself on push.req? No — we never wrote metadata. The
	// SHA + path were embedded in the original PushPrepareReq, which
	// we replay through a small in-memory cache (a.pushCommitCache).
	// Same with allow_roots validation result. If not in cache (a
	// crashed-mid-prep agent could have lost it) → bucket_unknown.
	a.pushCacheMu.Lock()
	cached, hit := a.pushCommitCache[req.TransferID]
	a.pushCacheMu.Unlock()
	if !hit {
		cancel()
		a.replyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "transfer_unknown",
			Error: "no in-process prep entry for this transfer"})
		return
	}

	a.replyCommit(nc, msg.Reply, proto.TransferCommitResp{OK: true})
	// Run the actual data-plane work asynchronously: emitting
	// ev.transfer is the receiver-finalization signal, not the
	// reply.
	go func() {
		defer cancel()
		startedAt := time.Now().UTC()
		bytes, sha, err := a.objectStoreGetAndWrite(commitCtx, store, "object", cached.vp, cached.force)
		if err != nil {
			var pve *PathValidationError
			code := "object_get_failed"
			msg := err.Error()
			if errors.As(err, &pve) {
				code = pve.Code
				msg = pve.Msg
			}
			a.pubTransferEvFailed(nc, "push", &proto.PushPrepareReq{
				TransferID: req.TransferID, Path: cached.path,
				SHA256: cached.sha256, Size: cached.size, Tier: "b",
				Bucket: req.Bucket,
			}, "b", code, msg, time.Since(startedAt))
			a.purgePushCommitCache(req.TransferID)
			return
		}
		if sha != cached.sha256 {
			a.pubTransferEvFailed(nc, "push", &proto.PushPrepareReq{
				TransferID: req.TransferID, Path: cached.path,
				SHA256: cached.sha256, Size: cached.size, Tier: "b",
				Bucket: req.Bucket,
			}, "b", "sha_mismatch",
				fmt.Sprintf("get sha256: want=%s got=%s", cached.sha256, sha),
				time.Since(startedAt))
			a.purgePushCommitCache(req.TransferID)
			return
		}
		a.pubTransferEv(nc, "complete", "push", &proto.PushPrepareReq{
			TransferID: req.TransferID, Path: cached.path,
			SHA256: cached.sha256, Size: cached.size, Tier: "b",
			Bucket: req.Bucket,
		}, "b", bytes, time.Since(startedAt))
		a.purgePushCommitCache(req.TransferID)
	}()
}

// objectStoreGetAndWrite streams the object into a tmp + computes
// SHA + renames atomically. Returns (bytes_written, hex_sha256, err).
func (a *Agent) objectStoreGetAndWrite(
	ctx context.Context,
	store jetstream.ObjectStore,
	objectName string,
	vp *ValidatedPath,
	force bool,
) (int64, string, error) {
	result, err := store.Get(ctx, objectName)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = result.Close() }()

	rnd := randSuffix()
	f, tmp, err := OpenForWriteAtomic(vp, rnd)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	tee := io.TeeReader(result, h)
	n, copyErr := io.Copy(f, tee)
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmp)
		return n, "", firstNonNilErr(copyErr, closeErr)
	}
	if err := RenameForWriteAtomic(vp, tmp, force); err != nil {
		_ = os.Remove(tmp)
		return n, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// handlePullForwarded — pull receiver is ctl; agent is the sender.
// On tier-A path agent reads the bytes + replies inline. On tier-B
// agent ObjectStore.Puts into the broker-supplied bucket and replies
// with the bucket coordinates.
func (a *Agent) handlePullForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: pull.req.forwarded without Reply inbox")
		return
	}
	var combined struct {
		proto.PullPrepareReq
		Bucket    string `json:"bucket,omitempty"`
		ObjectKey string `json:"object_key,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &combined); err != nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	req := combined.PullPrepareReq
	bucket := combined.Bucket
	objectKey := combined.ObjectKey
	if objectKey == "" {
		objectKey = "object"
	}

	vp, err := ValidateForRead(req.Path, a.cfg.AllowRoots)
	if err != nil {
		var pve *PathValidationError
		if errors.As(err, &pve) {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: pve.Code, Error: pve.Msg})
		} else {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
		}
		return
	}
	f, err := OpenForReadAtomic(vp)
	if err != nil {
		var pve *PathValidationError
		if errors.As(err, &pve) {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: pve.Code, Error: pve.Msg})
		} else {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
		}
		return
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "io_error", Error: err.Error()})
		return
	}
	size := st.Size()
	if size > 200*1024*1024 {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			Error: fmt.Sprintf("file size=%d > 200 MiB", size)})
		return
	}
	maxInline := req.MaxInline
	if maxInline <= 0 || maxInline > agentTierAMaxBytes {
		maxInline = agentTierAMaxBytes
	}

	if size <= maxInline {
		// Tier A — slurp + sha + reply.
		data, err := io.ReadAll(f)
		if err != nil {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
			return
		}
		sha := hexSHA256(data)
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: true, Tier: "a", Size: int64(len(data)),
			SHA256: sha, InlineData: data,
		})
		return
	}

	// Tier B — agent Puts into the broker-pre-created bucket.
	if a.js == nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "jetstream_unavailable"})
		return
	}
	if bucket == "" {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "bucket_unknown",
			Error: "broker did not supply a bucket name"})
		return
	}
	putCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store, err := a.js.ObjectStore(putCtx, bucket)
	if err != nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "bucket_unknown", Error: err.Error()})
		return
	}
	h := sha256.New()
	tee := io.TeeReader(f, h)
	if _, err := store.Put(putCtx, jetstream.ObjectMeta{Name: objectKey}, tee); err != nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "object_put_failed", Error: err.Error()})
		return
	}
	sha := hex.EncodeToString(h.Sum(nil))
	a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
		OK: true, Tier: "b", Size: size, SHA256: sha,
		Bucket: bucket, ObjectKey: objectKey,
	})
}

// pubTransferEv publishes ev.transfer.<id>.complete. Tier-aware.
func (a *Agent) pubTransferEv(nc *nats.Conn, kind, verb string, req *proto.PushPrepareReq,
	tier string, bytes int64, dur time.Duration) {
	payload, _ := json.Marshal(proto.TransferEvent{
		Kind: kind, Verb: verb, TransferID: req.TransferID,
		Tier: tier, Bucket: req.Bucket, Path: req.Path,
		Bytes: bytes, DurationMs: dur.Milliseconds(),
	})
	subj := proto.SubjEvTransfer(a.cfg.SID, a.cfg.NID, req.TransferID, kind)
	if err := nc.Publish(subj, payload); err != nil {
		a.cfg.Logger.Warn("agent: ev.transfer pub", "err", err, "subj", subj)
	}
}

// pubTransferEvFailed mirrors pubTransferEv with kind=failed.
func (a *Agent) pubTransferEvFailed(nc *nats.Conn, verb string, req *proto.PushPrepareReq,
	tier, code, errMsg string, dur time.Duration) {
	payload, _ := json.Marshal(proto.TransferEvent{
		Kind: "failed", Verb: verb, TransferID: req.TransferID,
		Tier: tier, Bucket: req.Bucket, Path: req.Path,
		Code: code, Error: errMsg, DurationMs: dur.Milliseconds(),
	})
	subj := proto.SubjEvTransfer(a.cfg.SID, a.cfg.NID, req.TransferID, "failed")
	if err := nc.Publish(subj, payload); err != nil {
		a.cfg.Logger.Warn("agent: ev.transfer failed pub", "err", err, "subj", subj)
	}
}

func (a *Agent) replyPush(nc *nats.Conn, replyTo string, resp proto.PushPrepareResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}
func (a *Agent) replyCommit(nc *nats.Conn, replyTo string, resp proto.TransferCommitResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}
func (a *Agent) replyPull(nc *nats.Conn, replyTo string, resp proto.PullPrepareResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}

// pushCommitEntry caches the per-transfer prep state so push-commit
// (which arrives on a separate subject) knows what bucket to Get from,
// what path to write to, and what SHA to verify.
type pushCommitEntry struct {
	vp     *ValidatedPath
	path   string
	sha256 string
	size   int64
	force  bool
}

// rememberPushCommit stores a prep entry. Must be called from
// handlePushForwarded BEFORE replying OK so a fast push-commit can
// race in without losing the entry.
func (a *Agent) rememberPushCommit(transferID string, e pushCommitEntry) {
	a.pushCacheMu.Lock()
	defer a.pushCacheMu.Unlock()
	if a.pushCommitCache == nil {
		a.pushCommitCache = map[string]pushCommitEntry{}
	}
	a.pushCommitCache[transferID] = e
}

func (a *Agent) purgePushCommitCache(transferID string) {
	a.pushCacheMu.Lock()
	defer a.pushCacheMu.Unlock()
	delete(a.pushCommitCache, transferID)
}

func hexSHA256(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func randSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func firstNonNilErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
