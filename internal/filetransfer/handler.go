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

package filetransfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Handler receives only transfer dependencies; the agent retains admission and cache ownership.
// Its worker is a lifetime owner; rebuilding a NATS session only replaces the control attachment.
// origin: hot-update S7 plan §8.1 / P20
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type Handler struct {
	Worker      xferworker.Manager
	WorkerError error
	Instance    func() string
	// ports binds the worker's actors to this host; the data attachment is a
	// clone of the control connection's identity (workerPorts.dialData).
	// origin: hot-update S7 plan §2.3 / INTEG-D agent wiring
	ports      *workerPorts
	stopMu     sync.Mutex
	closed     atomic.Bool
	stopped    bool
	Logger     *slog.Logger
	Roots      []string
	Mode       int
	JetStream  func() jetstream.JetStream
	SID        string
	NID        func() string
	Admit      func() (func(), bool)
	RunContext func() context.Context
	Remember   func(string, PushCommitEntry) bool
	Lookup     func(string) (PushCommitEntry, bool)
	Purge      func(string)
	Operations Operations
	// Runtime is the production continuation composition (nil for an
	// explicitly injected or disabled worker); it answers broker link controls.
	// origin: hot-update S7 plan §6.3 / P20
	Runtime *xferagent.Runtime
	// AdmitCreation gates first continuation creation on the image cut.
	AdmitCreation func() (func(), bool)
}

// Operations keeps the host's fixed transfer forwarding surface live after
// responsibility extraction. Hosts may supply adapters; standalone handlers
// use the same concrete byte/event/reply implementations directly. Adapters
// invoke concrete methods; each method dispatches only downstream operations,
// never its own adapter, so the forwarding graph remains acyclic.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type Operations struct {
	HandlePushTierA        func(*nats.Conn, *nats.Msg, *proto.PushPrepareReq, *transferpath.ValidatedPath)
	ObjectStoreGetAndWrite func(context.Context, jetstream.JetStream, jetstream.ObjectStore, string, string, *transferpath.ValidatedPath, int64, string, bool) (int64, error)
	PublishEvent           func(*nats.Conn, string, string, *proto.PushPrepareReq, string, int64, time.Duration)
	PublishFailedEvent     func(*nats.Conn, string, *proto.PushPrepareReq, string, string, string, time.Duration)
	ReplyPush              func(*nats.Conn, string, proto.PushPrepareResp)
	ReplyCommit            func(*nats.Conn, string, proto.TransferCommitResp)
	ReplyPull              func(*nats.Conn, string, proto.PullPrepareResp)
}

func handlerOperations(a *Handler) Operations {
	ops := a.Operations
	if ops.HandlePushTierA == nil {
		ops.HandlePushTierA = a.HandlePushTierA
	}
	if ops.ObjectStoreGetAndWrite == nil {
		ops.ObjectStoreGetAndWrite = a.ObjectStoreGetAndWrite
	}
	if ops.PublishEvent == nil {
		ops.PublishEvent = a.PublishEvent
	}
	if ops.PublishFailedEvent == nil {
		ops.PublishFailedEvent = a.PublishFailedEvent
	}
	if ops.ReplyPush == nil {
		ops.ReplyPush = a.ReplyPush
	}
	if ops.ReplyCommit == nil {
		ops.ReplyCommit = a.ReplyCommit
	}
	if ops.ReplyPull == nil {
		ops.ReplyPull = a.ReplyPull
	}
	return ops
}

// tier-A inline ceiling. batch C: the value now comes from internal/proto, which the broker and the
// ctl also read — three hand-kept copies of a bound the BROKER refuses at is three chances to drift
// on one edit, and the ctl offering what the broker will refuse is a user-visible failure.
const agentTierAMaxBytes = proto.XferTierAMaxBytes

// Hard per-transfer ceiling. Every streaming path enforces this while
// reading, not only from a pre-read stat, because regular files can grow.
const agentTransferMaxBytes = int64(proto.XferMaxBytes)

func PullBucketPayloadCeiling(advertised *int64) int64 {
	if advertised == nil || *advertised > agentTransferMaxBytes {
		return agentTransferMaxBytes
	}
	if *advertised < 0 {
		return 0
	}
	return *advertised
}

// pushCommitCacheSlack is the headroom a prep entry keeps beyond the time the CTL needs to upload
// that file. It preserves the shape of the pre-batch-C constant: the old TTL was 6m against a 5m
// tier-B watchdog, and that ONE MINUTE OF MARGIN was the entire meaning of the 6 — written nowhere.
const pushCommitCacheSlack = time.Minute

// PushCommitEntryTTL is how long ONE prep entry stays valid, derived from ITS OWN size.
//
// It used to be a single 6-minute constant, which worked only because it happened to sit just above
// the fixed 5-minute tier-B watchdog. Once the watchdog became size-derived, a 2 GiB push takes the
// ctl ~17 minutes to upload and the entry would be swept long before push-commit arrived — the agent
// would answer `transfer_unknown` and the whole transfer would fail, on exactly the large slow files
// this batch exists to make work.
//
// PER-ENTRY, not a raised global TTL, and that distinction is load-bearing. Raising the TTL globally
// would make every small entry linger ~3x longer, push the 1024-entry cache to capacity sooner, and
// the capacity path evicts the OLDEST entry — which is precisely the multi-minute large transfer we
// just extended the TTL to protect. Keying expiry on each entry's own size keeps small entries
// short-lived, so capacity pressure does not rise at all.
func PushCommitEntryTTL(size int64) time.Duration {
	// XferBudget, not XferLegBudget: the former carries the TIER FLOOR, the latter is raw
	// size/throughput. Deriving from the raw leg made the comment above a lie in the direction that
	// matters — an 8 MiB tier-B push got 5s+60s = 65 SECONDS where it used to have 6 minutes, and a
	// 100 MiB one got 110s. Every tier-B transfer under ~480 MiB came out with a prep entry SHORTER
	// than the broker's own watchdog for the same transfer, so a second concurrent prepare (the only
	// thing that sweeps this map) could evict it mid-upload and the push-commit would land on
	// transfer_unknown. A batch whose purpose is to make slow large files work must not shrink the
	// tolerance of medium ones to a fifth. With the floor, size==0 gives 5m+1m — byte-for-byte the
	// pre-batch-C constant — and 2 GiB gives 17m+1m.
	return proto.XferBudget("b", size, 1) + pushCommitCacheSlack
}

func PushCommitEntryExpired(e PushCommitEntry, now time.Time) bool {
	return now.Sub(e.Added) >= PushCommitEntryTTL(e.Size)
}

// HandlePushForwarded — push.req.forwarded entrypoint.
func (a *Handler) HandlePushForwarded(nc *nats.Conn, msg *nats.Msg) {
	if handleContinuation(a, nc, msg, "push") {
		return
	}
	if msg.Reply == "" {
		a.Logger.Warn("agent: push.req.forwarded without Reply inbox")
		return
	}
	// origin: hot-update S3 plan §4.0. Hold admission for the whole handler.
	release, admitted := a.Admit()
	if !admitted {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{Code: "try_again", Error: "agent_upgrading: image switch in progress; retry"})
		return
	}
	defer release()

	var req proto.PushPrepareReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	if req.Tier != "a" && req.Tier != "b" {
		handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: "tier_invalid", Error: req.Tier})
		return
	}
	vp, err := transferpath.ValidateForWrite(req.Path, a.Roots, a.Mode)
	if err != nil {
		code, errMsg := transferFailure(err, "io_error")
		handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: code, Error: errMsg})
		// Emit ev.transfer.failed so the broker can write audit
		// failed + reap its in-memory entry immediately, instead of
		// waiting on the per-tier watchdog timeout.
		handlerOperations(a).PublishFailedEvent(nc, "push", &req, req.Tier, code, errMsg, 0)
		return
	}

	switch req.Tier {
	case "a":
		handlerOperations(a).HandlePushTierA(nc, msg, &req, vp)
	case "b":
		// Cache prep state so push-commit knows what bucket + path
		// + sha to use. Bucket name was stamped by the broker.
		if !a.Remember(req.TransferID, PushCommitEntry{
			Validated: vp,
			Path:      req.Path,
			SHA256:    req.SHA256,
			Size:      req.Size,
			Force:     req.Force,
		}) {
			// batch C: the prep cache is full of LIVE entries. Refuse instead of evicting one — see
			// rememberPushCommit. Retriable: the entries drain as their transfers finish.
			handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{
				OK: false, Code: "too_many_in_flight",
				Error: "this agent already holds the maximum number of in-flight tier-B prepares; retry shortly"})
			handlerOperations(a).PublishFailedEvent(nc, "push", &req, req.Tier, "too_many_in_flight", "agent prep cache full", 0)
			return
		}
		handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{OK: true, Tier: "b"})
	}
}

// HandlePushTierA writes the inline bytes through the
// path.go tmp+rename pipeline, sha-verifies, and emits ev.transfer.
// The ctl-bound reply happens AFTER the write so ctl's RTT includes
// the actual outcome (file in place or rejected); ev.transfer is
// emitted unconditionally so the broker's audit always flows from the
// receiver-finalization invariant for symmetry with tier B.
func (a *Handler) HandlePushTierA(nc *nats.Conn, msg *nats.Msg, req *proto.PushPrepareReq, vp *transferpath.ValidatedPath) {
	startedAt := time.Now().UTC()
	finalize := func(code, errMsg string, bytes int64) {
		if code == "" {
			handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{OK: true, Tier: "a"})
			handlerOperations(a).PublishEvent(nc, "complete", "push", req, "a", bytes, time.Since(startedAt))
		} else {
			handlerOperations(a).ReplyPush(nc, msg.Reply, proto.PushPrepareResp{OK: false, Code: code, Error: errMsg})
			handlerOperations(a).PublishFailedEvent(nc, "push", req, "a", code, errMsg, time.Since(startedAt))
		}
	}

	// SHA verify the inline blob BEFORE touching disk.
	if got := hexSHA256(req.InlineData); got != req.SHA256 {
		finalize("sha_mismatch",
			fmt.Sprintf("inline_data sha256: want=%s got=%s", req.SHA256, got), 0)
		return
	}
	rnd := randSuffix()
	f, pending, err := transferpath.OpenForWriteAtomic(vp, rnd)
	if err != nil {
		code, detail := transferFailure(err, "io_error")
		finalize(code, detail, 0)
		return
	}
	defer pending.Abort()
	n, werr := f.Write(req.InlineData)
	if werr == nil {
		werr = f.Sync()
	}
	cerr := f.Close()
	if werr != nil || cerr != nil {
		finalize("io_error", firstNonNilErr(werr, cerr).Error(), int64(n))
		return
	}
	if err := transferpath.RenameForWriteAtomic(vp, pending, req.Force); err != nil {
		code, detail := transferFailure(err, "io_error")
		finalize(code, detail, int64(n))
		return
	}
	finalize("", "", int64(n))
}

// HandlePushCommitForwarded — tier B push receiver-side. ctl has
// finished ObjectStore.Put; we Get the object, SHA-verify, rename
// into place, and emit ev.transfer.<id>.complete|failed.
func (a *Handler) HandlePushCommitForwarded(nc *nats.Conn, msg *nats.Msg) {
	// Broker link controls share this authenticated intake (proto.TransferLinkVerb)
	// and are answered before any legacy cache lookup. Without a production
	// runtime a link is still answered: as an explicit cap0 probe or a refusal.
	// origin: hot-update S7 plan §6.3 / P20
	if a.Runtime != nil && a.Runtime.HandleLink(xferagent.LinkPeer{MaxPayload: nc.MaxPayload(), Publish: nc.Publish, Nkey: nc.Opts.Nkey, Sign: nc.Opts.SignatureCB}, msg) || a.Runtime == nil && refuseLink(nc, msg, a.Instance) {
		return
	}
	if handleContinuation(a, nc, msg, "push-commit") {
		return
	}
	if msg.Reply == "" {
		a.Logger.Warn("agent: push-commit.req.forwarded without Reply inbox")
		return
	}
	// origin: hot-update S3 plan §4.0. Hold admission for the whole handler.
	release, admitted := a.Admit()
	if !admitted {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{Code: "try_again", Error: "agent_upgrading: image switch in progress; retry"})
		return
	}
	defer release()

	var req proto.TransferCommitReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	// Look up the expected SHA + path + allow_roots validation saved
	// during push.req prepare (a.pushCommitCache — no object metadata
	// is written, the original PushPrepareReq is the only carrier).
	// Cache miss (agent restarted between prep and commit) →
	// transfer_unknown; the broker's watchdog writes the failed audit.
	//
	// batch C moved this lookup AHEAD of the context so the download deadline can be derived from the
	// transfer's own size. It used to be a hardcoded 5 minutes, which silently capped every tier-B
	// download at the value the broker's fixed watchdog happened to use — so raising only the broker's
	// budget would have made things WORSE, not better: the broker would patiently hold a bucket and a
	// goroutine for half an hour waiting on a download this side had already abandoned at 5 minutes.
	cached, hit := a.Lookup(req.TransferID)
	if !hit {
		handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "transfer_unknown",
			Error: "no in-process prep entry for this transfer"})
		return
	}

	// ONE leg: this end only performs the Get. The broker covers both crossings.
	parent := a.RunContext()
	if parent == nil {
		parent = context.Background() // ctx-none: NATS handler in an unstarted test agent
	}
	commitCtx, cancel := context.WithTimeout(parent, proto.XferBudget("b", cached.Size, 1))
	// Bind this transfer to the connection which received the commit. A later
	// session rebuild must not substitute a different connection mid-read.
	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{OK: false, Code: "jetstream_unavailable", Error: err.Error()})
		return
	}
	store, err := js.ObjectStore(commitCtx, req.Bucket)
	if err != nil {
		cancel()
		handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{
			OK: false, Code: "bucket_unknown", Error: err.Error()})
		return
	}

	handlerOperations(a).ReplyCommit(nc, msg.Reply, proto.TransferCommitResp{OK: true})
	// Run the actual data-plane work asynchronously: emitting
	// ev.transfer is the receiver-finalization signal, not the
	// reply.
	go func() {
		defer cancel()
		startedAt := time.Now().UTC()
		bytes, err := handlerOperations(a).ObjectStoreGetAndWrite(
			commitCtx, js, store, req.Bucket, req.TransferID, cached.Validated,
			cached.Size, cached.SHA256, cached.Force,
		)
		if err != nil {
			code, msg := transferFailure(err, "object_get_failed")
			handlerOperations(a).PublishFailedEvent(nc, "push", &proto.PushPrepareReq{
				TransferID: req.TransferID, Path: cached.Path,
				SHA256: cached.SHA256, Size: cached.Size, Tier: "b",
				Bucket: req.Bucket,
			}, "b", code, msg, time.Since(startedAt))
			a.Purge(req.TransferID)
			return
		}
		handlerOperations(a).PublishEvent(nc, "complete", "push", &proto.PushPrepareReq{
			TransferID: req.TransferID, Path: cached.Path,
			SHA256: cached.SHA256, Size: cached.Size, Tier: "b",
			Bucket: req.Bucket,
		}, "b", bytes, time.Since(startedAt))
		a.Purge(req.TransferID)
	}()
}

// ObjectStoreGetAndWrite streams the object into a pinned-parent tmp,
// enforces the prepare-time size, verifies SHA, then commits. Verification
// must happen before rename: a failed object never becomes the destination.
func (a *Handler) ObjectStoreGetAndWrite(
	ctx context.Context,
	js jetstream.JetStream,
	store jetstream.ObjectStore,
	bucket, objectName string,
	vp *transferpath.ValidatedPath,
	expectedSize int64,
	expectedSHA string,
	force bool,
) (int64, error) {
	if expectedSize < 0 || expectedSize > agentTransferMaxBytes {
		return 0, pathErr("too_large", "invalid expected size %d", expectedSize)
	}
	result, err := jsstream.GetTransferObject(ctx, js, store, bucket, objectName)
	if err != nil {
		return 0, err
	}
	defer func() { _ = result.Close() }()

	rnd := randSuffix()
	f, pending, err := transferpath.OpenForWriteAtomic(vp, rnd)
	if err != nil {
		return 0, err
	}
	defer pending.Abort()
	h := sha256.New()
	limited := &io.LimitedReader{R: result, N: expectedSize + 1}
	tee := io.TeeReader(limited, h)
	n, copyErr := io.Copy(f, tee)
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		return n, firstNonNilErr(copyErr, closeErr)
	}
	if n != expectedSize {
		return n, pathErr("size_mismatch",
			"object size=%d differs from prepare size=%d", n, expectedSize)
	}
	gotSHA := hex.EncodeToString(h.Sum(nil))
	if gotSHA != expectedSHA {
		return n, pathErr("sha_mismatch",
			"get sha256: want=%s got=%s", expectedSHA, gotSHA)
	}
	if err := ctx.Err(); err != nil {
		return n, err
	}
	if err := transferpath.RenameForWriteAtomic(vp, pending, force); err != nil {
		return n, err
	}
	return n, nil
}

// HandlePullForwarded — pull receiver is ctl; agent is the sender.
// On tier-A path agent reads the bytes + replies inline. On tier-B
// agent ObjectStore.Puts into the broker-supplied bucket and replies
// with the bucket coordinates.
func (a *Handler) HandlePullForwarded(nc *nats.Conn, msg *nats.Msg) {
	if handleContinuation(a, nc, msg, "pull") {
		return
	}
	if msg.Reply == "" {
		a.Logger.Warn("agent: pull.req.forwarded without Reply inbox")
		return
	}
	// origin: hot-update S3 plan §4.0. Hold admission for the whole handler.
	release, admitted := a.Admit()
	if !admitted {
		// origin: hot-update S4 plan §9.2 / N1-REFUSAL. v0.6.0 knows try_again as 75; keep the upgrade reason in detail.
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{Code: "try_again", Error: "agent_upgrading: image switch in progress; retry"})
		return
	}
	defer release()

	var combined struct {
		proto.PullPrepareReq
		Bucket                string `json:"bucket,omitempty"`
		ObjectKey             string `json:"object_key,omitempty"`
		BucketPayloadMaxBytes *int64 `json:"bucket_payload_max_bytes,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &combined); err != nil {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "json_parse", Error: err.Error()})
		return
	}
	req := combined.PullPrepareReq
	bucket := combined.Bucket
	objectKey := combined.ObjectKey
	if objectKey == "" {
		// Older brokers (pre-v0.2.2) used a per-bucket "object" key;
		// newer brokers send the transfer_id. Default to transfer_id
		// when broker didn't supply — matches the v0.2.2 design.
		objectKey = req.TransferID
	}

	vp, err := transferpath.ValidateForRead(req.Path, a.Roots, a.Mode)
	if err != nil {
		code, detail := transferFailure(err, "io_error")
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{OK: false, Code: code, Error: detail})
		return
	}
	f, err := transferpath.OpenForReadAtomic(vp)
	if err != nil {
		code, detail := transferFailure(err, "io_error")
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{OK: false, Code: code, Error: detail})
		return
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "io_error", Error: err.Error()})
		return
	}
	size := st.Size()
	if size > agentTransferMaxBytes {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			// origin: batch-c internal review F4 — DERIVED, not hand-copied. See proto.HumanBytes.
			Error: fmt.Sprintf("file size=%d > %s", size, proto.HumanBytes(agentTransferMaxBytes))})
		return
	}
	maxInline := req.MaxInline
	if maxInline <= 0 || maxInline > agentTierAMaxBytes {
		maxInline = agentTierAMaxBytes
	}

	if size <= maxInline {
		// Tier A: cap the read itself. A file can grow after Stat; if it
		// crosses the inline ceiling, rewind and continue through tier B.
		data, err := io.ReadAll(io.LimitReader(f, maxInline+1))
		if err != nil {
			handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
			return
		}
		if int64(len(data)) <= maxInline {
			sha := hexSHA256(data)
			handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: true, Tier: "a", Size: int64(len(data)),
				SHA256: sha, InlineData: data,
			})
			return
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
			return
		}
	}

	// Tier B — agent Puts into the broker-pre-created bucket.
	if a.JetStream() == nil {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "jetstream_unavailable"})
		return
	}
	if bucket == "" {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "bucket_unknown",
			Error: "broker did not supply a bucket name"})
		return
	}
	payloadMax := PullBucketPayloadCeiling(combined.BucketPayloadMaxBytes)
	if size > payloadMax {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			Error: fmt.Sprintf("file size=%d > broker bucket payload ceiling %d", size, payloadMax)})
		return
	}
	// batch C: the PULL leg deliberately does NOT use the file's real size, and that asymmetry with
	// the push leg above is the point.
	//
	// proto.PullPrepareReq carries no size, so the BROKER's pull watchdog has none either and budgets
	// the flat tier floor. Deriving this end from the real size would raise only one side of the same
	// transfer: a 2 GiB pull would give the agent ~17 minutes while the broker gave up at 5, wrote a
	// failed terminal, deleted the tracker entry and released the bucket from activeOBJStreams — after
	// which the home reaper's 2-minute grace can delete the object out from under a ctl that is still
	// downloading it. That is the "raise only one end and it gets WORSE" argument this batch is built
	// on, pointing the other way. Getting a real budget onto the pull path needs the size on the wire,
	// which is the deferred medium half (permanent decision N5).
	//
	// XferBudget with size 0 is the tier floor, written this way rather than as the constant so it
	// tracks whatever the broker's pull watchdog computes.
	//
	// This is the agent's cap0 upload. handleContinuation above already took
	// every continuation-marked request, so only a pre-S7 broker's plain pull
	// reaches here, and the bytes go through the single one-shot SDK adapter:
	// no retry, rewind or fixed-version writer state is shared with the capable
	// path (xferagent's jsstream writer).
	// origin: hot-update S7 plan §7 / T01 / T49 — capable path never reaches SDK Put
	deadline, err := controlbudget.After(proto.XferBudget("b", 0, 1))
	if err != nil {
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "io_error", Error: err.Error()})
		return
	}
	h := sha256.New()
	limited := &io.LimitedReader{R: f, N: payloadMax + 1}
	tee := io.TeeReader(limited, h)
	// The cap0 upload goes through the one-shot SDK adapter, the only Put site
	// in the transfer graph; it shares this callback's single transfer budget.
	// origin: hot-update S7 plan §7 / §9 / L1 / T49 (SDK Put only in PutLegacyOnce)
	info, err := legacyOnce(a.JetStream(), bucket, objectKey, tee, deadline)
	if err != nil {
		code := "object_put_failed"
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			code = "bucket_unknown"
		}
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: code, Error: err.Error()})
		return
	}
	uploaded := int64(info.Size)
	if uploaded > payloadMax {
		// ctx-none: NATS callback has no caller context; a short bound covers the purge.
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if store, err := a.JetStream().ObjectStore(delCtx, bucket); err == nil {
			_ = store.Delete(delCtx, objectKey)
		}
		handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			Error: fmt.Sprintf("file grew beyond broker bucket payload ceiling %d while reading (read=%d)",
				payloadMax, uploaded)})
		return
	}
	sha := hex.EncodeToString(h.Sum(nil))
	handlerOperations(a).ReplyPull(nc, msg.Reply, proto.PullPrepareResp{
		OK: true, Tier: "b", Size: uploaded, SHA256: sha,
		Bucket: bucket, ObjectKey: objectKey,
	})
}

// PublishEvent publishes ev.transfer.<id>.complete. Tier-aware.
func (a *Handler) PublishEvent(nc *nats.Conn, kind, verb string, req *proto.PushPrepareReq,
	tier string, bytes int64, dur time.Duration) {
	payload, _ := json.Marshal(proto.TransferEvent{
		Kind: kind, Verb: verb, TransferID: req.TransferID,
		Tier: tier, Bucket: req.Bucket, Path: req.Path,
		Bytes: bytes, DurationMs: dur.Milliseconds(),
	})
	subj := proto.SubjEvTransfer(a.SID, a.NID(), req.TransferID, kind)
	if err := nc.Publish(subj, payload); err != nil {
		a.Logger.Warn("agent: ev.transfer pub", "err", err, "subj", subj)
	}
}

// PublishFailedEvent mirrors PublishEvent with kind=failed. Tolerates
// being called with an empty TransferID (e.g. agent rejected the
// request before parsing); silently skip in that case so we don't
// publish an unnamed ev.transfer.
func (a *Handler) PublishFailedEvent(nc *nats.Conn, verb string, req *proto.PushPrepareReq,
	tier, code, errMsg string, dur time.Duration) {
	if req.TransferID == "" {
		return
	}
	payload, _ := json.Marshal(proto.TransferEvent{
		Kind: "failed", Verb: verb, TransferID: req.TransferID,
		Tier: tier, Bucket: req.Bucket, Path: req.Path,
		Code: code, Error: errMsg, DurationMs: dur.Milliseconds(),
	})
	subj := proto.SubjEvTransfer(a.SID, a.NID(), req.TransferID, "failed")
	if err := nc.Publish(subj, payload); err != nil {
		a.Logger.Warn("agent: ev.transfer failed pub", "err", err, "subj", subj)
	}
}

func (a *Handler) ReplyPush(nc *nats.Conn, replyTo string, resp proto.PushPrepareResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}
func (a *Handler) ReplyCommit(nc *nats.Conn, replyTo string, resp proto.TransferCommitResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}
func (a *Handler) ReplyPull(nc *nats.Conn, replyTo string, resp proto.PullPrepareResp) {
	payload, _ := json.Marshal(resp)
	_ = nc.Publish(replyTo, payload)
}

// PushCommitEntry caches the per-transfer prep state so push-commit
// (which arrives on a separate subject) knows what bucket to Get from,
// what path to write to, and what SHA to verify.
type PushCommitEntry struct {
	Validated *transferpath.ValidatedPath
	Path      string
	SHA256    string
	Size      int64
	Force     bool
	Added     time.Time
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

func pathErr(code, format string, a ...any) *transferpath.PathValidationError {
	return &transferpath.PathValidationError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

// LegacyPrepareCapacity preserves the original single-flight cache ceiling.
// origin: hot-update S7 plan §1.2 / P20
const LegacyPrepareCapacity = 1024

func LogTransferPolicy(log *slog.Logger, mode int) {
	if mode == 0 {
		// Posture-change signal: with no allow_roots configured, push/pull
		// now reaches the whole filesystem (matching run/exec). Logged once
		// at startup so an operator upgrading from the old empty==disabled
		// default sees the change at the moment it takes effect.
		log.Warn("file transfer: whole-filesystem reach (no file_transfer.allow_roots configured); " +
			"set file_transfer.allow_roots to narrow, or allow_roots: [] to disable push/pull")
	}
}

// RememberPrepared retains the legacy size-derived cache policy under its
// caller's lock. Adapting the private cache value preserves existing tests and
// avoids a second map of the same preparation responsibility.
// origin: hot-update S7 plan §11.1 / P20
func RememberPrepared[T any](mu *sync.Mutex, cache *map[string]T, id string, entry T, now time.Time, expired func(T, time.Time) bool) bool {
	mu.Lock()
	defer mu.Unlock()
	if *cache == nil {
		*cache = make(map[string]T)
	}
	for key, cached := range *cache {
		if expired(cached, now) {
			delete(*cache, key)
		}
	}
	if len(*cache) >= LegacyPrepareCapacity {
		// Every remaining entry is live. Refusing is honest and retriable; evicting one is a silent
		// failure whose victim is chosen to be the most expensive transfer in flight.
		return false
	}
	(*cache)[id] = entry
	return true
}

// The P01 directed carrier replaced the local P00 continuation mirror: the
// broker-stamped actor, admitted budget, sealed source and durable Grant are
// decoded by proto.DecodeAgentTransferCarrier. A nonzero continuation can
// never fall through to legacy Put or rename.
// origin: hot-update S7 plan §6.1 / §6.2 / §7 / P20 / INTEG-D agent wiring
func handleContinuation(h *Handler, nc *nats.Conn, msg *nats.Msg, verb string) bool {
	var marker struct {
		Continuation json.RawMessage `json:"continuation"`
	}
	if json.Unmarshal(msg.Data, &marker) != nil || len(marker.Continuation) == 0 || bytes.Equal(marker.Continuation, []byte("null")) {
		return false
	}
	if msg.Reply == "" {
		return true
	}
	nid := ""
	if h.NID != nil {
		nid = h.NID()
	}
	r, budget, source, grant, err := proto.DecodeAgentTransferCarrier(h.SID, nid, verb, msg.Data)
	code := xfercontract.CodeIntentConflict
	// Broker-stamped actor identity and the original permit must agree with this local owner.
	// origin: hot-update S7 plan §6.1 / P20
	if err == nil && (h.Instance == nil || r.Identity.AgentInstance != h.Instance()) {
		err = errors.New("transfer continuation names another agent instance")
	}
	// A missing or failed factory cannot authorize a fall back to legacy byte work.
	// origin: hot-update S7 plan §7 / P20
	if err == nil && (WorkerVersion(h) == "" || h.ports == nil) {
		err = errors.New("transfer worker unavailable; continuation retained at original owner")
		code = xfercontract.CodeOwnerRecovering
	}
	var status xfercontract.Status
	if err == nil {
		code = xfercontract.CodeResultUnknown
		status, err = applyContinuation(h, r, budget, source, grant)
	}
	response := xfercontract.Response{Status: &status}
	if err != nil {
		response = xfercontract.Response{Code: workerCode(err, code), Error: "transfer continuation refused; reconcile the original owner"}
		h.Logger.Debug("agent: transfer continuation refused", "verb", verb, "code", response.Code, "err", err)
	}
	request := r
	if request.Op == "" {
		// An undecodable carrier still gets a refusal of the matching DTO shape.
		request.Op, request.Intent = xfercontract.OpCommit, nil
		if verb != "push-commit" {
			request.Op, request.Intent = xfercontract.OpPrepare, &xfercontract.Intent{Verb: verb}
		}
	}
	body, encodeErr := proto.EncodeTransferResponse(request, response)
	if encodeErr != nil {
		h.Logger.Warn("agent: transfer continuation reply not encodable", "err", encodeErr)
		return true
	}
	_ = nc.Publish(msg.Reply, body)
	return true
}

// applyContinuation runs one directed broker carrier against the retained
// worker. Prepare creates (or replays) the entry; a pull starts its sender at
// once. Commit records the sealed geometry and budget, starts the receiver and
// installs the durable Grant; Apply resumes a receiver already parked at EOF.
// origin: hot-update S7 plan §4 / §6.1 / P20 / INTEG-D agent wiring
func applyContinuation(h *Handler, r xfercontract.Request, budget xfercontract.BudgetSeal, source *xfercontract.SourceSeal, grant *xfercontract.CommitGrant) (xfercontract.Status, error) {
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return xfercontract.Status{}, err
	}
	if err := h.ports.record(r, budget, source); err != nil {
		return xfercontract.Status{}, err
	}
	if r.Op == xfercontract.OpCommit {
		if err := h.Worker.Start(r.Identity); err != nil {
			return xfercontract.Status{}, err
		}
		if _, err := h.Worker.Apply(xfercontract.Control{Kind: xfercontract.ControlGrant, Identity: r.Identity, Grant: grant}, d); err != nil {
			return xfercontract.Status{}, err
		}
		return h.Worker.Status(r.Identity)
	}
	intent := *r.Intent
	// A retained preparation owns its original descriptor even after the pathname disappears.
	// Only a fresh entry is validated against the current filesystem policy;
	// Prepare itself is the idempotent authority check for a replay.
	// origin: hot-update S7 plan §4 / P20
	if status, statusErr := h.Worker.Status(r.Identity); statusErr != nil || status.State == xfercontract.StatusUnknown {
		if intent.Verb == "push" {
			_, err = transferpath.ValidateForWrite(intent.Path, h.Roots, h.Mode)
		} else {
			_, err = transferpath.ValidateForRead(intent.Path, h.Roots, h.Mode)
		}
		if err != nil {
			return xfercontract.Status{}, err
		}
	}
	status, err := h.Worker.Prepare(xfercontract.Prepare{CallID: r.CallID, Identity: r.Identity, Origin: r.Origin, Intent: intent,
		Permit: *r.Permit, Budget: budget, Inline: r.Inline}, d)
	if err != nil {
		return xfercontract.Status{}, err
	}
	if intent.Verb == "pull" {
		if err := h.Worker.Start(r.Identity); err != nil {
			return xfercontract.Status{}, err
		}
		return h.Worker.Status(r.Identity)
	}
	return status, nil
}

func PurgePrepared[T any](mu *sync.Mutex, cache *map[string]T, id string) {
	mu.Lock()
	defer mu.Unlock()
	delete(*cache, id)
}

func transferFailure(err error, fallback string) (string, string) {
	var pve *transferpath.PathValidationError
	if errors.As(err, &pve) {
		return pve.Code, pve.Msg
	}
	return fallback, err.Error()
}

// ResolveTransferMode decides from raw configuration, not canonicalized roots.
// An all-dropped narrow list stays narrow and rejects everything; it can never
// become open access merely because canonicalization found no usable paths.
// origin: hot-update S7 plan §11.1 / P20
func ResolveTransferMode(configured bool, roots []string) int {
	if len(roots) > 0 {
		return 1
	}
	if configured {
		return 2
	}
	return 0
}
