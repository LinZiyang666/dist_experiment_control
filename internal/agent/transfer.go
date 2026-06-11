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
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"
)

// tier-A inline ceiling mirrors broker side; file-transfer-plan §Tier selection.
const agentTierAMaxBytes = 8 * 1024 * 1024

// Hard per-transfer ceiling. Every streaming path enforces this while
// reading, not only from a pre-read stat, because regular files can grow.
const agentTransferMaxBytes = int64(2 * 1024 * 1024 * 1024)

const (
	pushCommitCacheTTL        = 6 * time.Minute
	pushCommitCacheMaxEntries = 1024
)

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
	vp, err := ValidateForWrite(req.Path, a.canonAllowRoots, a.transferMode)
	if err != nil {
		code := "io_error"
		errMsg := err.Error()
		var pve *PathValidationError
		if errors.As(err, &pve) {
			code = pve.Code
			errMsg = pve.Msg
		}
		a.replyPush(nc, msg.Reply, proto.PushPrepareResp{
			OK: false, Code: code, Error: errMsg})
		// Emit ev.transfer.failed so the broker can write audit
		// failed + reap its in-memory entry immediately, instead of
		// waiting on the per-tier watchdog timeout.
		a.pubTransferEvFailed(nc, "push", &req, req.Tier, code, errMsg, 0)
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
	f, pending, err := OpenForWriteAtomic(vp, rnd)
	if err != nil {
		var pve *PathValidationError
		if errors.As(err, &pve) {
			finalize(pve.Code, pve.Msg, 0)
		} else {
			finalize("io_error", err.Error(), 0)
		}
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
	if err := RenameForWriteAtomic(vp, pending, req.Force); err != nil {
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

	// Look up the expected SHA + path + allow_roots validation saved
	// during push.req prepare (a.pushCommitCache — no object metadata
	// is written, the original PushPrepareReq is the only carrier).
	// Cache miss (agent restarted between prep and commit) →
	// transfer_unknown; the broker's watchdog writes the failed audit.
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
		bytes, err := a.objectStoreGetAndWrite(
			commitCtx, store, req.TransferID, cached.vp,
			cached.size, cached.sha256, cached.force,
		)
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
		a.pubTransferEv(nc, "complete", "push", &proto.PushPrepareReq{
			TransferID: req.TransferID, Path: cached.path,
			SHA256: cached.sha256, Size: cached.size, Tier: "b",
			Bucket: req.Bucket,
		}, "b", bytes, time.Since(startedAt))
		a.purgePushCommitCache(req.TransferID)
	}()
}

// objectStoreGetAndWrite streams the object into a pinned-parent tmp,
// enforces the prepare-time size, verifies SHA, then commits. Verification
// must happen before rename: a failed object never becomes the destination.
func (a *Agent) objectStoreGetAndWrite(
	ctx context.Context,
	store jetstream.ObjectStore,
	objectName string,
	vp *ValidatedPath,
	expectedSize int64,
	expectedSHA string,
	force bool,
) (int64, error) {
	if expectedSize < 0 || expectedSize > agentTransferMaxBytes {
		return 0, pathErr("too_large", "invalid expected size %d", expectedSize)
	}
	result, err := store.Get(ctx, objectName)
	if err != nil {
		return 0, err
	}
	defer func() { _ = result.Close() }()

	rnd := randSuffix()
	f, pending, err := OpenForWriteAtomic(vp, rnd)
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
	if err := RenameForWriteAtomic(vp, pending, force); err != nil {
		return n, err
	}
	return n, nil
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
		// Older brokers (pre-v0.2.2) used a per-bucket "object" key;
		// newer brokers send the transfer_id. Default to transfer_id
		// when broker didn't supply — matches the v0.2.2 design.
		objectKey = req.TransferID
	}

	vp, err := ValidateForRead(req.Path, a.canonAllowRoots, a.transferMode)
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
	if size > agentTransferMaxBytes {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			Error: fmt.Sprintf("file size=%d > 2 GiB", size)})
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
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
			return
		}
		if int64(len(data)) <= maxInline {
			sha := hexSHA256(data)
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: true, Tier: "a", Size: int64(len(data)),
				SHA256: sha, InlineData: data,
			})
			return
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
				OK: false, Code: "io_error", Error: err.Error()})
			return
		}
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
	limited := &io.LimitedReader{R: f, N: agentTransferMaxBytes + 1}
	tee := io.TeeReader(limited, h)
	info, err := store.Put(putCtx, jetstream.ObjectMeta{Name: objectKey}, tee)
	if err != nil {
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "object_put_failed", Error: err.Error()})
		return
	}
	uploaded := int64(info.Size)
	if uploaded > agentTransferMaxBytes {
		_ = store.Delete(putCtx, objectKey)
		a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
			OK: false, Code: "too_large",
			Error: fmt.Sprintf("file grew beyond 2 GiB while reading (read=%d)", uploaded)})
		return
	}
	sha := hex.EncodeToString(h.Sum(nil))
	a.replyPull(nc, msg.Reply, proto.PullPrepareResp{
		OK: true, Tier: "b", Size: uploaded, SHA256: sha,
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

// pubTransferEvFailed mirrors pubTransferEv with kind=failed. Tolerates
// being called with an empty TransferID (e.g. agent rejected the
// request before parsing); silently skip in that case so we don't
// publish an unnamed ev.transfer.
func (a *Agent) pubTransferEvFailed(nc *nats.Conn, verb string, req *proto.PushPrepareReq,
	tier, code, errMsg string, dur time.Duration) {
	if req.TransferID == "" {
		return
	}
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
	added  time.Time
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
	now := time.Now()
	for id, cached := range a.pushCommitCache {
		if now.Sub(cached.added) >= pushCommitCacheTTL {
			delete(a.pushCommitCache, id)
		}
	}
	if len(a.pushCommitCache) >= pushCommitCacheMaxEntries {
		var oldestID string
		var oldest time.Time
		for id, cached := range a.pushCommitCache {
			if oldestID == "" || cached.added.Before(oldest) {
				oldestID, oldest = id, cached.added
			}
		}
		delete(a.pushCommitCache, oldestID)
	}
	e.added = now
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

// ─── Path validation (allow_roots + symlink defence) ──────────────────

// PathValidationError carries both a human message and a machine-readable
// code that lands in the proto.Push/PullPrepareResp.Code field.
//
// The code names come from file-transfer-plan §"Refusing dangerous paths"
// step list and the §Audit code column. Any new code added here must
// also be documented in the plan.
type PathValidationError struct {
	Code string
	Msg  string
}

func (e *PathValidationError) Error() string { return e.Code + ": " + e.Msg }

// IsPathValidationError unwraps to bool so callers can do
// `if errors.As(err, &pve) { ... }` without per-call helper noise.
func IsPathValidationError(err error) bool {
	var pve *PathValidationError
	return errors.As(err, &pve)
}

// pathErr is a tiny constructor — keeps call sites short.
func pathErr(code, format string, a ...any) *PathValidationError {
	return &PathValidationError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

// CanonAllowRoots cleans + de-duplicates the allow_roots list and
// returns each entry in EvalSymlinks-resolved form. Any allow_root that
// fails EvalSymlinks (does not exist, is not a directory, contains an
// unresolvable symlink) is silently dropped. Dropping shrinks the
// narrow-mode root list but NEVER widens reach: transferMode is decided
// upstream from the RAW config presence (resolveTransferMode), so a
// narrow config whose entries all drop stays modeNarrow with an empty
// list and rejects every path (fail-closed) — it does not collapse to the
// whole-FS open mode. The result is always sorted by length descending so
// the longest match wins (so /srv/local/alice does not get shadowed by
// /srv).
//
// Callers should invoke this ONCE at startup (Agent.New stores the
// result in a.canonAllowRoots) and pass the result to ValidateForRead /
// ValidateForWrite — those funcs no longer canonicalize per-call.
// Audit shard P11 F3: previously these helpers re-evaluated symlinks
// on every push/pull request, which was both a perf hit and a TOCTOU
// risk if an operator's allow_root happened to be symlinked.
func CanonAllowRoots(roots []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(roots))
	for _, r := range roots {
		if r == "" {
			continue
		}
		if !filepath.IsAbs(r) {
			continue
		}
		clean := filepath.Clean(r)
		resolved, err := filepath.EvalSymlinks(clean)
		if err != nil {
			continue
		}
		// Must actually be a directory — a regular file as an
		// allow_root would let the caller "write to /any/path/under/it"
		// which makes no sense.
		st, err := os.Stat(resolved)
		if err != nil || !st.IsDir() {
			continue
		}
		if _, dup := seen[resolved]; dup {
			continue
		}
		seen[resolved] = struct{}{}
		out = append(out, resolved)
	}
	// Longest-prefix-wins ordering.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if len(out[j]) > len(out[i]) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// containedIn returns the matched allow_root or "" if path is not inside
// any of them. filepath.Rel handles the filesystem root correctly (`/`
// contains `/etc/hosts`) while still preventing `/srv/localfoo` from
// matching `/srv/local`. Both sides are EvalSymlinks-resolved already.
func containedIn(resolvedPath string, roots []string) string {
	for _, r := range roots {
		rel, err := filepath.Rel(r, resolvedPath)
		if err != nil || filepath.IsAbs(rel) {
			continue
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return r
		}
	}
	return ""
}

// ValidatedPath is what ValidateForRead / ValidateForWrite return on
// success. It carries both the absolute path the agent should open
// AND the matched allow_root (so the caller can attribute audit
// "tier=" / "root=" cleanly).
type ValidatedPath struct {
	Abs string // EvalSymlinks-resolved-parent + clean leaf; safe to open with O_NOFOLLOW
	// Parent and Base preserve the canonical parent/leaf split so the
	// openat pipeline can pin the directory and never re-traverse an
	// attacker-swappable parent path.
	Parent string
	Base   string
	// AllowRoot is the matched allow_root in narrow mode; "" in open mode
	// (no narrowing configured). Informational for audit/log attribution
	// only — it has zero readers today.
	AllowRoot string
}

// transferMode is the agent-wide push/pull path policy, resolved once at
// New() from the RAW agent.yaml file_transfer.allow_roots presence (NEVER
// from len(canonAllowRoots) — see resolveTransferMode). It selects how the
// containment step in ValidateForWrite/ValidateForRead behaves; every
// OTHER mechanism check (absolute, EvalSymlinks-parent, parent-exists,
// leaf-symlink, regular-file, O_NOFOLLOW, dev+inode TOCTOU) runs in ALL
// modes, because those defend a hostile NON-member process on the agent
// host — a threat orthogonal to which files an authenticated member may
// touch (the member already has unrestricted run/exec; requirements.md §9.3).
type transferMode int

const (
	// modeOpen: no allow_roots configured (key absent/null). Whole-FS
	// reach, equal to run/exec; the containment step is SKIPPED but every
	// mechanism check still runs. The default, including fresh installs.
	modeOpen transferMode = iota
	// modeNarrow: non-empty allow_roots. containedIn prefix check;
	// path_outside_roots on miss. An all-dropped narrow list stays here
	// and rejects everything (fail-closed); it never falls to modeOpen.
	modeNarrow
	// modeDisabled: explicit `allow_roots: []`. Every push/pull replies
	// transfer_disabled before any path work.
	modeDisabled
)

// resolveTransferMode decides the policy from RAW config, never from the
// canonicalized root list. rootsConfigured is true iff the operator wrote
// an allow_roots key in agent.yaml (yaml.v3 leaves the slice nil when the
// key is absent/null, non-nil len-0 for an explicit `[]`). rawRoots is the
// pre-CanonAllowRoots list, so a narrow config whose entries all drop
// during canonicalization still resolves to modeNarrow (reject-all),
// NEVER to modeOpen — the single most important security invariant of the
// transfer-unrestrict change.
func resolveTransferMode(rootsConfigured bool, rawRoots []string) transferMode {
	if len(rawRoots) > 0 {
		return modeNarrow
	}
	if rootsConfigured {
		return modeDisabled // explicit allow_roots: []
	}
	return modeOpen // key absent → whole-FS default
}

// ValidateForWrite is the push-side check: agent is going to create a
// file at this path and write bytes. Steps follow file-transfer-plan
// §"Refusing dangerous paths":
//
//  1. mode != modeDisabled (explicit `allow_roots: []` → transfer_disabled).
//  2. absolute path required.
//  3. EvalSymlinks-resolve the parent dir; ENOENT → path_parent_missing
//     (parent dirs are NOT auto-created in v2.0).
//  4. In modeNarrow, <resolved-parent>/<base> must be inside one
//     allow_root (path_outside_roots on miss). modeOpen skips this step.
//  5. If destination already exists, must be a regular file (not a
//     symlink, dir, device, fifo) — symlink → not_a_regular_file.
//
// Caller still opens with O_NOFOLLOW|O_EXCL|O_CREAT to defeat any
// race-window symlink swap between this validation and open. dst-
// exists is NOT considered an error here (the caller's `--force`
// logic decides what to do); the leaf-symlink check still happens
// before that decision so a symlink dest can never silently dereference.
//
// `canonRoots` MUST be the result of CanonAllowRoots (called once at
// agent startup); this function does NOT re-canonicalize.
func ValidateForWrite(rawPath string, canonRoots []string, mode transferMode) (*ValidatedPath, error) {
	if mode == modeDisabled {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots: [])")
	}
	if !filepath.IsAbs(rawPath) {
		return nil, pathErr("path_not_absolute",
			"%s: must be absolute", rawPath)
	}
	clean := filepath.Clean(rawPath)
	parent := filepath.Dir(clean)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_parent_missing",
				"%s: parent directory does not exist (run `tether exec ... mkdir -p` first)", parent)
		}
		return nil, pathErr("io_error",
			"resolve parent of %s: %v", rawPath, err)
	}
	abs := filepath.Join(resolvedParent, filepath.Base(clean))
	// Containment applies only in narrow mode. In open mode matched stays
	// "" (whole-FS reach); every mechanism check below still runs.
	matched := ""
	if mode == modeNarrow {
		matched = containedIn(abs, canonRoots)
		if matched == "" {
			return nil, pathErr("path_outside_roots",
				"%s: not under any allow_root (%v)", abs, canonRoots)
		}
	}
	// If the leaf already exists, it must be a regular file. A symlink
	// at the leaf is rejected here so a follow-up O_NOFOLLOW open
	// failure isn't the operator's only signal.
	if st, err := os.Lstat(abs); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, pathErr("not_a_regular_file",
				"%s: refuses to follow symlink", abs)
		}
		if !st.Mode().IsRegular() {
			return nil, pathErr("not_a_regular_file",
				"%s: not a regular file (mode=%s)", abs, st.Mode())
		}
	} else if !os.IsNotExist(err) {
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	return &ValidatedPath{
		Abs: abs, Parent: resolvedParent, Base: filepath.Base(clean),
		AllowRoot: matched,
	}, nil
}

// ValidateForRead is the pull-side check: agent is going to open this
// path read-only and stream its bytes. Steps mirror ValidateForWrite,
// plus:
//
//   - leaf must exist (else path_not_found);
//   - leaf must NOT be a symlink (lstat check; O_NOFOLLOW on the open
//     would also catch it, but lstat gives a clean error code);
//   - leaf must be a regular file (not a dir / device / fifo /
//     socket).
//
// TOCTOU: the caller is expected to open(O_RDONLY|O_NOFOLLOW) and
// fstat the resulting fd, comparing dev+inode against the lstat
// captured here. The race window between lstat and open is small but
// not zero on adversarial filesystems; dev+inode mismatch surfaces as
// `path_race` at the open site.
//
// `canonRoots` MUST be the result of CanonAllowRoots (called once at
// agent startup); this function does NOT re-canonicalize.
func ValidateForRead(rawPath string, canonRoots []string, mode transferMode) (*ValidatedPath, error) {
	if mode == modeDisabled {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots: [])")
	}
	if !filepath.IsAbs(rawPath) {
		return nil, pathErr("path_not_absolute",
			"%s: must be absolute", rawPath)
	}
	clean := filepath.Clean(rawPath)
	parent := filepath.Dir(clean)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found",
				"%s: parent directory does not exist", parent)
		}
		return nil, pathErr("io_error",
			"resolve parent of %s: %v", rawPath, err)
	}
	abs := filepath.Join(resolvedParent, filepath.Base(clean))
	// Containment applies only in narrow mode. In open mode matched stays
	// "" (whole-FS reach); the leaf checks below still run.
	matched := ""
	if mode == modeNarrow {
		matched = containedIn(abs, canonRoots)
		if matched == "" {
			return nil, pathErr("path_outside_roots",
				"%s: not under any allow_root (%v)", abs, canonRoots)
		}
	}
	st, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found", "%s: not found", abs)
		}
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return nil, pathErr("not_a_regular_file",
			"%s: refuses to follow symlink", abs)
	}
	if !st.Mode().IsRegular() {
		return nil, pathErr("not_a_regular_file",
			"%s: not a regular file (mode=%s)", abs, st.Mode())
	}
	return &ValidatedPath{
		Abs: abs, Parent: resolvedParent, Base: filepath.Base(clean),
		AllowRoot: matched,
	}, nil
}

func validatedParentAndBase(vp *ValidatedPath) (string, string, error) {
	parent, base := vp.Parent, vp.Base
	if parent == "" {
		var err error
		parent, err = filepath.EvalSymlinks(filepath.Dir(vp.Abs))
		if err != nil {
			return "", "", err
		}
	}
	if base == "" {
		base = filepath.Base(vp.Abs)
	}
	return parent, base, nil
}

// openDirNoSymlinks opens an already-EvalSymlinks-resolved absolute
// directory one component at a time. Each component uses O_NOFOLLOW, so a
// hostile local process cannot replace a validated parent component with a
// symlink between validation and the eventual file operation.
func openDirNoSymlinks(absDir string) (int, error) {
	clean := filepath.Clean(absDir)
	if !filepath.IsAbs(clean) {
		return -1, fmt.Errorf("directory is not absolute: %s", absDir)
	}
	fd, err := unix.Open(string(filepath.Separator),
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if clean == string(filepath.Separator) {
		return fd, nil
	}
	parts := strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		next, openErr := unix.Openat(fd, part,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func unixModeIsRegular(mode uint32) bool {
	return mode&unix.S_IFMT == unix.S_IFREG
}

func unixModeIsSymlink(mode uint32) bool {
	return mode&unix.S_IFMT == unix.S_IFLNK
}

func dirFDStillNamesPath(dirFD int, parent string) (bool, error) {
	currentFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return false, err
	}
	defer func() { _ = unix.Close(currentFD) }()
	var pinned, current unix.Stat_t
	if err := unix.Fstat(dirFD, &pinned); err != nil {
		return false, err
	}
	if err := unix.Fstat(currentFD, &current); err != nil {
		return false, err
	}
	return pinned.Dev == current.Dev && pinned.Ino == current.Ino, nil
}

// OpenForReadAtomic pins the canonical parent directory with openat,
// lstats and opens the leaf relative to that fd, then compares dev+inode.
// Parent and leaf swaps therefore fail closed instead of being followed.
func OpenForReadAtomic(vp *ValidatedPath) (*os.File, error) {
	parent, base, err := validatedParentAndBase(vp)
	if err != nil {
		return nil, pathErr("path_race", "resolve validated parent of %s: %v", vp.Abs, err)
	}
	dirFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return nil, pathErr("path_race", "open validated parent %s: %v", parent, err)
	}
	defer func() { _ = unix.Close(dirFD) }()

	var pre unix.Stat_t
	if err := unix.Fstatat(dirFD, base, &pre, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, pathErr("path_not_found", "%s: vanished before open", vp.Abs)
		}
		return nil, pathErr("io_error", "lstat-pre %s: %v", vp.Abs, err)
	}
	if unixModeIsSymlink(uint32(pre.Mode)) {
		return nil, pathErr("not_a_regular_file",
			"%s: refused to follow symlink", vp.Abs)
	}
	if !unixModeIsRegular(uint32(pre.Mode)) {
		return nil, pathErr("not_a_regular_file",
			"%s: not regular before open (mode=%#o)", vp.Abs, pre.Mode)
	}

	fd, err := unix.Openat(dirFD, base, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, pathErr("not_a_regular_file",
				"%s: refused to follow symlink (O_NOFOLLOW)", vp.Abs)
		}
		if errors.Is(err, unix.ENOENT) {
			return nil, pathErr("path_not_found", "%s: vanished between lstat and open", vp.Abs)
		}
		return nil, pathErr("io_error", "open %s: %v", vp.Abs, err)
	}
	f := os.NewFile(uintptr(fd), vp.Abs)
	if f == nil {
		_ = unix.Close(fd)
		return nil, pathErr("io_error", "open %s: invalid file descriptor", vp.Abs)
	}
	var post unix.Stat_t
	if err := unix.Fstat(fd, &post); err != nil {
		_ = f.Close()
		return nil, pathErr("io_error", "fstat %s: %v", vp.Abs, err)
	}
	if pre.Dev != post.Dev || pre.Ino != post.Ino {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: dev/inode changed between lstat and open (lstat=%d/%d open=%d/%d)",
			vp.Abs, pre.Dev, pre.Ino, post.Dev, post.Ino)
	}
	if !unixModeIsRegular(uint32(post.Mode)) {
		_ = f.Close()
		return nil, pathErr("not_a_regular_file",
			"%s: not regular after open (mode=%#o)", vp.Abs, post.Mode)
	}
	if same, err := dirFDStillNamesPath(dirFD, parent); err != nil || !same {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: parent changed while opening (same=%v err=%v)", vp.Abs, same, err)
	}
	return f, nil
}

// AtomicWrite retains the validated parent fd from tmp creation through
// commit. Abort removes the tmp relative to that same fd, so cleanup cannot
// be redirected by swapping a parent path.
type AtomicWrite struct {
	dirFD   int
	tmpName string
	dstName string
	tmpPath string
	dstPath string
	parent  string
	done    bool
}

func (w *AtomicWrite) Abort() {
	if w == nil || w.done {
		return
	}
	_ = unix.Unlinkat(w.dirFD, w.tmpName, 0)
	_ = unix.Close(w.dirFD)
	w.done = true
}

func (w *AtomicWrite) finish() {
	if w.done {
		return
	}
	_ = unix.Close(w.dirFD)
	w.done = true
}

// OpenForWriteAtomic creates a tmp sibling relative to a pinned canonical
// parent fd. The caller writes, fsyncs, closes, then commits through
// RenameForWriteAtomic. A deferred pending.Abort() is required.
func OpenForWriteAtomic(vp *ValidatedPath, randSuffix string) (*os.File, *AtomicWrite, error) {
	parent, base, err := validatedParentAndBase(vp)
	if err != nil {
		return nil, nil, pathErr("path_race",
			"resolve validated parent of %s: %v", vp.Abs, err)
	}
	dirFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return nil, nil, pathErr("path_race",
			"open validated parent %s: %v", parent, err)
	}
	tmpName := base + ".tmp." + randSuffix
	tmpPath := filepath.Join(parent, tmpName)
	fd, err := unix.Openat(dirFD, tmpName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600)
	if err != nil {
		_ = unix.Close(dirFD)
		if errors.Is(err, unix.EEXIST) {
			return nil, nil, pathErr("io_error",
				"%s: tmp file exists (stale write?)", tmpPath)
		}
		if errors.Is(err, unix.ELOOP) {
			return nil, nil, pathErr("not_a_regular_file",
				"%s: refused symlink tmp leaf (O_NOFOLLOW)", tmpPath)
		}
		return nil, nil, pathErr("io_error", "open %s: %v", tmpPath, err)
	}
	f := os.NewFile(uintptr(fd), tmpPath)
	if f == nil {
		_ = unix.Close(fd)
		_ = unix.Unlinkat(dirFD, tmpName, 0)
		_ = unix.Close(dirFD)
		return nil, nil, pathErr("io_error", "open %s: invalid file descriptor", tmpPath)
	}
	return f, &AtomicWrite{
		dirFD: dirFD, tmpName: tmpName, dstName: base,
		tmpPath: tmpPath, dstPath: vp.Abs, parent: parent,
	}, nil
}

// RenameForWriteAtomic commits a populated tmp. No-force uses linkat as
// an atomic create-if-absent operation; unlike Lstat+Rename it cannot
// overwrite a destination created in the check/rename window.
func RenameForWriteAtomic(vp *ValidatedPath, pending *AtomicWrite, force bool) error {
	if pending == nil || pending.done || pending.dstPath != vp.Abs {
		return pathErr("io_error", "invalid or completed atomic write for %s", vp.Abs)
	}
	if same, err := dirFDStillNamesPath(pending.dirFD, pending.parent); err != nil || !same {
		return pathErr("path_race",
			"%s: parent changed before commit (same=%v err=%v)", vp.Abs, same, err)
	}
	if force {
		if err := unix.Renameat(
			pending.dirFD, pending.tmpName,
			pending.dirFD, pending.dstName,
		); err != nil {
			return pathErr("io_error", "rename %s -> %s: %v",
				pending.tmpPath, vp.Abs, err)
		}
		pending.finish()
		return nil
	}

	if err := unix.Linkat(
		pending.dirFD, pending.tmpName,
		pending.dirFD, pending.dstName, 0,
	); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return pathErr("dst_exists",
				"%s: destination exists; pass --force to overwrite", vp.Abs)
		}
		return pathErr("io_error", "link %s -> %s: %v",
			pending.tmpPath, vp.Abs, err)
	}
	// Destination is committed. A failed tmp unlink only leaves a second
	// hardlink to the same inode; it must not turn a successful commit into
	// a retry that would report dst_exists.
	_ = unix.Unlinkat(pending.dirFD, pending.tmpName, 0)
	pending.finish()
	return nil
}
