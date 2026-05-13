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
	"syscall"
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
	vp, err := ValidateForWrite(req.Path, a.canonAllowRoots)
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
		bytes, sha, err := a.objectStoreGetAndWrite(commitCtx, store, req.TransferID, cached.vp, cached.force)
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
		// Older brokers (pre-v0.2.2) used a per-bucket "object" key;
		// newer brokers send the transfer_id. Default to transfer_id
		// when broker didn't supply — matches the v0.2.2 design.
		objectKey = req.TransferID
	}

	vp, err := ValidateForRead(req.Path, a.canonAllowRoots)
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
// unresolvable symlink) is silently dropped — operator misconfiguration
// must not turn an allow_root accident into a "no roots → allow
// everything" footgun. The result is always sorted by length descending
// so the longest match wins (so /srv/local/alice does not get shadowed
// by /srv).
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

// containedIn returns the matched allow_root or "" if path is not
// inside any of them. Comparison is "<resolved-leaf>/" vs "<root>/" so
// /srv/localfoo doesn't match /srv/local. Both sides are
// EvalSymlinks-resolved already (caller's responsibility).
func containedIn(resolvedPath string, roots []string) string {
	needle := resolvedPath + "/"
	for _, r := range roots {
		hay := r + "/"
		if strings.HasPrefix(needle, hay) {
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
	Abs       string // EvalSymlinks-resolved-parent + clean leaf; safe to open with O_NOFOLLOW
	AllowRoot string
}

// ValidateForWrite is the push-side check: agent is going to create a
// file at this path and write bytes. Steps follow file-transfer-plan
// §"Refusing dangerous paths":
//
//  1. allow_roots non-empty (else `transfer_disabled`).
//  2. absolute path required.
//  3. EvalSymlinks-resolve the parent dir; ENOENT → path_parent_missing
//     (parent dirs are NOT auto-created in v2.0).
//  4. <resolved-parent>/<base> must be inside one allow_root.
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
func ValidateForWrite(rawPath string, canonRoots []string) (*ValidatedPath, error) {
	if len(canonRoots) == 0 {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots is empty)")
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
	matched := containedIn(abs, canonRoots)
	if matched == "" {
		return nil, pathErr("path_outside_roots",
			"%s: not under any allow_root (%v)", abs, canonRoots)
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
	return &ValidatedPath{Abs: abs, AllowRoot: matched}, nil
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
func ValidateForRead(rawPath string, canonRoots []string) (*ValidatedPath, error) {
	if len(canonRoots) == 0 {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots is empty)")
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
	matched := containedIn(abs, canonRoots)
	if matched == "" {
		return nil, pathErr("path_outside_roots",
			"%s: not under any allow_root (%v)", abs, canonRoots)
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
	return &ValidatedPath{Abs: abs, AllowRoot: matched}, nil
}

// OpenForReadAtomic is the open half of the pull-side TOCTOU defence:
// after ValidateForRead returns vp, call this to get a regular-file fd.
// It re-stats via the fd and verifies dev+inode match what lstat
// captured; on mismatch the file changed type / was swapped underneath
// us between lstat and open → path_race.
func OpenForReadAtomic(vp *ValidatedPath) (*os.File, error) {
	preLstat, err := os.Lstat(vp.Abs)
	if err != nil {
		return nil, pathErr("io_error", "lstat-pre %s: %v", vp.Abs, err)
	}
	preSys, ok := preLstat.Sys().(*syscall.Stat_t)
	if !ok {
		// Non-Linux fallback: just open with NOFOLLOW; we lose
		// dev+inode TOCTOU evidence but the open itself still
		// refuses to follow a symlink at the leaf.
		return os.OpenFile(vp.Abs, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	}
	f, err := os.OpenFile(vp.Abs, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		// O_NOFOLLOW on a symlink leaf returns ELOOP on Linux.
		var perr *os.PathError
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.ELOOP) {
			return nil, pathErr("not_a_regular_file",
				"%s: refused to follow symlink (O_NOFOLLOW)", vp.Abs)
		}
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found", "%s: vanished between lstat and open", vp.Abs)
		}
		return nil, pathErr("io_error", "open %s: %v", vp.Abs, err)
	}
	postStat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, pathErr("io_error", "fstat %s: %v", vp.Abs, err)
	}
	postSys, ok := postStat.Sys().(*syscall.Stat_t)
	if !ok {
		return f, nil
	}
	if preSys.Dev != postSys.Dev || preSys.Ino != postSys.Ino {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: dev/inode changed between lstat and open (lstat=%d/%d open=%d/%d)",
			vp.Abs, preSys.Dev, preSys.Ino, postSys.Dev, postSys.Ino)
	}
	if !postStat.Mode().IsRegular() {
		_ = f.Close()
		return nil, pathErr("not_a_regular_file",
			"%s: not regular after open (mode=%s)", vp.Abs, postStat.Mode())
	}
	return f, nil
}

// OpenForWriteAtomic creates a tmp sibling under the same parent dir
// using O_NOFOLLOW|O_EXCL|O_CREAT|O_WRONLY (mode 0600). The caller
// writes bytes, fsyncs, then RenameForWriteAtomic replaces the final
// destination atomically. The tmp filename uses a "<base>.tmp.<rand>"
// pattern so a partial write left behind by a crashed agent is easy to
// spot and clean up. If the destination already exists, callers
// decide via `force` whether to rename-overwrite or fail.
func OpenForWriteAtomic(vp *ValidatedPath, randSuffix string) (*os.File, string, error) {
	tmp := vp.Abs + ".tmp." + randSuffix
	f, err := os.OpenFile(tmp,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW,
		0o600)
	if err != nil {
		// O_EXCL on an existing tmp (collision with a stale crash
		// remainder, or an attacker pre-creating the path) → bubble.
		var perr *os.PathError
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.EEXIST) {
			return nil, "", pathErr("io_error",
				"%s: tmp file exists (stale write?)", tmp)
		}
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.ELOOP) {
			return nil, "", pathErr("not_a_regular_file",
				"%s: tmp parent traversed a symlink (O_NOFOLLOW)", tmp)
		}
		return nil, "", pathErr("io_error", "open %s: %v", tmp, err)
	}
	return f, tmp, nil
}

// RenameForWriteAtomic moves the populated tmp into vp.Abs. If
// `force` is false and vp.Abs exists, returns dst_exists without
// touching anything. Otherwise calls os.Rename which is POSIX-atomic
// on the same filesystem (the tmp is a sibling so this always holds).
func RenameForWriteAtomic(vp *ValidatedPath, tmpPath string, force bool) error {
	if !force {
		if _, err := os.Lstat(vp.Abs); err == nil {
			return pathErr("dst_exists",
				"%s: destination exists; pass --force to overwrite", vp.Abs)
		} else if !os.IsNotExist(err) {
			return pathErr("io_error", "lstat %s: %v", vp.Abs, err)
		}
	}
	if err := os.Rename(tmpPath, vp.Abs); err != nil {
		return pathErr("io_error", "rename %s -> %s: %v", tmpPath, vp.Abs, err)
	}
	return nil
}
