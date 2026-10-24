package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NewLegacyTransfer is only called after SelectTransferClient returns explicit
// cap0. The legacy owner gates this entire transaction once, including SDK Put;
// a lost response cannot reenter it or fabricate an exact history receipt.
// origin: hot-update S7 plan §7 / §9 / INTEG-D
func NewLegacyTransfer(cfg xferclient.Config, local string) (xferclient.LegacyInvocation, error) {
	t, ok := cfg.Transport.(*TransferTransport)
	if !ok {
		return nil, errors.New("legacy transfer requires its authenticated transport")
	}
	if cfg.Intent.RateLimitBPS != 0 {
		return nil, &xferclient.Error{Code: x.CodePayloadUnsupported, Cause: errors.New("legacy peer cannot seal a rate limit")}
	}
	return xferclient.NewLegacyLocal(xferclient.LegacyConfig{Base: cfg.Base, Intent: cfg.Intent, TransferID: cfg.Identity.TransferID,
		PutOnce: func(d x.Deadline) (x.LegacyResult, error) { return t.legacyOnce(cfg, local, d) }}, local)
}

func (t *TransferTransport) legacyRequest(ctx context.Context, subject string, request, response any, d x.Deadline) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	// Legacy prepare can include all byte IO before replying. Keep the original
	// business deadline, and send once: v1's control cap does not apply here.
	// origin: S7 internal review rv-n1compat F5
	requestCtx, cancel, err := AdmissionContext(ctx, d)
	if err != nil {
		return err
	}
	defer cancel()
	msg, _, err := transferRequestAttempt(requestCtx, t.control, subject, body)
	if err != nil {
		return err
	}
	return json.Unmarshal(msg.Data, response)
}

func (t *TransferTransport) legacyOnce(cfg xferclient.Config, local string, d x.Deadline) (x.LegacyResult, error) {
	// ctx-none: LegacyInvocation supplies its absolute deadline, not a context.
	ctx, cancel, err := AdmissionContext(context.Background(), d)
	if err != nil {
		return x.LegacyResult{}, err
	}
	defer cancel()
	id := cfg.Identity
	if cfg.Intent.Verb == "push" {
		return t.legacyPush(ctx, cfg, d)
	}
	started := time.Now()
	var pr proto.PullPrepareResp
	err = t.legacyRequest(ctx, proto.SubjCmdBy(id.SID, t.id.PublicKey, id.NID, "pull"), proto.PullPrepareReq{TransferID: id.TransferID, Path: cfg.Intent.Path, MaxInline: min(int64(proto.XferTierAMaxBytes), t.control.MaxPayload()/2-4096), Force: cfg.Intent.Force}, &pr, d)
	if err != nil {
		return x.LegacyResult{}, err
	}
	if !pr.OK {
		// Agent-side prepare failures happen after the broker accepted and
		// tracked this transfer, so finalize them for audit + object cleanup.
		// A duplicate transfer ID is rejected before insertion; finalizing that
		// ID would instead claim and terminate the original in-flight pull.
		if PullPrepareFailureNeedsFinalize(pr.Code) {
			t.abandonLegacy(ctx, id, "", started, pr.Code, pr.Error)
		}
		return legacyRefusal(pr.Code, pr.Error)
	}
	// Until the destination lands, every failure tells the cap0 broker the
	// pull ended (finalize{failed}, best effort), as the pre-S7 pull did; the
	// broker otherwise held the entry and its object to the watchdog budget.
	// After the rename nothing may claim failure for a landed file.
	// origin: hot-update S7 plan §7 / §9 / P31; pre-S7 runPull finishPullTierA/B
	abandon := func(code string, cause error) error {
		t.abandonLegacy(ctx, id, pr.Tier, started, code, cause.Error())
		return cause
	}
	refuse := func(code, detail string) (x.LegacyResult, error) {
		t.abandonLegacy(ctx, id, pr.Tier, started, code, detail)
		return legacyRefusal(code, detail)
	}
	if pr.Size < 0 || pr.Size > proto.XferMaxBytes {
		return refuse(x.CodePayloadUnsupported, "invalid legacy size")
	}
	var reader io.ReadCloser
	switch pr.Tier {
	case "a":
		if int64(len(pr.InlineData)) != pr.Size || pr.Size > proto.XferTierAMaxBytes {
			return refuse(x.CodeSourceChanged, "invalid inline size")
		}
		reader = io.NopCloser(bytes.NewReader(pr.InlineData))
	case "b":
		if pr.Bucket != proto.XferBucketName(id.SID) || pr.ObjectKey != id.TransferID {
			return refuse(x.CodeIntentConflict, "legacy object binding changed")
		}
		js, err := t.OpenData(id, d)
		if err != nil {
			return x.LegacyResult{}, abandon("jetstream_unavailable", err)
		}
		store, err := js.ObjectStore(ctx, pr.Bucket)
		if err != nil {
			return x.LegacyResult{}, abandon("bucket_unknown", err)
		}
		result, err := jsstream.GetTransferObject(ctx, js, store, pr.Bucket, pr.ObjectKey)
		if err != nil {
			return x.LegacyResult{}, abandon("object_get_failed", err)
		}
		reader = result
	default:
		return refuse(x.CodePayloadUnsupported, "invalid legacy tier")
	}
	defer reader.Close() //nolint:errcheck // Closing the read-only object reader cannot change the verified legacy outcome.
	vp, err := transferpath.ValidateForWrite(local, nil, transferpath.ModeOpen)
	if err != nil {
		return x.LegacyResult{}, abandon("io_error", err)
	}
	file, pending, err := transferpath.OpenForWriteAtomic(vp, id.TransferID)
	if err != nil {
		return x.LegacyResult{}, abandon("io_error", err)
	}
	// The private tmp descriptor is this invocation's; nothing else closes it
	// (Abort and the rename act on names). Left to the GC finalizer it leaked
	// one descriptor per pull until a collection happened to run.
	// origin: INTEG-D cmd/tether race suite (TestBothPullPathsCarryTheMode fd gate)
	defer file.Close() //nolint:errcheck // Sync and atomic rename determine durability; descriptor teardown cannot revise a landed result.
	defer pending.Abort()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(reader, pr.Size+1))
	if err != nil {
		return x.LegacyResult{}, abandon("object_get_failed", err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if n != pr.Size || digest != pr.SHA256 {
		return refuse(x.CodeSourceChanged, "legacy source size or digest mismatch")
	}
	if err = file.Sync(); err != nil {
		return x.LegacyResult{}, abandon("io_error", err)
	}
	if err = transferpath.RenameForWriteAtomic(vp, pending, cfg.Intent.Force); err != nil {
		return x.LegacyResult{}, abandon("io_error", err)
	}
	var final proto.TransferFinalizeResp
	err = t.legacyRequest(ctx, proto.SubjCtrlTransferFinalize(t.id.PublicKey, id.SID, id.TransferID), proto.TransferFinalize{Kind: "complete", TransferID: id.TransferID, Tier: pr.Tier, Bucket: pr.Bucket, Path: cfg.Intent.Path, Bytes: n}, &final, d)
	if err != nil {
		return x.LegacyResult{}, err
	}
	if !final.OK {
		return legacyRefusal(final.Code, final.Error)
	}
	return x.LegacyResult{Kind: "complete", Bytes: n, SHA256: digest, BestEffort: true}, nil
}

func (t *TransferTransport) legacyPush(ctx context.Context, cfg xferclient.Config, d x.Deadline) (x.LegacyResult, error) {
	id := cfg.Identity
	size := cfg.Intent.Size
	started := time.Now()
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(cfg.Source, 0, size)); err != nil {
		return x.LegacyResult{}, err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	tier := "b"
	if size <= min(int64(proto.XferTierAMaxBytes), t.control.MaxPayload()/2-4096) {
		tier = "a"
	}
	req := proto.PushPrepareReq{TransferID: id.TransferID, Path: cfg.Intent.Path, Size: size, SHA256: digest, Force: cfg.Intent.Force, Tier: tier}
	if tier == "a" {
		data, err := io.ReadAll(io.NewSectionReader(cfg.Source, 0, size))
		if err != nil {
			return x.LegacyResult{}, err
		}
		req.InlineData = data
	}
	var prep proto.PushPrepareResp
	if err := t.legacyRequest(ctx, proto.SubjCmdBy(id.SID, t.id.PublicKey, id.NID, "push"), req, &prep, d); err != nil {
		return x.LegacyResult{}, err
	}
	if !prep.OK {
		return legacyRefusal(prep.Code, prep.Error)
	}
	if tier == "b" {
		// From the accepted prepare until a commit is forwarded, a cap0 broker
		// holds this transfer's slot in the session bucket for us, and only its
		// size budget would end it. Every exit before that tells it we gave up
		// (finalize{failed}, best effort): the broker arbitrates by its own
		// committed flag, so after a lost commit reply it refuses the finalize
		// if it did forward. This is not a retry; nothing is uploaded again.
		// origin: simcluster-speed 0a (drill 67) / review R4-2-F9; hot-update S7 plan §7 / §9 / P31
		abandon := func(code, detail string) { t.abandonLegacy(ctx, id, "b", started, code, detail) }
		js, err := t.OpenData(id, d)
		if err != nil {
			abandon("jetstream_unavailable", err.Error())
			return x.LegacyResult{}, err
		}
		owner, err := xferclient.NewLegacyObject(xferclient.LegacyConfig{Base: cfg.Base, Intent: cfg.Intent, TransferID: id.TransferID}, js, proto.XferBucketName(id.SID), id.TransferID, io.NewSectionReader(cfg.Source, 0, size))
		if err != nil {
			abandon("object_put_failed", err.Error())
			return x.LegacyResult{}, err
		}
		result, err := owner.Run(d)
		_ = owner.Stop(d)
		if err != nil {
			code := "object_put_failed"
			if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				code = "bucket_bind_failed"
			}
			abandon(code, err.Error())
			return result, err
		}
		if result.Bytes != size || result.SHA256 != digest {
			tombstoneLegacyUpload(ctx, js, proto.XferBucketName(id.SID), id.TransferID)
			abandon(x.CodeSourceChanged, "legacy source changed during upload")
			return legacyRefusal(x.CodeSourceChanged, "legacy source changed during upload")
		}
		// An ACK is best effort only. A lost ACK returns unresolved without another
		// object upload, another commit attempt or a replacement business identity.
		// The old receiver ACKs before Get/SHA/rename. Admit its terminal
		// subscription before commit so even an immediate failure is observed.
		// origin: S7 internal review rv-n1compat F1
		events, err := t.control.SubscribeSync(proto.SubjEvTransfer(id.SID, id.NID, id.TransferID, "*"))
		if err != nil {
			abandon("commit_lost", err.Error())
			return x.LegacyResult{}, err
		}
		defer events.Unsubscribe() //nolint:errcheck // Subscription teardown is best-effort after the one-shot outcome and cannot revise the remote transfer result.
		if err = t.control.FlushWithContext(ctx); err != nil {
			abandon("commit_lost", err.Error())
			return x.LegacyResult{}, err
		}
		if errors.Is(t.control.LastError(), nats.ErrPermissionViolation) {
			return x.LegacyResult{}, nats.ErrPermissionViolation
		}
		var commit proto.TransferCommitResp
		if err = t.legacyRequest(ctx, proto.SubjCmdBy(id.SID, t.id.PublicKey, id.NID, "push-commit"), proto.TransferCommitReq{TransferID: id.TransferID, Bucket: proto.XferBucketName(id.SID), ObjectKey: id.TransferID}, &commit, d); err != nil {
			abandon("commit_lost", err.Error())
			return x.LegacyResult{}, err
		}
		if !commit.OK {
			// A refusal is sent INSTEAD of forwarding: the entry is not committed.
			abandon(commit.Code, commit.Error)
			return legacyRefusal(commit.Code, commit.Error)
		}
		return legacyReceiverOutcome(ctx, events, id, size, digest)
	}
	return x.LegacyResult{Kind: "complete", Bytes: size, SHA256: digest, BestEffort: true}, nil
}

// SDK Delete publishes deleted metadata before its best-effort chunk purge.
// Members cannot purge the session stream, so this cleanup needs its own short
// deadline; the broker reaper retains the remaining chunk responsibility.
// origin: TestTombstoneUploadedObjectReturnsOnItsOwnDeadline / hot-update S7 plan §7
func tombstoneLegacyUpload(parent context.Context, js jetstream.JetStream, bucket, key string) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	store, err := js.ObjectStore(ctx, bucket)
	if err == nil {
		_ = store.Delete(ctx, key)
	}
}

// Legacy events are best-effort evidence, never a v1 history receipt. Missing,
// malformed or mismatched evidence leaves this invocation unresolved.
// origin: S7 internal review rv-n1compat F1
func legacyReceiverOutcome(ctx context.Context, events *nats.Subscription, id x.Identity, size int64, digest string) (x.LegacyResult, error) {
	msg, err := events.NextMsgWithContext(ctx)
	if err != nil {
		return x.LegacyResult{}, err
	}
	var ev proto.TransferEvent
	if err = json.Unmarshal(msg.Data, &ev); err != nil {
		return x.LegacyResult{}, err
	}
	if ev.TransferID != id.TransferID || ev.Verb != "push" || ev.Tier != "b" ||
		msg.Subject != proto.SubjEvTransfer(id.SID, id.NID, id.TransferID, ev.Kind) {
		return x.LegacyResult{}, errors.New("legacy receiver event identity mismatch")
	}
	if ev.Kind == "failed" {
		return legacyRefusal(ev.Code, ev.Error)
	}
	if ev.Kind != "complete" || ev.Code != "" || ev.Error != "" || ev.Bytes != size {
		return x.LegacyResult{}, errors.New("legacy receiver event has no valid terminal outcome")
	}
	return x.LegacyResult{Kind: "complete", Bytes: ev.Bytes, SHA256: digest, BestEffort: true}, nil
}

// abandonLegacy tells a cap0 broker this invocation gave up. It has its own
// three-second budget: it must still reach the broker after the invocation's
// deadline ended the byte work. Its reply is advisory; nothing is retried.
func (t *TransferTransport) abandonLegacy(parent context.Context, id x.Identity, tier string, started time.Time, code, detail string) {
	d, err := controlbudget.After(3 * time.Second)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	defer cancel()
	var resp proto.TransferFinalizeResp
	_ = t.legacyRequest(ctx, proto.SubjCtrlTransferFinalize(t.id.PublicKey, id.SID, id.TransferID), proto.TransferFinalize{Kind: "failed",
		TransferID: id.TransferID, Tier: tier, Code: code, Error: detail, DurationMs: time.Since(started).Milliseconds()}, &resp, d)
}

// PullPrepareFailureNeedsFinalize: a refused pull prepare was already tracked
// by the broker (finalize it), except a duplicate transfer ID, which was
// rejected before insertion — finalizing it would end the ORIGINAL pull.
func PullPrepareFailureNeedsFinalize(code string) bool {
	return code != "transfer_id_in_flight"
}

func legacyRefusal(code, detail string) (x.LegacyResult, error) {
	return x.LegacyResult{Kind: "failed", Code: code, Error: detail, BestEffort: true}, nil
}

// Retire legacy byte attachments on the same bounded command cleanup path.
func StopLegacyTransfer(owner xferclient.LegacyInvocation) error {
	return stopTransferOwner(owner)
}
