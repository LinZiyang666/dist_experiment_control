package xferclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Legacy is selected by the caller only after explicit precreation cap0
// negotiation. It has no permit, checkpoint, exact receipt or resume interface.
// origin: hot-update S7 plan §7 / §9 / M01
type Legacy struct {
	cfg             LegacyConfig
	gate            chan struct{}
	called, stopped bool
	result          x.LegacyResult
	err             error
}

var _ LegacyInvocation = (*Legacy)(nil)
var _ LegacyFactory = NewLegacy

func NewLegacy(cfg LegacyConfig) (LegacyInvocation, error) {
	if cfg.TransferID == "" || cfg.PutOnce == nil {
		return nil, clientError(x.CodePayloadUnsupported, x.Identity{TransferID: cfg.TransferID}, x.PhaseOpen, false, errors.New("legacy requires an original ID and a one-shot operation"))
	}
	l := &Legacy{cfg: cfg, gate: make(chan struct{}, 1)}
	l.gate <- struct{}{}
	return l, nil
}

// Refuse a known local conflict before prepare creates remote work. Once
// sent, the one-shot legacy owner must conservatively report uncertainty.
// The atomic no-clobber primitive still arbitrates later destination races.
// origin: S7 internal review MAKETEST-TRANSFER F9; S7 plan §7 / §9 / T40
func NewLegacyLocal(cfg LegacyConfig, local string) (LegacyInvocation, error) {
	if cfg.Intent.Verb == "pull" && !cfg.Intent.Force {
		if _, err := os.Lstat(local); err == nil {
			return nil, &Error{Code: "dst_exists", TransferID: cfg.TransferID, Cause: errors.New("destination exists; pass --force to overwrite")}
		}
	}
	return NewLegacy(cfg)
}

// NewLegacyObject keeps the only SDK operation adapter in the legacy owner.
// The outer CLI calls it only after explicit cap0 negotiation; it binds the
// one-shot jsstream.PutLegacyOnce itself, so the legacy call graph names its
// single SDK site statically (the L1 gate follows it). Its source and JS
// attachment remain borrowed caller resources. This constructor cannot
// produce Invocation or be reached from NewClient.
// origin: hot-update S7 plan §7 / §9 / M01 / T49
func NewLegacyObject(cfg LegacyConfig, js jetstream.JetStream, bucket, key string, source io.Reader) (LegacyInvocation, error) {
	return newLegacyObject(cfg, jsstream.PutLegacyOnce, js, bucket, key, source)
}

// newLegacyObject lets the package's own tests substitute the one-shot
// operation; production has exactly the binding above.
func newLegacyObject(cfg LegacyConfig, put jsstream.LegacyPutFactory, js jetstream.JetStream, bucket, key string, source io.Reader) (LegacyInvocation, error) {
	if put == nil || js == nil || source == nil || bucket == "" || key == "" || key != cfg.TransferID {
		return nil, clientError(x.CodePayloadUnsupported, x.Identity{TransferID: cfg.TransferID}, x.PhaseOpen, false, errors.New("legacy object operation requires the original key and data attachment"))
	}
	cfg.PutOnce = func(deadline x.Deadline) (x.LegacyResult, error) {
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return x.LegacyResult{}, err
		}
		// ctx-none: the one-shot owner receives an absolute deadline.
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		defer cancel()
		var info *jetstream.ObjectInfo
		err = putWithJSWatchdog(ctx, func(pctx context.Context) error {
			var putErr error
			info, putErr = put(js, bucket, key, legacyPutReader{Reader: source, ctx: pctx}, deadline)
			return putErr
		}, func(pctx context.Context) (uint64, error) { return probeJetStreamForBucket(pctx, js, bucket) }, jsPutProbeInterval, jsPutProbeTimeout, jsPutProbeStrikes)
		if err != nil {
			return x.LegacyResult{BestEffort: true}, err
		}
		if info == nil || info.Name != key || info.Bucket != bucket || info.NUID == "" || info.Deleted || info.Size > proto.XferMaxBytes {
			return x.LegacyResult{BestEffort: true}, errors.New("legacy operation returned no matching object metadata")
		}
		digest, err := jetstream.DecodeObjectDigest(info.Digest)
		if err != nil || len(digest) != sha256.Size {
			return x.LegacyResult{BestEffort: true}, errors.New("legacy operation returned an invalid object digest")
		}
		return x.LegacyResult{Kind: "complete", Bytes: int64(info.Size), SHA256: hex.EncodeToString(digest), BestEffort: true}, nil
	}
	return NewLegacy(cfg)
}

func (l *Legacy) Run(deadline x.Deadline) (x.LegacyResult, error) {
	if err := l.acquire(deadline); err != nil {
		return x.LegacyResult{}, err
	}
	defer func() { l.gate <- struct{}{} }()
	if l.called {
		return l.result, l.err
	}
	if l.stopped {
		return x.LegacyResult{}, clientError(x.CodeResultUnknown, x.Identity{TransferID: l.cfg.TransferID}, x.PhaseOpen, false, errors.New("legacy invocation stopped"))
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return x.LegacyResult{}, err
	}
	// Mark before entering the operation: an unknown reply, panic, timeout or
	// a second Run must never invoke SDK Put a second time with a fresh NUID.
	// origin: hot-update S7 plan §7 / T33
	l.called = true
	l.result.BestEffort = true
	l.err = clientError(x.CodeResultUnknown, x.Identity{TransferID: l.cfg.TransferID}, x.PhaseOpen, true, errors.New("legacy operation did not return"))
	l.result, l.err = l.cfg.PutOnce(deadline)
	l.result.BestEffort = true
	if l.err != nil || l.result.Kind == "complete" && (l.result.Error != "" || l.result.Code != "") || l.result.Kind != "complete" && l.result.Kind != "failed" {
		l.err = clientError(x.CodeResultUnknown, x.Identity{TransferID: l.cfg.TransferID}, x.PhaseOpen, true, l.err)
	} else if l.result.Kind == "failed" {
		code := l.result.Code
		if code == "" {
			code = x.CodeResultUnknown
		}
		l.err = clientError(code, x.Identity{TransferID: l.cfg.TransferID}, x.PhaseOpen, false, errors.New("legacy transfer failed: "+l.result.Error))
	}
	return l.result, l.err
}

func (l *Legacy) Stop(deadline x.Deadline) error {
	if err := l.acquire(deadline); err != nil {
		return err
	}
	defer func() { l.gate <- struct{}{} }()
	l.stopped = true
	return nil
}

func (l *Legacy) acquire(deadline x.Deadline) error {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-l.gate:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

// The historical watchdog only cancels and joins this ONE upload. S7 forbids
// the historical retry ladder, even when its diagnostic calls a stall retryable.
// origin: TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes / hot-update S7 plan §7
// The tier-B Put liveness watchdog (gotcha #84). Three consecutive failed probes 10 s apart = 30 s of a
// JetStream that answers nothing while chunks are in flight — long enough to ride out a leader election
// (nats-server's JS meta election completes in seconds), short enough that the operator learns of a
// lost quorum in half a minute instead of at the end of the size budget.
const (
	jsPutProbeInterval = 10 * time.Second
	jsPutProbeTimeout  = 5 * time.Second
	jsPutProbeStrikes  = 3
)

// putWithJSWatchdog runs `put` and, while it is in flight, calls `probe` every `interval` with its own
// `probeTimeout`. The probe answers two questions at once — "does the bucket's stream answer with a
// leader" (an error means no) and "what is its last sequence" — and each question has its own strike
// count:
//   - `strikes` consecutive probe FAILURES cancel the put with stallNoAnswer; a success resets the count;
//   - `strikes` consecutive successful probes whose sequence did not move since the previous reading
//     (the first reading is taken before the put starts) cancel it with stallNoProgress; a sequence that
//     moved resets the count. A slow-but-moving upload advances the sequence by one per 128 KiB chunk,
//     so at the 2 MiB/s admission floor it can never sit still for a whole interval; another writer in
//     the same session bucket can mask a dead upload (its writes move the sequence too), in which case
//     the put simply falls back to its size budget — no worse than before the watchdog.
//
// The put's own result wins whenever it returns first, so a healthy JetStream never sees the watchdog
// act. Injected `put` and `probe` make the ladder table-testable without a NATS connection
// (cmd/tether/transfer_abandon_test.go).
func putWithJSWatchdog(ctx context.Context, put func(context.Context) error, probe func(context.Context) (uint64, error),
	interval, probeTimeout time.Duration, strikes int) error {
	sample := func() (uint64, error) {
		qctx, qcancel := context.WithTimeout(ctx, probeTimeout)
		defer qcancel()
		return probe(qctx)
	}
	// The baseline sequence, read before any chunk leaves: the no-progress count is relative to it.
	lastSeq, err0 := sample()
	haveSeq := err0 == nil
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- put(pctx) }()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	fails, stuck := 0, 0
	var last error
	// cut cancels the put and waits for it to unwind before the store is abandoned — and then reads
	// what the put itself has to say, because a cancelled Put is not always a stalled one:
	//
	//   - nil: the upload was COMPLETE (meta written, every chunk acked) and the Put was sitting in
	//     nats.go's trailing best-effort PURGE of the object's previous chunk generation — the object
	//     name already existed from an earlier attempt. The ctl's ACL denies STREAM.PURGE (by design;
	//     the broker's reaper owns the bucket) and nats.go does not fail a request on a publish
	//     permissions violation, so that purge waits for the context and the sequence sits still. The
	//     first version returned the stall verdict here, so every retry after a genuine stall
	//     re-uploaded the object in full, hung in the same purge, was cut as "no progress" again, and
	//     the push was refused as jetstream_not_ready while the bucket held a complete, correct object
	//     (round-2 review R2-F1). A Put that says nil has done its job; the old chunks are the
	//     reaper's (gotcha #85). The cost that remains is one strike window of latency on that path.
	//   - the put's OWN error: every Put failure after the first chunk runs nats.go's purgePartial,
	//     which ends in the same denied PURGE, so a local read error or a chunk-ack "no response from
	//     stream" is held until the watchdog cuts — and used to be laundered into the no-progress
	//     face (R2-F3). The put's verdict goes to the classifier instead: a chunk-ack refusal is a
	//     genuinely retryable instant face, an I/O error is object_put_failed, neither is JetStream.
	//   - a cancellation artifact (context.Canceled / DeadlineExceeded, or nats.ErrTimeout — what Put
	//     maps ITS context's expiry to): the put learned nothing on its own; the watchdog's verdict
	//     stands.
	cut := func(e *jetStreamStallError) error {
		cancel()
		perr := <-done
		switch {
		case perr == nil:
			return nil
		case errors.Is(perr, context.Canceled), errors.Is(perr, context.DeadlineExceeded), errors.Is(perr, nats.ErrTimeout):
			return e
		default:
			return perr
		}
	}
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			cancel()
			return <-done
		case <-ticker.C:
			seq, perr := sample()
			if perr != nil {
				stuck, haveSeq = 0, false
				fails++
				last = perr
				if fails >= strikes {
					return cut(&jetStreamStallError{kind: stallNoAnswer, strikes: fails, window: time.Duration(fails) * interval, last: last})
				}
				continue
			}
			fails = 0
			if haveSeq && seq == lastSeq {
				stuck++
			} else {
				stuck = 0
			}
			lastSeq, haveSeq = seq, true
			if stuck >= strikes {
				return cut(&jetStreamStallError{kind: stallNoProgress, strikes: stuck, window: time.Duration(stuck) * interval, lastSeq: seq})
			}
		}
	}
}

// probeJetStreamForBucket is the watchdog's liveness question: can THIS bucket's backing stream
// (OBJ_<bucket>) take a chunk right now. Two halves, both read from ONE STREAM.INFO round trip:
//
//  1. the request is answered at all — a JetStream whose meta group has just lost quorum answers nothing
//     (drill 67's injection: the R2 stream's peer is stopped; the ctl-side Put sat its whole budget on
//     images #6/#7, and this probe's failures are what cut it at 30 s on image #9, batch16/67x);
//  2. the answer names a stream LEADER — a meta layer that answers again but with `cluster.leader == ""`
//     cannot commit a publish (no ack: "no response from stream" / "no responders"), so an answered
//     STREAM.INFO with no leader is a probe FAILURE, not health. Recorded evidence for this shape is the
//     drill-67 /jsz sampler on image #8 (batch14/67v, batch15/67w): `leader:null` in the t+0 sample after
//     the peer's return, a leader from ≈t+5 s on. The longer "≈40 s no answer, then 2 ms leaderless
//     answers" sequence that motivated splitting the two halves was seen in an ad-hoc in-process 2-node
//     run on 2026-09-19 whose transcript was NOT saved (round-2 review R5-F3) — it is a design rationale,
//     not a receipt; the predicate itself is pinned by TestProbeJetStreamForBucketRejectsALeaderlessStream.
//     What the same samples ALSO showed is that the post-recovery 120 s stall happened WITH a leader
//     present — which is why this probe alone could not see it and the no-progress rule
//     (stallNoProgress) exists.
//
// It is deliberately ONLY that call: `$JS.API.STREAM.INFO.OBJ_xfer-<sid>` is in the ctl's per-session ACL
// (internal/auth/permissions.go), while `$JS.API.INFO` (AccountInfo) is not — the first deploy-tier run of
// this watchdog probed AccountInfo, every probe was a permissions violation, and a HEALTHY 12 MB push was
// cut after 30 s as "JetStream stopped answering" (drill 67 CONTROL(after) attempt 1, 2026-09-19). A probe
// the ctl is not allowed to make is indistinguishable from a dead JetStream; only the permitted one counts.
// A non-clustered JetStream (single broker) carries no cluster block; there, an answer is health.
// The returned sequence is the stream's last sequence from the same answer — the watchdog's progress
// reading (see stallNoProgress).
func probeJetStreamForBucket(ctx context.Context, js streamLookup, bucket string) (uint64, error) {
	st, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return 0, fmt.Errorf("bucket stream OBJ_%s: %w", bucket, err)
	}
	ci := st.CachedInfo()
	if streamInfoIsLeaderless(ci) {
		return 0, fmt.Errorf("bucket stream OBJ_%s: no stream leader (the API answers, the stream cannot commit)", bucket)
	}
	if ci == nil {
		return 0, nil
	}
	return ci.State.LastSeq, nil
}

// streamLookup is the ONE JetStream method the probe uses. jetstream.JetStream satisfies it; naming it
// lets a test hand the probe a stream handle the embedded server cannot produce — a clustered stream
// with no leader — so the probe's second half is pinned through the probe itself, not only through the
// predicate below (round-2 review R4-2-F1: disconnecting the two stayed green).
type streamLookup interface {
	Stream(ctx context.Context, name string) (jetstream.Stream, error)
}

// streamInfoIsLeaderless is the probe's second half as a predicate: a clustered stream whose info names no
// leader. A nil info or a missing cluster block (standalone JetStream) is NOT leaderless.
func streamInfoIsLeaderless(ci *jetstream.StreamInfo) bool {
	return ci != nil && ci.Cluster != nil && ci.Cluster.Leader == ""
}

// legacyPutReader carries cancellation to the single SDK adapter without
// changing its frozen deadline signature or granting a retry/resume interface.
// origin: TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes / hot-update S7 plan §7
type legacyPutReader struct {
	io.Reader
	ctx context.Context
}

func (r legacyPutReader) LegacyPutContext() context.Context { return r.ctx }
