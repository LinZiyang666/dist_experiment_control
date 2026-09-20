package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: simcluster-speed 0a (drill 67): a tier-B push whose upload failed before commit must tell
// the broker so, or the session's bucket stays held for the whole watchdog budget.

func startJetStreamNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// TestPushTierBAbandonSendsFailedFinalize plays the broker from inside the test: prepare is answered
// OK, but no bucket is ever created, so the ctl's ObjectStore bind fails after prepare — exactly the
// "slot held, nobody to release it" shape. The ctl must then publish finalize{failed} for that
// transfer id before returning its error. Deleting the abandon() calls in pushTierB leaves the
// finalize subscription empty and this test red.
func TestPushTierBAbandonSendsFailedFinalize(t *testing.T) {
	url := startJetStreamNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const actor, sid, nid = "UACTORPUBKEYFORTEST", "lab", "lab-1"
	// The broker half: answer push prepare OK (bucket "exists" as far as the reply says).
	prepSub, err := nc.Subscribe(proto.SubjCmdBy(sid, actor, nid, "push"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.PushPrepareResp{OK: true, Tier: "b"})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepSub.Unsubscribe() }()
	// ...and record what the ctl says on the finalize subject.
	fins := make(chan proto.TransferFinalize, 4)
	finSub, err := nc.Subscribe(proto.SubjCtrlTransferFinalize(actor, sid, "*"), func(m *nats.Msg) {
		var fin proto.TransferFinalize
		_ = json.Unmarshal(m.Data, &fin)
		fins <- fin
		body, _ := json.Marshal(proto.TransferFinalizeResp{OK: true})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finSub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	local := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(local, []byte(strings.Repeat("x", 4096)), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetOut(&strings.Builder{})
	tid := newTransferID()
	err = pushTierB(cmd, nc, actor, sid, remoteSpec{Node: nid, Path: "/tmp/payload.bin"}, tid, local, 4096, false, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "bind bucket") {
		t.Fatalf("expected the bucket bind to fail (no bucket was created), got %v", err)
	}

	select {
	case fin := <-fins:
		if fin.Kind != "failed" || fin.TransferID != tid || fin.Tier != "b" {
			t.Fatalf("finalize = %+v, want kind=failed transfer_id=%s tier=b", fin, tid)
		}
		if fin.Code != "bucket_bind_failed" {
			t.Fatalf("finalize code = %q, want bucket_bind_failed", fin.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ctl abandoned its upload without telling the broker (no finalize{failed} within 5s)")
	}
}

// TestPhaseTimeoutForDerivesFromSize pins the per-phase bound: the flag's own words ("derived from
// the file size") now hold — a small transfer gets the floor-plus-margin, the maximum size gets the
// old flat default, and an explicit --timeout wins whatever the size.
func TestPhaseTimeoutForDerivesFromSize(t *testing.T) {
	mk := func(changed bool, v time.Duration) *cobra.Command {
		c := &cobra.Command{}
		var d time.Duration
		c.Flags().DurationVar(&d, "timeout", cliTransferTimeoutDefault, "")
		if changed {
			if err := c.Flags().Set("timeout", v.String()); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}
	small := phaseTimeoutFor(mk(false, 0), 12_000_000, cliTransferTimeoutDefault)
	if want := proto.XferTimeoutTierBFloor + 2*time.Minute; small != want {
		t.Fatalf("12 MB phase timeout = %s, want floor+margin %s", small, want)
	}
	if small >= cliTransferTimeoutDefault {
		t.Fatalf("a 12 MB push must not inherit the 2 GiB worst case (%s)", cliTransferTimeoutDefault)
	}
	maxSize := phaseTimeoutFor(mk(false, 0), proto.XferMaxBytes, cliTransferTimeoutDefault)
	if maxSize != cliTransferTimeoutDefault {
		t.Fatalf("max-size phase timeout = %s, want the flat default %s (never tighter than the broker)", maxSize, cliTransferTimeoutDefault)
	}
	explicit := phaseTimeoutFor(mk(true, 90*time.Second), 12_000_000, 90*time.Second)
	if explicit != 90*time.Second {
		t.Fatalf("an explicit --timeout must win, got %s", explicit)
	}
}

// origin: gotcha #84 (simcluster-speed internal review round 1 R1-F1 / plan X27). The Put-leg watchdog: a
// put that finishes wins (its own result, success or failure, even while probes fail); `strikes`
// consecutive probe failures — not fewer, and not a single failure surrounded by successes — cancel the
// put with stallNoAnswer; a recovered probe resets the count. The second half (image #8 receipt): `strikes`
// consecutive healthy probes whose sequence did not move cancel it with stallNoProgress; a moving sequence
// resets that count, so a slow-but-landing upload is never cut. Injected put/probe fakes, millisecond
// intervals, no NATS.
func TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes(t *testing.T) {
	blockUntilCancelled := func(c context.Context) error { <-c.Done(); return c.Err() }
	failing := func(context.Context) (uint64, error) { return 0, errors.New("no leader") }
	// advancing: a healthy stream taking the upload — the sequence moves on every reading.
	advancing := func() func(context.Context) (uint64, error) {
		var n uint64
		return func(context.Context) (uint64, error) { n++; return n, nil }
	}
	// stuck: a healthy-looking stream that accepts nothing — leader present, sequence frozen.
	stuck := func(context.Context) (uint64, error) { return 94, nil }
	slowPut := func(d time.Duration) func(context.Context) error {
		return func(c context.Context) error {
			select {
			case <-time.After(d):
				return nil
			case <-c.Done():
				return errors.New("cut by the watchdog")
			}
		}
	}

	t.Run("healthy JS: a slow put is never cut", func(t *testing.T) {
		if err := putWithJSWatchdog(context.Background(), slowPut(120*time.Millisecond), advancing(), 10*time.Millisecond, 5*time.Millisecond, 3); err != nil {
			t.Fatalf("healthy, advancing probes must never cancel a put: %v", err)
		}
	})
	t.Run("stalled JS: cancelled after exactly `strikes` failed probes", func(t *testing.T) {
		probes := 0
		probe := func(c context.Context) (uint64, error) { probes++; return failing(c) }
		start := time.Now()
		err := putWithJSWatchdog(context.Background(), blockUntilCancelled, probe, 20*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if !errors.As(err, &stall) || stall.kind != stallNoAnswer {
			t.Fatalf("want a stallNoAnswer *jetStreamStallError, got %T %v", err, err)
		}
		// probes = 1 baseline reading + 3 failed samples.
		if probes != 4 || stall.strikes != 3 {
			t.Fatalf("probes=%d strikes=%d, want 4/3 (baseline + cancel on the third consecutive failure, no more)", probes, stall.strikes)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("took %s — the put was not cancelled by the watchdog", took)
		}
		if !strings.Contains(err.Error(), "3 probes over") || !errors.Is(err, stall.last) {
			t.Fatalf("stall error must carry the count, the window and the last probe error: %v", err)
		}
	})
	t.Run("a recovered probe resets the count: fail,ok,fail,ok,fail → still running at strikes=2", func(t *testing.T) {
		adv := advancing()
		seq := []bool{true, false, true, false, true} // true = failing; never two in a row
		i := 0
		probe := func(c context.Context) (uint64, error) {
			if i < len(seq) {
				f := seq[i]
				i++
				if f {
					return failing(c)
				}
			}
			return adv(c) // healthy from here on; the put decides
		}
		// strikes=2 here (the production value is 3): the reset is what is under test, and a threshold
		// other than the production constant keeps the parameter a real parameter.
		if err := putWithJSWatchdog(context.Background(), slowPut(200*time.Millisecond), probe, 15*time.Millisecond, 5*time.Millisecond, 2); err != nil {
			t.Fatalf("alternating failure and success must never reach two consecutive strikes: %v", err)
		}
	})
	t.Run("healthy but frozen: cancelled with stallNoProgress after exactly `strikes` still readings", func(t *testing.T) {
		probes := 0
		probe := func(c context.Context) (uint64, error) { probes++; return stuck(c) }
		start := time.Now()
		// A bounded context: a watchdog that never notices the frozen sequence would otherwise block this
		// subtest on the never-returning put until the package timeout (the mutation reads as a hang).
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := putWithJSWatchdog(ctx, blockUntilCancelled, probe, 20*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if !errors.As(err, &stall) || stall.kind != stallNoProgress {
			t.Fatalf("want a stallNoProgress *jetStreamStallError, got %T %v", err, err)
		}
		if probes != 4 || stall.strikes != 3 || stall.lastSeq != 94 {
			t.Fatalf("probes=%d strikes=%d lastSeq=%d, want 4/3/94 (baseline + three unmoved readings)", probes, stall.strikes, stall.lastSeq)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("took %s — the put was not cancelled by the watchdog", took)
		}
		if !strings.Contains(err.Error(), "accepted none of the upload") || !strings.Contains(err.Error(), "last_seq stuck at 94") {
			t.Fatalf("no-progress error must say so and carry the frozen sequence: %v", err)
		}
	})
	t.Run("a moving sequence resets the no-progress count: still,still,moved,still,still → still running", func(t *testing.T) {
		seqs := []uint64{94, 94, 94, 95, 95, 95} // baseline 94; two still; moved; two still — never three in a row
		i, n := 0, uint64(1000)
		probe := func(context.Context) (uint64, error) {
			if i < len(seqs) {
				s := seqs[i]
				i++
				return s, nil
			}
			n++
			return n, nil // moving from here on; the put decides
		}
		if err := putWithJSWatchdog(context.Background(), slowPut(200*time.Millisecond), probe, 15*time.Millisecond, 5*time.Millisecond, 3); err != nil {
			t.Fatalf("a sequence that moves every third reading must not reach three still strikes: %v", err)
		}
	})
	t.Run("the put's own failure wins over a probe failure in flight", func(t *testing.T) {
		putErr := errors.New("object too large")
		put := func(context.Context) error { return putErr }
		err := putWithJSWatchdog(context.Background(), put, failing, 10*time.Millisecond, 5*time.Millisecond, 3)
		if !errors.Is(err, putErr) {
			t.Fatalf("want the put's own error, got %v", err)
		}
	})
	t.Run("caller ctx cancelled: the put's ctx error comes back, not a stall", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(30 * time.Millisecond); cancel() }()
		err := putWithJSWatchdog(ctx, blockUntilCancelled, advancing(), 10*time.Millisecond, 5*time.Millisecond, 3)
		var stall *jetStreamStallError
		if errors.As(err, &stall) || !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled from the put, got %v", err)
		}
	})
}

// TestPutWithJSRetryRetriesOnlyTheInstantJetStreamFaces pins the bounded retry around the Put (gotcha #84,
// instant face, image #7 receipt: the injected push failed in 0.9 s with `Put: nats: no responders` and
// the watchdog — built for the stall face — never got to act). Table: which errors are retried, how many
// times, that the wait doubles and honours the context, and that the refusal carries count + window + face.
// origin: simcluster-speed drill 67 receipt on image #7 (#84 instant face)
func TestPutWithJSRetryRetriesOnlyTheInstantJetStreamFaces(t *testing.T) {
	transientFace := func(_ context.Context, err error) (string, bool) {
		if errors.Is(err, nats.ErrNoResponders) {
			return "no responders", true
		}
		return "", false
	}
	var slept []time.Duration
	recordSleep := func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }

	t.Run("transient every time: exactly `attempts` puts, doubling waits, then a refusal", func(t *testing.T) {
		slept = nil
		puts := 0
		put := func(context.Context) error { puts++; return nats.ErrNoResponders }
		err := putWithJSRetry(context.Background(), put, transientFace, 3, 3*time.Second, recordSleep)
		var refused *jetStreamRefusedError
		if !errors.As(err, &refused) {
			t.Fatalf("want *jetStreamRefusedError, got %T %v", err, err)
		}
		if puts != 3 || refused.attempts != 3 {
			t.Fatalf("puts=%d attempts=%d, want 3/3", puts, refused.attempts)
		}
		if len(slept) != 2 || slept[0] != 3*time.Second || slept[1] != 6*time.Second {
			t.Fatalf("waits = %v, want [3s 6s] (doubling, none after the last attempt)", slept)
		}
		if !strings.Contains(err.Error(), "refused 3 attempt(s) over") || !strings.Contains(err.Error(), "no responders") || !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("refusal must carry the count, the window and the last face, and unwrap to the cause: %v", err)
		}
	})
	t.Run("transient then healthy: the second attempt's success is the result", func(t *testing.T) {
		slept = nil
		puts := 0
		put := func(context.Context) error {
			puts++
			if puts == 1 {
				return nats.ErrNoResponders
			}
			return nil
		}
		if err := putWithJSRetry(context.Background(), put, transientFace, 3, time.Second, recordSleep); err != nil || puts != 2 {
			t.Fatalf("err=%v puts=%d, want nil/2", err, puts)
		}
	})
	t.Run("a non-transient error is returned at once, never retried into", func(t *testing.T) {
		slept = nil
		puts := 0
		stall := &jetStreamStallError{kind: stallNoAnswer, strikes: 3, window: 30 * time.Second, last: errors.New("no leader")}
		put := func(context.Context) error { puts++; return stall }
		err := putWithJSRetry(context.Background(), put, transientFace, 3, 3*time.Second, recordSleep)
		if !errors.Is(err, stall) || puts != 1 || len(slept) != 0 {
			t.Fatalf("a watchdog stall must come back untouched on the first attempt: err=%v puts=%d slept=%v", err, puts, slept)
		}
	})
	t.Run("a no-progress stall on the last attempt: the refusal wraps it and carries its face", func(t *testing.T) {
		// The production classifier, not the local stub: this pins that stallNoProgress IS retried and
		// that the refusal's `last` names the frozen sequence (what the operator and drill 67 read).
		slept = nil
		puts := 0
		stall := &jetStreamStallError{kind: stallNoProgress, strikes: 3, window: 30 * time.Second, lastSeq: 94}
		put := func(context.Context) error { puts++; return stall }
		err := putWithJSRetry(context.Background(), put, jetStreamUnavailableFace, 2, 3*time.Second, recordSleep)
		var refused *jetStreamRefusedError
		if !errors.As(err, &refused) || puts != 2 || refused.attempts != 2 || !strings.Contains(refused.last, "last_seq stuck at 94") {
			t.Fatalf("a no-progress stall must be retried and refused with its face: err=%v puts=%d", err, puts)
		}
		if !errors.As(err, &stall) {
			t.Fatalf("the refusal must unwrap to the stall it is made of: %v", err)
		}
	})
	t.Run("context expires during the wait: refusal with the attempts so far, no further put", func(t *testing.T) {
		puts := 0
		put := func(context.Context) error { puts++; return nats.ErrNoResponders }
		ctx, cancel := context.WithCancel(context.Background())
		sleep := func(c context.Context, _ time.Duration) error { cancel(); return c.Err() }
		err := putWithJSRetry(ctx, put, transientFace, 3, 3*time.Second, sleep)
		var refused *jetStreamRefusedError
		if !errors.As(err, &refused) || refused.attempts != 1 || puts != 1 {
			t.Fatalf("want a refusal after 1 attempt when the context dies in the wait, got %v (puts=%d)", err, puts)
		}
	})
}

// TestJetStreamUnavailableFaceClassifiesPutErrors pins the classifier: the four JetStream "not available"
// answers are transient; the phase budget's own expiry (Put maps it to nats.ErrTimeout with the context
// already done), a cancel, and an I/O error are not.
// origin: simcluster-speed drill 67 receipt on image #7 (#84 instant face)
func TestJetStreamUnavailableFaceClassifiesPutErrors(t *testing.T) {
	live := context.Background()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"no responders (Put's object lookup)", live, nats.ErrNoResponders, true},
		{"wrapped no responders", live, fmt.Errorf("Put: %w", nats.ErrNoResponders), true},
		{"no response from stream (chunk acks)", live, jetstream.ErrNoStreamResponse, true},
		{"API 503", live, &jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}, true},
		{"nats timeout with the budget still alive (an API request's own clock)", live, nats.ErrTimeout, true},
		{"nats timeout with the budget spent (Put's mapping of ctx expiry)", expired, nats.ErrTimeout, false},
		{"watchdog: no progress on a healthy stream (chunks dropped in a leader election)", live, &jetStreamStallError{kind: stallNoProgress, strikes: 3, lastSeq: 94}, true},
		{"watchdog: no answer (JetStream down)", live, &jetStreamStallError{kind: stallNoAnswer, strikes: 3, last: errors.New("no leader")}, false},
		{"API 404 stream not found", live, jetstream.ErrStreamNotFound, false},
		{"cancelled", live, context.Canceled, false},
		{"I/O error", live, errors.New("read /tmp/x: input/output error"), false},
		{"nil", live, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			face, got := jetStreamUnavailableFace(c.ctx, c.err)
			if got != c.want {
				t.Fatalf("transient = %v (face %q), want %v", got, face, c.want)
			}
			if got && face == "" {
				t.Fatal("a transient classification must name its face")
			}
		})
	}
}

// TestProbeJetStreamForBucketRejectsALeaderlessStream pins the second half of the probe: an answered
// STREAM.INFO whose cluster block names no leader is a FAILURE. Measured hermetically on a 2-node R2 store
// (2026-09-19): ≈40 s after the peer stops the meta layer answers STREAM.INFO in 2 ms with
// `cluster.leader == ""` while every publish gets no ack — the state in which the first probe version let
// a Put sit its whole --timeout (drill 67 CONTROL(after) attempt 1 on image #7, 121 s). The embedded
// single-node server here cannot produce a clustered leaderless stream, so the check is exercised
// through the same predicate the probe applies to the cached info, against both shapes.
// origin: simcluster-speed drill 67 receipt on image #7 (#84 post-recovery face)
func TestProbeJetStreamForBucketRejectsALeaderlessStream(t *testing.T) {
	url := startJetStreamNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-leader"}); err != nil {
		t.Fatal(err)
	}
	// Single node, no cluster block: an answer is health — and the reading is the stream's last sequence,
	// which the watchdog's progress half compares between probes (an object store that just received
	// nothing reads 0; after one PutBytes it reads the number of messages the Put wrote).
	seq0, err := probeJetStreamForBucket(ctx, js, "xfer-leader")
	if err != nil {
		t.Fatalf("a standalone JetStream that answers is healthy: %v", err)
	}
	store, err := js.ObjectStore(ctx, "xfer-leader")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutBytes(ctx, "obj", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	seq1, err := probeJetStreamForBucket(ctx, js, "xfer-leader")
	if err != nil {
		t.Fatalf("probe after a put: %v", err)
	}
	if seq1 <= seq0 {
		t.Fatalf("the probe's reading must move when the stream takes a write: %d → %d", seq0, seq1)
	}
	// A bucket that does not exist: the API answers 404 — that is a failure too (the watchdog's caller
	// bound the bucket before the Put, so this only happens if it vanished underneath).
	if _, err := probeJetStreamForBucket(ctx, js, "xfer-missing"); err == nil {
		t.Fatal("a missing bucket stream must fail the probe")
	}
	// The leaderless predicate itself, on the info shapes a clustered server returns — AND the probe
	// applying it, through a stream handle the embedded server cannot produce. Both are needed: the
	// predicate table is red when the predicate lies, the probe table is red when the probe stops
	// asking it (round-2 review R4-2-F1: `if false && streamInfoIsLeaderless(ci)` stayed green).
	for _, c := range []struct {
		name string
		info *jetstream.StreamInfo
		want bool
	}{
		{"no cluster block (standalone)", &jetstream.StreamInfo{State: jetstream.StreamState{LastSeq: 7}}, false},
		{"cluster block with a leader", &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: "brk1"}, State: jetstream.StreamState{LastSeq: 7}}, false},
		{"cluster block, leader empty", &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: ""}, State: jetstream.StreamState{LastSeq: 7}}, true},
	} {
		if got := streamInfoIsLeaderless(c.info); got != c.want {
			t.Fatalf("%s: leaderless = %v, want %v", c.name, got, c.want)
		}
		seq, err := probeJetStreamForBucket(ctx, fakeStreamLookup{info: c.info}, "xfer-fake")
		if c.want {
			if err == nil || !strings.Contains(err.Error(), "no stream leader") {
				t.Fatalf("%s: the probe must FAIL on a leaderless answer, got seq=%d err=%v", c.name, seq, err)
			}
			continue
		}
		if err != nil || seq != 7 {
			t.Fatalf("%s: the probe must return the answer's last_seq (7), got seq=%d err=%v", c.name, seq, err)
		}
	}
}

// fakeStreamLookup hands probeJetStreamForBucket a stream handle with a scripted cached info. The
// embedded jetstream.Stream is nil: only CachedInfo is overridden, any other call panics — which is
// the point, the probe must not need anything else.
type fakeStreamLookup struct{ info *jetstream.StreamInfo }

func (f fakeStreamLookup) Stream(context.Context, string) (jetstream.Stream, error) {
	return fakeStream{info: f.info}, nil
}

type fakeStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (f fakeStream) CachedInfo() *jetstream.StreamInfo { return f.info }

// origin: simcluster-speed internal review round 1 R6-F4 / R2-F3. The upload succeeded, the commit
// request's reply was lost (nobody answers push-commit here): the ctl must still tell the broker it gave
// up — the broker arbitrates by its committed flag (verb_mismatch if it had forwarded, slot freed if it
// never saw the commit), so sending is always safe and not sending held the slot to the full budget.
func TestPushTierBCommitLostStillSendsFailedFinalize(t *testing.T) {
	// push-commit has no subscriber at all — the request times out, which is the lost-reply shape.
	f := startCommitPathFixture(t, nil)
	err := f.push()
	if err == nil || !strings.Contains(err.Error(), "tier B commit") {
		t.Fatalf("expected the commit request to fail (no responder), got %v", err)
	}
	f.expectFailedFinalize("commit_lost")
}

// TestPushTierBCommitRefusedSendsFailedFinalize is the third commit-path abandon: the broker answered
// push-commit with a refusal (tier_invalid, not_owner_or_creator …) INSTEAD of forwarding it. The entry
// is not committed, so the ctl must free the slot with finalize{failed, <the refusal code>} — round-2
// review R4-2-F9 deleted that abandon with every test green (only the lost-reply sibling was pinned).
// origin: simcluster-speed review round 2 R4-2-F9
func TestPushTierBCommitRefusedSendsFailedFinalize(t *testing.T) {
	f := startCommitPathFixture(t, func(m *nats.Msg) {
		body, _ := json.Marshal(proto.TransferCommitResp{OK: false, Code: "tier_invalid", Error: "entry downgraded to tier a"})
		_ = m.Respond(body)
	})
	err := f.push()
	if err == nil || !strings.Contains(err.Error(), "commit refused: code=tier_invalid") {
		t.Fatalf("expected the refusal to surface, got %v", err)
	}
	f.expectFailedFinalize("tier_invalid")
}

// commitPathFixture plays the broker for pushTierB up to the commit: prepare OK, the session bucket
// really exists (so the Put succeeds), every finalize is recorded, and push-commit is answered by
// `commit` — nil for "nobody answers".
type commitPathFixture struct {
	t    *testing.T
	nc   *nats.Conn
	fins chan proto.TransferFinalize
	tid  string
	push func() error
}

const commitFixtureActor, commitFixtureSID, commitFixtureNID = "UACTORPUBKEYFORTEST", "lab", "lab-1"

func startCommitPathFixture(t *testing.T, commit nats.MsgHandler) *commitPathFixture {
	t.Helper()
	url := startJetStreamNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName(commitFixtureSID)}); err != nil {
		t.Fatal(err)
	}
	subscribe := func(subj string, h nats.MsgHandler) {
		sub, err := nc.Subscribe(subj, h)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	subscribe(proto.SubjCmdBy(commitFixtureSID, commitFixtureActor, commitFixtureNID, "push"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.PushPrepareResp{OK: true, Tier: "b"})
		_ = m.Respond(body)
	})
	f := &commitPathFixture{t: t, nc: nc, fins: make(chan proto.TransferFinalize, 4), tid: newTransferID()}
	subscribe(proto.SubjCtrlTransferFinalize(commitFixtureActor, commitFixtureSID, "*"), func(m *nats.Msg) {
		var fin proto.TransferFinalize
		_ = json.Unmarshal(m.Data, &fin)
		f.fins <- fin
		body, _ := json.Marshal(proto.TransferFinalizeResp{OK: true})
		_ = m.Respond(body)
	})
	if commit != nil {
		subscribe(proto.SubjCmdBy(commitFixtureSID, commitFixtureActor, commitFixtureNID, "push-commit"), commit)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(local, []byte(strings.Repeat("y", 4096)), 0o600); err != nil {
		t.Fatal(err)
	}
	f.push = func() error {
		cmd := &cobra.Command{}
		cmd.SetContext(context.Background())
		cmd.SetOut(&strings.Builder{})
		return pushTierB(cmd, nc, commitFixtureActor, commitFixtureSID, remoteSpec{Node: commitFixtureNID, Path: "/tmp/payload.bin"}, f.tid, local, 4096, false, 2*time.Second)
	}
	return f
}

func (f *commitPathFixture) expectFailedFinalize(code string) {
	f.t.Helper()
	select {
	case fin := <-f.fins:
		if fin.Kind != "failed" || fin.TransferID != f.tid || fin.Code != code {
			f.t.Fatalf("finalize = %+v, want kind=failed transfer_id=%s code=%s", fin, f.tid, code)
		}
	case <-time.After(5 * time.Second):
		f.t.Fatalf("the broker was left untold (no finalize{failed,%s} within 5s)", code)
	}
}

// TestProbeJetStreamForBucketStaysInsideTheCtlACL runs the Put watchdog's probe as a client that holds
// EXACTLY the activated-member permissions (internal/auth.PermissionsForActivatedMember) against an
// embedded JetStream that enforces them. The probe must answer "healthy" for the session's own bucket:
// a probe the ctl is not allowed to make fails on a HEALTHY JetStream and reads as a stall, so the
// watchdog would cut every push longer than strikes×interval. That is not hypothetical — the first
// deploy-tier run of the watchdog probed AccountInfo (`$JS.API.INFO`, not in the ACL) and cut a healthy
// 12 MB push after 30 s (drill 67 CONTROL(after) attempt 1, 2026-09-19). The negative control inside the
// test proves the fixture really denies `$JS.API.INFO`; re-adding AccountInfo to the probe turns it red.
// origin: simcluster-speed review round 1 deploy receipt (drill 67, image #6)
func TestProbeJetStreamForBucketStaysInsideTheCtlACL(t *testing.T) {
	f := startCtlACLBucket(t, "probesid")
	ctx := f.ctx

	// Negative control: the fixture must really deny the account-wide API, otherwise a probe that
	// strayed outside the per-session grants would pass here and fail only in production. Note the
	// SHAPE of the denial: nats.go does not fail a request on a publish permissions violation (the
	// error only reaches the async handler, processTransientError), so the forbidden request simply
	// waits out its context — which is exactly how a wrong probe becomes a "stall" of probeTimeout.
	// It therefore gets its own short context so that it cannot consume the probe's budget below.
	nctx, ncancel := context.WithTimeout(ctx, 2*time.Second)
	_, negErr := f.cjs.AccountInfo(nctx)
	ncancel()
	if negErr == nil {
		t.Fatal("negative control: the activated-member ACL let $JS.API.INFO through; the fixture proves nothing")
	}
	sawViolation := false
	for _, e := range f.drainAsyncErrs() {
		if strings.Contains(e, "$JS.API.INFO") {
			sawViolation = true
		}
	}
	if !sawViolation {
		t.Fatalf("negative control: AccountInfo failed (%v) but not as a permissions violation on $JS.API.INFO", negErr)
	}

	if _, err := probeJetStreamForBucket(ctx, f.cjs, f.bucket); err != nil {
		t.Fatalf("probe against a healthy bucket, holding only the ctl's own ACL, failed: %v", err)
	}
	// A probe that succeeded by luck while still tripping a permissions violation (e.g. a second,
	// forbidden half whose failure was swallowed) is the same defect one code path away.
	time.Sleep(200 * time.Millisecond)
	if errs := f.drainAsyncErrs(); len(errs) != 0 {
		t.Fatalf("the probe tripped async errors on the ctl connection: %q", errs)
	}
}

// ctlACLBucket is an embedded JetStream that enforces EXACTLY the activated-member permissions
// (internal/auth.PermissionsForActivatedMember) on a "ctl" user, with the session's bucket created by an
// unrestricted admin — the fixture every ACL-sensitive tier-B test shares. drainAsyncErrs returns and
// clears the permissions violations the ctl connection has collected (nats.go surfaces them there, not
// as request errors).
type ctlACLBucket struct {
	ctx    context.Context
	bucket string
	cjs    jetstream.JetStream
	store  jetstream.ObjectStore
	mu     sync.Mutex
	errs   []string
}

func (f *ctlACLBucket) drainAsyncErrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.errs
	f.errs = nil
	return out
}

func startCtlACLBucket(t *testing.T, sid string) *ctlACLBucket {
	t.Helper()
	perms := auth.PermissionsForActivatedMember("probeactor", sid, true)
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.Users = []*server.User{
		{Username: "admin", Password: "admin"},
		{Username: "ctl", Password: "ctl", Permissions: &server.Permissions{
			Publish:   &server.SubjectPermission{Allow: []string(perms.Pub.Allow), Deny: []string(perms.Pub.Deny)},
			Subscribe: &server.SubjectPermission{Allow: []string(perms.Sub.Allow), Deny: []string(perms.Sub.Deny)},
		}},
	}
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	f := &ctlACLBucket{ctx: ctx, bucket: "xfer-" + sid}

	admin, err := nats.Connect(ns.ClientURL(), nats.UserInfo("admin", "admin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	ajs, err := jetstream.New(admin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ajs.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: f.bucket}); err != nil {
		t.Fatalf("create bucket as admin: %v", err)
	}
	ctl, err := nats.Connect(ns.ClientURL(), nats.UserInfo("ctl", "ctl"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			f.mu.Lock()
			f.errs = append(f.errs, err.Error())
			f.mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctl.Close)
	if f.cjs, err = jetstream.New(ctl); err != nil {
		t.Fatal(err)
	}
	if f.store, err = f.cjs.ObjectStore(ctx, f.bucket); err != nil {
		t.Fatal(err)
	}
	return f
}

// The production ladder with fast clocks: putWithJSRetry over putWithJSWatchdog over the real probe,
// against the ACL fixture's store. `put` is the Put body; attempts counts the ladder's calls.
func runPutLadder(f *ctlACLBucket, put func(context.Context) error) (attempts int, err error) {
	err = putWithJSRetry(f.ctx, func(c context.Context) error {
		attempts++
		return putWithJSWatchdog(c, put, func(c context.Context) (uint64, error) {
			return probeJetStreamForBucket(c, f.cjs, f.bucket)
		}, 100*time.Millisecond, time.Second, 3)
	}, jetStreamUnavailableFace, 3, 10*time.Millisecond, sleepCtx)
	return attempts, err
}

// Under the ctl's ACL a Put whose object NAME already exists (a previous attempt's meta landed) ends
// in nats.go's purge of the OLD chunk group — denied, and held until the context ends, so the
// watchdog cuts it as "no progress" with the upload COMPLETE. The cut must then honour the Put's nil:
// before it did, every retry after a genuine stall re-uploaded, hung in the same purge, and the push
// was refused as jetstream_not_ready while the bucket held a correct object.
// origin: simcluster-speed review round 2 R2-F1
func TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge(t *testing.T) {
	f := startCtlACLBucket(t, "purgesid")
	payload := []byte(strings.Repeat("y", 300*1024)) // 3 chunks
	put := func(c context.Context) error {
		_, err := f.store.Put(c, jetstream.ObjectMeta{Name: "T"}, strings.NewReader(string(payload)))
		return err
	}
	// Attempt "1": fresh name, no trailing purge, returns on its own.
	if attempts, err := runPutLadder(f, put); err != nil || attempts != 1 {
		t.Fatalf("fresh-name put: attempts=%d err=%v", attempts, err)
	}
	// Attempt "2": same name. The Put completes and then sits in the denied purge; the watchdog cuts
	// it after 3 unmoved probes, reads the Put's nil, and the ladder reports success in ONE attempt.
	start := time.Now()
	attempts, err := runPutLadder(f, put)
	if err != nil {
		t.Fatalf("existing-name put through the ladder: %v (after %s)", err, time.Since(start).Round(time.Millisecond))
	}
	if attempts != 1 {
		t.Fatalf("existing-name put took %d attempts, want 1 (a re-upload means the cut did not honour the Put's nil)", attempts)
	}
	got, err := f.store.GetBytes(f.ctx, "T")
	if err != nil || string(got) != string(payload) {
		t.Fatalf("object after the cut: err=%v identical=%v", err, string(got) == string(payload))
	}
	// The denied purge is the expected, harmless residue — and the proof this test exercised the path.
	var sawPurge bool
	for _, e := range f.drainAsyncErrs() {
		if strings.Contains(e, "$JS.API.STREAM.PURGE.") {
			sawPurge = true
		}
	}
	if !sawPurge {
		t.Fatal("no STREAM.PURGE permissions violation was recorded: the fixture did not take the existing-name path")
	}
}

// failAfterFirstChunk hands out one chunk and then fails with a local error — a disk going bad under
// the upload.
type failAfterFirstChunk struct {
	n    int
	fail error
}

func (r *failAfterFirstChunk) Read(p []byte) (int, error) {
	if r.n > 0 {
		return 0, r.fail
	}
	r.n++
	for i := range p {
		p[i] = 'z'
	}
	return len(p), nil
}

// Every Put failure after the first chunk runs nats.go's purgePartial, which ends in the same denied
// PURGE, so the Put's OWN error is held until the watchdog cuts. The cut must return that error to the
// classifier — a local I/O error is object_put_failed, not three retries and a jetstream_not_ready
// blaming a healthy JetStream.
// origin: simcluster-speed review round 2 R2-F3
func TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge(t *testing.T) {
	f := startCtlACLBucket(t, "ioerrsid")
	ioErr := errors.New("read /dev/sdb: input/output error")
	attempts, err := runPutLadder(f, func(c context.Context) error {
		_, perr := f.store.Put(c, jetstream.ObjectMeta{Name: "E"}, &failAfterFirstChunk{fail: ioErr})
		return perr
	})
	if !errors.Is(err, ioErr) {
		t.Fatalf("the ladder must surface the reader's own error, got %v", err)
	}
	var refused *jetStreamRefusedError
	var stall *jetStreamStallError
	if errors.As(err, &refused) || errors.As(err, &stall) {
		t.Fatalf("a local read error was laundered into a JetStream face: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("a local read error was retried %d times; it is not transient", attempts)
	}
}

// tombstoneUploadedObject runs the abandon-path Delete on its own short deadline: the tombstone lands,
// the denied purge is allowed to time out, and control returns in ≈jsTombstoneTimeout — not after the
// phase budget the caller was on.
// origin: simcluster-speed review round 2 R2-F5
func TestTombstoneUploadedObjectReturnsOnItsOwnDeadline(t *testing.T) {
	f := startCtlACLBucket(t, "tombsid")
	if _, err := f.store.PutBytes(f.ctx, "D", []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	phase, cancel := context.WithTimeout(f.ctx, 20*time.Second) // the caller's (long) phase context
	defer cancel()
	start := time.Now()
	tombstoneUploadedObject(phase, f.store, "D")
	if el := time.Since(start); el > jsTombstoneTimeout+time.Second {
		t.Fatalf("tombstone took %s; it must be bounded by jsTombstoneTimeout (%s), not the phase context", el, jsTombstoneTimeout)
	}
	info, err := f.store.GetInfo(f.ctx, "D", jetstream.GetObjectInfoShowDeleted())
	if err != nil || !info.Deleted {
		t.Fatalf("the tombstone did not land: info=%+v err=%v", info, err)
	}
}
