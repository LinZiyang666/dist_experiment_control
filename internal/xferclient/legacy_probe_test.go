package xferclient

import (
	"context"
	"errors"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

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
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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

// origin: TestProbeJetStreamForBucketRejectsALeaderlessStream
func TestProbeJetStreamForBucketRejectsALeaderlessStream(t *testing.T) {
	f := startCtlACLBucket(t, "probe")
	url := f.cjs.Conn().ConnectedUrl()
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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

// origin: TestProbeJetStreamForBucketStaysInsideTheCtlACL
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

// origin: TestPutWithJSWatchdogCancelsAStalledPutOnlyAfterStrikes / hot-update S7 plan §7
func TestTransferLegacySDKCancellationJoinsOriginalPut(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLegacySDKCancellation(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy SDK cancellation", g)
	testharness.AssertNoFDLeak(t, "legacy SDK cancellation", fd, 0)
}
func exerciseLegacySDKCancellation(t *testing.T) {
	f := startCtlACLBucket(t, "cancel")
	parent, cancel := context.WithCancel(f.ctx)
	defer cancel()
	d, _ := controlbudget.After(10 * time.Second)
	// A read failure after the first chunk enters the SDK's denied purge. The
	// watchdog's cancellation must release that join without changing its error.
	original := errors.New("source read failed")
	source := &legacyFailingSource{remaining: 128 << 10, cause: original}
	done := make(chan error, 1)
	go func() {
		_, err := jsstream.PutLegacyOnce(f.cjs, f.bucket, "original", legacyPutReader{Reader: source, ctx: parent}, d)
		done <- err
	}()
	if !testharness.WaitFor(t, 2*time.Second, 10*time.Millisecond, func() bool {
		for _, err := range f.drainAsyncErrs() {
			if strings.Contains(err, "$JS.API.STREAM.PURGE") {
				return true
			}
		}
		return false
	}) {
		cancel()
		<-done
		t.Fatal("SDK never reached the denied partial purge")
	}
	started := time.Now()
	cancel()
	err := <-done
	if !errors.Is(err, original) || time.Since(started) > time.Second {
		t.Fatalf("cancellation did not join the original SDK operation: %v after %s", err, time.Since(started))
	}
	if source.calls != 2 {
		t.Fatalf("source was retried: reads=%d", source.calls)
	}
}

type legacyFailingSource struct {
	remaining, calls int
	cause            error
}

func (r *legacyFailingSource) Read(p []byte) (int, error) {
	r.calls++
	if r.remaining == 0 {
		return 0, r.cause
	}
	n := min(len(p), r.remaining)
	clear(p[:n])
	r.remaining -= n
	return n, nil
}
