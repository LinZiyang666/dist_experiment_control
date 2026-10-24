package main

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// The historical watchdog names retain the denied-purge regression oracles.
// S7 supplies the absolute deadline through the one-shot legacy owner: a Put
// that completed before its denied purge still succeeds, while a local read
// failure retains its cause and cannot be retried under a replacement NUID.
// origin: hot-update S7 plan §7 / T33 / INTEG-G restored R2-F1 assertion
func TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLegacyDeniedPurgeCompletion(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge", g)
	testharness.AssertNoFDLeak(t, "TestPutWatchdogHonoursAPutThatFinishedInsideTheDeniedPurge", fd, 0)
}

func runOriginalLegacyObject(t *testing.T, f *ctlACLBucket, key string, source io.Reader) error {
	t.Helper()
	owner, err := xferclient.NewLegacyObject(xferclient.LegacyConfig{TransferID: key}, f.cjs, f.bucket, key, source)
	if err != nil {
		t.Fatal(err)
	}
	deadline, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, firstErr := owner.Run(deadline)
	// A repeated Run must replay the same evidence without another source read.
	deadline, err = controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	again, secondErr := owner.Run(deadline)
	if again != result || secondErr != firstErr {
		t.Fatal("legacy owner replaced the original result")
	}
	if err := owner.Stop(deadline); err != nil {
		t.Fatal(err)
	}
	return firstErr
}

type legacyReadCounter struct {
	io.Reader
	reads int
}

func (r *legacyReadCounter) Read(p []byte) (int, error) { r.reads++; return r.Reader.Read(p) }

func exerciseLegacyDeniedPurgeCompletion(t *testing.T) {
	f := startCtlACLBucket(t, "purgesid")
	payload := strings.Repeat("y", 300*1024)
	for _, existing := range []bool{false, true} {
		source := &legacyReadCounter{Reader: strings.NewReader(payload)}
		start := time.Now()
		if err := runOriginalLegacyObject(t, f, "T", source); err != nil {
			t.Fatalf("existing=%v: %v", existing, err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("denied purge exceeded the operation deadline")
		}
		// Three chunks and the EOF read. A second Put would read EOF once more.
		if source.reads != 4 {
			t.Fatalf("source read %d times; one upload requires 4 reads", source.reads)
		}
	}
	got, err := f.store.GetBytes(f.ctx, "T")
	if err != nil || string(got) != payload {
		t.Fatalf("object after denied purge differs: %v", err)
	}
	requireDeniedPurge(t, f)
}

func requireDeniedPurge(t *testing.T, f *ctlACLBucket) {
	t.Helper()
	for _, e := range f.drainAsyncErrs() {
		if strings.Contains(e, "$JS.API.STREAM.PURGE.") {
			return
		}
	}
	t.Fatal("fixture did not exercise the denied STREAM.PURGE path")
}

// origin: hot-update S7 plan §7 / T33 / INTEG-G restored R2-F3 assertion
func TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLegacyDeniedPurgeReadError(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge", g)
	testharness.AssertNoFDLeak(t, "TestPutWatchdogReturnsThePutsOwnErrorHeldByTheDeniedPurge", fd, 0)
}

func exerciseLegacyDeniedPurgeReadError(t *testing.T) {
	f := startCtlACLBucket(t, "ioerrsid")
	ioErr := errors.New("read /dev/sdb: input/output error")
	source := &failAfterFirstChunk{fail: ioErr}
	start := time.Now()
	err := runOriginalLegacyObject(t, f, "E", source)
	if !errors.Is(err, ioErr) {
		t.Fatalf("local read error was lost or reclassified: %v", err)
	}
	if source.n != 2 {
		t.Fatalf("failed source was read %d times; it must not be retried", source.n)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("denied purge exceeded the operation deadline")
	}
	requireDeniedPurge(t, f)
}

type failAfterFirstChunk struct {
	n    int
	fail error
}

func (r *failAfterFirstChunk) Read(p []byte) (int, error) {
	r.n++
	if r.n > 1 {
		return 0, r.fail
	}
	for i := range p {
		p[i] = 'z'
	}
	return len(p), nil
}
