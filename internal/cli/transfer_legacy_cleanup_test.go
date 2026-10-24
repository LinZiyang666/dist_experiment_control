package cli

import (
	"context"
	"github.com/LinZiyang666/tether/internal/auth"
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

// origin: TestTombstoneUploadedObjectReturnsOnItsOwnDeadline
func TestTombstoneUploadedObjectReturnsOnItsOwnDeadline(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTombstoneUploadedObjectReturnsOnItsOwnDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestTombstoneUploadedObjectReturnsOnItsOwnDeadline", g)
	testharness.AssertNoFDLeak(t, "TestTombstoneUploadedObjectReturnsOnItsOwnDeadline", fd, 0)
}
func exerciseTombstoneUploadedObjectReturnsOnItsOwnDeadline(t *testing.T) {
	f := startCtlACLBucket(t, "tombsid")
	if _, err := f.store.PutBytes(f.ctx, "D", []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	phase, cancel := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancel()
	start := time.Now()
	tombstoneLegacyUpload(phase, f.cjs, f.bucket, "D")
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("cleanup used the phase deadline: %s", elapsed)
	}
	denied := false
	for _, err := range f.drainAsyncErrs() {
		if strings.Contains(err, "$JS.API.STREAM.PURGE.") {
			denied = true
		}
	}
	if !denied {
		t.Fatal("fixture did not exercise the denied STREAM.PURGE path")
	}
	info, err := f.store.GetInfo(f.ctx, "D", jetstream.GetObjectInfoShowDeleted())
	if err != nil || !info.Deleted {
		t.Fatalf("tombstone did not land: %+v %v", info, err)
	}
}
