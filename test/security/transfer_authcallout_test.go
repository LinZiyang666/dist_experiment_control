// Real auth_callout + JetStream tier-B push/pull regression.
//
// v0.2.0/v0.2.1/v0.2.2/v0.2.3 each shipped a JWT perm bug that only
// fired against an auth_callout broker because anonymous-NATS test
// harnesses don't enforce JWT perms at all. This file pins down the
// COMPLETE set of $JS subjects nats.go ObjectStore Put / Get visit so
// the perm template is locked in before another version cuts. Run
// before any change to internal/auth/permissions.go that touches the
// OBJ_xfer-* / $O.xfer-* lines.

package security_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// startJSAuthCalloutNATS combines p3's auth_callout setup with p7's
// JetStream setup. Returns (url, brokerSeed, accountSeed).
func startJSAuthCalloutNATS(t *testing.T) (string, []byte, []byte) {
	t.Helper()
	brokerKp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	brokerPub, _ := brokerKp.PublicKey()
	rawBrokerSeed, _ := brokerKp.Seed()
	brokerSeed := append([]byte(nil), rawBrokerSeed...)
	brokerKp.Wipe()

	accountKp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ := accountKp.PublicKey()
	rawAcct, _ := accountKp.Seed()
	accountSeed := append([]byte(nil), rawAcct...)
	accountKp.Wipe()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	if os.Getenv("TETHER_TEST_VERBOSE") != "" {
		opts.Debug = true
		opts.Trace = true
	}
	opts.Nkeys = []*natsserver.NkeyUser{
		{
			Nkey:        brokerPub,
			Permissions: jwtToServerPerms(auth.PermissionsForBroker()),
		},
	}
	opts.AuthCallout = &natsserver.AuthCallout{
		Issuer:    accountPub,
		Account:   "$G",
		AuthUsers: []string{brokerPub},
	}
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded JS+authcallout nats-server not ready")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ns.JetStreamEnabled() && ns.JetStreamIsCurrent() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ns.ClientURL(), brokerSeed, accountSeed
}

func jwtToServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: p.Pub.Allow, Deny: p.Pub.Deny},
		Subscribe: &natsserver.SubjectPermission{Allow: p.Sub.Allow, Deny: p.Sub.Deny},
	}
}

func openTransferDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func silentTransferLog() *slog.Logger {
	if os.Getenv("TETHER_TEST_VERBOSE") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBrokerWithAuthJS boots a broker bound to the JS+authcallout NATS.
func startBrokerWithAuthJS(t *testing.T, url string, db *sql.DB, brokerSeed, accountSeed []byte) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentTransferLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
		AuthCallout: &broker.AuthCalloutConfig{
			BrokerNkeySeed: brokerSeed,
			AccountSeed:    accountSeed,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Probe auth_callout readiness with a throwaway nkey.
	probeKp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	probePub, _ := probeKp.PublicKey()
	probeSeed, _ := probeKp.Seed()
	probeSeedCopy := append([]byte(nil), probeSeed...) // copy BEFORE Wipe
	probeKp.Wipe()
	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(url,
			nats.Nkey(probePub, func(nonce []byte) ([]byte, error) {
				kp, kerr := nkeys.FromSeed(probeSeedCopy)
				if kerr != nil {
					return nil, kerr
				}
				defer kp.Wipe()
				return kp.Sign(nonce)
			}),
			nats.Name(cli.CtlNameUnactivated),
			nats.Timeout(200*time.Millisecond))
		if err == nil {
			nc.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("auth_callout not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// freshTransferIdentity isolates a CLI nkey under t.TempDir().
func freshTransferIdentity(t *testing.T) *cli.Identity {
	t.Helper()
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// startAgentAuthJS boots an agent connected via auth_callout with a
// fresh nkey + PIN-bound provisioning. Returns a stop function.
func startAgentAuthJS(t *testing.T, url string, db *sql.DB, sid, nid, pin string, allowRoots []string) func() {
	t.Helper()
	id, err := cli.EnsureAgentIdentity(t.TempDir(), sid)
	if err != nil {
		t.Fatal(err)
	}
	cfg := agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		PIN:               pin,
		Identity:          id,
		Logger:            silentTransferLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
		AllowRoots:        allowRoots,
	}
	a, err := agent.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// waitNodeOnline polls SQLite until (sid,nid) reports ONLINE or fail.
func waitNodeOnline(t *testing.T, db *sql.DB, sid, nid string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		var status string
		err := db.QueryRow("SELECT status FROM nodes WHERE sid=? AND nid=?", sid, nid).Scan(&status)
		if err == nil && status == "ONLINE" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s/%s not ONLINE after %s", sid, nid, deadline)
}

// TestTierB_PushPull_AuthCallout — the regression that v0.2.0..v0.2.3
// bug squad failed to catch locally. Real auth_callout + real
// JetStream + real ObjectStore Put/Get + byte-for-byte verify.
func TestTierB_PushPull_AuthCallout(t *testing.T) {
	url, bSeed, aSeed := startJSAuthCalloutNATS(t)
	db := openTransferDB(t)

	// Owner ctl creates session with a real bcrypt PIN.
	ownerID := freshTransferIdentity(t)
	pin := "123456"
	pinHash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", ownerID.Fingerprint, pinHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBrokerWithAuthJS(t, url, db, bSeed, aSeed)()

	// Agent provisions itself with the matching PIN.
	root := t.TempDir()
	defer startAgentAuthJS(t, url, db, "lab", "a-test", pin, []string{root})()
	waitNodeOnline(t, db, "lab", "a-test", 5*time.Second)

	// CTL connects activated.
	nc, err := cli.ConnectNATSWithNkey(url, ownerID, nats.Name(cli.CtlNameForSession("lab")))
	if err != nil {
		t.Fatalf("ctl activated connect: %v", err)
	}
	defer nc.Close()

	// 12 MiB random payload — definitely tier B.
	const size = 12 * 1024 * 1024
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	sha := hexSHA256ForTransferTest(payload)
	dst := filepath.Join(root, "transferred.bin")
	tid := "tid-authcallout-1"

	// Step 1: PushPrepareReq{Tier=b}. Broker creates the per-session
	// bucket xfer-lab and forwards.
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: tid, Path: dst, Size: int64(size), SHA256: sha, Tier: "b",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", ownerID.PublicKey, "a-test", "push"), body)
	if err != nil {
		t.Fatalf("push prepare: %v", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("prepare refused: code=%s err=%s", pr.Code, pr.Error)
	}

	// Step 2: ObjectStore.Put — THIS is the path that hit perm
	// violations in v0.2.0..v0.2.3. nats.go internally fires
	// $JS.API.STREAM.INFO, $JS.API.DIRECT.GET, $JS.FC.*, plus the
	// $O.xfer-lab.M.> and $O.xfer-lab.C.> data subjects. If any of
	// these are missing from the JWT, this will fail with a perm
	// violation in nats.go's async error log.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	bucket := proto.XferBucketName("lab")
	storeCtx, storeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer storeCancel()
	store, err := js.ObjectStore(storeCtx, bucket)
	if err != nil {
		t.Fatalf("ctl bind bucket %s: %v", bucket, err)
	}
	if _, err := store.Put(storeCtx, jetstream.ObjectMeta{Name: tid},
		bytes.NewReader(payload)); err != nil {
		t.Fatalf("ctl Put (PERM CHECK!): %v", err)
	}

	// Step 3: push-commit so the agent runs Get.
	body, _ = json.Marshal(proto.TransferCommitReq{
		TransferID: tid, Bucket: bucket, ObjectKey: tid,
	})
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer commitCancel()
	resp, err = nc.RequestWithContext(commitCtx,
		proto.SubjCmdBy("lab", ownerID.PublicKey, "a-test", "push-commit"), body)
	if err != nil {
		t.Fatalf("push-commit: %v", err)
	}
	var cr proto.TransferCommitResp
	if err := json.Unmarshal(resp.Data, &cr); err != nil {
		t.Fatal(err)
	}
	if !cr.OK {
		t.Fatalf("commit refused: code=%s err=%s", cr.Code, cr.Error)
	}

	// Step 4: wait for ev.transfer.complete from the AGENT — this
	// confirms agent.Get also passed every JWT perm gate.
	evSub, err := nc.SubscribeSync(proto.SubjEvTransfer("lab", "a-test", tid, "*"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = evSub.Unsubscribe() }()
	evCtx, evCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer evCancel()
	msg, err := evSub.NextMsgWithContext(evCtx)
	if err != nil {
		t.Fatalf("ev.transfer (agent didn't finalize — likely agent perm gap): %v", err)
	}
	var ev proto.TransferEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "complete" {
		t.Fatalf("ev.kind=%q (code=%s err=%s) — agent failed somewhere",
			ev.Kind, ev.Code, ev.Error)
	}

	// Byte verify on the agent side.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst on agent: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("byte mismatch (got=%d want=%d)", len(got), len(payload))
	}
}

// TestTierB_OpenDefault_AuthCallout is the open-mode (no allow_roots) twin
// of TestTierB_PushPull_AuthCallout. The transfer-unrestrict default flip
// means a fresh agent reaches the whole filesystem; this is the ONLY harness
// exercising that open-mode write path under real auth_callout + JWT/ACL +
// JetStream. The two existing auth tests pass a non-empty root (modeNarrow),
// so without this an open-mode JWT/ObjectStore-perm regression would ship
// undetected. dst is OUTSIDE any allow_root (there are none) — only open
// mode permits it.
func TestTierB_OpenDefault_AuthCallout(t *testing.T) {
	url, bSeed, aSeed := startJSAuthCalloutNATS(t)
	db := openTransferDB(t)

	ownerID := freshTransferIdentity(t)
	pin := "123456"
	pinHash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", ownerID.Fingerprint, pinHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBrokerWithAuthJS(t, url, db, bSeed, aSeed)()

	// nil allowRoots → RootsConfigured stays false → modeOpen (whole-FS).
	defer startAgentAuthJS(t, url, db, "lab", "a-test", pin, nil)()
	waitNodeOnline(t, db, "lab", "a-test", 5*time.Second)

	nc, err := cli.ConnectNATSWithNkey(url, ownerID, nats.Name(cli.CtlNameForSession("lab")))
	if err != nil {
		t.Fatalf("ctl activated connect: %v", err)
	}
	defer nc.Close()

	const size = 12 * 1024 * 1024
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	sha := hexSHA256ForTransferTest(payload)
	dst := filepath.Join(t.TempDir(), "open-authcallout.bin") // outside any root
	tid := "tid-open-authcallout"

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: tid, Path: dst, Size: int64(size), SHA256: sha, Tier: "b",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", ownerID.PublicKey, "a-test", "push"), body)
	if err != nil {
		t.Fatalf("push prepare: %v", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("open-mode prepare refused: code=%s err=%s", pr.Code, pr.Error)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	bucket := proto.XferBucketName("lab")
	storeCtx, storeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer storeCancel()
	store, err := js.ObjectStore(storeCtx, bucket)
	if err != nil {
		t.Fatalf("ctl bind bucket %s: %v", bucket, err)
	}
	if _, err := store.Put(storeCtx, jetstream.ObjectMeta{Name: tid},
		bytes.NewReader(payload)); err != nil {
		t.Fatalf("ctl Put (PERM CHECK!): %v", err)
	}

	body, _ = json.Marshal(proto.TransferCommitReq{
		TransferID: tid, Bucket: bucket, ObjectKey: tid,
	})
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer commitCancel()
	resp, err = nc.RequestWithContext(commitCtx,
		proto.SubjCmdBy("lab", ownerID.PublicKey, "a-test", "push-commit"), body)
	if err != nil {
		t.Fatalf("push-commit: %v", err)
	}
	var cr proto.TransferCommitResp
	if err := json.Unmarshal(resp.Data, &cr); err != nil {
		t.Fatal(err)
	}
	if !cr.OK {
		t.Fatalf("commit refused: code=%s err=%s", cr.Code, cr.Error)
	}

	evSub, err := nc.SubscribeSync(proto.SubjEvTransfer("lab", "a-test", tid, "*"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = evSub.Unsubscribe() }()
	evCtx, evCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer evCancel()
	msg, err := evSub.NextMsgWithContext(evCtx)
	if err != nil {
		t.Fatalf("ev.transfer (agent didn't finalize in open mode): %v", err)
	}
	var ev proto.TransferEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "complete" {
		t.Fatalf("ev.kind=%q (code=%s err=%s)", ev.Kind, ev.Code, ev.Error)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst on agent: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("byte mismatch (got=%d want=%d)", len(got), len(payload))
	}
}

// TestTierB_PullThenPush_AuthCallout exercises the pull path (ctl
// Gets). Same JWT perm coverage but flips the Put/Get role.
func TestTierB_PullThenPush_AuthCallout(t *testing.T) {
	url, bSeed, aSeed := startJSAuthCalloutNATS(t)
	db := openTransferDB(t)

	ownerID := freshTransferIdentity(t)
	pin := "123456"
	pinHash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", ownerID.Fingerprint, pinHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBrokerWithAuthJS(t, url, db, bSeed, aSeed)()

	root := t.TempDir()
	src := filepath.Join(root, "source.bin")
	const size = 12 * 1024 * 1024
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	defer startAgentAuthJS(t, url, db, "lab", "a-test", pin, []string{root})()
	waitNodeOnline(t, db, "lab", "a-test", 5*time.Second)

	nc, err := cli.ConnectNATSWithNkey(url, ownerID, nats.Name(cli.CtlNameForSession("lab")))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	tid := "tid-authcallout-pull"
	body, _ := json.Marshal(proto.PullPrepareReq{
		TransferID: tid, Path: src, MaxInline: 1 * 1024 * 1024, // force tier B
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", ownerID.PublicKey, "a-test", "pull"), body)
	if err != nil {
		t.Fatalf("pull prepare (this is where agent.Put runs and would hit perm violations): %v", err)
	}
	var pr proto.PullPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("pull refused: code=%s err=%s", pr.Code, pr.Error)
	}
	if pr.Tier != "b" {
		t.Fatalf("expected tier=b, got %q", pr.Tier)
	}

	// Step 2: ctl Gets the object.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	getCtx, getCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer getCancel()
	store, err := js.ObjectStore(getCtx, pr.Bucket)
	if err != nil {
		t.Fatalf("bind bucket: %v", err)
	}
	result, err := store.Get(getCtx, pr.ObjectKey)
	if err != nil {
		t.Fatalf("Get (PERM CHECK ctl side): %v", err)
	}
	defer func() { _ = result.Close() }()
	got, err := io.ReadAll(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("pull byte mismatch (got=%d want=%d)", len(got), len(payload))
	}

	// Step 3: finalize so the broker writes audit + cleans the object.
	finBody, _ := json.Marshal(proto.TransferFinalize{
		Kind: "complete", TransferID: tid, Tier: "b", Bucket: pr.Bucket,
		Bytes: int64(size),
	})
	finCtx, finCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finCancel()
	finResp, err := nc.RequestWithContext(finCtx,
		proto.SubjCtrlTransferFinalize(ownerID.PublicKey, "lab", tid), finBody)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	var fr proto.TransferFinalizeResp
	if err := json.Unmarshal(finResp.Data, &fr); err != nil {
		t.Fatal(err)
	}
	if !fr.OK {
		t.Errorf("finalize refused: code=%q err=%q", fr.Code, fr.Error)
	}
}

func hexSHA256ForTransferTest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
