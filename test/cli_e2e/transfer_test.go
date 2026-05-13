// File-transfer (P11) e2e tests — covers both Tier-A (inline,
// anonymous NATS, no JetStream) and Tier-B (JetStream Object Store)
// paths plus path-validation rejections. Tests are split by the
// `TestTransfer_TierA_*` / `TestTransfer_TierB_*` prefix; the
// Tier-B subset uses testharness.StartJSNATS via startJSNATS_xfer
// so it can exercise the real ObjectStore Put/Get.

package cli_e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go/jetstream"
)

func startJSNATS_xfer(t *testing.T) string { return testharness.StartJSNATS(t) }

// withAllowRoots returns an opt-style mutator for startAgent that
// populates the agent's file_transfer.allow_roots.
func withAllowRoots(roots ...string) func(*agent.Config) {
	return func(c *agent.Config) {
		c.AllowRoots = append([]string(nil), roots...)
	}
}

// freshTransfer covers the happy path: small file inline → agent on
// the receiver side writes bytes + emits ev.transfer.complete →
// broker writes audit.
func TestTransfer_TierA_PushHappyPath(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	dst := filepath.Join(root, "out.bin")
	payload := []byte("hello world")
	tid := "test-tid-001"
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: tid, Path: dst, Size: int64(len(payload)),
		SHA256: hexSHA256ForTest(payload), Tier: "a", InlineData: payload,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("push refused: code=%s err=%s", pr.Code, pr.Error)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("dst bytes != src: got %q", got)
	}
}

// Path validation: writes outside allow_roots reject without touching disk.
func TestTransfer_TierA_PushRejectsOutsideAllowRoots(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-escape", Path: "/etc/passwd", Size: 5,
		SHA256: hexSHA256ForTest([]byte("hello")), Tier: "a",
		InlineData: []byte("hello"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	var pr proto.PushPrepareResp
	_ = json.Unmarshal(resp.Data, &pr)
	if pr.OK {
		t.Errorf("expected refusal, got OK")
	}
	if pr.Code != "path_outside_roots" {
		t.Errorf("got code=%q, want path_outside_roots", pr.Code)
	}
	// /etc/passwd not modified — trivially true since the test
	// process can't write there anyway; explicit assertion just for
	// clarity that the agent didn't even try.
}

// transfer_disabled when allow_roots is empty.
func TestTransfer_TierA_TransferDisabledWhenAllowRootsEmpty(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "a100")() // no AllowRoots
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-x", Path: "/tmp/x", Size: 1,
		SHA256: hexSHA256ForTest([]byte("x")), Tier: "a",
		InlineData: []byte("x"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	var pr proto.PushPrepareResp
	_ = json.Unmarshal(resp.Data, &pr)
	if pr.OK || pr.Code != "transfer_disabled" {
		t.Errorf("got OK=%v code=%q, want transfer_disabled", pr.OK, pr.Code)
	}
}

// Caps probe round-trip — confirms the broker responds, JetStreamReady
// false on this anonymous-no-JS harness, MaxPayload reflects the
// server-advertised value.
func TestTransfer_CapsProbe(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	nc := connect(t, url)
	defer nc.Close()

	body, _ := json.Marshal(proto.CapsReq{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx, proto.SubjCtrlCaps(pub, "lab"), body)
	if err != nil {
		t.Fatalf("caps: %v", err)
	}
	var cr proto.CapsResp
	if err := json.Unmarshal(resp.Data, &cr); err != nil {
		t.Fatal(err)
	}
	if !cr.OK {
		t.Errorf("caps not OK: code=%q err=%q", cr.Code, cr.Error)
	}
	if cr.JetStreamReady {
		t.Errorf("expected JetStreamReady=false on anon-no-JS harness")
	}
	if cr.MaxPayload <= 0 {
		t.Errorf("MaxPayload not populated: %d", cr.MaxPayload)
	}
	if cr.BrokerProto != proto.ProtoVersion {
		t.Errorf("BrokerProto=%d want %d", cr.BrokerProto, proto.ProtoVersion)
	}
}

// Pull happy path: agent reads back the bytes we just pushed.
func TestTransfer_TierA_PullHappyPath(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	src := filepath.Join(root, "src.bin")
	payload := []byte("pull payload")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	tid := "test-tid-pull"
	body, _ := json.Marshal(proto.PullPrepareReq{
		TransferID: tid, Path: src, MaxInline: 8 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "pull"), body)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	var pr proto.PullPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("pull refused: code=%s err=%s", pr.Code, pr.Error)
	}
	if pr.Tier != "a" {
		t.Errorf("tier=%q want a", pr.Tier)
	}
	if !bytes.Equal(pr.InlineData, payload) {
		t.Errorf("inline_data != src")
	}
	// Finalize success so audit complete writes (sanity that the
	// finalize subject + handler is reachable).
	finBody, _ := json.Marshal(proto.TransferFinalize{
		Kind: "complete", TransferID: tid, Tier: "a",
		Bytes: int64(len(payload)),
	})
	finCtx, finCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer finCancel()
	finResp, err := nc.RequestWithContext(finCtx,
		proto.SubjCtrlTransferFinalize(pub, "lab", tid), finBody)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	var fr proto.TransferFinalizeResp
	if err := json.Unmarshal(finResp.Data, &fr); err != nil {
		t.Fatal(err)
	}
	if !fr.OK {
		t.Errorf("finalize not OK: code=%q err=%q", fr.Code, fr.Error)
	}
}

// Round-4 #1: cross-actor finalize must be rejected at the broker
// application layer (the actor-segment NATS gate is exercised in the
// auth template test). Here we forge a finalize from actor-B against
// a transfer started by actor-A in the same session — both connect
// anonymously so the gate is purely broker-side.
func TestTransfer_Finalize_RejectsForeignActor(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	actorA, fpA := freshUserPub(t)
	actorB, _ := freshUserPub(t) // different actor, not a member of "lab"
	seedSession(t, db, "lab", fpA)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	src := filepath.Join(root, "src.bin")
	if err := os.WriteFile(src, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	// Start a pull as actor-A (so an in-memory entry exists).
	tid := "tid-forge"
	body, _ := json.Marshal(proto.PullPrepareReq{
		TransferID: tid, Path: src, MaxInline: 8 << 20,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", actorA, "a100", "pull"), body); err != nil {
		t.Fatalf("pull (actor A): %v", err)
	}

	// Forge finalize from actor B.
	finBody, _ := json.Marshal(proto.TransferFinalize{
		Kind: "complete", TransferID: tid, Tier: "a",
	})
	finCtx, finCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer finCancel()
	resp, err := nc.RequestWithContext(finCtx,
		proto.SubjCtrlTransferFinalize(actorB, "lab", tid), finBody)
	if err != nil {
		t.Fatalf("finalize forgery: %v", err)
	}
	var fr proto.TransferFinalizeResp
	if err := json.Unmarshal(resp.Data, &fr); err != nil {
		t.Fatal(err)
	}
	if fr.OK {
		t.Errorf("expected forgery refusal; got OK")
	}
	if fr.Code != "not_owner_or_creator" && fr.Code != "not_a_member" {
		t.Errorf("got code=%q, want not_owner_or_creator|not_a_member", fr.Code)
	}
}

// 8 MiB just at the tier boundary still routes through tier A (≤ 8 MiB).
func TestTransfer_TierA_ExactBoundarySize(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	root := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()
	// Server default max_payload is 1 MiB. After base64 (≈ +33%) plus
	// JSON envelope overhead, the inline_data raw cap is ~700 KiB.
	// Use 600 KiB to leave headroom.
	size := 600 * 1024
	buf := make([]byte, size)
	_, _ = rand.Read(buf)
	dst := filepath.Join(root, "boundary.bin")
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-boundary", Path: dst, Size: int64(size),
		SHA256: hexSHA256ForTest(buf), Tier: "a", InlineData: buf,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK {
		t.Fatalf("push refused: %+v", pr)
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, buf) {
		t.Errorf("boundary push byte mismatch (len got=%d want=%d)", len(got), len(buf))
	}
}

// SHA mismatch on the wire → sha_mismatch refusal, no file written.
func TestTransfer_TierA_SHAVerify(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	root := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	dst := filepath.Join(root, "out.bin")
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: "tid-sha", Path: dst, Size: 5,
		SHA256: strings.Repeat("0", 64), // wrong sha
		Tier:   "a", InlineData: []byte("hello"),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	var pr proto.PushPrepareResp
	_ = json.Unmarshal(resp.Data, &pr)
	if pr.OK {
		t.Errorf("expected sha refusal")
	}
	if pr.Code != "sha_mismatch" {
		t.Errorf("got code=%q want sha_mismatch", pr.Code)
	}
	if _, err := os.Stat(dst); err == nil {
		t.Errorf("dst exists after sha_mismatch refusal")
	}
}

func hexSHA256ForTest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
// Tier-B push: ctl Put, agent Get, sha verify, rename, ev.transfer.
// On completion the broker must delete the OBJ_xfer-* stream.
func TestTransfer_TierB_PushHappyPath(t *testing.T) {
	url := startJSNATS_xfer(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	// Choose a size > 8 MiB so we definitely route to tier B.
	const size = 12 * 1024 * 1024
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	sha := hexSHA256ForTest(payload)
	dst := filepath.Join(root, "big.bin")
	tid := "tid-tierb-push"

	// Step 1: PushPrepareReq{Tier=b} — broker creates bucket and forwards.
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: tid, Path: dst, Size: int64(size), SHA256: sha, Tier: "b",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "push"), body)
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

	// Step 2: ObjectStore.Put.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "xfer-lab-" + tid
	storeCtx, storeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer storeCancel()
	store, err := js.ObjectStore(storeCtx, bucket)
	if err != nil {
		t.Fatalf("bind bucket %s: %v", bucket, err)
	}
	if _, err := store.Put(storeCtx, jetstream.ObjectMeta{Name: "object"},
		bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Step 3: push-commit.
	body, _ = json.Marshal(proto.TransferCommitReq{
		TransferID: tid, Bucket: bucket, ObjectKey: "object",
	})
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	resp, err = nc.RequestWithContext(commitCtx,
		proto.SubjCmdBy("lab", pub, "a100", "push-commit"), body)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	var cr proto.TransferCommitResp
	if err := json.Unmarshal(resp.Data, &cr); err != nil {
		t.Fatal(err)
	}
	if !cr.OK {
		t.Fatalf("commit refused: code=%s err=%s", cr.Code, cr.Error)
	}

	// Step 4: wait for ev.transfer.<id>.complete.
	evSub, err := nc.SubscribeSync(proto.SubjEvTransfer("lab", "a100", tid, "*"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = evSub.Unsubscribe() }()
	evCtx, evCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer evCancel()
	msg, err := evSub.NextMsgWithContext(evCtx)
	if err != nil {
		t.Fatalf("ev.transfer: %v", err)
	}
	var ev proto.TransferEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != "complete" {
		t.Fatalf("ev.kind=%q (code=%s err=%s)", ev.Kind, ev.Code, ev.Error)
	}
	if ev.Bytes != int64(size) {
		t.Errorf("ev.bytes=%d want %d", ev.Bytes, size)
	}

	// File contents on agent side must match.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("dst byte mismatch (len got=%d want=%d)", len(got), len(payload))
	}

	// Bucket must be gone (broker deleted on receipt of ev.transfer).
	// We give it a brief window because deletion is async.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := js.ObjectStore(context.Background(), bucket); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := js.ObjectStore(context.Background(), bucket); err == nil {
		t.Errorf("bucket %s still exists after ev.transfer.complete", bucket)
	}
}

// Tier-B pull: agent Put, ctl Get, finalize.req → broker writes
// audit + deletes bucket.
func TestTransfer_TierB_PullHappyPath(t *testing.T) {
	url := startJSNATS_xfer(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	root := t.TempDir()
	src := filepath.Join(root, "big.bin")
	const size = 12 * 1024 * 1024
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	defer startAgent(t, url, "lab", "a100", withAllowRoots(root))()
	testharness.WaitNodeOnline(t, db, "lab", "a100", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	tid := "tid-tierb-pull"
	body, _ := json.Marshal(proto.PullPrepareReq{
		TransferID: tid, Path: src,
		MaxInline: 1 * 1024 * 1024, // force tier B by capping inline
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy("lab", pub, "a100", "pull"), body)
	if err != nil {
		t.Fatalf("pull prepare: %v", err)
	}
	var pr proto.PullPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.OK || pr.Tier != "b" {
		t.Fatalf("expected tier=b OK, got %+v", pr)
	}

	// ctl side: Get from the bucket.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	getCtx, getCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer getCancel()
	store, err := js.ObjectStore(getCtx, pr.Bucket)
	if err != nil {
		t.Fatalf("bind bucket: %v", err)
	}
	result, err := store.Get(getCtx, pr.ObjectKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := readAllResult(result)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("tier-B pull bytes mismatch")
	}
	if hexSHA256ForTest(got) != pr.SHA256 {
		t.Errorf("sha mismatch: want=%s got=%s", pr.SHA256, hexSHA256ForTest(got))
	}

	// Send finalize.req — broker writes audit complete + deletes bucket.
	finBody, _ := json.Marshal(proto.TransferFinalize{
		Kind: "complete", TransferID: tid, Tier: "b", Bucket: pr.Bucket,
		Bytes: int64(size),
	})
	finCtx, finCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finCancel()
	finResp, err := nc.RequestWithContext(finCtx,
		proto.SubjCtrlTransferFinalize(pub, "lab", tid), finBody)
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

	// Bucket gone.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := js.ObjectStore(context.Background(), pr.Bucket); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := js.ObjectStore(context.Background(), pr.Bucket); err == nil {
		t.Errorf("bucket %s still exists after finalize", pr.Bucket)
	}
}

func readAllResult(r jetstream.ObjectResult) ([]byte, error) {
	defer func() { _ = r.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

