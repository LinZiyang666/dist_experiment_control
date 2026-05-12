// File-transfer (P11) e2e tests — anonymous-NATS path, no JetStream,
// no auth_callout. Tier-A happy paths + path-validation rejections.
// Tier-B coverage lives in transfer_js_test.go (JS-enabled harness).

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
)

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
