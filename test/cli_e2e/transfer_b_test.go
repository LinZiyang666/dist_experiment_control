// JetStream-enabled file-transfer (Tier-B) e2e tests. Same anonymous
// pattern as transfer_test.go, just with StartJSNATS — exercises the
// real ObjectStore Put/Get path, the bucket-cleanup on completion,
// and the optimistic pull bucket reaping.

package cli_e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go/jetstream"
)

func startJSNATS_xfer(t *testing.T) string { return testharness.StartJSNATS(t) }

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

