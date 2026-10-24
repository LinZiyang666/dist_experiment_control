package filetransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestCapZeroPullUploadsThroughTheOneShotAdapter drives the agent's explicit
// cap0 pull (no continuation carrier) above the inline ceiling. The upload is
// the one-shot jsstream.PutLegacyOnce adapter — the only SDK Put site the S7
// call graph allows — and must land exactly the source bytes under the
// broker-named object, replying the size and digest the ctl verifies.
// origin: hot-update S7 plan §7 / §9 / L1 / T49 (agent cap0 Put only via PutLegacyOnce)
func TestCapZeroPullUploadsThroughTheOneShotAdapter(t *testing.T) {
	s := natstest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	defer s.WaitForShutdown()
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: bounded test fixture.
	defer cancel()
	bucket := proto.XferBucketName("lab")
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("cap0 pull bytes|"), 400)
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Logger: testharness.SilentLog(), SID: "lab", NID: func() string { return "node" }, Instance: func() string { return "agent" },
		JetStream: func() jetstream.JetStream { return js }, Admit: func() (func(), bool) { return func() {}, true }}
	body, err := json.Marshal(struct {
		proto.PullPrepareReq
		Bucket    string `json:"bucket"`
		ObjectKey string `json:"object_key"`
	}{PullPrepareReq: proto.PullPrepareReq{TransferID: "legacy-pull", Path: path, MaxInline: 1024}, Bucket: bucket, ObjectKey: "legacy-pull"})
	if err != nil {
		t.Fatal(err)
	}
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	h.HandlePullForwarded(nc, &nats.Msg{Reply: inbox, Data: body})
	reply, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PullPrepareResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if !resp.OK || resp.Tier != "b" || resp.Size != int64(len(payload)) || resp.SHA256 != hex.EncodeToString(sum[:]) || resp.ObjectKey != "legacy-pull" {
		t.Fatalf("cap0 pull reply: %+v", resp)
	}
	got, err := store.GetBytes(ctx, "legacy-pull")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("the uploaded object differs from the source: %v", err)
	}
}
