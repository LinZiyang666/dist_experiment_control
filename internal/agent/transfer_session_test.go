//go:build linux

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/LinZiyang666/tether/internal/xferagentimage"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// An imported receiver opens its data attachment on its runner, independently
// of the session goroutine publishing a replacement management connection.
// origin: S7 internal review CUT-CAPACITY F6
func TestTransferImportedReceiverSnapshotsRebuiltSession(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferReceiverSession(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "imported transfer receiver session", g)
	testharness.AssertNoFDLeak(t, "imported transfer receiver session", fd, 0)
}

func exerciseTransferReceiverSession(t *testing.T) {
	t.Helper()
	url := cutJetStream(t)
	nc, err := nats.Connect(url, nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	old := transferSessionAgent(t, url)
	old.js = js // No session or worker exists yet.
	data := bytes.Repeat([]byte("receiver image keeps its synced bytes\n"), 2048)
	sum := sha256.Sum256(data)
	id := x.Identity{SID: old.cfg.SID, NID: nidOf(old), AgentInstance: old.instanceID, ActorFP: "actor",
		TransferID: "sessionrace", EntryNonce: "entrynonce", IntentDigest: hex.EncodeToString(sum[:]), ObjectVersion: "fixedversion"}
	intent := x.Intent{Verb: "push", Tier: "b", Path: filepath.Join(t.TempDir(), "target"), Size: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]), ChunkSize: 32 << 10, Force: true}
	seedTransferSessionObject(t, js, id, intent, data)
	h := transferHandler(old)
	now := time.Now().UTC()
	origin := proto.Origin{BrokerID: "broker", InstanceID: "broker-instance", BootID: "broker-boot", Generation: 1}
	link := proto.TransferLink{Op: proto.TransferLinkPrepare, CallID: "prepare-session-race", Identity: proto.Identity(id), Origin: origin,
		Intent: (*proto.Intent)(&intent),
		Permit: &proto.CreationPermit{Identity: proto.Identity(id), Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "verified"},
		Budget: &proto.BudgetSeal{StartedAt: now, Size: intent.Size, EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(300 * time.Second), SourceSealed: true}}
	body, err := json.Marshal(proto.TransferCommitReq{Link: &link})
	if err != nil {
		t.Fatal(err)
	}
	var answer proto.TransferCommitResp
	peer := xferagent.LinkPeer{MaxPayload: nc.MaxPayload(), Publish: func(_ string, body []byte) error { return json.Unmarshal(body, &answer) }}
	if !h.Runtime.HandleLink(peer, &nats.Msg{Reply: "reply", Data: body}) || !answer.OK {
		t.Fatalf("prepare did not install the real receiver intent: %+v", answer)
	}
	if err := h.Worker.Start(id); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		status, err := h.Worker.Status(id)
		return err == nil && status.DurableOffset == intent.Size
	}) {
		t.Fatal("receiver did not Sync its payload before the image cut")
	}
	frozen, err := xferagentimage.Worker(h.Runtime).Freeze(transferSessionDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil || len(resources) == 0 || state.Entries != 1 {
		t.Fatalf("receiver image export: entries=%d resources=%d err=%v", state.Entries, len(resources), err)
	}
	before, err := xferimage.DecodeWorker(state.Encoded)
	if err != nil || len(before.Receivers) != 1 || before.Receivers[0].Checkpoint.DurableOffset != intent.Size {
		t.Fatalf("cut omitted the live receiver's synced prefix: %v", err)
	}
	next := transferSessionAgent(t, url)
	next.instanceID = old.instanceID
	reading := make(chan struct{}, 1)
	var sampling, release atomic.Bool
	var samples atomic.Uint64
	// Instrument only the host getter the imported runner actually calls.
	// Repeated snapshots widen the race window without replacing a worker,
	// reader, session, backend or any byte operation with a fake.
	build := func() *filetransfer.Handler {
		shell := transferShell(next)
		current := shell.JetStream
		shell.JetStream = func() jetstream.JetStream {
			if sampling.Load() {
				select {
				case reading <- struct{}{}:
				default:
				}
				for !release.Load() {
					_ = current()
					samples.Add(1)
					runtime.Gosched()
				}
			}
			return current()
		}
		return shell
	}
	imported, _, err := xferagentimage.Importer(&next.transfers, next.cfg.TransferWorker, build).Import(state, handofftest.DupResources(t, resources))
	if err != nil {
		t.Fatal(err)
	}
	if err := imported.Thaw(transferSessionDeadline(t)); err != nil {
		t.Fatal(err)
	}
	sampling.Store(true)
	defer release.Store(true)
	registered := make(chan struct{}, 1)
	_, err = nc.Subscribe(proto.SubjNodeRegister(next.cfg.SID, nidOf(next)), func(msg *nats.Msg) {
		select {
		case <-reading:
			body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
			_ = msg.Respond(body)
			registered <- struct{}{}
		case <-t.Context().Done():
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	done := make(chan error, 1)
	go func() { _, err := next.session(ctx); done <- err }()
	defer func() {
		release.Store(true)
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("rebuilt session did not join")
		}
	}()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return next.controlReady.Load() != nil }) {
		t.Fatal("session did not publish its replacement JetStream while the receiver resumed")
	}
	select {
	case <-registered:
	default:
		t.Fatal("session never registered during the receiver's attachment lookup")
	}
	if samples.Load() == 0 {
		t.Fatal("imported receiver never sampled the session binding")
	}
	release.Store(true)
	// Freeze joins the resumed Step, including its real OpenData and EOF check.
	// Import resets the reader, so even a full synced prefix must reopen it.
	again, err := imported.Freeze(transferSessionDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	projection, _, err := again.Export()
	if err != nil || projection.Generation != state.Generation+1 {
		t.Fatalf("resumed receiver lost its current image generation: %v", err)
	}
	decoded, err := xferimage.DecodeWorker(projection.Encoded)
	if err != nil || len(decoded.Receivers) != 1 || decoded.Receivers[0].Checkpoint.DurableOffset != intent.Size {
		t.Fatalf("resumed receiver lost its synced prefix: %v", err)
	}
	if decoded.Receivers[0].Reader.ConnectionIdentity == before.Receivers[0].Reader.ConnectionIdentity {
		t.Fatal("imported receiver did not reopen its reader on a new data attachment")
	}
	if _, err := os.Stat(intent.Path); !os.IsNotExist(err) {
		t.Fatalf("receiver landed without a grant: %v", err)
	}
}

func transferSessionAgent(t *testing.T, url string) *Agent {
	t.Helper()
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), RemoteFSMode: "off",
		Logger: testharness.SilentLog(), HeartbeatInterval: time.Hour, RegisterTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAgentTransfers(a); stopProcessRuntime(t.Context(), a) })
	return a
}

func transferSessionDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func seedTransferSessionObject(t *testing.T, js jetstream.JetStream, id x.Identity, intent x.Intent, data []byte) {
	t.Helper()
	bucket := proto.XferBucketName(id.SID)
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: id, Bucket: bucket, Stream: "OBJ_" + bucket, StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano),
		Size: intent.Size, SHA256: intent.SHA256, ChunkSize: intent.ChunkSize, Chunks: uint32((len(data) + int(intent.ChunkSize) - 1) / int(intent.ChunkSize))}
	w, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < spec.Chunks; index++ {
		offset := int(index * spec.ChunkSize)
		if _, err := w.Append(index, data[offset:min(len(data), offset+int(spec.ChunkSize))], transferSessionDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finalize(transferSessionDeadline(t)); err != nil {
		t.Fatal(err)
	}
}
