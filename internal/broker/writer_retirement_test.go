package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercapacity"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Real NKey ACLs, production ctl transport/finalize ingress, durable ledger,
// serialized tracker, original writer and tombstone ACK close this seam. The
// creator has no current membership database: retained identity authorizes
// only its positive retirement witness, never byte admission or a terminal.
// origin: S7 internal review rv-terminal / FIX-TERM request 3
func TestTransferAuthenticatedWriterWitnessFencesFailedObjectCleanup(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAuthenticatedWriterRetirement(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseAuthenticatedWriterRetirement(t *testing.T) {
	t.Helper()
	keys := make([]nkeys.KeyPair, 3)
	pubs := make([]string, 3)
	users := make([]*server.NkeyUser, 3)
	for i := range keys {
		var err error
		keys[i], err = nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		defer keys[i].Wipe()
		pubs[i], err = keys[i].PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		users[i] = &server.NkeyUser{Nkey: pubs[i]}
		if i > 0 {
			users[i].Permissions = &server.Permissions{Publish: &server.SubjectPermission{Allow: []string{proto.SubjCtrlTransferFinalize(pubs[i], "session", "original")}},
				Subscribe: &server.SubjectPermission{Allow: []string{auth.InboxPrefixFor(pubs[i]) + ".>"}}}
		}
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir(), Nkeys: users})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("authenticated server did not start")
	}
	conns := make([]*nats.Conn, 3)
	for i := range conns {
		conns[i], err = nats.Connect(s.ClientURL(), nats.Nkey(pubs[i], keys[i].Sign), nats.CustomInboxPrefix(auth.InboxPrefixFor(pubs[i])), nats.ReconnectBufSize(-1))
		if err != nil {
			t.Fatal(err)
		}
		defer conns[i].Close()
	}
	js, err := jetstream.New(conns[0])
	if err != nil {
		t.Fatal(err)
	}
	store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-session"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_xfer-session")
	if err != nil {
		t.Fatal(err)
	}
	policy := stream.CachedInfo().Config
	policy.Duplicates = proto.ObjectRecoveryHorizon
	if _, err = js.UpdateStream(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if err = jsstream.EnsureHistoryStream(t.Context(), js, "session", 1); err != nil {
		t.Fatal(err)
	}
	base := x.Base{Clock: func() (string, int64, error) { ns, err := controlbudget.Now(); return "boot", ns, err }, UTC: time.Now, Capacity: xfercapacity.New("writer-witness-")}
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(t.TempDir(), "ledger"), FallbackRoot: filepath.Join(t.TempDir(), "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	var view x.DecisionPort
	rows := xferserve.RowsFrom(func() x.DecisionPort { return view }, func() x.LedgerPort { return ledger })
	origin := x.Origin{BrokerID: "broker", InstanceID: "original", BootID: "boot", Generation: 1}
	tracker, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: history, ResolveReceiver: xferserve.Receiver{Rows: rows}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tracker.Stop(transferRouteDeadline(t, time.Second)); err != nil {
			t.Error(err)
		}
	}()
	view, err = xfertrack.Decisions(tracker)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(pubs[1])
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("original"), 8192)
	sum := sha256.Sum256(data)
	id := x.Identity{SID: "session", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "original", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "originalversion"}
	intent := x.Intent{Verb: "push", Tier: "b", Path: "original-target", Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), ChunkSize: 32768}
	now := time.Now()
	permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "authenticated-test-admission"}
	d := transferRouteDeadline(t, 5*time.Second)
	if _, err = tracker.Admit(permit, intent, d); err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.SourceSeal(id, x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, ChunkSize: 32768, Chunks: 2, SourceFingerprint: "pinned-source"}, d); err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: id, Bucket: "xfer-session", Stream: "OBJ_xfer-session", StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano),
		Size: intent.Size, SHA256: intent.SHA256, ChunkSize: 32768, Chunks: 2}
	journal := &xferserve.RetirementJournal{Root: filepath.Join(t.TempDir(), "retirement")}
	if err = journal.Pin(id, func() (x.ObjectSpec, error) { return spec, nil }); err != nil {
		t.Fatal(err)
	}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := writer.Append(0, data[:32768], d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Cancel(id, x.CancelRequest{CallID: "cancel", Origin: origin, Reason: x.CancelUser}, d); err != nil {
		t.Fatal(err)
	}
	objects := xferserve.Objects{Retirement: journal, JS: func() jetstream.JetStream { return js }, Rows: rows}
	proof := x.RetirementProof{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, OwnerGeneration: 1, ReceiverRetired: true}
	if _, err = objects.Retire(id, proof, d); err == nil {
		t.Fatal("failed push purged before its original writer joined")
	}
	b := &Broker{}
	b.nc.Store(conns[0])
	b.transferRuntime.Store(&BrokerTransfers{Origin: origin, Tracker: tracker, Ledger: ledger, Decisions: view, Link: &xferserve.Link{Retirement: journal}})
	sub, err := conns[0].Subscribe(proto.SubjectPrefix+".ctrl.by.*.s.session.transfer.original.finalize.req", b.handleFinalizeReq)
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Error(err)
		}
	}()
	if err = conns[0].FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err = writer.Stop(d); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Freeze(d)
	if err != nil {
		t.Fatal(err)
	}
	request := proto.TransferWriterRetirementReq{Version: x.Version, CallID: "witness", Identity: proto.Identity(id), Origin: proto.Origin(origin),
		Writer: proto.TransferWriterRetirement{Bucket: spec.Bucket, Stream: spec.Stream, StreamCreated: spec.StreamCreated, SHA256: spec.SHA256, Size: spec.Size,
			ChunkSize: spec.ChunkSize, Chunks: spec.Chunks, AttachmentGeneration: 1, WriterRetired: true, ExpectedStreamSeq: chunk.StreamSeq}}
	if !serveWriterRetirement(b, &nats.Msg{Reply: "_INBOX.shared", Data: []byte(`{"writer_retirement_version":"transfer-v1","kind":"failed"}`)}, pubs[1], id.SID, id.TransferID) {
		t.Fatal("refused retirement marker selected the legacy finalize path")
	}
	for _, defect := range []string{"actor", "origin", "nonce", "incarnation", "join", "shape", "version", "oversized", "source", "bucket", "stream", "geometry", "generation"} {
		bad := request
		nc, actor := conns[1], pubs[1]
		switch defect {
		case "actor":
			nc, actor = conns[2], pubs[2]
		case "origin":
			bad.Origin.InstanceID = "replacement"
		case "nonce":
			bad.Identity.EntryNonce = "replacement"
		case "incarnation":
			bad.Writer.StreamCreated = time.Now().Add(time.Hour).Format(time.RFC3339Nano)
		case "join":
			bad.Writer.WriterRetired = false
		case "version":
			bad.Version = "unknown"
		case "source":
			bad.Writer.SHA256 = strings.Repeat("0", 64)
		case "bucket":
			bad.Writer.Bucket = "xfer-replacement"
		case "stream":
			bad.Writer.Stream = "OBJ_xfer-replacement"
		case "geometry":
			bad.Writer.Size++
		case "generation":
			bad.Writer.AttachmentGeneration = 0
		}
		body, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		if defect == "shape" {
			body = []byte(strings.TrimSuffix(string(body), "}") + `,"outcome":{"outcome":"landed"}}`)
		}
		if defect == "oversized" {
			body = append(body, []byte(strings.Repeat(" ", 4096))...)
		}
		msg, err := nc.Request(proto.SubjCtrlTransferFinalize(actor, id.SID, id.TransferID), body, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var ack proto.TransferWriterRetirementResp
		if err = json.Unmarshal(msg.Data, &ack); err != nil || ack.Code == "" {
			t.Fatalf("%s witness accepted: %s %v", defect, msg.Data, err)
		}
		row, err := rows(id, d)
		if err != nil || row.Cleanup.WriterFence.WriterRetired {
			t.Fatalf("%s forged retirement changed durable disposition", defect)
		}
	}
	seed, err := keys[1].Seed()
	if err != nil {
		t.Fatal(err)
	}
	identity := &cli.Identity{Seed: seed, PublicKey: pubs[1], Fingerprint: fp}
	transport, err := cli.NewTransferTransport(conns[1], s.ClientURL(), identity, &cli.CanonicalTransferWire{Actor: pubs[1]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
	}()
	producer, ok := any(transport).(interface {
		RetireWriter(x.Identity, x.Origin, x.WriterState, x.Deadline) error
	})
	if !ok {
		t.Fatal("production ctl transport has no retirement channel")
	}
	if err = producer.RetireWriter(id, origin, state, d); err != nil {
		t.Fatal("native witness delivery:", err)
	}
	if err = producer.RetireWriter(id, origin, state, d); err != nil {
		t.Fatal("exact witness replay was not idempotent:", err)
	}
	census, err := ledger.Scan(d)
	if err != nil || len(census.Rows) != 1 || !census.Rows[0].Cleanup.WriterFence.WriterRetired || census.Rows[0].Cleanup.WriterFence.ExpectedStreamSeq < chunk.StreamSeq {
		t.Fatalf("authenticated witness was not durable: %+v %v", census, err)
	}
	receipt, err := objects.Retire(id, proof, d)
	if err != nil || !receipt.ObjectPurged || receipt.FenceSequence <= chunk.StreamSeq {
		t.Fatalf("failed object has no real tombstone ACK: %+v %v", receipt, err)
	}
	if info, err := store.GetInfo(t.Context(), id.TransferID, jetstream.GetObjectInfoShowDeleted()); err != nil || !info.Deleted {
		t.Fatal("retirement metadata missing:", err)
	}
}
