package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type legacyHistoryRow struct {
	raw []byte
	rec schema.AuditTransfer
}

func contextWithTestTimeout() (context.Context, context.CancelFunc) {
	// ctx-root: bounded test inspection of the embedded JetStream.
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// legacyHistoryRows is an independent physical oracle: it walks the history
// stream itself and decodes raw transfer rows, never the production scanner.
func legacyHistoryRows(js jetstream.JetStream, sid, tid string) ([]legacyHistoryRow, error) {
	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	stream, err := js.Stream(ctx, jsstream.HistoryStreamName(sid))
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	var rows []legacyHistoryRow
	for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var rec schema.AuditTransfer
		if msg.Subject != proto.SubjAuditTransfer(sid) || json.Unmarshal(msg.Data, &rec) != nil || rec.TransferID != tid {
			continue
		}
		rows = append(rows, legacyHistoryRow{raw: msg.Data, rec: rec})
	}
	return rows, nil
}

type legacyForward struct {
	row     xferInflightRecord
	rowErr  error
	history []legacyHistoryRow
	histErr error
}

type legacyHistoryFixture struct {
	b            *Broker
	nc           *nats.Conn
	js           jetstream.JetStream
	actor        string
	seen         chan legacyForward
	events       *nats.Subscription
	startRefused bool // exact history is refusing at admission time
}

// sink, when set, is the cluster Raft forward; it is installed before any
// subscription goroutine exists, as wireClusterLate does before Run serves.
func newLegacyHistoryFixture(t *testing.T, sink func(schema.AuditTransfer, func())) *legacyHistoryFixture {
	t.Helper()
	url := testharness.StartJSNATS(t)
	db := openDB(t)
	actor, fp := testharness.FreshUserPub(t)
	if _, err := session.Create(db, "lab", "lab", fp, "pinhash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := session.AddMember(db, "lab", fp, session.RoleOwner, session.ViaPin, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "lab-1", ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.0.0"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	f := &legacyHistoryFixture{nc: nc, js: js, actor: actor, seen: make(chan legacyForward, 4),
		b: &Broker{cfg: Config{DB: db, Logger: silentLogger(), Now: time.Now, ClusterDataDir: t.TempDir()}, transfers: xfertrack.NewLegacyTracker(),
			transferAuditSink: sink}}
	setBrokerJS(f.b, js)
	t.Cleanup(func() { setBrokerJS(f.b, nil) })
	f.b.nc.Store(nc) // the broker's own connection, as after Run: an extra event would be visible
	subscribe := func(subject string, handler nats.MsgHandler) {
		if _, err := nc.Subscribe(subject, handler); err != nil {
			t.Fatal(err)
		}
	}
	subscribe(proto.SubjCmdBy("lab", actor, "lab-1", "push"), func(msg *nats.Msg) { f.b.handlePushReq(nc, msg) })
	subscribe(proto.SubjectPrefix+".s.*.ev.node.*.transfer.*.complete", f.b.handleEvTransfer)
	// The old receiver answers the original ctl inbox itself. What the broker had
	// made durable and published BEFORE it forwarded is captured at this instant.
	subscribe(proto.SubjCmdForwarded("lab", "lab-1", "push"), func(msg *nats.Msg) {
		var req proto.PushPrepareReq
		_ = json.Unmarshal(msg.Data, &req)
		var seen legacyForward
		seen.row, seen.rowErr = readXferInflight(filepath.Join(f.b.xferInflightDir(), xferInflightFilename(req.TransferID)))
		seen.history, seen.histErr = legacyHistoryRows(js, "lab", req.TransferID)
		f.seen <- seen
		reply, _ := json.Marshal(proto.PushPrepareResp{OK: true})
		_ = nc.Publish(msg.Reply, reply)
	})
	if f.events, err = nc.SubscribeSync(proto.SubjEvTransfer("lab", "lab-1", "*", "*")); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

// push drives one real cap0 tier-A prepare and returns the identity the broker
// had retained, durably and in history, before the receiver saw the request.
func (f *legacyHistoryFixture) push(t *testing.T, tid string) string {
	t.Helper()
	data := []byte("cap0")
	sum := sha256.Sum256(data)
	body, _ := json.Marshal(proto.PushPrepareReq{TransferID: tid, Path: "/dst/" + tid, Tier: "a", Size: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]), InlineData: data})
	reply, err := f.nc.Request(proto.SubjCmdBy("lab", f.actor, "lab-1", "push"), body, 10*time.Second)
	var resp proto.PushPrepareResp
	if err != nil || json.Unmarshal(reply.Data, &resp) != nil || !resp.OK {
		t.Fatalf("cap0 prepare did not reach its receiver: %v %s", err, reply.Data)
	}
	var seen legacyForward
	select {
	case seen = <-f.seen:
	case <-time.After(10 * time.Second):
		t.Fatal("prepare never forwarded")
	}
	start := seen.row.Start
	if seen.rowErr != nil || start == nil || start.EntryNonce == "" || !start.BestEffort || start.TerminalID != "" {
		t.Fatalf("the admission identity was not durable before the forward: %+v %v", start, seen.rowErr)
	}
	if f.startRefused {
		// Exact history refused the START: the forward still proceeds, and only the
		// retained ledger row holds that START until settlement confirms it.
		if seen.histErr != nil || len(seen.history) != 0 {
			t.Fatalf("a refused START reached history anyway: %+v %v", seen.history, seen.histErr)
		}
		return start.EntryNonce
	}
	if seen.histErr != nil || len(seen.history) != 1 || seen.history[0].rec.Kind != "start" || seen.history[0].rec.EntryNonce != start.EntryNonce {
		t.Fatalf("the exact START did not carry the retained admission identity before the forward: %+v %v", seen.history, seen.histErr)
	}
	return start.EntryNonce
}

func (f *legacyHistoryFixture) complete(t *testing.T, tid string) {
	t.Helper()
	ev, _ := json.Marshal(proto.TransferEvent{Kind: "complete", Verb: "push", TransferID: tid, Tier: "a", Bytes: 4})
	if err := f.nc.Publish(proto.SubjEvTransfer("lab", "lab-1", tid, "complete"), ev); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool { return f.b.transfers.Get(tid) == nil }) {
		t.Fatal("the receiver's decision never left the tracker")
	}
}

// requireExactPair is the full START/TERMINAL check: one row per slot, the same
// admission nonce, the v1 terminal slot formula, the best-effort mark and no
// receiver authority, with the physical bytes equal to the canonical bytes.
func requireExactPair(t *testing.T, js jetstream.JetStream, tid, nonce string) []byte {
	t.Helper()
	rows, err := legacyHistoryRows(js, "lab", tid)
	if err != nil || len(rows) != 2 || rows[0].rec.Kind != "start" || rows[1].rec.Kind != "complete" {
		t.Fatalf("history is not exactly START then complete: %d rows %v", len(rows), err)
	}
	for _, row := range rows {
		canonical, err := schema.CanonicalTransferAudit(row.rec)
		if err != nil || string(canonical.Bytes) != string(row.raw) || row.rec.EntryNonce != nonce || !row.rec.BestEffort ||
			row.rec.AgentInstance != "" || row.rec.IntentDigest != "" || row.rec.ObjectVersion != "" {
			t.Fatalf("history row lost its canonical cap0 identity: %s %v", row.raw, err)
		}
	}
	if rows[1].rec.TerminalID != schema.TransferTerminalID("lab", tid, nonce) || rows[1].rec.Bytes != 4 {
		t.Fatalf("terminal is not bound to the admission slot: %+v", rows[1].rec)
	}
	return rows[1].raw
}

func (f *legacyHistoryFixture) ledgerRows(tid string) int {
	n := 0
	for _, dir := range []string{f.b.xferInflightDir(), f.b.xferTerminalOutboxDir()} {
		if _, err := os.Stat(filepath.Join(dir, xferInflightFilename(tid))); err == nil {
			n++
		}
	}
	return n
}

func (f *legacyHistoryFixture) setHistoryMaxAge(t *testing.T, age time.Duration) {
	t.Helper()
	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	stream, err := f.js.Stream(ctx, jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxAge = age
	if _, err := f.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
}

// A new broker serving an old receiver retains the cap0 admission identity
// before forwarding, binds the receiver's one decision to that identity, and
// retires its ledger only after exact history holds both rows. The old agent's
// own event stays the one compatibility event; a late opposite event changes
// nothing. Exact history that is refused keeps both ledger roles, and the
// recovery pass replays the identical staged decision once history is exact.
// origin: hot-update S7 plan §4 / §5.1 / §5.2 / §9 / L12 (S7 integration oplegacy F2)
func TestLegacyTransferHistoryRetainsAdmissionIdentity(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyTransferHistory(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy transfer history", g)
	testharness.AssertNoFDLeak(t, "legacy transfer history", fd, 0)
}

func exerciseLegacyTransferHistory(t *testing.T) {
	f := newLegacyHistoryFixture(t, nil)
	nonce := f.push(t, "tid-exact")
	f.complete(t, "tid-exact")
	terminal := requireExactPair(t, f.js, "tid-exact", nonce)
	if f.ledgerRows("tid-exact") != 0 {
		t.Fatal("exactly confirmed transfer kept its ledger rows")
	}
	if msg, err := f.events.NextMsg(time.Second); err != nil || msg.Subject != proto.SubjEvTransfer("lab", "lab-1", "tid-exact", "complete") {
		t.Fatalf("the receiver's own compatibility event is missing: %v", err)
	}
	if msg, err := f.events.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("the broker emitted a second compatibility event: %s", msg.Subject)
	}
	late, _ := json.Marshal(proto.TransferEvent{Kind: "failed", Verb: "push", TransferID: "tid-exact", Tier: "a", Code: "late"})
	f.b.handleEvTransfer(&nats.Msg{Subject: proto.SubjEvTransfer("lab", "lab-1", "tid-exact", "failed"), Data: late})
	if again := requireExactPair(t, f.js, "tid-exact", nonce); string(again) != string(terminal) || f.ledgerRows("tid-exact") != 0 {
		t.Fatal("a late opposite event changed the decided history")
	}

	// Exact history refuses (MaxAge is not a durable CAS horizon): the decision is
	// staged, the TERMINAL is withheld and both roles stay retained.
	nonce = f.push(t, "tid-blocked")
	f.setHistoryMaxAge(t, time.Hour)
	f.complete(t, "tid-blocked")
	rows, err := legacyHistoryRows(f.js, "lab", "tid-blocked")
	staged, rerr := readXferInflight(filepath.Join(f.b.xferInflightDir(), xferInflightFilename("tid-blocked")))
	if err != nil || len(rows) != 1 || rows[0].rec.Kind != "start" || rerr != nil || staged.Start == nil || staged.Terminal == nil ||
		staged.Terminal.EntryNonce != nonce || staged.Terminal.Kind != "complete" {
		t.Fatalf("refused history did not retain the staged decision: rows=%d err=%v staged=%+v %v", len(rows), err, staged.Terminal, rerr)
	}
	want, err := schema.CanonicalTransferAudit(*staged.Terminal)
	if err != nil {
		t.Fatal(err)
	}
	f.setHistoryMaxAge(t, 0)
	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	if _, err := f.b.finalizeStrandedXfers(ctx); err != nil {
		t.Fatal(err)
	}
	if got := requireExactPair(t, f.js, "tid-blocked", nonce); string(got) != string(want.Bytes) || f.ledgerRows("tid-blocked") != 0 {
		t.Fatal("recovery did not replay the identical staged decision before retiring it")
	}

	// The START itself was refused at admission: the live settlement must publish
	// the retained START first and only then the TERMINAL, never retire on the
	// TERMINAL alone.
	f.setHistoryMaxAge(t, time.Hour)
	f.startRefused = true
	nonce = f.push(t, "tid-late-start")
	f.startRefused = false
	f.setHistoryMaxAge(t, 0)
	f.complete(t, "tid-late-start")
	requireExactPair(t, f.js, "tid-late-start", nonce)
	if f.ledgerRows("tid-late-start") != 0 {
		t.Fatal("settled transfer kept its ledger rows")
	}

	// Cluster mode: the Raft forward never carries a canonical cap0 row, so a
	// mixed-version leader can neither strip its identity nor publish a plain
	// copy; the origin's exact writer alone produces the same exact pair.
	var forwardMu sync.Mutex
	var forwarded []schema.AuditTransfer
	cluster := newLegacyHistoryFixture(t, func(rec schema.AuditTransfer, _ func()) {
		forwardMu.Lock()
		defer forwardMu.Unlock()
		forwarded = append(forwarded, rec)
	})
	nonce = cluster.push(t, "tid-cluster")
	cluster.complete(t, "tid-cluster")
	requireExactPair(t, cluster.js, "tid-cluster", nonce)
	forwardMu.Lock()
	defer forwardMu.Unlock()
	if len(forwarded) != 0 || cluster.ledgerRows("tid-cluster") != 0 {
		t.Fatalf("a canonical cap0 row rode the Raft forward or kept its ledger: forwarded=%d", len(forwarded))
	}
}

// Every JetStream broker installs the continuation owner, so its finalize pass
// must still drive the cap0 ledger beside it: a stranded canonical START is
// synthesized into its bound #57 failure and settled through exact history,
// and the owner's own advance still runs first. Only a missing owner refuses.
// origin: hot-update S7 plan §5.1 / §5.3 / M13 (S7 integration oplegacy F2)
func TestInstalledTransferOwnerStillDrivesLegacyLedger(t *testing.T) {
	url := testharness.StartJSNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := contextWithTestTimeout()
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	b, g, p := newWiringGraph(t)
	b.cfg.ClusterDataDir = t.TempDir()
	setBrokerJS(b, js)
	defer setBrokerJS(b, nil)
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	p.takeCalls()
	started := time.Now().Add(-time.Hour)
	start, retained := legacyTransferLedger(b).Admit(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "start", Verb: "push",
		Ts: started, Session: "lab", Node: "lab-1", ActorNkey: "UACTOR", ActorFp: "fp", TransferID: "tid-stranded", Path: "/dst", Size: 4, Tier: "a"})
	if retained == nil {
		t.Fatal("fixture: no canonical admission")
	}
	if err := b.writeLedgerRecord(b.xferInflightDir(), xferInflightRecord{TransferID: start.TransferID, Session: start.Session, Node: start.Node,
		ActorNkey: start.ActorNkey, ActorFp: start.ActorFp, Verb: start.Verb, Tier: start.Tier, Path: start.Path, StartedAt: started, Size: 4, Start: retained}); err != nil {
		t.Fatal(err)
	}
	if n, err := b.finalizeStrandedXfers(ctx); err != nil || n != 1 {
		t.Fatalf("installed owner starved the cap0 ledger: finalized=%d err=%v", n, err)
	}
	if got := p.takeCalls(); len(got) == 0 || got[len(got)-1] != "advance" {
		t.Fatalf("the continuation owner's own advance did not run: %v", got)
	}
	rows, err := legacyHistoryRows(js, "lab", "tid-stranded")
	if err != nil || len(rows) != 2 || rows[0].rec.EntryNonce != start.EntryNonce || rows[1].rec.Code != "home_broker_restart" ||
		rows[1].rec.TerminalID != schema.TransferTerminalID("lab", "tid-stranded", start.EntryNonce) {
		t.Fatalf("stranded canonical row was not settled exactly: %d rows %v", len(rows), err)
	}
	for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
		if _, err := os.Stat(filepath.Join(dir, xferInflightFilename("tid-stranded"))); err == nil {
			t.Fatalf("settled cap0 row survived in %s", dir)
		}
	}
	b.transferRuntime.Store(nil)
	b.cfg.TransferFactory = func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error) {
		t.Fatal("finalize invoked the constructor")
		return nil, nil
	}
	if _, err := b.finalizeStrandedXfers(ctx); err == nil {
		t.Fatal("a missing owner fell back to legacy synthesis")
	}
}
