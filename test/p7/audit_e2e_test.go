// Package p7_test exercises P7 audit + history end-to-end with a
// JetStream-enabled embedded NATS, in-process broker, in-process
// agent. Same anonymous-auth harness as test/p4 / test/p5 / test/p6
// — the JetStream upgrade is independent of the auth_callout layer.
package p7_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ----- harness --------------------------------------------------------------
//
// Generic primitives live in internal/testharness; helpers below are
// the phase-specific bits.

func startJSNATS(t *testing.T) string            { return testharness.StartJSNATS(t) }
func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

func seedSession(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	stackharness.SeedSession(t, db, sid, fp)
}

func startBroker(t *testing.T, url string, db *sql.DB) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// JS init runs after subscriptions; give it a moment.
	time.Sleep(150 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

func startAgent(t *testing.T, url, sid, nid string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

func runExec(t *testing.T, nc *nats.Conn, sid, actor, nid string, argv []string) {
	t.Helper()
	body, _ := json.Marshal(proto.ExecReq{Argv: argv})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest(proto.SubjCmdBy(sid, actor, nid, "exec"), inbox, body); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(200 * time.Millisecond)
		if err != nil {
			continue
		}
		var c proto.ExecChunk
		if json.Unmarshal(msg.Data, &c) == nil && (c.Kind == "exit" || c.Kind == "error") {
			return
		}
	}
	t.Fatal("exec never completed")
}

// ----- tests ----------------------------------------------------------------

// TestAuditEntriesLandInHistoryStream is the architecture's main P7
// observation: 50 exec calls → tether history -n 100 sees them all
// in time order. We verify by reading the history-<sid> stream
// directly via JetStream consumer (the same path tether history
// uses).
func TestAuditEntriesLandInHistoryStream(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// session.Create (above) just inserts the row; the history stream
	// only gets created via handleSessionCreate. Pre-create it
	// directly so audit publishes from the in-process broker land.
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Drive 50 successful execs; each produces one audit.call
	// (verb=exec) AND two audit.proc (start + exit). We'll verify
	// both kinds land.
	const N = 50
	for i := 0; i < N; i++ {
		runExec(t, nc, "lab", pub, "lab-1", []string{"true"})
	}

	// Audit publishes land asynchronously after the agent's exit
	// chunk reaches ctl. Poll the stream's message count until at
	// least 3*N (= 50 audit.call + 50 audit.proc{start} + 50
	// audit.proc{exit}) instead of dead-reckoning a 300ms window
	// (audit shard 05 F1: 300ms is too tight on slow CI).
	js, _ := jetstream.New(nc)
	stream, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
	if err != nil {
		t.Fatal(err)
	}
	var lastMsgs uint64
	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		info, err := stream.Info(context.Background())
		if err != nil {
			return false
		}
		lastMsgs = info.State.Msgs
		return info.State.Msgs >= uint64(3*N)
	}) {
		t.Fatalf("history-lab too few messages after 5s: got %d want >= %d", lastMsgs, 3*N)
	}

	// Sanity: replay all of them and bucket by audit kind.
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	it, err := cons.Messages()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Stop()

	counts := map[string]int{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := it.Next()
		if err != nil {
			break
		}
		toks := strings.Split(msg.Subject(), ".")
		counts[toks[len(toks)-1]]++
		meta, _ := msg.Metadata()
		if meta != nil && meta.NumPending == 0 {
			break
		}
	}
	if counts["call"] < N {
		t.Errorf("audit.call count: got %d want >= %d", counts["call"], N)
	}
	if counts["proc"] < 2*N {
		t.Errorf("audit.proc count: got %d want >= %d", counts["proc"], 2*N)
	}
}

// TestSessionRmDeletesStreamAndCascades runs the architecture H.3 3-phase
// rm end-to-end with JetStream on. After rm:
//   - history-lab stream is GONE,
//   - SQLite has no `lab` rows in any table,
//   - sys.events received a session_destroyed event.
func TestSessionRmDeletesStreamAndCascades(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	// origin: prerelease audit round 2 — `session create` now requires an admitted
	// fingerprint, the same sentence a real deployment says with
	// `tether admin session-allow`.
	if _, err := session.AllowCreator(db, fp, "test", "", time.Now()); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db)()

	// Drive session create through the real handler so the stream gets
	// EnsureHistoryStream'd and a session_created event is emitted.
	nc, _ := nats.Connect(url)
	defer nc.Close()

	createBody, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "test-pin-1234"})
	respMsg, err := nc.Request(proto.SubjCtrlBy(pub, "session.create.req"), createBody, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var createResp proto.SessionCreateResp
	if err := json.Unmarshal(respMsg.Data, &createResp); err != nil {
		t.Fatal(err)
	}
	if createResp.Error != "" {
		t.Fatalf("session.create: %s", createResp.Error)
	}
	if createResp.OwnerFP != fp {
		t.Errorf("owner_fp: got %q want %q", createResp.OwnerFP, fp)
	}

	// Stream must exist now.
	js, _ := jetstream.New(nc)
	if _, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab")); err != nil {
		t.Fatalf("history-lab missing right after create: %v", err)
	}

	// Subscribe to sys.events before triggering rm.
	gotDestroyed := make(chan struct{}, 1)
	subSys, _ := nc.Subscribe(proto.SubjSysEvents, func(m *nats.Msg) {
		if strings.Contains(string(m.Data), "session_destroyed") {
			select {
			case gotDestroyed <- struct{}{}:
			default:
			}
		}
	})
	defer func() { _ = subSys.Unsubscribe() }()

	// rm by owner.
	rmBody, _ := json.Marshal(proto.SessionRmReq{})
	rmMsg, err := nc.Request(proto.SubjCtrlSessionRm(pub, "lab"), rmBody, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var rmResp proto.SessionRmResp
	if err := json.Unmarshal(rmMsg.Data, &rmResp); err != nil {
		t.Fatal(err)
	}
	if !rmResp.OK {
		t.Fatalf("rm rejected: %s %s", rmResp.Code, rmResp.Error)
	}

	// Stream must be gone.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab"))
		if err != nil {
			break // gone — good
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab")); err == nil {
		t.Error("history-lab stream still exists after rm")
	}

	// SQLite rows for lab must be cascaded away.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='lab'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("sessions row still present after rm: count=%d", count)
	}

	// sys.events session_destroyed must have been observed.
	select {
	case <-gotDestroyed:
	case <-time.After(1 * time.Second):
		t.Error("sys.events session_destroyed never arrived")
	}
}

// TestDeletingSessionResumesOnBrokerRestart simulates a crash mid-rm:
// SQLite is in DELETING but the JS stream was never deleted. A fresh
// broker.Run on the same DB MUST finish phase ②③ on its own.
func TestDeletingSessionResumesOnBrokerRestart(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// Pre-create history stream + tombstone manually (= simulate
	// "broker died after phase ① and before phase ②").
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "lab", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	// Boot a broker — it should run reconcileHistoryStreamsOnBoot
	// which finds the DELETING row and resumes ②③.
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)

	deadline := time.Now().Add(2 * time.Second)
	streamGone := false
	rowsGone := false
	for time.Now().Before(deadline) {
		if _, err := js.Stream(context.Background(), jsstream.HistoryStreamName("lab")); err != nil {
			streamGone = true
		}
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE sid='lab'`).Scan(&n)
		if n == 0 {
			rowsGone = true
		}
		if streamGone && rowsGone {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("resume incomplete: streamGone=%v rowsGone=%v", streamGone, rowsGone)
}

// TestOrphanHistoryStreamCleanedOnBoot covers the third H.3 startup
// rule: a history-<sid> stream with NO matching SQLite session must
// be deleted by the broker on boot.
func TestOrphanHistoryStreamCleanedOnBoot(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)

	// Manually create a history-ghost stream with no DB session.
	{
		nc, _ := nats.Connect(url)
		js, _ := jetstream.New(nc)
		if err := jsstream.EnsureHistoryStream(context.Background(), js, "ghost", jsstream.ReplicasSingle); err != nil {
			t.Fatal(err)
		}
		nc.Close()
	}

	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := js.Stream(context.Background(), jsstream.HistoryStreamName("ghost")); err != nil {
			return // gone — good
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("orphan history-ghost stream survived broker boot")
}

// TestExecOnDeletingSessionRejected pins the C.1 §6 contract under
// JS-enabled mode (was already covered by test/p4 pre-JS, but this
// makes sure the H.3 phase ① tombstone is observed by exec
// pre-flight even before phase ② / ③ get a chance to run).
func TestExecOnDeletingSessionRejected(t *testing.T) {
	url := startJSNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Broker boot will finalize the rm (stream + rows gone). For this
	// test we want to catch the moment between ① and ② → keep the
	// stream-not-existing tolerant by NOT pre-creating it. The exec
	// req should be rejected by C.1 §6 regardless.
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.ExecReq{Argv: []string{"echo", "x"}})
	respMsg, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "exec"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var c proto.ExecChunk
	_ = json.Unmarshal(respMsg.Data, &c)
	if c.Kind != "error" || !strings.Contains(c.Error, "session_not_found_or_deleting") {
		t.Errorf("expected session_not_found_or_deleting, got %+v", c)
	}
}
