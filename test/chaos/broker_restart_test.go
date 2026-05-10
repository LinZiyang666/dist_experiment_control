// Broker-restart chaos tests. Cover dimension B from the chaos
// checklist: SQLite WAL recovery, ctl-during-restart behavior, and
// process state survival across a broker bounce.
package chaos_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestBrokerRestartSQLiteWALRecovery pins B.7: a broker that's
// killed mid-write must come back without SQLite corruption. We use
// an on-disk SQLite (modernc) so WAL semantics actually apply
// (in-memory ":memory:" trivially can't lose data because it's
// gone with the process).
//
// Sequence: (1) boot broker against on-disk db, register an agent,
// stamp some rows; (2) hard-cancel the broker (= simulated kill -9
// — the cleanest "non-graceful" we have without unsafe syscall
// games); (3) reopen the same db; (4) verify the rows are
// intact AND the next broker can read them.
func TestBrokerRestartSQLiteWALRecovery(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 20*time.Second)
	defer cancel()

	url := startNATS(t)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "state.db")

	db, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	// First broker. Run + stop fast to simulate non-graceful exit
	// (we still go through ctx-cancel — Go can't actually os.Kill its
	// own process from inside a test without taking the test runner
	// down, so "fast cancel + immediate close + reopen" is the closest
	// non-os-level approximation).
	bh1 := startBrokerExplicit(t, url, db)
	// Force one node row through real NATS register so the SQLite
	// actually has something to lose.
	body, _ := json.Marshal(proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion, NID: "n1",
	})
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if _, err := nc.Request(proto.SubjNodeRegister("lab", "n1"), body, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	bh1.Stop()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen same dbfile. If WAL recovery is broken this is where we'd
	// see "database is locked" / "malformed image" / panic.
	db2, err := storage.Open("file:" + dbPath)
	if err != nil {
		t.Fatalf("reopen db after restart: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	var status string
	if err := db2.QueryRow(
		`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
	).Scan(&status); err != nil {
		t.Fatalf("reopened db missing the row: %v", err)
	}
	// Status will be either ONLINE (if the restart ran fast enough that
	// the boot-reconcile didn't trip stale yet) or OFFLINE (if the
	// boot-reconcile decided the heartbeat was stale). Both are
	// recovery-OK; the bug we're guarding against is "row vanished" or
	// "db file corrupt".
	if status != "ONLINE" && status != "OFFLINE" && status != "STALE" {
		t.Errorf("unexpected status after restart: %q", status)
	}

	// Boot a second broker against the recovered db. Should come up
	// without complaint.
	_ = startBrokerExplicit(t, url, db2)

	// Issue a fresh ps request to confirm the broker is actually serving.
	psBody, _ := json.Marshal(proto.PsReq{})
	pub, _ := freshUserPub(t)
	respMsg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), psBody, 2*time.Second)
	if err != nil {
		t.Fatalf("ps after restart: %v", err)
	}
	var resp proto.PsResp
	_ = json.Unmarshal(respMsg.Data, &resp)
	// fp doesn't match (fresh keypair) so we expect not_a_member, NOT
	// a hang or store error — that's the "broker actually serving"
	// signal we want here.
	if resp.Code == "" {
		// owner happens to match? OK, just means ps came back.
		t.Logf("ps returned %d processes after restart", len(resp.Processes))
	} else if resp.Code != "not_a_member" {
		t.Errorf("ps after restart returned unexpected code: %s %s", resp.Code, resp.Error)
	}
	_ = ctx
}

// TestBrokerRestartCtlGetsErrorThenSucceeds pins B.8: a CTL request
// fired while the broker is down must get a clean error (no responders
// / deadline), and a retry after the broker is back must succeed.
func TestBrokerRestartCtlGetsErrorThenSucceeds(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 15*time.Second)
	defer cancel()

	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	bh := startBrokerExplicit(t, url, db)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	psBody, _ := json.Marshal(proto.PsReq{})

	// Sanity: ps works while broker is alive.
	if _, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), psBody, 2*time.Second); err != nil {
		t.Fatalf("baseline ps failed: %v", err)
	}

	// Kill broker. NATS stays up (this is the case where the broker
	// crashed but the bus didn't).
	bh.Stop()

	// Allow NATS a beat to notice the subscriber went away — without
	// this the request could land in an interest-pruning race window
	// where the no-responders header isn't yet set and the request
	// waits out its full deadline. 200ms is overkill on a fast box;
	// CI with a busy machine still resolves well under our 1s deadline.
	if !testharness.WaitFor(t, 2*time.Second, 50*time.Millisecond, func() bool {
		reqCtx, c := context.WithTimeout(ctx, 200*time.Millisecond)
		_, err := nc.RequestWithContext(reqCtx, proto.SubjCtrlPs(pub, "lab"), psBody)
		c()
		return err != nil // we WANT an error here
	}) {
		t.Fatal("requests still succeed after broker stop")
	}

	t0 := time.Now()
	reqCtx, reqCancel := context.WithTimeout(ctx, 1*time.Second)
	_, err = nc.RequestWithContext(reqCtx, proto.SubjCtrlPs(pub, "lab"), psBody)
	reqCancel()
	if err == nil {
		t.Fatal("expected error against killed broker")
	}
	if elapsed := time.Since(t0); elapsed > 1500*time.Millisecond {
		t.Errorf("request took %v — should have failed quickly", elapsed)
	}

	// Restart broker against same db. ps should work again.
	_ = startBrokerExplicit(t, url, db)

	if !testharness.WaitFor(t, 5*time.Second, 100*time.Millisecond, func() bool {
		_, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), psBody, 2*time.Second)
		return err == nil
	}) {
		t.Fatal("ps did not recover after broker restart")
	}
}

// TestBrokerRestartConvergesONLINEAfterReconnect pins B.10: a broker
// restart with a live agent must converge — the agent's MaxReconnects(-1)
// kicks in, agent re-registers, broker reconciles, the row is ONLINE.
//
// This is the production scenario for "broker rolling restart".
func TestBrokerRestartConvergesONLINEAfterReconnect(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 25*time.Second)
	defer cancel()
	_ = ctx

	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	bh1 := startBrokerExplicit(t, url, db)
	_ = startAgentExplicit(t, url, "lab", "n1")

	testharness.WaitNodeOnline(t, db, "lab", "n1", 5*time.Second)

	// Bounce the broker (NATS stays up so the agent's underlying
	// nats.Conn just keeps going).
	bh1.Stop()

	// Without anyone to handle heartbeats, the broker's boot reconcile
	// would mark the node OFFLINE on the next start — but actually the
	// agent keeps publishing heartbeats throughout. The new broker
	// should re-pick up the (sid, nid) on its next heartbeat handler.
	//
	// We give it a generous window because the agent's RegisterTimeout
	// and our broker's boot delay both add up.
	_ = startBrokerExplicit(t, url, db)

	if !testharness.WaitFor(t, 10*time.Second, 100*time.Millisecond, func() bool {
		var st string
		err := db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		return err == nil && st == "ONLINE"
	}) {
		var st string
		_ = db.QueryRow(
			`SELECT status FROM nodes WHERE sid='lab' AND nid='n1'`,
		).Scan(&st)
		t.Errorf("node did not reconverge to ONLINE after broker restart; status=%q", st)
	}
}
