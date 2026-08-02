package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// startNATS launches an embedded NATS server on an ephemeral port and returns
// the client URL. The server is shut down via t.Cleanup.
// B9: delegates to the single implementation in internal/testharness. The body here was
// character-for-character identical to testharness.StartNATS — same DefaultTestOptions, same Port = -1,
// same Shutdown+WaitForShutdown cleanup, same 2s readiness deadline. The local name is kept so the ~40
// call sites in this package read unchanged.
func startNATS(t *testing.T) string {
	t.Helper()
	return testharness.StartNATS(t)
}

// openDB returns a fresh in-memory SQLite. Some broker tests want the
// "lab" session pre-seeded so node.Register has a valid FK target;
// callers that need session-create flow itself should NOT use this.
//
// Use :memory: in unit tests — modernc.org/sqlite occasionally leaves a
// transient -journal file behind that races t.TempDir's RemoveAll. The
// full-file path is exercised by test/p2 e2e instead.
// openDB delegates to internal/testharness (B9) — see the note in internal/session/session_test.go.
// openDBWithSession below still builds on it, so the seeding variants stay local where they belong.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return testharness.OpenDB(t)
}

// openDBWithSession returns openDB(t) plus a single ACTIVE session row
// for `sid`, so node.Register can target it. Use this in tests that
// don't go through `tether session create`.
func openDBWithSession(t *testing.T, sid string) *sql.DB {
	t.Helper()
	db := openDB(t)
	if _, err := db.Exec(
		`INSERT INTO sessions(sid, name, owner_pubkey_fp, pin_hash) VALUES (?,?,?,?)`,
		sid, sid, "SHA256:test-owner", "test-hash",
	); err != nil {
		t.Fatalf("seed session %q: %v", sid, err)
	}
	return db
}

// B9: delegates to internal/testharness. The local body discarded unconditionally; the shared one adds
// the TETHER_TEST_VERBOSE escape hatch, which is set nowhere in the repo, so the default path is
// byte-identical — and a failing leak or reconcile test is exactly when you want those logs back.
func silentLogger() *slog.Logger {
	return testharness.SilentLog()
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("New must reject empty NATSURL")
	}
	if _, err := New(Config{NATSURL: "nats://x"}); err == nil {
		t.Error("New must reject nil DB")
	}
}

// Run a real broker against an embedded NATS, send one register.req and one
// heartbeat, and assert the node row is ONLINE with a fresh heartbeat
// timestamp. Exercises subject parsing, JSON decode, and the node package
// integration end-to-end (within one process).
func TestBrokerRegisterAndHeartbeat(t *testing.T) {
	url := startNATS(t)
	db := openDBWithSession(t, "lab")

	b, err := New(Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLogger(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// Wait for subscriptions to be installed.
	waitNATSReady(t, url)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// register.req
	req := proto.NodeRegisterReq{
		ProtoVersion: proto.ProtoVersion, ReleaseVersion: proto.ReleaseVersion,
		NID: "lab-1", OS: "linux", Arch: "amd64",
	}
	payload, _ := json.Marshal(req)
	reply, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), payload, 2*time.Second)
	if err != nil {
		t.Fatalf("register request: %v", err)
	}
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("register rejected: code=%s err=%s", resp.Code, resp.Error)
	}

	// heartbeat (no reply)
	hb, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now().UTC()})
	if err := nc.Publish(proto.SubjNodeHeartbeat("lab", "lab-1"), hb); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Poll DB until visible.
	deadline := time.Now().Add(2 * time.Second)
	for {
		snaps, err := node.List(db)
		if err != nil {
			t.Fatal(err)
		}
		if len(snaps) == 1 && snaps[0].Status == "ONLINE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node never appeared ONLINE: %+v", snaps)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Run returned non-canceled error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not exit on cancel")
	}
}

// Bad register payloads must be rejected without crashing the broker.
func TestBrokerRejectsBadPayloads(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)

	b, _ := New(Config{NATSURL: url, DB: db, Logger: silentLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()
	waitNATSReady(t, url)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	cases := []struct {
		name    string
		subject string
		body    []byte
		wantErr string
	}{
		{
			name:    "garbage_json",
			subject: proto.SubjNodeRegister("lab", "lab-1"),
			body:    []byte("not json"),
			wantErr: "json_parse",
		},
		{
			name:    "nid_mismatch",
			subject: proto.SubjNodeRegister("lab", "lab-1"),
			body: mustJSON(proto.NodeRegisterReq{
				ProtoVersion: proto.ProtoVersion, NID: "different-nid",
			}),
			wantErr: "nid_mismatch",
		},
		{
			name:    "proto_mismatch",
			subject: proto.SubjNodeRegister("lab", "lab-1"),
			body: mustJSON(proto.NodeRegisterReq{
				ProtoVersion: proto.ProtoVersion + 99,
				NID:          "lab-1",
			}),
			wantErr: "proto_mismatch",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reply, err := nc.Request(c.subject, c.body, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var resp proto.NodeRegisterResp
			_ = json.Unmarshal(reply.Data, &resp)
			if resp.OK {
				t.Fatal("expected reject, got OK")
			}
			if resp.Code != c.wantErr {
				t.Errorf("code = %q, want %q (msg=%q)", resp.Code, c.wantErr, resp.Error)
			}
		})
	}
}

// origin: upgrade-safety plan §2 — N-1 window nail for the register handshake
// (architecture §21.1 site #1).
//
// The handshake pins EXACT ProtoVersion equality — deliberately NOT an
// [N-1, N] acceptance window. The N-1 compatibility unit is the release; the
// wire epoch (ProtoVersion) is frozen inside the window, so same-window peers
// are always epoch-equal and this check never fires on them. A peer from the
// PREVIOUS epoch publishes on the tether.v(N-1).* subject tree this broker
// does not even subscribe, so an acceptance branch here would be unreachable
// dead code advertising a compatibility that transport routing denies.
// If you are bumping ProtoVersion and this test got in your way: you owe the
// dual-tree subscription first — see architecture §21.4.
func TestRegisterProtoEqualityIsExact(t *testing.T) {
	url := startNATS(t)
	db := openDBWithSession(t, "lab") // the accept arm must clear the C.1 session gate in every mode

	b, _ := New(Config{NATSURL: url, DB: db, Logger: silentLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()
	waitNATSReady(t, url)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	cases := []struct {
		name       string
		protoVer   int
		wantOK     bool
		wantReject string
	}{
		{name: "exact_epoch_accepted", protoVer: proto.ProtoVersion, wantOK: true},
		{name: "previous_epoch_rejected", protoVer: proto.ProtoVersion - 1, wantReject: "proto_mismatch"},
		{name: "next_epoch_rejected", protoVer: proto.ProtoVersion + 1, wantReject: "proto_mismatch"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := mustJSON(proto.NodeRegisterReq{
				ProtoVersion: c.protoVer, ReleaseVersion: proto.ReleaseVersion,
				NID: "lab-1", OS: "linux", Arch: "amd64",
			})
			reply, err := nc.Request(proto.SubjNodeRegister("lab", "lab-1"), body, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var resp proto.NodeRegisterResp
			if err := json.Unmarshal(reply.Data, &resp); err != nil {
				t.Fatal(err)
			}
			if c.wantOK {
				if !resp.OK {
					t.Fatalf("same-epoch register must succeed; got code=%q err=%q", resp.Code, resp.Error)
				}
				return
			}
			if resp.OK || resp.Code != c.wantReject {
				t.Errorf("proto=%d: got ok=%v code=%q, want reject %q", c.protoVer, resp.OK, resp.Code, c.wantReject)
			}
		})
	}
}

// Heartbeat for an unknown node must not crash the broker (warning log only).
func TestHeartbeatForUnknownNodeIgnored(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)

	b, _ := New(Config{NATSURL: url, DB: db, Logger: silentLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = b.Run(ctx) }()
	waitNATSReady(t, url)

	nc, _ := nats.Connect(url)
	defer nc.Close()

	hb, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now()})
	if err := nc.Publish(proto.SubjNodeHeartbeat("ghost", "phantom"), hb); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	// Give the broker a moment to process.
	time.Sleep(100 * time.Millisecond)

	snaps, _ := node.List(db)
	if len(snaps) != 0 {
		t.Errorf("ghost heartbeat should not create a node row: %+v", snaps)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// waitNATSReady polls until the broker's register subscription is live by
// sending a deliberately bad JSON payload (the broker rejects with code
// "json_parse" before touching the DB, so this probe leaves no rows behind).
func waitNATSReady(t *testing.T, url string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := nc.Request(
			proto.SubjNodeRegister("readyprobe", "readyprobe"),
			[]byte("not-json"),
			200*time.Millisecond,
		)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker not ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ----------------------------------------------------------------------
// ps-retention-plan §B — periodic GC ticker.
// ----------------------------------------------------------------------

// insertExitedRow / insertRunningRow are direct-SQL inserts that bypass
// the proc.Insert FK-checking path (the broker_test.go fixtures don't
// always seed a matching node row). They mimic what tests in §A use.
func insertExitedRow(t *testing.T, db *sql.DB, pid string, endedAt time.Time) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash)
		 VALUES ('lab','lab','SHA256:o','ph')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(sid, nid, status) VALUES ('lab','lab-1','ONLINE')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, ended_at, status, started_by_fp)
		 VALUES (?,?,?,?,?,?,?,?)`,
		pid, "lab", "lab-1", `["x"]`,
		endedAt.Add(-time.Second), endedAt, "EXITED", "SHA256:u",
	); err != nil {
		t.Fatal(err)
	}
}

func insertRunningRow(t *testing.T, db *sql.DB, pid string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO sessions(sid, name, owner_pubkey_fp, pin_hash)
		 VALUES ('lab','lab','SHA256:o','ph')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO nodes(sid, nid, status) VALUES ('lab','lab-1','ONLINE')`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO processes(pid, sid, nid, argv, started_at, status, started_by_fp)
		 VALUES (?,?,?,?,?,?,?)`,
		pid, "lab", "lab-1", `["x"]`, time.Now(), "RUNNING", "SHA256:u",
	); err != nil {
		t.Fatal(err)
	}
}

// B1 — TestProcGCTicker_RemovesAgedExited: the periodic GC sweep
// deletes EXITED rows past cutoff and leaves RUNNING rows alone.
func TestProcGCTicker_RemovesAgedExited(t *testing.T) {
	natsURL := startNATS(t)
	db := openDB(t)

	// Seed: 1 EXITED with ended_at = 1s ago, 1 RUNNING.
	insertExitedRow(t, db, "old", time.Now().Add(-time.Second))
	insertRunningRow(t, db, "alive")

	br, err := New(Config{
		NATSURL:           natsURL,
		DB:                db,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReconcileInterval: time.Hour, // silence reconcile in this test
		ProcRetention:     50 * time.Millisecond,
		ProcGCInterval:    30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- br.Run(ctx) }()

	// Wait for at least 3 GC ticks (~90ms); 200ms is generous.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var c int
		_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='EXITED'`).Scan(&c)
		if c == 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	var c int
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='EXITED'`).Scan(&c)
	if c != 0 {
		t.Errorf("EXITED count after GC: want 0 got %d", c)
	}
	var r int
	_ = db.QueryRow(`SELECT COUNT(*) FROM processes WHERE status='RUNNING'`).Scan(&r)
	if r != 1 {
		t.Errorf("RUNNING count after GC: want 1 got %d", r)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Run() returned %v, want context.Canceled or nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after ctx cancel")
	}
}

// B2 — defaults from broker.New are applied when Config leaves them zero.
func TestProcGCTicker_DefaultsApplied(t *testing.T) {
	db := openDB(t)
	br, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222",
		DB:      db,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if br.cfg.ProcRetention != time.Hour {
		t.Errorf("ProcRetention default: want 1h got %v", br.cfg.ProcRetention)
	}
	if br.cfg.ProcGCInterval != 5*time.Minute {
		t.Errorf("ProcGCInterval default: want 5m got %v", br.cfg.ProcGCInterval)
	}
}

// B3 — broker Run() returns when ctx is canceled; GC ticker stops cleanly.
func TestProcGCTicker_ShutdownClean(t *testing.T) {
	natsURL := startNATS(t)
	db := openDB(t)

	br, err := New(Config{
		NATSURL:           natsURL,
		DB:                db,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReconcileInterval: time.Hour,
		ProcRetention:     time.Hour,
		ProcGCInterval:    50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- br.Run(ctx) }()

	time.Sleep(120 * time.Millisecond) // let at least one tick fire
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Errorf("Run() returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return within 2s of ctx cancel")
	}
}
