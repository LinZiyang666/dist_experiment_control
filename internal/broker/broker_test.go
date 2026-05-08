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
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
	natstest "github.com/nats-io/nats-server/v2/test"
)

// startNATS launches an embedded NATS server on an ephemeral port and returns
// the client URL. The server is shut down via t.Cleanup.
func startNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// openDB returns a fresh in-memory SQLite. Some broker tests want the
// "lab" session pre-seeded so node.Register has a valid FK target;
// callers that need session-create flow itself should NOT use this.
//
// Use :memory: in unit tests — modernc.org/sqlite occasionally leaves a
// transient -journal file behind that races t.TempDir's RemoveAll. The
// full-file path is exercised by test/p2 e2e instead.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
