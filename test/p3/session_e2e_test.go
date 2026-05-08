// Package p3_test exercises the P3 session/auth surface end-to-end:
// embedded NATS + in-process broker + two simulated CLI clients with
// distinct nkey identities. Drives session create / list / join / rm and
// verifies multi-shell isolation via the TETHER_SESSION env var.
package p3_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
	natstest "github.com/nats-io/nats-server/v2/test"
)

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

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func startBroker(t *testing.T, url string, db *sql.DB) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL: url, DB: db,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
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
	waitBrokerReady(t, url)

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit on cancel")
		}
	}
}

// waitBrokerReady polls a known broker subject with a payload that's rejected
// at the JSON-parse stage (no DB writes).
func waitBrokerReady(t *testing.T, url string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := nc.Request(
			proto.SubjCtrlSessionCreate("U"+string(make([]byte, 55))),
			[]byte("not-json"), 200*time.Millisecond,
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

// freshIdentity generates a fresh CLI identity in an isolated tether home.
func freshIdentity(t *testing.T) (*cli.Identity, string) {
	t.Helper()
	home := t.TempDir()
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	return id, home
}

func TestSessionCreateAndOwnerCanList(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	stop := startBroker(t, url, db)
	defer stop()

	id, _ := freshIdentity(t)
	nc, err := cli.ConnectNATSWithNkey(url, id)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// session create lab --pin 123456
	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "123456"})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(id.PublicKey), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("create rejected: %s", resp.Error)
	}
	if resp.SID != "lab" {
		t.Errorf("expected sid=lab, got %q", resp.SID)
	}
	if resp.OwnerFP != id.Fingerprint {
		t.Errorf("owner fp mismatch: got %q want %q", resp.OwnerFP, id.Fingerprint)
	}

	// session list — should see lab as owner.
	msg, err = nc.Request(proto.SubjCtrlSessionList(id.PublicKey), []byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var lresp proto.SessionListResp
	if err := json.Unmarshal(msg.Data, &lresp); err != nil {
		t.Fatal(err)
	}
	if lresp.Error != "" {
		t.Fatalf("list rejected: %s", lresp.Error)
	}
	if len(lresp.Sessions) != 1 || lresp.Sessions[0].SID != "lab" || !lresp.Sessions[0].IsOwner {
		t.Errorf("expected one ownership of 'lab', got %+v", lresp.Sessions)
	}
}

func TestSessionJoinAcceptsCorrectPIN(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer startBroker(t, url, db)()

	owner, _ := freshIdentity(t)
	ownerNC, _ := cli.ConnectNATSWithNkey(url, owner)
	defer ownerNC.Close()

	body, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "shared-secret"})
	msg, err := ownerNC.Request(proto.SubjCtrlSessionCreate(owner.PublicKey), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var cresp proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &cresp); err != nil || cresp.Error != "" {
		t.Fatalf("create: %v %v", err, cresp.Error)
	}

	// Second identity joins with correct PIN.
	member, _ := freshIdentity(t)
	memberNC, _ := cli.ConnectNATSWithNkey(url, member)
	defer memberNC.Close()

	jbody, _ := json.Marshal(proto.SessionJoinReq{PIN: "shared-secret"})
	msg, err = memberNC.Request(proto.SubjCtrlSessionJoin(member.PublicKey, "lab"), jbody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var jresp proto.SessionJoinResp
	if err := json.Unmarshal(msg.Data, &jresp); err != nil {
		t.Fatal(err)
	}
	if !jresp.OK {
		t.Fatalf("join rejected (%s): %s", jresp.Code, jresp.Error)
	}
	if jresp.IsOwner {
		t.Errorf("PIN-joined member should NOT be owner")
	}

	// Member can list lab.
	msg, _ = memberNC.Request(proto.SubjCtrlSessionList(member.PublicKey), []byte("{}"), 2*time.Second)
	var lresp proto.SessionListResp
	_ = json.Unmarshal(msg.Data, &lresp)
	if len(lresp.Sessions) != 1 || lresp.Sessions[0].SID != "lab" || lresp.Sessions[0].IsOwner {
		t.Errorf("expected member visibility of lab, got %+v", lresp.Sessions)
	}
}

func TestSessionJoinRejectsWrongPIN(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer startBroker(t, url, db)()

	owner, _ := freshIdentity(t)
	ownerNC, _ := cli.ConnectNATSWithNkey(url, owner)
	defer ownerNC.Close()

	cbody, _ := json.Marshal(proto.SessionCreateReq{Name: "lab", PIN: "right-pin"})
	if _, err := ownerNC.Request(proto.SubjCtrlSessionCreate(owner.PublicKey), cbody, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	intruder, _ := freshIdentity(t)
	intruderNC, _ := cli.ConnectNATSWithNkey(url, intruder)
	defer intruderNC.Close()

	jbody, _ := json.Marshal(proto.SessionJoinReq{PIN: "wrong-pin"})
	msg, err := intruderNC.Request(proto.SubjCtrlSessionJoin(intruder.PublicKey, "lab"), jbody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var jresp proto.SessionJoinResp
	_ = json.Unmarshal(msg.Data, &jresp)
	if jresp.OK {
		t.Fatal("join must reject wrong PIN")
	}
	if jresp.Code != "invalid_pin" {
		t.Errorf("expected code invalid_pin, got %q", jresp.Code)
	}

	// Intruder must NOT appear in lab's members.
	msg, _ = intruderNC.Request(proto.SubjCtrlSessionList(intruder.PublicKey), []byte("{}"), 2*time.Second)
	var lresp proto.SessionListResp
	_ = json.Unmarshal(msg.Data, &lresp)
	if len(lresp.Sessions) != 0 {
		t.Errorf("intruder should see no sessions, got %+v", lresp.Sessions)
	}
}

func TestSessionRmOwnerOnly(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer startBroker(t, url, db)()

	owner, _ := freshIdentity(t)
	ownerNC, _ := cli.ConnectNATSWithNkey(url, owner)
	defer ownerNC.Close()
	_ = mustCreateSession(t, ownerNC, owner, "lab", "p")

	// Member joins.
	member, _ := freshIdentity(t)
	memberNC, _ := cli.ConnectNATSWithNkey(url, member)
	defer memberNC.Close()
	mustJoin(t, memberNC, member, "lab", "p")

	// Member tries rm — must be rejected as not_owner.
	msg, _ := memberNC.Request(
		proto.SubjCtrlSessionRm(member.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second,
	)
	var rresp proto.SessionRmResp
	_ = json.Unmarshal(msg.Data, &rresp)
	if rresp.OK || rresp.Code != "not_owner" {
		t.Fatalf("non-owner rm should be rejected with code=not_owner, got %+v", rresp)
	}

	// Owner rm — must succeed.
	msg, _ = ownerNC.Request(
		proto.SubjCtrlSessionRm(owner.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second,
	)
	rresp = proto.SessionRmResp{}
	_ = json.Unmarshal(msg.Data, &rresp)
	if !rresp.OK {
		t.Fatalf("owner rm should succeed, got %+v", rresp)
	}

	// Subsequent rm: already_deleting.
	msg, _ = ownerNC.Request(
		proto.SubjCtrlSessionRm(owner.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second,
	)
	rresp = proto.SessionRmResp{}
	_ = json.Unmarshal(msg.Data, &rresp)
	if rresp.OK || rresp.Code != "already_deleting" {
		t.Errorf("second rm should be already_deleting, got %+v", rresp)
	}
}

func TestSessionJoinRejectedAfterTombstone(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	defer startBroker(t, url, db)()

	owner, _ := freshIdentity(t)
	ownerNC, _ := cli.ConnectNATSWithNkey(url, owner)
	defer ownerNC.Close()
	mustCreateSession(t, ownerNC, owner, "lab", "p")

	// Owner tombstones.
	_, err := ownerNC.Request(
		proto.SubjCtrlSessionRm(owner.PublicKey, "lab"),
		[]byte("{}"), 2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Newcomer tries join — must be rejected as deleting.
	newcomer, _ := freshIdentity(t)
	newcomerNC, _ := cli.ConnectNATSWithNkey(url, newcomer)
	defer newcomerNC.Close()

	jbody, _ := json.Marshal(proto.SessionJoinReq{PIN: "p"})
	msg, err := newcomerNC.Request(proto.SubjCtrlSessionJoin(newcomer.PublicKey, "lab"), jbody, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var jresp proto.SessionJoinResp
	_ = json.Unmarshal(msg.Data, &jresp)
	if jresp.OK || jresp.Code != "deleting" {
		t.Errorf("post-tombstone join should be deleting, got %+v", jresp)
	}
}

// Multi-shell isolation: two CLI processes use distinct TETHER_SESSION env;
// cli.ReadCurrentSession in each "shell" returns the per-shell value.
func TestMultiShellSessionIsolation(t *testing.T) {
	homeA := t.TempDir()
	homeB := t.TempDir()
	if err := cli.WriteCurrentSession(homeA, "lab"); err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteCurrentSession(homeB, "prod"); err != nil {
		t.Fatal(err)
	}

	if got := readWithEnv(t, "TETHER_SESSION", "lab", homeA); got != "lab" {
		t.Errorf("shell A: got %q want lab", got)
	}
	if got := readWithEnv(t, "TETHER_SESSION", "prod", homeB); got != "prod" {
		t.Errorf("shell B: got %q want prod", got)
	}
	if got := readWithEnv(t, "TETHER_SESSION", "", homeA); got != "lab" {
		t.Errorf("shell A no env: should fall back to file 'lab', got %q", got)
	}
}

// Wrong PIN does NOT match the FreshArgon hash format → broker hashing path
// also exercised. (Sanity check: HashPIN works as expected.)
func TestArgonRoundtripUsedByBroker(t *testing.T) {
	hash, err := auth.HashPIN("hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifyPIN("hello", hash); err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifyPIN("nope", hash); !errors.Is(err, errors.New("pin: mismatch")) && err == nil {
		t.Fatal("VerifyPIN must reject wrong pin")
	}
}

// helpers ---------------------------------------------------------------------

func mustCreateSession(t *testing.T, nc *nats.Conn, id *cli.Identity, sid, pin string) string {
	t.Helper()
	body, _ := json.Marshal(proto.SessionCreateReq{Name: sid, PIN: pin})
	msg, err := nc.Request(proto.SubjCtrlSessionCreate(id.PublicKey), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionCreateResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != "" {
		t.Fatalf("create: %s", resp.Error)
	}
	return resp.SID
}

func mustJoin(t *testing.T, nc *nats.Conn, id *cli.Identity, sid, pin string) {
	t.Helper()
	body, _ := json.Marshal(proto.SessionJoinReq{PIN: pin})
	msg, err := nc.Request(proto.SubjCtrlSessionJoin(id.PublicKey, sid), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.SessionJoinResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("join %q: %s %s", sid, resp.Code, resp.Error)
	}
}

func readWithEnv(t *testing.T, key, val, home string) string {
	t.Helper()
	prev, hadPrev := os.LookupEnv(key)
	if val == "" {
		_ = os.Unsetenv(key)
	} else {
		_ = os.Setenv(key, val)
	}
	defer func() {
		if hadPrev {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	}()
	return cli.ReadCurrentSession(home)
}

// Compile-time check that filepath helper isn't unused (used by the helpers
// above through TempDir flow).
var _ = filepath.Join
