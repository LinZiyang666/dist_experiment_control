package p10_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// ----- harness --------------------------------------------------------------

func startNATS(t *testing.T) string              { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

func seedSession(t *testing.T, db *sql.DB, sid, ownerFP string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, ownerFP, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

func startBroker(t *testing.T, url string, db *sql.DB, allow []string) func() {
	t.Helper()
	ready := make(chan struct{})
	b, err := broker.New(broker.Config{
		NATSURL:                  url,
		DB:                       db,
		Logger:                   silentLog(),
		ReconcileInterval:        50 * time.Millisecond,
		StaleAfter:               300 * time.Millisecond,
		OfflineAfter:             900 * time.Millisecond,
		UpgradeURLAllowlist:      allow,
		UpgradeForwardTimeoutDur: 5 * time.Second,
		ReadyCh:                  ready,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Wait for broker.Run to actually install all subscriptions —
	// the previous "WaitConnect + assume Run is fast enough" was
	// racy on slow CI runners (TestUpgradeNonOwnerRejected hit
	// "no responders" intermittently). ReadyCh closes only after
	// every Subscribe + a Flush round-trip to the NATS server.
	testharness.WaitConnect(t, url, 3*time.Second)
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("broker did not signal ReadyCh in 3s")
	}
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

func startAgentWithUpgrade(t *testing.T, url, sid, nid string, exePath string, allow []string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:               url,
		SID:                   sid,
		NID:                   nid,
		Logger:                silentLog(),
		HeartbeatInterval:     100 * time.Millisecond,
		RegisterTimeout:       2 * time.Second,
		UpgradeURLAllowlist:   allow,
		UpgradeNoExit:         true, // CRITICAL: do not let an upgrade os.Exit the test runner
		UpgradeExecutablePath: exePath,
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

// upgradeRequest fires one upgrade.req via NATS and returns the
// decoded reply.
func upgradeRequest(t *testing.T, nc *nats.Conn, sid, actor, nid string, req proto.UpgradeReq) proto.UpgradeResp {
	t.Helper()
	body, _ := json.Marshal(req)
	respMsg, err := nc.Request(proto.SubjCmdBy(sid, actor, nid, "upgrade"), body, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// makeTarball returns (tarballBytes, sha256Hex) for a one-file
// archive containing a `tether` binary whose contents are content.
func makeTarball(t *testing.T, content []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{
		Name: "tether",
		Mode: 0o755,
		Size: int64(len(content)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// ----- broker-side gate tests ----------------------------------------------

// TestUpgradeURLNotInAllowlistRejected: broker rejects URLs whose
// prefix is not configured. No agent involvement; fails before the
// .forwarded subject is ever published.
func TestUpgradeURLNotInAllowlistRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`,
		time.Now().UTC(), time.Now().UTC(),
	)

	defer startBroker(t, url, db, []string{"https://allowed.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          "https://evil.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK {
		t.Errorf("expected reject for non-allowed URL; got OK")
	}
	if resp.Code != "url_not_allowed" {
		t.Errorf("expected code=url_not_allowed; got %q (%s)", resp.Code, resp.Error)
	}
}

// TestUpgradeBadSHA256FormatRejected: broker pre-validates the
// 64-hex format before forwarding (cheap sanity, agent does the
// real cryptographic verify).
func TestUpgradeBadSHA256FormatRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`,
		time.Now().UTC(), time.Now().UTC(),
	)

	defer startBroker(t, url, db, []string{"https://allowed.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       "not-hex",
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK || resp.Code != "sha256_invalid" {
		t.Errorf("expected sha256_invalid; got %+v", resp)
	}
}

// TestUpgradeProtoMismatchRejected: cross-proto upgrade attempts
// must fail (architecture J.4 routes those through reinstall, not
// this verb).
func TestUpgradeProtoMismatchRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`,
		time.Now().UTC(), time.Now().UTC(),
	)

	defer startBroker(t, url, db, []string{"https://allowed.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion + 99,
	})
	if resp.OK || resp.Code != "proto_bump_requires_reinstall" {
		t.Errorf("expected proto_bump_requires_reinstall; got %+v", resp)
	}
}

// TestUpgradeNonOwnerRejected: a session member who is NOT the
// owner cannot trigger an upgrade.
func TestUpgradeNonOwnerRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seedSession(t, db, "lab", ownerFP)

	intruderPub, intruderFP := freshUserPub(t)
	if err := session.AddMember(db, "lab", intruderFP, session.RoleMember, session.ViaPin, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`,
		time.Now().UTC(), time.Now().UTC(),
	)

	defer startBroker(t, url, db, []string{"https://allowed.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", intruderPub, "lab-1", proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK || resp.Code != "not_owner" {
		t.Errorf("expected not_owner; got %+v", resp)
	}
}

// TestUpgradeEmptyAllowlistRejectsAll: J.4 § 安全约束 requires the
// operator to opt in to upgrades by configuring an allowlist; an
// empty allow rejects every URL.
func TestUpgradeEmptyAllowlistRejectsAll(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'ONLINE', ?)`,
		time.Now().UTC(), time.Now().UTC(),
	)

	defer startBroker(t, url, db, nil)() // no allowlist

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          "https://github.com/LinZiyang666/tether/releases/download/v0.0.1/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK || resp.Code != "url_not_allowed" {
		t.Errorf("expected url_not_allowed for empty allowlist; got %+v", resp)
	}
}

// TestUpgradeNodeOfflineRejected: agent-not-ONLINE pre-flight gate
// (mirrors handleExecReq).
func TestUpgradeNodeOfflineRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	_, _ = db.Exec(
		`INSERT INTO nodes(nid, sid, last_heartbeat_at, status, registered_at)
		 VALUES ('lab-1', 'lab', ?, 'OFFLINE', ?)`,
		time.Now().UTC().Add(-2*time.Minute), time.Now().UTC(),
	)

	defer startBroker(t, url, db, []string{"https://allowed.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          "https://allowed.example.com/tether.tar.gz",
		SHA256:       strings.Repeat("a", 64),
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK || resp.Code != "node_offline" {
		t.Errorf("expected node_offline; got %+v", resp)
	}
}

// ----- agent-side flow tests -----------------------------------------------

// TestUpgradeReplacesBinaryHappyPath drives the full path: live
// broker + live agent + a real local HTTP server hosting a fake
// tarball. The agent's UpgradeExecutablePath is pinned to a
// sandbox file so the install actually rewrites it (we then read
// it back and assert the new content matches). Without
// UpgradeExecutablePath the test would silently overwrite the
// go-test binary, which is gross even when it works.
func TestUpgradeReplacesBinaryHappyPath(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	binBody := []byte("#!/bin/sh\necho NEW-AGENT-V9.9.9\n")
	tarball, sum := makeTarball(t, binBody)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	// Sandbox the install target. Pre-write a sentinel so we can
	// detect the atomic rename actually happened.
	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", exePath, []string{srv.URL})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          srv.URL + "/tether.tar.gz",
		SHA256:       sum,
		ProtoVersion: proto.ProtoVersion,
	})
	if !resp.OK {
		t.Fatalf("expected OK upgrade; got %+v", resp)
	}

	// Sentinel must have been replaced with the new binary content.
	got, err := os.ReadFile(exePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, binBody) {
		t.Errorf("post-upgrade binary content:\n  got %q\n  want %q",
			string(got), string(binBody))
	}
	fi, err := os.Stat(exePath)
	if err != nil {
		t.Fatal(err)
	}
	if mode := fi.Mode().Perm(); mode != 0o755 {
		t.Errorf("post-upgrade binary mode: got %o want 0755", mode)
	}
}

// TestUpgradeAgentRejectsBadSHA: the broker accepts (its sanity
// regex says 64 hex chars are fine) but the agent's cryptographic
// verify against the actual download fails. Agent must reply
// sha256_mismatch and NOT touch its own binary.
func TestUpgradeAgentRejectsBadSHA(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, _ := makeTarball(t, []byte("real content"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", "", []string{srv.URL})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	bogus := strings.Repeat("d", 64)
	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          srv.URL + "/tether.tar.gz",
		SHA256:       bogus,
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK {
		t.Errorf("expected reject; got OK")
	}
	if !strings.Contains(resp.Code, "sha256_mismatch") {
		t.Errorf("expected sha256_mismatch in reply; got %+v", resp)
	}
}

// TestUpgradeAgentRejectsNonAllowlistedURL: even if the broker
// somehow forwarded a URL the agent doesn't trust (operator
// misconfiguration where agent's allowlist is stricter), agent
// MUST refuse. We simulate by giving broker a permissive allow
// list and agent a stricter one.
func TestUpgradeAgentRejectsNonAllowlistedURL(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, []byte("payload"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	// Broker accepts the test-server URL; agent only accepts
	// example.com so the mismatch surfaces as
	// url_not_allowed_local from the agent.
	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", "",
		[]string{"https://only-this.example.com/"})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
		URL:          srv.URL + "/tether.tar.gz",
		SHA256:       sum,
		ProtoVersion: proto.ProtoVersion,
	})
	if resp.OK {
		t.Errorf("expected reject; got OK")
	}
	if !strings.Contains(resp.Code, "url_not_allowed_local") {
		t.Errorf("expected url_not_allowed_local; got %+v", resp)
	}
}
