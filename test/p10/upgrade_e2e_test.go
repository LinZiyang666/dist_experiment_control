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
	"fmt"
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
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
)

// ----- harness --------------------------------------------------------------

func startNATS(t *testing.T) string              { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                    { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string) { return testharness.FreshUserPub(t) }

func seedSession(t *testing.T, db *sql.DB, sid, ownerFP string) {
	t.Helper()
	stackharness.SeedSession(t, db, sid, ownerFP)
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
	cfg := agent.Config{
		NATSURL:               url,
		SID:                   sid,
		NID:                   nid,
		Logger:                silentLog(),
		HeartbeatInterval:     100 * time.Millisecond,
		RegisterTimeout:       2 * time.Second,
		UpgradeURLAllowlist:   allow,
		UpgradeNoExit:         true, // CRITICAL: do not let an upgrade os.Exit the test runner
		UpgradeExecutablePath: exePath,
		UpgradeBootProofID:    "p10-upgrade",
	}
	if exePath != "" {
		// F1: the commit proof hashes the process's RUNNING image; in these
		// e2e fixtures the sandbox binary plays that role (the alternative —
		// the go-test binary — would make every commit test a sibling test).
		cfg.UpgradeRunningImagePath = exePath
	}
	a, err := agent.New(cfg)
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

// fakeTetherScript returns an EXECUTABLE stand-in for a tether release binary:
// a shell script that answers `version` the way cmd/tether does
// (`tether <tag> (proto vN)`). The upgrade-safety smoke gate execs the staged
// binary before the install flips, so happy-path fixtures must actually run —
// the old inert []byte("payload") fixtures only exercise the paths that fail
// BEFORE the smoke gate. origin: upgrade-safety plan §7 (fixture migration).
func fakeTetherScript(version string) []byte {
	return []byte("#!/bin/sh\necho 'tether " + version + " (proto v2)'\n")
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
// this verb). N-1 nail (internal review S15): the check is EXACT
// equality — the epoch below and the epoch above are both refused,
// deliberately not an [N-1, N] window (architecture §21.1: an N-1
// epoch peer lives on the tether.v(N-1).* subject tree this broker
// does not subscribe; an acceptance branch here would be dead code).
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

	for _, protoVer := range []int{proto.ProtoVersion - 1, proto.ProtoVersion + 1, proto.ProtoVersion + 99} {
		resp := upgradeRequest(t, nc, "lab", pub, "lab-1", proto.UpgradeReq{
			URL:          "https://allowed.example.com/tether.tar.gz",
			SHA256:       strings.Repeat("a", 64),
			ProtoVersion: protoVer,
		})
		if resp.OK || resp.Code != "proto_bump_requires_reinstall" {
			t.Errorf("proto=%d: expected proto_bump_requires_reinstall; got %+v", protoVer, resp)
		}
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

	binBody := fakeTetherScript("v9.9.9-test")
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
	// The smoke gate guarantees NewVersion is the NORMALIZED release tag (what
	// the new binary will report as ReleaseVersion on register) — ctl --wait's
	// equality check rides on this exact string.
	if resp.NewVersion != "v9.9.9-test" {
		t.Errorf("NewVersion: got %q, want the normalized tag %q", resp.NewVersion, "v9.9.9-test")
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

	// upgrade-safety plan §3.1: the flip must leave the OLD binary in the prev
	// slot and a pending marker beside it — that pair IS the rollback path.
	prev, err := os.ReadFile(exePath + ".prev")
	if err != nil {
		t.Fatalf("prev slot missing after install: %v", err)
	}
	if string(prev) != "OLD" {
		t.Errorf("prev slot content: got %q, want the pre-upgrade sentinel", string(prev))
	}
	markerRaw, err := os.ReadFile(filepath.Join(sandbox, ".tether-upgrade.json"))
	if err != nil {
		t.Fatalf("upgrade marker missing after install: %v", err)
	}
	var marker struct {
		State      string `json:"state"`
		NewVersion string `json:"new_version"`
	}
	if err := json.Unmarshal(markerRaw, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.State != "pending" || marker.NewVersion != "v9.9.9-test" {
		t.Errorf("marker after install: got state=%q new_version=%q, want pending/v9.9.9-test",
			marker.State, marker.NewVersion)
	}
}

// TestUpgradeSmokeGateRejectsNonBinary: a tarball whose sha VERIFIES but whose
// `tether` entry cannot exec (inert bytes — wrong arch, truncation, not a
// binary) must be refused by the smoke gate BEFORE anything on disk changes:
// no flip, no prev slot, no marker. origin: upgrade-safety plan §3.2 F2.
func TestUpgradeSmokeGateRejectsNonBinary(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, []byte("real content, but not executable"))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

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
	if resp.OK || !strings.Contains(resp.Code, "smoke_failed") {
		t.Fatalf("expected smoke_failed; got %+v", resp)
	}
	if got, err := os.ReadFile(exePath); err != nil || string(got) != "OLD" {
		t.Errorf("smoke_failed must leave the binary untouched; got %q err=%v", string(got), err)
	}
	if _, err := os.Stat(exePath + ".prev"); !os.IsNotExist(err) {
		t.Errorf("smoke_failed must not create a prev slot (err=%v)", err)
	}
	if _, err := os.Stat(filepath.Join(sandbox, ".tether-upgrade.json")); !os.IsNotExist(err) {
		t.Errorf("smoke_failed must not write an upgrade marker (err=%v)", err)
	}
}

// TestUpgradeCommitOnRegister: the health-check-in leg of the upgrade state
// machine, end to end — an agent that boots as the STAGED binary (pending
// marker whose new_sha matches its executable) registers with a live broker,
// and that first successful register promotes the marker to committed.
// origin: upgrade-safety plan §3.1 (register success = commit point).
func TestUpgradeCommitOnRegister(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("NEW-BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("NEW-BINARY"))
	prevSum := sha256.Sum256([]byte("OLD-BINARY"))
	if err := os.WriteFile(exePath+".prev", []byte("OLD-BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	pending := fmt.Sprintf(`{"state":"pending","prev_sha":%q,"new_sha":%q,`+
		`"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":1,"boot_budget":3,`+
		`"target_sid":"lab","target_nid":"lab-1","upgrade_id":"p10-upgrade"}`,
		hex.EncodeToString(prevSum[:]), hex.EncodeToString(sum[:]),
		time.Now().UTC().Add(2*time.Minute).Format(time.RFC3339))
	markerPath := filepath.Join(sandbox, ".tether-upgrade.json")
	if err := os.WriteFile(markerPath, []byte(pending), 0o644); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db, nil)()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", exePath, nil)()

	// 5s, not 3s (internal review S30): under e2e-parallel contention the
	// register retry loop's backoff can push past 3s on a loaded box.
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, err := os.ReadFile(markerPath)
		if err == nil {
			var m struct {
				State string `json:"state"`
			}
			if json.Unmarshal(raw, &m) == nil && m.State == "committed" {
				break
			}
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(markerPath)
			t.Fatalf("marker never reached committed after register; marker=%s", string(raw))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestUpgradeRejectedWhilePending: the install entry gate refuses a second
// upgrade while a pending marker is inside its register deadline — installing
// over it would clobber the only known-good binary in the prev slot.
// origin: upgrade-safety plan §3.2 F6.
func TestUpgradeRejectedWhilePending(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, fakeTetherScript("v9.9.9-test"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A prior upgrade staged its binary and is awaiting its register deadline.
	pending := fmt.Sprintf(`{"state":"pending","prev_sha":"aa","new_sha":"bb",`+
		`"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":0,"boot_budget":3}`,
		time.Now().UTC().Add(2*time.Minute).Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(sandbox, ".tether-upgrade.json"), []byte(pending), 0o644); err != nil {
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
	if resp.OK || !strings.Contains(resp.Code, "upgrade_in_progress") {
		t.Fatalf("expected upgrade_in_progress; got %+v", resp)
	}
	if got, _ := os.ReadFile(exePath); string(got) != "OLD" {
		t.Errorf("gate must leave the binary untouched; got %q", string(got))
	}
}

// TestUpgradeConcurrentInstallRejected: the install mutex (internal review
// S2) — two upgrade messages racing the SAME agent must resolve to exactly
// one install; the loser is refused with upgrade_in_progress BEFORE it can
// touch the prev slot. Deliberately DRIVES THE AGENT'S FORWARDED SUBJECT
// DIRECTLY, no broker: a single broker serializes upgrade.req in its one
// subscription callback, so the hermetic broker path can never race — the
// race S2 closes is the HA-cluster shape where two brokers forward to one
// agent concurrently, and the agent's per-message dispatch goroutines
// (exec.go dispatchForwarded) are the only concurrency that matters. The
// stub mirror stalls so the two handler goroutines genuinely overlap.
// Deleting the install mutex turns this red (both replies come back OK).
func TestUpgradeConcurrentInstallRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, fakeTetherScript("v9.9.9-test"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond) // hold the first install inside the pipeline
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	// The broker exists so the agent can register and install its forwarded
	// subscription; the racing requests below bypass it on purpose.
	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "lab-1", exePath, []string{srv.URL})()

	nc1, _ := nats.Connect(url)
	defer nc1.Close()
	nc2, _ := nats.Connect(url)
	defer nc2.Close()

	body, _ := json.Marshal(proto.UpgradeForwardedReq{
		URL: srv.URL + "/tether.tar.gz", SHA256: sum, ActorFP: "test-fp",
	})
	subj := proto.SubjCmdForwarded("lab", "lab-1", "upgrade")
	type result struct {
		resp proto.UpgradeForwardedResp
		err  error
	}
	results := make(chan result, 2)
	for _, conn := range []*nats.Conn{nc1, nc2} {
		go func(c *nats.Conn) {
			msg, err := c.Request(subj, body, 10*time.Second)
			if err != nil {
				results <- result{err: err}
				return
			}
			var resp proto.UpgradeForwardedResp
			uerr := json.Unmarshal(msg.Data, &resp)
			results <- result{resp: resp, err: uerr}
		}(conn)
	}
	var oks, busy int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		switch {
		case r.resp.OK:
			oks++
		case r.resp.Code == "upgrade_in_progress":
			busy++
		default:
			t.Fatalf("unexpected reply: %+v", r.resp)
		}
	}
	if oks != 1 || busy != 1 {
		t.Fatalf("concurrent installs: got %d OK / %d busy, want exactly 1 / 1", oks, busy)
	}
}

// TestUpgradeConcurrentInstallAcrossAgentsRejected: external review F1
// failure path two — two DIFFERENT agent instances (nids) sharing one binary
// path race their installs. Their in-process install mutexes are per-Agent
// and cannot see each other; the host-wide flock is the only serializer.
// Deleting the flock turns this red (both replies come back OK and the prev
// slot/marker are written by interleaved transactions).
func TestUpgradeConcurrentInstallAcrossAgentsRejected(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, fakeTetherScript("v9.9.9-test"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond) // hold the winner inside the pipeline
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}

	defer startBroker(t, url, db, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "n1", exePath, []string{srv.URL})()
	defer startAgentWithUpgrade(t, url, "lab", "n2", exePath, []string{srv.URL})()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.UpgradeForwardedReq{
		URL: srv.URL + "/tether.tar.gz", SHA256: sum, ActorFP: "test-fp",
	})
	type result struct {
		resp proto.UpgradeForwardedResp
		err  error
	}
	results := make(chan result, 2)
	for _, nid := range []string{"n1", "n2"} {
		go func(n string) {
			msg, err := nc.Request(proto.SubjCmdForwarded("lab", n, "upgrade"), body, 10*time.Second)
			if err != nil {
				results <- result{err: err}
				return
			}
			var resp proto.UpgradeForwardedResp
			uerr := json.Unmarshal(msg.Data, &resp)
			results <- result{resp: resp, err: uerr}
		}(nid)
	}
	var oks, busy int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		switch {
		case r.resp.OK:
			oks++
		case r.resp.Code == "upgrade_in_progress":
			busy++
		default:
			t.Fatalf("unexpected reply: %+v", r.resp)
		}
	}
	if oks != 1 || busy != 1 {
		t.Fatalf("cross-agent concurrent installs: got %d OK / %d busy, want exactly 1 / 1", oks, busy)
	}
}

// TestUpgradeStalePendingMarkerAdmitsRetry: the entry gate's other half
// (internal review S18) — a pending marker whose register deadline has
// ALREADY PASSED (a failed flip with nothing left alive to resolve it) must
// NOT block: the operator's retry goes through and re-stages cleanly.
func TestUpgradeStalePendingMarkerAdmitsRetry(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)

	tarball, sum := makeTarball(t, fakeTetherScript("v9.9.9-test"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	sandbox := t.TempDir()
	exePath := filepath.Join(sandbox, "tether")
	if err := os.WriteFile(exePath, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := fmt.Sprintf(`{"state":"pending","prev_sha":"aa","new_sha":"bb",`+
		`"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":0,"boot_budget":3}`,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(sandbox, ".tether-upgrade.json"), []byte(stale), 0o644); err != nil {
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
		t.Fatalf("stale pending marker must not block a retry; got %+v", resp)
	}
	if got, _ := os.ReadFile(exePath); !bytes.Equal(got, fakeTetherScript("v9.9.9-test")) {
		t.Errorf("retry did not re-stage the binary; got %q", string(got))
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
