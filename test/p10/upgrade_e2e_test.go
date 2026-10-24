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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradetx"
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
	ctx, cancel := context.WithCancel(t.Context())
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
	ctx, cancel := context.WithCancel(t.Context())
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

// Both legacy hops must refuse independently. Sending the old forwarded
// envelope here is a negative control, never authority to install or migrate.
// origin: hot-update S8 plan §7.2 / FIX-INST handoff
func agentHandlerUpgradeRequest(t *testing.T, nc *nats.Conn, sid, actor, nid string, req proto.UpgradeReq) proto.UpgradeResp {
	t.Helper()
	refused := upgradeRequest(t, nc, sid, actor, nid, req)
	if refused.OK || refused.NewVersion != "" || refused.Code != "try_again" || refused.Error != "legacy_quiescence_unproved" {
		t.Fatalf("unfenced legacy request bypassed migration policy: %+v", refused)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(proto.UpgradeForwardedReq{URL: req.URL, SHA256: req.SHA256, ActorFP: fp})
	if err != nil {
		t.Fatal(err)
	}
	message, err := nc.Request(proto.SubjCmdForwarded(sid, nid, "upgrade"), body, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var forwarded proto.UpgradeForwardedResp
	if err := json.Unmarshal(message.Data, &forwarded); err != nil {
		t.Fatal(err)
	}
	if forwarded.BinaryReplaced {
		t.Fatalf("grantless forwarded request replaced the binary: %+v", forwarded)
	}
	return proto.UpgradeResp{OK: forwarded.OK, Code: forwarded.Code, Error: forwarded.Error, NewVersion: forwarded.NewVersion}
}

// fakeTetherScript returns an EXECUTABLE stand-in for a tether release binary:
// a shell script that answers `version` the way cmd/tether does
// (`tether <tag> (proto vN)`). The upgrade-safety smoke gate execs the staged
// binary before the install flips, so happy-path fixtures must actually run —
// the old inert []byte("payload") fixtures only exercise the paths that fail
// BEFORE the smoke gate. S8 probes these bytes through FetchCandidate on the
// ctl host; a normalized version alone confers no hot-cut capability or grant.
// origin: upgrade-safety plan §7 (fixture migration); hot-update S8 plan §5.2.
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

// Fixtures using agentHandlerUpgradeRequest assert the legacy broker refusal
// and the agent's independent refusal of the obsolete forwarded envelope.
// origin: hot-update S8 plan §7.2 / FIX-INST handoff

// Candidate extraction still preserves the normalized release, exact content
// and mode. The old fake release cannot authorize a disk flip: S8 requires
// stage + Start under the original target-issued grant. The sandbox sentinel
// and absence of a newly-created prev/marker must survive both legacy hops.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeLegacyInstallRequiresTransaction(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeLegacyInstall(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeLegacyInstallRequiresTransaction", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeLegacyInstallRequiresTransaction", fd, 0)
}

func exerciseUpgradeLegacyInstall(t *testing.T) {
	f := newUpgradeFixture(t, fakeTetherScript("v9.9.9-test"))
	before := captureUpgradeDisk(t, f)
	candidate, err := upgradecli.FetchCandidate(upgradetx.ComponentAgent, f.request().URL, f.archiveSHA,
		binaryupgrade.FetchURL, probeUpgradeCandidate)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = candidate.Close() }()
	// Keep the original extraction, normalized release and executable-mode
	// assertions at the S8 candidate owner. They do not certify activation.
	if candidate.Release != "v9.9.9-test" {
		t.Fatalf("candidate release: got %q, want v9.9.9-test", candidate.Release)
	}
	got, err := os.ReadFile(candidate.Path)
	if err != nil || !bytes.Equal(got, fakeTetherScript("v9.9.9-test")) {
		t.Fatalf("candidate content: got %q err=%v", got, err)
	}
	info, err := os.Stat(candidate.Path)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("candidate mode: got %v err=%v, want 0755", info, err)
	}
	target, err := upgradecli.DescribeNode(t.Context(), f.session(), "lab-1")
	if err != nil {
		t.Fatal(err)
	}
	req, err := upgradecli.BuildPrepare(target, candidate, f.fp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	grant, err := upgradecli.NodeEndpoint(f.session(), "lab-1", 5*time.Second).Prepare(t.Context(), target.Key, req)
	if err != nil || grant.Handle.Key != f.keys["lab-1"] || len(grant.Handle.TxID) != 32 ||
		grant.SourceSHA != target.SourceSHA || grant.TargetSHA != candidate.SHA || grant.ActorFP != f.fp || grant.Issuer.PID != os.Getpid() {
		t.Fatalf("target did not issue the inert candidate-bound grant: %+v err=%v", grant, err)
	}
	// A prepared grant still needs explicit publication and Start. Neither
	// a successful version probe nor the old URL envelope supplies those.
	requests := f.requests.Load()
	resp := agentHandlerUpgradeRequest(t, f.nc, "lab", f.pub, "lab-1", f.request())
	assertUpgradeRefusal(t, resp, "upgrade_deferred", "a target-issued upgrade transaction is required", 75)
	assertNoUpgradeDownload(t, f, requests)
	assertUpgradeDiskUnchanged(t, before)
}

// A SHA-valid archive containing inert bytes (wrong arch or truncation) must
// fail the real version probe before any binary/prev/marker mutation. S8 moved
// download and the first smoke gate to the ctl's pre-prepare candidate owner.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeSmokeGateRejectsNonBinary(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeSmokeGate(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeSmokeGateRejectsNonBinary", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeSmokeGateRejectsNonBinary", fd, 0)
}

func exerciseUpgradeSmokeGate(t *testing.T) {
	f := newUpgradeFixture(t, []byte("real content, but not executable"))
	before := captureUpgradeDisk(t, f)
	candidate, err := upgradecli.FetchCandidate(upgradetx.ComponentAgent, f.request().URL, f.archiveSHA,
		binaryupgrade.FetchURL, probeUpgradeCandidate)
	defer func() { _ = candidate.Close() }()
	if upgradecli.ExitCode(err) != 64 || err == nil || !strings.Contains(err.Error(), "candidate executable failed its version probe") {
		t.Fatalf("expected pre-prepare version-probe rejection (rc 64), got candidate=%+v err=%v", candidate, err)
	}
	if f.requests.Load() != 1 {
		t.Fatal("smoke rejection was not reached after a real archive download")
	}
	assertUpgradeDiskUnchanged(t, before)
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

// A second legacy request cannot overwrite pending recovery evidence or the
// only known-good previous binary. Its envelope lacks S8 authorization even
// when the former v1 install gate would have returned upgrade_in_progress.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeRejectedWhilePending(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradePendingRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeRejectedWhilePending", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeRejectedWhilePending", fd, 0)
}

func exerciseUpgradePendingRefusal(t *testing.T) {
	f := newUpgradeFixtureWithDisk(t, fakeTetherScript("v9.9.9-test"), func(f *upgradeFixture) {
		writeUpgradePending(t, f, time.Now().Add(2*time.Minute))
	})
	before := captureUpgradeDisk(t, f)
	resp := agentHandlerUpgradeRequest(t, f.nc, "lab", f.pub, "lab-1", f.request())
	assertUpgradeRefusal(t, resp, "upgrade_deferred", "a target-issued upgrade transaction is required", 75)
	assertNoUpgradeDownload(t, f, 0)
	assertUpgradeDiskUnchanged(t, before)
}

// Two HA forwards racing one agent must not touch its binary or prev slot.
// S8 removes the old premise that one grantless request installs successfully:
// both refuse. The real domain reservation separately admits exactly one
// contender, replacing the former per-Agent install-mutex assertion.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeConcurrentInstallRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeConcurrentInstall(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeConcurrentInstallRejected", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeConcurrentInstallRejected", fd, 0)
}

func exerciseUpgradeConcurrentInstall(t *testing.T) {
	f := newUpgradeFixture(t, fakeTetherScript("v9.9.9-test"))
	exerciseUpgradeConcurrentRefusals(t, f, []string{"lab-1", "lab-1"})
	assertUpgradeReservationContention(t, f, []string{"lab-1", "lab-1"})
}

// Different agents sharing one canonical binary need a host-wide domain
// reservation; per-Agent mutexes cannot see each other. Both old forwards
// refuse without side effects, and their registered sibling keys compete for
// exactly one real S8 lease held until both replies have been collected.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeConcurrentInstallAcrossAgentsRejected(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeConcurrentAgents(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeConcurrentInstallAcrossAgentsRejected", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeConcurrentInstallAcrossAgentsRejected", fd, 0)
}

func exerciseUpgradeConcurrentAgents(t *testing.T) {
	f := newUpgradeFixture(t, fakeTetherScript("v9.9.9-test"), "n1", "n2")
	exerciseUpgradeConcurrentRefusals(t, f, []string{"n1", "n2"})
	assertUpgradeReservationContention(t, f, []string{"n1", "n2"})
}

// An expired legacy marker no longer authorizes an operator's fresh install.
// Keep the failed-flip recovery evidence and previous binary intact across
// repeated unfenced requests. Pending history is never GC'd merely by age.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeExpiredLegacyMarkerCannotAuthorizeRetry(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeExpiredMarker(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeExpiredLegacyMarkerCannotAuthorizeRetry", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeExpiredLegacyMarkerCannotAuthorizeRetry", fd, 0)
}

func exerciseUpgradeExpiredMarker(t *testing.T) {
	f := newUpgradeFixtureWithDisk(t, fakeTetherScript("v9.9.9-test"), func(f *upgradeFixture) {
		writeUpgradePending(t, f, time.Now().Add(-time.Hour))
	})
	before := captureUpgradeDisk(t, f)
	// Marker expiry is not a grant and unresolved evidence must survive.
	// Repeating the request must not turn that refusal into a fresh install.
	for attempt := 0; attempt < 2; attempt++ {
		resp := agentHandlerUpgradeRequest(t, f.nc, "lab", f.pub, "lab-1", f.request())
		assertUpgradeRefusal(t, resp, "upgrade_deferred", "a target-issued upgrade transaction is required", 75)
		assertNoUpgradeDownload(t, f, 0)
		assertUpgradeDiskUnchanged(t, before)
	}
}

// A syntactically valid 64-hex SHA must still match the actual HTTP archive.
// S8 verifies this on the ctl before Prepare, extraction/probing or staging;
// a mismatch must leave the agent binary and all recovery evidence untouched.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeCandidateRejectsBadSHA(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeCandidateSHA(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeCandidateRejectsBadSHA", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeCandidateRejectsBadSHA", fd, 0)
}

func exerciseUpgradeCandidateSHA(t *testing.T) {
	f := newUpgradeFixture(t, fakeTetherScript("v9.9.9-test"))
	before := captureUpgradeDisk(t, f)
	probes := 0
	candidate, err := upgradecli.FetchCandidate(upgradetx.ComponentAgent, f.request().URL, strings.Repeat("d", 64),
		binaryupgrade.FetchURL, func(path string) (string, agentimage.Capabilities, error) {
			probes++
			return probeUpgradeCandidate(path)
		})
	defer func() { _ = candidate.Close() }()
	if upgradecli.ExitCode(err) != 64 || err == nil || !strings.Contains(err.Error(), "candidate archive does not match --sha256") {
		t.Fatalf("expected archive SHA rejection (rc 64), got candidate=%+v err=%v", candidate, err)
	}
	if probes != 0 || f.requests.Load() != 1 {
		t.Fatalf("SHA mismatch must follow download and precede execution: downloads=%d probes=%d", f.requests.Load(), probes)
	}
	assertUpgradeDiskUnchanged(t, before)
}

// A permissive broker and stricter agent configuration cannot authorize a
// download through the old forwarded envelope. S8 replaced agent URL installs
// with immutable staged receipts; the legacy route refuses before fetching.
// origin: hot-update S8 plan §3.3 / §5.1 / §5.2 / §7.2; S8 internal review R2/R5/R6 P10
func TestUpgradeLegacyURLCannotAuthorizeDownload(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeLegacyURL(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestUpgradeLegacyURLCannotAuthorizeDownload", g)
	testharness.AssertNoFDLeak(t, "TestUpgradeLegacyURLCannotAuthorizeDownload", fd, 0)
}

func exerciseUpgradeLegacyURL(t *testing.T) {
	f := newUpgradeFixture(t, []byte("payload"))
	before := captureUpgradeDisk(t, f)
	// The broker trusts this URL; the agent's configured allowlist does not.
	// S8's URL-free Start consumes a staged receipt, while this old envelope
	// is refused before any download even if its archive SHA is correct.
	resp := agentHandlerUpgradeRequest(t, f.nc, "lab", f.pub, "lab-1", f.request())
	assertUpgradeRefusal(t, resp, "upgrade_deferred", "a target-issued upgrade transaction is required", 75)
	assertNoUpgradeDownload(t, f, 0)
	assertUpgradeDiskUnchanged(t, before)
}
