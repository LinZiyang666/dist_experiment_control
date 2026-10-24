//go:build linux

package cli_e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Registration is a cold fixture premise, before the agent owns any business.
// The real CLI boot opens its journal and constructs its production endpoint;
// the fixture supplies neither an engine port nor a successful cut receipt.
// origin: hot-update S8 plan §3.1 / §5.1 / §5.2 / §9.4
func newS8UpgradeStack(t *testing.T, srv *httptest.Server) *realStack {
	t.Helper()
	return newUpgradeStack(t, srv, func(s *realStack) {
		s.beforeAgent = func(s *realStack) {
			role := upgradecli.AgentBootRole(s.agentHome, s.sid, s.nid, uint32(os.Geteuid()))
			if err := os.MkdirAll(role.ControlDir, 0o700); err != nil {
				t.Fatal(err)
			}
			domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: s.agentBin, ControlDir: role.ControlDir,
				DeploymentID: "cli-upgrade-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "cli-agent"}}})
			if err != nil {
				t.Fatal(err)
			}
			key := role.Want
			key.DomainID = domain
			for _, path := range []string{filepath.Dir(role.Journal(key, upgradelock.Registration{})), filepath.Join(role.ControlDir, "artifacts")} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
}

// An inert grant must name the images actually present at either end; the
// fixture cannot invent an execution ID or reuse another candidate's grant.
// origin: hot-update S8 plan §3.1 / §5.2
func prepareS8NodeUpgrade(t *testing.T, s *realStack, srv *httptest.Server, index int, a upgradeArtifact) (upgradetx.Grant, string, ctlResult) {
	t.Helper()
	s.waitFreshHeartbeat(t, time.Second)
	path := filepath.Join(t.TempDir(), "grant.json")
	res := upgradeTransactionNode(t, s, "upgrade", "--url", fmt.Sprintf("%s/%d.tar.gz", srv.URL, index), "--sha256", a.tarSHA, "--prepare-only", "--grant-out", path)
	if res.rc != 0 {
		return upgradetx.Grant{}, path, res
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var grant upgradetx.Grant
	if err := json.Unmarshal(body, &grant); err != nil {
		t.Fatal(err)
	}
	if len(grant.Handle.TxID) != 32 || grant.SourceSHA != upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", s.agentPID())) || grant.TargetSHA != a.binSHA || grant.Mode != upgradetx.ModeHot {
		t.Fatalf("Prepare did not bind the actual images: %+v", grant)
	}
	return grant, path, res
}

// Use the public product chain. Stage publishes only; Start is followed by
// fresh Wait, and neither an old marker nor BinaryReplaced signs completion.
// origin: hot-update S8 plan §5.2 LQ1 / §7.1
func completeS8NodeUpgrade(t *testing.T, s *realStack, srv *httptest.Server, index int, a upgradeArtifact) upgradetx.Grant {
	t.Helper()
	grant, path, res := prepareS8NodeUpgrade(t, s, srv, index, a)
	if res.rc != 0 {
		t.Fatalf("Prepare: %+v", res)
	}
	receipt := stageS8NodeUpgrade(t, s, grant, path, a)
	start := upgradeTransactionNode(t, s, "upgrade", "--tx-id", grant.Handle.TxID, "--artifact-receipt", receipt)
	requireAcceptedStart(t, "authorized Start", start)
	waitS8NodeUpgrade(t, s, grant)
	return grant
}

// requireAcceptedStart admits exactly the two Start answers of an accepted
// live cut: completed (0), or accepted and activating (74 with that exact
// detail) — the agent replies before its own exec, so the outcome belongs to
// the fresh Wait and its three receipts, which every caller then requires.
// Any other 74, a refusal or an unknown reply fails.
// origin: hot-update S8 plan §7.1; S8 BROKER-EXPORTER R-11 (lead acceptance of LEGACY-ROUTE-TESTS)
func requireAcceptedStart(t *testing.T, what string, start ctlResult) {
	t.Helper()
	if start.rc != 0 && (start.rc != 74 || !strings.Contains(start.stderr, "start accepted and activating")) {
		t.Fatalf("%s: %+v", what, start)
	}
}

func stageS8NodeUpgrade(t *testing.T, s *realStack, grant upgradetx.Grant, path string, a upgradeArtifact) string {
	t.Helper()
	candidate := filepath.Join(t.TempDir(), "tether")
	if err := agentimage.ExtractBinary(a.tarball, candidate, binaryupgrade.MaxTarballBytes, (*os.File).Sync); err != nil {
		t.Fatal(err)
	}
	receipt := filepath.Join(t.TempDir(), "receipt.json")
	// Stage is a local root command, without ctl's --home/--nats-url flags.
	// Its configuration must resolve the agent account's private registration.
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.ComponentHandover)
	defer cancel()
	stage := exec.CommandContext(ctx, s.bin, "upgrade", "stage", "--grant", path, "--source", fmt.Sprintf("/proc/%d/exe", s.agentPID()), "--candidate", candidate, "--receipt-out", receipt)
	stage.Env = append(ctlEnv(), "HOME="+s.agentHome, "TETHER_HOME="+s.agentHome)
	if out, err := testharness.CombinedOutput(stage); err != nil {
		t.Fatalf("root stage: %v\n%s", err, out)
	}
	if got := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", s.agentPID())); got != grant.SourceSHA {
		t.Fatal("inert stage changed the running image")
	}
	return receipt
}

// Start's reply and an old marker cannot certify the three completion points.
// origin: hot-update S8 plan §4.2 / §7.1
func waitS8NodeUpgrade(t *testing.T, s *realStack, grant upgradetx.Grant) {
	t.Helper()
	wait := upgradeTransactionNode(t, s, "upgrade-wait", "--tx-id", grant.Handle.TxID, "--timeout", "30s", "--json")
	view := upgradeTransactionView(t, wait)
	if err := upgradetx.Completed(view.Observation.Current, view.Observation); err != nil {
		t.Fatal(err)
	}
	if view.Historical.Grant.Handle.TxID != grant.Handle.TxID || view.Historical.EffectiveSHA != grant.TargetSHA {
		t.Fatal("Wait completed another transaction")
	}
}

// Independent kernel identity, fresh census and activation timing keep a disk
// selection or stale receipt from signing a live same-PID cut.
// origin: hot-update S8 plan §4.2 / §5.2 / §6.3
func requireS8UpgradeReceipt(t *testing.T, s *realStack, pid int, identity string, grant upgradetx.Grant, generation uint64) {
	t.Helper()
	got, err := upgradeProcessIdentity(pid)
	if err != nil || s.agentPID() != pid || got != identity {
		t.Fatalf("same-PID exec lost identity: %s/%s %v", identity, got, err)
	}
	if upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid)) != grant.TargetSHA {
		t.Fatal("disk selection did not replace the running process")
	}
	// Canonical is the stable dispatcher; the durable effective selector is the
	// installed image. The old disk==running marker projection is insufficient.
	first := upgradeTransactionView(t, upgradeTransactionNode(t, s, "upgrade-status", "--tx-id", grant.Handle.TxID, "--json"))
	second := upgradeTransactionView(t, upgradeTransactionNode(t, s, "upgrade-status", "--tx-id", grant.Handle.TxID, "--json"))
	if first.Observation.Nonce == second.Observation.Nonce || second.Observation.At < first.Observation.At {
		t.Fatal("status reused a stale observation")
	}
	if err := upgradetx.Completed(second.Observation.Current, second.Observation); err != nil {
		t.Fatal(err)
	}
	// The completed attempt is exactly this grant's: its S8 generation is the
	// grant's (the first cut of a booted generation-1 runtime is generation 2).
	// `generation` below is the agent's image-activation count, a different
	// counter (one activation per cut).
	// origin: S8 BROKER-EXPORTER R-11 (lead acceptance of LEGACY-ROUTE-TESTS)
	if !upgradetx.SameCut(second.Observation.Current.Handle, grant.Handle) || second.Observation.Current.Handle.Generation != grant.Handle.Generation ||
		second.Observation.RunningSHA != grant.TargetSHA || second.Observation.EffectiveSHA != grant.TargetSHA {
		t.Fatalf("wrong effective image/generation: %+v", second.Observation)
	}
	logs, err := os.ReadFile(s.agentOut)
	if err != nil {
		t.Fatal(err)
	}
	lines := regexp.MustCompile(`agent: image activated generation=(\d+) tasks=(\d+) freeze_window_ms=(\d+) shim_ms=(\d+) import_ms=(\d+)`).FindAllStringSubmatch(string(logs), -1)
	if len(lines) != int(generation) || s.agentLogCount("agent: image activated") != int(generation) {
		t.Fatalf("expected one activation per generation %d: %s", generation, logs)
	}
	last := lines[len(lines)-1]
	freeze, err := strconv.ParseInt(last[3], 10, 64)
	if last[1] != strconv.FormatUint(generation, 10) || last[2] != "1" || err != nil || freeze <= 0 {
		t.Fatalf("activation receipt invalid: %s", last[0])
	}
	// origin: hot-update S10 lead decision D-A / agent freeze SLO
	testharness.RequireCandidateBudget(t, fmt.Sprintf("/proc/%d/exe", pid), time.Duration(freeze)*time.Millisecond, upgradebudget.LocalFreezeWindow)
	t.Logf("activation receipt: %s", last[0])
	requireUpgradeFDs(t, pid)
}

func exerciseNodeUpgradeLiveExec(t *testing.T, candidate upgradeArtifact, chain bool) {
	t.Helper()
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	source := packUpgradeArtifact(t, mustReadFile(t, buildTether(t)))
	if source.binSHA == candidate.binSHA {
		t.Fatal("candidate must differ from source")
	}
	srv := serveUpgradeArtifacts(t, candidate, source)
	s := newS8UpgradeStack(t, srv)
	pid := s.agentPID()
	ticks, err := upgradeProcessIdentity(pid)
	if err != nil {
		t.Fatal(err)
	}
	p := startUpgradeProducer(t, s)
	requireUpgradeFDs(t, pid)
	hops := []upgradeArtifact{candidate}
	if chain {
		hops = append(hops, source)
	}
	for i, a := range hops {
		grant := completeS8NodeUpgrade(t, s, srv, i, a)
		requireS8UpgradeReceipt(t, s, pid, ticks, grant, uint64(i+1))
		p.release(t, 20*(i+1))
		p.checkpoint(t, 20*(i+2))
	}
	p.finish(t, s)
}

func exerciseNodeUpgradeBusyResource(t *testing.T, legacyCtl string, candidate upgradeArtifact) {
	t.Helper()
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	srv := serveUpgradeArtifacts(t, candidate)
	s := newS8UpgradeStack(t, srv)
	pid := s.agentPID()
	ticks, err := upgradeProcessIdentity(pid)
	if err != nil {
		t.Fatal(err)
	}
	sha := upgradeFileSHA(t, s.agentBin)
	p := startUpgradeProducer(t, s)
	ready, release := filepath.Join(t.TempDir(), "pty-ready"), filepath.Join(t.TempDir(), "pty-close")
	script := fmt.Sprintf("[ -t 0 ] && [ -t 1 ] || exit 91; : > '%s'; while [ ! -f '%s' ]; do sleep 0.02; done; exit 0", ready, release)
	// A reliable PTY is transferable; an actual S3 ctl pins legacy admission.
	pty := startUpgradeCtlBinary(t, s, legacyCtl, "run", "sh", "-c", script)
	if !testharnessWait(15*time.Second, func() bool { _, err := os.Stat(ready); return err == nil }) {
		t.Fatal("real run never opened a PTY")
	}
	legacy := nodeUpgrade(t, s, srv, 0, candidate, "--wait=false")
	if legacy.rc != 64 || !strings.Contains(legacy.stderr, "legacy_busy") {
		t.Fatalf("legacy wire busy refusal: %+v", legacy)
	}
	grant, path, res := prepareS8NodeUpgrade(t, s, srv, 0, candidate)
	if res.rc != 0 {
		t.Fatalf("inert busy Prepare: %+v", res)
	}
	receipt := stageS8NodeUpgrade(t, s, grant, path, candidate)
	res = upgradeTransactionNode(t, s, "upgrade", "--tx-id", grant.Handle.TxID, "--artifact-receipt", receipt)
	if res.rc != 75 || !strings.Contains(res.stderr, "upgrade_deferred") || !strings.Contains(res.stderr, "legacy_session_busy") {
		t.Fatalf("authorized PTY Start refusal: %+v", res)
	}
	requireUpgradeUnchanged(t, s, sha)
	p.release(t, 20)
	p.checkpoint(t, 40)
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if res := pty.result(t); res.rc != 0 {
		t.Fatalf("PTY close: %+v", res)
	}
	res = upgradeTransactionNode(t, s, "upgrade", "--tx-id", grant.Handle.TxID, "--artifact-receipt", receipt)
	requireAcceptedStart(t, "retry original unaccepted grant after PTY close", res)
	waitS8NodeUpgrade(t, s, grant)
	requireS8UpgradeReceipt(t, s, pid, ticks, grant, 1)
	p.release(t, 40)
	p.checkpoint(t, 60)
	p.finish(t, s)
}

func exerciseNodeUpgradeLegacyCandidate(t *testing.T) {
	t.Helper()
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	executed := filepath.Join(t.TempDir(), "legacy-executed")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = version ]; then\n  printf 'tether v0.0.0-legacy (proto %s)\\n%s/%s\\n%s\\n'\n  exit 0\nfi\n: > '%s'\nprintf 'legacy candidate executed\\n'\nexit 92\n", proto.SubjectVersionToken, runtime.GOOS, runtime.GOARCH, runtime.Version(), executed)
	a := packUpgradeArtifact(t, []byte(script))
	srv := serveUpgradeArtifacts(t, a)
	s := newS8UpgradeStack(t, srv)
	sha := upgradeFileSHA(t, s.agentBin)
	p := startUpgradeProducer(t, s)
	grant, path, res := prepareS8NodeUpgrade(t, s, srv, 0, a)
	if res.rc != 0 {
		t.Fatalf("inert legacy Prepare: %+v", res)
	}
	receipt := stageS8NodeUpgrade(t, s, grant, path, a)
	res = upgradeTransactionNode(t, s, "upgrade", "--tx-id", grant.Handle.TxID, "--artifact-receipt", receipt)
	if res.rc != 64 || !strings.Contains(res.stderr, "image_abi_unsupported") {
		t.Errorf("legacy candidate refusal: %+v", res)
	}
	requireUpgradeUnchanged(t, s, sha)
	if _, err := os.Stat(executed); !os.IsNotExist(err) {
		t.Errorf("agent executed legacy candidate: %v", err)
	}
	p.release(t, 20)
	p.checkpoint(t, 40)
	p.finish(t, s)
}
