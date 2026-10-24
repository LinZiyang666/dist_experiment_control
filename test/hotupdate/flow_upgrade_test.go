//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Registration models installer retrofit before the first daemon start. It
// grants no completion proof and writes no selector, artifact or journal.
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F2; hot-update S8 plan §5.2
func flowRegisteredEnvironment(t *testing.T, s *flowProductStack) []string {
	t.Helper()
	home := filepath.Join(filepath.Dir(s.agentBinary), "agent-home")
	role := upgradecli.AgentBootRole(home, "flow-lab", "flow-agent", uint32(os.Geteuid()))
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: s.agentBinary, ControlDir: role.ControlDir,
		DeploymentID: "flow-fixture", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "tether-agent@flow-lab"}}}); err != nil {
		t.Fatal(err)
	}
	return flowSandboxEnvironment(home)
}

// The original sockets stay live while the BUILT ctl prepares, root-stages and
// starts the target's exact grant. A lost Start reply is not completion: only
// the independent current query after upgrade-wait can close this exercise.
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F2; hot-update S8 plan §4.2 / §5.2 / §7.2
func flowTransactionUpgrade(t *testing.T, s *flowProductStack, a flowArtifacts) (upgradetx.View, error) {
	t.Helper()
	var view upgradetx.View
	dir := t.TempDir()
	grantPath, receiptPath := filepath.Join(dir, "grant.json"), filepath.Join(dir, "receipt.json")
	out, err := flowProductCommand(t, s, "node", "upgrade", "flow-agent", "--url", s.artifactURL+"/candidate.tar.gz",
		"--sha256", a.tarSHA, "--prepare-only", "--grant-out", grantPath)
	if err != nil {
		return view, fmt.Errorf("prepare: %w: %s", err, out)
	}
	body, err := os.ReadFile(grantPath)
	if err != nil {
		return view, err
	}
	var grant upgradetx.Grant
	if err := json.Unmarshal(body, &grant); err != nil {
		return view, err
	}
	if grant.Handle.TxID == "" || grant.TargetSHA != a.candidateSHA {
		return view, errors.New("prepare did not bind the candidate to a target-issued grant")
	}
	agentHome := filepath.Join(filepath.Dir(s.agentBinary), "agent-home")
	cmd := exec.CommandContext(t.Context(), s.binary, "upgrade", "stage", "--grant", grantPath,
		"--source", fmt.Sprintf("/proc/%d/exe", s.agent.Process.Pid), "--candidate", a.candidate, "--receipt-out", receiptPath)
	cmd.Env = flowSandboxEnvironment(agentHome)
	if out, err := testharness.CombinedOutput(cmd); err != nil {
		return view, fmt.Errorf("stage: %w: %s", err, out)
	}
	out, err = flowProductCommand(t, s, "node", "upgrade", "flow-agent", "--tx-id", grant.Handle.TxID, "--artifact-receipt", receiptPath)
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 74) {
		return view, fmt.Errorf("start: %w: %s", err, out)
	}
	t.Logf("authorized Start tx=%s native_error=%v output=%s", grant.Handle.TxID, err, out)
	out, err = flowProductCommandWithin(t, s, 150*time.Second, "node", "upgrade-wait", "flow-agent",
		"--tx-id", grant.Handle.TxID, "--timeout", "120s", "--json")
	if err != nil {
		return view, fmt.Errorf("wait: %w: %s", err, out)
	}
	if err := json.Unmarshal(out, &view); err != nil {
		return view, fmt.Errorf("wait View: %w: %s", err, out)
	}
	if !transactionSameCut(view.Observation.Current.Handle, grant.Handle) || view.Observation.Current.TargetSHA != a.candidateSHA {
		return view, errors.New("wait completed another grant or candidate")
	}
	if err := upgradetx.Completed(view.Observation.Current, view.Observation); err != nil {
		return view, err
	}
	return view, nil
}

func flowActivationWindow(t *testing.T, log string) int64 {
	t.Helper()
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "agent: image activated") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if value, found := strings.CutPrefix(field, "freeze_window_ms="); found {
				ms, err := strconv.ParseInt(value, 10, 64)
				if err == nil && ms > 0 {
					return ms
				}
			}
		}
	}
	t.Fatal("actual activation lacks a positive freeze window")
	return 0
}

// A transaction receipt proves the cut; this separate durable register row
// proves that the original register observer still sees the candidate release.
// origin: S10 internal review TAGGED-SUITE-SWEEP-A F2; hot-update S8 plan §4.2
func flowRequireUpgradedRegister(t *testing.T, s *flowProductStack) {
	t.Helper()
	var release, status string
	err := s.db.QueryRow(`SELECT release_version, status FROM nodes WHERE sid=? AND nid=?`, "flow-lab", "flow-agent").Scan(&release, &status)
	if err != nil || release != "v0.0.0-flow-1" || status != "ONLINE" {
		t.Fatalf("post-exec registration release=%s status=%s: %v", release, status, err)
	}
}
