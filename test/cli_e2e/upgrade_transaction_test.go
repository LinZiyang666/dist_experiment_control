//go:build linux

package cli_e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// Candidate B changes the actual text renderer in addition to its release.
// A label-only ldflags variant cannot prove two S8 implementations. Overlay
// mappings are composed with the caller's mutations before the nested build.
// origin: hot-update S8 plan §6.1 / §9.1 / P23
func buildUpgradeTransactionCandidate(t *testing.T) (string, upgradeArtifact) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "internal/upgradetx/presentation.go")
	flags := strings.Fields(os.Getenv("GOFLAGS"))
	replace := map[string]string{}
	var kept []string
	for _, flag := range flags {
		if value, ok := strings.CutPrefix(flag, "-overlay="); ok {
			body, err := os.ReadFile(value)
			if err != nil {
				t.Fatal(err)
			}
			var overlay struct{ Replace map[string]string }
			if err := json.Unmarshal(body, &overlay); err != nil {
				t.Fatal(err)
			}
			for from, to := range overlay.Replace {
				replace[from] = to
			}
		} else {
			kept = append(kept, flag)
		}
	}
	input := source
	if shadow, ok := replace[source]; ok {
		input = shadow
	}
	body, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(body), "Historical: tx_id=") != 1 {
		t.Fatal("transaction renderer variant anchor changed")
	}
	body = []byte(strings.Replace(string(body), "Historical: tx_id=", "Historical execution: tx_id=", 1))
	dir := t.TempDir()
	shadow := filepath.Join(dir, "presentation.go")
	if err := os.WriteFile(shadow, body, 0600); err != nil {
		t.Fatal(err)
	}
	replace[source] = shadow
	overlay, err := json.Marshal(struct{ Replace map[string]string }{replace})
	if err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(manifest, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	bin := filepath.Join(dir, "tether-candidate")
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", bin, "-ldflags", "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.0.0-s8-b", "./cmd/tether")
	if raceEnabled && !testharness.ReleaseBudgetRun(t) {
		cmd.Args = append(cmd.Args[:2:2], append([]string{"-race"}, cmd.Args[2:]...)...)
	}
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS="+strings.Join(append(kept, "-overlay="+manifest), " "))
	if testharness.ReleaseBudgetRun(t) {
		testharness.BuildReleaseCandidate(t, root, bin, "v0.0.0-s8-b", strings.Join(append(kept, "-overlay="+manifest), " "))
	} else if output, err := testsupervisor.CombinedOutput(cmd); err != nil {
		t.Fatalf("real candidate build: %v\n%s", err, output)
	}
	artifact, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	a := packUpgradeArtifact(t, artifact)
	if a.binSHA == upgradeFileSHA(t, buildTether(t)) {
		t.Fatal("A and B have identical executable bytes")
	}
	t.Logf("candidate path=%s sha=%s source_change=upgradetx.Render race=%t", bin, a.binSHA, raceEnabled && !testharness.ReleaseBudgetRun(t))
	return bin, a
}

func upgradeTransactionRequireCLI(t *testing.T) {
	t.Helper()
	bin := buildTether(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "node", "upgrade-status", "--help")
	cmd.Env = append(ctlEnv(), "HOME="+t.TempDir(), "TETHER_HOME="+t.TempDir())
	out, err := testsupervisor.CombinedOutput(cmd)
	if err != nil || !strings.Contains(string(out), "--tx-id") {
		t.Fatalf("NOT_RUN pending P19/P13-P17: real transaction CLI is not wired: %v\n%s", err, out)
	}
}

func upgradeTransactionNode(t *testing.T, s *realStack, verb string, args ...string) ctlResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 125*time.Second)
	defer cancel()
	return s.ctlCommand(ctx, append([]string{"node", verb, s.nid}, args...)...)
}

func upgradeTransactionView(t *testing.T, result ctlResult) upgradetx.View {
	t.Helper()
	if result.rc != 0 {
		t.Fatalf("transaction query rc=%d stderr=%s", result.rc, result.stderr)
	}
	var view upgradetx.View
	if err := json.Unmarshal([]byte(result.stdout), &view); err != nil {
		t.Fatalf("private View JSON: %v\n%s", err, result.stdout)
	}
	if len(view.Observation.Nonce) != 32 || view.Observation.At <= 0 {
		t.Fatal("query lacks a fresh observation challenge")
	}
	return view
}

// A real accepted forward/reverse must keep the original child and controller.
// This literal CLI test is deliberately separate from the R1 partial-Take and
// pre-Replay observers in hotupdate: a final output comparison cannot sign them.
// origin: hot-update S8 plan §7.1 / §8.1 R2 / §11.2 P23 / M02/M07
func TestUpgradeTransactionCLIUsesFreshCurrentProof(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBinaryTest(t, binaryFixtureTestBudget) {
		return
	}
	upgradeTransactionRequireCLI(t)
	_, candidate := buildUpgradeTransactionCandidate(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeTransactionCLI(t, candidate) })
	}
	testharness.AssertNoGoroutineLeak(t, "transaction CLI forward reverse x5", g)
	testharness.AssertNoFDLeak(t, "transaction CLI forward reverse x5", fd, 0)
}

func exerciseUpgradeTransactionCLI(t *testing.T, candidate upgradeArtifact) {
	t.Helper()
	// The shared stack owns NATS/auth/broker and the supervised real agent.
	// Its private canonical copy is the source; ctl keeps its immutable build.
	srv := serveUpgradeArtifacts(t, candidate)
	s := newS8UpgradeStack(t, srv)
	pid := s.agentPID()
	ticks, err := upgradeProcessIdentity(pid)
	if err != nil {
		t.Fatal(err)
	}
	sourceSHA := upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid))
	marker, gate := filepath.Join(t.TempDir(), "child-started"), filepath.Join(t.TempDir(), "child-release")
	script := "echo $$ > '" + marker + "'.$$; while test ! -e '" + gate + "'; do sleep 0.02; done; printf transaction-original-child; exit 23"
	ctl := startUpgradeCtl(t, s, "exec", "/bin/sh", "-c", script)
	waitMarker(t, marker)
	markers, err := filepath.Glob(marker + ".*")
	if err != nil || len(markers) != 1 {
		t.Fatal("original task was not spawned exactly once")
	}
	childBody, err := os.ReadFile(markers[0])
	if err != nil {
		t.Fatal(err)
	}
	childPID := strings.TrimSpace(string(childBody))
	childOSPID, err := strconv.Atoi(childPID)
	if err != nil {
		t.Fatal(err)
	}
	childTicks, err := upgradeProcessIdentity(childOSPID)
	if err != nil {
		t.Fatal(err)
	}
	grantPath := filepath.Join(t.TempDir(), "grant.json")
	before := upgradeTransactionNode(t, s, "upgrade", "--url", srv.URL+"/0.tar.gz", "--sha256", candidate.tarSHA, "--prepare-only", "--grant-out", grantPath)
	if before.rc != 0 {
		t.Fatalf("real forward rc=%d: %s", before.rc, before.stderr)
	}
	// Only the inert target-issued authorization supplies the execution ID.
	grantBytes, err := os.ReadFile(grantPath)
	if err != nil {
		t.Fatal(err)
	}
	var grant upgradetx.Grant
	if err := json.Unmarshal(grantBytes, &grant); err != nil {
		t.Fatal(err)
	}
	id := grant.Handle.TxID
	if len(id) != 32 {
		t.Fatal("Start omitted the target-issued transaction ID")
	}
	// Local publication and asynchronous Start use the same product chain as
	// node upgrade; completion still requires fresh Wait and all three receipts.
	// origin: hot-update S8 plan §5.2 / §7.1; S8 internal review upgrade-maketest F3
	receiptPath := stageS8NodeUpgrade(t, s, grant, grantPath, candidate)
	start := upgradeTransactionNode(t, s, "upgrade", "--tx-id", id, "--artifact-receipt", receiptPath)
	requireAcceptedStart(t, "authorized Start", start)
	waitS8NodeUpgrade(t, s, grant)
	first := upgradeTransactionView(t, upgradeTransactionNode(t, s, "upgrade-status", "--tx-id", id, "--json"))
	second := upgradeTransactionView(t, upgradeTransactionNode(t, s, "upgrade-status", "--tx-id", id, "--json"))
	if first.Observation.Nonce == second.Observation.Nonce || second.Observation.At < first.Observation.At {
		t.Fatal("completed response reused a query challenge")
	}
	if err := upgradetx.Completed(second.Observation.Current, second.Observation); err != nil {
		t.Fatal(err)
	}
	if s.agentPID() != pid || upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid)) != candidate.binSHA {
		t.Fatal("disk selection did not replace the same running process")
	}
	afterTicks, err := upgradeProcessIdentity(pid)
	if err != nil || afterTicks != ticks {
		t.Fatal("forward changed process incarnation")
	}
	// Keep candidate-admitted work live through reverse too. Recreating it with
	// equivalent output would change its original PID/start ticks or spawn count.
	newMarker, newGate := filepath.Join(t.TempDir(), "new-child"), filepath.Join(t.TempDir(), "new-release")
	newScript := "echo $$ > '" + newMarker + "'.$$; while test ! -e '" + newGate + "'; do sleep 0.02; done; printf new-generation-request; exit 29"
	newWork := startUpgradeCtl(t, s, "exec", "/bin/sh", "-c", newScript)
	waitMarker(t, newMarker)
	newMarkers, err := filepath.Glob(newMarker + ".*")
	if err != nil || len(newMarkers) != 1 {
		t.Fatal("candidate did not admit exactly one new child")
	}
	newBody, err := os.ReadFile(newMarkers[0])
	if err != nil {
		t.Fatal(err)
	}
	newPID, err := strconv.Atoi(strings.TrimSpace(string(newBody)))
	if err != nil {
		t.Fatal(err)
	}
	newTicks, err := upgradeProcessIdentity(newPID)
	if err != nil {
		t.Fatal(err)
	}
	reverse := upgradeTransactionNode(t, s, "upgrade-rollback", "--tx-id", id)
	if reverse.rc != 0 {
		t.Fatalf("manual reverse rc=%d: %s", reverse.rc, reverse.stderr)
	}
	if s.agentPID() != pid || upgradeFileSHA(t, fmt.Sprintf("/proc/%d/exe", pid)) != sourceSHA {
		t.Fatal("reverse failed to run the recorded previous image")
	}
	oldAfter, oldErr := upgradeProcessIdentity(childOSPID)
	newAfter, newErr := upgradeProcessIdentity(newPID)
	if oldErr != nil || newErr != nil || oldAfter != childTicks || newAfter != newTicks {
		t.Fatal("reverse replaced or killed an original/candidate-admitted task")
	}
	if err := os.WriteFile(newGate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	newResult := newWork.result(t)
	if newResult.rc != 29 || newResult.stdout != "new-generation-request" || markerCount(t, newMarker) != 1 {
		t.Fatal("reverse lost the original new task or its terminal")
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	result := ctl.result(t)
	if result.rc != 23 || result.stdout != "transaction-original-child" {
		t.Fatalf("original ctl lost its child/terminal: %+v", result)
	}
	if markerCount(t, marker) != 1 || childPID == "" {
		t.Fatal("rollback respawned the original business task")
	}
}
