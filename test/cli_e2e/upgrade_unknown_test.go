//go:build linux

package cli_e2e_test

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

var upgradeUnknownEvidence = flag.String("upgrade-unknown-evidence", "", "private P26 live accepted-transaction fixtures for query/wait")

type upgradeUnknownFixture struct {
	RunID                            string `json:"run_id"`
	Round                            int    `json:"round"`
	Binary, Home, NATSURL, NID, TxID string
	Historical                       upgradetx.Record
}

// No responder synthesizes an unknown View here. The real broker/agent must
// answer an accepted historical transaction whose CURRENT observation has
// become unknown. A never-issued ID instead means upgrade_grant_unknown; it
// cannot exercise this distinction. P26 retains each live fixture until all
// five CLI rounds have joined, using its existing observation-loss mechanism.
// origin: hot-update S8 plan §7.1 / P18/P19/P23
// origin: S10 internal review FINAL-REAL-FAILS F3
func TestUpgradeUnknownQueryAndWaitHaveDistinctExitCodes(t *testing.T) {
	// S10 §0 separates unavailable upstream live evidence from hermetic
	// behavior. Keep the required five-round assertion pending explicitly;
	// a supplied fixture still undergoes every historical/current proof check.
	if *upgradeUnknownEvidence == "" {
		// SETUP-RED, never a skip (TestMissingDependencyNeverSkips).
		t.Fatal("SETUP-RED: NOT_RUN pending P13-P17/P19: -upgrade-unknown-evidence requires a live accepted transaction with lost current observation (S10 plan §0)")
	}
	upgradeTransactionRequireCLI(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseUpgradeUnknownCLI(t, round) })
	}
	testharness.AssertNoGoroutineLeak(t, "unknown upgrade CLI x5", g)
	testharness.AssertNoFDLeak(t, "unknown upgrade CLI x5", fd, 0)
}

func exerciseUpgradeUnknownCLI(t *testing.T, round int) {
	t.Helper()
	if *upgradeUnknownEvidence == "" {
		t.Fatal("NOT_RUN pending P13-P17/P19: -upgrade-unknown-evidence requires a live accepted transaction with lost current observation")
	}
	root, err := filepath.Abs(*upgradeUnknownEvidence)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("round-%d.json", round)))
	if err != nil {
		t.Fatal(err)
	}
	var fixture upgradeUnknownFixture
	if err := json.Unmarshal(body, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Round != round || fixture.RunID != fmt.Sprintf("p23-unknown/round-%d", round) || len(fixture.TxID) != 32 || fixture.Historical.Grant.Handle.TxID != fixture.TxID || fixture.Historical.Forward.Phase != upgradetx.PhaseCompleted {
		t.Fatal("unknown fixture did not retain an accepted completed historical transaction")
	}
	for _, path := range []string{fixture.Binary, fixture.Home} {
		actual, err := filepath.EvalSymlinks(path)
		if err != nil || actual != path || !strings.HasPrefix(path, root+string(filepath.Separator)) {
			t.Fatal("unknown CLI requires private fixture artifact/home paths")
		}
	}
	endpoint, err := url.Parse(fixture.NATSURL)
	if err != nil || endpoint.Scheme != "nats" || endpoint.Hostname() != "127.0.0.1" {
		t.Fatal("unknown CLI requires a hermetic loopback control plane")
	}
	run := func(verb string, extra ...string) ctlResult {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		args := append([]string{"node", verb, fixture.NID, "--home", fixture.Home, "--nats-url", fixture.NATSURL, "--tx-id", fixture.TxID}, extra...)
		cmd := exec.CommandContext(ctx, fixture.Binary, args...)
		cmd.Env = append(ctlEnv(), "HOME="+fixture.Home, "TETHER_HOME="+fixture.Home)
		var stdout, stderr ptyCapture
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := testsupervisor.RunProcess(cmd)
		if cmd.ProcessState == nil {
			t.Fatalf("unknown CLI could not be supervised: %v", err)
		}
		return ctlResult{rc: cmd.ProcessState.ExitCode(), stdout: string(stdout.bytes()), stderr: string(stderr.bytes())}
	}
	first := upgradeTransactionView(t, run("upgrade-status", "--json"))
	second := upgradeTransactionView(t, run("upgrade-status", "--json"))
	if first.Observation.Nonce == second.Observation.Nonce || first.Observation.OwnerKnown || second.Observation.OwnerKnown || first.Code != upgradetx.CodeUnsettled || second.Code != upgradetx.CodeUnsettled {
		t.Fatal("unknown query signed a known completion or reused its nonce")
	}
	if !reflectUpgradeUnknownHistory(first, second, fixture.Historical) {
		t.Fatal("current uncertainty rewrote historical completion")
	}
	start := time.Now()
	wait := run("upgrade-wait", "--timeout", "1s")
	if wait.rc != 74 || !strings.Contains(wait.stderr+wait.stdout, "upgrade_unsettled") || time.Since(start) > 4*time.Second {
		t.Fatalf("unknown Wait must be bounded rc74: %+v", wait)
	}
	last := upgradeTransactionView(t, run("upgrade-status", "--json"))
	if !reflectUpgradeUnknownHistory(first, last, fixture.Historical) || last.Observation.OwnerKnown || last.Observation.Nonce == second.Observation.Nonce {
		t.Fatal("Wait fabricated recovery or restarted an unknown transaction")
	}
	if upgradetx.Completed(last.Observation.Current, last.Observation) == nil {
		t.Fatal("unknown current observation signed completion")
	}
}

func reflectUpgradeUnknownHistory(a, b upgradetx.View, historical upgradetx.Record) bool {
	left, _ := json.Marshal(a.Historical)
	right, _ := json.Marshal(b.Historical)
	original, _ := json.Marshal(historical)
	return string(left) == string(right) && string(left) == string(original)
}
