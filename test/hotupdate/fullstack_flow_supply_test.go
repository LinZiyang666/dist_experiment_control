//go:build linux

package hotupdate_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
)

// The same Python keeper used by the deployment fixture owns real original
// sockets and independent sender/receiver captures here. Draining a sealed
// 901-second supply is accelerated; product cuts and SSH remain P22's work.
// origin: hot-update S10 plan §3.2 / §10.2 P12 / §11.2
func TestFullstackFlowSupplyOutlivesSchedule(t *testing.T) {
	started := time.Now()
	supervisor := filepath.Join(t.TempDir(), "supervise")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", supervisor, "./test/fullstackharness/cmd/supervise")
	cmd.Dir = "../.."
	if output, err := fullstackharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build shared supervisor: %v\n%s", err, output)
	}
	t.Setenv("SIM_UPGRADE_SUPERVISOR", supervisor)
	t.Logf("flow supply supervisor build=%s", time.Since(started))
	// Warm the permanent netpoll/process-supervision descriptors before the
	// exact FD baseline; all measured rounds still create and join every owner.
	exerciseFlowSupply(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	started = time.Now()
	for round := 0; round < 5; round++ {
		exerciseFlowSupply(t)
	}
	testharness.AssertNoGoroutineLeak(t, "original flow keeper and collector", beforeG)
	testharness.AssertNoFDLeak(t, "original flow keeper and collector", beforeFD, 0)
	t.Logf("flow supply five-cycle=%s; product cut=not applicable", time.Since(started))
}

func exerciseFlowSupply(t *testing.T) {
	t.Helper()
	started := time.Now()
	// Use a short owned socket directory: sockaddr_un is limited independently
	// of the test runner's potentially long evidence directory.
	root, err := os.MkdirTemp("", "flow-supply-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "../simcluster/drills/lib/fullstack_flow.py",
		"selftest", "--root", root)
	output, err := fullstackharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("flow supply selftest: %v\n%s", err, output)
	}
	var receipt struct {
		Tier      string `json:"tier"`
		Scenarios int    `json:"scenarios"`
		Joined    int    `json:"joined"`
		Supply    []struct {
			Records    int    `json:"records"`
			Directions int    `json:"directions"`
			Digest     string `json:"source_sha256"`
			Costs      struct {
				Startup        int64 `json:"startup"`
				IO             int64 `json:"io"`
				Cut            int64 `json:"cut"`
				Settle         int64 `json:"settle"`
				SupervisorJoin int64 `json:"supervisor_join"`
			} `json:"cost_ns"`
		} `json:"supply"`
	}
	if err := json.Unmarshal(output, &receipt); err != nil {
		t.Fatalf("decode actual keeper receipt: %v\n%s", err, output)
	}
	if receipt.Tier != "hermetic" || receipt.Scenarios != 3 || receipt.Joined != 6 || len(receipt.Supply) != 1 {
		t.Fatalf("incomplete flow lifecycle receipt: %s", output)
	}
	if supply := receipt.Supply[0]; supply.Records != 9042 || supply.Directions != 2 || len(supply.Digest) != 64 {
		t.Fatalf("source did not outlive the complete schedule: %s", output)
	}
	costs := receipt.Supply[0].Costs
	if costs.Startup <= 0 || costs.IO <= 0 || costs.Settle <= 0 || costs.SupervisorJoin <= 0 || costs.Cut != 0 {
		t.Fatalf("incomplete keeper cost receipt: %s", output)
	}
	t.Logf("flow keeper total=%s costs_ns startup=%d io=%d cut=%d settle=%d supervisor_join=%d",
		time.Since(started), costs.Startup, costs.IO, costs.Cut, costs.Settle, costs.SupervisorJoin)
}
