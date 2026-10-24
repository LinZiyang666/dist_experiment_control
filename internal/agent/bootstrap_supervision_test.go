//go:build linux

package agent

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// A PTY owns a different session from the isolated test process. Its ignored
// HUP survives the runner's normal exit and a kill of the runner's process
// group. The outer supervisor also contains the deliberately defective arm
// when the isolated runner is mutation-tested without supervision.
// origin: S8 internal review FIX-AGENTRACE-B F2
func TestIsolatedAgentReapsDetachedPTYSession(t *testing.T) {
	const name = "TestIsolatedAgentReapsDetachedPTYSession"
	if os.Getenv("_TETHER_AGENT_TEST_HELPER") == name {
		if runIsolatedAgentTest(t, name) {
			t.Fatal("isolated helper did not enter its body")
		}
		leaveActivationPTY(t)
		return
	}
	if isolatePTYAuthorityTest(t, name) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseIsolatedActivationPTY(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "isolated PTY supervision x5", beforeG)
	testharness.AssertNoFDLeak(t, "isolated PTY supervision x5", beforeFD, 0)
}

func exerciseIsolatedActivationPTY(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	output, runErr := isolatedAgentOutput(t, "TestIsolatedAgentReapsDetachedPTYSession", dir)
	if !bytes.Contains(output, []byte("=== RUN   TestIsolatedAgentReapsDetachedPTYSession\n")) {
		t.Fatalf("isolated PTY body did not run: %v\n%s", runErr, output)
	}
	data, err := os.ReadFile(filepath.Join(dir, "activation-pty-child"))
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	var ticks int64
	if _, err := fmt.Sscanf(string(data), "%d %d", &pid, &ticks); err != nil || pid <= 0 || ticks <= 0 {
		t.Fatal("invalid isolated PTY identity receipt")
	}
	if ptyMemberAlive(pid, ticks) {
		// Emergency cleanup is not the oracle. The original identity must
		// already be dead when the isolated runner returns its result.
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatalf("isolated runner returned with a live detached PTY: pid=%d ticks=%d", pid, ticks)
	}
	if current, err := readStartTimeTicks(pid); err == nil && current == ticks {
		t.Fatal("isolated runner returned before reaping its detached PTY")
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "process exited with surviving descendants") {
		t.Fatalf("isolated runner lost its descendant failure: err=%v\n%s", runErr, output)
	}
}

func leaveActivationPTY(t *testing.T) {
	t.Helper()
	a := newBootAgent(t, "nats://127.0.0.1:1", "node")
	scope := ptyTestScope(a)
	h := ptyProbe(t, a, scope)
	q := ptyTaskRequest(scope, h, "create")
	q.Run = &proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "trap '' HUP TERM; printf ready; exec sleep 60"}}
	reply, err := a.processRuntime.Handle(scope, ownProcessRun(scope, q), "_TINBOX.pty")
	if err != nil || reply.PID == "" {
		t.Fatalf("create isolated PTY: %v", err)
	}
	waitFor(t, bootTestBudget, func() bool {
		r, err := a.processRuntime.Handle(scope, ptyTaskRequest(scope, h, "resume"), "_TINBOX.pty")
		return err == nil && r.Stdout != nil && string(r.Stdout.Data) == "ready"
	})
	row, ok := a.workloads.Lookup(reply.PID)
	if !ok || row.OSPID <= 0 || row.StartTimeTicks <= 0 {
		t.Fatal("isolated PTY has no kernel identity")
	}
	path := filepath.Join(os.Getenv("TMPDIR"), "activation-pty-child")
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d %d\n", row.OSPID, row.StartTimeTicks)), 0600); err != nil {
		t.Fatal(err)
	}
	// Model main returning after an early boot failure: testing cleanups and
	// process-group cleanup cannot discharge this detached terminal session.
	os.Exit(0)
}
