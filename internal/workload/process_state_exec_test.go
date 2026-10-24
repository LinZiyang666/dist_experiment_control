//go:build linux

// Real: same-PID exec, original live/zombie children, retained exits and inherited input pipes.
// Stubbed: control admission, network transports, broker state and application upgrade policy.
// This proves process-table restoration, not complete application hot update.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

const processImageMode = "TETHER_TEST_PROCESS_IMAGE_MODE"
const processImageReport = "TETHER_TEST_PROCESS_IMAGE_REPORT"

type processImageFixture struct {
	State      workload.ProcessState `json:"state"`
	UnownedPID int                   `json:"unowned_pid"`
}

type processImageReceipt struct {
	ParentPID  int `json:"parent_pid"`
	LivePID    int `json:"live_pid"`
	HeldPID    int `json:"held_pid"`
	UnownedPID int `json:"unowned_pid"`
	LiveRC     int `json:"live_rc"`
	HeldRC     int `json:"held_rc"`
	UnownedRC  int `json:"unowned_rc"`
}

func TestProcessStateExecPreservesOriginalChildren(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(processImageMode); mode {
	case "before":
		processImageBeforeExec(t)
		return
	case "after":
		processImageAfterExec(t)
		return
	case "":
	default:
		t.Fatalf("unknown process image test mode %q", mode)
	}
	report := filepath.Join(t.TempDir(), "receipt.json")
	ctx, cancel := context.WithTimeout(t.Context(), 4*processTestBudget)
	defer cancel()
	dir := t.TempDir()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessStateExecPreservesOriginalChildren$", "-test.count=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Dir = dir
	cmd.Env = append(processImageEnvironment("before"), processImageReport+"="+report)
	cmd.Env = append(cmd.Env, "TMPDIR="+dir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("same-PID process restore: %v\n%s", err, output)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	var got processImageReceipt
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ParentPID != cmd.Process.Pid || got.LivePID <= 0 || got.HeldPID <= 0 || got.UnownedPID <= 0 ||
		got.LiveRC != 23 || got.HeldRC != 37 || got.UnownedRC != 41 {
		t.Fatalf("wrong kernel/parent receipt after exec: %+v (original parent %d)", got, cmd.Process.Pid)
	}
}

func processImageBeforeExec(t *testing.T) {
	t.Helper()
	var manager workload.Manager
	live := startProcessFixture(t, 23)
	held := startProcessFixture(t, 37)
	terminal := startProcessFixture(t, 19)
	unowned := startProcessFixture(t, 41)
	for _, item := range []struct {
		child *processFixture
		id    string
		held  bool
	}{
		{live, "live", false}, {held, "held", true}, {terminal, "terminal", false},
	} {
		identity := workload.Identity{ID: item.id, OSPID: item.child.pid, StartedAt: time.Now().UTC()}
		var err error
		if item.held {
			err = manager.AdoptDraining(identity, item.child.process)
		} else {
			err = manager.Adopt(identity, item.child.process)
		}
		if err != nil {
			t.Fatal(err)
		}
		item.child.owner, item.child.id = &manager, item.id
	}
	t.Cleanup(func() { manager.Thaw(); _ = manager.AllowReap("held") })
	terminal.finish(t)
	result := receiveExit(t, observeExit(&manager, "terminal"))
	if result.err != nil || result.exit.Code != 19 {
		t.Fatalf("pre-exec terminal: %+v", result)
	}
	terminal.done = true
	held.finish(t)
	waitUntilKernelChildWaitable(t, held.pid)
	if _, err := manager.Freeze(); err != nil {
		t.Fatal(err)
	}
	state, err := manager.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	// A prior failed wait is already a terminal delivery receipt. This fixture
	// tests lossless import of that error; it does not manufacture a kernel exit.
	state.Records = append(state.Records, workload.Record{
		Identity: workload.Identity{ID: "wait-error", OSPID: terminal.pid, StartedAt: time.Now().UTC()},
		Exit:     &workload.Exit{Code: -1, EndedAt: time.Now().UTC(), WaitError: "retained ECHILD before exec"},
	})
	fixture := processImageFixture{State: state, UnownedPID: unowned.pid}
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{
		Role: "process-test", Instance: "same-parent", Generation: 1, Data: data,
	}, []handoff.Resource{{ID: "inputs", Kind: "pipe", Files: map[string]*os.File{
		"live": live.input, "unowned": unowned.input,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestProcessStateExecPreservesOriginalChildren$", "-test.count=1"}, processImageEnvironment("after")); err != nil {
		t.Fatalf("exec original owner: %v", err)
	}
	t.Fatal("successful exec returned to the old image")
}

func processImageAfterExec(t *testing.T) {
	t.Helper()
	packageState, err := handoff.Restore()
	if err != nil || packageState == nil {
		t.Fatalf("restore handoff envelope: %v", err)
	}
	defer func() { _ = packageState.Close() }()
	var fixture processImageFixture
	if err := json.Unmarshal(packageState.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.State.OwnerPID != os.Getpid() {
		t.Fatal("exec replaced the parent process identity")
	}
	var manager workload.Manager
	installed, unownedReaped := false, false
	t.Cleanup(func() {
		if installed {
			manager.Thaw()
			for _, row := range fixture.State.Records {
				if row.Exit == nil {
					_ = manager.AllowReap(row.ID)
				}
			}
			_ = manager.SignalAll(syscall.SIGKILL)
			for _, row := range fixture.State.Records {
				if row.Exit == nil {
					_ = receiveExit(t, observeExit(&manager, row.ID))
				}
			}
		} else {
			for _, row := range fixture.State.Records {
				if row.Exit == nil {
					_ = syscall.Kill(-row.OSPID, syscall.SIGKILL)
					var status syscall.WaitStatus
					_, _ = syscall.Wait4(row.OSPID, &status, 0, nil)
				}
			}
		}
		if !unownedReaped {
			_ = syscall.Kill(fixture.UnownedPID, syscall.SIGKILL)
			var status syscall.WaitStatus
			_, _ = syscall.Wait4(fixture.UnownedPID, &status, 0, nil)
		}
	})
	if err := manager.RestoreProcesses(fixture.State); err != nil {
		t.Fatal(err)
	}
	installed = true
	wantTerminal := workload.Exit{}
	livePID, heldPID := 0, 0
	for _, row := range fixture.State.Records {
		switch row.ID {
		case "live":
			livePID = row.OSPID
		case "held":
			heldPID = row.OSPID
		case "terminal":
			wantTerminal = *row.Exit
		}
	}
	for _, id := range []string{"live", "held"} {
		if err := manager.Signal(id, 0); !errors.Is(err, workload.ErrFrozen) {
			t.Fatalf("import signal before activation: %v", err)
		}
	}
	if err := manager.AllowReap("held"); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("held exit released before activation: %v", err)
	}
	terminal, err := manager.Wait("terminal")
	if err != nil || !reflect.DeepEqual(terminal, wantTerminal) {
		t.Fatalf("terminal kernel receipt changed: %+v %v", terminal, err)
	}
	failedWait, err := manager.Wait("wait-error")
	if err == nil || err.Error() != "retained ECHILD before exec" || failedWait.Code != -1 {
		t.Fatalf("retained wait error changed: %+v %v", failedWait, err)
	}
	liveInput, err := packageState.TakeFile("inputs", "live")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = liveInput.Close() }()
	unownedInput, err := packageState.TakeFile("inputs", "unowned")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unownedInput.Close() }()
	// Both children retain their original parent and open input endpoints. The
	// non-owned child exits first, giving an incorrect P_ALL reaper a real trap.
	if _, err := unownedInput.WriteString("exit\n"); err != nil {
		t.Fatal(err)
	}
	_ = unownedInput.Close()
	waitUntilKernelChildWaitable(t, fixture.UnownedPID)
	if _, err := liveInput.WriteString("exit\n"); err != nil {
		t.Fatal(err)
	}
	_ = liveInput.Close()
	waitUntilKernelChildWaitable(t, livePID)
	waitUntilKernelChildWaitable(t, heldPID)
	manager.Thaw()
	manager.Thaw()
	live := receiveExit(t, observeExit(&manager, "live"))
	if live.err != nil || live.exit.Code != 23 {
		t.Fatalf("live original child: %+v", live)
	}
	// Waiting for another managed child ensures the imported reaper has run.
	// The held leader must nevertheless remain a kernel-owned zombie.
	waitUntilKernelChildWaitable(t, heldPID)
	for _, row := range manager.Snapshot() {
		if row.ID == "held" && (!row.ReapHeld || row.Exit != nil) {
			t.Fatalf("Thaw lost the output-lifetime hold: %+v", row)
		}
	}
	if err := manager.AllowReap("held"); err != nil {
		t.Fatal(err)
	}
	held := receiveExit(t, observeExit(&manager, "held"))
	if held.err != nil || held.exit.Code != 37 {
		t.Fatalf("original held zombie status: %+v", held)
	}
	unowned, err := os.FindProcess(fixture.UnownedPID)
	if err != nil {
		t.Fatal(err)
	}
	unownedStatus, err := unowned.Wait()
	unownedReaped = true
	if err != nil || unownedStatus.ExitCode() != 41 {
		t.Fatalf("manager stole non-owned exit: %v %v", unownedStatus, err)
	}
	receipt := processImageReceipt{ParentPID: os.Getpid(), LivePID: livePID, HeldPID: heldPID,
		UnownedPID: fixture.UnownedPID, LiveRC: live.exit.Code, HeldRC: held.exit.Code, UnownedRC: unownedStatus.ExitCode()}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(processImageReport), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func processImageEnvironment(mode string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(entry, "=")
		if key != processImageMode {
			env = append(env, entry)
		}
	}
	return append(env, processImageMode+"="+mode)
}
