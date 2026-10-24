//go:build linux

// Real: same-PID exec, original session members, stopped jobs, master/replay
// descriptors, frozen actor import and administrative cleanup. No network or
// agent upgrade policy is involved. The fixture supplies a legal mid-batch
// cleanup image; production Actor import/export/activation must preserve it.
package workload_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const sessionControlMode = "TETHER_TEST_SESSION_CONTROL_MODE"
const sessionControlRoot = "TETHER_TEST_SESSION_CONTROL_ROOT"

type sessionControlProcesses struct {
	Leader      int    `json:"leader"`
	Members     []int  `json:"members"`
	Environment string `json:"environment"`
}

type sessionControlCut struct {
	Processes workload.ProcessState    `json:"processes"`
	Terminal  ptysession.Image         `json:"terminal"`
	Fixture   sessionControlProcesses  `json:"fixture"`
	Remaining []workload.SessionMember `json:"remaining"`
}

type sessionControlReceipt struct {
	ParentPID        int   `json:"parent_pid"`
	LeaderPID        int   `json:"leader_pid"`
	LeaderTicks      int64 `json:"leader_ticks"`
	RemainingPID     int   `json:"remaining_pid"`
	EscapedPID       int   `json:"escaped_pid"`
	ExitSignal       int   `json:"exit_signal"`
	PendingRestored  bool  `json:"pending_restored"`
	MembersRestored  bool  `json:"members_restored"`
	EscapedAlive     bool  `json:"escaped_alive"`
	BudgetAfterClose int64 `json:"budget_after_close"`
	HandoffFDs       int   `json:"handoff_fds"`
	ReplayFDs        int   `json:"replay_fds"`
	PidFDs           int   `json:"pid_fds"`
}

// origin: hot-update S4 plan §6.2 / T16
func TestSessionCleanupSurvivesSamePIDExec(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch os.Getenv(sessionControlMode) {
	case "before":
		sessionCleanupBeforeExec(t)
		return
	case "after":
		sessionCleanupAfterExec(t)
		return
	case "":
	default:
		t.Fatal("unknown session cleanup helper mode")
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			root := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 3*processTestBudget)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSessionCleanupSurvivesSamePIDExec$", "-test.count=1")
			cmd.Dir = root
			cmd.Env = append(sessionControlEnvironment("before"), "TMPDIR="+root, sessionControlRoot+"="+root)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
			cmd.Cancel = func() error {
				// The supervisor is a subreaper; kill its known fixture before
				// cancelling it so a timeout does not orphan detached children.
				data, _ := os.ReadFile(filepath.Join(root, "ready"))
				var fixture sessionControlProcesses
				if json.Unmarshal(data, &fixture) == nil {
					for _, pid := range append(fixture.Members, fixture.Leader) {
						if pid > 0 {
							_ = syscall.Kill(pid, syscall.SIGKILL)
						}
					}
				}
				return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			t.Cleanup(func() {
				if cmd.Process != nil {
					_ = cmd.Cancel()
				}
			})
			output, err := cmd.CombinedOutput()
			if strings.Contains(string(output), "\"secret\"") || strings.Contains(string(output), "[redacted]") {
				t.Fatal("cleanup helper printed image secrets")
			}
			if err != nil {
				t.Fatalf("same-PID session cleanup (NOT_RUN if a P2/P3 stub is named): %v\n%s", err, output)
			}
			var receipt sessionControlReceipt
			readSessionJSON(t, filepath.Join(root, "receipt"), &receipt)
			if receipt.ParentPID != cmd.Process.Pid || receipt.LeaderPID <= 0 || receipt.LeaderTicks <= 0 ||
				receipt.RemainingPID <= 0 || receipt.EscapedPID <= 0 || receipt.ExitSignal != int(syscall.SIGKILL) ||
				!receipt.PendingRestored || !receipt.MembersRestored || !receipt.EscapedAlive || receipt.BudgetAfterClose != 0 ||
				receipt.HandoffFDs != 0 || receipt.ReplayFDs != 0 || receipt.PidFDs != 0 {
				t.Fatalf("incomplete cleanup/resource receipt: %+v", receipt)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "same-PID session cleanup", goroutines)
	testharness.AssertNoFDLeak(t, "same-PID session cleanup", fds, 0)
}

func sessionCleanupBeforeExec(t *testing.T) {
	t.Helper()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	root := os.Getenv(sessionControlRoot)
	session, err := pty.Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start([]string{python, "-c", sessionFixtureScript, root}, sessionControlEnvironment(""), "", ""); err != nil {
		t.Fatal(err)
	}
	var fixture sessionControlProcesses
	var manager workload.Manager
	adopted := false
	// This fallback reaps exact fixture PIDs; it never relies on the cleanup
	// code under test to prevent orphaned grandchildren on an assertion failure.
	defer func() { cleanupSessionFixture(t, &manager, "terminal", session.OSPID(), fixture.Members, adopted) }()
	readSessionJSON(t, filepath.Join(root, "ready"), &fixture)
	if fixture.Leader != session.OSPID() || len(fixture.Members) != 3 || fixture.Environment != "" {
		t.Fatalf("invalid session fixture: %+v", fixture)
	}
	waitSessionCondition(t, "stopped session member", func() bool { return sessionProcessState(t, fixture.Members[1]) == "T" })
	id := imageChildIdentity(t, processStateEnvelope(t), &processFixture{pid: session.OSPID()}, "terminal")
	if err := session.AdoptDraining(&manager, id); err != nil {
		t.Fatal(err)
	}
	adopted = true
	master, identity, err := session.TakeMaster()
	if err != nil {
		t.Fatal(err)
	}
	var budget workload.ReplayBudget
	input, err := workload.NewReplay(&budget, upgradebudget.PTYInputBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	output, err := workload.NewReplay(&budget, upgradebudget.StreamReplayBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	actor, err := ptysession.New(ptysession.Config{Manager: &manager, PID: id.ID, Master: master, Identity: identity, Input: input, Output: output, Clock: procrequest.BootNow})
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	defer func() { _ = actor.Close() }()
	deadline := time.Now().Add(processTestBudget)
	if _, _, err := actor.Freeze(deadline); err != nil {
		t.Fatal(err)
	}
	if err := actor.Revoke("revoked"); err != nil {
		t.Fatal(err)
	}
	// Complete one bounded administrative batch while the actor is frozen.
	// The leader is held as a zombie, one other PGID is killed, and the stopped
	// PGID deliberately survives this batch. The actor owns the pending revoke.
	members, err := workload.ScanSession(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("missing cross-PGID fixture: %+v", members)
	}
	if err := manager.WithControl(id.ID, func(c workload.Control) error { return c.Signal(syscall.SIGKILL) }); err != nil {
		t.Fatal(err)
	}
	waitUntilKernelChildWaitable(t, id.OSPID)
	for _, member := range members {
		if member.Identity.OSPID == fixture.Members[0] {
			if err := workload.SignalIdentity(member, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitSessionCondition(t, "first cleanup batch completed", func() bool {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", fixture.Members[0]))
		return errors.Is(err, os.ErrNotExist) || (err == nil && strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])[0] == "Z")
	})
	remaining, err := workload.ScanSession(id)
	if err != nil || len(remaining) != 1 || remaining[0].Identity.OSPID != fixture.Members[1] {
		t.Fatalf("partial batch did not retain stopped PGID: %+v %v", remaining, err)
	}

	// Leader death may send the foreground/orphaned group HUP followed by CONT.
	// Re-establish and observe the stopped premise after that kernel event;
	// otherwise the after-exec assertion wrongly attributes it to pure import.
	// origin: hot-update S4 plan §6.2 / T16
	if err := workload.SignalIdentity(remaining[0], syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	waitSessionCondition(t, "stopped member at cut", func() bool { return sessionProcessState(t, remaining[0].Identity.OSPID) == "T" })
	if _, err := manager.FreezeUntil(deadline); err != nil {
		t.Fatal(err)
	}
	processes, err := manager.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	image, files, err := actor.Freeze(deadline)
	if err != nil {
		t.Fatal(err)
	}
	if !image.CleanupPending || image.Revoked == "" {
		t.Fatal("frozen revocation lost cleanup responsibility")
	}
	// This is a legal batch-boundary fixture, populated exclusively from the
	// real census. Import followed by re-export must retain these exact members;
	// this does not substitute a fake actor or claim to exercise P3's batch loop.
	image.CleanupMembers = append([]workload.SessionMember(nil), remaining...)
	// Keep authority expired across the cut as well: Tick must settle the
	// reap hold through its repeated Revoke path, not only its revoked branch.
	// origin: hot-update S4 plan §6.2 / T16
	image.AuthorityDeadlineBootNS = 1
	cut := sessionControlCut{Processes: processes, Terminal: image, Fixture: fixture, Remaining: remaining}
	data, err := json.Marshal(cut)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "session-cleanup-test", Instance: "original-owner", Generation: 1, Data: data}, []handoff.Resource{{ID: "pty-task.terminal", Kind: "pty-task-v1", Files: files}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	env := append(sessionControlEnvironment("after"), sessionControlRoot+"="+root)
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestSessionCleanupSurvivesSamePIDExec$", "-test.count=1"}, env); err != nil {
		t.Fatal(err)
	}
	t.Fatal("successful exec returned")
}

func sessionCleanupAfterExec(t *testing.T) {
	t.Helper()
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("restore cleanup handoff: %v", err)
	}
	defer func() { _ = restored.Close() }()
	if again, err := handoff.Restore(); err != nil || again != nil {
		t.Fatalf("handoff consumed twice: %v", err)
	}
	var cut sessionControlCut
	if err := json.Unmarshal(restored.State.Data, &cut); err != nil {
		t.Fatal(err)
	}
	if cut.Processes.OwnerPID != os.Getpid() || !cut.Terminal.CleanupPending || len(cut.Remaining) != 1 {
		t.Fatal("same-PID cut lost pending member responsibility")
	}
	var manager workload.Manager
	installed := false
	cleaned := false
	defer func() {
		if !cleaned {
			cleanupSessionFixture(t, &manager, "terminal", cut.Fixture.Leader, cut.Fixture.Members, installed)
		}
	}()
	if err := manager.RestoreProcesses(cut.Processes); err != nil {
		t.Fatal(err)
	}
	installed = true
	row, ok := manager.Lookup("terminal")
	if !ok || row.Exit != nil || !row.ReapHeld || row.StartTimeTicks != cut.Terminal.Identity.StartTimeTicks {
		t.Fatal("cleanup leader was reaped or replaced across exec")
	}
	masterFile, err := restored.TakeFile("pty-task.terminal", "master")
	if err != nil {
		t.Fatal(err)
	}
	master, err := pty.RestoreMaster(masterFile, cut.Terminal.Identity, row)
	if err != nil {
		_ = masterFile.Close()
		t.Fatal(err)
	}
	var budget workload.ReplayBudget
	inputFile, err := restored.TakeFile("pty-task.terminal", "input-replay")
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	input, err := workload.RestoreReplay(&budget, cut.Terminal.Input, inputFile)
	if err != nil {
		_ = inputFile.Close()
		_ = master.Close()
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	outputFile, err := restored.TakeFile("pty-task.terminal", "output-replay")
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	output, err := workload.RestoreReplay(&budget, cut.Terminal.Output, outputFile)
	if err != nil {
		_ = outputFile.Close()
		_ = master.Close()
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	actor, err := ptysession.Import(ptysession.Config{Manager: &manager, PID: "terminal", Master: master, Identity: cut.Terminal.Identity, Input: input, Output: output, Clock: procrequest.BootNow}, cut.Terminal)
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	defer func() { _ = actor.Close() }()
	image, _, err := actor.Freeze(time.Now().Add(processTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	if !image.CleanupPending || !reflect.DeepEqual(image.CleanupMembers, cut.Remaining) {
		t.Fatalf("import/re-export dropped pending cleanup members: %+v", image.CleanupMembers)
	}
	if sessionProcessState(t, cut.Remaining[0].Identity.OSPID) != "T" {
		t.Fatal("pure import signalled the stopped member")
	}
	if err := actor.PreActivate(); err != nil {
		t.Fatal(err)
	}
	manager.Thaw()
	if err := actor.Activate(); err != nil {
		t.Fatal(err)
	}
	// This activation began with a live stopped member. Even if SIGKILL made
	// its second census empty, completion still needs a subsequent empty pair.
	// origin: hot-update S4 plan §6.2 / T16
	pending, _, err := actor.Freeze(time.Now().Add(processTestBudget))
	if err != nil || !pending.CleanupPending {
		t.Fatalf("single census settled cleanup: pending=%v err=%v", pending.CleanupPending, err)
	}
	if err := actor.PreActivate(); err != nil {
		t.Fatal(err)
	}
	if err := actor.Activate(); err != nil {
		t.Fatal(err)
	}
	result := waitSessionCleanupExit(t, actor, observeExit(&manager, "terminal"))
	if result.err != nil || result.exit.Signal != int(syscall.SIGKILL) {
		t.Fatalf("cleanup lost real leader exit: %+v", result)
	}
	// A terminal receipt may not precede elimination of the remaining live
	// session member. A zombie is no longer a slave/fork owner; reap it below.
	for _, pid := range cut.Fixture.Members[:2] {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err == nil {
			state := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])[0]
			if state != "Z" && state != "X" {
				t.Fatalf("terminal published with a live session member %d (%s)", pid, state)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	escapedState := sessionProcessState(t, cut.Fixture.Members[2])
	if escapedState == "Z" || escapedState == "X" {
		t.Fatal("cleanup claimed arbitrary descendant containment")
	}
	if err := actor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	cleanupSessionFixture(t, &manager, "terminal", cut.Fixture.Leader, cut.Fixture.Members, true)
	cleaned = true
	if err := manager.Forget("terminal"); err != nil {
		t.Fatal(err)
	}
	receipt := sessionControlReceipt{ParentPID: os.Getpid(), LeaderPID: row.OSPID, LeaderTicks: row.StartTimeTicks,
		RemainingPID: cut.Remaining[0].Identity.OSPID, EscapedPID: cut.Fixture.Members[2], ExitSignal: result.exit.Signal,
		PendingRestored: image.CleanupPending, MembersRestored: reflect.DeepEqual(image.CleanupMembers, cut.Remaining), EscapedAlive: true, BudgetAfterClose: budget.Used()}
	for _, entry := range mustSessionFDEntries(t) {
		link, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		switch {
		case strings.Contains(link, "/memfd:tether-handoff"):
			receipt.HandoffFDs++
		case strings.Contains(link, "/memfd:tether-replay"):
			receipt.ReplayFDs++
		case link == "anon_inode:[pidfd]":
			receipt.PidFDs++
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv(sessionControlRoot), "receipt"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func cleanupSessionFixture(t *testing.T, manager *workload.Manager, id string, leader int, members []int, installed bool) {
	t.Helper()
	for _, pid := range members {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	if leader > 0 {
		_ = syscall.Kill(leader, syscall.SIGKILL)
	}
	if installed {
		manager.Thaw()
		_ = manager.AllowReap(id)
		if row, ok := manager.Lookup(id); ok && row.Exit == nil {
			_ = receiveExit(t, observeExit(manager, id))
		}
	} else if leader > 0 {
		var status syscall.WaitStatus
		_, _ = syscall.Wait4(leader, &status, 0, nil)
	}
	for _, pid := range members {
		var status syscall.WaitStatus
		for {
			_, err := syscall.Wait4(pid, &status, 0, nil)
			if !errors.Is(err, syscall.EINTR) {
				break
			}
		}
	}
}

func sessionControlEnvironment(mode string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(entry, "=")
		if key != sessionControlMode && key != sessionControlRoot {
			env = append(env, entry)
		}
	}
	if mode != "" {
		env = append(env, sessionControlMode+"="+mode)
	}
	return env
}

func readSessionJSON(t *testing.T, path string, out any) {
	t.Helper()
	var data []byte
	waitSessionCondition(t, "session receipt "+filepath.Base(path), func() bool {
		var err error
		data, err = os.ReadFile(path)
		return err == nil
	})
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
}

func mustSessionFDEntries(t *testing.T) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// A missing /proc leader is a real census failure, not an injected successful
// dependency. Consume that leader's status from outside the Manager solely to
// model a broken ownership boundary; production must keep the revoke pending.
// origin: hot-update S4 plan §6.2 / T15
func sessionCleanupScanFailure(t *testing.T) {
	t.Helper()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	root := os.Getenv(sessionControlRoot)
	session, err := pty.Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start([]string{python, "-c", sessionFixtureScript, root}, sessionControlEnvironment(""), "", ""); err != nil {
		t.Fatal(err)
	}
	var fixture sessionControlProcesses
	var manager workload.Manager
	adopted := false
	defer func() { cleanupSessionFixture(t, &manager, "scan-failure", session.OSPID(), fixture.Members, adopted) }()
	readSessionJSON(t, filepath.Join(root, "ready"), &fixture)
	id := imageChildIdentity(t, processStateEnvelope(t), &processFixture{pid: session.OSPID()}, "scan-failure")
	if err := session.AdoptDraining(&manager, id); err != nil {
		t.Fatal(err)
	}
	adopted = true
	master, identity, err := session.TakeMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = master.Close() }()
	var budget workload.ReplayBudget
	input, err := workload.NewReplay(&budget, upgradebudget.PTYInputBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	output, err := workload.NewReplay(&budget, upgradebudget.StreamReplayBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	actor, err := ptysession.New(ptysession.Config{Manager: &manager, PID: id.ID, Master: master, Identity: identity, Input: input, Output: output, Clock: procrequest.BootNow})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = actor.Close() }()
	if _, _, err := actor.Freeze(time.Now().Add(processTestBudget)); err != nil {
		t.Fatal(err)
	}
	if err := actor.Revoke("revoked"); err != nil {
		t.Fatal(err)
	}
	if err := actor.PreActivate(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Signal(id.ID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitUntilKernelChildWaitable(t, id.OSPID)
	var status syscall.WaitStatus
	if _, err := syscall.Wait4(id.OSPID, &status, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := workload.ScanSession(id); err == nil {
		t.Fatal("missing /proc leader did not produce census failure")
	}
	// Activation may report the failure or leave a retry obligation. Either
	// way a new cut must retain it; no error may be translated into completion.
	_ = actor.Activate()
	image, _, err := actor.Freeze(time.Now().Add(processTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	if !image.CleanupPending || image.Revoked == "" {
		t.Fatal("failed census erased the pending cleanup obligation")
	}
	if row, ok := manager.Lookup(id.ID); !ok || !row.ReapHeld {
		t.Fatal("failed cleanup released the leader's reap hold")
	}
}

func exerciseSessionScanFailure(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*processTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessControlGuardAndSessionCleanup$", "-test.count=1")
	root := t.TempDir()
	cmd.Dir = t.TempDir()
	cmd.Env = append(sessionControlEnvironment("scan-failure"), sessionControlRoot+"="+root)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		data, _ := os.ReadFile(filepath.Join(root, "ready"))
		var fixture sessionControlProcesses
		if json.Unmarshal(data, &fixture) == nil {
			for _, pid := range append(fixture.Members, fixture.Leader) {
				if pid > 0 {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("census failure cleanup (NOT_RUN if a P2/P3 stub is named): %v\n%s", err, output)
	}
}

func waitSessionCleanupExit(t *testing.T, actor *ptysession.Actor, exits <-chan observedExit) observedExit {
	t.Helper()
	deadline := time.NewTimer(processTestBudget)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case result := <-exits:
			return result
		case <-tick.C:
			if err := actor.Tick(ptysession.HealthProof{}); err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("restored cleanup did not release the original leader")
			return observedExit{}
		}
	}
}
