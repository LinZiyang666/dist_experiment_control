//go:build linux

// freeze_test.go — the deferrals (nothing frozen when the cut is refused)
// and the rollback path (Freeze → Prepare → exec failed → Thaw) over real
// children, five rounds, with the resource gate.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §5.1.
package agentimage

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// requireNothingFrozen proves a refused cut touched no domain.
func requireNothingFrozen(t *testing.T, f *bundleFixture, courier *courierStub, h proto.ProcessHandle) {
	t.Helper()
	if f.rt.Frozen() || f.manager.Frozen() {
		t.Fatalf("refused cut left runtime=%t table=%t frozen", f.rt.Frozen(), f.manager.Frozen())
	}
	if courier.exports != 0 {
		t.Fatal("refused cut exported the courier outbox")
	}
	if reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1}); err != nil || reply.Phase != "running" {
		t.Fatalf("runtime not serving after a refused cut: %+v %v", reply, err)
	}
}

func requireDeferred(t *testing.T, err error, reason string) {
	t.Helper()
	var deferred *DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != reason {
		t.Fatalf("want deferral %s, got %v", reason, err)
	}
}

// origin: hot-update S2 plan §5.1 — TestSpawnInFlightDefersFreeze: every
// hazardous condition defers with its reason and freezes nothing: an abandoned
// spawn slot, a probe still out (including one the timeout gave up on), a
// Busy hook, and a creation still starting in the runtime.
func TestSpawnInFlightDefersFreeze(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "defer-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	probe := &bundleProbe{block: make(chan bool)}
	policy := bundleSpawnPolicy(t, "/defer-nfs", probe, time.Now)
	courier := &courierStub{state: courierEvents()}
	owners := Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier, Lineage: Lineage{Generation: 1}}
	deadline := func() time.Time { return time.Now().Add(bundleTestBudget) }

	// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 2.
	// A raw slot is now an in-time operation. Preserve the immediate deferral
	// assertion by exercising the real watchdog-to-reaper transition instead.
	releaseSpawn := make(chan struct{})
	if err := policy.RunStart(func() error { <-releaseSpawn; return nil }, time.Millisecond); !errors.Is(err, spawnsafe.ErrSpawnTimeout) {
		t.Fatal(err)
	}
	releaseAbandoned := sync.OnceFunc(func() {
		close(releaseSpawn)
		if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, func() bool { return policy.WedgedCount() == 0 }) {
			t.Error("abandoned slot did not drain")
		}
	})
	t.Cleanup(releaseAbandoned)
	_, err := Freeze(owners, deadline())
	requireDeferred(t, err, ReasonSpawnInFlight)
	requireNothingFrozen(t, f, courier, h)
	releaseAbandoned()

	// A probe that never returns: launched through the exported path, then
	// timed out (sticky dead) — still in flight for the fence.
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		_, _ = policy.Prepare([]string{"/bin/true"}, "/defer-nfs", "/usr/bin", nil, false)
	}()
	waitDeadline := time.Now().Add(bundleTestBudget)
	for policy.Census().ProbesInFlight == 0 {
		if time.Now().After(waitDeadline) {
			t.Fatal("probe never launched")
		}
		time.Sleep(time.Millisecond)
	}
	_, err = Freeze(owners, deadline())
	requireDeferred(t, err, ReasonRemoteFSProbeInFlight)
	requireNothingFrozen(t, f, courier, h)
	<-probeDone // the launcher timed out (2 s probe timeout in bundleSpawnPolicy)
	_, err = Freeze(owners, deadline())
	requireDeferred(t, err, ReasonRemoteFSProbeInFlight)
	requireNothingFrozen(t, f, courier, h)
	probe.block <- false // the probe returns: the mount stays dead, the fence is clear
	for policy.Census().ProbesInFlight != 0 {
		if time.Now().After(waitDeadline) {
			t.Fatal("probe never returned")
		}
		time.Sleep(time.Millisecond)
	}

	busy := Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier, Lineage: Lineage{Generation: 1},
		Busy: []BusyCheck{func() (string, string, bool) { return "", "", false }, func() (string, string, bool) { return "pty_busy", "3 sessions", true }}}
	_, err = Freeze(busy, deadline())
	requireDeferred(t, err, "pty_busy")
	requireNothingFrozen(t, f, courier, h)

	// A creation still in the adapter's Start: the runtime reports ErrBusy
	// at the deadline and the fence maps it to spawn_in_flight.
	fdBefore := testharness.FDCount()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseStart := sync.OnceFunc(func() { close(release) })
	slow := f.cfg
	slowManager := &workload.Manager{}
	slow.Manager = slowManager
	slow.Start = func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		close(entered)
		<-release
		stdio, err := workload.NewStdio()
		if err != nil {
			return nil, "", err
		}
		cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdio.Bind(cmd)
		if err := cmd.Start(); err != nil {
			stdio.Close()
			return nil, "", err
		}
		stdio.CloseChild()
		if err := slowManager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now()}, cmd.Process); err != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			stdio.Close()
			return nil, "", err
		}
		return stdio, "", nil
	}
	slowRT, err := execsession.New(slow)
	if err != nil {
		t.Fatal(err)
	}
	// origin: docs/testing-standards.md §三 A1 — assert this fixture's
	// ownership after cleanup, while retaining the runtime so finalizers
	// cannot hide unclosed replay files from the descriptor census.
	t.Cleanup(func() {
		if used := slowRT.ReplayBudgetUsed(); used != 0 {
			t.Errorf("starting runtime retained %d replay bytes after cleanup", used)
		}
		if rows := slowManager.Snapshot(); len(rows) != 0 {
			t.Errorf("starting runtime retained process rows after cleanup: %+v", rows)
		}
		if got := testharness.FDCount(); got != fdBefore {
			t.Errorf("starting runtime descriptors after cleanup: %d vs %d", got, fdBefore)
		}
		runtime.KeepAlive(slowRT)
	})
	t.Cleanup(func() {
		slowManager.Thaw()
		_ = slowManager.SignalAll(syscall.SIGKILL)
		// SignalAll can run before the blocked Start adopts its child.
		// Close joins that startup owner and releases its pipes and replays.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), bundleTestBudget)
		defer cancel()
		if err := slowRT.Close(ctx); err != nil {
			t.Errorf("close starting runtime: %v", err)
		}
	})
	t.Cleanup(releaseStart)
	other := &bundleFixture{manager: slowManager, cfg: slow, rt: slowRT, scope: f.scope, exited: map[string]int{}}
	other.create(t, "starting", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	<-entered
	_, err = Freeze(Owners{Runtime: slowRT, Manager: slowManager, Lineage: Lineage{Generation: 1}}, time.Now().Add(150*time.Millisecond))
	requireDeferred(t, err, ReasonSpawnInFlight)
	if slowRT.Frozen() || slowManager.Frozen() {
		t.Fatal("deferred cut left the fence up")
	}
	releaseStart()
}

// origin: hot-update S2 plan §5.1 — TestFreezeDefersOnForeignProcessRows: a
// child in the shared table that no reliable task owns (a legacy exec, a
// PTY) defers the cut BEFORE the runtime is frozen.
func TestFreezeDefersOnForeignProcessRows(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	fdBefore := testharness.FDCount()
	var f *bundleFixture
	// origin: docs/testing-standards.md §三 A1 — a discarded exported
	// runtime does not release the process table's reap hold or pidfd.
	t.Cleanup(func() {
		if f != nil {
			if rows := f.manager.Snapshot(); len(rows) != 0 {
				t.Errorf("foreign-row fixture retained process rows after cleanup: %+v", rows)
			}
		}
		if got := testharness.FDCount(); got != fdBefore {
			t.Errorf("foreign-row fixture descriptors after cleanup: %d vs %d", got, fdBefore)
		}
		runtime.KeepAlive(f)
	})
	f = newBundleFixture(t, "foreign-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	legacy := exec.Command("/bin/sh", "-c", "exec sleep 30")
	legacy.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := legacy.Start(); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Adopt(workload.Identity{ID: "legacy-exec", OSPID: legacy.Process.Pid, StartedAt: time.Now()}, legacy.Process); err != nil {
		t.Fatal(err)
	}
	courier := &courierStub{state: courierEvents()}
	_, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: courier, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	requireDeferred(t, err, ReasonForeignProcessRows)
	requireNothingFrozen(t, f, courier, h)
	_ = f.manager.Signal("legacy-exec", syscall.SIGKILL)
	_ = f.manager.AllowReap("legacy-exec")
	if _, err := f.manager.Wait("legacy-exec"); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Forget("legacy-exec"); err != nil {
		t.Fatal(err)
	}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: courier, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatalf("cut after the foreign child was retired: %v", err)
	}
	// Roll back this successful cut before the fixture closes its runtime:
	// Close on an exported frozen runtime means discard, not child teardown.
	t.Cleanup(func() {
		if err := frozen.Thaw(); err != nil {
			t.Errorf("thaw foreign-row fixture: %v", err)
		}
	})
}

// origin: hot-update S2 plan exit #5 / §5.1 — five rollbacks (Freeze →
// Prepare → Prepared.Close → Frozen.Thaw, the exec-failed path) return
// every descriptor, memfd and goroutine, and the runtime keeps serving:
// the child's bytes and terminal arrive after the last rollback. Missing
// the Manager thaw would make the second round's Manager.Freeze fail;
// missing the registry thaw would make the drain fail.
func TestBundleFreezeRollbackReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "rollback-instance")
	marker := t.TempDir() + "/go"
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "printf before; while [ ! -e " + marker + " ]; do sleep 0.02; done; printf after; exit 23"}, nil)
	f.awaitRunning(t, h)
	exerciseBundleFreezeRollback(t, f) // warm-up
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseBundleFreezeRollback(t, f)
	}
	testharness.AssertNoGoroutineLeak(t, "bundle-freeze-rollback", beforeG)
	testharness.AssertNoFDLeak(t, "bundle-freeze-rollback", beforeFD, 0)
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out, reply := f.drain(t, h)
	if out != "beforeafter" || reply.ExitCode != 23 {
		t.Fatalf("task after five rollbacks: out=%q reply=%+v", out, reply)
	}
}

func exerciseBundleFreezeRollback(t *testing.T, f *bundleFixture) {
	t.Helper()
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: &courierStub{state: courierEvents()}, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := frozen.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	// While frozen, nothing pumps: a resume parks.
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		_, _ = f.rt.Handle(f.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 99999, Op: "probe",
			ActorFP: f.scope.ActorFP, ClientLineage: f.scope.ClientLineage}, "_TINBOX.bundle")
	}()
	select {
	case <-parked:
		t.Fatal("probe answered under the fence")
	case <-time.After(30 * time.Millisecond):
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	<-parked
	if f.rt.Frozen() || f.manager.Frozen() {
		t.Fatal("rollback left the fence up")
	}
}

// origin: hot-update S3 plan §1.4 P5 — a revocation that lands between
// Freeze and exec is only marked under the fence; Prepare must re-export the
// runtime so the dispatched bundle carries it, or the new image would thaw a
// task its session already revoked. The courier domain is deliberately not
// re-read (it keeps delivering until exec).
func TestFrozenPrepareCarriesRevocationsMarkedAfterFreeze(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "late-revoke-instance")
	h, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	courier := &courierStub{state: courierEvents()}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Courier: courier, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	if len(frozen.Bundle().Runtime.Revocations) != 0 {
		t.Fatalf("fixture cut already carries revocations: %+v", frozen.Bundle().Runtime.Revocations)
	}
	// Under the fence: marks only, no kill (revoke.go), and it is not in the
	// bundle Freeze assembled.
	if _, err := f.rt.Revoke(f.scope.SID, "session_destroyed"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(f.manager.Snapshot()[0].OSPID, 0); err != nil {
		t.Fatal("a revoke under the fence killed the child")
	}
	courierExportsBefore := courier.exports
	prepared, err := frozen.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if courier.exports != courierExportsBefore {
		t.Fatalf("Prepare re-read the courier outbox (%d → %d exports)", courierExportsBefore, courier.exports)
	}
	b := frozen.Bundle()
	if len(b.Runtime.Revocations) != 1 || b.Runtime.Revocations[0].SID != f.scope.SID || b.Runtime.Revocations[0].Reason != "session_destroyed" {
		t.Fatalf("Prepare did not re-export the revocation: %+v", b.Runtime.Revocations)
	}
	if b.Runtime.Tasks[0].Revoked == "" {
		t.Fatalf("the live task is not marked revoked in the re-exported image: %+v", b.Runtime.Tasks[0])
	}
	// Rollback: the deferred kill is carried out by Thaw (a revoked task
	// refuses resume, so the child's death is read from the table).
	osPID := f.manager.Snapshot()[0].OSPID
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(bundleTestBudget)
	for {
		rows := f.manager.Snapshot()
		if len(rows) == 0 || rows[0].Exit != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("revoked child %d still alive after thaw: %+v", osPID, rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// origin: hot-update S3 plan §1.4 P6 — the deferred set is a contract with
// the operator (architecture §22.8): nine distinct codes, in that order.
// origin: hot-update S5 internal review rv-authority-7
func TestDeferredReasonSetIsTheDocumentedOne(t *testing.T) {
	want := []string{"spawn_in_flight", "remote_fs_probe_in_flight", "foreign_process_rows", "image_too_large",
		"runtime_closed", "platform_unsupported", "transfers_in_flight", "legacy_session_busy", "ss_state_unrepresentable"}
	if len(DeferredReasons) != len(want) {
		t.Fatalf("DeferredReasons has %d entries, want %d", len(DeferredReasons), len(want))
	}
	seen := make(map[string]bool, len(DeferredReasons))
	for i, reason := range DeferredReasons {
		if reason != want[i] {
			t.Fatalf("DeferredReasons[%d] = %q, documented %q", i, reason, want[i])
		}
		if seen[reason] {
			t.Fatalf("reason %q listed twice", reason)
		}
		seen[reason] = true
	}
}
