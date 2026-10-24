//go:build linux

// fence_admission_test.go — the image fence's admission sources under
// states the plan's §1.5 table names but the round-trip tests do not reach:
// Export on an imported runtime before Thaw, Detach after a Thaw, Close
// under a foreign-frozen process table, attach ops during quiescing, and
// the running-abandoned Discard's effect on the reap hold. Real children,
// real pipes, real memfd replays.
// origin: hot-update S2 internal review, fence lane.
package execsession

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S2 internal review, fence lane — plan §1.5 row
// "Export" × "frozen+imported (new image, before Thaw)" says "允许（回滚重导
// 当前态）"; Import never fills task.frozen, so Export refuses every imported
// task with "has no frozen state" until the runtime has been thawed and
// frozen again. A bootstrap that imported and then wants to hand the SAME
// cut back to the previous image (D6's re-dispatch) cannot re-export it.
func TestFenceExportOnImportedRuntimeBeforeThaw(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	src := newImageDupSource(t, rf.resources)
	imported, salvage, err := Import(rf.cfg, rf.img, rf.rows, src)
	if err != nil {
		t.Fatalf("import: %v (salvage %d)", err, len(salvage))
	}
	if !imported.Frozen() {
		t.Fatal("an imported runtime must be born frozen")
	}
	reexport, resources, exportErr := imported.Export()
	if exportErr != nil {
		t.Fatalf("plan §1.5 promises Export on a frozen imported runtime before Thaw; got: %v", exportErr)
	}
	// The re-export is the same cut: same tasks, offsets and budget; its files
	// are the imported duplicates, each open.
	if len(reexport.Tasks) != len(rf.img.Tasks) || reexport.BudgetUsed != rf.img.BudgetUsed || len(resources) != len(rf.resources) {
		t.Fatalf("re-export differs from the cut: tasks=%d/%d budget=%d/%d resources=%d/%d",
			len(reexport.Tasks), len(rf.img.Tasks), reexport.BudgetUsed, rf.img.BudgetUsed, len(resources), len(rf.resources))
	}
	for i := range reexport.Tasks {
		if reexport.Tasks[i].PID != rf.img.Tasks[i].PID || reexport.Tasks[i].LastTouch != rf.img.Tasks[i].LastTouch ||
			(reexport.Tasks[i].Stdout == nil) != (rf.img.Tasks[i].Stdout == nil) {
			t.Fatalf("re-exported task %d differs: %+v vs %+v", i, reexport.Tasks[i], rf.img.Tasks[i])
		}
		if reexport.Tasks[i].Stdout != nil && reexport.Tasks[i].Stdout.Replay != rf.img.Tasks[i].Stdout.Replay {
			t.Fatalf("re-exported stdout state differs for task %d", i)
		}
	}
	for _, res := range resources {
		for name, f := range res.Files {
			if _, err := f.Stat(); err != nil {
				t.Fatalf("re-exported %s/%s is closed: %v", res.ID, name, err)
			}
		}
	}
	// After an Export the originals may have a second owner (a Prepared):
	// Detach is refused; Discard is the epilogue (S2 internal review F2/F3).
	if _, err := imported.Detach(); !errors.Is(err, ErrNotFrozen) {
		t.Fatalf("Detach after Export: %v, want ErrNotFrozen", err)
	}
	if err := imported.Discard(); err != nil {
		t.Fatalf("discard after re-export: %v", err)
	}
}

// origin: hot-update S2 internal review, fence lane — fence.go's Detach
// comment and plan §1.5 say a THAWED imported runtime is refused; the guard
// is only `frozen && imported`, so an imported runtime that was thawed
// (observers running, offsets moved) and frozen again is dismantled by
// Detach: the files come back with no Image describing them and the live
// task's observer is left to find nil actors.
func TestFenceDetachAcceptedAfterThaw(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	imported := imageSwitch(t, rf.f, rf.cfg, rf.img, rf.resources, rf.rows)
	// The live task now has an observer (Thaw started it) and its pumps run.
	imported.mu.Lock()
	live := imported.tasks[rf.live]
	owned := live != nil && live.owned
	imported.mu.Unlock()
	if !owned {
		t.Fatal("the imported live task is not owned after Thaw")
	}
	imageFreeze(t, imported)
	resources, err := imported.Detach()
	if err != nil {
		if !errors.Is(err, ErrNotFrozen) {
			t.Fatalf("detach after thaw: %v", err)
		}
		return // refused, as documented
	}
	for _, res := range resources {
		for _, f := range res.Files {
			_ = f.Close()
		}
	}
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
	t.Fatalf("Detach accepted a thawed-then-refrozen imported runtime and returned %d resources; the doc says it is refused", len(resources))
}

// origin: hot-update S2 internal review, fence lane — plan §1.5 row
// "Close(ctx)" × "frozen (old image, not exported)" says "thawLocked 后
// teardown", and Close's own contract (S0) is "actual runtime teardown":
// every child is SIGKILLed. Under the fence the process table is normally
// frozen too (agentimage freezes it right after the runtime), and
// Manager.Signal answers ErrFrozen, which Close discards with `_ =`. Close
// then returns nil with the child alive, its row live, no Exited delivered:
// a shutdown between Freeze and Thaw silently leaks the subtree.
func TestFenceCloseUnderFrozenTableLeavesChildAlive(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 256)
	manager := r.cfg.Manager
	scope := clockedScope("closeUnderFrozenTable")
	live, _ := clockedCreate(t, r, scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, live)
	imageFreeze(t, r)
	rows, err := manager.Freeze() // what agentimage.Freeze does next
	if err != nil {
		t.Fatal(err)
	}
	osPID := rows[0].OSPID
	t.Cleanup(func() { _ = syscall.Kill(-osPID, syscall.SIGKILL) })
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
	defer cancel()
	// Under a frozen table Close cannot kill or reap: it must refuse loudly
	// and change nothing (S2 internal review F4/V1/R3/P6).
	if err := r.Close(ctx); !errors.Is(err, workload.ErrFrozen) {
		manager.Thaw()
		t.Fatalf("Close under a frozen process table: %v, want workload.ErrFrozen", err)
	}
	time.Sleep(100 * time.Millisecond)
	if state := imageProcState(t, osPID); state == "" || state == "Z" {
		manager.Thaw()
		t.Fatalf("a refused Close still killed the child (state %q)", state)
	}
	if !r.Frozen() {
		manager.Thaw()
		t.Fatal("a refused Close lowered the fence")
	}
	// The rollback order (table first) makes Close a real teardown again:
	// the child is killed AND reaped, and the exit is delivered.
	manager.Thaw()
	if err := r.Close(ctx); err != nil {
		t.Fatalf("close after the table thawed: %v", err)
	}
	deadline := time.Now().Add(runtimeTestBudget)
	for len(manager.Snapshot()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("closed runtime left the table populated: %+v (child state %q)", manager.Snapshot(), imageProcState(t, osPID))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// origin: hot-update S2 internal review, fence lane — D11 truncates a
// stalled stream with Output.Discard, whose ReadError makes outputsEnded
// report the stream ENDED while its pump is still draining a live pipe.
// With both streams truncated, observe calls Manager.AllowReap every tick
// although neither EOF was seen, which releases the AdoptDraining hold: the
// leader is reaped as soon as it exits even while a descendant still holds
// the pipes, the task goes terminal, Forget drops the row, and a later
// Revoke (kill of the owned process group) can no longer reach that
// descendant — the subtree AdoptDraining exists to keep killable escapes.
func TestFenceDiscardingStreamsReleaseReapHoldWhileDescendantLives(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 64)
	manager := r.cfg.Manager
	scope := clockedScope("discardHold")
	var exited atomic.Int32
	r.cfg.Exited = func(string, int) { exited.Add(1) }
	marker := t.TempDir() + "/go"
	// The leader fills both logs (64 retained + 16 in each pipe), forks a
	// descendant that inherits both pipes, waits for the marker and exits.
	// With the AdoptDraining hold intact the task must stay running until
	// the descendant releases the pipes.
	req, owner := clockedCreate(t, r, scope, "held", []string{"/bin/sh", "-c",
		"head -c 80 /dev/zero; head -c 80 /dev/zero >&2; sleep 30 & while [ ! -e " + marker + " ]; do sleep 0.01; done; exit 0"})
	failureAwaitRunning(t, r, scope, req)
	rows := manager.Snapshot()
	if len(rows) != 1 || !rows[0].ReapHeld {
		t.Fatalf("fixture child is not reap-held: %+v", rows)
	}
	osPID := rows[0].OSPID
	t.Cleanup(func() { _ = syscall.Kill(-osPID, syscall.SIGKILL) })
	full := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return owner.output[0] != nil && owner.output[1] != nil &&
			owner.output[0].Snapshot().Replay.End == 64 && owner.output[1].Snapshot().Replay.End == 64
	}
	deadline := time.Now().Add(5 * time.Second)
	for !full() {
		if time.Now().After(deadline) {
			t.Fatal("logs never filled")
		}
		time.Sleep(time.Millisecond)
	}
	clock.Store(req.Handle.Lease.ExpiresBootNS + AbandonedRunningGrace.Nanoseconds() + int64(time.Second))
	res, err := r.Sweep()
	if err != nil || res.Truncated != 2 {
		t.Fatalf("sweep: %+v %v, want both streams truncated", res, err)
	}
	// The child is alive and its streams are being drained, not ended.
	time.Sleep(100 * time.Millisecond)
	rows = manager.Snapshot()
	if len(rows) != 1 || rows[0].Exit != nil {
		t.Fatalf("leader reaped while alive: %+v", rows)
	}
	if rows[0].ReapHeld {
		t.Logf("reap hold intact while both streams are discarding (good)")
	} else {
		t.Logf("reap hold RELEASED while both streams are discarding and the child is alive")
	}
	// Let the leader exit; the descendant (sleep 30) still holds both pipes.
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.done:
	case <-time.After(500 * time.Millisecond):
		// Correct behaviour: the leader is a held zombie until the
		// descendant releases the pipes. Prove the hold is what kept it.
		if rows = manager.Snapshot(); len(rows) != 1 || rows[0].Exit != nil || !rows[0].ReapHeld {
			t.Fatalf("leader not held as a zombie: %+v", rows)
		}
		if killed, err := r.Revoke(scope.SID, "session_destroyed"); err != nil || killed != 1 {
			t.Fatalf("revoke of the running task: %d %v", killed, err)
		}
		failureAwait(t, owner.done, "revoked task terminal")
		if err := syscall.Kill(-osPID, 0); err == nil {
			t.Fatal("revoke did not reach the descendant")
		}
		return
	}
	// Defect path: the task is terminal although a descendant still holds
	// both pipes. The group still has the descendant: kill(-pgid, 0) works.
	if err := syscall.Kill(-osPID, 0); err != nil {
		t.Fatalf("descendant gone already: %v", err)
	}
	if killed, err := r.Revoke(scope.SID, "session_destroyed"); err != nil {
		t.Fatalf("revoke: %d %v", killed, err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(-osPID, 0); err == nil {
		t.Fatalf("task went terminal (exited=%d) while a descendant holds its pipes, and Revoke could not reach that descendant: the discarding streams released the AdoptDraining hold", exited.Load())
	}
	t.Fatalf("task went terminal (exited=%d) while a descendant still held its pipes", exited.Load())
}

// origin: hot-update S2 internal review, fence lane — plan §1.5 rows
// "Handle probe / resume" × "quiescing" say the attach ops keep flowing
// while Freeze waits for a creation in flight (only create parks). The
// existing tests exercise a parked create and a resume AFTER the refused
// Freeze; this one issues a probe and a resume DURING the wait.
func TestFenceAttachOpsFlowWhileQuiescing(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	manager := &workload.Manager{}
	r, err := New(Config{Instance: "quiescingInstance", Manager: manager, ReplayCapacity: 64 << 10,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
			stdio, err := failureStartChild(manager, pid, req)
			return stdio, "", err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
		defer cancel()
		_ = r.Close(ctx)
	})
	scope := clockedScope("quiescingSession")
	inflight, _ := clockedCreate(t, r, scope, "inflight", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwait(t, entered, "startup owner entering Start")
	freezeDone := make(chan error, 1)
	go func() { freezeDone <- r.Freeze(time.Now().Add(400 * time.Millisecond)) }()
	time.Sleep(30 * time.Millisecond)
	r.mu.Lock()
	quiescing := r.quiescing
	r.mu.Unlock()
	if !quiescing {
		t.Fatal("Freeze is not quiescing")
	}
	type outcome struct {
		reply proto.ProcessResumeReply
		err   error
	}
	results := make(chan outcome, 2)
	probe := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 50, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}
	resume := inflight
	resume.CallID = 51
	for _, req := range []proto.ProcessResumeReq{probe, resume} {
		go func(req proto.ProcessResumeReq) {
			reply, err := r.Handle(scope, req, "failure-inbox")
			results <- outcome{reply, err}
		}(req)
	}
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("attach op under quiescing: %+v %v", got.reply, got.err)
			}
		case <-time.After(150 * time.Millisecond):
			t.Fatal("an attach op parked while Freeze was only quiescing")
		}
	}
	if err := <-freezeDone; !errors.Is(err, procrequest.ErrBusy) {
		t.Fatalf("freeze: %v", err)
	}
}
