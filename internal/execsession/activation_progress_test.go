//go:build linux

package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 internal review rv-fd rv-fd-2 (independent verifier)
func TestActivationRetryResumesOnlyUnfinishedActors(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseActivationRetryResumesOnlyUnfinishedActors(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestActivationRetryResumesOnlyUnfinishedActors", beforeG)
	testharness.AssertNoFDLeak(t, "TestActivationRetryResumesOnlyUnfinishedActors", beforeFD, 0)
}

func exerciseActivationRetryResumesOnlyUnfinishedActors(t *testing.T) {
	f := newPTYRuntime(t, 0)
	for _, id := range []string{"first", "second"} {
		ptyCreate(t, f, id, []string{"/bin/sh", "-c", "stty -echo; printf ready; exec sleep 30"})
	}
	if err := f.r.Freeze(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	fault := errors.New("transient second activation")
	f.r.cfg.PTYActivate = func(_ string, a *ptysession.Actor) error {
		calls++
		if calls == 2 {
			return fault
		}
		return a.Activate()
	}
	if err := f.r.Thaw(); !errors.Is(err, fault) {
		t.Fatalf("initial activation: %v", err)
	}
	if err := f.r.Thaw(); err != nil {
		t.Errorf("retry rejected completed actor: %v; first activation calls=%d", err, calls)
	}
	if calls != 3 {
		t.Fatalf("retry revisited a completed owner: activation calls=%d", calls)
	}
	f.r.mu.Lock()
	actors := f.r.sortedTasks()
	f.r.mu.Unlock()
	// A legal actor-level retry also bypasses the clock and preserves receipts.
	// Runtime progress must still skip its hook, rather than depending on this.
	f.clockFault.Store(true)
	for _, owner := range actors {
		before := owner.pty.InputStatus()
		if err := owner.pty.PreActivate(); err != nil {
			t.Fatal(err)
		}
		if err := owner.pty.Activate(); err != nil {
			t.Fatal(err)
		}
		if after := owner.pty.InputStatus(); before != after {
			t.Fatal("idempotent retry changed input receipts")
		}
	}
	f.clockFault.Store(false)
}

// origin: hot-update S4 internal review rv-fd rv-fd-3
func TestFreezeDeadlineIncludesRuntimeOwnershipSnapshot(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newPTYRuntime(t, 0)
			ptyCreate(t, f, "snapshot", []string{"/bin/sh", "-c", "exec sleep 30"})
			f.r.mu.Lock()
			done := make(chan error, 1)
			go func() {
				deadline := time.Now().Add(30 * time.Millisecond)
				_, err := TaskPIDsUntil(f.r, deadline)
				done <- errors.Join(err, CheckLiveUntil(f.r, deadline))
			}()
			late := false
			var err error
			select {
			case err = <-done:
			case <-time.After(300 * time.Millisecond):
				late = true
			}
			f.r.mu.Unlock()
			if late {
				err = <-done
			}
			if err == nil || late {
				t.Fatalf("ownership snapshot missed deadline: late=%t err=%v", late, err)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "runtime ownership precheck", beforeG)
	testharness.AssertNoFDLeak(t, "runtime ownership precheck", beforeFD, 0)
}

// origin: hot-update S4 internal review rv-fd rv-fd-3
func TestFreezeRollbackBoundsRuntimeAccountingLock(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseFreezeRollbackAccountingLock(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "runtime rollback lock", beforeG)
	testharness.AssertNoFDLeak(t, "runtime rollback lock", beforeFD, 0)
}

// Position the real freeze transaction after registry admission, while owning
// the accounting lock. Polling Freeze's transient freezing flag missed the
// entire 80 ms transaction when the observer was descheduled under gate load.
// The contention and 80/300 ms deadlines below are unchanged; only the setup
// interleaving is now guaranteed before the deadline starts.
// origin: hot-update S6 plan §11 / TESTFIX-2
func exerciseFreezeRollbackAccountingLock(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	f := newPTYRuntime(t, 0)
	q, _ := ptyCreate(t, f, "recovery-lock", []string{"/bin/sh", "-c", "exec sleep 30"})
	// Creation is asynchronous. Use the former one-second admission-observation
	// budget for its durable running state before starting the 80 ms cut.
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		rec, err := f.r.registry.Lookup(f.scope, *q.Handle)
		return err == nil && rec.Phase == "running"
	}) {
		t.Fatal("PTY creation never reached running before the rollback exercise")
	}
	f.r.mu.Lock()
	state, err := f.r.registry.Freeze()
	if err != nil {
		f.r.mu.Unlock()
		t.Fatal(err)
	}
	f.r.freezing, f.r.quiescing = true, true
	f.r.activation.recovery = make(chan struct{})
	tasks := f.r.sortedTasks()
	f.r.ioActive++
	done := make(chan error, 1)
	go func() { done <- f.r.freezeTasks(time.Now().Add(80*time.Millisecond), state, f.clock.Load(), tasks) }()
	// Contention covers the drain and rollback publication. Returning within
	// the deadline must retain an acknowledged owner until this lock is released.
	late := false
	select {
	case err = <-done:
	case <-time.After(300 * time.Millisecond):
		late = true
	}
	f.r.ioActive--
	f.r.mu.Unlock()
	if late {
		err = <-done
	}
	if !errors.Is(err, ErrRollbackPending) || late {
		t.Fatalf("rollback exceeded deadline or lost recovery ownership: late=%t err=%v", late, err)
	}
	if err := awaitFreezeRecovery(f.r, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Freeze(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S4 internal review rv-fd rv-fd-V1
func TestFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor", beforeG)
	testharness.AssertNoFDLeak(t, "TestFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor", beforeFD, 0)
}

func exerciseFailedFreezeRollbackDoesNotOpenAdmissionWithFrozenActor(t *testing.T) {
	f := newPTYRuntime(t, 0)
	var armed atomic.Bool
	var samples atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	fault := errors.New("rollback boot clock unavailable")
	f.r.now = func() (int64, error) {
		if armed.Load() {
			switch samples.Add(1) {
			case 1:
				close(entered)
				<-release
			case 2:
			default:
				return 0, fault
			}
		}
		return f.clock.Load(), nil
	}
	for _, id := range []string{"first", "second"} {
		q, _ := ptyCreate(t, f, id, []string{"/bin/sh", "-c", "stty -echo; printf ready; exec sleep 30"})
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Stdout != nil && string(p.Stdout.Data) == "ready" })
	}
	f.r.mu.Lock()
	tasks := f.r.sortedTasks()
	f.r.mu.Unlock()
	first, second := tasks[0].pty, tasks[1].pty
	// Close the real registry/observer admission before taking the Manager
	// transaction. Otherwise an observer can pin ioActive in Manager.Lookup,
	// legitimately deferring Freeze before either actor is touched. This unit
	// positions the production freezeTasks rollback, not that earlier refusal.
	f.r.mu.Lock()
	state, err := f.r.registry.Freeze()
	if err != nil {
		f.r.mu.Unlock()
		t.Fatal(err)
	}
	f.r.freezing, f.r.quiescing = true, true
	f.r.activation.recovery = make(chan struct{})
	f.r.mu.Unlock()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.ioActive == 0
	}) {
		t.Fatal("observers did not drain before control")
	}
	armed.Store(true)
	controlDone := make(chan error, 1)
	go func() {
		_, err := second.ApplyControl(ptysession.ControlRequest{Seq: 1, Op: "keepalive"})
		controlDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("control never owned token")
	}
	now, clockErr := f.r.now()
	if clockErr != nil {
		t.Fatal(clockErr)
	}
	err = f.r.freezeTasks(time.Now().Add(500*time.Millisecond), state, now, tasks)
	close(release)
	if cerr := <-controlDone; cerr != nil {
		t.Fatal(cerr)
	}
	if err == nil {
		t.Fatal("freeze did not encounter blocked second actor")
	}
	if err := awaitFreezeRecovery(f.r, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if samples.Load() < 3 {
		t.Fatal("rollback activation clock fault was not exercised")
	}
	if !errors.Is(err, ErrRollbackPending) {
		t.Fatalf("rollback responsibility was hidden: %v", err)
	}
	armed.Store(false)
	_, inputErr := first.AcceptInput(0, []byte("x"), false)
	f.r.mu.Lock()
	admitted := !f.r.frozen && !f.r.quiescing && !f.r.freezing
	f.r.mu.Unlock()
	t.Logf("freeze error=%v; clock samples=%d; admissionOpen=%t; first actor input=%v", err, samples.Load(), admitted, inputErr)
	if admitted || !errors.Is(inputErr, workload.ErrFrozen) {
		t.Fatal("rollback discarded activation error or reopened admission with frozen owner")
	}
	if err := f.r.Thaw(); err != nil {
		t.Fatalf("retry lost rollback owner: %v", err)
	}
	if _, err := first.AcceptInput(0, []byte("x"), false); err != nil {
		t.Fatalf("recovered actor still frozen: %v", err)
	}
	f.close(t)
}

// origin: hot-update S4 CLOSE thaw failure / plan §4.2
func TestExecThawFailureRetainsPartialActivationResponsibility(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseExecThawFailure(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "exec thaw failure x5", beforeG)
	testharness.AssertNoFDLeak(t, "exec thaw failure x5", beforeFD, 0)
}

func exerciseExecThawFailure(t *testing.T) {
	f := newRuntimeFixture(t, 256)
	for _, id := range []string{"first", "second"} {
		q, _ := clockedCreate(t, f.r, f.scope, id, []string{"/bin/sh", "-c", "printf ready; exec sleep 30"})
		failureAwaitRunning(t, f.r, f.scope, q)
	}
	if err := f.r.Freeze(time.Now().Add(runtimeTestBudget)); err != nil {
		t.Fatal(err)
	}
	owners := f.r.sortedTasks()
	beforeBudget := f.r.ReplayBudgetUsed()
	calls := make(map[*task]int)
	fault := errors.New("pipe thaw failed after partial progress")
	f.r.thawExec = func(t *task) error {
		calls[t]++
		if err := thawTask(t); err != nil {
			return err
		}
		if t == owners[1] && calls[t] == 1 {
			return fault
		}
		return nil
	}
	if err := f.r.Thaw(); !errors.Is(err, fault) {
		t.Fatalf("pipe thaw failure was hidden: %v", err)
	}
	f.r.mu.Lock()
	closedAdmission := f.r.partialActive && f.r.quiescing && !f.r.frozen && !f.r.activating
	completed := f.r.activation.completed[owners[0]] && !f.r.activation.completed[owners[1]]
	f.r.mu.Unlock()
	if !closedAdmission || !completed || f.r.ReplayBudgetUsed() != beforeBudget || len(f.r.TaskPIDs()) != 2 {
		t.Fatal("partial thaw reopened admission, marked a failed owner complete or released resources")
	}
	// A real probe must wait while those owners are retained, then receive a
	// capability from the same runtime after retry restores the unfinished one.
	done := make(chan error, 1)
	go func() {
		_, err := f.r.Handle(f.scope, proto.ProcessResumeReq{Version: 1, CallID: 9, Op: "probe",
			ActorFP: f.scope.ActorFP, ClientLineage: f.scope.ClientLineage}, "probe-inbox")
		done <- err
	}()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.activation.changed != nil
	}) {
		t.Fatal("partial activation admitted a probe before recovery")
	}
	select {
	case err := <-done:
		t.Fatalf("partial activation admitted a waiter: %v", err)
	default:
	}
	if err := f.r.Thaw(); err != nil {
		t.Fatalf("retry lost the partial activation owner: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls[owners[0]] != 1 || calls[owners[1]] != 2 || f.r.ReplayBudgetUsed() != beforeBudget {
		t.Fatal("retry revisited completed streams or changed retained accounting")
	}
	f.r.thawExec = nil
	f.close(t)
}
