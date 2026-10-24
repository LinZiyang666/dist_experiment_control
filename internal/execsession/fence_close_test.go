//go:build linux

// fence_close_test.go — Runtime.Close on a frozen runtime that was never
// exported: the teardown arm the plan's §1.5 table calls "thaw, then tear
// down as usual". Real children, real pipes, real memfd replays.
// origin: hot-update S2 internal review, fence lane (verifier).
package execsession

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S2 internal review, fence lane (verifier) — Close on
// a frozen, unexported runtime whose process table is NOT frozen. S0's Close
// contract (observe's closed branch: SIGKILL, closeTask, AllowReap, finish →
// Exited delivered, row forgotten) is what plan §1.5 promises for this cell
// ("thawLocked 后 teardown"). Under the fence observe leaves through the
// frozen+closed gate meant for a Discarded runtime, so nobody ever calls
// AllowReap or finish: the child is killed by Close's own Signal but stays a
// ReapHeld zombie in the table, Exited is never delivered, and the table's
// reap loop ticks for ever. The existing fence test's "Close under the fence
// releases every waiter" epilogue leaked exactly this shape before the fix
// (Close now thaws locally first — runtime.go Close); the test asserts the
// S0 contract and is named for the invariant it keeps, not the defect.
func TestFenceCloseOnFrozenUnexportedRuntimeKeepsExitAccounting(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 256)
	manager := r.cfg.Manager
	var exited atomic.Int32
	r.cfg.Exited = func(string, int) { exited.Add(1) }
	scope := clockedScope("closeFrozenUnexported")
	live, owner := clockedCreate(t, r, scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, live)
	rows := manager.Snapshot()
	if len(rows) != 1 || !rows[0].ReapHeld {
		t.Fatalf("fixture child is not reap-held: %+v", rows)
	}
	osPID := rows[0].OSPID
	t.Cleanup(func() { _ = syscall.Kill(-osPID, syscall.SIGKILL) })
	imageFreeze(t, r) // the table stays live: agentimage has not frozen it (or already thawed it)
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	failureAwait(t, owner.done, "closed task published")
	// Give the reaper and a would-be finish every chance: S0's closed branch
	// reaches finish within a few 10 ms ticks.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(manager.Snapshot()) == 0 && exited.Load() == 1 {
			return // the plan's teardown: reaped, accounted, forgotten
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows = manager.Snapshot()
	state := imageProcState(t, osPID)
	t.Fatalf("Close on a frozen unexported runtime: exited=%d rows=%+v child state=%q — "+
		"the child was killed but never reaped (ReapHeld hold never released), no Exited was delivered "+
		"and the table's reap loop keeps polling the zombie", exited.Load(), rows, state)
}
