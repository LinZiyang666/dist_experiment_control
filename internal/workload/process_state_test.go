//go:build linux

// Real: child processes, kernel wait ownership, process identity and pidfd lifetimes.
// Stubbed: application admission, stdio adapters, courier, and upgrade coordination.
// These tests cover only the process-table import/export contract.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

func TestProcessStateRejectsInvalidImportWithoutSideEffects(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	live := startProcessFixture(t, 31)
	member := startProcessFixtureInGroup(t, 43, live.pid)
	zombie := startProcessFixture(t, 37)
	zombie.finish(t)
	waitUntilKernelChildWaitable(t, zombie.pid)
	base := processStateEnvelope(t)
	base.Records = []workload.Record{
		{Identity: imageChildIdentity(t, base, live, "live")},
		{Identity: imageChildIdentity(t, base, zombie, "zombie"), ReapHeld: true},
	}
	memberIdentity := imageChildIdentity(t, base, member, "not-a-group-leader")
	for _, tc := range []struct {
		name string
		edit func(*workload.ProcessState)
	}{
		{"version", func(s *workload.ProcessState) { s.Version++ }},
		{"owner-pid", func(s *workload.ProcessState) { s.OwnerPID++ }},
		{"owner-start", func(s *workload.ProcessState) { s.OwnerStartTimeTicks++ }},
		{"owner-boot", func(s *workload.ProcessState) { s.BootID += "-wrong" }},
		{"empty-task", func(s *workload.ProcessState) { s.Records[0].ID = "" }},
		{"duplicate-task", func(s *workload.ProcessState) { s.Records[1].ID = s.Records[0].ID }},
		{"duplicate-live-pid", func(s *workload.ProcessState) {
			s.Records[1].Identity = s.Records[0].Identity
			s.Records[1].ID = "other"
		}},
		{"missing-start-time", func(s *workload.ProcessState) { s.Records[0].StartedAt = time.Time{} }},
		{"missing-child-ticks", func(s *workload.ProcessState) { s.Records[0].StartTimeTicks = 0 }},
		{"child-boot", func(s *workload.ProcessState) { s.Records[0].BootID += "-wrong" }},
		{"partial-valid-then-wrong-ticks", func(s *workload.ProcessState) { s.Records[1].StartTimeTicks++ }},
		{"non-child", func(s *workload.ProcessState) {
			s.Records[1].OSPID = os.Getpid()
			s.Records[1].StartTimeTicks = s.OwnerStartTimeTicks
		}},
		{"wrong-process-group", func(s *workload.ProcessState) {
			s.Records[1] = workload.Record{Identity: memberIdentity}
		}},
		{"terminal-held", func(s *workload.ProcessState) { s.Records[1].Exit = imageExit(37) }},
		{"terminal-code-disagrees", func(s *workload.ProcessState) {
			s.Records[1].ReapHeld = false
			s.Records[1].Exit = imageExit(37)
			s.Records[1].Exit.Code++
		}},
		{"terminal-signal-disagrees", func(s *workload.ProcessState) {
			s.Records[1].ReapHeld = false
			s.Records[1].Exit = imageExit(37)
			s.Records[1].Exit.Signal = int(syscall.SIGKILL)
		}},
		{"wait-error-cannot-claim-success", func(s *workload.ProcessState) {
			s.Records[1].ReapHeld = false
			s.Records[1].Exit = imageExit(0)
			s.Records[1].Exit.WaitError = "wait failed"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			state := copyProcessState(base)
			tc.edit(&state)
			var manager workload.Manager
			if err := manager.RestoreProcesses(state); err == nil {
				t.Fatal("invalid import succeeded")
			}
			if rows := manager.Snapshot(); len(rows) != 0 {
				t.Fatalf("failed import installed partial ownership: %+v", rows)
			}
			if err := manager.Signal("live", 0); !errors.Is(err, workload.ErrUnknown) {
				t.Fatalf("failed import changed receiver state: %v", err)
			}
			if err := live.process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("failed import damaged live child: %v", err)
			}
			waitUntilKernelChildWaitable(t, zombie.pid)
			// A failure must leave the receiver reusable, not secretly frozen.
			if err := manager.RestoreProcesses(processStateEnvelope(t)); err != nil {
				t.Fatalf("failed import poisoned receiver: %v", err)
			}
		})
	}
	state, err := zombie.process.Wait()
	zombie.done = true
	if err != nil || state.ExitCode() != 37 {
		t.Fatalf("invalid import consumed or changed the original status: state=%v err=%v", state, err)
	}
	live.finish(t)
	state, err = live.process.Wait()
	live.done = true
	if err != nil || state.ExitCode() != 31 {
		t.Fatalf("invalid import signalled or reaped the live child: state=%v err=%v", state, err)
	}
	member.finish(t)
	state, err = member.process.Wait()
	member.done = true
	if err != nil || state.ExitCode() != 43 {
		t.Fatalf("invalid group import damaged the unrelated child: state=%v err=%v", state, err)
	}
}

func TestProcessStateExportRequiresFrozenOriginalIdentity(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, tc := range []struct {
		name, boot string
		ticks      int64
	}{
		{name: "fill-missing"}, {name: "reject-known-boot", boot: "wrong-boot"},
		{name: "reject-known-ticks", ticks: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var manager workload.Manager
			child := startProcessFixture(t, 29)
			id := workload.Identity{ID: "exported", OSPID: child.pid, StartedAt: time.Now().UTC(), BootID: tc.boot, StartTimeTicks: tc.ticks}
			if err := manager.AdoptDraining(id, child.process); err != nil {
				t.Fatal(err)
			}
			child.owner, child.id = &manager, id.ID
			t.Cleanup(func() { manager.Thaw(); _ = manager.AllowReap(id.ID) })
			if _, err := manager.ExportProcesses(); err == nil {
				t.Fatal("export without a process barrier succeeded")
			}
			if _, err := manager.Freeze(); err != nil {
				t.Fatal(err)
			}
			state, err := manager.ExportProcesses()
			if tc.name != "fill-missing" {
				if err == nil {
					t.Fatal("export replaced a conflicting identity with the current process")
				}
				return
			}
			if err != nil || len(state.Records) != 1 || state.Records[0].BootID == "" || state.Records[0].StartTimeTicks <= 0 || !state.Records[0].ReapHeld {
				t.Fatalf("incomplete original identity export: %+v %v", state, err)
			}
			state.Records[0].ID, state.Records[0].ReapHeld = "changed", false
			again, err := manager.ExportProcesses()
			if err != nil || again.Records[0].ID != id.ID || !again.Records[0].ReapHeld {
				t.Fatalf("export aliases ownership: %+v %v", again, err)
			}
		})
	}
}

func TestProcessStateTerminalRecordsNeverOwnReusedPID(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	child := startProcessFixture(t, 41)
	state := processStateEnvelope(t)
	// These are historical journal fixtures. Their OS PID now belongs to a
	// different live child, so importing their receipts must not open a waiter.
	state.Records = []workload.Record{
		{Identity: workload.Identity{ID: "old-exit", OSPID: child.pid, StartedAt: time.Now().UTC()}, Exit: imageExit(23)},
		{Identity: workload.Identity{ID: "old-wait-error", OSPID: child.pid, StartedAt: time.Now().UTC()}, Exit: &workload.Exit{Code: -1, EndedAt: time.Now().UTC(), WaitError: "retained ECHILD"}},
	}
	want := *state.Records[0].Exit
	var manager workload.Manager
	if err := manager.RestoreProcesses(state); err != nil {
		t.Fatal(err)
	}
	state.Records[0].Exit.Code = 99
	state.Records[1].Exit.WaitError = "changed"
	snapshot := manager.Snapshot()
	snapshot[0].Exit.Code = 88
	got, err := manager.Wait("old-exit") // terminal receipts work while frozen
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("terminal state aliases import/snapshot: %+v %v", got, err)
	}
	got, err = manager.Wait("old-wait-error")
	if err == nil || err.Error() != "retained ECHILD" || got.Code != -1 {
		t.Fatalf("terminal wait error was changed or lost: %+v %v", got, err)
	}
	exported, err := manager.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	exported.Records[0].Exit.Code = 77
	got, err = manager.Wait("old-exit")
	if err != nil || got.Code != 23 {
		t.Fatalf("export aliases retained terminal result: %+v %v", got, err)
	}
	manager.Thaw()
	if err := manager.Signal("old-exit", 0); !errors.Is(err, workload.ErrExited) {
		t.Fatalf("terminal receipt gained signal authority over reused PID: %v", err)
	}
	if err := manager.SignalAll(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	child.finish(t)
	status, err := child.process.Wait()
	child.done = true
	if err != nil || status.ExitCode() != 41 {
		t.Fatalf("terminal restore reaped/signalled an unrelated child: %v %v", status, err)
	}
}

func TestProcessStateThawAndAdoptKeepOneWaitOwner(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exerciseProcessRestore(t, "concurrent-restore")
}

func TestProcessStateRepeatedRestoreReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exerciseProcessRestore(t, "warmup")
	baselineG := runtime.NumGoroutine()
	baselineFD := testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseProcessRestore(t, fmt.Sprintf("restore-round-%d", i))
	}
	testharness.AssertNoGoroutineLeak(t, "process state restore", baselineG)
	testharness.AssertNoFDLeak(t, "process state restore", baselineFD, 0)
}

func exerciseProcessRestore(t *testing.T, id string) {
	t.Helper()
	child := startProcessFixture(t, 29)
	other := startProcessFixture(t, 17)
	unowned := startProcessFixture(t, 43)
	state := processStateEnvelope(t)
	state.Records = []workload.Record{{Identity: imageChildIdentity(t, state, child, id)}}
	// A terminal row may share its numeric PID with a later live incarnation.
	state.Records = append(state.Records, workload.Record{Identity: workload.Identity{
		ID: id + "-historic", OSPID: child.pid, StartedAt: time.Now().UTC(),
	}, Exit: imageExit(13)})
	var manager workload.Manager
	if err := manager.RestoreProcesses(state); err != nil {
		t.Fatal(err)
	}
	child.owner, child.id = &manager, id
	_ = child.process.Release() // import owns a fresh handle; the fixture's is obsolete
	if err := manager.RestoreProcesses(state); err == nil {
		t.Fatal("second import into an installed manager succeeded")
	}
	if err := manager.Signal(id, 0); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("import activated before Thaw: %v", err)
	}
	if err := manager.Adopt(other.identity(id+"-early"), other.process); !errors.Is(err, workload.ErrFrozen) {
		t.Fatalf("import accepted new child before Thaw: %v", err)
	}
	unowned.finish(t)
	waitUntilKernelChildWaitable(t, unowned.pid)
	child.finish(t)
	waitUntilKernelChildWaitable(t, child.pid)
	var thaw sync.WaitGroup
	for i := 0; i < 8; i++ {
		thaw.Add(1)
		go func() { defer thaw.Done(); manager.Thaw() }()
	}
	manager.Thaw()
	other.adopt(t, &manager, id+"-later")
	other.finish(t)
	thaw.Wait()
	for _, fixture := range []*processFixture{child, other} {
		want := 29
		if fixture == other {
			want = 17
		}
		first := receiveExit(t, observeExit(&manager, fixture.id))
		second := receiveExit(t, observeExit(&manager, fixture.id))
		if first.err != nil || first.exit.Code != want || !reflect.DeepEqual(first, second) {
			t.Fatalf("multiple/competing wait owners for %s: first=%+v second=%+v", fixture.id, first, second)
		}
		fixture.done = true
		assertKernelChildReaped(t, fixture.pid)
		if err := manager.Forget(fixture.id); err != nil {
			t.Fatal(err)
		}
	}
	status, err := unowned.process.Wait()
	unowned.done = true
	if err != nil || status.ExitCode() != 43 {
		t.Fatalf("restored manager consumed a non-owned child: %v %v", status, err)
	}
	if err := manager.Forget(id + "-historic"); err != nil {
		t.Fatal(err)
	}
}

func processStateEnvelope(t *testing.T) workload.ProcessState {
	t.Helper()
	var empty workload.Manager
	if _, err := empty.Freeze(); err != nil {
		t.Fatal(err)
	}
	state, err := empty.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func imageChildIdentity(t *testing.T, state workload.ProcessState, child *processFixture, id string) workload.Identity {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", child.pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
	if len(fields) < 20 {
		t.Fatalf("short child stat: %q", data)
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return workload.Identity{ID: id, OSPID: child.pid, BootID: state.BootID,
		StartTimeTicks: ticks, StartedAt: time.Now().UTC()}
}

func imageExit(code int) *workload.Exit {
	return &workload.Exit{Code: code, Status: uint32(code << 8), EndedAt: time.Now().UTC()}
}

func copyProcessState(state workload.ProcessState) workload.ProcessState {
	state.Records = append([]workload.Record(nil), state.Records...)
	for i := range state.Records {
		if state.Records[i].Exit != nil {
			exit := *state.Records[i].Exit
			state.Records[i].Exit = &exit
		}
	}
	return state
}

// origin: hot-update S2 plan §4 item 5.
// Discard is the Manager half of an import that succeeded and then had to be
// abandoned (re-dispatch to the previous image): handles go, nothing is
// reaped or signalled, and the child is still exactly where the kernel left
// it for whoever restores next. Any table that ever polled, or that was
// never restored, is refused unchanged — those own wait statuses of their
// own and dropping them would strand zombies.
func TestManagerDiscardReleasesRestoredHandlesWithoutReaping(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	child := startProcessFixture(t, 31)
	state := processStateEnvelope(t)
	state.Records = []workload.Record{{Identity: imageChildIdentity(t, state, child, "discard-me")}}
	baselineFD := testharness.FDCount()
	var manager workload.Manager
	if err := manager.RestoreProcesses(state); err != nil {
		t.Fatal(err)
	}
	if testharness.FDCount() <= baselineFD {
		t.Fatal("fixture assumption broken: RestoreProcesses did not open a process handle")
	}
	if !manager.Frozen() {
		t.Fatal("restored manager reports not frozen")
	}
	if err := manager.Discard(); err != nil {
		t.Fatal(err)
	}
	if got := testharness.FDCount(); got != baselineFD {
		t.Fatalf("discard leaked handles: fd=%d baseline=%d", got, baselineFD)
	}
	if rows := manager.Snapshot(); len(rows) != 0 || manager.Frozen() {
		t.Fatalf("discard left state behind: rows=%d frozen=%t", len(rows), manager.Frozen())
	}
	if err := syscall.Kill(child.pid, 0); err != nil {
		t.Fatalf("discard touched the child: %v", err)
	}
	// The manager is reusable and the child is still ours to wait for.
	if err := manager.RestoreProcesses(state); err != nil {
		t.Fatalf("restore after discard: %v", err)
	}
	child.owner, child.id = &manager, "discard-me"
	_ = child.process.Release()
	child.finish(t)
	manager.Thaw()
	result := receiveExit(t, observeExit(&manager, "discard-me"))
	if result.err != nil || result.exit.Code != 31 {
		t.Fatalf("child result after discard+restore: %+v", result)
	}
	child.done = true

	// Refusals: a thawed (polling) table, a plain frozen table, an unused one.
	if err := manager.Discard(); err == nil {
		t.Fatal("discard accepted a thawed table")
	}
	var plain workload.Manager
	other := startProcessFixture(t, 17)
	other.adopt(t, &plain, "plain")
	if _, err := plain.Freeze(); err != nil {
		t.Fatal(err)
	}
	if err := plain.Discard(); err == nil {
		t.Fatal("discard accepted a table that owns a wait status")
	}
	if rows := plain.Snapshot(); len(rows) != 1 {
		t.Fatalf("refused discard changed the table: %d rows", len(rows))
	}
	plain.Thaw()
	other.finish(t)
	if result := receiveExit(t, observeExit(&plain, "plain")); result.err != nil || result.exit.Code != 17 {
		t.Fatalf("refused discard broke the wait owner: %+v", result)
	}
	other.done = true
	var unused workload.Manager
	if err := unused.Discard(); err == nil {
		t.Fatal("discard accepted an unused table")
	}
}
