//go:build linux

// Real replay descriptors and PTY children; image corruption and Take failures
// are injected. These are in-process cuts, not same-PID syscall.Exec receipts.
package execsession

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §5.1 / T19
func TestPTYImageClosedMasterAndTombstones(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("file-shapes", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		st := ptysession.Image{Version: 1, Input: workload.ReplayState{Version: 1, Capacity: upgradebudget.PTYInputBytes}, Output: workload.ReplayState{Version: 1, Capacity: workload.DefaultReplayCapacity, FIN: true}, MasterClosed: true, SourceEnded: true, InputEnded: true, InputError: "pty input aborted: hangup"}
		ti := TaskImage{PID: "closed-master", Kind: proto.ProcessKindPTY, PTY: &st, Files: map[string]FileIdentity{filePTYInput: {}, filePTYOutput: {}}}
		for _, terminal := range []bool{false, true} {
			names, budget, err := validatePTYImage(ti, terminal)
			if err != nil || !reflect.DeepEqual(names, []string{filePTYInput, filePTYOutput}) || budget != upgradebudget.PTYInputBytes+workload.DefaultReplayCapacity {
				t.Fatalf("closed master child terminal=%t: files=%v budget=%d err=%v", terminal, names, budget, err)
			}
		}
		ti.Files[fileStderrReplay] = FileIdentity{}
		if _, _, err := validatePTYImage(ti, true); err == nil {
			t.Fatal("PTY accepted a real stderr replay")
		}
		delete(ti.Files, fileStderrReplay)
		st.MasterClosed = false
		st.Identity.Version = 1
		st.Identity.Started = true
		st.Identity.BootID = "boot"
		st.Identity.LeaderPID = 123
		st.Identity.StartTimeTicks = 456
		st.SourceEnded = false
		st.InputEnded = false
		st.InputError = ""
		st.Output.FIN = false
		ti.Files[filePTYMaster] = FileIdentity{}
		names, _, err := validatePTYImage(ti, false)
		if err != nil || len(names) != 3 {
			t.Fatalf("live PTY file shape: %v %v", names, err)
		}
		if _, _, err := validatePTYImage(ti, true); err == nil {
			t.Fatal("terminal image retained live master")
		}
		res := taskResource(ti.Kind, ti.PID, nil)
		if res.Kind != "pty-task-v1" || res.ID != "pty-task.closed-master" {
			t.Fatalf("kind dispatched as exec: %+v", res)
		}
		ts := TombstoneImage{Kind: proto.ProcessKindPTY, PTY: &ptysession.Tombstone{InputACK: 3, InputAccepted: 7, InputEnded: true, InputError: "pty input aborted: write_error"}, FinalFIN: [2]bool{true, true}}
		if !validPTYTombstone(ts) {
			t.Fatal("A<Q abort tombstone refused")
		}
		if domains := RequiredDomains(Image{Tombstones: []TombstoneImage{ts}}); !reflect.DeepEqual(domains, []string{"exec-v1", "pty-v1"}) {
			t.Fatal("tombstone domain disappeared")
		}
		settled := Image{Registry: procrequest.State{Records: []procrequest.Record{{Kind: proto.ProcessKindPTY, Phase: "settled"}}}}
		if domains := RequiredDomains(settled); len(domains) != 2 {
			t.Fatal("settled record lost PTY domain")
		}
		ts.FinalEnd[1] = 1
		if validPTYTombstone(ts) {
			t.Fatal("tombstone admitted nonempty stderr")
		}
	})
	t.Run("real-closed-and-terminal", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		f := newPTYRuntime(t, 0)
		q, owner := ptyCreate(t, f, "hup-immune", []string{"/bin/sh", "-c", "trap '' HUP; stty -echo; printf ready; while :; do read line || :; done"})
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Stdout != nil && bytes.Contains(p.Stdout.Data, []byte("ready"))
		})
		if owner.logs[2] != nil || f.r.ReplayBudgetUsed() != upgradebudget.PTYInputBytes+workload.DefaultReplayCapacity {
			t.Fatal("merged PTY output allocated a real stderr replay")
		}
		hangup := q
		hangup.Op = "hangup"
		hangup.ControlSeq = 1
		reply, err := f.r.Handle(f.scope, hangup, "pty-inbox")
		requirePTYDependency(t, err)
		if reply.ControlACK != 1 {
			t.Fatal("hangup was not committed")
		}
		img, _, _ := ptyCut(t, f)
		if len(img.Tasks) != 1 || !img.Tasks[0].PTY.MasterClosed || len(img.Tasks[0].Files) != 2 {
			t.Fatal("closed master retained wrong resource set")
		}
		row, ok := f.manager.Lookup(owner.pid)
		if !ok || row.Exit != nil {
			t.Fatal("Hangup invented a terminal for HUP-immune child")
		}
		f.manager.Thaw()
		if err := f.r.Thaw(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
			t.Fatal(err)
		}
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
		f.clock.Add(int64(AbandonedTerminalGrace + time.Minute))
		if result, err := f.r.Sweep(); err != nil || result.Discarded != 1 {
			t.Fatalf("PTY terminal did not become a tombstone: %+v %v", result, err)
		}
		img, resources, _ := ptyCut(t, f)
		if len(resources) != 0 || len(img.Tombstones) != 1 || img.Tombstones[0].PTY == nil || img.BudgetUsed != 0 {
			t.Fatal("PTY tombstone retained descriptors or lost diagnostics")
		}
	})
	t.Run("second-task-salvage", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		exercisePTYImageFailure(t)
	})
}

func ptyCut(t *testing.T, f *ptyRuntimeFixture) (Image, []handoff.Resource, []workload.Record) {
	t.Helper()
	imageFreeze(t, f.r)
	if _, err := f.manager.Freeze(); err != nil {
		t.Fatal(err)
	}
	process, err := f.manager.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	img, resources, err := f.r.Export()
	if err != nil {
		t.Fatal(err)
	}
	return img, resources, process.Records
}

func ptyRoundTrip(t *testing.T, f *ptyRuntimeFixture) {
	t.Helper()
	img, resources, rows := ptyCut(t, f)
	source := newImageDupSource(t, resources)
	imported, salvage, err := Import(f.cfg, img, rows, source)
	requirePTYDependency(t, err)
	if len(salvage) != 0 {
		t.Fatal("successful import returned salvage")
	}
	if err := imported.PreActivate(); err != nil {
		t.Fatal(err)
	}
	if err := f.r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	f.manager.Thaw()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
}

type ptyFailTake struct {
	source           *imageDupSource
	failID, failName string
}

func (s *ptyFailTake) TakeFile(id, name string) (*os.File, error) {
	if id == s.failID && name == s.failName {
		return nil, errors.New("injected second-task Take failure")
	}
	return s.source.TakeFile(id, name)
}

// origin: hot-update S4 plan §4.2 / T34
func TestPTYRuntimeFiveRoundImportFailures(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exercisePTYImageFailure(t) // warm the real terminal poller before measuring
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePTYImageFailure(t)
		exercisePTYImageFailure(t, true)
		// Pin the former FX-A flake: a real Wait completes in partial activation
		// before the imported observer publishes its terminal. Revocation must
		// use that exit evidence rather than census a reaped leader.
		// origin: hot-update S4 plan §4.2 / T34; PRODUCT-R4 FX-A
		t.Run(fmt.Sprintf("waited_exit_%d", round), exercisePTYWaitedExitBeforeObserver)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY import failures", beforeG)
	testharness.AssertNoFDLeak(t, "PTY import failures", beforeFD, 0)
}

func exercisePTYImageFailure(t *testing.T, closePartial ...bool) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	requests := make([]proto.ProcessResumeReq, 0, 2)
	for i := 0; i < 2; i++ {
		q, _ := ptyCreate(t, f, fmt.Sprintf("image-%d", i), []string{"/bin/sh", "-c", "stty raw -echo; printf ready; while IFS= read -r line; do printf '%s' \"$line\"; done"})
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Stdout != nil && bytes.Equal(p.Stdout.Data, []byte("ready"))
		})
		requests = append(requests, q)
	}
	ptyExerciseFreezeRollback(t, f, requests)
	img, resources, rows := ptyCut(t, f)
	if len(img.Tasks) != 2 || len(resources) != 2 {
		t.Fatal("two-PTY import fixture is empty")
	}
	for _, stage := range []string{"take", "validate"} {
		source := newImageDupSource(t, resources)
		candidate := imageClone(t, img)
		var input FileSource = source
		if stage == "take" {
			input = &ptyFailTake{source: source, failID: resources[1].ID, failName: filePTYOutput}
		} else {
			// Same devpts inode is insufficient: CheckMaster must reject a wrong
			// PTY index without changing either original's flags or closing it.
			candidate.Tasks[1].PTY.Identity.PTYIndex++
		}
		imported, salvage, err := Import(f.cfg, candidate, rows, input)
		if err == nil || imported != nil {
			t.Fatalf("%s failure installed a partial runtime", stage)
		}
		if len(salvage) < 1 || salvage[0].Kind != ptyResourceKind || salvage[0].Files[filePTYMaster] == nil || salvage[0].Files[filePTYInput] == nil || salvage[0].Files[filePTYOutput] == nil {
			t.Fatalf("%s lost first task resources: %v", stage, err)
		}
		// Salvage + untaken Borrow is a complete re-dispatch, with each original
		// resource name and identity represented once, all descriptors still open.
		combined := make(map[string]map[string]*os.File)
		for _, res := range salvage {
			combined[res.ID] = res.Files
		}
		for id, files := range source.files {
			if combined[id] == nil {
				combined[id] = make(map[string]*os.File)
			}
			for name, file := range files {
				combined[id][name] = file
			}
		}
		for _, res := range resources {
			if len(combined[res.ID]) != len(res.Files) {
				t.Fatal("salvage and Borrow lost a descriptor")
			}
			for name, file := range combined[res.ID] {
				imageFileOpen(t, name, file)
				got, _ := fileIdentity(file)
				want, _ := fileIdentity(res.Files[name])
				if got != want {
					t.Fatal("salvage returned a different kernel object")
				}
			}
		}
		// Re-import the actual Salvage+Borrow set, not another duplicate of the
		// exporter. This proves the returned wrappers are usable for redispatch.
		recovered, remainder, recoverErr := Import(f.cfg, img, rows, &imageDupSource{files: combined})
		if recoverErr != nil || len(remainder) != 0 {
			t.Fatalf("salvage cannot be redispatched: %v", recoverErr)
		}
		returned, detachErr := recovered.Detach()
		if detachErr != nil {
			t.Fatal(detachErr)
		}
		for _, res := range returned {
			for _, file := range res.Files {
				_ = file.Close()
			}
		}
		source.Close()
	}
	source := newImageDupSource(t, resources)
	imported, salvage, err := Import(f.cfg, img, rows, source)
	requirePTYDependency(t, err)
	if len(salvage) != 0 {
		t.Fatal("valid import returned salvage")
	}
	if err := f.r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	f.manager.Thaw()
	// Inject a commit failure after the first real actor has activated. This
	// must require a fresh current-owner cut, never Detach of the initial cut.
	calls := 0
	f.r.activatePTY = func(actor *ptysession.Actor) error {
		calls++
		if calls == 2 {
			return errors.New("injected second actor activation failure")
		}
		return actor.Activate()
	}
	if err := f.r.Thaw(); err == nil || calls != 2 || !f.r.Active() {
		t.Fatalf("partial activation lost active boundary: calls=%d err=%v", calls, err)
	}
	if _, err := f.r.Detach(); !errors.Is(err, ErrNotFrozen) {
		t.Fatalf("partial activation detached initial image: %v", err)
	}
	f.r.activatePTY = nil
	if len(closePartial) != 0 && closePartial[0] {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := f.r.Close(ctx); err != nil {
			t.Fatal("partial activation could not retire its current owners", err)
		}
		if f.r.ReplayBudgetUsed() != 0 || len(f.manager.Snapshot()) != 0 || f.exits.Load() != 2 {
			t.Fatal("partial activation teardown lost terminal ownership or leaked resources")
		}
		return
	}
	ptyRoundTrip(t, f)
	for i, q := range requests {
		control := q
		control.Op = "resize"
		control.ControlSeq = 1
		control.Resize = &proto.PtyResizeEvent{Cols: 91 + i, Rows: 31 + i}
		if p, err := f.r.Handle(f.scope, control, "pty-inbox"); err != nil || p.ControlACK != 1 {
			t.Fatalf("control after failed import: %v", err)
		}
		input := q
		input.Op = "input"
		input.Input = []byte(fmt.Sprintf("roundtrip-%d\n", i))
		if _, err := f.r.Handle(f.scope, input, "pty-inbox"); err != nil {
			t.Fatal(err)
		}
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.InputACK == uint64(len(input.Input)) && p.Stdout != nil && bytes.Contains(p.Stdout.Data, bytes.TrimSuffix(input.Input, []byte("\n")))
		})
	}
	ptyRoundTrip(t, f)
	if _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); err != nil {
		t.Fatal(err)
	}
	for _, q := range requests {
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	}
	if f.exits.Load() != 2 {
		t.Fatalf("observers published %d exits for two original children", f.exits.Load())
	}
	f.close(t)
	if f.r.ReplayBudgetUsed() != 0 || len(f.manager.Snapshot()) != 0 {
		t.Fatal("five-round lifecycle retained a budget or process owner")
	}
}

// The second master's read-only observation fails after the first actor froze.
// Rollback must prepare and activate that first actor again, including when no
// image switch occurred. The descriptors and all I/O remain real.
// origin: hot-update S4 plan §4.1 / T34
type ptyObservationFault struct {
	pty.Master
	fail atomic.Bool
	err  error
}

func (m *ptyObservationFault) Observe() (pty.TerminalObservation, error) {
	if m.fail.Swap(false) {
		return pty.TerminalObservation{}, m.err
	}
	return m.Master.Observe()
}

func ptyExerciseFreezeRollback(t *testing.T, f *ptyRuntimeFixture, requests []proto.ProcessResumeReq) {
	t.Helper()
	img, _, rows := ptyCut(t, f)
	owner := f.r.tasks[img.Tasks[1].PID]
	st := *owner.frozen.pty
	files, err := owner.pty.Detach()
	if err != nil {
		t.Fatal(err)
	}
	var row workload.Record
	for _, candidate := range rows {
		if candidate.ID == owner.pid {
			row = candidate
		}
	}
	master, err := pty.RestoreMaster(files[filePTYMaster], st.Identity, row)
	if err != nil {
		t.Fatal(err)
	}
	input, err := workload.RestoreReplay(&f.r.budget, st.Input, files[filePTYInput])
	if err != nil {
		t.Fatal(err)
	}
	output, err := workload.RestoreReplay(&f.r.budget, st.Output, files[filePTYOutput])
	if err != nil {
		t.Fatal(err)
	}
	fault := &ptyObservationFault{Master: master, err: errors.New("injected second master observation failure")}
	actor, err := ptysession.Import(ptysession.Config{Manager: &f.manager, PID: owner.pid, Master: fault, Identity: st.Identity,
		Input: input, Output: output, Clock: f.cfg.Clock}, st)
	if err != nil {
		t.Fatal(err)
	}
	f.r.mu.Lock()
	owner.pty, owner.logs[0], owner.logs[1] = actor, input, output
	f.r.mu.Unlock()
	f.manager.Thaw()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	fault.fail.Store(true)
	if err := f.r.Freeze(time.Now().Add(time.Second)); !errors.Is(err, fault.err) || f.r.Frozen() {
		t.Fatalf("second actor freeze did not roll back the current owners: %v", err)
	}
	for _, request := range requests {
		if _, err := f.r.Handle(f.scope, request, "pty-inbox"); err != nil {
			t.Fatal("failed freeze stranded a previously frozen actor", err)
		}
	}
}
