//go:build linux

package agentimage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §4.2 / T23 — isolate the composition boundary
// from P6's actor preparation: after preparation has succeeded, a real runtime
// refusal still follows a thawed Manager and must never yield pristine salvage.
func TestImportedThawFailureCommitsResourceResponsibility(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newBundleFixture(t, fmt.Sprintf("activation-boundary-%d", round))
			ctx, cancel := context.WithTimeout(t.Context(), bundleTestBudget)
			defer cancel()
			if err := f.rt.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.manager.Freeze(); err != nil {
				t.Fatal(err)
			}
			imp := &Imported{Runtime: f.rt, Manager: f.manager, prepared: true}
			if err := imp.Thaw(); !errors.Is(err, execsession.ErrClosed) {
				t.Fatalf("real runtime failure not reached: %v", err)
			}
			if !imp.Active() || f.manager.Frozen() || imp.FreezeWindow() != 0 {
				t.Fatal("failed thaw did not retain active resource responsibility")
			}
			if salvage, err := imp.Detach(); err == nil || salvage != nil {
				t.Fatal("failed active thaw returned pristine salvage")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "imported-active-boundary", beforeG)
	testharness.AssertNoFDLeak(t, "imported-active-boundary", beforeFD, 0)
}

// origin: hot-update S4 plan §5.1 — this checks the composition bound only;
// each domain's file-shape validation belongs to execsession.Import.
func TestBundleDescriptorBoundCountsActualFiles(t *testing.T) {
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	for _, names := range [][]string{
		{"master", "input-replay", "output-replay"},
		{"input-replay", "output-replay"},
		{"stdin-replay", "stdin-target", "stdout-replay", "stdout-source", "stderr-replay", "stderr-source"},
	} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			b := Bundle{Version: BundleVersion, Instance: "file-bound", Lineage: Lineage{Generation: 1, BootID: boot, ProtoVersion: proto.ProtoVersion},
				Runtime:   execsession.Image{Version: execsession.ImageVersion, Instance: "file-bound", Registry: procrequest.State{BootID: boot}},
				Processes: workload.ProcessState{BootID: boot}, Courier: CourierState{Version: CourierStateVersion}}
			for files := 0; files < maxHandoffFiles; {
				ti := execsession.TaskImage{PID: fmt.Sprintf("task-%d", len(b.Runtime.Tasks)), Files: map[string]execsession.FileIdentity{}}
				for _, name := range names {
					if files == maxHandoffFiles {
						break
					}
					files++
					ti.Files[name] = execsession.FileIdentity{Dev: 1, Ino: uint64(files)}
				}
				b.Runtime.Tasks = append(b.Runtime.Tasks, ti)
			}
			if err := b.Validate(); err != nil {
				t.Fatalf("exact file bound refused: %v", err)
			}
			b.Runtime.Tasks = append(b.Runtime.Tasks, execsession.TaskImage{PID: "one-more", Files: map[string]execsession.FileIdentity{"output-replay": {Dev: 1, Ino: maxHandoffFiles + 1}}})
			var deferred *DeferredError
			if err := b.Validate(); !errors.As(err, &deferred) || deferred.Reason != ReasonImageTooLarge {
				t.Fatalf("one file beyond bound: %v", err)
			}
		})
	}
}

// origin: hot-update S4 plan §2.3 / §5.2 — exercise the final packing step
// directly so its callback can be checked independently of Manager.FreezeUntil.
// The runtime is real; a late revocation must be observed before any duplicate.
func TestPrepareCheckedSeesFinalExportBeforeDuplicating(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "checked-instance")
	if err := f.rt.Freeze(time.Now().Add(bundleTestBudget)); err != nil {
		t.Fatal(err)
	}
	image, resources, err := f.rt.Export()
	if err != nil {
		t.Fatal(err)
	}
	boot := image.Registry.BootID
	cut := &Frozen{owners: Owners{Runtime: f.rt, Manager: f.manager}, resources: resources, frozenAtBootNS: image.FrozenAtBootNS,
		bundle: Bundle{Version: BundleVersion, Instance: image.Instance, Runtime: image,
			Lineage:   Lineage{Generation: 1, BootID: boot, ProtoVersion: proto.ProtoVersion},
			Processes: workload.ProcessState{BootID: boot}, Courier: CourierState{Version: CourierStateVersion}}}
	t.Cleanup(func() { _ = cut.Thaw() })
	if _, err := f.rt.Revoke(f.scope.SID, "session_destroyed"); err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("reader lacks the final domain")
	calls := 0
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for range 5 {
		prepared, err := cut.PrepareChecked(func(b Bundle) error {
			calls++
			if len(b.Runtime.Revocations) != 1 || b.Runtime.Revocations[0].SID != f.scope.SID {
				t.Fatal("checked a stale export")
			}
			// The one new descriptor holds public calibration metadata; no
			// business descriptor may be duplicated before the reader check.
			// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
			if testharness.FDCount() != beforeFD+1 {
				t.Fatal("unexpected descriptors before compatibility check")
			}
			return refusal
		})
		if prepared != nil || !errors.Is(err, refusal) {
			t.Fatalf("compatibility refusal: prepared=%v err=%v", prepared != nil, err)
		}
	}
	if calls != 5 || !f.rt.Frozen() {
		t.Fatal("refusal consumed the cut or skipped the callback")
	}
	testharness.AssertNoGoroutineLeak(t, "checked-refusal", beforeG)
	testharness.AssertNoFDLeak(t, "checked-refusal", beforeFD, 0)
	prepared, err := cut.PrepareChecked(func(Bundle) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

// The JSON view deliberately asserts the frozen wire shape at the composition
// boundary without duplicating the runtime owner's internal validation logic.
func ptyTaskState(t *testing.T, task execsession.TaskImage) ptysession.Image {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Kind string            `json:"kind"`
		PTY  *ptysession.Image `json:"pty"`
	}
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	if view.Kind != proto.ProcessKindPTY || view.PTY == nil {
		t.Fatal("runtime did not export the PTY domain")
	}
	return *view.PTY
}

func newPTYBundleConfig(t *testing.T, instance string) *bundleFixture {
	t.Helper()
	f := newBundleConfig(t, instance)
	f.cfg.StartPTY = func(pid string, req *proto.RunReq) (*pty.Session, string, error) {
		session, err := pty.Allocate(req.Cols, req.Rows)
		if err != nil {
			return nil, "", err
		}
		if err := session.Start(req.Argv, ptyBundleEnvironment("", ""), req.Cwd, ""); err != nil {
			_ = session.Close()
			return nil, "", err
		}
		boot, err := procrequest.CurrentBootID()
		if err != nil {
			session.KillAndWaitAfterAbandonedStart()
			_ = session.Close()
			return nil, "", err
		}
		id := workload.Identity{ID: pid, OSPID: session.OSPID(), StartedAt: time.Now().UTC(),
			BootID: boot, StartTimeTicks: bundleChainStartTicks(t, session.OSPID())}
		if err := session.AdoptDraining(f.manager, id); err != nil {
			_ = syscall.Kill(-id.OSPID, syscall.SIGKILL)
			_, _ = session.Wait()
			_ = session.Close()
			return nil, "", err
		}
		f.mu.Lock()
		f.spawned++
		f.mu.Unlock()
		return session, "", nil
	}
	f.cfg.PTYStarted = func(string, proto.RunReq, workload.Identity) {}
	return f
}

func createBundlePTY(t *testing.T, f *bundleFixture, request, script string) (proto.ProcessHandle, string) {
	t.Helper()
	probe, err := f.handle(proto.ProcessResumeReq{Op: "probe", Kind: proto.ProcessKindPTY, ClientLineage: f.scope.ClientLineage})
	if err != nil || probe.Lease == nil || probe.PTYVersion != proto.PTYVersion {
		t.Fatalf("PTY capability missing (requires P6): version=%d err=%v", probe.PTYVersion, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: request}
	spec := proto.RunReq{Argv: []string{"/bin/sh", "-c", script}, Cols: 80, Rows: 24, KAIntervalMS: 1000}
	reply, err := f.handle(proto.ProcessResumeReq{Op: "create", Kind: proto.ProcessKindPTY, Handle: &h, AttachEpoch: 1, Run: &spec})
	if err != nil || reply.PID == "" {
		t.Fatalf("PTY create: phase=%s err=%v", reply.Phase, err)
	}
	return h, reply.PID
}

// origin: hot-update S4 plan §4.2 / §12 P7 — failure on the second resource
// must preserve the first master and every taken file, with their original kind.
func TestPTYBundleSalvagePreservesEveryTakenDescriptor(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newPTYBundleConfig(t, fmt.Sprintf("salvage-%d", round))
			var err error
			f.rt, err = execsession.New(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			var handles []proto.ProcessHandle
			for n := range 2 {
				handle, _ := createBundlePTY(t, f, fmt.Sprint(n), "printf ready; exec sleep 30")
				handles = append(handles, handle)
			}
			cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cut.Thaw() })
			if len(cut.resources) != 3 || cut.resources[2].ID != calibrationResourceID {
				t.Fatal("fixture needs two real PTY resources and calibration custody")
			}
			src := handofftest.DupResources(t, cut.resources)
			second := cut.resources[1].ID
			src.Swap(second, "input-replay", "output-replay")
			cfg := f.cfg
			cfg.Manager = &workload.Manager{}
			imp, salvage, err := Import(cut.Bundle(), cut.Bundle().Processes.Records, src, Target{Runtime: cfg})
			if err == nil || imp != nil || salvage == nil {
				t.Fatal("second resource mismatch did not refuse")
			}
			defer func() { _ = salvage.Close() }()
			merged, err := handoff.MergeResources(salvage.Resources, src.Borrow())
			if err != nil {
				t.Fatal(err)
			}
			if len(merged) != 3 || len(salvage.Resources) == 0 || len(salvage.Resources[0].Files) != 3 || src.Remaining() == 0 {
				t.Fatal("salvage/remainder fixture is empty or lost the first master")
			}
			for _, resource := range merged {
				if resource.ID == calibrationResourceID {
					if resource.Kind != calibrationResourceKind || len(resource.Files) != 1 || resource.Files["state"] == nil {
						t.Fatal("salvage lost calibration custody")
					}
					continue
				}
				if resource.Kind != "pty-task-v1" || len(resource.Files) != 3 {
					t.Fatalf("salvage changed kind/files: %s %d", resource.Kind, len(resource.Files))
				}
				for name, file := range resource.Files {
					if _, err := file.Stat(); err != nil {
						t.Fatalf("salvage closed %s: %v", name, err)
					}
				}
			}
			prepared, err := salvage.Prepare(src.Borrow()...)
			if err != nil {
				t.Fatal(err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			// Preparing a redispatch duplicates files; it must not close or
			// repair the exporter's master open-file-description.
			for _, resource := range cut.resources[:2] {
				if _, err := resource.Files["master"].Stat(); err != nil {
					t.Fatalf("original master closed: %v", err)
				}
			}
			if err := cut.Thaw(); err != nil {
				t.Fatal(err)
			}
			// The retained original master must still perform real I/O, not
			// merely pass fstat. The untouched line discipline echoes this
			// fresh input while the original child keeps its slave open.
			for _, handle := range handles {
				probe := fmt.Sprintf("proof-%d\n", round)
				input, err := f.handle(proto.ProcessResumeReq{Op: "input", Kind: proto.ProcessKindPTY, Handle: &handle, AttachEpoch: 1, Input: []byte(probe)})
				if err != nil || input.InputAccepted != uint64(len(probe)) {
					t.Fatalf("original master refused new input: %v", err)
				}
				deadline := time.Now().Add(bundleTestBudget)
				for {
					reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Kind: proto.ProcessKindPTY, Handle: &handle, AttachEpoch: 1})
					if err != nil {
						t.Fatal(err)
					}
					if reply.Stdout != nil && strings.Contains(string(reply.Stdout.Data), strings.TrimSpace(probe)) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("original master lost echo after refused import")
					}
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "pty-bundle-salvage", beforeG)
	testharness.AssertNoFDLeak(t, "pty-bundle-salvage", beforeFD, 0)
}

func ptyBundleEnvironment(mode, report string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, "TETHER_TEST_PTY_BUNDLE_") {
			env = append(env, entry)
		}
	}
	if mode != "" {
		env = append(env, "TETHER_TEST_PTY_BUNDLE_MODE="+mode, "TETHER_TEST_PTY_BUNDLE_REPORT="+report)
	}
	return env
}
