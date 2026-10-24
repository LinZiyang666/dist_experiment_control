//go:build linux

// image_import_test.go — the construction stage of Import (stage 3 of the
// three-stage import, S2 plan D6): a bundle whose data and descriptors pass
// every read-only predicate but whose actors cannot be built must hand back
// EVERY taken descriptor through the workload Detach primitives, with the
// exporter's budget and process table untouched and no goroutine started.
// runtime_image_test.go's rejection table stops at stage 1/2; this file is
// the stage-3 half (buildTask's fail closure and detachInto).
// origin: hot-update S2 internal review, resources lane.
package execsession

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// TestRuntimeImageImportSalvagesAfterConstructionFailure drives Import past
// stage 1 and 2 (the image is self-consistent and the files are the real
// ones) into a stage-3 refusal from RestoreReplay / RestoreInput /
// RestoreOutput, on the first task (buildTask's own fail closure) and on the
// second (detachInto over an already-built task). Every file that was taken
// must come back open, CLOEXEC and with its exported kernel identity; the
// descriptor count must return to the pre-import baseline once salvage and
// the untaken files are closed; the exporter's budget, table and child are
// untouched; then the intact image still imports and the live task drains.
func TestRuntimeImageImportSalvagesAfterConstructionFailure(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	live, terminal := rf.task(rf.live), rf.task(rf.terminal)
	if live == nil || terminal == nil || live.Stdin == nil || live.Stdout == nil || live.Stdout.Output == nil ||
		terminal.Stdout == nil || terminal.Stderr == nil {
		t.Fatalf("fixture cut shape: %+v", rf.img)
	}
	if rf.img.Tasks[0].PID != rf.live {
		t.Fatalf("fixture order: the live task must sort first (tasks %v)", []string{rf.img.Tasks[0].PID, rf.img.Tasks[1].PID})
	}
	expectedFiles := 0
	for _, ti := range rf.img.Tasks {
		expectedFiles += len(ti.Files)
	}
	// A regular file opened non-blocking and read-only: CheckStreamFile accepts
	// it (readable, O_NONBLOCK, CLOEXEC, no FIFO rule on the read side) and
	// the identity is patched into the image, so only RestoreOutput's poller
	// registration (stage 3) can refuse it.
	plainPath := t.TempDir() + "/plain-source"
	if err := os.WriteFile(plainPath, []byte("not a pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	plain, err := os.OpenFile(plainPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	plainIdentity, err := fileIdentity(plain)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		edit  func(img *Image)
		files func(*imageDupSource)
		stage string // a substring only buildTask's fail closure produces
	}{
		{name: "first-task-input-actor", stage: "stdin actor:", edit: func(img *Image) {
			// EOF delivered but the log is not FIN: RestoreInput refuses it,
			// after the stdin log was already restored (and must be detached).
			ti := &img.Tasks[0]
			ti.Stdin.Replay.FIN, ti.Stdin.Input.Replay.FIN = false, false
		}},
		{name: "first-task-stdout-actor", stage: "output 0 actor:", edit: func(img *Image) {
			// A non-pollable source passes every data and shape predicate
			// (stage 1/2) and is refused by RestoreOutput after stdin and the
			// stdout log were built.
			img.Tasks[0].Files[fileStdoutSource] = plainIdentity
		}, files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.live)]
			_ = files[fileStdoutSource].Close()
			files[fileStdoutSource] = imageDup(t, plain)
		}},
		{name: "first-task-stderr-actor", stage: "output 1 actor:", edit: func(img *Image) {
			img.Tasks[0].Files[fileStderrSource] = plainIdentity
		}, files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.live)]
			_ = files[fileStderrSource].Close()
			files[fileStderrSource] = imageDup(t, plain)
		}},
		{name: "second-task-after-first-built", stage: "output 0 actor:", edit: func(img *Image) {
			// The live task is fully built when the terminal task fails:
			// detachInto must return the live task's five files as well.
			img.Tasks[1].Files[fileStdoutSource] = plainIdentity
		}, files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.terminal)]
			_ = files[fileStdoutSource].Close()
			files[fileStdoutSource] = imageDup(t, plain)
		}},
	}
	usedBefore := rf.f.r.ReplayBudgetUsed()
	tableBefore := rf.f.manager.Snapshot()
	baselineG := runtime.NumGoroutine()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			img := imageClone(t, rf.img)
			tc.edit(&img)
			fdBefore := testharness.FDCount()
			src := newImageDupSource(t, rf.resources)
			if tc.files != nil {
				tc.files(src)
			}
			imported, salvage, err := Import(rf.cfg, img, rf.rows, src)
			if err == nil || imported != nil {
				if imported != nil {
					_, _ = imported.Detach()
				}
				t.Fatalf("%s: imported", tc.name)
			}
			if !errors.Is(err, ErrImage) || !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("%s: not a stage-3 refusal: %v", tc.name, err)
			}
			// Stage 3 takes every expected file first: all of them must be back.
			got := 0
			for _, res := range salvage {
				var ti *TaskImage
				for i := range img.Tasks {
					if resourceID(img.Tasks[i].PID) == res.ID {
						ti = &img.Tasks[i]
					}
				}
				if ti == nil {
					t.Fatalf("%s: salvage names an unknown resource %s", tc.name, res.ID)
				}
				for name, f := range res.Files {
					imageFileOpen(t, res.ID+"/"+name, f)
					id, err := fileIdentity(f)
					if err != nil || id != ti.Files[name] {
						t.Fatalf("%s: salvaged %s/%s is not the exported descriptor: %+v %v", tc.name, res.ID, name, id, err)
					}
					got++
					_ = f.Close()
				}
			}
			if got != expectedFiles {
				t.Fatalf("%s: salvage returned %d files, the cut exported %d", tc.name, got, expectedFiles)
			}
			src.Close()
			if now := testharness.FDCount(); now != fdBefore {
				t.Fatalf("%s: descriptors leaked or closed: fd=%d before=%d", tc.name, now, fdBefore)
			}
			if used := rf.f.r.ReplayBudgetUsed(); used != usedBefore {
				t.Fatalf("%s: exporter budget moved %d → %d", tc.name, usedBefore, used)
			}
			imageChildAlive(t, rf.osPID)
			if rows := rf.f.manager.Snapshot(); len(rows) != len(tableBefore) {
				t.Fatalf("%s: process table changed: %+v", tc.name, rows)
			}
			if g := runtime.NumGoroutine(); g > baselineG+2 {
				t.Fatalf("%s: goroutines %d > baseline %d (a pump started under a refused import)", tc.name, g, baselineG)
			}
		})
	}
	// Nothing above consumed the cut: the intact image imports and the live
	// task ends under the new runtime.
	imported := imageSwitch(t, rf.f, rf.cfg, rf.img, rf.resources, rf.rows)
	if got := imported.ReplayBudgetUsed(); got != usedBefore {
		t.Fatalf("budget after the intact import %d, exported %d", got, usedBefore)
	}
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
	client := &runtimeClient{handle: live.Handle, epoch: 1, inbox: "_TINBOX.actor.original"}
	rf.f.drain(t, client)
	if client.terminal.Signal != int(syscall.SIGKILL) {
		t.Fatalf("live task after the refused imports: %+v", client.terminal)
	}
	rf.f.settle(t, client)
	_ = proto.ProcessResumeVersion
}
