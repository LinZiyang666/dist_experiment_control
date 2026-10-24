//go:build linux

// image_test.go — Import's construction stage (stage 3 of image.go) and
// what it hands back when it fails. The rejection table in
// runtime_image_test.go stops at stages 1 and 2 (nothing built yet); the rows
// here get past both, so buildTask has restored a log, an input actor or an
// output actor before the refusal, and the only code that moves a file back
// OUT of a half-built task (buildTask's fail closure, detachTask, detachInto)
// has to return every taken descriptor exactly once. Real: exec.Cmd
// children, pipes, memfd replays, descriptor duplication. Stubbed: handoff's
// sealed snapshot (the dup-backed FileSource of runtime_image_test.go).
// origin: hot-update S2 internal review, import lane.
package execsession

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// imageFileFD reads a descriptor number without File.Fd (which would clear
// O_NONBLOCK on the shared open-file description).
func imageFileFD(t *testing.T, f *os.File) int {
	t.Helper()
	raw, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := raw.Control(func(x uintptr) { fd = int(x) }); err != nil {
		t.Fatal(err)
	}
	return fd
}

// imageSalvageSet renders a salvage (or an image's files) as sorted
// "id/name" keys and proves no *os.File and no descriptor number appears twice.
func imageSalvageSet(t *testing.T, resources []handoffResourceView) []string {
	t.Helper()
	var keys []string
	pointers := make(map[*os.File]string)
	fds := make(map[int]string)
	for _, res := range resources {
		for name, f := range res.files {
			key := res.id + "/" + name
			if f == nil {
				t.Fatalf("%s: nil file in salvage", key)
			}
			if prev, dup := pointers[f]; dup {
				t.Fatalf("%s and %s: the same *os.File returned twice", prev, key)
			}
			pointers[f] = key
			if fd := imageFileFD(t, f); fds[fd] != "" {
				t.Fatalf("%s and %s: the same descriptor %d returned twice", fds[fd], key, fd)
			} else {
				fds[fd] = key
			}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// handoffResourceView is the part of handoff.Resource the salvage checks read.
type handoffResourceView struct {
	id    string
	files map[string]*os.File
}

// TestImportSalvagesConstructionFailure: a refusal in stage 3 — after every
// descriptor was taken and some actors were already rebuilt on them — must
// hand back exactly the set of files the image declared, each open and
// CLOEXEC, each exactly once, and must leave the exporting runtime with
// nothing consumed (its child alive, its process table unchanged, its own
// Thaw still able to drain the task to a real terminal). The rows are the
// four construction failures that are NOT caught by stages 1 or 2 today
// (RestoreInput/RestoreOutput/RestoreReplay re-validate what the image layer
// does not pre-check: the actor and log schema versions, Base ≤ End) plus the
// one genuine OS-level surprise stage 3 exists for: a source descriptor that
// passes CheckStreamFile but is not pollable (a regular file), which only
// RestoreOutput's SetReadDeadline can detect.
//
// Task A is live with an unfinished stdin (its child never reads, the pipe
// is full, so stdin-target travels and RestoreInput has a real write end);
// task B is terminal (drained, both outputs FIN with their actors) so a
// failure in A's streams exercises detachInto over B when B sorts first, and
// a failure in B's exercises it over A.
func TestImportSalvagesConstructionFailure(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newRuntimeFixture(t, 256)
	hA := f.lease(t, "unfinished-stdin")
	pidA := f.create(t, hA, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}, Stdin: runtimePayload(192 << 10)}).PID
	clientA := &runtimeClient{handle: hA, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.waitFor(t, "A running", func() bool {
		reply, err := f.request(clientA.req("resume"), clientA.inbox)
		return err == nil && reply.Phase == "running"
	})
	hB := f.lease(t, "terminal-awaiting-ack")
	pidB := f.create(t, hB, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf out; printf err >&2; exit 7"}}).PID
	clientB := &runtimeClient{handle: hB, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.drain(t, clientB)
	img, resources, rows := imageCut(t, f)
	var taskA, taskB *TaskImage
	for i := range img.Tasks {
		switch img.Tasks[i].PID {
		case pidA:
			taskA = &img.Tasks[i]
		case pidB:
			taskB = &img.Tasks[i]
		}
	}
	if taskA == nil || taskB == nil || taskA.Stdin == nil || taskA.Stdin.Input == nil || taskA.Stdin.Input.EOF ||
		taskA.Files[fileStdinTarget] == (FileIdentity{}) || taskA.Stdin.Replay.Base >= taskA.Stdin.Replay.End {
		t.Fatalf("fixture cut does not carry an unfinished stdin: A=%+v", taskA)
	}
	if taskB.Stdout == nil || taskB.Stdout.Output == nil || !taskB.Stdout.Replay.FIN || taskB.Stdin != nil {
		t.Fatalf("fixture cut does not carry a terminal task with FIN actors: B=%+v", taskB)
	}
	var osPID int
	for _, row := range rows {
		if row.ID == pidA {
			osPID = row.OSPID
		}
	}
	if osPID == 0 {
		t.Fatal("no process row for A")
	}
	last := img.Tasks[len(img.Tasks)-1].PID
	// A regular file opened non-blocking and read-only: CheckStreamFile
	// (readable, O_NONBLOCK, CLOEXEC, no FIFO requirement on the read side)
	// accepts it; Go's poller cannot register it, so SetReadDeadline fails.
	plainPath := t.TempDir() + "/plain-source"
	if err := os.WriteFile(plainPath, []byte("not a pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	plain, err := os.OpenFile(plainPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	setTask := func(img *Image, pid string, fn func(*TaskImage)) {
		for i := range img.Tasks {
			if img.Tasks[i].PID == pid {
				fn(&img.Tasks[i])
			}
		}
	}
	rowsTable := []struct {
		name  string
		img   func(*Image)
		files func(*imageDupSource)
		text  string // the construction step that must have refused
	}{
		{name: "input-eof-claimed-early", text: "stdin actor",
			// EOF claimed while Base < End: every stage-1 rule holds (no target
			// expected, states agree), RestoreInput refuses it after the stdin
			// log was restored — that log must come back through the salvage.
			img: func(img *Image) {
				setTask(img, pidA, func(ti *TaskImage) {
					ti.Stdin.Input.EOF = true
					delete(ti.Files, fileStdinTarget)
				})
			},
			files: func(src *imageDupSource) {
				_ = src.files[resourceID(pidA)][fileStdinTarget].Close()
				delete(src.files[resourceID(pidA)], fileStdinTarget)
			}},
		{name: "non-pollable-stdout-source-in-first-task", text: "output 0 actor",
			img: func(img *Image) {
				id, err := fileIdentity(plain)
				if err != nil {
					t.Fatal(err)
				}
				setTask(img, pidA, func(ti *TaskImage) { ti.Files[fileStdoutSource] = id })
			},
			files: func(src *imageDupSource) {
				files := src.files[resourceID(pidA)]
				_ = files[fileStdoutSource].Close()
				files[fileStdoutSource] = imageDup(t, plain)
			}},
		{name: "non-pollable-stderr-source-in-first-task", text: "output 1 actor",
			img: func(img *Image) {
				id, err := fileIdentity(plain)
				if err != nil {
					t.Fatal(err)
				}
				setTask(img, pidA, func(ti *TaskImage) { ti.Files[fileStderrSource] = id })
			},
			files: func(src *imageDupSource) {
				files := src.files[resourceID(pidA)]
				_ = files[fileStderrSource].Close()
				files[fileStderrSource] = imageDup(t, plain)
			}},
		{name: "non-pollable-source-in-last-task", text: "output 0 actor",
			img: func(img *Image) {
				id, err := fileIdentity(plain)
				if err != nil {
					t.Fatal(err)
				}
				setTask(img, last, func(ti *TaskImage) { ti.Files[fileStdoutSource] = id })
			},
			files: func(src *imageDupSource) {
				files := src.files[resourceID(last)]
				_ = files[fileStdoutSource].Close()
				files[fileStdoutSource] = imageDup(t, plain)
			}},
	}
	baselineG := runtime.NumGoroutine()
	tableBefore := fmt.Sprintf("%+v", f.manager.Snapshot())
	for _, tc := range rowsTable {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			img := imageClone(t, img)
			tc.img(&img)
			fdBefore := testharness.FDCount()
			src := newImageDupSource(t, resources)
			if tc.files != nil {
				tc.files(src)
			}
			imported, salvage, err := Import(f.r.cfg, img, rows, src)
			if err == nil || imported != nil {
				if imported != nil {
					_, _ = imported.Detach()
				}
				t.Fatalf("%s was imported", tc.name)
			}
			if !errors.Is(err, ErrImage) || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("%s: want an ErrImage from %q, got %v", tc.name, tc.text, err)
			}
			views := make([]handoffResourceView, 0, len(salvage))
			for _, res := range salvage {
				views = append(views, handoffResourceView{id: res.ID, files: res.Files})
			}
			got := imageSalvageSet(t, views)
			// The salvage must be exactly what the (mutated) image declares.
			var expected []string
			for _, ti := range img.Tasks {
				for name := range ti.Files {
					expected = append(expected, resourceID(ti.PID)+"/"+name)
				}
			}
			sort.Strings(expected)
			if strings.Join(got, "\n") != strings.Join(expected, "\n") {
				t.Fatalf("%s: salvage is not exactly the declared files\n got: %v\nwant: %v", tc.name, got, expected)
			}
			for _, res := range salvage {
				for name, file := range res.Files {
					imageFileOpen(t, res.ID+"/"+name, file)
					_ = file.Close()
				}
			}
			src.Close()
			if got := testharness.FDCount(); got != fdBefore {
				t.Fatalf("%s: importer leaked or closed descriptors: fd=%d before=%d", tc.name, got, fdBefore)
			}
			imageChildAlive(t, osPID)
			if got := fmt.Sprintf("%+v", f.manager.Snapshot()); got != tableBefore {
				t.Fatalf("%s: process table changed:\n%s\n%s", tc.name, tableBefore, got)
			}
			if g := runtime.NumGoroutine(); g > baselineG+2 {
				t.Fatalf("%s: goroutines %d > baseline %d", tc.name, g, baselineG)
			}
		})
	}
	// Nothing was consumed: the exporting runtime thaws and both tasks reach
	// their real ends — A is still alive with its unread stdin, B settles.
	f.manager.Thaw()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	f.settle(t, clientB)
	_ = f.manager.Signal(pidA, syscall.SIGKILL)
	f.drain(t, clientA)
	if clientA.terminal.Signal != int(syscall.SIGKILL) {
		t.Fatalf("A after the refused imports: %+v", clientA.terminal)
	}
	f.settle(t, clientA)
	if used := f.r.ReplayBudgetUsed(); used != 0 {
		t.Fatalf("budget after settlement: %d", used)
	}
}
