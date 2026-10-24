//go:build linux

// Real: same-PID exec, the original external pipe reader, retained kernel bytes,
// partial-write offsets, replay FD import, and delivery of one terminal EOF.
// Stubbed: network carrier, process table, and application upgrade policy.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const inputImageMode = "TETHER_TEST_INPUT_IMAGE_MODE"
const inputImageReport = "TETHER_TEST_INPUT_IMAGE_REPORT"

type inputImageState struct {
	PID   int                 `json:"pid"`
	Inode uint64              `json:"inode"`
	State workload.InputState `json:"state"`
}

func TestInputExecPreservesOriginalPipeWithoutLossOrDuplicate(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(inputImageMode); mode {
	case "before":
		inputBeforeExec(t)
		return
	case "after":
		inputAfterExec(t)
		return
	case "":
	default:
		t.Fatalf("unknown input image mode %q", mode)
	}
	reader, writer := inputPipe(t)
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	var original unix.Stat_t
	inputControl(t, reader, func(fd uintptr) {
		if err := unix.Fstat(int(fd), &original); err != nil {
			t.Fatal(err)
		}
	})
	report := filepath.Join(t.TempDir(), "after-exec.json")
	ctx, cancel := context.WithTimeout(t.Context(), 3*inputTestBudget)
	defer cancel()
	dir := t.TempDir()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInputExecPreservesOriginalPipeWithoutLossOrDuplicate$", "-test.count=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Dir = dir
	cmd.Env = append(inputImageEnvironment("before"), inputImageReport+"="+report)
	cmd.Env = append(cmd.Env, "TMPDIR="+dir)
	cmd.ExtraFiles = []*os.File{writer}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	_ = writer.Close() // Only the original child image now owns the write end.
	inputAwait(t, func() bool { _, err := os.Stat(report); return err == nil })
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	var receipt inputImageState
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.PID != cmd.Process.Pid || receipt.Inode != original.Ino ||
		receipt.State.Replay.Base != 4096 || receipt.State.Replay.End != 64<<10 ||
		!receipt.State.Replay.FIN || receipt.State.EOF {
		t.Fatalf("new image changed original writer identity or applied prefix: %+v", receipt)
	}
	_ = reader.SetReadDeadline(time.Now().Add(inputTestBudget))
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, inputPayload(64<<10)) {
		t.Fatalf("original reader lost/duplicated bytes or EOF across exec: got=%d err=%v", len(got), err)
	}
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatalf("new input image did not finish cleanly: %v\n%s", err, output.String())
	}
}

func inputBeforeExec(t *testing.T) {
	t.Helper()
	// ExtraFiles is the test bootstrap only. Application handoff itself must
	// accept only nonblocking CLOEXEC resources and preserve both properties.
	unix.CloseOnExec(3)
	if err := unix.SetNonblock(3, true); err != nil {
		t.Fatal(err)
	}
	writer := os.NewFile(3, "original-input-writer")
	defer func() { _ = writer.Close() }()
	payload := inputPayload(64 << 10)
	log, err := workload.NewReplay(&workload.ReplayBudget{}, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if n, err := log.Append(payload); n != len(payload) || err != nil {
		t.Fatalf("append before exec: n=%d err=%v", n, err)
	}
	if err := log.Finish(); err != nil {
		t.Fatal(err)
	}
	pump, err := workload.NewInput(writer, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	inputAwait(t, func() bool { return inputQueued(t, writer) == 4096 })
	ctx, cancel := context.WithTimeout(t.Context(), inputTestBudget)
	defer cancel()
	state, files, err := pump.Freeze(ctx)
	if err != nil || state.Replay.Base != 4096 || state.EOF || state.WriteError != "" {
		t.Fatalf("freeze original blocked writer: %+v %v", state, err)
	}
	fixture := inputImageState{PID: os.Getpid(), State: state}
	inputControl(t, writer, func(fd uintptr) {
		var stat unix.Stat_t
		if err := unix.Fstat(int(fd), &stat); err != nil {
			t.Fatal(err)
		}
		fixture.Inode = stat.Ino
	})
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "input-test", Instance: "original-writer", Generation: 1, Data: data},
		[]handoff.Resource{{ID: "stdin", Kind: "input", Files: files}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestInputExecPreservesOriginalPipeWithoutLossOrDuplicate$", "-test.count=1"}, inputImageEnvironment("after")); err != nil {
		t.Fatalf("exec original input owner: %v", err)
	}
	t.Fatal("successful exec returned to the old input image")
}

func inputAfterExec(t *testing.T) {
	t.Helper()
	image, err := handoff.Restore()
	if err != nil || image == nil {
		t.Fatalf("restore input image envelope: %v", err)
	}
	defer func() { _ = image.Close() }()
	var fixture inputImageState
	if err := json.Unmarshal(image.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.PID != os.Getpid() {
		t.Fatal("input image replacement changed original owner PID")
	}
	writer, err := image.TakeFile("stdin", "target")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	inputControl(t, writer, func(fd uintptr) {
		var stat unix.Stat_t
		if err := unix.Fstat(int(fd), &stat); err != nil || stat.Ino != fixture.Inode {
			t.Fatalf("restored writer is not the original endpoint: inode=%d err=%v", stat.Ino, err)
		}
	})
	file, err := image.TakeFile("stdin", "replay")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	log, err := workload.RestoreReplay(&workload.ReplayBudget{}, fixture.State.Replay, file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	pump, err := workload.RestoreInput(fixture.State, writer, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pump.Close() }()
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	// Publish the new-image receipt atomically; the external original reader
	// starts draining only after this, keeping pre-exec partial writes real.
	path := os.Getenv(inputImageReport)
	if err := os.WriteFile(path+".tmp", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
	pump.Thaw()
	inputAwait(t, func() bool { return pump.Snapshot().EOF })
	final := pump.Snapshot()
	if final.WriteError != "" || final.Replay.Base != final.Replay.End {
		t.Fatalf("restored input did not deliver its retained suffix: %+v", final)
	}
}

func inputImageEnvironment(mode string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(entry, inputImageMode+"=") {
			env = append(env, entry)
		}
	}
	return append(env, inputImageMode+"="+mode)
}
