//go:build linux

// Real: same-PID exec, original external pipe writer, frozen memfd bytes and EOF.
// Stubbed: application bootstrap, carrier ACK protocol and upgrade coordinator.
// The original writer remains outside the replaced reader process throughout.
// origin: docs/reviews/hot-update-plan.md §4.2.
package workload

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const outputImageMode = "TETHER_TEST_OUTPUT_IMAGE_MODE"

type outputImageState struct {
	Output OutputState `json:"output"`
	PID    int         `json:"pid"`
	Device uint64      `json:"device"`
	Inode  uint64      `json:"inode"`
}

func TestOutputExecRetainsOriginalPipeAndUnacknowledgedBytes(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(outputImageMode); mode {
	case "before":
		outputImageBefore(t)
		return
	case "after":
		outputImageAfter(t)
		return
	case "":
	default:
		t.Fatalf("unknown output image test mode %q", mode)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 4*outputTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOutputExecRetainsOriginalPipeAndUnacknowledgedBytes$", "-test.count=1")
	cmd.Env = outputImageEnvironment("before")
	cmd.ExtraFiles = []*os.File{reader}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close() }()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helperPID := cmd.Process.Pid
	_ = reader.Close()
	var once sync.Once
	var waitErr error
	wait := func() error { once.Do(func() { waitErr = cmd.Wait() }); return waitErr }
	defer func() { _ = cmd.Process.Kill(); _ = writer.Close(); _ = wait() }()
	want := outputBytes(256 << 10)
	written := outputStartWriter(writer, want)
	if file, ok := stdout.(*os.File); ok {
		if err := file.SetReadDeadline(time.Now().Add(outputTestBudget)); err != nil {
			t.Fatal(err)
		}
	}
	lines := bufio.NewReader(stdout)
	ready, err := lines.ReadString('\n')
	if err != nil || ready != "output-frozen\n" {
		detail, _ := lines.Peek(lines.Buffered())
		_ = cmd.Process.Kill()
		_ = wait()
		t.Fatalf("helper freeze: %q %v\n%s\n%s", ready, err, detail, stderr.String())
	}
	select {
	case result := <-written:
		t.Fatalf("original writer was not backpressured before exec: %+v", result)
	default:
	}
	if _, err := stdin.Write([]byte("exec\n")); err != nil {
		t.Fatal(err)
	}
	line, err := lines.ReadString('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		_ = wait()
		t.Fatalf("restored output receipt: %v\n%s", err, stderr.String())
	}
	wantReceipt := fmt.Sprintf("output-restored:%d:%d:%x\n", helperPID, len(want), sha256.Sum256(want))
	if line != wantReceipt {
		t.Fatalf("wrong original stream receipt: got=%q want=%q", line, wantReceipt)
	}
	outputRequireWriter(t, written, len(want))
	if err := wait(); err != nil {
		t.Fatalf("output exec helper: %v\n%s", err, stderr.String())
	}
}

func outputImageBefore(t *testing.T) {
	t.Helper()
	// ExtraFiles is only the fixture bootstrap, before Output takes ownership:
	// os.StartProcess extracts File.Fd and can make this inherited pipe blocking.
	// Register the raw source with Go's poller before constructing its owner.
	// The actual image handoff below must preserve these established flags.
	unix.CloseOnExec(3)
	if err := unix.SetNonblock(3, true); err != nil {
		t.Fatal(err)
	}
	source := os.NewFile(3, "original-output-pipe")
	defer func() { _ = source.Close() }()
	log, err := NewReplay(&ReplayBudget{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewOutput(source, log)
	if err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	outputAwait(t, func() bool { return log.Snapshot().End == 1024 })
	state, files := outputFreeze(t, out)
	info, err := files["source"].Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	metadata, err := json.Marshal(outputImageState{Output: state, PID: os.Getpid(), Device: uint64(stat.Dev), Inode: stat.Ino})
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata) >= 4096 {
		t.Fatalf("healthy pipe bytes leaked into metadata: %d", len(metadata))
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "output-test", Instance: "original-writer", Generation: 1, Data: metadata},
		[]handoff.Resource{{ID: "output", Kind: "pipe-replay", Files: files}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if _, err := fmt.Fprintln(os.Stdout, "output-frozen"); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil || line != "exec\n" {
		t.Fatalf("outer writer did not authorize fixture exec: %q %v", line, err)
	}
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestOutputExecRetainsOriginalPipeAndUnacknowledgedBytes$", "-test.count=1"}, outputImageEnvironment("after")); err != nil {
		t.Fatal(err)
	}
	t.Fatal("exec returned to the original output actor")
}

func outputImageAfter(t *testing.T) {
	t.Helper()
	packageState, err := handoff.Restore()
	if err != nil || packageState == nil {
		t.Fatalf("output handoff restore: %v", err)
	}
	defer func() { _ = packageState.Close() }()
	var metadata outputImageState
	if err := json.Unmarshal(packageState.State.Data, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.PID != os.Getpid() {
		t.Fatalf("reader PID changed: %d -> %d", metadata.PID, os.Getpid())
	}
	source, err := packageState.TakeFile("output", "source")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	file, err := packageState.TakeFile("output", "replay")
	if err != nil {
		t.Fatal(err)
	}
	log, err := RestoreReplay(&ReplayBudget{}, metadata.Output.Replay, file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	out, err := RestoreOutput(metadata.Output, source, log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Close() }()
	info, err := source.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if uint64(stat.Dev) != metadata.Device || stat.Ino != metadata.Inode {
		t.Fatal("restore replaced the original business pipe")
	}
	prefix := make([]byte, 1024)
	if n, err := log.Read(0, prefix); err != nil || n != len(prefix) || !bytes.Equal(prefix, outputBytes(len(prefix))) {
		t.Fatalf("pre-exec replay bytes changed: n=%d err=%v", n, err)
	}
	if err := log.Ack(1024); err != ErrFrozen {
		t.Fatalf("restored log activated before Thaw: %v", err)
	}
	out.Thaw()
	got := outputCollect(t, log, 0)
	if state := out.Snapshot(); state.ReadError != "" || !state.Replay.FIN || len(state.Pending) != 0 {
		t.Fatalf("original writer's EOF lost: %+v", state)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "output-restored:%s:%d:%x\n", strconv.Itoa(os.Getpid()), len(got), sha256.Sum256(got)); err != nil {
		t.Fatal(err)
	}
}

func outputImageEnvironment(mode string) []string {
	var env []string
	for _, value := range handoff.CleanEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(value, "=")
		if key != outputImageMode {
			env = append(env, value)
		}
	}
	return append(env, outputImageMode+"="+mode)
}
