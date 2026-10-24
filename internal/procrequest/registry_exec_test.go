//go:build linux

// Real: same-PID image replacement, original child, parent relation and lease key.
// Stubbed: broker authorization, reliable streams and application upgrade policy.
// Registry restoration is not an activation of the full workload image.
// origin: docs/reviews/hot-update-plan.md §4.2.
package procrequest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
)

const requestImageMode = "TETHER_TEST_REQUEST_IMAGE_MODE"
const requestImageReport = "TETHER_TEST_REQUEST_IMAGE_REPORT"

type requestImageState struct {
	Registry State               `json:"registry"`
	Scope    Scope               `json:"scope"`
	Handle   proto.ProcessHandle `json:"handle"`
	OwnerPID int                 `json:"owner_pid"`
	ChildPID int                 `json:"child_pid"`
	Ticks    uint64              `json:"ticks"`
}

type requestImageReceipt struct {
	OwnerPID int    `json:"owner_pid"`
	ChildPID int    `json:"child_pid"`
	Ticks    uint64 `json:"ticks"`
	ExitCode int    `json:"exit_code"`
	Logical  string `json:"logical"`
}

func TestRegistryExecRetainsCreationIdentityAndOriginalChild(t *testing.T) {
	switch mode := os.Getenv(requestImageMode); mode {
	case "before":
		requestImageBefore(t)
		return
	case "after":
		requestImageAfter(t)
		return
	case "":
	default:
		t.Fatalf("unknown request registry image mode %q", mode)
	}
	report := filepath.Join(t.TempDir(), "receipt.json")
	ctx, cancel := context.WithTimeout(t.Context(), 4*requestTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRegistryExecRetainsCreationIdentityAndOriginalChild$", "-test.count=1")
	cmd.Env = append(requestImageEnvironment("before"), requestImageReport+"="+report)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("same-PID registry handoff: %v\n%s", err, output)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	var receipt requestImageReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.OwnerPID != cmd.Process.Pid || receipt.ChildPID <= 0 || receipt.Ticks == 0 || receipt.ExitCode != 23 || receipt.Logical != "originalTask" {
		t.Fatalf("wrong same-PID registry receipt: %+v; helper PID=%d", receipt, cmd.Process.Pid)
	}
}

func requestImageBefore(t *testing.T) {
	t.Helper()
	r, err := New("image-instance", 4, requestTestTTL)
	if err != nil {
		t.Fatal(err)
	}
	scope := requestScope()
	lease, err := r.Issue(scope)
	if err != nil {
		t.Fatal(err)
	}
	h := proto.ProcessHandle{Lease: lease, RequestID: "originalRequest"}
	if _, fresh, err := r.Reserve(scope, h, requestSpec(), "originalTask", "_TINBOX.actor.reply", 1); err != nil || !fresh {
		t.Fatalf("initial child reservation: fresh=%t err=%v", fresh, err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	child := exec.Command("/bin/sh", "-c", "read -r value; [ \"$value\" = resume ] || exit 91; exit 23")
	child.Stdin, child.Stdout, child.Stderr = reader, os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	pid := child.Process.Pid
	// A successful exec does not run this defer. Failed preparation must not
	// leave the original child waiting forever on a pipe whose owner failed.
	defer func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		var status syscall.WaitStatus
		_, _ = syscall.Wait4(pid, &status, 0, nil)
	}()
	_ = reader.Close()
	if err := r.Running(h, "originalTask"); err != nil {
		t.Fatal(err)
	}
	parent, ticks := requestChildIdentity(t, pid)
	if parent != os.Getpid() {
		t.Fatalf("child parent=%d want original owner %d", parent, os.Getpid())
	}
	// Release only the old Go pidfd handle. The kernel child remains alive and
	// waitable by the original parent after that parent's image replacement.
	if err := child.Process.Release(); err != nil {
		t.Fatal(err)
	}
	terminalLease, err := r.Issue(scope)
	if err != nil {
		t.Fatal(err)
	}
	terminalHandle := proto.ProcessHandle{Lease: terminalLease, RequestID: "finishedRequest"}
	if _, fresh, err := r.Reserve(scope, terminalHandle, requestSpec(), "finishedTask", "_TINBOX.actor.finished", 1); err != nil || !fresh {
		t.Fatalf("terminal reservation: fresh=%t err=%v", fresh, err)
	}
	finished := exec.Command("/bin/sh", "-c", "exit 17")
	if err := finished.Run(); err == nil || finished.ProcessState.ExitCode() != 17 {
		t.Fatalf("pre-exec real terminal: err=%v status=%v", err, finished.ProcessState)
	}
	if err := r.Complete(terminalHandle, "finishedTask", Terminal{ID: "finishedTerminal", ExitCode: 17}); err != nil {
		t.Fatal(err)
	}
	state, err := r.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(requestImageState{Registry: state, Scope: scope, Handle: h, OwnerPID: os.Getpid(), ChildPID: pid, Ticks: ticks})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: state.Instance, Generation: 1, Data: payload}, []handoff.Resource{{ID: "original-child", Kind: "pipe", Files: map[string]*os.File{"stdin": writer}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestRegistryExecRetainsCreationIdentityAndOriginalChild$", "-test.count=1"}, requestImageEnvironment("after")); err != nil {
		t.Fatalf("replace registry image: %v", err)
	}
	t.Fatal("successful exec unexpectedly returned")
}

func requestImageAfter(t *testing.T) {
	t.Helper()
	image, err := handoff.Restore()
	if err != nil || image == nil {
		t.Fatalf("restore private image: present=%t err=%v", image != nil, err)
	}
	defer func() { _ = image.Close() }()
	var state requestImageState
	if err := json.Unmarshal(image.State.Data, &state); err != nil {
		t.Fatal(err)
	}
	if state.OwnerPID != os.Getpid() {
		t.Fatalf("owner PID changed across image: old=%d now=%d", state.OwnerPID, os.Getpid())
	}
	parent, ticks := requestChildIdentity(t, state.ChildPID)
	if parent != os.Getpid() || ticks != state.Ticks {
		t.Fatalf("original child identity changed: ppid=%d ticks=%d want ppid=%d ticks=%d", parent, ticks, os.Getpid(), state.Ticks)
	}
	process, err := os.FindProcess(state.ChildPID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = process.Kill(); _, _ = process.Wait() }()
	r, err := Restore(state.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Issue(state.Scope); !errors.Is(err, ErrFrozen) {
		t.Fatalf("image activated registry before runtime fence: %v", err)
	}
	r.Thaw()
	got, fresh, err := r.Reserve(state.Scope, state.Handle, requestSpec(), "would-respawn", "_TINBOX.actor.reply", 1)
	if err != nil || fresh || got.PID != "originalTask" {
		t.Fatalf("same handle lost original task after exec: fresh=%t pid=%q err=%v", fresh, got.PID, err)
	}
	for _, record := range state.Registry.Records {
		if record.PID == "finishedTask" {
			retained, err := r.Lookup(state.Scope, record.Handle)
			if err != nil || retained.Terminal == nil || retained.Terminal.ExitCode != 17 {
				t.Fatalf("real terminal receipt lost across exec: terminal=%v err=%v", retained.Terminal, err)
			}
		}
	}
	input, err := image.TakeFile("original-child", "stdin")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	if _, err := input.Write([]byte("resume\n")); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	exit, err := process.Wait()
	if err != nil || exit.ExitCode() != 23 {
		t.Fatalf("original child wait receipt: status=%v err=%v", exit, err)
	}
	if err := r.Complete(state.Handle, "originalTask", Terminal{ID: "originalTerminal", ExitCode: exit.ExitCode()}); err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(requestImageReceipt{OwnerPID: os.Getpid(), ChildPID: state.ChildPID, Ticks: ticks, ExitCode: exit.ExitCode(), Logical: got.PID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(requestImageReport), receipt, 0600); err != nil {
		t.Fatal(err)
	}
}

func requestChildIdentity(t *testing.T, pid int) (int, uint64) {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		t.Fatal("malformed Linux process identity")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		t.Fatal("short Linux process identity")
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatal(err)
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return parent, ticks
}

func requestImageEnvironment(mode string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, requestImageMode+"=") {
			env = append(env, entry)
		}
	}
	return append(env, requestImageMode+"="+mode)
}
