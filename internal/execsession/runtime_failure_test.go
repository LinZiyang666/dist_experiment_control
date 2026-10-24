//go:build linux

// These tests exercise real replay files, pipes, and OS children under the
// runtime's resource owner. Start hooks control failure timing only. They do
// not cover carrier transport or process import across an image switch.
package execsession

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	cpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func failureRuntime(t *testing.T, capacity int64, start func(*workload.Manager, string, *proto.ExecReq) (*workload.Stdio, string, error)) *Runtime {
	t.Helper()
	dir := t.TempDir()

	manager := &workload.Manager{}
	r, err := New(Config{Instance: "failureInstance", Manager: manager, ReplayCapacity: capacity,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			if req.Cwd == "" {
				copy := *req
				copy.Cwd = dir
				req = &copy
			}
			return start(manager, pid, req)
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	return r
}

func failureRequest(t *testing.T, r *Runtime) (procrequest.Scope, proto.ProcessResumeReq, *task) {
	t.Helper()
	digest := sha256.Sum256([]byte("runtime failure actor"))
	scope := procrequest.Scope{SID: "failureSession", NID: "failureNode", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "failureClient"}
	probe, err := r.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "failure-inbox")
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v, %v", probe, err)
	}
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2, Op: "create", ActorFP: scope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: *probe.Lease, RequestID: "failureRequest"}, AttachEpoch: 1,
		Exec: &proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}}
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || reply.Phase != "starting" {
		t.Fatalf("create: %+v, %v", reply, err)
	}
	r.mu.Lock()
	owner := r.tasks[reply.PID]
	r.mu.Unlock()
	if owner == nil {
		t.Fatal("create did not retain a resource owner")
	}
	req.CallID++
	req.Op, req.Exec = "resume", nil
	return scope, req, owner
}

func failureAwait(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func failureStartChild(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, error) {
	stdio, err := workload.NewStdio()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
	cmd.Dir = req.Cwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdio.Bind(cmd)
	if err := cmd.Start(); err != nil {
		stdio.Close()
		return nil, err
	}
	stdio.CloseChild()
	if err := manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now()}, cmd.Process); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		stdio.Close()
		return nil, err
	}
	return stdio, nil
}

func TestRuntimeCloseCanceledStartupReclaimsLateFailure(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	entered, release := make(chan struct{}), make(chan struct{})
	r := failureRuntime(t, 64, func(_ *workload.Manager, _ string, _ *proto.ExecReq) (*workload.Stdio, string, error) {
		close(entered)
		<-release
		return nil, "", fmt.Errorf("controlled failure before child start")
	})
	r.budget.Limit = (64 << 10) + 128
	// Release startup before the cleanup registered by failureRuntime.
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	_, _, owner := failureRequest(t, r)
	failureAwait(t, entered, "start hook")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close during blocked startup = %v", err)
	}
	close(release)
	failureAwait(t, owner.done, "late failed startup cleanup")
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 {
		t.Errorf("closed runtime retained %d completed startup owner(s)", retained)
	}
	for i, log := range owner.logs[1:] {
		if _, err := log.Read(0, nil); !errors.Is(err, os.ErrClosed) {
			t.Errorf("closed runtime retained output replay %d: Read = %v", i, err)
		}
	}
	// A single full allocation proves that every reservation was released.
	probe, err := workload.NewReplay(&r.budget, r.budget.Limit)
	if err != nil {
		t.Errorf("completed startup retained replay budget: %v", err)
	} else {
		_ = probe.Close()
	}
}

func TestRuntimeCloseReapsChildWithUnacknowledgedFullOutput(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	r := failureRuntime(t, 8, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		childSpec := *req
		childSpec.Argv = []string{"/bin/sh", "-c", "printf 12345678901234567890; exec sleep 30"}
		stdio, err := failureStartChild(manager, pid, &childSpec)
		return stdio, "", err
	})
	r.budget.Limit = (64 << 10) + 16
	_, _, owner := failureRequest(t, r)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		full := owner.output[0] != nil && owner.logs[1].Snapshot().End == 8
		r.mu.Unlock()
		if full {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("stdout never reached real replay backpressure")
		}
	}
	children := r.cfg.Manager.Snapshot()
	if len(children) != 1 || children[0].Exit != nil || !children[0].ReapHeld {
		t.Fatalf("expected original child held behind unacknowledged output: %+v", children)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(children[0].OSPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("original child remains after completed Close: %v", err)
	}
	if children := r.cfg.Manager.Snapshot(); len(children) != 0 {
		t.Errorf("manager retained child: %+v", children)
	}
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 {
		t.Errorf("closed task remains: %d", retained)
	}
	probe, err := workload.NewReplay(&r.budget, r.budget.Limit)
	if err != nil {
		t.Fatal(err)
	}
	_ = probe.Close()
}

func TestRuntimePumpSetupFailureRetainsObservableTerminal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var starts atomic.Int32
	r := failureRuntime(t, 8, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		stdio, err := failureStartChild(manager, pid, req)
		if err == nil {
			starts.Add(1)
		}
		// This valid configuration admits an eight-byte replay. The warning
		// forces stderr setup to fail after the child and stdout pump exist.
		return stdio, strings.Repeat("w", 16), err
	})
	scope, req, owner := failureRequest(t, r)
	failureAwait(t, owner.done, "setup failure and sole-owner reap")
	if starts.Load() != 1 || len(r.cfg.Manager.Snapshot()) != 0 {
		t.Fatalf("child ownership after setup failure: starts=%d children=%+v", starts.Load(), r.cfg.Manager.Snapshot())
	}
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil {
		t.Fatalf("completed setup failure is not observable through resume: %v (terminal=%+v)", err, reply)
	}
	if reply.Phase != "terminal" || reply.TerminalID == "" || reply.Error == "" || reply.ExitCode != -1 {
		t.Fatalf("setup failure lost its real terminal: %+v", reply)
	}
}

func TestRuntimePartialAllocationFailureSettlesAndReleasesBudget(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var starts atomic.Int32
	r := failureRuntime(t, 8, func(_ *workload.Manager, _ string, _ *proto.ExecReq) (*workload.Stdio, string, error) {
		starts.Add(1)
		return nil, "", fmt.Errorf("unexpected start")
	})
	// stdin and stdout fit; stderr fails while earlier real memfds exist.
	r.budget.Limit = (64 << 10) + 8
	scope, req, owner := failureRequest(t, r)
	failureAwait(t, owner.done, "partial allocation failure")
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || reply.Phase != "terminal" || reply.TerminalID == "" || reply.Error == "" ||
		reply.Stdout == nil || !reply.Stdout.FIN || reply.Stderr == nil || !reply.Stderr.FIN || starts.Load() != 0 {
		t.Fatalf("partial allocation response: %+v, err=%v starts=%d", reply, err, starts.Load())
	}
	// No child was ever created: both empty streams are a true EOF, never an
	// abort. origin: docs/reviews/hot-update-stable-tree-plan.md §3 T3
	if reply.Stdout.Error != "" || reply.Stderr.Error != "" {
		t.Fatalf("allocation failure reported as a stream abort: %+v", reply)
	}
	req.CallID++
	req.Op, req.TerminalID, req.StdoutFIN, req.StderrFIN = "terminal-ack", reply.TerminalID, true, true
	settled, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || settled.Phase != "settled" {
		t.Fatalf("settle partial allocation: %+v, %v", settled, err)
	}
	probe, err := workload.NewReplay(&r.budget, r.budget.Limit)
	if err != nil {
		t.Fatal(err)
	}
	_ = probe.Close()
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 {
		t.Fatalf("settled task still retained: %d", retained)
	}
}

// failureStartPTYStdoutChild starts a real child whose stdout is a PTY slave
// and returns the pollable master as the task's stdout source. When the
// child exits and its copy of the slave was the last one, the kernel ends
// the master with EIO instead of EOF: the one exceptional stream end the
// product pump records on its own, with the whole tail still readable
// first (internal/pty proves the ordering). No test hook touches the runtime.
func failureStartPTYStdoutChild(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, error) {
	raw, slave, err := cpty.Open()
	if err != nil {
		return nil, err
	}
	// creack's Open already called File.Fd on raw, which put it back into
	// blocking mode; a fresh CLOEXEC duplicate registers with Go's poller
	// the way internal/pty prepares its master.
	conn, err := raw.SyscallConn()
	if err != nil {
		_ = raw.Close()
		_ = slave.Close()
		return nil, err
	}
	duplicate, inner := -1, error(nil)
	if err := conn.Control(func(fd uintptr) {
		duplicate, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3)
		if inner == nil {
			inner = unix.SetNonblock(duplicate, true)
		}
	}); err != nil || inner != nil {
		if duplicate >= 0 {
			_ = unix.Close(duplicate)
		}
		_ = raw.Close()
		_ = slave.Close()
		return nil, errors.Join(err, inner)
	}
	master := os.NewFile(uintptr(duplicate), raw.Name())
	_ = raw.Close()
	stdio, err := workload.NewStdio()
	if err != nil {
		_ = master.Close()
		_ = slave.Close()
		return nil, err
	}
	cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
	cmd.Dir = req.Cwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdio.Bind(cmd)
	cmd.Stdout = slave
	if err := cmd.Start(); err != nil {
		stdio.Close()
		_ = master.Close()
		_ = slave.Close()
		return nil, err
	}
	stdio.CloseChild()
	_ = slave.Close() // the child's copy is now the last slave
	_ = stdio.Stdout.Close()
	stdio.Stdout = master
	if err := manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now()}, cmd.Process); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		stdio.Close()
		return nil, err
	}
	return stdio, nil
}

// failureStartChildWithOutput starts the setup-failure child and returns only
// once it has written its 20 stdout bytes and touched marker, so the SIGKILL
// that follows the failed stderr setup provably races nothing: the bytes are
// already in the kernel pipe and the pump must drain them to a true EOF.
func failureStartChildWithOutput(t *testing.T, manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, error) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "wrote")
	childSpec := *req
	childSpec.Argv = []string{"/bin/sh", "-c", "printf 12345678901234567890; : > " + marker + "; exec sleep 30"}
	stdio, err := failureStartChild(manager, pid, &childSpec)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			return stdio, nil
		}
		if time.Now().After(deadline) {
			_ = manager.Signal(pid, syscall.SIGKILL)
			stdio.Close()
			return nil, fmt.Errorf("child never reported its output")
		}
		time.Sleep(time.Millisecond)
	}
}

// failureProbeBudget proves every replay reservation was released: one
// allocation of the whole limit must succeed.
func failureProbeBudget(t *testing.T, r *Runtime) {
	t.Helper()
	probe, err := workload.NewReplay(&r.budget, r.budget.Limit)
	if err != nil {
		t.Fatalf("replay budget still reserved: %v", err)
	}
	_ = probe.Close()
}

// failureCollect drives resume/ack until both streams have ended and the
// terminal is known, mirroring a client that only echoes true EOFs as FIN.
// It returns the collected bytes and the last reply.
func failureCollect(t *testing.T, r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq) ([2][]byte, [2]bool, [2]string, proto.ProcessResumeReply) {
	t.Helper()
	var got [2][]byte
	var fin [2]bool
	var abort [2]string
	deadline := time.Now().Add(5 * time.Second)
	for {
		req.CallID++
		req.Op = "resume"
		req.StdoutACK, req.StderrACK, req.StdoutFIN, req.StderrFIN = uint64(len(got[0])), uint64(len(got[1])), fin[0], fin[1]
		reply, err := r.Handle(scope, req, "failure-inbox")
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		for i, batch := range []*proto.ProcessOutput{reply.Stdout, reply.Stderr} {
			if batch == nil {
				continue
			}
			if batch.Offset != uint64(len(got[i])) || len(batch.Data) > 32<<10 {
				t.Fatalf("stream %d: offset %d want %d (%d bytes)", i, batch.Offset, len(got[i]), len(batch.Data))
			}
			if batch.FIN && batch.Error != "" {
				t.Fatalf("stream %d carries both end markers: %+v", i, batch)
			}
			if (fin[i] || abort[i] != "") && len(batch.Data) != 0 {
				t.Fatalf("stream %d advanced after its end", i)
			}
			got[i] = append(got[i], batch.Data...)
			if batch.FIN {
				fin[i] = true
			}
			if batch.Error != "" {
				abort[i] = batch.Error
			}
		}
		if reply.Phase == "terminal" && (fin[0] || abort[0] != "") && (fin[1] || abort[1] != "") {
			return got, fin, abort, reply
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams never ended: fin=%v abort=%q phase=%q", fin, abort, reply.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T5
func TestRuntimeSetupFailureAbortsUnbuiltStreamAndSettles(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	r := failureRuntime(t, 8, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		stdio, err := failureStartChildWithOutput(t, manager, pid, req)
		// stdout already exists when the 16-byte warning makes the 8-byte
		// stderr log refuse its pump: a real post-start setup failure.
		return stdio, strings.Repeat("w", 16), err
	})
	r.budget.Limit = (64 << 10) + 16
	scope, req, owner := failureRequest(t, r)
	failureAwait(t, owner.done, "setup failure and sole-owner reap")
	got, fin, abort, terminal := failureCollect(t, r, scope, req)
	if string(got[0]) != "12345678901234567890" || !fin[0] || abort[0] != "" {
		t.Fatalf("stdout must drain the killed child's pipe to its true EOF: %q fin=%v abort=%q", got[0], fin[0], abort[0])
	}
	if len(got[1]) != 0 || fin[1] || abort[1] == "" || !strings.Contains(abort[1], "output requires unused replay log") {
		t.Fatalf("stderr must be aborted, not FIN: bytes=%d fin=%v abort=%q", len(got[1]), fin[1], abort[1])
	}
	if terminal.Signal != 9 || terminal.ExitCode != -1 || terminal.Error == "" || terminal.TerminalID == "" {
		t.Fatalf("setup failure lost its real terminal: %+v", terminal)
	}
	r.mu.Lock()
	stderrFIN := owner.logs[2].Snapshot().FIN
	r.mu.Unlock()
	if stderrFIN {
		t.Fatal("aborted stream was given a fabricated FIN")
	}
	// A FIN claim on the aborted stream is refused; so is settling without
	// echoing stdout's true EOF.
	req.CallID++
	req.Op, req.TerminalID = "terminal-ack", terminal.TerminalID
	req.StdoutACK, req.StdoutFIN, req.StderrACK, req.StderrFIN = 20, true, 0, true
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, workload.ErrReplayOffset) {
		t.Fatalf("FIN claim on aborted stderr accepted: %v", err)
	}
	req.CallID++
	req.StdoutFIN, req.StderrFIN = false, false
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, procrequest.ErrConflict) {
		t.Fatalf("settled without echoing stdout's true EOF: %v", err)
	}
	if still, err := r.registry.Lookup(scope, *req.Handle); err != nil || still.Phase != "terminal" {
		t.Fatalf("refused settlements retired responsibility: phase=%q err=%v", still.Phase, err)
	}
	req.CallID++
	req.StdoutFIN, req.StderrFIN = true, false
	for i := 0; i < 2; i++ {
		settled, err := r.Handle(scope, req, "failure-inbox")
		if err != nil || settled.Phase != "settled" || settled.TerminalID != terminal.TerminalID {
			t.Fatalf("terminal-ack (attempt %d): %+v, %v", i, settled, err)
		}
		req.CallID++
	}
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 || len(r.cfg.Manager.Snapshot()) != 0 {
		t.Fatalf("settled task retained owners: tasks=%d children=%d", retained, len(r.cfg.Manager.Snapshot()))
	}
	if _, err := owner.logs[1].Read(0, nil); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("settled task retained its stdout log: %v", err)
	}
	failureProbeBudget(t, r)
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T6
func TestRuntimeReadErrorAbortsStreamAtFinalOffsetAndSettles(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	const total = 40960
	r := failureRuntime(t, 64<<10, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		childSpec := *req
		childSpec.Argv = []string{"/bin/sh", "-c", "head -c 40960 /dev/zero | tr '\\0' a; exit 23"}
		stdio, err := failureStartPTYStdoutChild(manager, pid, &childSpec)
		return stdio, "", err
	})
	r.budget.Limit = 3 * (64 << 10)
	scope, req, owner := failureRequest(t, r)
	failureAwait(t, owner.done, "child exit and kernel EIO on the last slave")
	r.mu.Lock()
	state := owner.output[0].Snapshot()
	r.mu.Unlock()
	if state.ReadError == "" || state.Replay.FIN || state.Replay.End != total {
		t.Fatalf("pump did not record the EIO after the whole tail: %+v", state)
	}
	// The first batch is short of the end and must carry neither marker; the
	// abort travels only with the batch that reaches End.
	req.CallID++
	req.Op = "resume"
	first, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || first.Stdout == nil || first.Stdout.Offset != 0 || len(first.Stdout.Data) != 32<<10 || first.Stdout.FIN || first.Stdout.Error != "" ||
		first.Stderr == nil || !first.Stderr.FIN || first.Stderr.Error != "" || first.ExitCode != 23 {
		t.Fatalf("non-final batch carries an end marker or wrong terminal: %+v err=%v", first, err)
	}
	got, fin, abort, terminal := failureCollect(t, r, scope, req)
	if len(got[0]) != total || fin[0] || !strings.Contains(abort[0], "input/output error") {
		t.Fatalf("stdout abort at final offset: bytes=%d fin=%v abort=%q", len(got[0]), fin[0], abort[0])
	}
	if !fin[1] || abort[1] != "" || terminal.ExitCode != 23 {
		t.Fatalf("stderr/terminal: fin=%v abort=%q terminal=%+v", fin[1], abort[1], terminal)
	}
	req.CallID++
	req.Op, req.TerminalID = "terminal-ack", terminal.TerminalID
	req.StdoutACK, req.StdoutFIN, req.StderrACK, req.StderrFIN = 32<<10, false, 0, true
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, procrequest.ErrConflict) {
		t.Fatalf("settled before the abort offset was acknowledged: %v", err)
	}
	req.CallID++
	req.StdoutACK, req.StdoutFIN = total, true
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, workload.ErrReplayOffset) {
		t.Fatalf("FIN claim on aborted stdout accepted: %v", err)
	}
	req.CallID++
	req.StdoutFIN = false
	settled, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || settled.Phase != "settled" {
		t.Fatalf("terminal-ack at the abort offset: %+v, %v", settled, err)
	}
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 || len(r.cfg.Manager.Snapshot()) != 0 {
		t.Fatalf("settled task retained owners: tasks=%d children=%d", retained, len(r.cfg.Manager.Snapshot()))
	}
	failureProbeBudget(t, r)
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T7
func TestRuntimeCloseCanceledStartupReclaimsLateSuccess(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	entered, release := make(chan struct{}), make(chan struct{})
	var ospid atomic.Int32
	r := failureRuntime(t, 64, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		close(entered)
		<-release
		stdio, err := failureStartChild(manager, pid, req)
		if err == nil {
			ospid.Store(int32(manager.Snapshot()[0].OSPID))
		}
		return stdio, "", err
	})
	r.budget.Limit = (64 << 10) + 128
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	_, _, owner := failureRequest(t, r)
	failureAwait(t, entered, "start hook")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close during blocked startup = %v", err)
	}
	close(release)
	failureAwait(t, owner.done, "late successful startup under a closed runtime")
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 {
		t.Errorf("closed runtime retained %d late-started owner(s)", retained)
	}
	if pid := ospid.Load(); pid == 0 {
		t.Fatal("late startup never produced a real child")
	} else if err := syscall.Kill(int(pid), 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("late-started child outlived the closed runtime: %v", err)
	}
	if children := r.cfg.Manager.Snapshot(); len(children) != 0 {
		t.Errorf("manager retained child: %+v", children)
	}
	for i, log := range owner.logs[1:] {
		if _, err := log.Read(0, nil); !errors.Is(err, os.ErrClosed) {
			t.Errorf("closed runtime retained output replay %d: Read = %v", i, err)
		}
	}
	failureProbeBudget(t, r)
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T8
//
// The defect this guards (a blocking select racing an expired context against
// an already-closed done channel) fails about half the time per attempt, so a
// single attempt would kill the mutation only with a coin flip. Twenty
// attempts inside one run make a surviving mutation a one-in-a-million event
// even under -count=1. origin: S0 internal review (gates verifier, missed).
func TestRuntimeCloseRetiresUnsettledTerminalWithoutWaiting(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for attempt := 0; attempt < 20; attempt++ {
		manager := &workload.Manager{}
		r, err := New(Config{Instance: "failureInstance", Manager: manager, ReplayCapacity: 64,
			Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
				childSpec := *req
				childSpec.Argv = []string{"/bin/sh", "-c", "printf out; exit 23"}
				stdio, err := failureStartChild(manager, pid, &childSpec)
				return stdio, "", err
			},
			Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
		if err != nil {
			t.Fatal(err)
		}
		r.budget.Limit = (64 << 10) + 128
		_, _, owner := failureRequest(t, r)
		failureAwait(t, owner.done, "terminal without settlement")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// A published task needs no waiting: an expired context must not be
		// allowed to race the done channel and leave the task behind.
		if err := r.Close(ctx); err != nil {
			t.Fatalf("attempt %d: Close with an expired context left published work: %v", attempt, err)
		}
		r.mu.Lock()
		retained := len(r.tasks)
		r.mu.Unlock()
		if retained != 0 {
			t.Fatalf("attempt %d: closed runtime retained %d unsettled terminal(s)", attempt, retained)
		}
		for i, log := range owner.logs[1:] {
			if _, err := log.Read(0, nil); !errors.Is(err, os.ErrClosed) {
				t.Errorf("attempt %d: output replay %d not retired: %v", attempt, i, err)
			}
		}
		failureProbeBudget(t, r)
	}
}

// origin: S0 internal review C-1 / G2 — a Start failure text the registry
// would refuse (invalid UTF-8, or longer than 4096 bytes) must still become a
// collectable terminal; before boundedError covered both halves the terminal
// was silently dropped and the task sat in "starting" holding its replays.
func TestRuntimeStartFailureTextIsAlwaysAcceptedAsTerminal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, tc := range []struct {
		name string
		text string
	}{
		{"invalid_utf8", "fork/exec /bin/\xff\xfe/tool: permission denied"},
		{"oversize", strings.Repeat("x", 5000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var exits atomic.Int32
			manager := &workload.Manager{}
			r, err := New(Config{Instance: "failureInstance", Manager: manager, ReplayCapacity: 64,
				Start: func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
					return nil, "", errors.New(tc.text)
				},
				Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) { exits.Add(1) }})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := r.Close(ctx); err != nil {
					t.Errorf("runtime cleanup: %v", err)
				}
			})
			r.budget.Limit = (64 << 10) + 128
			scope, req, owner := failureRequest(t, r)
			failureAwait(t, owner.done, "start failure")
			req.CallID++
			req.Op = "resume"
			reply, err := r.Handle(scope, req, "failure-inbox")
			if err != nil || reply.Phase != "terminal" || reply.TerminalID == "" || reply.Error == "" || exits.Load() != 1 {
				t.Fatalf("start failure did not become a terminal: %+v err=%v exits=%d", reply, err, exits.Load())
			}
			if len(reply.Error) > 4096 || !utf8.ValidString(reply.Error) {
				t.Fatalf("terminal error not bounded: len=%d valid=%v", len(reply.Error), utf8.ValidString(reply.Error))
			}
			req.CallID++
			req.Op, req.TerminalID, req.StdoutFIN, req.StderrFIN = "terminal-ack", reply.TerminalID, true, true
			if settled, err := r.Handle(scope, req, "failure-inbox"); err != nil || settled.Phase != "settled" {
				t.Fatalf("settle start failure: %+v, %v", settled, err)
			}
			failureProbeBudget(t, r)
		})
	}
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T9
func TestRuntimeConcurrentCloseRetiresOnce(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	r := failureRuntime(t, 8, func(manager *workload.Manager, pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		childSpec := *req
		childSpec.Argv = []string{"/bin/sh", "-c", "printf 12345678901234567890; exec sleep 30"}
		stdio, err := failureStartChild(manager, pid, &childSpec)
		return stdio, "", err
	})
	r.budget.Limit = (64 << 10) + 16
	_, _, owner := failureRequest(t, r)
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		full := owner.output[0] != nil && owner.logs[1].Snapshot().End == 8
		r.mu.Unlock()
		if full {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stdout never reached real replay backpressure")
		}
		time.Sleep(time.Millisecond)
	}
	children := r.cfg.Manager.Snapshot()
	if len(children) != 1 {
		t.Fatalf("expected one adopted child: %+v", children)
	}
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs <- r.Close(ctx)
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Close: %v", err)
		}
	}
	if err := syscall.Kill(children[0].OSPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("original child remains after concurrent Close: %v", err)
	}
	if rows := r.cfg.Manager.Snapshot(); len(rows) != 0 {
		t.Errorf("manager retained child: %+v", rows)
	}
	r.mu.Lock()
	retained := len(r.tasks)
	r.mu.Unlock()
	if retained != 0 {
		t.Errorf("task remains after concurrent Close: %d", retained)
	}
	failureProbeBudget(t, r)
}

// exerciseRuntimeSetupFailure runs the post-start setup failure shape to
// settlement and then closes the runtime, so the repeated-execution resource
// guard also covers the aborted-stream retirement path.
func exerciseRuntimeSetupFailure(t *testing.T) {
	t.Helper()
	manager := &workload.Manager{}
	r, err := New(Config{Instance: "failureInstance", Manager: manager, ReplayCapacity: 8,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := failureStartChildWithOutput(t, manager, pid, req)
			return stdio, strings.Repeat("w", 16), err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	r.budget.Limit = (64 << 10) + 16
	scope, req, owner := failureRequest(t, r)
	failureAwait(t, owner.done, "setup failure")
	got, fin, abort, terminal := failureCollect(t, r, scope, req)
	if len(got[0]) != 20 || !fin[0] || abort[1] == "" || fin[1] {
		t.Fatalf("setup failure cycle: stdout=%d fin=%v stderr abort=%q", len(got[0]), fin, abort[1])
	}
	req.CallID++
	req.Op, req.TerminalID = "terminal-ack", terminal.TerminalID
	req.StdoutACK, req.StdoutFIN, req.StderrACK, req.StderrFIN = 20, true, 0, false
	if settled, err := r.Handle(scope, req, "failure-inbox"); err != nil || settled.Phase != "settled" {
		t.Fatalf("settle setup failure: %+v, %v", settled, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if rows := manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("cycle retained %d process owners", len(rows))
	}
}

// origin: hot-update S1 internal review E5(b) — finish's non-frozen refusal
// branch. When the registry refuses the terminal for any reason but a freeze
// (here: a conflicting terminal was recorded under the handle first), the
// child's result is still delivered to the broker (Exited fires with the
// real code), the process table is cleared, the task is marked lostTerminal
// and every later attach reports ErrTerminalUnrecorded instead of hanging.
// Mutation: skipping Exited/Forget on the refusal, or dropping lostTerminal,
// fails here.
func TestRuntimeFinishDeliversExitWhenCompleteFails(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	manager := &workload.Manager{}
	exited := make(chan int, 1)
	warned := make(chan error, 1)
	r, err := New(Config{Instance: "lostTerminalInstance", Manager: manager, ReplayCapacity: 64 << 10,
		Clock: func() (int64, error) { return clock.Load(), nil },
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := failureStartChild(manager, pid, req)
			return stdio, "", err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {},
		Exited:  func(_ string, code int) { exited <- code },
		Warn: func(_ string, err error) {
			select {
			case warned <- err:
			default:
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Close(ctx)
	})
	scope := clockedScope("lostTerminalSession")
	req, owner := clockedCreate(t, r, scope, "lostTerminal", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, req)
	// A foreign terminal lands on the record first: the runtime's own
	// Complete will now be refused with ErrConflict (not a freeze).
	foreign := procrequest.Terminal{ID: "01foreignterminalxxxxxxxxxx", ExitCode: 99}
	if err := r.registry.Complete(*req.Handle, owner.pid, foreign); err != nil {
		t.Fatalf("recording the conflicting terminal: %v", err)
	}
	if err := manager.Signal(owner.pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	failureAwait(t, owner.done, "startup owner after the refused terminal")
	select {
	case code := <-exited:
		if code != -1 {
			t.Fatalf("Exited delivered %d, want -1 (signal death)", code)
		}
	default:
		t.Fatal("the refused terminal swallowed the Exited event: the broker row would stay RUNNING forever")
	}
	select {
	case err := <-warned:
		if !errors.Is(err, procrequest.ErrConflict) {
			t.Fatalf("Warn carried %v, want the registry's ErrConflict", err)
		}
	default:
		t.Fatal("the refused terminal was not reported through Warn")
	}
	if rows := manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("process table not cleared after the refused terminal: %+v", rows)
	}
	r.mu.Lock()
	lost := owner.lostTerminal
	r.mu.Unlock()
	if !lost {
		t.Fatal("task not marked lostTerminal")
	}
	req.CallID++
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, ErrTerminalUnrecorded) {
		t.Fatalf("attach after a refused terminal: %v, want ErrTerminalUnrecorded", err)
	}
}
