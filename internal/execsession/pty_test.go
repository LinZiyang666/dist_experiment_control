//go:build linux

// Real PTY children, Manager adoption, actor pumps and replay files. Faults
// target the kernel write or replay boundary; no broker or network is involved.
package execsession

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

func requirePTYDependency(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type ptyRuntimeFixture struct {
	r            *Runtime
	cfg          Config
	manager      workload.Manager
	scope        procrequest.Scope
	starts       atomic.Int32
	exits        atomic.Int32
	clock        atomic.Int64
	clockFault   atomic.Bool
	beforeStart  func()
	setupFailure bool
}

func newPTYRuntime(t *testing.T, budget int64) *ptyRuntimeFixture {
	t.Helper()
	dir := t.TempDir()

	f := &ptyRuntimeFixture{scope: runtimeScope()}
	f.clock.Store(int64(time.Second))
	f.cfg = Config{Instance: "ptyRuntime", Manager: &f.manager, ReplayCapacity: workload.DefaultReplayCapacity, ReplayBudget: budget,
		Clock: func() (int64, error) {
			if f.clockFault.Load() {
				return 0, errors.New("injected boot clock failure")
			}
			return f.clock.Load(), nil
		},
		Start: func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
			return nil, "", errors.New("unexpected exec spawn")
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) { f.exits.Add(1) },
		PTYStarted: func(string, proto.RunReq, workload.Identity) {}}
	f.cfg.StartPTY = func(pid string, spec *proto.RunReq) (*pty.Session, string, error) {
		f.starts.Add(1)
		if f.beforeStart != nil {
			f.beforeStart()
		}
		session, err := pty.Allocate(80, 24)
		if err != nil {
			return nil, "", err
		}
		if err = session.Start(spec.Argv, ptyChildEnvironment(), dir, spec.Argv[0]); err != nil {
			_ = session.Close()
			return nil, "", err
		}
		group := session.OSPID()
		t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
		id, err := ptyChildIdentity(pid, session.OSPID())
		if err != nil {
			session.KillAndWaitAfterAbandonedStart()
			_ = session.Close()
			return nil, "", err
		}
		if err = session.AdoptDraining(&f.manager, id); err != nil {
			session.KillAndWaitAfterAbandonedStart()
			_ = session.Close()
			return nil, "", err
		}
		if f.setupFailure {
			_ = session.Close()
		}
		return session, "", nil
	}
	r, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *ptyRuntimeFixture) close(t *testing.T) {
	t.Helper()
	if f.r == nil {
		return
	}
	f.manager.Thaw()
	if f.r.Frozen() {
		_ = f.r.Thaw()
	}
	// ctx-root: test fixture teardown must outlive the test's canceled context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.r.Close(ctx); err != nil {
		t.Errorf("PTY teardown: %v", err)
	}
}

func ptyRequest(t *testing.T, f *ptyRuntimeFixture, id string, argv []string) proto.ProcessResumeReq {
	t.Helper()
	probe, err := f.r.Handle(f.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", Kind: proto.ProcessKindPTY, ActorFP: f.scope.ActorFP, ClientLineage: f.scope.ClientLineage}, "pty-inbox")
	requirePTYDependency(t, err)
	if probe.Lease == nil || probe.PTYVersion != proto.PTYVersion {
		t.Fatal("PTY capability was not advertised")
	}
	return proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2, Op: "create", Kind: proto.ProcessKindPTY, ActorFP: f.scope.ActorFP, Handle: &proto.ProcessHandle{Lease: *probe.Lease, RequestID: id}, AttachEpoch: 1, Run: &proto.RunReq{Argv: argv}}
}

func ptyCreate(t *testing.T, f *ptyRuntimeFixture, id string, argv []string) (proto.ProcessResumeReq, *task) {
	t.Helper()
	q := ptyRequest(t, f, id, argv)
	reply, err := f.r.Handle(f.scope, q, "pty-inbox")
	requirePTYDependency(t, err)
	f.r.mu.Lock()
	owner := f.r.tasks[reply.PID]
	f.r.mu.Unlock()
	if owner == nil {
		t.Fatal("PTY create retained no owner")
	}
	q.Op, q.Run = "resume", nil
	return q, owner
}

func ptyStatus(t *testing.T, f *ptyRuntimeFixture, q proto.ProcessResumeReq, predicate func(proto.ProcessResumeReply) bool) proto.ProcessResumeReply {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		q.CallID++
		reply, err := f.r.Handle(f.scope, q, "pty-inbox")
		// Revoked live tasks refuse status until the sole observer records their
		// real terminal. Still require convergence and the full terminal receipt.
		if !errors.Is(err, ErrRevoked) {
			requirePTYDependency(t, err)
		}
		if strings.Contains(reply.Error, "not implemented (S4 P") {
			t.Fatalf("PTY status returned an unimplemented dependency: %s", reply.Error)
		}
		if err == nil && predicate(reply) {
			return reply
		}
		if time.Now().After(deadline) {
			t.Fatalf("PTY state did not converge: phase=%s terminal=%s input=%d/%d error=%q", reply.Phase, reply.TerminalID, reply.InputACK, reply.InputAccepted, reply.Error)
		}
		time.Sleep(time.Millisecond)
	}
}

// origin: hot-update S4 plan §5.1 / T25
func TestPTYSpawnBudgetAndFailureOwnership(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("capacity-before-start", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			f := newPTYRuntime(t, upgradebudget.PTYInputBytes+workload.DefaultReplayCapacity-1)
			q := ptyRequest(t, f, fmt.Sprintf("budget-%d", round), []string{"/bin/true"})
			_, err := f.r.Handle(f.scope, q, "pty-inbox")
			if !errors.Is(err, procrequest.ErrCapacity) || f.starts.Load() != 0 || f.r.ReplayBudgetUsed() != 0 {
				t.Fatalf("capacity refusal after spawn or leaked reservation: err=%v starts=%d budget=%d", err, f.starts.Load(), f.r.ReplayBudgetUsed())
			}
			if _, err := f.r.registry.Lookup(f.scope, *q.Handle); !errors.Is(err, procrequest.ErrUnknown) {
				t.Fatalf("budget refusal reserved a handle: %v", err)
			}
			f.close(t)
		}
		testharness.AssertNoGoroutineLeak(t, "PTY capacity", beforeG)
		testharness.AssertNoFDLeak(t, "PTY capacity", beforeFD, 0)
	})
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprintf("setup-failure-%t", failure), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newPTYRuntime(t, upgradebudget.PTYInputBytes+workload.DefaultReplayCapacity)
			f.setupFailure = failure
			q := ptyRequest(t, f, "one-spawn", []string{"/bin/sh", "-c", "printf complete; exit 0"})
			originalSpec := q.Run
			first, err := f.r.Handle(f.scope, q, "pty-inbox")
			requirePTYDependency(t, err)
			if f.r.ReplayBudgetUsed() != upgradebudget.PTYInputBytes+workload.DefaultReplayCapacity {
				t.Fatal("child was admitted without both replay reservations")
			}
			for i := 0; i < 3; i++ {
				q.CallID++
				duplicate, err := f.r.Handle(f.scope, q, "pty-inbox")
				requirePTYDependency(t, err)
				if duplicate.PID != first.PID {
					t.Fatal("duplicate changed child identity")
				}
			}
			q.Op, q.Run = "resume", nil
			terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
				return p.Phase == "terminal" && p.Stdout != nil && (p.Stdout.FIN || p.Stdout.Error != "")
			})
			if f.starts.Load() != 1 || f.exits.Load() != 1 || failure && terminal.Error == "" {
				t.Fatalf("setup failure lost ownership: starts=%d exits=%d error=%q", f.starts.Load(), f.exits.Load(), terminal.Error)
			}
			// A failed adopted child stays owned across an image and another
			// duplicate create, after cleanup has actually completed.
			ptyRoundTrip(t, f)
			duplicate := q
			duplicate.Op, duplicate.Run = "create", originalSpec
			if got, err := f.r.Handle(f.scope, duplicate, "pty-inbox"); err != nil || got.PID != first.PID || f.starts.Load() != 1 {
				t.Fatalf("terminal duplicate respawned or lost ownership: pid=%s starts=%d err=%v", got.PID, f.starts.Load(), err)
			}
			q.Op, q.TerminalID = "terminal-ack", terminal.TerminalID
			q.StdoutACK = terminal.Stdout.Offset + uint64(len(terminal.Stdout.Data))
			q.StdoutFIN, q.StderrFIN = terminal.Stdout.FIN, true
			settled, err := f.r.Handle(f.scope, q, "pty-inbox")
			if err != nil || settled.Phase != "settled" {
				t.Fatalf("settle: %s %v", settled.Phase, err)
			}
			if f.r.ReplayBudgetUsed() != 0 || len(f.manager.Snapshot()) != 0 {
				t.Fatal("settlement leaked budget or child")
			}
		})
	}
}

const ptyAbortMode = "TETHER_TEST_PTY_ABORT_MODE"

// origin: hot-update S4 plan §3.2 / T12
func TestPtyInputAbortStillSettles(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if mode := os.Getenv(ptyAbortMode); mode != "" {
		exercisePTYInputAbort(t, mode)
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exercisePTYInputAbort(t, mode)
		}
		testharness.AssertNoGoroutineLeak(t, "PTY input abort", beforeG)
		testharness.AssertNoFDLeak(t, "PTY input abort", beforeFD, 0)
		return
	}
	for _, mode := range []string{"write-eio", "replay-prefix"} {
		t.Run(mode, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			// RLIMIT_FSIZE is process-wide. Isolate the real partial pwrite fault in
			// this test binary, never fork go test or alter the parent's limits.
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPtyInputAbortStillSettles$", "-test.count=1", "-test.v")
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
			cmd.Dir = dir
			cmd.Env = append(ptyChildEnvironment(), ptyAbortMode+"="+mode)
			cmd.Env = append(cmd.Env, "TMPDIR="+dir)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("abort helper: %v\n%s", err, output)
			}
			if !bytes.Contains(output, []byte("PTY_ABORT_SETTLED")) {
				t.Fatalf("helper produced no settlement receipt: %s", output)
			}
		})
	}
}

// ptyWriteFault borrows the real master for output, but executes each injected
// input write against the unmapped null page of /proc/self/mem. Linux returns
// a real EIO from pwrite; no simulated successful bytes enter actor accounting.
type ptyWriteFault struct {
	pty.Master
	fault *os.File
}

func (m *ptyWriteFault) Write(data []byte) (int, error) { return m.fault.WriteAt(data, 0) }

func exercisePTYInputAbort(t *testing.T, mode string) {
	f := newPTYRuntime(t, 0)
	q, owner := ptyCreate(t, f, "abort", []string{"/bin/sh", "-c", "stty raw -echo; printf output-complete; read line; exit 0"})
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
		return p.Stdout != nil && bytes.Contains(p.Stdout.Data, []byte("output-complete"))
	})
	imageFreeze(t, f.r)
	state := *owner.frozen.pty
	if mode == "replay-prefix" {
		// The first physical ring fragment succeeds; the wrapped fragment
		// hits a real kernel EIO. Accepted must include only those two bytes.
		state.Input.Base = uint64(upgradebudget.PTYInputBytes - 2)
		state.Input.End = state.Input.Base
	}
	files, err := owner.pty.Detach()
	requirePTYDependency(t, err)
	var row workload.Record
	row, _ = f.manager.Lookup(owner.pid)
	master, err := pty.RestoreMaster(files[filePTYMaster], state.Identity, row)
	requirePTYDependency(t, err)
	input, err := workload.RestoreReplay(&f.r.budget, state.Input, files[filePTYInput])
	if err != nil {
		t.Fatal(err)
	}
	output, err := workload.RestoreReplay(&f.r.budget, state.Output, files[filePTYOutput])
	if err != nil {
		t.Fatal(err)
	}
	cfg := ptysession.Config{Manager: &f.manager, PID: owner.pid, Master: master, Identity: state.Identity, Input: input, Output: output, Clock: f.cfg.Clock}
	if mode == "write-eio" {
		fault, err := os.OpenFile("/proc/self/mem", os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = fault.Close() }()
		cfg.Master = &ptyWriteFault{Master: master, fault: fault}
	}
	actor, err := ptysession.Import(cfg, state)
	requirePTYDependency(t, err)
	owner.pty, owner.logs[0], owner.logs[1] = actor, input, output
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	restoreReplay := func() {}
	if mode == "replay-prefix" {
		restoreReplay = ptyReplayPrefixFault(t, files[filePTYInput])
		defer restoreReplay()
	}
	inputReq := q
	inputReq.Op = "input"
	inputReq.InputSeq = state.Input.Base
	inputReq.Input = []byte("pending\n")
	_, err = f.r.Handle(f.scope, inputReq, "pty-inbox")
	restoreReplay()
	if err != nil {
		t.Fatalf("input failure lost status: %v", err)
	}
	aborted := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.InputEnded })
	if aborted.InputACK >= aborted.InputAccepted || aborted.InputError == "" {
		t.Fatalf("abort fabricated applied bytes or FIN: %d/%d %q", aborted.InputACK, aborted.InputAccepted, aborted.InputError)
	}
	if mode == "replay-prefix" && aborted.InputAccepted != state.Input.End+2 {
		t.Fatalf("partial pwrite retained %d bytes, want exactly 2", aborted.InputAccepted-state.Input.End)
	}
	// Let the real child exit zero through its original master; actor input is
	// already closed. This bypass is test-owned orchestration, never ACK data.
	// The actor has permanently stopped its writer; this test-owned write must
	// explicitly clear that borrower's wake deadline before orchestrating exit.
	if err := master.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := master.Write([]byte("done\n")); err != nil {
		t.Fatal(err)
	}
	terminal := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" && p.Stdout != nil && p.Stdout.FIN })
	if terminal.ExitCode != 0 || !bytes.Equal(terminal.Stdout.Data, []byte("output-complete")) {
		t.Fatalf("abort lost output/real rc: %+v", terminal)
	}
	ptyRoundTrip(t, f)
	terminal = ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	if terminal.InputError != aborted.InputError || terminal.InputACK != aborted.InputACK || terminal.InputAccepted != aborted.InputAccepted {
		t.Fatal("image erased input abort responsibility")
	}
	q.Op, q.TerminalID = "terminal-ack", terminal.TerminalID
	q.StdoutACK = uint64(len("output-complete"))
	q.StdoutFIN, q.StderrFIN = true, true
	settled, err := f.r.Handle(f.scope, q, "pty-inbox")
	if err != nil || settled.Phase != "settled" || settled.InputError == "" {
		t.Fatalf("A<Q abort did not settle with diagnosis: %s %q %v", settled.Phase, settled.InputError, err)
	}
	// Exit-code 74 presentation belongs to P10/P12; this runtime receipt keeps
	// the real rc0 and all diagnostic fields that must force that presentation.
	f.close(t)
	t.Log("PTY_ABORT_SETTLED")
}

func ptyChildEnvironment() []string {
	env := runtimeChildEnvironment()
	out := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, ptyAbortMode+"=") {
			out = append(out, entry)
		}
	}
	return out
}

// The adapter contract carries real boot/ticks metadata at adoption, just as
// agent.execChildIdentity does. A zero-valued fixture would bypass the kernel
// identity checks this package is meant to preserve across import.
func ptyChildIdentity(pid string, ospid int) (workload.Identity, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return workload.Identity{}, err
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(ospid) + "/stat")
	if err != nil {
		return workload.Identity{}, err
	}
	closing := strings.LastIndexByte(string(stat), ')')
	if closing < 0 {
		return workload.Identity{}, errors.New("missing process comm boundary")
	}
	fields := strings.Fields(string(stat[closing+1:]))
	if len(fields) < 20 {
		return workload.Identity{}, errors.New("short process identity")
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return workload.Identity{}, err
	}
	return workload.Identity{ID: pid, OSPID: ospid, BootID: strings.TrimSpace(string(boot)), StartTimeTicks: ticks, StartedAt: time.Now().UTC()}, nil
}

// origin: hot-update S4 plan §3.2 / T12 — fault the second physical ring write.
// /proc/self/mem writes the first fragment into a MAP_SHARED alias of the real
// replay's last page. The wrap to address zero fails with kernel EIO. This
// avoids Go 1.26 File.WriteAt losing a short pwrite's count when its internal
// retry fails under RLIMIT_FSIZE; both the retained prefix and the error here
// are real syscalls, and the original sealed replay is restored before export.
func ptyReplayPrefixFault(t *testing.T, file *os.File) func() {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	target, backup := -1, -1
	var inner error
	if err := raw.Control(func(fd uintptr) {
		target = int(fd)
		backup, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3)
	}); err != nil || inner != nil {
		t.Fatalf("replay fault duplicate: %v %v", err, inner)
	}
	page := uintptr(os.Getpagesize())
	base := uintptr(upgradebudget.PTYInputBytes) - page
	addr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, base, page, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_FIXED_NOREPLACE, uintptr(backup), base)
	if errno != 0 {
		_ = unix.Close(backup)
		t.Fatalf("map replay fault page: %v", errno)
	}
	if addr != base {
		t.Fatal("kernel ignored fixed no-replace mapping")
	}
	memory, err := os.OpenFile("/proc/self/mem", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = memory.Close() }()
	memRaw, err := memory.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := memRaw.Control(func(fd uintptr) { inner = unix.Dup3(int(fd), target, unix.O_CLOEXEC) }); err != nil || inner != nil {
		t.Fatalf("install replay fault: %v %v", err, inner)
	}
	restored := false
	return func() {
		if restored {
			return
		}
		restored = true
		if err := unix.Dup3(backup, target, unix.O_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		_ = unix.Close(backup)
		if _, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, addr, page, 0); errno != 0 {
			t.Fatal(errno)
		}
	}
}
