//go:build linux

// image_test.go — the phase machine of a dispatched image, driven in-process
// from a real cut (a live child, real pipes and replay memfds duplicated into
// a DupSource) so every transition runs the exact code the exec path runs.
// Real: exec.Cmd children, execsession runtime, agentimage Freeze/Import.
// Stubbed: the dispatch itself (FromCut instead of handoff.Restore); the
// exec path is boot_exec_test.go. origin: hot-update S3 plan §1.3 / §7.
package agentboot

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

const imageTestBudget = 20 * time.Second

// origin: hot-update S4 plan §4.2 / T23 — a partial activation has resource
// responsibility but no completion proof. These phase guards are independent
// of the actor fault injection exercised by the real-PTY test.
func TestImagePartialActivationKeepsOwnershipUntilCurrentCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	assertBootActivationBoundary(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for range 5 {
		f := newCutFixture(t, "partial-phase-owner")
		ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
		if err := f.rt.Close(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		imp := &agentimage.Imported{Runtime: f.rt, Manager: f.manager}
		img := &Image{phase: PhaseActive, imported: imp}
		img.mu.Lock()
		img.activating = true
		img.mu.Unlock()
		if _, _, _, err := img.Activate(); err == nil || !strings.HasPrefix(err.Error(), "agentboot: activate in phase") {
			t.Fatal("partial activation permitted an overlapping retry")
		}
		img.mu.Lock()
		img.activating = false
		img.mu.Unlock()
		img.MarkReady()
		if img.Phase() != PhaseActive {
			t.Fatal("partial activation became ready without a completion proof")
		}
		if prepared, err := img.Repack(); err == nil || prepared != nil {
			t.Fatal("active responsibility repacked a pristine bundle")
		}
		img.Abandon(nil, CrashWindowNoRollbackTarget, "partial activation")
		if img.Phase() != PhaseActive || img.Imported() != imp {
			t.Fatal("bootstrap abandoned an active resource owner")
		}
		img.mu.Lock()
		img.activated = true
		img.mu.Unlock()
		img.MarkReady()
		if img.Phase() != PhaseReady {
			t.Fatal("completed activation could not become ready")
		}
		img.Abandon(nil, CrashWindowNoRollbackTarget, "ready image")
		if img.Phase() != PhaseReady || img.Imported() != imp {
			t.Fatal("bootstrap abandoned a ready resource owner")
		}
	}
	testharness.AssertNoGoroutineLeak(t, "boot-partial-phase", beforeG)
	testharness.AssertNoFDLeak(t, "boot-partial-phase", beforeFD, 0)
}

// The actor integration test owns the behavioral proof. This structural
// complement remains runnable while P6 is a sentinel, and protects the boot
// phase's placement before the first resource can run. Synthetic controls
// prove it cannot turn green by losing its target during a refactor.
func assertBootActivationBoundary(t *testing.T) {
	t.Helper()
	check := func(source string) bool {
		file, err := parser.ParseFile(token.NewFileSet(), "image.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		var active, thaw token.Pos
		functions, thaws, detaches := 0, 0, 0
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "Activate" {
				continue
			}
			functions++
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.AssignStmt:
					if len(node.Lhs) != 1 || len(node.Rhs) != 1 {
						break
					}
					left, ok := node.Lhs[0].(*ast.SelectorExpr)
					right, rok := node.Rhs[0].(*ast.Ident)
					if ok && rok && left.Sel.Name == "phase" && right.Name == "PhaseActive" && (active == 0 || node.Pos() < active) {
						active = node.Pos()
					}
				case *ast.CallExpr:
					selector, ok := node.Fun.(*ast.SelectorExpr)
					if !ok {
						break
					}
					owner, ok := selector.X.(*ast.Ident)
					if !ok || owner.Name != "imp" {
						break
					}
					if selector.Sel.Name == "Thaw" {
						thaws++
						thaw = node.Pos()
					}
					if selector.Sel.Name == "Detach" {
						detaches++
					}
				}
				return true
			})
		}
		return functions == 1 && thaws == 1 && active != 0 && active < thaw && detaches == 0
	}
	for _, sample := range []struct {
		body string
		want bool
	}{
		{"i.phase = PhaseActive; imp.Thaw()", true},
		{"imp.Thaw(); i.phase = PhaseActive", false},
		{"i.phase = PhaseActive; imp.Thaw(); imp.Detach()", false},
		{"i.phase = PhaseActive", false},
	} {
		if got := check("package sample; func Activate(){" + sample.body + "}"); got != sample.want {
			t.Fatalf("activation-boundary scanner self-check: %q got=%t", sample.body, got)
		}
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("activation scanner has no source location")
	}
	source, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "image.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !check(string(source)) {
		t.Fatal("boot activation must commit PhaseActive before Thaw and cannot Detach on its error path")
	}
}

// cutFixture is one exporter: a runtime over its own process table, with a
// live child, and the receiver's target config over a fresh table.
type cutFixture struct {
	mu      sync.Mutex
	rt      *execsession.Runtime
	manager *workload.Manager
	cfg     execsession.Config
	scope   procrequest.Scope
	calls   uint64
	exited  map[string]int
}

func newCutFixture(t *testing.T, instance string) *cutFixture {
	t.Helper()
	dir := t.TempDir()

	f := &cutFixture{manager: &workload.Manager{}, exited: map[string]int{}}
	digest := sha256.Sum256([]byte("agentboot actor"))
	f.scope = procrequest.Scope{SID: "lab", NID: "node", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "boot-client"}
	f.cfg = execsession.Config{Instance: instance, Manager: f.manager, ReplayCapacity: 256,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			if err != nil {
				return nil, "", err
			}
			cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
			cmd.Dir = dir
			cmd.Env = handoff.CleanEnvironment(os.Environ())
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdio.Bind(cmd)
			if err := cmd.Start(); err != nil {
				stdio.Close()
				return nil, "", err
			}
			group := cmd.Process.Pid
			t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
			stdio.CloseChild()
			if err := f.manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
			return stdio, "", nil
		},
		Started: func(string, proto.ExecReq, workload.Identity) {},
		Exited:  func(pid string, _ int) { f.mu.Lock(); f.exited[pid]++; f.mu.Unlock() },
	}
	rt, err := execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	t.Cleanup(func() {
		f.manager.Thaw()
		_ = f.manager.SignalAll(syscall.SIGKILL)
		ctx, cancel := context.WithTimeout(context.Background(), imageTestBudget)
		defer cancel()
		_ = f.rt.Close(ctx)
	})
	return f
}

func (f *cutFixture) handle(rt *execsession.Runtime, req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	f.mu.Lock()
	f.calls++
	req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, f.calls, f.scope.ActorFP
	f.mu.Unlock()
	return rt.Handle(f.scope, req, "_TINBOX.boot")
}

func (f *cutFixture) create(t *testing.T, argv []string) proto.ProcessHandle {
	t.Helper()
	probe, err := f.handle(f.rt, proto.ProcessResumeReq{Op: "probe", ClientLineage: f.scope.ClientLineage})
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v %v", probe, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "task"}
	spec := proto.ExecReq{Argv: argv}
	reply, err := f.handle(f.rt, proto.ProcessResumeReq{Op: "create", Handle: &h, AttachEpoch: 1, Exec: &spec})
	if err != nil || reply.PID == "" {
		t.Fatalf("create: %+v %v", reply, err)
	}
	deadline := time.Now().Add(imageTestBudget)
	for {
		reply, err := f.handle(f.rt, proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1})
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if reply.Phase == "running" && reply.Stdout != nil && len(reply.Stdout.Data) > 0 {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatalf("never running with output: %+v", reply)
		}
		time.Sleep(time.Millisecond)
	}
}

// drain reads both streams to the end on rt and settles.
func (f *cutFixture) drain(t *testing.T, rt *execsession.Runtime, h proto.ProcessHandle) (string, proto.ProcessResumeReply) {
	t.Helper()
	var out, errOut []byte
	var outFIN, errFIN bool
	deadline := time.Now().Add(imageTestBudget)
	for {
		reply, err := f.handle(rt, proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1,
			StdoutACK: uint64(len(out)), StdoutFIN: outFIN, StderrACK: uint64(len(errOut)), StderrFIN: errFIN})
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if reply.Stdout != nil {
			out = append(out, reply.Stdout.Data...)
			outFIN = reply.Stdout.FIN || reply.Stdout.Error != ""
		}
		if reply.Stderr != nil {
			errOut = append(errOut, reply.Stderr.Data...)
			errFIN = reply.Stderr.FIN || reply.Stderr.Error != ""
		}
		if reply.Phase == "terminal" && outFIN && errFIN {
			ack := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &h, AttachEpoch: 1, TerminalID: reply.TerminalID,
				StdoutACK: uint64(len(out)), StdoutFIN: true, StderrACK: uint64(len(errOut)), StderrFIN: true}
			if settled, err := f.handle(rt, ack); err != nil || settled.Phase != "settled" {
				t.Fatalf("settle: %+v %v", settled, err)
			}
			return string(out), reply
		}
		if time.Now().After(deadline) {
			t.Fatalf("never ended: %+v", reply)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// cut freezes the exporter and wraps the cut as a held in-process image.
func (f *cutFixture) cut(t *testing.T, generation uint64) (*Image, *agentimage.Frozen, *handofftest.DupSource) {
	t.Helper()
	frozen, err := agentimage.Freeze(agentimage.Owners{Runtime: f.rt, Manager: f.manager, Lineage: agentimage.Lineage{Generation: generation}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	_, resources, err := f.rt.Export()
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, resources)
	b := frozen.Bundle()
	return FromCut(b, b.Processes.Records, src, resources), frozen, src
}

func (f *cutFixture) target() agentimage.Target {
	cfg := f.cfg
	cfg.Instance = ""
	cfg.Manager = &workload.Manager{}
	return agentimage.Target{Runtime: cfg}
}

func TestImageHeldToReadyCarriesTheChild(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newCutFixture(t, "boot-instance")
	marker := t.TempDir() + "/go"
	h := f.create(t, []string{"/bin/sh", "-c", "printf before; while [ ! -e " + marker + " ]; do sleep 0.02; done; printf after; exit 23"})
	img, frozen, _ := f.cut(t, 3)
	if img.Phase() != PhaseHeld || img.Generation() != 3 {
		t.Fatalf("fresh cut: phase=%q generation=%d", img.Phase(), img.Generation())
	}
	if _, _, _, err := img.Activate(); err == nil {
		t.Fatal("activate before restore succeeded")
	}
	imp, err := img.Restore(f.target())
	if err != nil {
		t.Fatal(err)
	}
	if img.Phase() != PhaseImported || img.Tasks() != 1 || img.Imported() != imp {
		t.Fatalf("after restore: phase=%q tasks=%d", img.Phase(), img.Tasks())
	}
	if _, err := img.Restore(f.target()); err == nil {
		t.Fatal("second restore succeeded")
	}
	// The exporter's copies are done with: an in-process switch discards them.
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	window, shim, imported, err := img.Activate()
	if err != nil {
		t.Fatal(err)
	}
	if img.Phase() != PhaseActive || window <= 0 || shim < 0 || imported < 0 {
		t.Fatalf("after activate: phase=%q window=%v shim=%v import=%v", img.Phase(), window, shim, imported)
	}
	if _, err := img.Repack(); err == nil {
		t.Fatal("an active image repacked")
	}
	img.MarkReady()
	if img.Phase() != PhaseReady {
		t.Fatalf("phase after MarkReady: %q", img.Phase())
	}
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out, reply := f.drain(t, imp.Runtime, h)
	if out != "beforeafter" || reply.ExitCode != 23 {
		t.Fatalf("child through the new image: out=%q reply=%+v", out, reply)
	}
	ctx, cancel := context.WithTimeout(context.Background(), imageTestBudget)
	defer cancel()
	if err := imp.Runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestImageRefusedRepacksSalvageWithRemainder(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newCutFixture(t, "refuse-instance")
	h := f.create(t, []string{"/bin/sh", "-c", "printf before; exec sleep 30"})
	fdBefore := testharness.FDCount()
	img, frozen, src := f.cut(t, 2)
	b := frozen.Bundle()
	id := "exec-task." + b.Runtime.Tasks[0].PID
	src.Swap(id, "stdout-replay", "stderr-replay") // identity mismatch after some files were taken
	if _, err := img.Restore(f.target()); err == nil {
		t.Fatal("swapped image imported")
	}
	if img.Phase() != PhaseRefused {
		t.Fatalf("phase after refusal: %q", img.Phase())
	}
	if src.Remaining() == 0 {
		t.Fatal("fixture: refusal took every file; the remainder path is not exercised")
	}
	prepared, err := img.Repack()
	if err != nil {
		t.Fatalf("repack of a refused image: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	// Nothing moved: the image can be repacked again, and abandoned cleanly.
	prepared, err = img.Repack()
	if err != nil {
		t.Fatalf("second repack: %v", err)
	}
	_ = prepared.Close()
	img.Abandon(nil, CrashWindowNoRollbackTarget, "test")
	if img.Phase() != PhaseLost || img.CrashWindow() != CrashWindowNoRollbackTarget {
		t.Fatalf("after abandon: %q %q", img.Phase(), img.CrashWindow())
	}
	src.Close()
	// The frozen exporter still owns its one calibration snapshot.
	// Abandon must release every imported business descriptor.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	if got := testharness.FDCount(); got != fdBefore+1 {
		t.Fatalf("descriptors after refusal+abandon: %d vs %d", got, fdBefore+1)
	}
	// The exporter still owns the child: thaw and finish it.
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	if got := testharness.FDCount(); got != fdBefore {
		t.Fatalf("descriptors after exporter thaw: %d vs %d", got, fdBefore)
	}
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	if _, reply := f.drain(t, f.rt, h); reply.Signal != int(syscall.SIGKILL) {
		t.Fatalf("exporter after rollback: %+v", reply)
	}
}

func TestImageImportedRepacksByDetaching(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newCutFixture(t, "detach-instance")
	h := f.create(t, []string{"/bin/sh", "-c", "printf before; exec sleep 30"})
	img, frozen, _ := f.cut(t, 2)
	if _, err := img.Restore(f.target()); err != nil {
		t.Fatal(err)
	}
	prepared, err := img.Repack()
	if err != nil {
		t.Fatalf("repack of an imported image: %v", err)
	}
	_ = prepared.Close()
	if img.Phase() != PhaseRefused || img.Imported() != nil {
		t.Fatalf("after detaching repack: phase=%q", img.Phase())
	}
	if _, _, _, err := img.Activate(); err == nil {
		t.Fatal("a detached image activated")
	}
	img.Abandon(nil, CrashWindowNoRollbackTarget, "test")
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	_ = f.manager.Signal(b.Runtime.Tasks[0].PID, syscall.SIGKILL)
	f.drain(t, f.rt, h)
}

func TestBeginWithoutADispatchIsAnOrdinaryStart(t *testing.T) {
	t.Setenv("_TETHER_HANDOFF", "")
	_ = os.Unsetenv("_TETHER_HANDOFF")
	Reset()
	if img := Begin(nil, "TETHER_INSTANCE_ID"); img != nil {
		t.Fatalf("Begin without a dispatch returned %+v", img.Summary())
	}
	if Current() != nil || Claim() != nil {
		t.Fatal("an ordinary start has a current image")
	}
	// A malformed dispatch is the W1 crash window: lost, logged, continued.
	t.Setenv("_TETHER_HANDOFF", "garbage")
	Reset()
	img := Begin(nil, "TETHER_INSTANCE_ID")
	if img == nil || img.Phase() != PhaseLost || img.CrashWindow() != CrashWindowHandoffRestore {
		t.Fatalf("W1: %+v", img)
	}
	if Claim() != nil {
		t.Fatal("a lost image was claimable")
	}
	if got, err := handoff.Restore(); err != nil || got != nil {
		t.Fatalf("Begin did not consume the dispatch variable: %v %v", got, err)
	}
	if !errors.Is(fmtErr(img), errNotHeld) {
		t.Fatal("restore of a lost image did not refuse")
	}
	Reset()
}

var errNotHeld = errors.New("not held")

func fmtErr(img *Image) error {
	if _, err := img.Restore(agentimage.Target{}); err != nil && strings.Contains(err.Error(), `phase "lost"`) {
		return errNotHeld
	}
	return nil
}
