//go:build linux

package agentboot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §8 T23 — the first actor writes a real queued
// suffix before the second actor's injected activation error. Both imported
// responsibility markers must already be active; recovery cuts the current
// mixed active/frozen owner set, never the original image.
func TestPTYPartialActivationNeverDetachesInitialImage(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exercisePTYPartialActivation(t, round)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "pty-partial-activation", beforeG)
	testharness.AssertNoFDLeak(t, "pty-partial-activation", beforeFD, 0)
}

func exercisePTYPartialActivation(t *testing.T, round int) {
	t.Helper()
	f := newCutFixture(t, fmt.Sprintf("partial-pty-%d", round))
	// Retire the empty exec-only fixture before installing its PTY adapter.
	ctx, cancel := context.WithTimeout(t.Context(), imageTestBudget)
	defer cancel()
	if err := f.rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	f.cfg.StartPTY = func(pid string, req *proto.RunReq) (*pty.Session, string, error) {
		session, err := pty.Allocate(req.Cols, req.Rows)
		if err != nil {
			return nil, "", err
		}
		if err := session.Start(req.Argv, nil, req.Cwd, ""); err != nil {
			_ = session.Close()
			return nil, "", err
		}
		identity, err := activationChildIdentity(pid, session.OSPID())
		if err != nil {
			session.KillAndWaitAfterAbandonedStart()
			_ = session.Close()
			return nil, "", err
		}
		if err := session.AdoptDraining(f.manager, identity); err != nil {
			_ = syscall.Kill(-identity.OSPID, syscall.SIGKILL)
			_, _ = session.Wait()
			_ = session.Close()
			return nil, "", err
		}
		return session, "", nil
	}
	f.cfg.PTYStarted = func(string, proto.RunReq, workload.Identity) {}
	var err error
	f.rt, err = execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	marker, ledger := work+"/read", work+"/bytes"
	first, firstPID := activationCreatePTY(t, f, "first", `stty -echo -icanon min 1 time 0 -onlcr; printf ready
while [ ! -e "$1" ]; do sleep 0.02; done
dd bs=4096 count=32 iflag=fullblock of="$2" 2>/dev/null
printf drained
IFS= read -r middle; printf '<%s>' "$middle"
IFS= read -r next; printf '<%s>' "$next"; exec sleep 30`, marker, ledger)
	second, secondPID := activationCreatePTY(t, f, "second", "stty -echo; printf ready; exec sleep 30")
	for _, handle := range []proto.ProcessHandle{first, second} {
		activationAwait(t, func() bool {
			r, err := activationCall(f, f.rt, handle, proto.ProcessResumeReq{Op: "resume"})
			if err != nil {
				t.Fatal(err)
			}
			return r.Stdout != nil && string(r.Stdout.Data) == "ready"
		}, "both real slaves ready")
	}
	// A large os.File.Write reports its short prefix only when Freeze wakes
	// the syscall. Seed one completed write first, then fill the stopped slave;
	// waiting for a large write's ACK before cutting would deadlock the fixture.
	seed, err := activationCall(f, f.rt, first, proto.ProcessResumeReq{Op: "input", Input: []byte("x")})
	if err != nil || seed.InputAccepted != 1 {
		t.Fatal("seed input was not accepted", err)
	}
	activationAwait(t, func() bool {
		r, err := activationCall(f, f.rt, first, proto.ProcessResumeReq{Op: "resume"})
		if err != nil {
			t.Fatal(err)
		}
		return r.InputACK == 1
	}, "one completed kernel write before the blocked suffix")
	for offset := 1; offset < 128<<10; {
		payload := bytes.Repeat([]byte("x"), min(32<<10, (128<<10)-offset))
		reply, err := activationCall(f, f.rt, first, proto.ProcessResumeReq{Op: "input", InputSeq: uint64(offset), Input: payload})
		if err != nil || reply.InputAccepted != uint64(offset+len(payload)) {
			t.Fatalf("queue input: accepted=%d err=%v", reply.InputAccepted, err)
		}
		offset += len(payload)
	}
	activationAwait(t, func() bool {
		r, err := activationCall(f, f.rt, first, proto.ProcessResumeReq{Op: "resume"})
		if err != nil {
			t.Fatal(err)
		}
		return r.InputACK > 0 && r.InputACK < r.InputAccepted
	}, "blocked slave with a real queued suffix")
	control, err := activationCall(f, f.rt, first, proto.ProcessResumeReq{Op: "resize", ControlSeq: 1, Resize: &proto.PtyResizeEvent{Cols: 80, Rows: 24}})
	if err != nil || control.ControlACK != 1 {
		t.Fatal("fixture has no committed control receipt")
	}
	img, original, src := f.cut(t, 1)
	initial := original.Bundle()
	if len(initial.Runtime.Tasks) != 2 || initial.Runtime.Tasks[0].PID != firstPID {
		t.Fatal("fixture requires first actor before second actor")
	}
	old := activationTaskState(t, initial.Runtime.Tasks[0])
	if old.Input.Base == 0 || old.Input.Base >= old.Input.End || old.ControlSeq != 1 || old.Output.End == 0 {
		t.Fatal("initial cut has no queued suffix, control receipt or output")
	}
	failure := errors.New("injected second actor activation failure")
	target := f.target()
	// This is an in-process switch: share the sole wait owner. A second
	// restored Manager would leave the original reaper and its pidfds alive.
	target.Runtime.Manager = f.manager
	activationCalls := 0
	// P6 owns actor iteration. Its optional PTYActivate hook must expose the
	// real actor; the first callback below activates it and proves
	// real child progress. No P1/P2/P3/P6 implementation is replaced here.
	installPTYActivationFault(t, &target.Runtime, func(pid string, actor *ptysession.Actor) error {
		activationCalls++
		if pid == secondPID {
			return failure
		}
		if pid != firstPID {
			return fmt.Errorf("unexpected PTY activation owner")
		}
		if img.Phase() != PhaseActive {
			return errors.New("boot phase was not active before the first actor")
		}
		if err := actor.Activate(); err != nil {
			return err
		}
		if err := os.WriteFile(marker, nil, 0o600); err != nil {
			return err
		}
		activationAwait(t, func() bool { info, err := os.Stat(ledger); return err == nil && info.Size() == 128<<10 }, "first actor applied queued suffix before the second fails")
		input, err := actor.AcceptInput(128<<10, []byte("middle\n"), false)
		if err != nil || input.Accepted != (128<<10)+7 {
			return fmt.Errorf("first actor did not accept new input: %w", err)
		}
		for _, request := range []ptysession.ControlRequest{{Seq: 2, Op: "resize", Cols: 91, Rows: 37}, {Seq: 3, Op: "keepalive"}} {
			receipt, err := actor.ApplyControl(request)
			if err != nil || receipt.Seq != request.Seq || receipt.Error != "" {
				return fmt.Errorf("first actor did not commit new control: %w", err)
			}
		}
		activationAwait(t, func() bool {
			batch, err := actor.OutputBatch(1, false, 32<<10)
			if err != nil {
				t.Fatal(err)
			}
			return actor.InputStatus().Applied == (128<<10)+7 && bytes.Contains(batch.Data, []byte("drained<middle>"))
		}, "first actor applied new input and ACKed new output before the second fails")
		return nil
	})
	imp, err := img.Restore(target)
	if err != nil {
		t.Fatal(err)
	}
	currentRT, currentManager := imp.Runtime, imp.Manager
	t.Cleanup(func() {
		currentManager.Thaw()
		_ = currentManager.SignalAll(syscall.SIGKILL)
		// ctx-none: test cleanup runs after t.Context cancellation.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), imageTestBudget)
		defer cleanupCancel()
		_ = currentRT.Close(cleanupCtx)
	})
	if err := original.Discard(); err != nil {
		t.Fatal(err)
	}
	window, _, _, err := img.Activate()
	if !errors.Is(err, failure) || activationCalls != 2 {
		t.Fatalf("second actor activation failure not exercised: calls=%d err=%v", activationCalls, err)
	}
	if img.Phase() != PhaseActive || img.Imported() != imp || !imp.Active() || imp.Manager.Frozen() || window != 0 || imp.FreezeWindow() != 0 {
		t.Fatal("partial activation lost responsibility or reported a successful window")
	}
	if _, err := imp.Detach(); err == nil {
		t.Fatal("partially active import detached its original image")
	}
	if _, err := img.Repack(); err == nil {
		t.Fatal("partially active boot image repacked the original bundle")
	}
	img.MarkReady()
	if img.Phase() != PhaseActive {
		t.Fatal("partial activation became control-ready")
	}
	img.Abandon(nil, CrashWindowNoRollbackTarget, "test partial failure")
	if img.Phase() != PhaseActive || img.Imported() != imp {
		t.Fatal("bootstrap cleanup dropped the partial runtime")
	}
	current, err := agentimage.Freeze(agentimage.Owners{Runtime: imp.Runtime, Manager: imp.Manager,
		Lineage: agentimage.Lineage{Generation: initial.Lineage.Generation + 1}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatalf("cut mixed active/frozen actors: %v", err)
	}
	b := current.Bundle()
	advanced := activationTaskState(t, b.Runtime.Tasks[0])
	if b.Lineage.Generation != 2 || advanced.Input.Base != (128<<10)+7 || advanced.Input.End != (128<<10)+7 || advanced.Input.Base <= old.Input.Base || advanced.Output.Base != 1 || advanced.Output.End <= old.Output.End || advanced.ControlSeq != 3 || advanced.ControlDigest == old.ControlDigest || advanced.LastKASeq != 3 {
		t.Fatal("current cut lost applied bytes, current output or the committed control receipt")
	}
	_, resources, err := imp.Runtime.Export()
	if err != nil {
		t.Fatal(err)
	}
	newSource := handofftest.DupResources(t, resources)
	cfg := f.cfg
	cfg.Manager = imp.Manager
	// The recovery uses the current bundle and the shared frozen table.
	recovered, salvage, err := agentimage.Import(b, b.Processes.Records, newSource, agentimage.Target{Runtime: cfg})
	if err != nil {
		t.Fatalf("current-cut recovery: %v (salvage %v)", err, salvage)
	}
	if err := current.Discard(); err != nil {
		t.Fatal(err)
	}
	currentRT = recovered.Runtime
	if err := recovered.Thaw(); err != nil {
		t.Fatal(err)
	}
	control, err = activationCall(f, currentRT, first, proto.ProcessResumeReq{Op: "resize", ControlSeq: 4, Resize: &proto.PtyResizeEvent{Cols: 92, Rows: 38}, StdoutACK: 1})
	if err != nil || control.ControlACK != 4 {
		t.Fatalf("control after current-cut recovery: ack=%d err=%v", control.ControlACK, err)
	}
	input, err := activationCall(f, currentRT, first, proto.ProcessResumeReq{Op: "input", InputSeq: (128 << 10) + 7, Input: []byte("next\n"), StdoutACK: 1})
	if err != nil || input.InputAccepted != (128<<10)+12 {
		t.Fatal("current-cut recovery did not reopen input")
	}
	activationAwait(t, func() bool {
		r, err := activationCall(f, currentRT, first, proto.ProcessResumeReq{Op: "resume", StdoutACK: 1})
		if err != nil {
			t.Fatal(err)
		}
		return r.InputACK == (128<<10)+12 && r.Stdout != nil && bytes.Contains(r.Stdout.Data, []byte("<next>"))
	}, "both directions and controls restored after the current cut")
	for _, res := range resources {
		if master := res.Files["master"]; master != nil {
			if _, err := master.Stat(); err == nil {
				t.Fatal("discard kept the previous owner master wrapper")
			}
		}
	}
	src.Close()
	newSource.Close()
}

func activationCall(f *cutFixture, rt *execsession.Runtime, h proto.ProcessHandle, req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	req.Kind, req.Handle, req.AttachEpoch = proto.ProcessKindPTY, &h, 1
	return f.handle(rt, req)
}

func activationCreatePTY(t *testing.T, f *cutFixture, request, script string, args ...string) (proto.ProcessHandle, string) {
	t.Helper()
	probe, err := f.handle(f.rt, proto.ProcessResumeReq{Op: "probe", Kind: proto.ProcessKindPTY, ClientLineage: f.scope.ClientLineage})
	if err != nil || probe.Lease == nil || probe.PTYVersion != proto.PTYVersion {
		t.Fatalf("PTY capability missing (requires P6): version=%d err=%v", probe.PTYVersion, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: request}
	argv := append([]string{"/bin/sh", "-c", script, "activation-child"}, args...)
	spec := proto.RunReq{Argv: argv, Cols: 80, Rows: 24, KAIntervalMS: 1000}
	reply, err := activationCall(f, f.rt, h, proto.ProcessResumeReq{Op: "create", Run: &spec})
	if err != nil || reply.PID == "" {
		t.Fatalf("PTY create: %v", err)
	}
	return h, reply.PID
}

func activationAwait(t *testing.T, predicate func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(imageTestBudget)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", label)
		}
		time.Sleep(time.Millisecond)
	}
}

func activationTaskState(t *testing.T, task execsession.TaskImage) ptysession.Image {
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
		t.Fatal("runtime omitted the PTY image domain")
	}
	return *view.PTY
}

// The hook wraps real activation and injects only the second owner's failure.
func installPTYActivationFault(t *testing.T, cfg *execsession.Config, hook func(string, *ptysession.Actor) error) {
	t.Helper()
	cfg.PTYActivate = hook
}

func activationChildIdentity(pid string, ospid int) (workload.Identity, error) {
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
