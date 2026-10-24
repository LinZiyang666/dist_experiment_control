//go:build linux

package agentboot

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// origin: hot-update S5 plan §5.4 / §8 TestFlowSSRedispatchUsesCurrentState
// Inspect the real sealed manifest produced by the boot current-cut path.
// Byte advancement and a revoke after Freeze distinguish current re-export
// from both the imported bundle and the cached cut. This is an in-process
// preparation receipt; same-PID flow/SS exec is owned by P20–P22.
func TestFlowSSRedispatchUsesCurrentState(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseCurrentRedispatch(t, round)
		})
		t.Run(fmt.Sprintf("live/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseLiveFlowSSRedispatch(t, round)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "flow-ss-current-redispatch", beforeG)
	testharness.AssertNoFDLeak(t, "flow-ss-current-redispatch", beforeFD, 0)
}

func exerciseCurrentRedispatch(t *testing.T, round int) {
	t.Helper()
	f := newCutFixture(t, fmt.Sprintf("current-redispatch-%d", round))
	marker := t.TempDir() + "/advance"
	h := f.create(t, []string{"/bin/sh", "-c", `printf before; while [ ! -e "$1" ]; do sleep 0.02; done; printf after; exec sleep 30`, "current-child", marker})
	img, original, src := f.cut(t, 1)
	t.Cleanup(src.Close)
	initial := original.Bundle()
	if len(initial.Runtime.Tasks) != 1 || initial.Runtime.Tasks[0].Stdout.Replay.End != 6 {
		t.Fatal("initial cut has no independent byte baseline")
	}
	target := f.target()
	target.Runtime.Manager = f.manager
	imp, err := img.Restore(target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		imp.Manager.Thaw()
		_ = imp.Manager.SignalAll(syscall.SIGKILL)
		// ctx-none: cleanup outlives the test's canceled context.
		ctx, cancel := context.WithTimeout(context.Background(), imageTestBudget)
		defer cancel()
		_ = imp.Runtime.Close(ctx)
	})
	if err := original.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := img.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	activationAwait(t, func() bool {
		r, err := f.handle(imp.Runtime, proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1, StdoutACK: 3})
		if err != nil {
			t.Fatal(err)
		}
		return r.Stdout != nil && r.Stdout.Offset == 3 && string(r.Stdout.Data) == "oreafter"
	}, "the imported child advanced output and ACK before the second cut")
	cut, err := agentimage.Freeze(agentimage.Owners{Runtime: imp.Runtime, Manager: imp.Manager,
		Lineage: agentimage.Lineage{Generation: 2}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cut.Thaw() })
	cached := cut.Bundle()
	if len(cached.Runtime.Tasks) != 1 || cached.Runtime.Tasks[0].Stdout.Replay.Base != 3 || cached.Runtime.Tasks[0].Stdout.Replay.End != 11 || len(cached.Runtime.Revocations) != 0 {
		t.Fatal("current cut did not capture real byte progress before the late decision")
	}
	for _, phase := range []Phase{PhaseHeld, PhaseRefused, PhaseImported, PhaseLost, PhaseNone} {
		probe := &Image{phase: phase, imported: imp, generation: 1}
		if prepared, err := PrepareCurrent(probe, cut, nil); err == nil || prepared != nil {
			if prepared != nil {
				_ = prepared.Close()
			}
			t.Fatal("current re-dispatch accepted a pristine or lost phase")
		}
	}
	for _, generation := range []uint64{0, 2, math.MaxUint64} {
		probe := &Image{phase: PhaseActive, imported: imp, generation: generation}
		prepared, err := PrepareCurrent(probe, cut, nil)
		if err == nil || prepared != nil {
			if prepared != nil {
				_ = prepared.Close()
			}
			t.Fatal("current re-dispatch accepted a stale or overflowing generation")
		}
		if generation == math.MaxUint64 && !strings.Contains(err.Error(), "generation exhausted") {
			t.Fatal("current re-dispatch did not refuse generation overflow before preparation")
		}
	}
	foreign := *imp
	foreign.Bundle.Instance = "foreign-current-owner"
	probe := &Image{phase: PhaseActive, imported: &foreign, generation: 1}
	if prepared, err := PrepareCurrent(probe, cut, nil); err == nil || prepared != nil {
		if prepared != nil {
			_ = prepared.Close()
		}
		t.Fatal("current re-dispatch accepted another instance's cut")
	}
	probe = &Image{phase: PhaseActive, imported: imp, generation: 1, activating: true}
	if prepared, err := PrepareCurrent(probe, cut, nil); err == nil || prepared != nil {
		if prepared != nil {
			_ = prepared.Close()
		}
		t.Fatal("current re-dispatch overlapped an activation attempt")
	}
	if prepared, err := PrepareCurrent(nil, cut, nil); err == nil || prepared != nil {
		t.Fatal("current re-dispatch accepted a missing image")
	}
	if prepared, err := PrepareCurrent(img, nil, nil); err == nil || prepared != nil {
		t.Fatal("current re-dispatch accepted a missing cut")
	}
	probe = &Image{phase: PhaseActive, generation: 1}
	if prepared, err := PrepareCurrent(probe, cut, nil); err == nil || prepared != nil {
		t.Fatal("current re-dispatch accepted a missing imported owner")
	}
	if killed, err := imp.Runtime.Revoke(f.scope.SID, "late-current-cut"); err != nil || killed != 1 {
		t.Fatalf("late revoke did not record the frozen task: killed=%d err=%v", killed, err)
	}
	refused := errors.New("reader domain rejected")
	checks := 0
	prepared, err := PrepareCurrent(img, cut, func(b agentimage.Bundle) error {
		checks++
		if len(b.Runtime.Revocations) != 1 || b.Runtime.Tasks[0].Revoked != "late-current-cut" {
			t.Fatal("reader check received a stale cut")
		}
		return refused
	})
	if prepared != nil {
		_ = prepared.Close()
	}
	if !errors.Is(err, refused) || prepared != nil || checks != 1 {
		t.Fatal("current re-dispatch bypassed the final reader check")
	}
	// Failed Prepare only owns its duplicates. It must preserve the cut and
	// originals for a subsequent attempt or local Thaw.
	if prepared, err := PrepareCurrent(img, cut, nil, handoff.Resource{ID: "invalid:resource", Kind: "test"}); err == nil || prepared != nil {
		t.Fatal("invalid extra resource did not fail preparation")
	}
	for attempt, ready := range []bool{false, true} {
		if ready {
			img.MarkReady()
		}
		// A prior failed Prepare may itself have refreshed the cached bundle.
		// Put a distinct decision after that attempt so merely reusing its
		// export cannot pass the final-manifest oracle.
		lateSID := fmt.Sprintf("late-dispatch-%t", ready)
		if _, err := imp.Runtime.Revoke(lateSID, "late-dispatch-cut"); err != nil {
			t.Fatal(err)
		}
		before := currentManifestFDs(t)
		prepared, err := PrepareCurrent(img, cut, nil)
		if err != nil {
			t.Fatal(err)
		}
		b := readCurrentManifest(t, before)
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		if b.Lineage.Generation != 2 || b.Instance != initial.Instance || len(b.Runtime.Tasks) != 1 {
			t.Fatal("current re-dispatch reused the initial image lineage or owner set")
		}
		task, old := b.Runtime.Tasks[0], initial.Runtime.Tasks[0]
		if task.PID != old.PID || task.Stdout.Replay.Base != 3 || task.Stdout.Replay.End != 11 || task.Files["stdout-source"] != old.Files["stdout-source"] || b.Processes.Records[0].OSPID != initial.Processes.Records[0].OSPID {
			t.Fatal("current re-dispatch lost advanced offsets or original descriptor identity")
		}
		seenCurrent, seenLate := false, false
		for _, revoked := range b.Runtime.Revocations {
			seenCurrent = seenCurrent || revoked.SID == f.scope.SID
			seenLate = seenLate || revoked.SID == lateSID
		}
		if task.Revoked != "late-current-cut" || len(b.Runtime.Revocations) != attempt+2 || !seenCurrent || !seenLate {
			t.Fatal("current re-dispatch omitted the decision after Freeze")
		}
		if len(currentManifestFDs(t)) != len(before) {
			t.Fatal("closing Prepared retained its sealed manifest")
		}
		if img.Imported() != imp || img.Generation() != 1 {
			t.Fatal("failed-exec preparation changed the current boot owner or lineage")
		}
	}
}

// The manifest is inspected only in memory through its sealed memfd. No
// secret-bearing State or Bundle is written to a receipt, file or test log.
func currentManifestFDs(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool)
	for _, entry := range entries {
		path := filepath.Join("/proc/self/fd", entry.Name())
		name, err := os.Readlink(path)
		if err == nil && strings.HasPrefix(name, "/memfd:tether-handoff ") {
			out[path] = true
		}
	}
	return out
}

func readCurrentManifest(t *testing.T, before map[string]bool) agentimage.Bundle {
	t.Helper()
	var path string
	for candidate := range currentManifestFDs(t) {
		if !before[candidate] {
			if path != "" {
				t.Fatal("preparation created more than one sealed manifest")
			}
			path = candidate
		}
	}
	if path == "" {
		t.Fatal("preparation did not create a sealed manifest")
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		State handoff.State `json:"state"`
	}
	if err := json.Unmarshal(blob, &manifest); err != nil {
		t.Fatal("cannot decode prepared manifest")
	}
	b, err := agentimage.Decode(manifest.State)
	if err != nil {
		t.Fatal("cannot decode prepared bundle")
	}
	return b
}

// origin: hot-update S5 plan §5.4 / T34 / T44
// A real carrier and SS connection advance while the first PTY activation hook
// is running. Only the second PTY hook injects failure. The independent peer
// retains its AEAD nonces through the current-cut re-dispatch and next grant.
func exerciseLiveFlowSSRedispatch(t *testing.T, round int) {
	t.Helper()
	f := newCutFixture(t, fmt.Sprintf("live-partial-%d", round))
	if err := f.rt.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	budget := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	f.cfg.SharedReplayBudget = budget
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
	first, firstPID := activationCreatePTY(t, f, "first", "printf ready; exec sleep 30")
	second, secondPID := activationCreatePTY(t, f, "second", "printf ready; exec sleep 30")
	for _, handle := range []proto.ProcessHandle{first, second} {
		activationAwait(t, func() bool {
			r, err := activationCall(f, f.rt, handle, proto.ProcessResumeReq{Op: "resume"})
			return err == nil && r.Stdout != nil && string(r.Stdout.Data) == "ready"
		}, "two real actors ready")
	}
	ss := ssproxy.New(testharness.SilentLog())
	keys := []ssproxy.Key{{KeyID: "bob", Secret: "live-nonce-reference"}}
	ssPort, err := ss.Start(0, keys)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Stop()
	target := liveRedispatchListener(t)
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	control, public := liveRedispatchListener(t), liveRedispatchListener(t)
	address, port := control.Addr().String(), public.Addr().(*net.TCPAddr).Port
	_ = control.Close()
	_ = public.Close()
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "live-broker", BootID: boot, OwnerGen: 1,
		TokenLookup: func(tunnel.AuthRequest) error { return nil },
		Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 1, ProxyEpoch: 1, AppliedRevision: 2,
				Keys: []tunnel.AuthorizedKey{{ID: "bob", Secret: []byte(keys[0].Secret)}}}, nil
		}}
	broker := tunnel.NewServer(address, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, testharness.SilentLog())
	if err := broker.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := broker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	client := tunnel.NewClient(address, "lab", "node", func(int) (int, error) { return ssPort, nil }, testharness.SilentLog())
	cfg.Budget, cfg.InstanceID = budget, f.cfg.Instance
	cfg.BeforeLocalConnect = func(id flow.ID, tuple rawsocket.Tuple) error { return ss.ExpectFlow([16]byte(id), tuple) }
	if err := client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := ss.SetFlowRevoke(func(id [16]byte, _ string) { _ = client.RevokeFlow(flow.ID(id), flow.ReasonRevoked) }); err != nil {
		t.Fatal(err)
	}
	client.Start(ctx)
	defer client.Close(port)
	if err := client.Open(port, ssPort, "fixture-token"); err != nil {
		t.Fatal(err)
	}
	wire, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(wire.Close)
	_ = wire.SetDeadline(time.Now().Add(imageTestBudget))
	ref := &liveSSReference{conn: wire, password: keys[0].Secret}
	targetPort := target.Addr().(*net.TCPAddr).Port
	ref.send(t, append([]byte{1, 127, 0, 0, 1, byte(targetPort >> 8), byte(targetPort)}, []byte("before")...))
	peer, err := target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	_ = peer.SetDeadline(time.Now().Add(imageTestBudget))
	liveSSExchange(t, ref, peer, "before")
	originalFingerprint := liveRedispatchFingerprint(t, peer)
	if err := client.PrepareHandoff(2, [16]byte{1}, time.Now().Add(imageTestBudget)); err != nil {
		t.Fatal(err)
	}
	flowCut, err := client.FreezeFlows(time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = flowCut.Thaw() }()
	flowImage, flowSnapshot, err := flowCut.Export()
	if err != nil {
		t.Fatalf("NOT_RUN pending I1: live flow export: %v", err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(flowSnapshot.Close)
	ssCut, err := ss.Freeze(time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ssCut.Thaw() }()
	ssImage, ssSnapshot, err := ssCut.Export()
	if err != nil {
		t.Fatalf("NOT_RUN pending I2: live SS export: %v", err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(ssSnapshot.Close)
	// Capture business identities while the original cut is still valid. They
	// must retire in the receiving owner, before fixture/Prepared cleanup.
	// origin: hot-update S5 plan §5.3 / §6.5 / T34
	ssBusiness := make(map[rawsocket.Fingerprint]bool)
	for _, resource := range ssSnapshot.Resources {
		for name, file := range resource.Files {
			if name != "loopback" && name != "upstream" {
				continue
			}
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			stat := info.Sys().(*syscall.Stat_t)
			ssBusiness[rawsocket.Fingerprint{Dev: stat.Dev, Ino: stat.Ino, Type: stat.Mode & syscall.S_IFMT}] = true
		}
	}
	if len(ssBusiness) != 2 {
		t.Fatal("original SS loopback/upstream ownership premise is empty")
	}
	_, runtimeCut, oldSource := f.cut(t, 2)
	// origin: hot-update S5 plan §5.3 / T34. A refused later domain still
	// leaves the exporter frozen; release its fence before fixture cleanup.
	defer func() { _ = runtimeCut.Thaw() }()
	defer oldSource.Close()
	initial := runtimeCut.Bundle()
	initial.Flow, initial.SS = &flowImage, &ssImage
	resources, err := handoff.MergeResources(oldSource.Borrow(), flowSnapshot.Resources, ssSnapshot.Resources)
	if err != nil {
		t.Fatal(err)
	}
	source := handofftest.DupResources(t, resources)
	defer source.Close()
	img := FromCut(initial, initial.Processes.Records, source, source.Borrow())
	defer func() { img.Abandon(nil, CrashWindowNoRollbackTarget, "test finished") }()
	var imp *agentimage.Imported
	failure := errors.New("second actor activation refused")
	tgt := f.target()
	tgt.Runtime.Manager = f.manager
	tgt.Flow = cfg
	var advanced atomic.Bool
	tgt.Runtime.PTYActivate = func(pid string, actor *ptysession.Actor) error {
		if pid == secondPID {
			return failure
		}
		if pid != firstPID || img.Phase() != PhaseActive {
			return errors.New("partial owner boundary missing")
		}
		if err := actor.Activate(); err != nil {
			return err
		}
		if err := imp.Flow.ActivateRaw(); err != nil {
			return err
		}
		if err := imp.SS.ActivateRaw(); err != nil {
			return err
		}
		imp.Flow.Start(ctx)
		if err := imp.Flow.ResumeCarriers(time.Now().Add(imageTestBudget)); err != nil {
			return err
		}
		if err := liveRedispatchAuthorize(imp.Flow, imp.SS); err != nil {
			return err
		}
		ref.send(t, []byte("advanced"))
		liveSSExchange(t, ref, peer, "advanced")
		advanced.Store(true)
		return nil
	}
	imp, err = img.Restore(tgt)
	if err != nil {
		t.Fatalf("composite import: %v", err)
	}
	// Callback pointers do not travel in the sealed image. The restoring
	// owner must bind revoke before activation, including a partial attempt.
	// origin: hot-update S5 plan §6.5 / §7.1 / T34
	if err := imp.SS.SetFlowRevoke(func(id [16]byte, _ string) {
		_ = imp.Flow.RevokeFlow(flow.ID(id), flow.ReasonRevoked)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		imp.Manager.Thaw()
		_ = imp.Manager.SignalAll(syscall.SIGKILL)
		_ = imp.Runtime.Close(context.WithoutCancel(t.Context()))
	}()
	defer imp.SS.Stop()
	defer imp.Flow.Close(port)
	if err := runtimeCut.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := flowCut.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := ssCut.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := img.Activate(); !errors.Is(err, failure) || !advanced.Load() || img.Phase() != PhaseActive {
		t.Fatal("real partial activation did not retain the advanced owner")
	}
	if prepared, err := img.Repack(); err == nil || prepared != nil {
		t.Fatal("partial activation repacked pristine nonce/offset state")
	}
	current, err := agentimage.Freeze(agentimage.Owners{Runtime: imp.Runtime, Manager: imp.Manager, Flow: imp.Flow, SS: imp.SS,
		Lineage: agentimage.Lineage{Generation: 3, InstanceID: initial.Lineage.InstanceID}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = current.Thaw() }()
	before := currentManifestFDs(t)
	prepared, err := PrepareCurrent(img, current, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The in-process exec model retains Prepared's old Go wrappers. Retire
	// them after the new owners close the transferred descriptor numbers,
	// preventing their finalizers from closing a later test's reused FDs.
	// origin: hot-update S5 plan §5.3 / T34
	defer func() { _ = prepared.Close() }()
	b := readCurrentManifest(t, before)
	if b.Flow == nil || b.SS == nil || len(b.Flow.Flows.Records) != 1 || len(initial.Flow.Flows.Records) != 1 {
		t.Fatal("current redispatch lost live flow or SS owner")
	}
	now, old := b.Flow.Flows.Records[0], initial.Flow.Flows.Records[0]
	if b.Lineage.Generation != 3 || now.ID != old.ID || now.RawRef != old.RawRef || now.RxApplied <= old.RxApplied || now.TxEnd <= old.TxEnd || b.SS.ConnectionCount != 1 {
		t.Fatal("current redispatch reused pristine offsets or original socket was replaced")
	}
	// Transfer the real Prepared duplicates to another bootstrap, so an
	// omitted/reset nonce in the second export cannot pass by merely thawing
	// the old in-memory actor. The old Prepared owner disappears on exec.
	var manifestPath string
	for path := range currentManifestFDs(t) {
		if !before[path] {
			manifestPath = path
		}
	}
	blob, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(blob, &envelope); err != nil {
		t.Fatal(err)
	}
	t.Setenv("_TETHER_HANDOFF", filepath.Base(manifestPath)+":"+envelope.Nonce)
	Reset()
	defer Reset()
	next := Begin(nil)
	if next == nil || next.Phase() != PhaseHeld {
		t.Fatal("current re-dispatch was not held")
	}
	t.Cleanup(func() { next.Abandon(nil, CrashWindowNoRollbackTarget, "test") })
	recoveryTarget := f.target()
	recoveryTarget.Runtime.Manager = imp.Manager
	recoveryTarget.Flow = cfg
	recovered, err := next.Restore(recoveryTarget)
	if err != nil {
		t.Fatal("current flow/SS re-import refused", err)
	}
	if err := recovered.SS.SetFlowRevoke(func(id [16]byte, _ string) {
		_ = recovered.Flow.RevokeFlow(flow.ID(id), flow.ReasonRevoked)
	}); err != nil {
		t.Fatal(err)
	}
	defer recovered.SS.Stop()
	defer recovered.Flow.Close(port)
	defer func() {
		recovered.Manager.Thaw()
		_ = recovered.Manager.SignalAll(syscall.SIGKILL)
		_ = recovered.Runtime.Close(context.WithoutCancel(t.Context()))
	}()
	if err := current.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := next.Activate(); err != nil {
		t.Fatal(err)
	}
	recovered.Flow.Start(ctx)
	if err := recovered.ResumeCarriers(time.Now().Add(imageTestBudget)); err != nil {
		t.Fatal(err)
	}
	for _, authority := range tunnel.FlowAuthorities(recovered.Flow) {
		keys := make([]ssproxy.Key, len(authority.Keys))
		for i, key := range authority.Keys {
			keys[i] = ssproxy.Key{KeyID: key.ID, Secret: string(key.Secret)}
		}
		if err := recovered.AuthorizeSS(ssproxy.Grant{Enabled: authority.Enabled, Generation: authority.Generation, Epoch: authority.ProxyEpoch, AppliedRevision: authority.AppliedRevision, Keys: keys}); err != nil {
			t.Fatal(err)
		}
	}
	if !recovered.ActivationComplete() {
		t.Fatal("current re-dispatch omitted fresh authority")
	}
	// Traffic alone can pass through a reconnect that silently retains the
	// previous owner. A following PREPARE requires the new generation at both
	// peers and proves the consumed permit was replaced for the current cut.
	// origin: hot-update S5 plan §5.4 / T34
	if err := recovered.Flow.PrepareHandoff(b.Lineage.Generation+1, [16]byte{4}, time.Now().Add(imageTestBudget)); err != nil {
		t.Fatal("current redispatch did not advance handoff ownership", err)
	}
	ref.send(t, []byte("current"))
	liveSSExchange(t, ref, peer, "current")
	if ref.txNonce[0] != 6 || ref.rxNonce[0] != 6 || liveRedispatchFingerprint(t, peer) != originalFingerprint {
		t.Fatal("nonce continuity or original business identity lost after current redispatch")
	}
	_ = target.SetDeadline(time.Now().Add(20 * time.Millisecond))
	if extra, err := target.Accept(); err == nil {
		_ = extra.Close()
		t.Fatal("current cut redialed the target")
	}
	recovered.SS.Stop()
	recovered.Flow.Close(port)
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := os.Stat("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		stat := info.Sys().(*syscall.Stat_t)
		if ssBusiness[rawsocket.Fingerprint{Dev: stat.Dev, Ino: stat.Ino, Type: stat.Mode & syscall.S_IFMT}] {
			t.Fatal("restored SS cleanup retained an original business socket")
		}
	}
}

func liveRedispatchAuthorize(client *tunnel.Client, server *ssproxy.Server) error {
	authorities := tunnel.FlowAuthorities(client)
	if len(authorities) > 0 {
		authority := authorities[0]
		keys := make([]ssproxy.Key, len(authority.Keys))
		for i, key := range authority.Keys {
			keys[i] = ssproxy.Key{KeyID: key.ID, Secret: string(key.Secret)}
		}
		return server.Authorize(ssproxy.Grant{Enabled: authority.Enabled, Generation: authority.Generation, Epoch: authority.ProxyEpoch, AppliedRevision: authority.AppliedRevision, Keys: keys})
	}
	return ssproxy.ErrAuthorityPending
}
func liveRedispatchListener(t *testing.T) *net.TCPListener {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	return ln
}
func liveRedispatchFingerprint(t *testing.T, conn *net.TCPConn) rawsocket.Fingerprint {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	var failure error
	if err := raw.Control(func(fd uintptr) { failure = syscall.Fstat(int(fd), &st) }); err != nil || failure != nil {
		t.Fatal("original socket fingerprint unavailable")
	}
	return rawsocket.Fingerprint{Dev: st.Dev, Ino: st.Ino, Type: st.Mode & syscall.S_IFMT}
}

type liveSSReference struct {
	conn             net.Conn
	password         string
	tx, rx           cipher.AEAD
	txNonce, rxNonce [12]byte
}

func liveSSCipher(t *testing.T, password string, salt []byte) cipher.AEAD {
	t.Helper()
	var master, prior []byte
	for len(master) < 32 {
		h := md5.New()
		_, _ = h.Write(prior)
		_, _ = h.Write([]byte(password))
		prior = h.Sum(nil)
		master = append(master, prior...)
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master[:32], salt, []byte("ss-subkey")), key); err != nil {
		t.Fatal(err)
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}
func liveSSNonce(n *[12]byte) {
	for i := range n {
		n[i]++
		if n[i] != 0 {
			return
		}
	}
	panic("reference nonce exhausted")
}
func (r *liveSSReference) send(t *testing.T, payload []byte) {
	t.Helper()
	if r.tx == nil {
		salt := bytes.Repeat([]byte{0x42}, 32)
		r.tx = liveSSCipher(t, r.password, salt)
		if _, err := r.conn.Write(salt); err != nil {
			t.Fatal(err)
		}
	}
	var size [2]byte
	binary.BigEndian.PutUint16(size[:], uint16(len(payload)))
	wire := r.tx.Seal(nil, r.txNonce[:], size[:], nil)
	liveSSNonce(&r.txNonce)
	wire = append(wire, r.tx.Seal(nil, r.txNonce[:], payload, nil)...)
	liveSSNonce(&r.txNonce)
	if _, err := r.conn.Write(wire); err != nil {
		t.Fatal(err)
	}
}
func liveSSExchange(t *testing.T, r *liveSSReference, peer net.Conn, want string) {
	t.Helper()
	data := make([]byte, len(want))
	if _, err := io.ReadFull(peer, data); err != nil || string(data) != want {
		t.Fatal("original target byte ledger failed", err)
	}
	if _, err := peer.Write(data); err != nil {
		t.Fatal(err)
	}
	if r.rx == nil {
		salt := make([]byte, 32)
		if _, err := io.ReadFull(r.conn, salt); err != nil {
			t.Fatal(err)
		}
		r.rx = liveSSCipher(t, r.password, salt)
	}
	length := make([]byte, 18)
	if _, err := io.ReadFull(r.conn, length); err != nil {
		t.Fatal(err)
	}
	plain, err := r.rx.Open(nil, r.rxNonce[:], length, nil)
	if err != nil {
		t.Fatal("response nonce changed")
	}
	liveSSNonce(&r.rxNonce)
	size := int(binary.BigEndian.Uint16(plain))
	if size < 1 || size > 0x3fff {
		t.Fatal("invalid independent SS response size")
	}
	wire := make([]byte, size+16)
	if _, err := io.ReadFull(r.conn, wire); err != nil {
		t.Fatal(err)
	}
	plain, err = r.rx.Open(nil, r.rxNonce[:], wire, nil)
	if err != nil || string(plain) != want {
		t.Fatal("response nonce or byte ledger changed")
	}
	liveSSNonce(&r.rxNonce)
}
