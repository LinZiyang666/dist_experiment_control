//go:build linux

package cli_e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/transferharness"
)

// Establish the fixed fixture observation window before launching the first
// ctl. Provisioning has its own bound; counting that work against a later
// invocation killed the fixture before the ctl's original 60-second timeout.
// This context only bounds observations: every ctl keeps its original timeout
// and sealed byte deadline, while the supervisor owns process lifetime.
// origin: S7 internal review CONTINUATION-VICTIM F3
func beginTransferCohort(t *testing.T, f *transferStack) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	f.ctx = ctx
}

// Current artifacts require deployment registration and explicit privileged
// staging. This adapter uses the same real transaction CLI as the serve suite;
// it supplies no receipt, product state or receiver outcome itself.
// origin: S7 internal review TRANSFER-MAKETEST F6 / S7 plan §8.2 / §9 / T45
// Artifact extraction is fixture setup; the grant is deliberately prepared
// only after the original transfer is admitted, so its scope names that work.
// origin: S7 internal review CONTINUATION-VICTIM F2 / S7 plan §10.2
func (f *transferStack) agentTransactionCandidate(t *testing.T, p *transferPeer, artifact upgradeArtifact) string {
	t.Helper()
	candidate := filepath.Join(f.h.Root, "agent-candidate-"+p.sid+"-"+artifact.binSHA)
	writeArtifactBinary(t, artifact, candidate)
	return candidate
}

func (f *transferStack) prepareAgentTransactionCut(t *testing.T, p *transferPeer, url string, artifact upgradeArtifact, agentPID int, candidate string) transferAgentCut {
	t.Helper()
	cut := transferAgentCut{stack: f, peer: p}
	control := cut.control(t)
	dir := t.TempDir()
	grantPath := filepath.Join(dir, "agent-grant.json")
	cut.receipt = filepath.Join(dir, "agent-receipt.json")
	ctx, cancel := context.WithTimeout(f.ctx, 150*time.Second)
	defer cancel()
	if r := control.ctlCommand(ctx, "node", "upgrade", p.nid, "--url", url, "--sha256", artifact.tarSHA, "--prepare-only", "--grant-out", grantPath); r.rc != 0 {
		t.Fatalf("agent prepare rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	var err error
	cut.tx, err = grantTxID(grantPath)
	if err != nil {
		t.Fatal(err)
	}
	if r := control.transactionCtl(60*time.Second, []string{"TETHER_HOME=" + p.agentHome}, "upgrade", "stage", "--grant", grantPath,
		"--source", fmt.Sprintf("/proc/%d/exe", agentPID), "--candidate", candidate, "--receipt-out", cut.receipt); r.rc != 0 {
		t.Fatalf("agent stage rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	return cut
}

type transferAgentCut struct {
	stack       *transferStack
	peer        *transferPeer
	tx, receipt string
}

func (cut transferAgentCut) control(t *testing.T) *realStack {
	f, p := cut.stack, cut.peer
	return &realStack{t: t, bin: f.bin, sid: p.sid, nid: p.nid, url: f.url, ctlHome: p.home, agentHome: p.agentHome}
}

func (cut transferAgentCut) start(t *testing.T) error {
	control := cut.control(t)
	ctx, cancel := context.WithTimeout(cut.stack.ctx, 150*time.Second)
	defer cancel()
	if r := control.ctlCommand(ctx, "node", "upgrade", cut.peer.nid, "--tx-id", cut.tx, "--artifact-receipt", cut.receipt); r.rc != 0 && r.rc != 74 {
		return fmt.Errorf("agent start rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	if r := control.ctlCommand(ctx, "node", "upgrade-wait", cut.peer.nid, "--tx-id", cut.tx, "--timeout", "120s"); r.rc != 0 {
		return fmt.Errorf("agent wait rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	return nil
}

// Reverse exports the current owner of the accepted transaction. A second
// download/prepare/stage would be another forward, not the required rollback.
// origin: S7 internal review CONTINUATION-VICTIM F2 / S7 plan §8.2 / §10.2
func (cut transferAgentCut) rollback(t *testing.T) error {
	ctx, cancel := context.WithTimeout(cut.stack.ctx, 150*time.Second)
	defer cancel()
	if r := cut.control(t).ctlCommand(ctx, "node", "upgrade-rollback", cut.peer.nid, "--tx-id", cut.tx, "--timeout", "120s"); r.rc != 0 {
		return fmt.Errorf("agent rollback rc=%d: %s%s", r.rc, r.stdout, r.stderr)
	}
	return nil
}

// Victims have separate registered installations and buckets. Their real cuts
// may overlap; serializing them spends the first victim's byte budget on the
// second agent. The group returns only after every cut and its identity checks.
// origin: S7 internal review CONTINUATION-VICTIM F2 / S7 plan §10.3 / T45
func runTransferVictimCuts(t *testing.T, victims []transferVictim, cut func(*testing.T, transferVictim)) {
	t.Helper()
	t.Run("cuts", func(t *testing.T) {
		for _, victim := range victims {
			t.Run(victim.peer.sid, func(t *testing.T) {
				t.Parallel()
				cut(t, victim)
			})
		}
	})
}

// This tests only fixture scheduling: both callbacks must enter before either
// may finish, and cancellation cannot start until both have joined. The binary
// victim suite supplies real cuts and retains all original-process assertions.
// origin: S7 internal review CONTINUATION-VICTIM F2
func TestTransferVictimCutsOverlapAndJoinBeforeCancellation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferVictimCutJoin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "victim cut join", g)
	testharness.AssertNoFDLeak(t, "victim cut join", fd, 0)
}

func exerciseTransferVictimCutJoin(t *testing.T) {
	t.Helper()
	entered := make(chan struct{}, 2)
	release, joined := make(chan struct{}), make(chan bool, 1)
	returned := make(chan struct{})
	go func() {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		defer close(release)
		for range 2 {
			select {
			case <-entered:
			case <-timer.C:
				joined <- false
				return
			}
		}
		select {
		case <-returned:
			joined <- false
		default:
			joined <- true
		}
	}()
	var completed atomic.Int32
	runTransferVictimCuts(t, []transferVictim{{peer: &transferPeer{sid: "cancel"}}, {peer: &transferPeer{sid: "revoke"}}}, func(*testing.T, transferVictim) {
		entered <- struct{}{}
		<-release
		completed.Add(1)
	})
	close(returned)
	if !<-joined || completed.Load() != 2 {
		t.Fatal("victim cuts did not overlap and join before cancellation")
	}
}

// Canceling a spent provisioning context cannot kill an already supervised
// process or consume the live cohort's wait. The shell is only a fixture
// lifetime witness; the binary suites still require real transfer outcomes.
// origin: S7 internal review CONTINUATION-VICTIM F3
func TestTransferFixtureSetupCannotConsumeOriginalInvocationWait(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferFixtureDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer fixture deadline", g)
	testharness.AssertNoFDLeak(t, "transfer fixture deadline", fd, 0)
}

func exerciseTransferFixtureDeadline(t *testing.T) {
	t.Helper()
	h := transferharness.New(t, transferSupervisor{})
	setup, cancelSetup := context.WithCancel(t.Context())
	defer cancelSetup()
	f := &transferStack{t: t, h: h, ctx: setup}
	binary := copyTransferArtifact(t, h.Root, "fixture-shell", "/bin/sh")
	ready, release := filepath.Join(h.Root, "ready"), filepath.Join(h.Root, "release")
	p := f.start(binary, h.Root, "-c", `printf ready > "$1"; while test ! -e "$2"; do sleep 0.02; done; printf original`, "fixture", ready, release)
	if !testharnessWait(5*time.Second, func() bool { _, err := os.Stat(ready); return err == nil }) {
		t.Fatal("supervised lifetime witness did not enter its gate")
	}
	before := sampleTransferIdentity(t, p)
	cancelSetup()
	beginTransferCohort(t, f)
	if f.ctx.Err() != nil {
		t.Fatal("expired setup context consumed the original invocation wait")
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r, stdout, stderr := transferNative(f.ctx, t, p)
	if r.NativeRC != 0 || stdout != "original" || r.PID != before.PID {
		t.Fatalf("setup cancellation killed or replaced the original process: rc=%d %s %s", r.NativeRC, stdout, stderr)
	}
}
