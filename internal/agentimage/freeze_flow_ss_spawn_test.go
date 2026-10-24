//go:build linux

package agentimage

import (
	"errors"
	"net/netip"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// Real: an isolated spawn helper, process table, bidirectional TLS/yamux flow,
// and retained SS listener/material. The adoption barrier exposes the window
// after the helper releases its slot but before Runtime commits Running.
// origin: hot-update S5 plan §5.3 / S4 SPAWN-INFLIGHT tests 1–4
func TestFreezeFlowSSDrainsSpawnBeforeProcessTable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeFlowSSSpawnDrain(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeFlowSSDrainsSpawnBeforeProcessTable", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeFlowSSDrainsSpawnBeforeProcessTable", beforeFD, 0)
}

func exerciseFreezeFlowSSSpawnDrain(t *testing.T) {
	entered, proceed, adopt := make(chan string, 1), make(chan struct{}), make(chan struct{})
	releaseStart := sync.OnceFunc(func() { close(proceed) })
	releaseAdopt := sync.OnceFunc(func() { close(adopt) })
	checked := make(chan error, 1)
	var f *bundleFixture
	var policy *spawnsafe.Policy
	var client *tunnel.Client
	ss := ssproxy.New(testharness.SilentLog())
	f, policy = spawnBundleFixtureWithAdoptionBarrier(t, entered, proceed, func() {
		var err error
		switch {
		case f.manager.Frozen():
			err = errors.New("process table froze before the admitted child adopted")
		case !policy.TryAcquireSpawnSlot():
			err = errors.New("spawn admission closed before Runtime joined its creation")
		default:
			policy.ReleaseSpawnSlot()
			// No revoke callback is installed: on a live SS server this valid
			// tuple cannot create a binding. Only its frozen gate returns Pending.
			tuple := rawsocket.Tuple{Local: netip.MustParseAddrPort("127.0.0.1:1"), Remote: netip.MustParseAddrPort("127.0.0.1:2")}
			if !errors.Is(ss.ExpectFlow([16]byte{1}, tuple), ssproxy.ErrAuthorityPending) {
				err = errors.New("SS safe-point fence did not precede Runtime drain")
			} else if client.PrepareHandoff(2, [16]byte{1}, time.Now().Add(bundleTestBudget)) == nil {
				err = errors.New("flow safe-point fence did not precede Runtime drain")
			}
		}
		checked <- err
		<-adopt
	})
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "spawn-material", Secret: "private-spawn-material"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ss.Stop)
	client = liveImageFlow(t, execsession.SharedReplayBudget(f.rt))
	h, pid := f.create(t, "composed-spawn", []string{"/bin/sh", "-c", "printf composed; exit 23"}, nil)
	select {
	case <-entered:
	case <-time.After(bundleTestBudget):
		t.Fatal("spawn helper never acquired its slot")
	}
	result := make(chan spawnCutResult, 1)
	go func() {
		cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy,
			Flow: client, SS: ss, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
		result <- spawnCutResult{cut, err}
	}()
	var got spawnCutResult
	joined := false
	t.Cleanup(func() {
		releaseStart()
		releaseAdopt()
		if !joined {
			got = <-result
		}
		if got.cut != nil && !got.cut.done {
			if err := got.cut.Thaw(); err != nil {
				t.Error(err)
			}
		}
	})
	// The SS admission guard proves its fence was requested while Start is
	// held; the adoption callback checks the flow fence independently.
	// Elapsed time is not a readiness oracle.
	if !testharness.WaitFor(t, bundleTestBudget/2, time.Millisecond, func() bool {
		tuple := rawsocket.Tuple{Local: netip.MustParseAddrPort("127.0.0.1:1"), Remote: netip.MustParseAddrPort("127.0.0.1:2")}
		return errors.Is(ss.ExpectFlow([16]byte{1}, tuple), ssproxy.ErrAuthorityPending)
	}) {
		t.Fatal("SS did not close admission while the spawn was in flight")
	}
	releaseStart()
	select {
	case err := <-checked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(bundleTestBudget):
		t.Fatal("spawn did not finish before adoption")
	}
	// Even with no live policy slot, the registry still owns a starting record.
	// The composed cut must join adoption rather than export or freeze Manager.
	reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1})
	if err != nil || reply.Phase != "starting" {
		t.Fatalf("attach during creation drain: phase=%s err=%v", reply.Phase, err)
	}
	if policy.Census().InFlight != 0 || f.manager.Frozen() {
		t.Fatal("slot release was mistaken for completed Runtime adoption")
	}
	if !policy.TryAcquireSpawnSlot() {
		t.Fatal("external slot was refused before creation drain completed")
	}
	releaseSlot := sync.OnceFunc(policy.ReleaseSpawnSlot)
	t.Cleanup(releaseSlot)
	releaseAdopt()
	if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, f.rt.Frozen) {
		t.Fatal("Runtime did not finish its creation fence")
	}
	if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, func() bool {
		if policy.TryAcquireSpawnSlot() {
			policy.ReleaseSpawnSlot()
			return false
		}
		return true
	}) {
		t.Fatal("policy did not close admission to drain the external slot")
	}
	if f.manager.Frozen() {
		t.Fatal("process table froze before the external spawn slot drained")
	}
	releaseSlot()
	got, joined = <-result, true
	if got.err != nil {
		t.Fatalf("composed cut rejected an in-time spawn: %v", got.err)
	}
	b := got.cut.Bundle()
	if b.Flow == nil || b.SS == nil || b.SpawnSafe == nil || len(b.Runtime.Tasks) != 1 || b.Runtime.Tasks[0].PID != pid || len(b.Flow.Flows.Records) != 1 {
		t.Fatal("composed cut lost the original spawn or retained Flow/SS domain")
	}
	state, err := got.cut.flow.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	prepared := false
	for _, record := range state.Records {
		prepared = prepared || record.Kind == sealedstate.KindPermit
	}
	if !prepared {
		t.Fatal("flow was fenced without a live-carrier PREPARE permit")
	}
	if err := got.cut.Thaw(); err != nil {
		t.Fatal(err)
	}
	out, terminal := f.drain(t, h)
	if out != "composed" || terminal.ExitCode != 23 || f.spawned != 1 {
		t.Fatalf("original request changed across cut: output=%q rc=%d starts=%d", out, terminal.ExitCode, f.spawned)
	}
}

// PREPARE spends most of the entry budget, then a non-Runtime slot consumes
// the remainder. Renewing any later drain deadline must fail this regression.
// origin: hot-update S5 plan §5.3 / S4 SPAWN-INFLIGHT absolute cut deadline
func TestFreezeFlowSSSpawnDrainSharesPrepareDeadline(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeFlowSSSpawnDeadline(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeFlowSSSpawnDrainSharesPrepareDeadline", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeFlowSSSpawnDrainSharesPrepareDeadline", beforeFD, 0)
}

func exerciseFreezeFlowSSSpawnDeadline(t *testing.T) {
	f := newBundleFixture(t, "composed-spawn-deadline")
	policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) { return nil, nil }})
	if err != nil || !policy.TryAcquireSpawnSlot() {
		t.Fatalf("external slot setup failed: %v", err)
	}
	releaseSlot := sync.OnceFunc(policy.ReleaseSpawnSlot)
	t.Cleanup(releaseSlot)
	ss := ssproxy.New(testharness.SilentLog())
	if _, err := ss.Start(0, []ssproxy.Key{{KeyID: "deadline-material", Secret: "private-deadline-material"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ss.Stop)
	entered, proceed := make(chan struct{}), make(chan struct{})
	releasePrepare := sync.OnceFunc(func() { close(proceed) })
	var armed atomic.Bool
	client := liveImageFlowWithAuthority(t, execsession.SharedReplayBudget(f.rt), func(req tunnel.AuthRequest) error {
		if !armed.CompareAndSwap(true, false) {
			return nil
		}
		close(entered)
		timer := time.NewTimer(max(time.Until(req.Deadline), 0))
		defer timer.Stop()
		select {
		case <-proceed:
			return nil
		case <-req.Stop:
			return errors.New("authority carrier stopped")
		case <-timer.C:
			return errors.New("authority deadline expired")
		}
	})
	armed.Store(true)
	deadline := time.Now().Add(400 * time.Millisecond)
	result := make(chan spawnCutResult, 1)
	var returnedAt time.Time
	go func() {
		cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy,
			Flow: client, SS: ss, Lineage: Lineage{Generation: 2}}, deadline)
		returnedAt = time.Now()
		result <- spawnCutResult{cut, err}
	}()
	var got spawnCutResult
	joined := false
	t.Cleanup(func() {
		releasePrepare()
		releaseSlot()
		if !joined {
			got = <-result
		}
		if got.cut != nil && !got.cut.done {
			_ = got.cut.Thaw()
		}
	})
	select {
	case <-entered:
	case <-time.After(bundleTestBudget):
		t.Fatal("PREPARE did not reach live authority")
	}
	if f.rt.Frozen() || f.manager.Frozen() {
		t.Fatal("PREPARE ran after the local cut")
	}
	// This controlled wait consumes the same absolute deadline supplied to
	// Freeze. No readiness or successful completion is inferred from the timer.
	timer := time.NewTimer(max(time.Until(deadline.Add(-100*time.Millisecond)), 0))
	<-timer.C
	releasePrepare()
	select {
	case got = <-result:
		joined = true
	case <-time.After(max(time.Until(deadline.Add(150*time.Millisecond)), 0)):
		t.Fatal("spawn drain renewed the deadline after Flow/SS PREPARE")
	}
	requireDeferred(t, got.err, ReasonSpawnInFlight)
	if returnedAt.Before(deadline) {
		t.Fatal("cut deferred an in-time slot before spending its remaining drain budget")
	}
	if got.cut != nil {
		t.Fatal("cut succeeded with an undrained external spawn slot")
	}
	if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, func() bool {
		if f.rt.Frozen() || !policy.TryAcquireSpawnSlot() {
			return false
		}
		policy.ReleaseSpawnSlot()
		return true
	}) {
		t.Fatal("expired composed cut stranded Runtime or spawn admission")
	}
	if f.manager.Frozen() {
		t.Fatal("failed spawn drain left the process table frozen")
	}
	if !ss.Serving() {
		t.Fatal("failed composed cut closed the original SS listener")
	}
	releaseSlot()
	h, _ := f.create(t, "after-composed-timeout", []string{"/bin/sh", "-c", "printf recovered"}, nil)
	out, terminal := f.drain(t, h)
	if out != "recovered" || terminal.ExitCode != 0 || f.spawned != 1 {
		t.Fatalf("request after rollback: output=%q rc=%d starts=%d", out, terminal.ExitCode, f.spawned)
	}
}
