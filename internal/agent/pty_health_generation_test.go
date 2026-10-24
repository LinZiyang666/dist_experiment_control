//go:build linux

package agent

import (
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 internal review rv-auth RV-AUTH-3
func TestPTYHealthProofCannotOutliveCurrentConnectionGeneration(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"stable", "change-in-probe", "change-after-check"} {
			t.Run(arm, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exercisePTYHealthGeneration(t, arm)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "authority regression", g0)
	testharness.AssertNoFDLeak(t, "authority regression", fd0, 0)
}

func exercisePTYHealthGeneration(t *testing.T, arm string) {
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(now - int64(190*time.Second))
	var block atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	oldClock := processBootClock
	processBootClock = func() (int64, error) {
		if block.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		return clock.Load(), nil
	}
	t.Cleanup(func() { processBootClock = oldClock })
	a := newBootAgent(t, startNATS(t), "node")
	nc, err := nats.Connect(a.cfg.NATSURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	a.ncBox.Store(nc)
	a.ptyHealthGeneration.Store(1)
	scope := ptyTestScope(a)
	h := ptyProbe(t, a, scope)
	q := ptyTaskRequest(scope, h, "create")
	q.Run = &proto.RunReq{Cwd: t.TempDir(), KAIntervalMS: 1000, Argv: []string{"/bin/sh", "-c", "trap '' HUP; printf ready; exec sleep 60"}}
	reply, err := a.processRuntime.Handle(scope, ownProcessRun(scope, q), "_TINBOX.pty")
	if err != nil {
		t.Fatal(err)
	}
	q = ptyTaskRequest(scope, h, "resume")
	waitFor(t, bootTestBudget, func() bool {
		p, err := a.processRuntime.Handle(scope, q, "_TINBOX.pty")
		return err == nil && p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "ready")
	})
	// Expire KA with a failed proof, fixing the recovery opportunity before
	// real boot time. No connection evidence or authority is fabricated.
	clock.Store(now - int64(6*time.Second))
	a.processRuntime.PTYHealth(ptysession.HealthProof{})
	now, err = procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	clock.Store(now + int64(time.Second))
	if !provePTYHealth(a, nc) {
		t.Fatal("first actual NATS round trip failed")
	}
	clock.Store(now + int64(4*time.Second))
	if arm == "change-in-probe" {
		oldProbe := ctlConnProbe
		ctlConnProbe = func(c *nats.Conn) error {
			err := oldProbe(c)
			c.Close()
			a.ncBox.Store(nil)
			a.ptyHealthGeneration.Add(1)
			return err
		}
		defer func() { ctlConnProbe = oldProbe }()
		if provePTYHealth(a, nc) {
			t.Fatal("agent ignored generation change before its final check")
		}
		if ptyState(t, a, reply.PID).MasterClosed {
			t.Fatal("invalidated proof closed master")
		}
		return
	}
	block.Store(true)
	done := make(chan bool, 1)
	go func() { done <- provePTYHealth(a, nc) }()
	select {
	case <-entered:
	case <-time.After(bootTestBudget):
		t.Fatal("health Tick did not reach post-probe clock barrier")
	}
	// Tick is now past the agent's successful probe and its final generation
	// check, but has not entered the actor's control transaction.
	if arm == "change-after-check" {
		nc.Close()
		a.ncBox.Store(nil)
		a.ptyHealthGeneration.Add(1)
	}
	close(release)
	select {
	case healthy := <-done:
		if healthy && arm == "change-after-check" {
			t.Log("provePTYHealth returned healthy after connection closed and generation changed")
		}
	case <-time.After(bootTestBudget):
		t.Fatal("health Tick did not finish")
	}
	st := ptyState(t, a, reply.PID)
	if arm == "stable" && !st.MasterClosed {
		t.Fatal("two healthy proofs did not close expired PTY")
	}
	if arm == "change-after-check" && st.MasterClosed {
		t.Fatal("stale generation-1 proof hung up the original PTY while current generation was 2 and NATS was disconnected")
	}
}
