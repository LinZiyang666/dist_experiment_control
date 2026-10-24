package agent

// ctl_liveness_test.go — the h1 D reaper: the pure decision table, the grace
// derivation, and the integration path (a real PTY child hung up after
// sustained keepalive silence; an unarmed run untouchable).
// origin: docs/reviews/h1-plan.md workstream D (zombie class b).

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

func TestShouldReapRunTable(t *testing.T) {
	now := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		rec  procRec
		conn bool
		want reapVerdict
	}{
		{"not armed (pre-h1 ctl)", procRec{kaGrace: 0, lastKA: now.Add(-time.Hour)}, true, reapNone},
		{"silence within grace", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Minute)}, true, reapNone},
		{"expired but conn unproven", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Hour)}, false, reapNone},
		{"expired, proven, first strike", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Hour)}, true, reapConfirm},
		{"expired, proven, second strike", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Hour), probeConfirmed: true}, true, reapNow},
		{"already reaped", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Hour), probeConfirmed: true, reaped: true}, true, reapNone},
		{"child already exited (recycled-pgid guard)", procRec{kaGrace: 3 * time.Minute, lastKA: now.Add(-time.Hour), probeConfirmed: true, waitDone: true}, true, reapNone},
	}
	for _, c := range cases {
		rec := c.rec
		if got := shouldReapRun(now, &rec, c.conn); got != c.want {
			t.Errorf("%s: verdict=%v want %v", c.name, got, c.want)
		}
	}
}

func TestKAGraceForTable(t *testing.T) {
	cases := []struct {
		ms   int
		want time.Duration
	}{
		{0, 0},                    // not advertised → never reap
		{-5, 0},                   // garbage → never reap
		{5000, 3 * time.Minute},   // 6×5s=30s < floor → floor
		{100, 3 * time.Minute},    // clamps up to 1s, still under floor
		{60000, 6 * time.Minute},  // 6×60s beats the floor
		{600000, 6 * time.Minute}, // interval clamps DOWN to 60s
	}
	for _, c := range cases {
		if got := kaGraceFor(c.ms); got != c.want {
			t.Errorf("kaGraceFor(%d)=%v want %v", c.ms, got, c.want)
		}
	}
}

// TestCtlLivenessReaperNeverReapsWithoutAProvenConn is the false-positive
// regression the plan's critique-4 BLOCKER demanded: with the agent's OWN
// conn unusable, keepalive silence says nothing about the ctl, so the reaper
// must restamp rather than hang up.
//
// The two subtests isolate the two guards, because in production they are
// defense-in-depth and each covers for the other — which is exactly how a
// deleted guard hides. Subtest 1 pins the `IsConnected()` half; subtest 2
// pins the probe by INJECTING a failing one (see ctlConnProbe's doc for why
// an in-process server cannot produce that state).
func TestCtlLivenessReaperNeverReapsWithoutAProvenConn(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	oldTick := ctlLivenessTick
	ctlLivenessTick = 30 * time.Millisecond
	t.Cleanup(func() { ctlLivenessTick = oldTick })

	t.Run("conn reports disconnected", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		url := startNATS(t)
		a := courierAgent(t, url)
		dir := t.TempDir()
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if pid := s.OSPID(); pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_, _ = s.Wait()
			}
			_ = s.Close()
		})
		if err := s.Start([]string{"sleep", "30"}, nil, dir, ""); err != nil {
			t.Fatal(err)
		}
		a.procsMu.Lock()
		a.procs["p"] = &procRec{sess: s, kaGrace: 50 * time.Millisecond, lastKA: time.Now().Add(-time.Hour)}
		a.procsMu.Unlock()
		a.ncBox.Load().Close() // IsConnected() == false from here on

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go a.ctlLivenessReaper(ctx)
		time.Sleep(500 * time.Millisecond) // many ticks

		a.procsMu.Lock()
		reaped := a.procs["p"].reaped
		lastKA := a.procs["p"].lastKA
		a.procsMu.Unlock()
		if reaped {
			t.Fatal("reaped while the agent's own conn was down — unobservable silence must never count against the ctl")
		}
		if time.Since(lastKA) > time.Second {
			t.Fatal("stamps were not restamped during the deaf window; the grace clock kept running")
		}
	})

	// The silent-partition shape: the conn is UP and nats.go reports
	// CONNECTED, but nothing can round-trip. Only the probe stands between
	// that state and a false hangup — the IsConnected() guard passes happily.
	// An in-process server cannot produce it (shutting it down flips the
	// client to reconnecting, so the first guard fires and the probe's
	// deletion stays invisible), so the probe is injected: this subtest is
	// the ONLY thing that fails when the probe is removed.
	t.Run("connected conn that cannot round-trip", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		url := startNATS(t)
		a := courierAgent(t, url)
		if nc := a.ncBox.Load(); !nc.IsConnected() {
			t.Fatal("fixture precondition: the conn must report CONNECTED")
		}
		oldProbe := ctlConnProbe
		ctlConnProbe = func(*nats.Conn) error { return errors.New("blackholed: no round trip") }
		t.Cleanup(func() { ctlConnProbe = oldProbe })

		dir := t.TempDir()
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if pid := s.OSPID(); pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_, _ = s.Wait()
			}
			_ = s.Close()
		})
		if err := s.Start([]string{"sleep", "30"}, nil, dir, ""); err != nil {
			t.Fatal(err)
		}
		a.procsMu.Lock()
		a.procs["p"] = &procRec{sess: s, kaGrace: 50 * time.Millisecond, lastKA: time.Now().Add(-time.Hour)}
		a.procsMu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go a.ctlLivenessReaper(ctx)
		time.Sleep(500 * time.Millisecond) // many ticks, all probe-failing

		a.procsMu.Lock()
		reaped := a.procs["p"].reaped
		a.procsMu.Unlock()
		if reaped {
			t.Fatal("reaped although the conn could not round-trip — nc.Status() alone is not proof " +
				"of health (it stays CONNECTED for minutes on a silent partition)")
		}
	})
}

// TestCtlLivenessReaperHangsUpSilentRun drives the real reaper against a real
// PTY child: the armed, silent run is hung up (Wait returns -1); the unarmed
// run (kaGrace=0 — every pre-h1 ctl) is untouchable no matter how silent.
// Mutation checks: deleting the restamp-on-unhealthy-conn guard or the
// probeConfirmed second strike must be caught by the table test above; this
// test pins the end-to-end SIGHUP outcome.
func TestCtlLivenessReaperHangsUpSilentRun(t *testing.T) {
	// A detached test runner may inherit SIGHUP=SIG_IGN. Notify installs a
	// handler in this process so exec resets the child's disposition to the
	// default; otherwise the child correctly inherits HUP immunity.
	// Stop restores the runner's original disposition after all child starts.
	// origin: hot-update S5 plan §8 / testing-standards §0.4
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCtlLivenessSilentRun(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ctl-liveness-silent-run", beforeG)
	testharness.AssertNoFDLeak(t, "ctl-liveness-silent-run", beforeFD, 0)
}

func exerciseCtlLivenessSilentRun(t *testing.T) {
	t.Helper()
	// Each round owns its NATS server/connection as well as its PTY children.
	// Run round cleanups now, before the next resource baseline is sampled.
	t.Run("silent-run", func(t *testing.T) { exerciseCtlLivenessSilentRunRound(t) })
}

func exerciseCtlLivenessSilentRunRound(t *testing.T) {
	t.Helper()
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url) // real conn in ncBox (IsConnected + Flush work)

	oldTick := ctlLivenessTick
	ctlLivenessTick = 50 * time.Millisecond
	t.Cleanup(func() { ctlLivenessTick = oldTick })

	mkSession := func(argv []string) *pty.Session {
		dir := t.TempDir()
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if pid := s.OSPID(); pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				_, _ = s.Wait()
			}
			_ = s.Close()
		})
		// origin: hot-update S5 PROC-LEAK. Detached launchers may ignore
		// HUP; reset the fixture disposition before testing the reaper.
		args := append([]string{"python3", "-c", "import os,signal,sys; signal.signal(signal.SIGHUP,signal.SIG_DFL); open('ready','w').close(); os.execvp(sys.argv[1],sys.argv[1:])"}, argv...)
		if err := s.Start(args, nil, dir, ""); err != nil {
			t.Fatal(err)
		}
		if !testharness.WaitFor(t, 2*time.Second, 10*time.Millisecond, func() bool { _, err := os.Stat(filepath.Join(dir, "ready")); return err == nil }) {
			t.Fatal("default HUP handler did not become ready")
		}
		return s
	}

	oldProbe := ctlConnProbe
	var successfulProbes atomic.Uint64
	ctlConnProbe = func(nc *nats.Conn) error {
		err := oldProbe(nc)
		if err == nil {
			successfulProbes.Add(1)
		}
		return err
	}
	t.Cleanup(func() { ctlConnProbe = oldProbe })

	// Block on the terminal instead of letting a timed sleep expire while the
	// host is busy. Only reaper hangup or explicit fixture teardown ends a run.
	// origin: hot-update S5 plan §8 / testing-standards §0.4
	armed := mkSession([]string{"/bin/sh", "-c", "read -r line; exit 99"})
	unarmed := mkSession([]string{"/bin/sh", "-c", "read -r line; exit 99"})
	a.procsMu.Lock()
	a.procs["armed"] = &procRec{sess: armed, kaGrace: 200 * time.Millisecond, lastKA: time.Now().Add(-time.Hour)}
	a.procs["unarmed"] = &procRec{sess: unarmed, kaGrace: 0, lastKA: time.Now().Add(-time.Hour)}
	a.procsMu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		a.ctlLivenessReaper(ctx)
	}()
	// Join before restoring the test's global tick/probe hooks. Cancellation
	// alone can leave a late ticker read racing the next loaded repetition.
	t.Cleanup(func() { cancel(); <-reaperDone })

	wait := func(session *pty.Session) <-chan int {
		result := make(chan int, 1)
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			rc, _ := session.Wait()
			result <- rc
		}()
		t.Cleanup(func() {
			select {
			case <-joined:
			default:
				_ = session.Signal(syscall.SIGKILL)
				<-joined
			}
		})
		return result
	}
	done, unarmedDone := wait(armed), wait(unarmed)
	select {
	case rc := <-done:
		if rc != -1 {
			t.Fatalf("hung-up run exited rc=%d, want -1 (signal death)", rc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("armed silent run was never hung up")
	}
	// Two-strike shape: the reaper needed ≥2 ticks, so the reap latch must be
	// set and the unarmed sibling must still be alive.
	a.procsMu.Lock()
	reaped := a.procs["armed"].reaped
	a.procsMu.Unlock()
	if !reaped {
		t.Fatal("armed run exited without the reaper's latch — something else killed it")
	}
	// Observe real successful NATS probes, rather than assuming a sleep means
	// the reaper got CPU time. The unarmed sibling must survive those passes.
	// origin: hot-update S5 plan §8 / testing-standards §0.4
	probesAfterReap := successfulProbes.Load()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		return successfulProbes.Load() >= probesAfterReap+3
	}) {
		t.Fatal("fixture did not observe three healthy reaper probes after hangup")
	}
	select {
	case rc := <-unarmedDone:
		t.Fatalf("unarmed run died (rc=%d) — a pre-h1 ctl's session must NEVER be reaped", rc)
	case <-time.After(500 * time.Millisecond):
		// still running — correct.
	}
}
