package agent

// ctl_liveness_test.go — the h1 D reaper: the pure decision table, the grace
// derivation, and the integration path (a real PTY child hung up after
// sustained keepalive silence; an unarmed run untouchable).
// origin: docs/reviews/h1-plan.md workstream D (zombie class b).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
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
	oldTick := ctlLivenessTick
	ctlLivenessTick = 30 * time.Millisecond
	t.Cleanup(func() { ctlLivenessTick = oldTick })

	t.Run("conn reports disconnected", func(t *testing.T) {
		url := startNATS(t)
		a := courierAgent(t, url)
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := s.Start([]string{"sleep", "30"}, nil, "", ""); err != nil {
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
		url := startNATS(t)
		a := courierAgent(t, url)
		if nc := a.ncBox.Load(); !nc.IsConnected() {
			t.Fatal("fixture precondition: the conn must report CONNECTED")
		}
		oldProbe := ctlConnProbe
		ctlConnProbe = func(*nats.Conn) error { return errors.New("blackholed: no round trip") }
		t.Cleanup(func() { ctlConnProbe = oldProbe })

		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := s.Start([]string{"sleep", "30"}, nil, "", ""); err != nil {
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
	url := startNATS(t)
	a := courierAgent(t, url) // real conn in ncBox (IsConnected + Flush work)

	oldTick := ctlLivenessTick
	ctlLivenessTick = 50 * time.Millisecond
	t.Cleanup(func() { ctlLivenessTick = oldTick })

	mkSession := func(argv []string) *pty.Session {
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		if err := s.Start(argv, nil, "", ""); err != nil {
			t.Fatal(err)
		}
		return s
	}

	armed := mkSession([]string{"sleep", "30"})
	unarmed := mkSession([]string{"sleep", "30"})
	a.procsMu.Lock()
	a.procs["armed"] = &procRec{sess: armed, kaGrace: 200 * time.Millisecond, lastKA: time.Now().Add(-time.Hour)}
	a.procs["unarmed"] = &procRec{sess: unarmed, kaGrace: 0, lastKA: time.Now().Add(-time.Hour)}
	a.procsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.ctlLivenessReaper(ctx)

	done := make(chan int, 1)
	go func() {
		rc, _ := armed.Wait()
		done <- rc
	}()
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
	time.Sleep(300 * time.Millisecond) // several more reaper ticks over the unarmed run
	unarmedDone := make(chan int, 1)
	go func() {
		rc, _ := unarmed.Wait()
		unarmedDone <- rc
	}()
	select {
	case rc := <-unarmedDone:
		t.Fatalf("unarmed run died (rc=%d) — a pre-h1 ctl's session must NEVER be reaped", rc)
	case <-time.After(500 * time.Millisecond):
		// still running — correct.
	}
}
