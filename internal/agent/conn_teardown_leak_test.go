package agent

import (
	"context"
	"log/slog"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: gotcha #72 — the LEAK half of the bounded-teardown contract, and the structural reason the
// forbidden fake fix cannot pass.
//
// "Wait a bit, then abandon the closer goroutine" makes a hang look like a recovery while leaking a
// goroutine and its socket on every teardown. This test drives 20 WEDGED teardowns (a connection
// whose close parks until the socket is poisoned — the field shape) and asserts the process returns
// to its goroutine and fd baseline. An abandoned closer cannot satisfy that, and the poison step is
// what makes the honest join bounded.
//
// Lives here rather than in test/concurrency because the ladder is package-internal: exporting a
// production seam purely so an external test could reach it would be worse than delegating to the
// SAME leak gate implementation test/concurrency uses (internal/testharness — deliberately not
// goleak, CLAUDE.md §5).

// numOpenFDs counts this process's descriptors. Linux-only in practice; on other platforms the
// directory is absent and the fd half of the assertion is skipped rather than faked.
func numOpenFDs(t *testing.T) (int, bool) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func TestWedgedTeardownRepeatsWithoutLeaking(t *testing.T) {
	// A real listener so each round consumes a REAL fd — a wedged closer that never returns would
	// hold its socket open and show up in the fd baseline, not just the goroutine count.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	// Close each accepted conn immediately: the SERVER side's descriptors are the fixture's own
	// bookkeeping, and holding them would make the fd assertion below measure this test instead of
	// the teardown ladder.
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			_ = c.Close()
		}
	}()

	oldClose, oldGrace := closeBudget, poisonGrace
	closeBudget, poisonGrace = 40*time.Millisecond, 200*time.Millisecond
	defer func() { closeBudget, poisonGrace = oldClose, oldGrace }()

	runtime.GC()
	gBefore := runtime.NumGoroutine()
	fdBefore, haveFD := numOpenFDs(t)

	const rounds = 20
	for i := 0; i < rounds; i++ {
		raw, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			t.Fatalf("round %d dial: %v", i, derr)
		}
		wc := &blockingConn{dead: make(chan struct{})}
		// Poisoning the wrapper must also drop the real socket, or the fd assertion below would be
		// measuring the test's own bookkeeping instead of the ladder's.
		wc.onRelease = func() { _ = raw.Close() }

		a := &Agent{cfg: Config{Logger: slog.New(slog.DiscardHandler), SID: "lab", NID: "n1"}}
		a.cfg.TeardownCloseFn = func(*nats.Conn) { <-wc.dead } // parks until poisoned
		tr := newConnTracker(context.Background(), func(context.Context, string, string) (net.Conn, error) {
			return wc, nil
		})
		if _, terr := tr.Dial("tcp", ln.Addr().String()); terr != nil {
			t.Fatalf("round %d tracker dial: %v", i, terr)
		}
		cancelled := false
		fin := &sessionFinalizer{tracker: tr, agent: a, cancel: func() { cancelled = true }}
		fin.Do(teardownRebuild)
		if !cancelled {
			t.Fatalf("round %d: the ladder did not cancel the session ctx", i)
		}
	}

	testharness.AssertNoGoroutineLeak(t, "agent wedged teardown x20", gBefore)
	if haveFD {
		// Poll: the kernel reaps closed sockets asynchronously.
		after := fdBefore
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			n, _ := numOpenFDs(t)
			after = n
			if after <= fdBefore+2 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if after > fdBefore+2 {
			t.Errorf("fd leak across %d wedged teardowns: before=%d after=%d — an abandoned closer holds its socket",
				rounds, fdBefore, after)
		}
	}
}
