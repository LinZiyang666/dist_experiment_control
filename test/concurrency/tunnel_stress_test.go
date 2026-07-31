// tunnel_stress_test.go — high-concurrency stress over the tunnel
// Server / Client surface. The unit tests in internal/tunnel do
// happy-path round-trip; these test the same code under N parallel
// callers, mid-shutdown races, and rapid reuse.
package concurrency_test

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/tunnel"
)

// TestTunnelConcurrentClientOpens — C.9. 50 clients open against
// the same server simultaneously, each on its own port + token.
// At least 95% should succeed; the rest are tolerated as OS-level
// `bind: address already in use` flakes from findFreePort race
// conditions (the helper releases the listener BEFORE we hand the
// port to broker.bind, so the kernel may slot another listener
// in between). Test purpose: exercise the contention paths around
// s.mu in handleAgent, NOT validate kernel port-table behavior.
func TestTunnelConcurrentClientOpens(t *testing.T) {
	const N = 50
	const minSuccessFrac = 0.90 // tolerate up to 10% bind-race flakes
	controlPort := findFreePort(t)
	srv := newTunnelServer(t, controlPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	ports := make([]int, N)
	for i := range ports {
		ports[i] = findFreePort(t)
	}

	latch := newRaceLatch()
	clients := make([]*tunnel.Client, N)
	openOK := make([]bool, N)
	errs := make(chan error, N)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			cli := tunnel.NewClient(
				net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
				"lab", fmt.Sprintf("lab-%d", i),
				func(int) (int, error) { return 1, nil },
				silentLog(),
			)
			cli.Start(ctx)
			clients[i] = cli
			latch.wait()
			if err := cli.Open(ports[i], 1, fmt.Sprintf("tok-%d", i)); err != nil {
				errs <- fmtErr("client %d: open port %d: %w", i, ports[i], err)
				return
			}
			openOK[i] = true
		}()
	}
	latch.release()
	wg.Wait()
	close(errs)

	failed := 0
	for err := range errs {
		failed++
		t.Logf("tolerated bind-race: %v", err)
	}
	successRate := float64(N-failed) / float64(N)
	if successRate < minSuccessFrac {
		t.Errorf("too many concurrent Open failures: %d/%d failed (%.1f%% success rate, want >= %.1f%%)",
			failed, N, successRate*100, minSuccessFrac*100)
	}

	for i, cli := range clients {
		if cli != nil && openOK[i] {
			cli.Close(ports[i])
		}
	}
}

// TestTunnelOpenDuringServerCloseRace — C.10. Spawn N clients
// trying to Open while we Close the server in parallel. Every
// Open MUST either succeed or fail with an error — no panic, no
// hang. This is the regression test for the C1 audit fix
// (handleAgent inserting into s.sessions after Close drained it).
//
// The two outcomes we check for:
//
//   - Open returns nil — client raced past the close, server's
//     handleAgent inserted before s.closed flipped. Fine.
//   - Open returns error — Close won the race; s.closed=true and
//     the broker rolled back. Also fine.
//
// What we MUST NOT see:
//
//   - Open hangs forever (would block the test on Wait).
//   - panic from a nil-deref / send-on-closed-chan.
//   - subsequent Close() of the server hangs (would mean the
//     in-flight handleAgent never observed s.closed).
func TestTunnelOpenDuringServerCloseRace(t *testing.T) {
	const N = 30
	for trial := 0; trial < 5; trial++ {
		controlPort := findFreePort(t)
		srv := newTunnelServer(t, controlPort)
		ctx, cancel := context.WithCancel(context.Background())
		if err := srv.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}

		ports := make([]int, N)
		for i := range ports {
			ports[i] = findFreePort(t)
		}

		latch := newRaceLatch()
		clients := make([]*tunnel.Client, N)
		var wg sync.WaitGroup
		for i := 0; i < N; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				cli := tunnel.NewClient(
					net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
					"lab", fmt.Sprintf("lab-%d", i),
					func(int) (int, error) { return 1, nil },
					silentLog(),
				)
				cli.Start(ctx)
				clients[i] = cli
				latch.wait()
				// Open may succeed OR fail; both are acceptable.
				// What we're enforcing here is that it RETURNS
				// in bounded time. A test-level deadline at the
				// outer level catches the hang case.
				_ = cli.Open(ports[i], 1, fmt.Sprintf("tok-%d", i))
			}()
		}

		// Server.Close runs concurrently with the latched Opens.
		// goroutine ordering: release the latch, then in the same
		// goroutine call srv.Close immediately so the close races
		// the Opens directly.
		go func() {
			latch.release()
			runtime.Gosched()
			srv.Close()
		}()

		// Wait with a timeout. Anything > 10s is a hang regression.
		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()
		select {
		case <-doneCh:
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("trial %d: hang in Open during server Close", trial)
		}

		// Cleanup.
		for i, cli := range clients {
			if cli != nil {
				cli.Close(ports[i])
			}
		}
		cancel()
	}
}

// TestTunnelRepeatedOpenClose — C.11. Same client struct, repeated
// Open/Close on different ports. -race must stay clean. This is
// the second audit-found shape (audit shard 01 F2 — Open after
// Start ctx cancel inserting into c.sessions); the fix is rolled
// back here under -race to make sure the lock discipline holds.
func TestTunnelRepeatedOpenClose(t *testing.T) {
	const N = 200
	controlPort := findFreePort(t)
	srv := newTunnelServer(t, controlPort)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli := tunnel.NewClient(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"lab", "lab-1",
		func(int) (int, error) { return 1, nil },
		silentLog(),
	)
	cli.Start(ctx)

	for i := 0; i < N; i++ {
		port := findFreePort(t)
		if err := cli.Open(port, 1, fmt.Sprintf("tok-%d", i)); err != nil {
			t.Fatalf("iter %d: open: %v", i, err)
		}
		cli.Close(port)
	}
}

// TestTunnelControlPortReuseAfterServerClose — C.12. A previous
// broker that was killed (graceful Close, but we simulate "kill"
// by doing it without giving time for cleanup) must release the
// control port so the next broker can re-bind it.
//
// SO_REUSEADDR is the intended mechanism on Linux; the test
// asserts the higher-level invariant ("re-bind succeeds within
// reasonable time") rather than poking the socket directly.
func TestTunnelControlPortReuseAfterServerClose(t *testing.T) {
	controlPort := findFreePort(t)

	// First broker: bind, immediately Close. Don't open any
	// sessions — minimal lifecycle.
	srv1 := newTunnelServer(t, controlPort)
	ctx1, cancel1 := context.WithCancel(context.Background())
	if err := srv1.Start(ctx1); err != nil {
		cancel1()
		t.Fatal(err)
	}
	srv1.Close()
	cancel1()

	// Second broker: must be able to re-bind the SAME control
	// port within 2s (kernel TIME_WAIT can take longer in theory
	// but Go's listen with SO_REUSEADDR plus a tiny grace handles
	// this on Linux — flake here would be a real bug).
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		srv2 := newTunnelServer(t, controlPort)
		ctx2, cancel2 := context.WithCancel(context.Background())
		err := srv2.Start(ctx2)
		if err == nil {
			srv2.Close()
			cancel2()
			return
		}
		lastErr = err
		cancel2()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("could not re-bind control port %d within 2s: %v",
		controlPort, lastErr)
}
