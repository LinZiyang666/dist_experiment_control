package tunnel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reconnectHarness wires a local HTTP server + broker tunnel server + agent
// client with fast backoff, returning the public port and a REGISTER counter.
type reconnectHarness struct {
	srv        *Server
	cli        *Client
	publicPort int
	localPort  int
	token      string
	registers  *atomic.Int64
	denyReason *atomic.Pointer[string] // non-nil → lookup returns DENY <reason> after the first OK
}

func newReconnectHarness(t *testing.T, logger *slog.Logger) *reconnectHarness {
	t.Helper()
	localPort := findFreePort(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "hello-from-agent") })
	httpSrv := &http.Server{Addr: "127.0.0.1:" + strconv.Itoa(localPort), Handler: mux}
	go func() { _ = httpSrv.ListenAndServe() }()
	t.Cleanup(func() { _ = httpSrv.Shutdown(context.Background()) })
	waitListen(t, localPort, 2*time.Second)

	controlPort := findFreePort(t)
	publicPort := findFreePort(t)
	const sid, nid = "lab", "lab-1"
	rawToken := "supersecret-reconnect-token-deadbeef"

	var registers atomic.Int64
	var denyReason atomic.Pointer[string]
	lookup := func(gotSid, gotNid string, gotPort int, gotHash string) error {
		n := registers.Add(1)
		if gotSid != sid || gotNid != nid || gotPort != publicPort || gotHash != hashToken(rawToken) {
			return errors.New("token_unknown_or_revoked")
		}
		// The first REGISTER always succeeds; a configured reason denies every
		// REGISTER thereafter (simulates revoke / proxy-off / transient fault).
		if n > 1 {
			if r := denyReason.Load(); r != nil {
				return errors.New(*r)
			}
		}
		return nil
	}

	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)), "127.0.0.1", lookup, logger)
	srvCtx, srvCancel := context.WithCancel(context.Background())
	t.Cleanup(srvCancel)
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)), sid, nid,
		func(p int) (int, error) { return localPort, nil }, logger)
	cli.SetBackoffForTest(10*time.Millisecond, 80*time.Millisecond)
	cli.Start(srvCtx)
	if err := cli.Open(publicPort, localPort, rawToken); err != nil {
		t.Fatalf("client.Open: %v", err)
	}
	return &reconnectHarness{
		srv: srv, cli: cli, publicPort: publicPort, localPort: localPort,
		token: rawToken, registers: &registers, denyReason: &denyReason,
	}
}

func (h *reconnectHarness) publicURL() string { return fmt.Sprintf("http://127.0.0.1:%d/", h.publicPort) }

func (h *reconnectHarness) waitRoundTrip(t *testing.T, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	var lastErr error
	for time.Now().Before(deadline) {
		body, err := httpGet(h.publicURL())
		if err == nil && string(body) == "hello-from-agent" {
			return
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("public round-trip never succeeded; last err=%v", lastErr)
}

// TestTunnelReconnectsAfterTransportDrop is the headline self-heal test: a
// transport drop (NOT an authoritative kill) must re-REGISTER and re-bind the
// public port so end-to-end bytes flow again — without any Open/restart.
func TestTunnelReconnectsAfterTransportDrop(t *testing.T) {
	for _, drops := range []int{1, 3} {
		t.Run(fmt.Sprintf("drops=%d", drops), func(t *testing.T) {
			h := newReconnectHarness(t, silentLog())
			h.waitRoundTrip(t, 3*time.Second) // baseline up
			before := h.registers.Load()
			for i := range drops {
				if !h.srv.DropTransport(h.publicPort) {
					t.Fatalf("drop %d: no live session to drop", i)
				}
				h.waitRoundTrip(t, 5*time.Second) // self-healed
			}
			if got := h.registers.Load(); got < before+int64(drops) {
				t.Fatalf("expected >= %d REGISTERs after %d drops, got %d", before+int64(drops), drops, got)
			}
			if !h.cli.SessionUp(h.publicPort) {
				t.Fatal("session should still be owned after successful reconnects")
			}
		})
	}
}

// TestTunnelReconnectTransientDenyRetries proves a transient DENY (try_again /
// public_port_bind_failed class) is retried, not treated terminal.
func TestTunnelReconnectTransientDenyRetries(t *testing.T) {
	for _, reason := range []string{"try_again", "public_port_bind_failed"} {
		t.Run(reason, func(t *testing.T) {
			h := newReconnectHarness(t, silentLog())
			h.waitRoundTrip(t, 3*time.Second)
			// Deny with a transient reason, then clear it ONLY AFTER observing at
			// least 2 denied REGISTERs (deterministic — no wall-clock guess).
			r := reason
			h.denyReason.Store(&r)
			before := h.registers.Load()
			h.srv.DropTransport(h.publicPort)
			deadline := time.Now().Add(3 * time.Second)
			for h.registers.Load() < before+2 {
				if time.Now().After(deadline) {
					t.Fatalf("transient DENY %q was not retried (only %d REGISTERs)", reason, h.registers.Load()-before)
				}
				time.Sleep(5 * time.Millisecond)
			}
			h.denyReason.Store(nil)           // broker recovers
			h.waitRoundTrip(t, 5*time.Second) // must self-heal (proves retry, not terminal)
			if !h.cli.SessionUp(h.publicPort) {
				t.Fatal("transient DENY must not terminate the supervisor")
			}
		})
	}
}

// TestTunnelReconnectStopsOnTerminalDeny proves an authoritative DENY stops the
// loop (no busy-loop, no resurrection) and drops the session slot.
func TestTunnelReconnectStopsOnTerminalDeny(t *testing.T) {
	for _, reason := range []string{"token_unknown_or_revoked", "session_closed", "some_unknown_future_reason"} {
		t.Run(reason, func(t *testing.T) {
			h := newReconnectHarness(t, silentLog())
			var mu sync.Mutex
			var events []bool
			h.cli.SetSessionStateHook(func(port int, up bool) {
				if port != h.publicPort {
					return
				}
				mu.Lock()
				events = append(events, up)
				mu.Unlock()
			})
			h.waitRoundTrip(t, 3*time.Second)
			r := reason
			h.denyReason.Store(&r)
			h.srv.DropTransport(h.publicPort)

			// Supervisor must terminate → slot dropped.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if !h.cli.SessionUp(h.publicPort) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if h.cli.SessionUp(h.publicPort) {
				t.Fatal("terminal DENY must drop the session slot")
			}
			// No busy-loop: REGISTER count must be bounded shortly after.
			n1 := h.registers.Load()
			time.Sleep(300 * time.Millisecond)
			if n2 := h.registers.Load(); n2-n1 > 2 {
				t.Fatalf("terminal DENY busy-looped: %d extra REGISTERs in 300ms", n2-n1)
			}
			// The down edge is the FINAL state: exactly one false, zero true —
			// a terminal-DENY node stays unready in /sub, never falsely re-ready.
			mu.Lock()
			defer mu.Unlock()
			falses, trues := 0, 0
			for _, up := range events {
				if up {
					trues++
				} else {
					falses++
				}
			}
			if falses != 1 || trues != 0 {
				t.Fatalf("terminal DENY edges: want exactly 1 down / 0 up, got %v", events)
			}
		})
	}
}

// TestTunnelReconnectFiresSessionStateCallback asserts the up/down edge
// ordering: no event on initial Open, then (false) on drop, (true) on recover.
func TestTunnelReconnectFiresSessionStateCallback(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	var mu sync.Mutex
	var events []bool
	h.cli.SetSessionStateHook(func(port int, up bool) {
		if port != h.publicPort {
			return
		}
		mu.Lock()
		events = append(events, up)
		mu.Unlock()
	})
	h.waitRoundTrip(t, 3*time.Second)
	h.srv.DropTransport(h.publicPort)
	h.waitRoundTrip(t, 5*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(events)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Settle a couple backoff windows to catch any SPURIOUS extra edge.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0] != false || events[1] != true {
		t.Fatalf("want EXACTLY [false,true] edge sequence (one per edge), got %v", events)
	}
}

// TestTunnelReconnectCtxCancelInterruptsBackoff proves a long backoff does not
// pin the supervisor: canceling the client ctx wakes it promptly.
func TestTunnelReconnectCtxCancelInterruptsBackoff(t *testing.T) {
	localPort := findFreePort(t)
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)
	const sid, nid = "lab", "lab-1"
	rawToken := "tok"
	var denyAfter atomic.Bool
	lookup := func(_, _ string, _ int, _ string) error {
		if denyAfter.Load() {
			return errors.New("try_again") // transient → keeps the loop in backoff
		}
		return nil
	}
	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)), "127.0.0.1", lookup, silentLog())
	srvCtx, srvCancel := context.WithCancel(t.Context())
	defer srvCancel()
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}
	cli := NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)), sid, nid,
		func(p int) (int, error) { return localPort, nil }, silentLog())
	cli.SetBackoffForTest(30*time.Second, 30*time.Second) // huge: only ctx-cancel can interrupt
	cliCtx, cliCancel := context.WithCancel(t.Context())
	cli.Start(cliCtx)
	if err := cli.Open(publicPort, localPort, rawToken); err != nil {
		t.Fatal(err)
	}
	denyAfter.Store(true)
	srv.DropTransport(publicPort) // → down edge → enters 30s backoff

	time.Sleep(150 * time.Millisecond) // ensure it's parked in backoff
	cliCancel()                        // must wake the select promptly

	done := make(chan struct{})
	go func() {
		for cli.SessionUp(publicPort) {
			time.Sleep(10 * time.Millisecond)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ctx-cancel did not interrupt the 30s backoff within 2s")
	}
}

// TestTunnelReconnectNeverLogsToken ensures the raw token never reaches logs
// across a drop → retry → terminal-DENY cycle.
func TestTunnelReconnectNeverLogsToken(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&syncWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := newReconnectHarness(t, logger)
	h.waitRoundTrip(t, 3*time.Second)
	r := "try_again"
	h.denyReason.Store(&r)
	h.srv.DropTransport(h.publicPort)
	time.Sleep(150 * time.Millisecond)
	term := "token_unknown_or_revoked"
	h.denyReason.Store(&term)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	logs := buf.String()
	mu.Unlock()
	if bytes.Contains([]byte(logs), []byte("supersecret-reconnect-token-deadbeef")) {
		t.Fatal("raw token leaked into logs")
	}
}

type syncWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// TestDenyIsTransient pins the terminal-vs-transient taxonomy.
func TestDenyIsTransient(t *testing.T) {
	cases := []struct {
		reason    string
		transient bool
	}{
		{"public_port_bind_failed", true},
		{"try_again", true},
		{"token_unknown_or_revoked", false},
		{"session_closed", false},
		{"malformed_register", false},
		{"", false},
		{"anything_unknown", false},
	}
	for _, c := range cases {
		if got := denyIsTransient(c.reason); got != c.transient {
			t.Errorf("denyIsTransient(%q)=%v, want %v", c.reason, got, c.transient)
		}
	}
}

// pollGoroutinesAtMost polls runtime.NumGoroutine down to <= ceiling for up to
// `within`, returning the last sample. Mirrors the concurrency-suite floor.
func pollGoroutinesAtMost(ceiling int, within time.Duration) int {
	deadline := time.Now().Add(within)
	var last int
	for time.Now().Before(deadline) {
		runtime.GC()
		last = runtime.NumGoroutine()
		if last <= ceiling {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}

// TestTunnelReconnectChurnNoGoroutineLeak: many drop/reconnect cycles must not
// accumulate goroutines — the supervisor is reused across redials (1 per open
// port), and per-session goroutines are 1:1 replaced. A reconnect leak would
// grow the count unboundedly with K.
func TestTunnelReconnectChurnNoGoroutineLeak(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	h.waitRoundTrip(t, 3*time.Second)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const K = 20
	for range K {
		if !h.srv.DropTransport(h.publicPort) {
			t.Fatal("no live session to drop")
		}
		h.waitRoundTrip(t, 5*time.Second)
	}
	if got := pollGoroutinesAtMost(baseline+2, 3*time.Second); got > baseline+2 {
		buf := make([]byte, 64*1024)
		n := runtime.Stack(buf, true)
		t.Fatalf("reconnect churn leaked goroutines: baseline=%d after %d cycles=%d\n%s", baseline, K, got, buf[:n])
	}
}

// TestTunnelReconnectVsConcurrentOpenSamePort: a drop-driven redial racing a
// concurrent Open on the SAME port must converge to exactly ONE owner (the
// later Open's generation), with the loser's transport closed — no leak, no
// double-owner, public port still works. Run under -race.
func TestTunnelReconnectVsConcurrentOpenSamePort(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	h.waitRoundTrip(t, 3*time.Second)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); h.srv.DropTransport(h.publicPort) }()
	go func() {
		defer wg.Done()
		// A fresh full-rebuild Open on the same port (as applyProxyDirective does).
		_ = h.cli.Open(h.publicPort, h.localPort, h.token)
	}()
	wg.Wait()

	h.waitRoundTrip(t, 5*time.Second) // exactly one owner must serve
	if !h.cli.SessionUp(h.publicPort) {
		t.Fatal("port must still be owned after the race")
	}
	if got := pollGoroutinesAtMost(baseline+2, 3*time.Second); got > baseline+2 {
		buf := make([]byte, 64*1024)
		n := runtime.Stack(buf, true)
		t.Fatalf("reconnect-vs-Open race leaked goroutines: baseline=%d got=%d\n%s", baseline, got, buf[:n])
	}
}

// TestTunnelReconnectClosesPredecessorConn pins B1: swapTransport must Close the
// predecessor conn on a successful redial, or a half-open drop leaks one fd per
// reconnect. White-box: capture the pre-drop conn, reconnect, assert it's closed.
func TestTunnelReconnectClosesPredecessorConn(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	h.waitRoundTrip(t, 3*time.Second)

	h.cli.mu.Lock()
	old := h.cli.sessions[h.publicPort].conn
	h.cli.mu.Unlock()
	if old == nil {
		t.Fatal("no live conn captured")
	}

	h.srv.DropTransport(h.publicPort)
	h.waitRoundTrip(t, 5*time.Second) // reconnected → swapTransport ran

	// A closed net.Conn errors on Write. Poll briefly to ride out async close.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = old.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := old.Write([]byte("x")); err != nil {
			return // closed — correct
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("predecessor conn still writable after reconnect — swapTransport leaked it")
}

// TestDenyErrorIsRegisterDenied pins errors.Is wrapping.
func TestDenyErrorIsRegisterDenied(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &DenyError{Reason: "token_unknown_or_revoked"})
	if !errors.Is(err, ErrRegisterDenied) {
		t.Fatal("DenyError must satisfy errors.Is(ErrRegisterDenied)")
	}
	var de *DenyError
	if !errors.As(err, &de) || de.Reason != "token_unknown_or_revoked" {
		t.Fatalf("errors.As failed to recover reason; de=%v", de)
	}
}
