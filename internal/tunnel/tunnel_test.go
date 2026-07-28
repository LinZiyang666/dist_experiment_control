package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// findFreePort grabs an ephemeral port the kernel hands us, then
// closes the listener so the test can reopen it via the tunnel
// machinery. Race-prone but acceptable for a single-process test.
func findFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// B9: delegates to internal/testharness. See the note on internal/broker's silentLogger — same body,
// same reasoning, and the shared version's TETHER_TEST_VERBOSE hatch is what you want when a fence or
// reconnect test fails.
func silentLog() *slog.Logger {
	return testharness.SilentLog()
}

// TestTunnelRoundTripsHTTP is the architecture-mandated P6 test
// without the rest of tetherd around it: agent runs an HTTP server on
// localPort; tunnel maps it to publicPort; an HTTP client hitting
// publicPort gets the agent's response body.
func TestTunnelRoundTripsHTTP(t *testing.T) {
	// 1. Local "agent" HTTP server.
	localPort := findFreePort(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello-from-agent")
	})
	httpSrv := &http.Server{
		Addr:    "127.0.0.1:" + strconv.Itoa(localPort),
		Handler: mux,
	}
	go func() { _ = httpSrv.ListenAndServe() }()
	defer func() { _ = httpSrv.Shutdown(context.Background()) }()
	waitListen(t, localPort, 1*time.Second)

	// 2. Broker tunnel server on a free control port.
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	const sid, nid = "lab", "lab-1"
	rawToken := "test-token-deadbeef"

	// Token lookup: only the configured (sid,nid,port,token) is OK.
	lookup := func(gotSid, gotNid string, gotPort int, gotHash string, _ int64) error {
		if gotSid != sid || gotNid != nid || gotPort != publicPort || gotHash != hashToken(rawToken) {
			return errors.New("token_mismatch")
		}
		return nil
	}
	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1", lookup, silentLog())
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}

	// 3. Agent client opens a tunnel from public→local.
	cli := NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		sid, nid, func(p int) (int, error) { return localPort, nil }, silentLog())
	cli.Start(srvCtx)
	if err := cli.Open(publicPort, localPort, rawToken); err != nil {
		t.Fatalf("client.Open: %v", err)
	}

	// 4. Hit the public port and verify the body comes from our local
	// HTTP server. Try a couple times to ride out tunnel-setup races.
	url := fmt.Sprintf("http://127.0.0.1:%d/", publicPort)
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		body, err := httpGet(url)
		if err == nil && string(body) == "hello-from-agent" {
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("public GET never succeeded; last err=%v", lastErr)
}

func TestTunnelDeniesBadToken(t *testing.T) {
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	rejected := func(_, _ string, _ int, _ string, _ int64) error {
		return errors.New("token_mismatch")
	}
	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1", rejected, silentLog())
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"lab", "lab-1", func(p int) (int, error) { return 0, nil }, silentLog())
	cli.Start(srvCtx)
	err := cli.Open(publicPort, 1, "bogus")
	if err == nil {
		t.Fatal("Open with bad token must return DENY error")
	}
}

// TestTunnelControlRequiresTLS pins the F3 fix: plain-text TCP can't
// complete REGISTER. A passive eavesdropper between agent and broker
// therefore can't observe the bearer token. We verify this by dialing
// the control listener with raw net.Dial (not tls.Dial), writing a
// well-formed REGISTER line, and asserting we never get the OK ack
// the protocol promises on success — the TLS handshake fails first.
func TestTunnelControlRequiresTLS(t *testing.T) {
	controlPort := findFreePort(t)
	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1",
		func(_, _ string, _ int, _ string, _ int64) error { return nil },
		silentLog())
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(controlPort), 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Write what would have been a valid REGISTER over a plain TCP
	// stream. The TLS handshake on the server side will treat this
	// as garbage and the connection should error out before we ever
	// see the OK reply.
	_, _ = conn.Write([]byte("REGISTER lab lab-1 14000 sometoken\n"))

	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	got := string(buf[:n])
	if err == nil && strings.HasPrefix(got, "OK") {
		t.Fatalf("plaintext TCP reached OK ack — control channel is not TLS-only; got %q", got)
	}
	// Either an error or a non-OK byte stream is acceptable here:
	// the only failure mode we care about is "the server completed
	// the protocol over plaintext", and that's the assertion above.
}

func TestTunnelClosesPublicPortOnSessionDrop(t *testing.T) {
	localPort := findFreePort(t)
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	const sid, nid = "lab", "lab-1"
	rawToken := "ok"
	lookup := func(_, _ string, _ int, _ string, _ int64) error { return nil }

	srv := NewServer(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1", lookup, silentLog())
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	if err := srv.Start(srvCtx); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		sid, nid, func(p int) (int, error) { return localPort, nil }, silentLog())
	cli.Start(srvCtx)
	if err := cli.Open(publicPort, localPort, rawToken); err != nil {
		t.Fatal(err)
	}
	// Public port should be reachable (well, it accepts; no body since
	// no local server, that's fine for THIS test).
	c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(publicPort), 1*time.Second)
	if err != nil {
		t.Fatalf("public dial after Open: %v", err)
	}
	_ = c.Close()

	// Close client side; server should drop public listener.
	cli.Close(publicPort)

	// Give the server a moment to clean up.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		c2, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(publicPort), 100*time.Millisecond)
		if err != nil {
			return // expected: server tore down public port
		}
		_ = c2.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("public port still reachable after client.Close")
}

// ----- tiny test helpers ----------------------------------------------------

func waitListen(t *testing.T, port int, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port %d never listened", port)
}

func httpGet(url string) ([]byte, error) {
	cli := &http.Client{Timeout: 1 * time.Second}
	resp, err := cli.Get(url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(resp.Body)
}
