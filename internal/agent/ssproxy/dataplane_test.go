package ssproxy

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/tunnel"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return p
}

// Round-2 F6 (combined data plane): prove the real end-to-end path a Clash
// consumer uses — SS client -> broker public port -> yamux tunnel -> agent SS
// server -> target -> back. Exercises ssproxy + the real internal/tunnel
// together (the two halves were proven separately; this is the join).
func TestDataPlaneSSOverTunnelRoundTrip(t *testing.T) {
	echoAddr, stopEcho := startEcho(t)
	defer stopEcho()
	log := slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	// Agent: embedded SS server on a loopback port.
	srv := New(nil)
	localSS, err := srv.Start(0, []Key{{KeyID: "alice", Secret: "alice-psk"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	controlPort := freeTCPPort(t)
	publicPort := freeTCPPort(t)

	// Broker tunnel server (auth allows any token here; the broker's real
	// TokenLookup is tested elsewhere).
	tsrv := tunnel.NewServer(net.JoinHostPort("127.0.0.1", itoa(controlPort)), "127.0.0.1",
		func(_, _ string, _ int, _ string, _ int64) error { return nil }, log)
	if err := tsrv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer tsrv.Close()

	// Agent tunnel client: maps the public port to the local SS port.
	tcli := tunnel.NewClient(net.JoinHostPort("127.0.0.1", itoa(controlPort)), "lab", "lab-1",
		func(int) (int, error) { return localSS, nil }, log)
	tcli.Start(ctx)
	if err := tcli.Open(publicPort, localSS, "token"); err != nil {
		t.Fatalf("tunnel open: %v", err)
	}

	// Give the public listener a moment to bind.
	time.Sleep(100 * time.Millisecond)

	// A Clash-style SS client connects to the BROKER PUBLIC PORT (not the SS
	// server directly) and egresses to the echo target through the tunnel.
	cl := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(publicPort)), "alice-psk", echoAddr)
	defer func() { _ = cl.conn.Close() }()
	msg := []byte("through-the-tunnel")
	cl.send(t, msg)
	got := cl.recv(t, len(msg))
	if !bytes.Equal(got, msg) {
		t.Fatalf("data plane round-trip mismatch: got %q want %q", got, msg)
	}

	// A wrong PSK over the same public path must be refused.
	bad := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(publicPort)), "WRONG-psk", echoAddr)
	defer func() { _ = bad.conn.Close() }()
	assertRejected(t, bad, "wrong psk over tunnel")
}
