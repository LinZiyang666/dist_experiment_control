package main

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// These are actual loopback TCP/TLS peers, not NATS/Caddy deployment receipts.
// origin: hot-update S9 plan §2.3 / §5.1 / L22; S9 INSTRUMENTS-3.
func TestPhysicalProtocolProbeKeepsExactConnectionAndClosesFailures(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProtocolProbe(t)
	}
	testharness.AssertNoGoroutineLeak(t, "physical protocol probe", beforeG)
	testharness.AssertNoFDLeak(t, "physical protocol probe", beforeFD, 0)
}

func exerciseProtocolProbe(t *testing.T) {
	t.Helper()
	for _, greeting := range []string{"INFO {\"server_id\":\"actual-loopback-peer\"}\r\n", "INFO {}\r\n", "HTTP/1.1 200 OK\r\n"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			peer, err := listener.Accept()
			if err != nil {
				return
			}
			defer closeTestResource(t, peer)
			// Separate writes exercise a legal fragmented TCP greeting.
			for _, part := range []string{greeting[:2], greeting[2:]} {
				_, _ = io.WriteString(peer, part)
			}
			_, _ = io.Copy(io.Discard, peer)
		}()
		deadline, err := controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		// ctx-root: hermetic protocol probe exercise.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		held, err := serviceProbe(ctx, probe{Address: listener.Addr().String(), Protocol: "nats"}, deadline)
		good := greeting == "INFO {\"server_id\":\"actual-loopback-peer\"}\r\n"
		if good && (err != nil || held == nil) {
			t.Fatalf("fragmented greeting refused: %v", err)
		}
		if !good && err == nil {
			t.Fatal("unrelated or incomplete greeting certified readiness")
		}
		if held != nil {
			_ = held.Close()
		}
		cancel()
		_ = listener.Close()
		<-done
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"example.com", "wrong.invalid"} {
		deadline, err := controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		// ctx-root: hermetic TLS peer exercise.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		held, err := serviceProbe(ctx, probe{Address: server.Listener.Addr().String(), Protocol: "tls", ServerName: name, CAFile: ca}, deadline)
		if name == "example.com" && (err != nil || held == nil) {
			t.Fatalf("trusted real TLS peer refused: %v", err)
		}
		if name == "wrong.invalid" && err == nil {
			t.Fatal("wrong TLS name certified readiness")
		}
		if held != nil {
			_ = held.Close()
		}
		cancel()
	}
	server.Close()
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
