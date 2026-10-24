package cli

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestConnectNATSWithNkeyRejectsNilIdentity(t *testing.T) {
	if _, err := ConnectNATSWithNkey("nats://127.0.0.1:4222", nil); err == nil {
		t.Fatal("expected error for nil identity")
	}
}

// Closed port — exercises the underlying nats.Connect() path and confirms
// the helper passes the URL through (rather than e.g. silently succeeding).
func TestConnectNATSWithNkeyFailsOnUnreachable(t *testing.T) {
	id := &Identity{
		PublicKey:   "U" + strings.Repeat("A", 55),
		Fingerprint: "SHA256:" + strings.Repeat("a", 43),
		// Seed left empty: P3 helper doesn't sign anything (no nkey credentials
		// are sent), so a nil seed is fine.
	}
	if _, err := ConnectNATSWithNkey("nats://127.0.0.1:1", id); err == nil {
		t.Fatal("expected connect error to closed port")
	}
}

// origin: hot-update S1 plan §1.5 (gotcha #89 family note). Every ctl
// connection must keep reconnecting through the auth_callout black window a
// broker restart opens: without IgnoreAuthErrorAbort nats.go permanently
// closes the connection after two consecutive auth errors, and a mid-stream
// `exec` dies instead of resuming. Mutation: drop either option → red.
func TestCtlConnOptionsIgnoreAuthErrorAbort(t *testing.T) {
	opts := nats.GetDefaultOptions()
	for _, o := range ctlConnOptions() {
		if err := o(&opts); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	if !opts.IgnoreAuthErrorAbort {
		t.Fatal("ctl connections must set IgnoreAuthErrorAbort: two auth errors during a broker restart would otherwise close the connection for good")
	}
	if opts.MaxReconnect != -1 {
		t.Fatalf("ctl connections must reconnect forever (MaxReconnect=-1), got %d", opts.MaxReconnect)
	}
}

// A real nats.go handshake can encounter the server keepalive before CONNECT's
// PONG during a slow callout. Only that exact incomplete exchange admits retry;
// authorization and other protocol errors retain their original classifications.
// origin: hot-update S6 plan §8.2 / T50 / COHORT-COLLECTOR
func TestConnectWithinKeepsEarlyPingRetryable(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	t.Setenv(DevNoAuthEnv, "1")
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseConnectEarlyPing(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "early handshake PING x5", g)
	testharness.AssertNoFDLeak(t, "early handshake PING x5", fds, 0)
}
func exerciseConnectEarlyPing(t *testing.T) {
	t.Helper()
	for _, reply := range []string{"PING\r\n", "INFO {}\r\n", "-ERR 'Authorization Violation'\r\n"} {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			_, _ = io.WriteString(conn, "INFO {\"server_id\":\"early-ping\",\"max_payload\":1048576,\"proto\":1}\r\n")
			reader := bufio.NewReader(conn)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if line == "PING\r\n" {
					_, _ = io.WriteString(conn, reply)
					return
				}
			}
		}()
		deadline, err := controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		nc, connectErr := ConnectWithin("nats://"+listener.Addr().String(), &Identity{}, deadline, nil)
		if nc != nil {
			nc.Close()
		}
		_ = listener.Close()
		<-done
		if nc != nil || connectErr == nil {
			t.Fatal("negative handshake unexpectedly connected")
		}
		if errors.Is(connectErr, nats.ErrTimeout) != (reply == "PING\r\n") {
			t.Errorf("handshake classification changed: reply=%q err=%v", reply, connectErr)
		}
	}
}
