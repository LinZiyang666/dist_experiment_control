//go:build linux

package brokermetrics

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §6.1 / §12.1 P09
func TestBrokerMetricsLeakCycles(t *testing.T) {
	// Initialize netpoll before measuring its permanent process descriptors.
	exerciseMetricsInheritedHTTP(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseMetricsInheritedHTTP(t)
	}
	testharness.AssertNoGoroutineLeak(t, "brokermetrics inherited HTTP", beforeG)
	testharness.AssertNoFDLeak(t, "brokermetrics inherited HTTP", beforeFD, 0)
}

func exerciseMetricsInheritedHTTP(t *testing.T) {
	t.Helper()
	deadline := func() controlbudget.Deadline {
		now, err := rawsocket.BootNow()
		if err != nil {
			t.Fatal(err)
		}
		return controlbudget.Deadline(now + int64(5*time.Second))
	}
	ln, err := Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	k, err := listenerimage.Capture(ln, "brokermetrics")
	if err != nil {
		if k != nil {
			_ = k.Close()
		} else {
			_ = ln.Close()
		}
		t.Fatal(err)
	}
	defer closeMetricsTestResource(t, k)
	original := k.Descriptor()
	var current atomic.Int32
	current.Store(1)
	owner, err := StartImage(k, func() Snapshot { return Snapshot{AlertsActive: int(current.Load())} }, func() (bool, string) { return true, "ready" }, deadline())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(deadline()) }()
	request := func(conn net.Conn, want string) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\n\r\n", "/metrics"); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Fatalf("inherited HTTP did not serve current generation: status=%d err=%v", resp.StatusCode, err)
		}
	}
	dial := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp", original.Address, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		return conn
	}
	conn := dial()
	defer closeMetricsTestResource(t, conn)
	request(conn, `tether_broker_alerts_active 1`)
	if err := owner.Fence(deadline()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Join(deadline()); err != nil {
		t.Fatal(err)
	}
	desc, _, err := k.Export()
	if err != nil {
		t.Fatal(err)
	}
	if desc != original {
		t.Fatal("HTTP listener identity changed")
	}
	// Connect while no Accept view is running. The original keeper must hold
	// LISTEN until the next owner can serve an actual response on this socket.
	fresh := dial()
	defer closeMetricsTestResource(t, fresh)
	file, err := k.Detach()
	if err != nil {
		t.Fatal(err)
	}
	next, err := listenerimage.Adopt(desc, file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer closeMetricsTestResource(t, next)
	current.Store(2)
	newer, err := StartImage(next, func() Snapshot { return Snapshot{AlertsActive: int(current.Load())} }, func() (bool, string) { return true, "ready" }, deadline())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newer.Stop(deadline()) }()
	// Cleanup of the detached generation cannot kill the inherited listener.
	if err := owner.Stop(deadline()); err != nil {
		t.Fatal(err)
	}
	request(fresh, `tether_broker_alerts_active 2`)
	if err := newer.Stop(deadline()); err != nil {
		t.Fatal(err)
	}
	probe, err := net.DialTimeout("tcp", original.Address, 200*time.Millisecond)
	if err == nil {
		closeMetricsTestResource(t, probe)
		t.Fatal("normal stop retained listener")
	}
}

// origin: hot-update S6 plan §11 / §12.1 P09
// Terminal-stop assertions close some resources before deferred cleanup runs.
// Only an already-closed resource is benign; other release failures fail the test.
func closeMetricsTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close test resource: %v", err)
	}
}
