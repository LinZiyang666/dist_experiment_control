//go:build linux

package clustermanifest

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
func TestBrokerManifestLeakCycles(t *testing.T) {
	// Initialize netpoll before measuring its permanent process descriptors.
	exerciseManifestInheritedHTTP(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseManifestInheritedHTTP(t)
	}
	testharness.AssertNoGoroutineLeak(t, "clustermanifest inherited HTTP", beforeG)
	testharness.AssertNoFDLeak(t, "clustermanifest inherited HTTP", beforeFD, 0)
}

func exerciseManifestInheritedHTTP(t *testing.T) {
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
	k, err := listenerimage.Capture(ln, "clustermanifest")
	if err != nil {
		if k != nil {
			_ = k.Close()
		} else {
			_ = ln.Close()
		}
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "k.Close", k.Close)
	original := k.Descriptor()
	var current atomic.Int32
	current.Store(1)
	owner, err := StartImage(k, func() ([]byte, bool) { return []byte(fmt.Sprintf(`{"generation":%d}`, current.Load())), true }, deadline())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(deadline()) }()
	request := func(conn net.Conn, want string) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\n\r\n", ManifestPath); err != nil {
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
	defer checkImageCleanup(t, "conn.Close", conn.Close)
	request(conn, `{"generation":1}`)
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
	defer checkImageCleanup(t, "fresh.Close", fresh.Close)
	file, err := k.Detach()
	if err != nil {
		t.Fatal(err)
	}
	next, err := listenerimage.Adopt(desc, file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "next.Close", next.Close)
	current.Store(2)
	newer, err := StartImage(next, func() ([]byte, bool) { return []byte(fmt.Sprintf(`{"generation":%d}`, current.Load())), true }, deadline())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newer.Stop(deadline()) }()
	// Cleanup of the detached generation cannot kill the inherited listener.
	if err := owner.Stop(deadline()); err != nil {
		t.Fatal(err)
	}
	request(fresh, `{"generation":2}`)
	if err := newer.Stop(deadline()); err != nil {
		t.Fatal(err)
	}
	probe, err := net.DialTimeout("tcp", original.Address, 200*time.Millisecond)
	if err == nil {
		checkImageCleanup(t, "probe.Close", probe.Close)
		t.Fatal("normal stop retained listener")
	}
}

// Cleanup still runs after a failed assertion. Report unexpected close errors
// without aborting the remaining defers; fenced sockets may already be closed.
// origin: hot-update S6 plan §3.2 / T55
func checkImageCleanup(t *testing.T, name string, closeOwner func() error) {
	t.Helper()
	if err := closeOwner(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("%s: %v", name, err)
	}
}
