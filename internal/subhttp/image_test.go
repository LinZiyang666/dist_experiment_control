//go:build linux

package subhttp

import (
	"bufio"
	"context"
	"database/sql"
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
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §6.1 / T20
func TestSubHTTPUsesCurrentStoreAfterThaw(t *testing.T) { exerciseSubHTTPInheritedHTTP(t) }

// origin: hot-update S6 plan §11 / §12.1 P09
func TestBrokerSubHTTPLeakCycles(t *testing.T) {
	exerciseSubHTTPInheritedHTTP(t) // initialize permanent netpoll state
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSubHTTPInheritedHTTP(t)
	}
	testharness.AssertNoGoroutineLeak(t, "subscription HTTP thaw", beforeG)
	testharness.AssertNoFDLeak(t, "subscription HTTP thaw", beforeFD, 0)
}

func exerciseSubHTTPInheritedHTTP(t *testing.T) {
	t.Helper()
	old, current := newDB(t), newDB(t)
	defer closeSubHTTPTestResource(t, old)
	defer closeSubHTTPTestResource(t, current)
	first, err := proxysub.Create(old, "lab", "subscriber", "fp", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := proxysub.Create(current, "lab", "subscriber", "fp", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.Exec(`UPDATE proxy_subscribers SET token_hash=? WHERE sub_id=?`, first.TokenHash, second.SubID); err != nil {
		t.Fatal(err)
	}
	for _, db := range []*sql.DB{old, current} {
		if _, err := db.Exec(`UPDATE sessions SET proxy_enabled=1 WHERE sid='lab'`); err != nil {
			t.Fatal(err)
		}
	}
	seedNode(t, old, "old-node", 14000, "ONLINE", 1)
	seedNode(t, current, "current-node", 14001, "ONLINE", 1)
	var slot atomic.Pointer[sql.DB]
	slot.Store(old)
	var resolved atomic.Int32
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
	k, err := listenerimage.Capture(ln, "sub-http")
	if err != nil {
		if k != nil {
			_ = k.Close()
		} else {
			_ = ln.Close()
		}
		t.Fatal(err)
	}
	defer closeSubHTTPTestResource(t, k)
	original := k.Descriptor()
	owner, err := StartImage(k, Config{DB: old, CurrentDB: func() *sql.DB { resolved.Add(1); return slot.Load() }, PublicHost: "broker.example.test"}, deadline())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Stop(deadline()) }()
	dial := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp", original.Address, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		return conn
	}
	send := func(conn net.Conn) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "GET /sub/%s HTTP/1.1\r\nHost: localhost\r\n\r\n", first.Token); err != nil {
			t.Fatal(err)
		}
	}
	check := func(reader *bufio.Reader, node, absentNode, psk string) {
		t.Helper()
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(body), node) || strings.Contains(string(body), absentNode) || !strings.Contains(string(body), psk) {
			t.Fatalf("subscription did not query current store: status=%d err=%v", resp.StatusCode, err)
		}
	}
	conn := dial()
	defer closeSubHTTPTestResource(t, conn)
	reader := bufio.NewReader(conn)
	send(conn)
	check(reader, "old-node", "current-node", first.PSK)
	ctx := context.Background() // ctx-root: held SQL pool connection fixture.
	held, err := old.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubHTTPTestResource(t, held)
	waits := old.Stats().WaitCount
	send(conn)
	// Non-vacuity: an actual accepted keepalive request is blocked inside SQL
	// admission before fencing, not just a fixture handler waiting on a channel.
	waitSubHTTP(t, func() bool { return old.Stats().WaitCount > waits })
	if err := owner.Fence(deadline()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Join(deadline()); err != nil {
		t.Fatal(err)
	}
	if resolved.Load() != 2 {
		t.Fatalf("request DB resolution count=%d", resolved.Load())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if old.Stats().InUse != 0 {
		t.Fatal("fenced subscription retained SQL connection")
	}
	if _, _, err := k.Export(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	slot.Store(current)
	fresh := dial()
	defer closeSubHTTPTestResource(t, fresh)
	send(fresh)
	if err := owner.Resume(); err != nil {
		t.Fatal(err)
	}
	check(bufio.NewReader(fresh), "current-node", "old-node", second.PSK)
	if k.Descriptor() != original || resolved.Load() != 3 {
		t.Fatal("thaw lost listener identity or current-store admission")
	}
	if err := owner.Stop(deadline()); err != nil {
		t.Fatal(err)
	}
	probe, err := net.DialTimeout("tcp", original.Address, 200*time.Millisecond)
	if err == nil {
		closeSubHTTPTestResource(t, probe)
		t.Fatal("subscription Stop retained LISTEN socket")
	}
}

func waitSubHTTP(t *testing.T, ready func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(end) {
			t.Fatal("subscription SQL admission timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// origin: hot-update S6 plan §11 / T20
// Terminal-stop assertions close some resources before deferred cleanup runs.
// Only an already-closed resource is benign; other release failures fail the test.
func closeSubHTTPTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, sql.ErrConnDone) {
		t.Errorf("close test resource: %v", err)
	}
}
