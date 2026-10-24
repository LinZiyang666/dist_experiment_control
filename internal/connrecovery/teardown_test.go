package connrecovery

import (
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S5 plan §10 / AGENT-SLIM-S5; moved tracker capacity regression.
func TestConnTrackerRegistryStaysBounded(t *testing.T) {
	// ctx-none: isolated dial fixture has no parent operation.
	tr := NewTracker(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return newBlockingConn(false), nil
	})
	for i := 0; i < 1000; i++ {
		if _, err := tr.Dial("tcp", "127.0.0.1:4222"); err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}
	tr.mu.Lock()
	n := len(tr.conns)
	tr.mu.Unlock()
	if n > TrackerCapacity {
		t.Fatalf("tracker holds %d conn references after 1000 redials (cap %d) — unbounded growth on a flapping link", n, TrackerCapacity)
	}
}

// origin: hot-update S9 plan §4.5 / T32–T33 — the legacy teardown adapter must retire only the NC generation.
func TestPlannedRecoveryLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePlannedRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "planned recovery", goroutines)
	testharness.AssertNoFDLeak(t, "planned recovery", fds, 0)
}

func exercisePlannedRecovery(t *testing.T) {
	t.Helper()
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := infratransport.New(1, deadline, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Interrupt()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }() // fixture listener; nothing to flush
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := listener.Accept(); accepted <- c }()
	c, err := tr.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	peer := <-accepted
	if peer == nil {
		t.Fatal("missing accepted raw socket")
	}
	defer func() { _ = peer.Close() }()
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = c.Read(make([]byte, 1)) }()
	// The independent socket models a business FD whose lifetime must outlast
	// NC teardown. Its original peer, not a replacement socket, reads after cut.
	business, businessPeer := net.Pipe()
	defer func() { _ = business.Close() }() // net.Pipe's Close cannot fail
	defer func() { _ = businessPeer.Close() }()
	if err := infratransport.Move(tr, nil, deadline, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("raw reader survived planned retirement")
	}
	writeDone := make(chan error, 1)
	go func() { _, err := business.Write([]byte{42}); writeDone <- err }()
	_ = businessPeer.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 1)
	if _, err := businessPeer.Read(b); err != nil || b[0] != 42 {
		t.Fatalf("original business FD did not survive: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

// blockingConn is a net.Conn whose Read/Write park forever until the conn is poisoned
// (SetDeadline in the past or Close) — the deterministic stand-in for a half-dead link.
type blockingConn struct {
	mu      sync.Mutex
	dead    chan struct{}
	closed  bool
	ignores bool // adversarial: ignore Close/SetDeadline entirely (the escalate path)
	// onRelease lets a test drop a REAL socket alongside the wrapper, so the fd leak gate measures
	// the ladder rather than the fixture's own bookkeeping.
	onRelease func()
}

func newBlockingConn(ignores bool) *blockingConn {
	return &blockingConn{dead: make(chan struct{}), ignores: ignores}
}

func (c *blockingConn) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.dead)
		if c.onRelease != nil {
			c.onRelease()
		}
	}
}

func (c *blockingConn) Read(b []byte) (int, error)  { <-c.dead; return 0, net.ErrClosed }
func (c *blockingConn) Write(b []byte) (int, error) { <-c.dead; return 0, net.ErrClosed }
func (c *blockingConn) Close() error {
	if c.ignores {
		return nil // adversarial: refuses to die — forces the escalation rung
	}
	c.release()
	return nil
}
func (c *blockingConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4zero} }
func (c *blockingConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero} }
func (c *blockingConn) SetDeadline(t time.Time) error {
	if c.ignores {
		return nil
	}
	if !t.IsZero() && t.Before(time.Now()) {
		c.release()
	}
	return nil
}
func (c *blockingConn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *blockingConn) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }
