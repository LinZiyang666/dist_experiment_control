package infratransport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func transportDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func awaitTransport(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatalf("%s did not join within the planned budget", what)
	}
}

func newTransportServer(t *testing.T) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("embedded NATS did not start")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}

// origin: hot-update S9 plan §4.5 / T32
func TestPlannedMoveInterruptsNATSMutexWait(t *testing.T) {
	s := newTransportServer(t)
	t.Run("dns_holds_real_nats_mutex", func(t *testing.T) { exerciseMutexMove(t, s.ClientURL()) })
	t.Run("late_worker_cannot_resurrect_generation", exerciseLateDial)
	t.Run("async_connect_cannot_claim_ready", func(t *testing.T) {
		tr, err := New(1, transportDeadline(t), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Interrupt()
		nc, err := tr.Connect("nats://127.0.0.1:1", func() []nats.Option { return []nats.Option{nats.RetryOnFailedConnect(true)} })
		if nc != nil {
			defer nc.Close()
		}
		if !errors.Is(err, infracontract.ErrUnsupported) || nc != nil {
			t.Fatalf("async CONNECT claimed a joined receipt: %v", err)
		}
		if len(tr.RawConnections()) != 0 {
			t.Fatal("unsupported CONNECT produced a raw attachment")
		}
	})
	t.Run("legacy_dialer_defers_before_effect", func(t *testing.T) {
		legacy := &legacyDialer{}
		_, err := New(1, transportDeadline(t), nil, legacy)
		if !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatal("legacy dialer was accepted")
		}
		if legacy.called {
			t.Fatal("legacy dialer was invoked before unsupported refusal")
		}
	})
}

type legacyDialer struct{ called bool }

func (d *legacyDialer) Dial(string, string) (net.Conn, error) {
	d.called = true
	return nil, errors.New("uninterruptible fixture")
}

func exerciseMutexMove(t *testing.T, url string) {
	t.Helper()
	tr, err := New(1, transportDeadline(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Interrupt()
	nc, err := tr.Connect(url, func() []nats.Option {
		return []nats.Option{nats.ReconnectWait(0), nats.MaxReconnects(-1), nats.NoCallbacksAfterClientClose()}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// The pure-Go resolver performs real DNS I/O against an unresponsive UDP
	// socket. Its nc.mu-holding reconnect cannot return until cancellation.
	dns, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Closed (and its reader joined) by the single defer registered below; no
	// statement between here and there can return early.
	queried, dnsDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(dnsDone)
		var once sync.Once
		b := make([]byte, 4096)
		for {
			_, _, e := dns.ReadFrom(b)
			if e != nil {
				return
			}
			once.Do(func() { close(queried) })
		}
	}()
	defer func() { _ = dns.Close(); <-dnsDone }()
	tr.resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", dns.LocalAddr().String())
	}
	if err := nc.SetServerPool([]string{"nats://planned-dns.invalid:4222"}); err != nil {
		t.Fatal(err)
	}
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	awaitTransport(t, queried, "real DNS query")
	poolDone, forceDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(poolDone); _ = nc.SetServerPool([]string{url}) }()
	go func() { defer close(forceDone); _ = nc.ForceReconnect() }()
	// Nonempty mutex oracle: neither API may finish while reconnect owns mu.
	select {
	case <-poolDone:
		t.Fatal("SetServerPool did not block on nc.mu")
	case <-forceDone:
		t.Fatal("ForceReconnect did not block on nc.mu")
	case <-time.After(40 * time.Millisecond):
	}
	started := time.Now()
	deadline := transportDeadline(t)
	if err := Move(tr, nc, deadline, nil); err != nil {
		t.Fatal(err)
	}
	awaitTransport(t, poolDone, "SetServerPool")
	awaitTransport(t, forceDone, "ForceReconnect")
	if time.Since(started) >= upgradebudget.RequestLatency {
		t.Fatal("planned move used the legacy teardown budget")
	}
	// The original dial still has almost five seconds left. A short fixture
	// bound distinguishes actual cancellation from waiting for natural expiry.
	// origin: hot-update S9 plan §4.5 / T32 / M16
	if time.Since(started) >= time.Second {
		t.Fatal("move waited for DNS expiry instead of interrupting nc.mu's owner")
	}
	if !errors.Is(tr.ctx.Err(), context.Canceled) {
		t.Fatal("old DNS generation was not canceled")
	}
	if _, err := tr.Dial("tcp", "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("old generation dial: %v", err)
	}
}

type lateDialer struct {
	started, canceled, release, exited chan struct{}
	raw                                net.Conn
}

func (d *lateDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("unexpected legacy Dial")
}
func (d *lateDialer) DialWithin(_, _ string, _ controlbudget.Deadline, stop infracontract.Stop) (net.Conn, error) {
	defer close(d.exited)
	close(d.started)
	<-stop
	close(d.canceled)
	<-d.release
	return d.raw, nil
}

func exerciseLateDial(t *testing.T) {
	t.Helper()
	raw, peer := net.Pipe()
	defer func() { _ = raw.Close() }() // net.Pipe's Close cannot fail
	defer func() { _ = peer.Close() }()
	d := &lateDialer{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{}), raw: raw}
	tr, err := New(7, transportDeadline(t), nil, d)
	if err != nil {
		t.Fatal(err)
	}
	dialDone := make(chan struct{})
	var dialErr error
	go func() { defer close(dialDone); _, dialErr = tr.Dial("tcp", "late.invalid:4222") }()
	awaitTransport(t, d.started, "late dial start")
	moveDone := make(chan struct{})
	go func() { tr.Interrupt(); close(moveDone) }()
	awaitTransport(t, d.canceled, "late dial cancellation")
	select {
	case <-moveDone:
		t.Error("move returned before its canceled worker exited")
	case <-time.After(40 * time.Millisecond):
	}
	// Install a real successor while the canceled old worker is still alive.
	next, err := New(8, transportDeadline(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Interrupt()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }() // fixture listener; the leak gate audits fds
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := listener.Accept(); accepted <- c }()
	successor, err := next.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	successorPeer := <-accepted
	if successorPeer == nil {
		t.Fatal("missing successor socket")
	}
	defer func() { _ = successorPeer.Close() }()
	close(d.release)
	awaitTransport(t, moveDone, "late worker join")
	awaitTransport(t, dialDone, "late dial result")
	awaitTransport(t, d.exited, "late dial exit")
	if !errors.Is(dialErr, context.Canceled) {
		t.Errorf("late old dial returned %v", dialErr)
	}
	_ = peer.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err := peer.Write([]byte("stale")); !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("late raw socket survived retirement: %v", err)
	}
	if len(tr.conns) != 0 {
		t.Error("old raw attachment was registered after retirement")
	}
	// The old worker's late return must leave the original successor usable.
	writeDone := make(chan error, 1)
	go func() { _, err := successor.Write([]byte{8}); writeDone <- err }()
	_ = successorPeer.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 1)
	if _, err := successorPeer.Read(b); err != nil || b[0] != 8 {
		t.Fatalf("late worker damaged successor socket: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	connections := next.RawConnections()
	if len(connections) != 1 || connections[0].Generation != 8 || connections[0].Sequence != 1 || connections[0] != successor.(*Connection) {
		t.Fatal("late worker changed successor attachment")
	}

}

// origin: hot-update S9 plan §12 P14 / L:TestInfraTransportLifecycle→exerciseTransport
func TestInfraTransportLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransport(t)
	}
	testharness.AssertNoGoroutineLeak(t, "planned transport retirement", goroutines)
	testharness.AssertNoFDLeak(t, "planned transport retirement", fds, 0)
}

func exerciseTransport(t *testing.T) {
	t.Helper()
	exerciseLateDial(t)
	tr, err := New(1, transportDeadline(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }() // fixture listener; the leak gate audits fds
	accepted := make(chan net.Conn, 1)
	go func() { c, _ := listener.Accept(); accepted <- c }()
	c, err := tr.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	peer := <-accepted
	if peer == nil {
		t.Fatal("no accepted raw socket")
	}
	defer func() { _ = peer.Close() }()
	if c.(*Connection).Generation != 1 || c.(*Connection).Sequence != 1 {
		t.Fatal("raw attachment is missing generation identity")
	}
	connections := tr.RawConnections()
	if len(connections) != 1 || connections[0] != c.(*Connection) || connections[0].RawConn() != c.(*Connection).Conn {
		t.Fatal("raw census does not contain the actually dialed attachment")
	}
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = c.Read(make([]byte, 1)) }()
	tr.Interrupt()
	awaitTransport(t, readDone, "raw reader")
	if len(tr.RawConnections()) != 0 {
		t.Fatal("retired raw attachment remained in the census")
	}
}
