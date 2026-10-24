package xfergc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ttlConsumerRelay forwards exact NATS frames. Only the consumer-CREATE
// request is delayed: the SDK has already read real metadata, and the server
// expires it before creating the watch. No response or metadata is fabricated.
type ttlConsumerRelay struct {
	listener net.Listener
	upstream string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	conns    []net.Conn
	hold     func() error
	err      error
	wg       sync.WaitGroup
}

func newTTLConsumerRelay(t *testing.T, f *gcFixture) *ttlConsumerRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	r := &ttlConsumerRelay{listener: listener, upstream: f.server.Addr().String(), ctx: ctx, cancel: cancel}
	r.wg.Add(1)
	go r.serve()
	return r
}

func (r *ttlConsumerRelay) serve() {
	defer r.wg.Done()
	client, err := r.listener.Accept()
	if err != nil {
		return
	}
	// The copy goroutine and the relay's own close also close these; a second
	// close is expected to report net.ErrClosed.
	defer func() { _ = client.Close() }()
	serverConn, err := (&net.Dialer{}).DialContext(r.ctx, "tcp", r.upstream)
	if err != nil {
		return
	}
	defer func() { _ = serverConn.Close() }()
	r.mu.Lock()
	r.conns = []net.Conn{client, serverConn}
	if r.ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// Hide the TCP fast-copy interfaces: Linux splice caches process-wide
		// pipe FDs, which are unrelated to the resource ownership under test.
		_, _ = io.CopyBuffer(struct{ io.Writer }{client}, struct{ io.Reader }{serverConn}, make([]byte, 32<<10))
		_ = client.Close()
	}()
	reader := bufio.NewReader(client)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		var payload []byte
		if len(fields) >= 3 && (fields[0] == "PUB" || fields[0] == "HPUB") {
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 || size > 2<<20 {
				return
			}
			payload = make([]byte, size+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}
			if strings.Contains(fields[1], ".CONSUMER.CREATE.OBJ_xfer-ttl") {
				r.mu.Lock()
				hold := r.hold
				r.hold = nil
				r.mu.Unlock()
				if hold != nil {
					if err := hold(); err != nil {
						r.mu.Lock()
						r.err = err
						r.mu.Unlock()
						return
					}
				}
			}
		}
		if _, err := io.WriteString(serverConn, line); err != nil {
			return
		}
		if len(payload) > 0 {
			if _, err := serverConn.Write(payload); err != nil {
				return
			}
		}
	}
}

func (r *ttlConsumerRelay) close() {
	r.cancel()
	_ = r.listener.Close()
	r.mu.Lock()
	for _, conn := range r.conns {
		_ = conn.Close()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

type ttlRaceStore struct {
	jetstream.ObjectStore
	seedStore jetstream.ObjectStore
	stream    jetstream.Stream
	relay     *ttlConsumerRelay
	nc        *nats.Conn
	fixture   *gcFixture
}

func newTTLRaceStore(t *testing.T, f *gcFixture) *ttlRaceStore {
	t.Helper()
	seedStore := f.store("ttl", time.Second)
	r := newTTLConsumerRelay(t, f)
	nc, err := nats.Connect("nats://"+r.listener.Addr().String(), nats.NoReconnect())
	if err != nil {
		r.close()
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		r.close()
		t.Fatal(err)
	}
	store, err := js.ObjectStore(f.ctx, "xfer-ttl")
	if err != nil {
		nc.Close()
		r.close()
		t.Fatal(err)
	}
	stream, err := f.js.Stream(f.ctx, "OBJ_xfer-ttl")
	if err != nil {
		nc.Close()
		r.close()
		t.Fatal(err)
	}
	return &ttlRaceStore{ObjectStore: store, seedStore: seedStore, stream: stream, relay: r, nc: nc, fixture: f}
}

func (s *ttlRaceStore) close() {
	s.nc.Close()
	s.relay.close()
}

func (s *ttlRaceStore) seed(t *testing.T) {
	t.Helper()
	// Ordinary SDK Put creates a valid object before enumeration starts. The
	// fixture never writes metadata once List is stalled, even on failure.
	if _, err := s.seedStore.PutBytes(s.fixture.ctx, "expires", []byte("TTL race payload")); err != nil {
		t.Fatal(err)
	}
	s.relay.mu.Lock()
	s.relay.hold = func() error {
		timer := time.NewTimer(4 * time.Second)
		defer timer.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			info, err := s.stream.Info(s.relay.ctx)
			if err != nil {
				return err
			}
			if info.State.Msgs == 0 && info.State.LastSeq > 0 {
				return nil
			}
			select {
			case <-s.relay.ctx.Done():
				return s.relay.ctx.Err()
			case <-timer.C:
				return errors.New("TTL did not expire before consumer CREATE")
			case <-ticker.C:
			}
		}
	}
	s.relay.mu.Unlock()
}

type blockedCensus struct {
	entered chan struct{}
	release chan struct{}
}

func (o blockedCensus) Census(x.Origin, x.Deadline) (x.LedgerCensus, error) {
	close(o.entered)
	select {
	case <-o.release:
		return x.LedgerCensus{Ready: true, Complete: true}, nil
	case <-time.After(time.Second):
		return x.LedgerCensus{}, errors.New("test RPC bound reached")
	}
}

// A deadline-only RPC cannot be interrupted via a context. Stop must retain
// its join responsibility when its own shorter waiting budget expires.
// origin: hot-update S7 plan §8.2 / T30
func TestTransferCollectorStopRetainsUnjoinedPass(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	owner := blockedCensus{entered: make(chan struct{}), release: make(chan struct{})}
	c := collectorFor(t, f, owner, &schedulingFinalizer{}, time.Now)
	result := make(chan error, 1)
	d := gcDeadline(t, time.Second)
	go func() {
		_, err := c.Pass(d)
		result <- err
	}()
	select {
	case <-owner.entered:
	case <-time.After(time.Second):
		t.Fatal("pass did not enter census")
	}
	err := c.Stop(gcDeadline(t, 40*time.Millisecond))
	close(owner.release)
	if !errors.Is(err, controlbudget.ErrExpired) {
		t.Errorf("Stop acknowledged an unjoined census: %v", err)
	}
	if err := c.Stop(gcDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled census pass fabricated success")
		}
	case <-time.After(time.Second):
		t.Fatal("retained pass did not join")
	}
}

// origin: hot-update S7 plan §11.3 / P17
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer collector", beforeG)
	testharness.AssertNoFDLeak(t, "transfer collector", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newGCFixture(t)
	defer f.close()
	store := newTTLRaceStore(t, f)
	defer store.close()
	store.seed(t)
	owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true}}
	finalizer := &schedulingFinalizer{}
	c := collectorFor(t, f, owners, finalizer, time.Now)
	trace := &tracedStore{ObjectStore: store, started: make(chan struct{})}
	entered := trace.started
	c.cfg.JS = tracedJS{JetStream: f.js, store: trace}
	result := make(chan error, 1)
	passDeadline := gcDeadline(t, 5*time.Second)
	go func() {
		_, err := c.Pass(passDeadline)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		_ = c.Stop(gcDeadline(t, time.Second))
		t.Fatal("pass did not enter actual TTL-empty List")
	}
	if _, err := c.Pass(gcDeadline(t, time.Second)); err == nil {
		t.Fatal("concurrent pass escaped single-flight")
	}
	if err := c.Stop(gcDeadline(t, time.Second)); err != nil {
		t.Fatalf("Stop failed to cancel and join actual List: %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled pass fabricated success")
		}
	case <-time.After(time.Second):
		t.Fatal("Stop returned before the pass joined")
	}
	assertListsJoined(t, trace)
	if _, err := c.Pass(gcDeadline(t, time.Second)); err == nil {
		t.Fatal("stopped collector reopened admission")
	}
	if err := c.Stop(gcDeadline(t, time.Second)); err != nil {
		t.Fatal("idempotent Stop failed")
	}
	// A timeout is not terminal Stop. Repeated bounded passes can still
	// finalize retained cleanup without a data/control transport connection.
	origin := x.Origin{BrokerID: "owner", InstanceID: "instance", BootID: "boot", Generation: 1}
	row := testGCRow(origin, "courier", "version", x.PhaseTerminalPending)
	owners.local = x.LedgerCensus{Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{row}}
	c = collectorFor(t, f, owners, finalizer, time.Now)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	// Empty-stream success still exercises publication progress after a
	// canceled generation; it does not need a second fabricated wedge.
	for pass := 0; pass < 2; pass++ {
		if _, err := c.Pass(gcDeadline(t, 2*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if len(finalizer.identities()) != 2 {
		t.Fatalf("timeout dropped publication responsibility: %+v", finalizer.identities())
	}
	if _, err := c.Pass(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("invalid deadline admitted work: %v", err)
	}
}
