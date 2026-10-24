package jsstream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §11.3 / T01–T04
func TestTransferLifecycleNoLeaks(t *testing.T) {
	// Warm the embedded server's process-global timers before the single baseline.
	t.Run("warmup", exerciseTransferLifecycle)
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer writer lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "transfer writer lifecycle", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	exerciseWriterStopTimeout(t, js)
	body := bytes.Repeat([]byte("lifecycle"), 5000)
	spec := writerSpec(t, js, "lifecycle", body, 32<<10)
	fault := &writerBlockingJS{JetStream: js, entered: make(chan struct{}), release: make(chan struct{})}
	w := mustWriter(t, fault, spec)
	var release sync.Once
	unblock := func() { release.Do(func() { close(fault.release) }) }
	defer unblock()
	deadline := writerDeadline(t)
	appended := make(chan error, 1)
	go func() { _, err := w.Append(0, body[:spec.ChunkSize], deadline); appended <- err }()
	select {
	case <-fault.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publish never reached ACK boundary")
	}
	short, _ := controlbudget.After(20 * time.Millisecond)
	if _, err := w.Freeze(short); err == nil {
		t.Fatal("Freeze failed to join admitted IO")
	}
	unblock()
	if err := <-appended; err == nil {
		t.Fatal("lost ACK returned success")
	}
	if err := ThawTransferWriter(w, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if w.(*objectWriter).closed.Load() {
		t.Fatal("timed-out Freeze stranded retained owner")
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if state.PendingAttempt == nil {
		t.Fatal("freeze dropped pending bytes")
	}
	// Caller-owned image slices cannot mutate the retained writer.
	state.PendingAttempt.Bytes[0] ^= 1
	again, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(state.PendingAttempt.Bytes, again.PendingAttempt.Bytes) {
		t.Fatal("freeze borrowed mutable bytes")
	}
	if _, err = ImportTransferWriter(js, state); err == nil {
		t.Fatal("corrupt pending hash imported")
	}
	if err = ThawTransferWriter(w, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := w.ResolvePending(writerDeadline(t)); err != nil || !found {
		t.Fatalf("rollback lost current responsibility: %v %v", found, err)
	}
	state, err = w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	successor, err := ImportTransferWriter(js, state)
	if err != nil {
		t.Fatal(err)
	}
	if err = ThawTransferWriter(successor, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	var observations []x.Observation
	if err = SetTransferWriterObserver(successor, func(o x.Observation) { observations = append(observations, o) }, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writerComplete(t, successor, spec, body, 1)
	if len(observations) != 3 || observations[0].Kind != x.ObserveAppend || observations[1].ActualACKSeq == 0 || observations[2].Kind != x.ObserveFinalize {
		t.Fatalf("writer observations incomplete: %+v", observations)
	}
	writerVerify(t, js, spec, body)
	if err = successor.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = successor.Append(0, body[:spec.ChunkSize], writerDeadline(t)); err == nil {
		t.Fatal("Stop left append admission open")
	}
	if err = ThawTransferWriter(successor, writerDeadline(t)); err == nil {
		t.Fatal("retired writer thawed")
	}
	state, err = successor.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce, WriterRetired: true, ConsumersRetired: true}
	receipt, err := RetireTransferObject(js, &state, proof, writerDeadline(t))
	if err != nil || !receipt.ObjectPurged {
		t.Fatalf("cleanup: %+v %v", receipt, err)
	}
	if rows := writerRaw(t, js, spec); len(rows) != 0 {
		t.Fatal("retirement retained original chunks")
	}
}

func exerciseWriterStopTimeout(t *testing.T, js jetstream.JetStream) {
	body := []byte("Stop must remain irreversible when its IO join times out")
	spec := writerSpec(t, js, "stop-timeout", body, 32<<10)
	fault := &writerBlockingJS{JetStream: js, entered: make(chan struct{}), release: make(chan struct{})}
	w := mustWriter(t, fault, spec)
	var release sync.Once
	unblock := func() { release.Do(func() { close(fault.release) }) }
	defer unblock()
	deadline := writerDeadline(t)
	appended := make(chan error, 1)
	go func() { _, err := w.Append(0, body, deadline); appended <- err }()
	select {
	case <-fault.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop test never reached admitted IO")
	}
	short, _ := controlbudget.After(20 * time.Millisecond)
	if err := w.Stop(short); err == nil {
		t.Fatal("Stop claimed an unjoined writer")
	}
	unblock()
	if err := <-appended; err == nil {
		t.Fatal("Stop fixture did not lose ACK")
	}
	if err := ThawTransferWriter(w, writerDeadline(t)); err == nil {
		t.Fatal("rollback reopened a timed-out Stop")
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if !state.RetirementFence.WriterRetired {
		t.Fatal("joined Stop proof was not exported")
	}
	proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce, WriterRetired: true, ConsumersRetired: true}
	if _, err = RetireTransferObject(js, &state, proof, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

type writerBlockingJS struct {
	jetstream.JetStream
	entered, release chan struct{}
}

func (j *writerBlockingJS) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, m, opts...)
	if err != nil {
		return ack, err
	}
	close(j.entered)
	select {
	case <-j.release:
	case <-ctx.Done():
	}
	return nil, context.DeadlineExceeded
}

type writerTestServer struct {
	srv  *natsserver.Server
	opts *natsserver.Options
}

func writerServer(t *testing.T, configure func(*natsserver.Options)) *writerTestServer {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true, MaxPayload: 2 << 20}
	if configure != nil {
		configure(opts)
	}
	fixture := &writerTestServer{opts: opts}
	fixture.start(t)
	t.Cleanup(func() { fixture.srv.Shutdown(); fixture.srv.WaitForShutdown() })
	return fixture
}
func (s *writerTestServer) start(t *testing.T) {
	t.Helper()
	srv, err := natsserver.NewServer(s.opts)
	if err != nil {
		t.Fatal(err)
	}
	s.srv = srv
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("writer JetStream server did not start")
	}
}
func (s *writerTestServer) url() string { return s.srv.ClientURL() }
func (s *writerTestServer) restart(t *testing.T) {
	s.srv.Shutdown()
	s.srv.WaitForShutdown()
	s.start(t)
}
func writerJS(t *testing.T, address string, opts ...nats.Option) jetstream.JetStream {
	t.Helper()
	opts = append(opts, nats.ReconnectBufSize(-1), nats.NoReconnect())
	conn, err := nats.Connect(address, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// This relay retains complete real client HPUB frames before the server sees
// them. It never fabricates an ACK, raw record, or JetStream API response.
type writerRelay struct {
	url          string
	listener     net.Listener
	upstream     string
	target       string
	held         chan writerHeldFrame
	done         chan struct{}
	mu           sync.Mutex
	peer, server net.Conn
	writeMu      sync.Mutex
}
type writerHeldFrame struct {
	relay *writerRelay
	bytes []byte
}

func (f writerHeldFrame) body() []byte { return f.bytes[bytes.Index(f.bytes, []byte("\r\n"))+2:] }
func (f writerHeldFrame) release(t *testing.T) {
	t.Helper()
	f.relay.writeMu.Lock()
	defer f.relay.writeMu.Unlock()
	if _, err := f.relay.server.Write(f.bytes); err != nil {
		t.Fatal(err)
	}
}
func newWriterRelay(t *testing.T, upstream, target string) *writerRelay {
	t.Helper()
	u, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &writerRelay{url: "nats://" + listener.Addr().String(), listener: listener, upstream: u.Host, target: target, held: make(chan writerHeldFrame, 8), done: make(chan struct{})}
	go r.run()
	t.Cleanup(func() {
		_ = listener.Close()
		r.mu.Lock()
		if r.peer != nil {
			_ = r.peer.Close()
		}
		if r.server != nil {
			_ = r.server.Close()
		}
		r.mu.Unlock()
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			t.Error("relay did not join")
		}
	})
	return r
}
func (r *writerRelay) next(t *testing.T) writerHeldFrame {
	t.Helper()
	select {
	case frame := <-r.held:
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("no actual held HPUB")
	}
	return writerHeldFrame{}
}
func (r *writerRelay) run() {
	defer close(r.done)
	peer, err := r.listener.Accept()
	if err != nil {
		return
	}
	defer func() { _ = peer.Close() }()
	server, err := net.DialTimeout("tcp", r.upstream, time.Second)
	if err != nil {
		return
	}
	defer func() { _ = server.Close() }()
	r.mu.Lock()
	r.peer, r.server = peer, server
	r.mu.Unlock()
	copied := make(chan struct{})
	go func() { _, _ = io.Copy(peer, server); _ = peer.Close(); close(copied) }()
	defer func() { _ = server.Close(); <-copied }()
	reader := bufio.NewReader(peer)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(string(line))
		frame := bytes.Clone(line)
		if len(fields) > 2 && (fields[0] == "HPUB" || fields[0] == "PUB") {
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 || size > 4<<20 {
				return
			}
			body := make([]byte, size+2)
			if _, err = io.ReadFull(reader, body); err != nil {
				return
			}
			frame = append(frame, body...)
			if fields[1] == r.target {
				r.held <- writerHeldFrame{relay: r, bytes: frame}
				continue
			}
		}
		r.writeMu.Lock()
		_, err = server.Write(frame)
		r.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

// Delay outgoing WebSocket bytes above a verified TLS connection.
// Adjacent packets share propagation delay, preserving the actual 100ms RTT.
type writerRTTDialer struct {
	tls  *tls.Config
	conn *writerRTTConn
}

func (d *writerRTTDialer) SkipTLSHandshake() bool { return true }
func (d *writerRTTDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, network, address, d.tls)
	if err != nil {
		return nil, err
	}
	d.conn = &writerRTTConn{Conn: conn, closed: make(chan struct{}), done: make(chan struct{}), queue: make(chan writerDelayedBytes, 8)}
	go d.conn.deliver()
	return d.conn, nil
}

type writerDelayedBytes struct {
	data []byte
	at   time.Time
}
type writerRTTConn struct {
	net.Conn
	active  atomic.Bool
	flights atomic.Int64
	closed  chan struct{}
	done    chan struct{}
	queue   chan writerDelayedBytes
	once    sync.Once
}

func (c *writerRTTConn) Write(p []byte) (int, error) {
	if !c.active.Load() {
		return c.Conn.Write(p)
	}
	packet := writerDelayedBytes{data: bytes.Clone(p), at: time.Now().Add(100 * time.Millisecond)}
	select {
	case c.queue <- packet:
		c.flights.Add(1)
		return len(p), nil
	case <-c.closed:
		return 0, net.ErrClosed
	}
}
func (c *writerRTTConn) deliver() {
	defer close(c.done)
	for {
		select {
		case <-c.closed:
			return
		case packet := <-c.queue:
			// sleep-fixture: propagation delay is pipelined; adjacent frames are not
			// serialized into multiple RTTs just because WebSocket/TLS fragmented them.
			timer := time.NewTimer(time.Until(packet.at))
			select {
			case <-timer.C:
			case <-c.closed:
				timer.Stop()
				return
			}
			if _, err := c.Conn.Write(packet.data); err != nil {
				_ = c.Conn.Close()
				return
			}
		}
	}
}
func (c *writerRTTConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	err := c.Conn.Close()
	<-c.done
	return err
}

func exerciseWriterRTT(t *testing.T, minimumRate float64) {
	serverTLS, clientTLS := objectWSSTLSConfigs(t)
	srv := writerServer(t, func(opts *natsserver.Options) {
		opts.Websocket = natsserver.WebsocketOpts{Host: "127.0.0.1", Port: -1, TLSConfig: serverTLS}
	})
	observer := writerJS(t, srv.url())
	body := bytes.Repeat([]byte("single-pending!!"), (16<<20)/16)
	spec := writerSpec(t, observer, "rtt", body, 1<<20)
	dialer := &writerRTTDialer{tls: clientTLS}
	// The custom dialer supplies the verified TLS layer. The SDK's ws scheme
	// prevents wrapping TLS twice (its SkipTLSHandshake path omits reader binding).
	data := writerJS(t, strings.Replace(srv.srv.WebsocketURL(), "wss://", "ws://", 1), nats.SetCustomDialer(dialer))
	if !dialer.conn.Conn.(*tls.Conn).ConnectionState().HandshakeComplete {
		t.Fatal("data transport did not complete verified TLS")
	}
	control := writerJS(t, srv.srv.WebsocketURL(), nats.Secure(clientTLS))
	sub, err := observer.Conn().Subscribe("writer.control", func(m *nats.Msg) { _ = m.Respond(m.Data) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = observer.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	type controlResult struct {
		count int
		err   error
		max   time.Duration
	}
	done := make(chan controlResult, 1)
	go func() {
		result := controlResult{}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				done <- result
				return
			case <-ticker.C:
				start := time.Now()
				_, err := control.Conn().Request("writer.control", []byte("ping"), 5*time.Second)
				result.max = max(result.max, time.Since(start))
				result.count++
				if err != nil {
					result.err = err
					done <- result
					return
				}
			}
		}
	}()
	defer cancel()
	w := mustWriter(t, data, spec)
	// Warm INFO before the throughput interval; every measured chunk still
	// requires both a durable publish ACK and independent raw readback.
	if err = w.(*objectWriter).ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	dialer.conn.active.Store(true)
	started := time.Now()
	for index := uint32(0); index < spec.Chunks; index++ {
		offset := int(index) * int(spec.ChunkSize)
		if _, err = w.Append(index, body[offset:offset+int(chunkLength(spec, index))], writerDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	cancel()
	result := <-done
	if result.err != nil || result.count < 5 || result.max > 5*time.Second {
		t.Fatalf("control starved: %+v", result)
	}
	flights := dialer.conn.flights.Load()
	if flights < int64(2*spec.Chunks) || elapsed < time.Duration(2*spec.Chunks)*100*time.Millisecond {
		t.Fatalf("RTT injection not exercised: flights=%d elapsed=%v", flights, elapsed)
	}
	rate := float64(len(body)) / elapsed.Seconds()
	if rate < minimumRate {
		t.Fatalf("single pending rate %.2f MiB/s < %.2f MiB/s at 100ms RTT (flights=%d elapsed=%v)", rate/(1<<20), minimumRate/(1<<20), flights, elapsed)
	}
	if _, err = w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	writerVerify(t, observer, spec, body)
	t.Logf("100ms WSS RTT: %.2f MiB/s, flights=%d, control_slots=%d max_control=%v", rate/(1<<20), flights, result.count, result.max)
}
