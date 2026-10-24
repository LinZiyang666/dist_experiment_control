//go:build linux && hotupdate_integration

package hotupdate_test

// Real: workspace-built processes, authenticated JetStream and TCP carriers.
// Simulated: loopback infrastructure and deterministic fixture payloads.
// Not covered: deployed WSS, infrastructure binary upgrades or S8 migration.

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type transferCarrier struct {
	listener net.Listener
	mu       sync.Mutex
	pairs    [][2]net.Conn
	done     chan struct{}
	wg       sync.WaitGroup
	// moved counts every byte the carrier sockets relayed, both directions.
	moved atomic.Int64
}

// transferCountingWriter adds each relayed write to the carrier's total.
type transferCountingWriter struct {
	w     io.Writer
	moved *atomic.Int64
}

func (c transferCountingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.moved.Add(int64(n))
	return n, err
}

func newTransferCarrier(t *testing.T, target string) *transferCarrier {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	transferMust(t, err)
	c := &transferCarrier{listener: listener, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		for {
			left, err := listener.Accept()
			if err != nil {
				return
			}
			right, err := net.DialTimeout("tcp", target, time.Second)
			if err != nil {
				_ = left.Close()
				continue
			}
			c.mu.Lock()
			c.pairs = append(c.pairs, [2]net.Conn{left, right})
			c.mu.Unlock()
			c.wg.Add(2)
			for _, pair := range [][2]net.Conn{{left, right}, {right, left}} {
				go func() {
					defer c.wg.Done()
					// TCP's splice fast path caches pipe FDs beyond this fixture.
					// Keep the proxy's buffer and descriptor lifetime explicit so
					// the shared FD gate measures actual unclosed fixture sockets.
					// origin: S7 internal review CUT-CAPACITY F6
					_, _ = io.Copy(transferCountingWriter{pair[0], &c.moved}, struct{ io.Reader }{pair[1]})
					_ = pair[0].Close()
					_ = pair[1].Close()
				}()
			}
		}
	}()
	return c
}
func (c *transferCarrier) close() {
	_ = c.listener.Close()
	<-c.done
	c.mu.Lock()
	for _, pair := range c.pairs {
		_ = pair[0].Close()
		_ = pair[1].Close()
	}
	c.mu.Unlock()
	c.wg.Wait()
}
func transferBreakCarrier(t *testing.T, s *transferStack) {
	t.Helper()
	c := s.carrier
	c.mu.Lock()
	count := len(c.pairs)
	if count == 0 {
		c.mu.Unlock()
		t.Fatal("no actual S5 carrier crossed the private proxy")
	}
	pair := c.pairs[count-1]
	c.mu.Unlock()
	transferMust(t, pair[0].Close())
	_ = pair[1].Close()
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.pairs) > count
	}) {
		t.Fatal("S5 carrier did not attach a replacement connection")
	}
}

// transferRequireCarriedLegs proves both byte legs rode the S5 carrier. A
// replaced carrier resumes its flows (S5), so the data attachments legitimately
// survive the break and their NATS IDs prove nothing about the path. Each leg
// moves the whole payload once across the tunnel (ctl and agent attachments
// both dial the exposed flow), so the relayed total must cover it twice; a leg
// left on a direct connection moves it only once.
// origin: S7 internal review CUT-CAPACITY F6 / hot-update S7 plan §10.3 / T46
func transferRequireCarriedLegs(t *testing.T, s *transferStack, size int64) {
	t.Helper()
	if moved := s.carrier.moved.Load(); moved < 2*size {
		t.Fatalf("S5 carrier relayed %d bytes for a %d-byte transfer: a byte leg bypassed the carrier", moved, size)
	}
}

var transferCarrierPort atomic.Uint64

// The transfer protocol uses NATS. Expose the private server through the real
// S5 flow before the call, then route ctl and agent data attachments through
// that port. An idle agent alone never creates a carrier to interrupt.
// origin: S7 internal review CUT-CAPACITY F6 / hot-update S7 plan §10.3 / T46
func routeTransferThroughCarrier(t *testing.T, s *transferStack) {
	t.Helper()
	// The flow's local end is the fixture's client listener (the gate when
	// present), so carried connections take the same path as direct ones.
	_, local, err := net.SplitHostPort(strings.TrimPrefix(s.url, "nats://"))
	transferMust(t, err)
	for attempt := 0; attempt < 32; attempt++ {
		port := strconv.Itoa(14000 + int((uint64(os.Getpid())+transferCarrierPort.Add(1))%1000))
		// The product binds its own port. A concurrent claimant is a real bind
		// refusal, which is the only reason setup can try another empty port.
		probe, err := net.Listen("tcp", "127.0.0.1:"+port)
		if err != nil {
			continue
		}
		transferMust(t, probe.Close())
		before := s.output(s.agent)
		_, err = s.command("expose", s.nid, "--name", "transfer-data", "--local", local, "--remote-port", port)
		if err != nil {
			log := s.output(s.agent)
			if len(log) >= len(before) && flowExposeBindFailure(err.Error(), log[len(before):], port) {
				continue
			}
			t.Fatal(err)
		}
		s.url = "nats://127.0.0.1:" + port
		transferMust(t, os.WriteFile(filepath.Join(s.h.Root, "carrier-data-url"), []byte(s.url), 0600))
		return
	}
	t.Fatal("no public transfer data port could be bound for the S5 carrier")
}

// transferNATSGate is the restart lane's client listener in front of the
// private server. Pinned nats-server v2.14.6 Shutdown clears s.sys in
// shutdownEventing while an auth callout that is processing a client's
// CONNECT can still read it in newRespInbox: a nil dereference that kills
// the test binary, or a -race report that fails every concurrent cell.
// LameDuckShutdown closes the listener first, but it also closes every
// established client before Shutdown's signalPullConsumers runs, so pending
// pulls lose the "409 Server Shutdown" status a real SIGTERM restart sends.
// The gate instead stops new handshakes and waits for each CONNECT the
// server already read to be answered, then an ordinary Shutdown runs while
// every established connection still carries that 409.
// origin: S7 internal review CUT-CAPACITY F5 / hot-update S7 plan §10.3 / T46
type transferNATSGate struct {
	listener net.Listener
	mu       sync.Mutex
	upstream string
	fenced   bool
	closed   bool
	inflight int
	refused  int
	// sealed: fence returned, so until reopen the server may answer no CONNECT.
	// late counts handshakes it nevertheless answered in that window.
	sealed   bool
	late     int
	conns    map[*transferGateConn]bool
	accepted chan struct{}
	wg       sync.WaitGroup
}

// forwarded: handshake bytes reached the server. answered: the server
// replied PONG/-ERR to that CONNECT or closed its own socket, so its
// readLoop can no longer enter the auth callout for this connection.
type transferGateConn struct {
	client, server      net.Conn
	forwarded, answered bool
}

func newTransferNATSGate(t *testing.T, upstream string) *transferNATSGate {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	transferMust(t, err)
	g := &transferNATSGate{listener: listener, upstream: upstream, conns: map[*transferGateConn]bool{}, accepted: make(chan struct{})}
	go func() {
		defer close(g.accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			target, refuse := g.upstream, g.fenced || g.closed
			if refuse {
				g.refused++
			}
			g.mu.Unlock()
			// A restart in progress refuses like a stopped server's closed port.
			if refuse {
				_ = client.Close()
				continue
			}
			server, err := net.DialTimeout("tcp", target, time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			c := &transferGateConn{client: client, server: server}
			g.mu.Lock()
			if g.closed {
				g.mu.Unlock()
				_, _ = client.Close(), server.Close()
				continue
			}
			g.conns[c] = true
			g.wg.Add(2)
			g.mu.Unlock()
			go g.clientToServer(c)
			go g.serverToClient(c)
		}
	}()
	return g
}

func (g *transferNATSGate) url() string { return "nats://" + g.listener.Addr().String() }

// clientToServer forwards a handshake only while the gate is open. A fenced
// connection whose CONNECT never reached the server is refused: the server
// read nothing from it, exactly as if the client had found the port closed.
func (g *transferNATSGate) clientToServer(c *transferGateConn) {
	defer g.wg.Done()
	buffer := make([]byte, 32<<10)
	for {
		n, err := c.client.Read(buffer)
		if n > 0 {
			g.mu.Lock()
			if !c.forwarded && !c.answered {
				if g.fenced || g.closed {
					g.refused++
					g.mu.Unlock()
					_, _ = c.client.Close(), c.server.Close()
					return
				}
				c.forwarded = true
				g.inflight++
			}
			g.mu.Unlock()
			if _, err := c.server.Write(buffer[:n]); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	// The server may still be processing a CONNECT it already read. Only its
	// own answer or close ends that handshake, so keep its read side open.
	g.mu.Lock()
	pending := c.forwarded && !c.answered
	g.mu.Unlock()
	if tcp, ok := c.server.(*net.TCPConn); pending && ok {
		_ = tcp.CloseWrite()
		return
	}
	_ = c.server.Close()
}

// serverToClient forwards with explicit buffers: TCP splice would cache pipe
// descriptors outside this fixture's FD accounting.
func (g *transferNATSGate) serverToClient(c *transferGateConn) {
	defer g.wg.Done()
	buffer := make([]byte, 32<<10)
	var line []byte
	clientGone := false
	for {
		n, err := c.server.Read(buffer)
		if n > 0 {
			g.mu.Lock()
			answered := c.answered
			g.mu.Unlock()
			if !answered {
				line = append(line, buffer[:n]...)
				if transferHandshakeAnswered(&line) {
					g.answer(c)
					answered = true
				}
			}
			if !clientGone {
				if _, werr := c.client.Write(buffer[:n]); werr != nil {
					clientGone = true
				}
			}
			// An established connection ends with its client. A pending
			// handshake keeps draining until the server answers or closes.
			if clientGone && answered {
				break
			}
		}
		if err != nil {
			break
		}
	}
	g.answer(c)
	_, _ = c.client.Close(), c.server.Close()
	g.mu.Lock()
	delete(g.conns, c)
	g.mu.Unlock()
}

func (g *transferNATSGate) answer(c *transferGateConn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c.forwarded && !c.answered {
		g.inflight--
		if g.sealed {
			g.late++
		}
	}
	c.answered = true
}

// The server answers a client's CONNECT+PING with PONG, or refuses it with
// -ERR; INFO precedes both. Only complete protocol lines are inspected.
func transferHandshakeAnswered(pending *[]byte) bool {
	for {
		end := bytes.Index(*pending, []byte("\r\n"))
		if end < 0 {
			return false
		}
		line := (*pending)[:end]
		*pending = (*pending)[end+2:]
		if bytes.HasPrefix(line, []byte("PONG")) || bytes.HasPrefix(line, []byte("-ERR")) {
			return true
		}
	}
}

// fence stops new handshakes and joins every CONNECT already forwarded. The
// established connections, and their pending pull requests, stay untouched.
func (g *transferNATSGate) fence(t *testing.T) {
	t.Helper()
	g.mu.Lock()
	g.fenced = true
	g.mu.Unlock()
	if !testharness.WaitFor(t, 10*time.Second, time.Millisecond, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.inflight == 0
	}) {
		t.Fatal("NATS gate: a forwarded CONNECT was never answered by the server")
	}
	g.mu.Lock()
	g.sealed = true
	g.mu.Unlock()
}

// lateAnswers is how many CONNECTs the server processed after the fence
// returned: each could have entered the auth callout while Shutdown cleared
// s.sys. A fenced restart has none; an unfenced one has many.
func (g *transferNATSGate) lateAnswers() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.late
}

// reopen points new connections at the restarted server on the same store.
func (g *transferNATSGate) reopen(upstream string) {
	g.mu.Lock()
	g.upstream, g.fenced, g.sealed = upstream, false, false
	g.mu.Unlock()
}

func (g *transferNATSGate) refusals() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.refused
}

func (g *transferNATSGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	_ = g.listener.Close()
	<-g.accepted
	g.mu.Lock()
	for c := range g.conns {
		_, _ = c.client.Close(), c.server.Close()
	}
	g.mu.Unlock()
	g.wg.Wait()
}

// A real same-store restart delivers "409 Server Shutdown" to a pending pull
// over its still-established connection, while a storm of authenticated
// CONNECTs runs against the gated listener. The binary runs under -race, so
// an unfenced CONNECT inside the auth callout during Shutdown is red even
// when the interleaving does not dereference the cleared server state.
// origin: S7 internal review CUT-CAPACITY F5 / hot-update S7 plan §10.3 / T46
func TestTransferSameStoreRestartFencesAuthenticationAndKeeps409(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRestartAuthentication(t, a)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer same-store authentication restart", g)
	testharness.AssertNoFDLeak(t, "transfer same-store authentication restart", fd, 0)
}

func exerciseTransferRestartAuthentication(t *testing.T, a transferArtifacts) {
	t.Helper()
	s := newTransferRestartStack(t, a)
	defer s.close()
	stream, err := s.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "restart409", Subjects: []string{"restart409"}, Storage: jetstream.FileStorage})
	transferMust(t, err)
	consumer, err := stream.CreateOrUpdateConsumer(t.Context(), jetstream.ConsumerConfig{Durable: "reader", AckPolicy: jetstream.AckExplicitPolicy})
	transferMust(t, err)
	reply, err := s.nc.SubscribeSync(nats.NewInbox())
	transferMust(t, err)
	defer func() { _ = reply.Unsubscribe() }()
	transferMust(t, s.nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.restart409.reader", reply.Subject, []byte(`{"batch":1,"expires":30000000000}`)))
	transferMust(t, s.nc.FlushTimeout(time.Second))
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		info, err := consumer.Info(t.Context())
		return err == nil && info.NumWaiting == 1
	}) {
		t.Fatal("no pending real pull request at the restart boundary")
	}
	identity, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	var workers sync.WaitGroup
	var connected atomic.Int64
	stop := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				nc, err := nats.Connect(s.url, nats.Timeout(100*time.Millisecond), nats.NoReconnect(), nats.Name(cli.CtlNameForSession(s.sid)),
					nats.Nkey(identity.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(identity.Seed, nonce) }))
				if err == nil {
					connected.Add(1)
					nc.Close()
				}
			}
		}()
	}
	defer func() { close(stop); workers.Wait() }()
	// The storm must be authenticating through the real callout before the
	// restart, or the fence below would be vacuous.
	if !testharness.WaitFor(t, 10*time.Second, time.Millisecond, func() bool { return connected.Load() >= 4 }) {
		t.Fatal("no authenticated CONNECT reached the callout before the restart")
	}
	refusedBefore := s.gate.refusals()
	s.interruptTransport("same-store-js")
	message, err := reply.NextMsg(5 * time.Second)
	if err != nil || message.Header.Get("Status") != "409" || message.Header.Get("Description") != "Server Shutdown" {
		t.Fatalf("same-store restart lost the pending pull's 409 Server Shutdown: %v message=%+v", err, message)
	}
	if s.gate.refusals() == refusedBefore {
		t.Fatal("the restart fenced no concurrent CONNECT; the authentication race was not exercised")
	}
	before := connected.Load()
	if !testharness.WaitFor(t, 10*time.Second, time.Millisecond, func() bool { return connected.Load() > before }) {
		t.Fatal("authenticated CONNECTs did not resume after the same-store restart")
	}
}

// origin: S7 internal review CUT-CAPACITY F6
func TestTransferCarrierProxyJoinsEverySocket(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferCarrierProxy(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer carrier TCP proxy", g)
	testharness.AssertNoFDLeak(t, "transfer carrier TCP proxy", fd, 0)
}

func exerciseTransferCarrierProxy(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	transferMust(t, err)
	defer func() { _ = listener.Close() }()
	body := bytes.Repeat([]byte("carrier"), 16384)
	done := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = connection.Close() }()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		data := make([]byte, len(body))
		_, err = io.ReadFull(connection, data)
		if err == nil && !bytes.Equal(data, body) {
			err = errors.New("proxy changed original carrier bytes")
		}
		if err == nil {
			_, err = connection.Write(data)
		}
		done <- err
	}()
	proxy := newTransferCarrier(t, listener.Addr().String())
	defer proxy.close()
	connection, err := net.DialTimeout("tcp", proxy.listener.Addr().String(), time.Second)
	transferMust(t, err)
	defer func() { _ = connection.Close() }()
	transferMust(t, connection.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = connection.Write(body)
	transferMust(t, err)
	data := make([]byte, len(body))
	_, err = io.ReadFull(connection, data)
	transferMust(t, err)
	if !bytes.Equal(data, body) {
		t.Fatal("carrier proxy changed the echo")
	}
	transferMust(t, <-done)
}
