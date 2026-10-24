//go:build linux

// natsrelay_test.go — a frame-aware TCP fault relay. The reply subject is
// fenced BEFORE the matching request is forwarded. We consume a real reply
// behind that fence before disconnecting: a cut alone proves no lost reply.
// No response is forged and retries are always the client's responsibility.
// origin: hot-update S4 plan §1.1 / §8.3 T36.
package cli_e2e_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type relayLoss struct {
	Request, Reply       []byte
	Subject              string
	BlockedBeforeForward bool
	ForwardedBytes       int64
}

// origin: hot-update S4 plan §8.3 / T36
// A fast close can hide an already-forwarded frame from nats.go's dispatcher.
// Measure writes at the socket boundary too: a failed Request alone cannot
// prove non-delivery, and the fence's own boolean is not independent evidence.
type relayReplyWrites struct {
	mu        sync.Mutex
	bySubject map[string]*atomic.Int64
}

type relayObservedConn struct {
	net.Conn
	writes *relayReplyWrites
}

func (c *relayObservedConn) Write(frame []byte) (int, error) {
	line, _, _ := bytes.Cut(frame, []byte("\r\n"))
	fields := strings.Fields(string(line))
	var counter *atomic.Int64
	if len(fields) > 1 && (fields[0] == "MSG" || fields[0] == "HMSG") {
		c.writes.mu.Lock()
		counter = c.writes.bySubject[fields[1]]
		c.writes.mu.Unlock()
	}
	n, err := c.Conn.Write(frame)
	if counter != nil {
		counter.Add(int64(n))
	}
	return n, err
}

type natsRelay struct {
	t          *testing.T
	ln         net.Listener
	upstream   string
	match      func(string, []byte) bool
	cut        atomic.Int32
	wg         sync.WaitGroup
	acceptDone chan struct{}
	stop       chan struct{}
	mu         sync.Mutex
	conns      map[net.Conn]struct{}
	losses     []relayLoss
	// Optional handshake: the receiver can inspect the first effect before
	// allowing reconnect/retry (signals must not coalesce in the oracle).
	release <-chan struct{}
	repeat  bool
}

func startNATSRelay(t *testing.T, upstream string, match func(string, []byte) bool) (*natsRelay, string) {
	return startNATSReplyFence(t, upstream, match, nil, false)
}

func startNATSReplyFence(t *testing.T, upstream string, match func(string, []byte) bool, release <-chan struct{}, repeat bool) (*natsRelay, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &natsRelay{t: t, ln: ln, upstream: strings.TrimPrefix(upstream, "nats://"), match: match,
		acceptDone: make(chan struct{}), stop: make(chan struct{}), conns: make(map[net.Conn]struct{}), release: release, repeat: repeat}
	go r.accept()
	t.Cleanup(func() {
		close(r.stop)
		_ = ln.Close()
		<-r.acceptDone // no Add can race Wait after the accept owner has joined
		r.mu.Lock()
		for c := range r.conns {
			_ = c.Close()
		}
		r.mu.Unlock()
		r.wg.Wait()
	})
	return r, "nats://" + ln.Addr().String()
}

func (r *natsRelay) receipts() []relayLoss {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]relayLoss(nil), r.losses...)
}

func (r *natsRelay) requireLoss(t *testing.T) relayLoss {
	t.Helper()
	losses := r.receipts()
	if len(losses) == 0 {
		t.Fatal("no reply was consumed behind the relay fence")
	}
	loss := losses[0]
	if !loss.BlockedBeforeForward || loss.ForwardedBytes != 0 || len(loss.Request) == 0 || len(loss.Reply) == 0 {
		t.Fatalf("reply non-delivery was not established: blocked=%v forwarded=%dB request=%dB reply=%dB", loss.BlockedBeforeForward, loss.ForwardedBytes, len(loss.Request), len(loss.Reply))
	}
	return loss
}

func (r *natsRelay) accept() {
	defer close(r.acceptDone)
	for {
		c, err := r.ln.Accept()
		if err != nil {
			return
		}
		r.mu.Lock()
		r.conns[c] = struct{}{}
		r.mu.Unlock()
		r.wg.Add(1)
		go r.serve(c)
	}
}

// readNATSFrame handles payload lengths, including embedded CRLF and NATS
// headers. JSON matching receives the body, not an HPUB header prefix.
func readNATSFrame(rd *bufio.Reader) (frame, body []byte, fields []string, err error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return nil, nil, nil, err
	}
	fields = strings.Fields(string(line))
	frame = line
	if len(fields) == 0 {
		return frame, nil, fields, nil
	}
	switch fields[0] {
	case "PUB", "HPUB", "MSG", "HMSG":
	default:
		return frame, nil, fields, nil
	}
	size, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil || size < 0 || size > 8<<20 {
		return nil, nil, nil, fmt.Errorf("invalid NATS frame size: %q", line)
	}
	payload := make([]byte, size+2)
	if _, err = io.ReadFull(rd, payload); err != nil {
		return nil, nil, nil, err
	}
	if !bytes.HasSuffix(payload, []byte("\r\n")) {
		return nil, nil, nil, fmt.Errorf("missing NATS payload terminator")
	}
	body = payload[:size]
	if fields[0] == "HPUB" || fields[0] == "HMSG" {
		hdr, parseErr := strconv.Atoi(fields[len(fields)-2])
		if parseErr != nil || hdr < 0 || hdr > size {
			return nil, nil, nil, fmt.Errorf("invalid NATS header length")
		}
		body = body[hdr:]
	}
	return append(frame, payload...), body, fields, nil
}

func (r *natsRelay) serve(client net.Conn) {
	defer r.wg.Done()
	originalClient := client
	defer func() { r.mu.Lock(); delete(r.conns, originalClient); r.mu.Unlock() }()
	writes := &relayReplyWrites{bySubject: make(map[string]*atomic.Int64)}
	client = &relayObservedConn{Conn: client, writes: writes}
	server, err := net.DialTimeout("tcp", r.upstream, 3*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	defer func() { _ = client.Close(); _ = server.Close() }()
	var fence sync.Mutex
	blocked := make(map[string]relayLoss)
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		rd := bufio.NewReaderSize(server, 1<<20)
		for {
			frame, body, fields, err := readNATSFrame(rd)
			if err != nil {
				return
			}
			fence.Lock()
			var loss relayLoss
			dropped := false
			if len(fields) > 1 && (fields[0] == "MSG" || fields[0] == "HMSG") {
				loss, dropped = blocked[fields[1]]
			}
			if dropped {
				writes.mu.Lock()
				counter := writes.bySubject[fields[1]]
				writes.mu.Unlock()
				loss.ForwardedBytes = counter.Load()
				loss.Reply = append([]byte(nil), body...)
				fence.Unlock()
				r.mu.Lock()
				r.losses = append(r.losses, loss)
				r.mu.Unlock()
				if r.release != nil {
					select {
					case <-r.release:
					case <-r.stop:
					}
				}
				return // serve closes both sockets only after the reply was swallowed
			}
			_, err = client.Write(frame)
			fence.Unlock()
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		rd := bufio.NewReaderSize(client, 1<<20)
		for {
			frame, body, fields, err := readNATSFrame(rd)
			if err != nil {
				return
			}
			if len(fields) > 3 && (fields[0] == "PUB" || fields[0] == "HPUB") && r.match(fields[1], body) && (r.repeat || r.cut.CompareAndSwap(0, 1)) {
				replyIndex := len(fields) - 2
				if fields[0] == "HPUB" {
					replyIndex--
				}
				if replyIndex != 2 {
					return
				} // a request without a reply cannot prove reply loss
				fence.Lock()
				// origin: hot-update S4 plan §8.3 T36 — this must precede server.Write.
				blocked[fields[replyIndex]] = relayLoss{Subject: fields[1], Request: append([]byte(nil), body...), BlockedBeforeForward: true}
				writes.mu.Lock()
				writes.bySubject[fields[replyIndex]] = new(atomic.Int64)
				writes.mu.Unlock()
				fence.Unlock()
				r.cut.Store(1)
			}
			if _, err = server.Write(frame); err != nil {
				return
			}
		}
	}()
	<-done
	_ = client.Close()
	_ = server.Close()
	<-done
}

// origin: hot-update S4 plan §8.3 T36. A fast responder is the adversary:
// forward-then-close can race it; our fence must supply a swallowed body.
func TestNATSRelayBlocksReplyBeforeRequest(t *testing.T) {
	url := startNATS(t)
	responder := connect(t, url)
	defer responder.Close()
	_, err := responder.Subscribe("relay.effect", func(m *nats.Msg) { _ = m.Respond(m.Data) })
	if err != nil {
		t.Fatal(err)
	}
	if err = responder.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, headers := range []bool{false, true} {
		t.Run(fmt.Sprintf("headers_%v", headers), func(t *testing.T) {
			r, relayURL := startNATSRelay(t, url, func(subject string, _ []byte) bool { return subject == "relay.effect" })
			nc, err := nats.Connect(relayURL, nats.NoReconnect())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			msg := nats.NewMsg("relay.effect")
			msg.Data = []byte("effect\r\nexactly once")
			if headers {
				msg.Header.Set("X-Relay-Test", "yes")
			}
			if reply, err := nc.RequestMsg(msg, 2*time.Second); err == nil {
				t.Fatalf("fenced reply reached client: %q", reply.Data)
			}
			loss := r.requireLoss(t)
			if !bytes.Equal(loss.Request, msg.Data) || !bytes.Equal(loss.Reply, msg.Data) {
				t.Fatal("relay receipt did not preserve exact bytes")
			}
		})
	}
}
