package broker

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// infraWire is a byte-faithful TCP relay in front of the broker's local
// server. It records every client→server verb of EVERY broker connection in
// the order the server receives them — the server-side order the auth-first
// contract is about, not the order of client-side callbacks or log lines.
// It is the test-side witness for infra_wiring_test.go, never a product port.
// origin: hot-update S9 plan §4.4 / T48; rv-business F2 ("real Broker.Run restart recording the order")
type infraWire struct {
	ln       net.Listener
	addr     string
	upstream string
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	events   []infraWireEvent
	next     int
	closed   bool
	wg       sync.WaitGroup
	// Tests may delay EOF delivery while preserving every real wire byte. A
	// stock server stopping does not synchronously retire its clients' gates.
	beforeClientClose func()
}

type infraWireEvent struct {
	conn                       int
	name, verb, subject, queue string
}

func newInfraWire(t *testing.T, upstream string) *infraWire {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	w := &infraWire{ln: ln, addr: ln.Addr().String(), upstream: upstream, conns: map[net.Conn]struct{}{}}
	w.wg.Add(1)
	go w.accept(ln)
	t.Cleanup(w.close)
	return w
}

func (w *infraWire) url() string { return "nats://" + w.addr }

// suspend stops listening while the local server is down, so a client dial is
// refused as it would be against the real port. An accepting relay with no
// upstream would instead hand nats.go a socket that it never closes after the
// failed INFO read (CLOSE_WAIT until GC) — a fixture artifact, not product.
func (w *infraWire) suspend() {
	w.mu.Lock()
	ln := w.ln
	w.mu.Unlock()
	_ = ln.Close()
}

func (w *infraWire) resume(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", w.addr)
	if err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	w.ln = ln
	w.wg.Add(1)
	w.mu.Unlock()
	go w.accept(ln)
}

func (w *infraWire) accept(ln net.Listener) {
	defer w.wg.Done()
	for {
		client, err := ln.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", w.upstream)
		if err != nil {
			_ = client.Close() // the local server is down: the SDK must redial
			continue
		}
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			_, _ = client.Close(), upstream.Close()
			return
		}
		w.next++
		id := w.next
		w.conns[client], w.conns[upstream] = struct{}{}, struct{}{}
		w.wg.Add(2)
		w.mu.Unlock()
		go func() {
			defer w.wg.Done()
			// Plain read/write: io.Copy between two TCP conns would splice(2)
			// through pooled pipes the runtime frees lazily, which the FD
			// leak gate would then report against the product.
			_, _ = io.Copy(struct{ io.Writer }{client}, struct{ io.Reader }{upstream})
			w.mu.Lock()
			beforeClose := w.beforeClientClose
			w.mu.Unlock()
			if beforeClose != nil {
				beforeClose()
			}
			_, _ = client.Close(), upstream.Close()
		}()
		go func() {
			defer w.wg.Done()
			w.relay(id, client, upstream)
			_, _ = client.Close(), upstream.Close()
		}()
	}
}

// relay records a verb BEFORE forwarding it, so the recorded order is never
// later than the order the server can observe.
func (w *infraWire) relay(id int, client, upstream net.Conn) {
	r := bufio.NewReaderSize(client, 64<<10)
	name := ""
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		var body []byte
		if len(fields) > 0 {
			switch verb := strings.ToUpper(fields[0]); verb {
			case "CONNECT":
				var info struct {
					Name string `json:"name"`
				}
				_ = json.Unmarshal([]byte(strings.TrimSpace(line[len("CONNECT "):])), &info)
				name = info.Name
				w.record(infraWireEvent{conn: id, name: name, verb: verb})
			case "SUB":
				queue := ""
				if len(fields) == 4 {
					queue = fields[2]
				}
				w.record(infraWireEvent{conn: id, name: name, verb: verb, subject: fields[1], queue: queue})
			case "PUB", "HPUB":
				n, _ := strconv.Atoi(fields[len(fields)-1])
				body = make([]byte, n+2)
				if _, err := io.ReadFull(r, body); err != nil {
					return
				}
				w.record(infraWireEvent{conn: id, name: name, verb: verb, subject: fields[1]})
			case "PING":
				w.record(infraWireEvent{conn: id, name: name, verb: verb})
			}
		}
		if _, err := upstream.Write(append([]byte(line), body...)); err != nil {
			return
		}
	}
}

func (w *infraWire) record(e infraWireEvent) {
	w.mu.Lock()
	w.events = append(w.events, e)
	w.mu.Unlock()
}

func (w *infraWire) mark() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

func (w *infraWire) since(i int) []infraWireEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.events[i:])
}

func (w *infraWire) close() {
	w.mu.Lock()
	w.closed = true
	conns := make([]net.Conn, 0, len(w.conns))
	for conn := range w.conns {
		conns = append(conns, conn)
	}
	ln := w.ln
	w.mu.Unlock()
	_ = ln.Close()
	for _, conn := range conns {
		_ = conn.Close()
	}
	w.wg.Wait()
}

func infraFind(events []infraWireEvent, from int, match func(infraWireEvent) bool) int {
	for i := from; i < len(events); i++ {
		if match(events[i]) {
			return i
		}
	}
	return -1
}

func infraLast(events []infraWireEvent, before int, match func(infraWireEvent) bool) int {
	for i := before - 1; i >= 0; i-- {
		if match(events[i]) {
			return i
		}
	}
	return -1
}

const infraSID, infraNID = "infrab", "node"

// infraConnCID is the server's own CID for the named client connection, 0 if absent.
func infraConnCID(t *testing.T, s *natsserver.Server, name string) uint64 {
	t.Helper()
	z, err := s.Connz(&natsserver.ConnzOptions{Limit: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for _, conn := range z.Conns {
		if conn.Name == name {
			return conn.Cid
		}
	}
	return 0
}

var infraRegisterSubject = proto.SubjectPrefix + ".ctrl.s.*.node.*.register.req"

// requireInfraAdmission anchors on the broker's business CONNECT and proves
// what reached the local server before it: the auth-only NC's normal queue
// SUB and its flush, then both ordinary identity CONNECTs, then (when a
// recovery is sealed) the read-only ACK-prefix read. No business SUB may
// precede the business CONNECT on any connection.
// origin: hot-update S9 plan §4.4 / §4.3 / T48; rv-business F2 / rv-infra F10
func requireInfraAdmission(t *testing.T, label string, events []infraWireEvent, prefix bool) {
	t.Helper()
	business := infraFind(events, 0, func(e infraWireEvent) bool { return e.verb == "CONNECT" && e.name == "tetherd" })
	if business < 0 {
		t.Fatalf("%s: no business CONNECT on the wire: %v", label, events)
	}
	if sub := infraFind(events, 0, func(e infraWireEvent) bool { return e.verb == "SUB" && e.subject == infraRegisterSubject }); sub >= 0 && sub < business {
		t.Fatalf("%s: business SUB reached the server before the business CONNECT: %v", label, events[:business+1])
	}
	member := infraLast(events, business, func(e infraWireEvent) bool { return e.verb == "CONNECT" && e.name == "tether-cli:"+infraSID })
	agent := infraLast(events, business, func(e infraWireEvent) bool {
		return e.verb == "CONNECT" && e.name == "tether-agent:"+infraSID+":"+infraNID
	})
	queue := infraLast(events, min(member, agent), func(e infraWireEvent) bool {
		return e.name == "tetherd-auth" && e.verb == "SUB" && e.subject == "$SYS.REQ.USER.AUTH" && e.queue == "tether-authcallout"
	})
	if member < 0 || agent < 0 || queue < 0 {
		t.Fatalf("%s: business admitted without the local normal queue and both ordinary CONNECTs (queue=%d member=%d agent=%d)", label, queue, member, agent)
	}
	flush := infraFind(events, queue+1, func(e infraWireEvent) bool { return e.conn == events[queue].conn && e.verb == "PING" })
	if flush < 0 || flush > min(member, agent) {
		t.Fatalf("%s: ordinary CONNECT preceded the normal queue's PONG barrier", label)
	}
	if !prefix {
		return
	}
	read := infraLast(events, business, func(e infraWireEvent) bool {
		return e.name == "tetherd-recovery" && e.verb == "PUB" &&
			(strings.HasPrefix(e.subject, "$JS.API.STREAM.MSG.GET.") || strings.HasPrefix(e.subject, "$JS.API.DIRECT.GET."))
	})
	if read < 0 || read < max(member, agent) {
		t.Fatalf("%s: business admitted before the read-only ACK-prefix read (read=%d member=%d agent=%d business=%d)", label, read, member, agent, business)
	}
}

// infraMutation is any JetStream asset/data mutation or audit append. A
// recovery producer that reaches the wire while the seal is held is exactly
// the repair-before-read failure (P18 M20).
func infraMutation(e infraWireEvent) bool {
	if e.verb != "PUB" && e.verb != "HPUB" {
		return false
	}
	for _, prefix := range []string{"$JS.API.STREAM.CREATE.", "$JS.API.STREAM.UPDATE.", "$JS.API.STREAM.DELETE.", "$JS.API.STREAM.PURGE.",
		"$JS.API.CONSUMER.CREATE.", "$JS.API.CONSUMER.DELETE.", "$O.", proto.SubjSysEvents, proto.SubjectPrefix + ".audit."} {
		if strings.HasPrefix(e.subject, prefix) {
			return true
		}
	}
	return false
}
