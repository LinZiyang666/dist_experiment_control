//go:build infracaddy_integration && linux && amd64

package infracaddy_test

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// origin: S9 internal review INFRA-SUITE-SUPPLIES F7; hot-update S9 plan §5.2 / T66 / M10 / M12 / M17
func TestCaddyRetirementHandlesLateUpgrade(t *testing.T) {
	t.Run("old_keepalive_upgrades_after_known_cohort_moves", checkLateUpgrade)
	t.Run("finite_slow_http_finishes_before_graceful_signal", checkFiniteSlowHTTP)
	t.Run("permanent_keepalive_remains_a_retirement_responsibility", checkPermanentKeepalive)
	t.Run("old_admin_binding_cannot_load_successor", checkRetiredAdminBinding)
	t.Run("blocked_upgraded_writer_joins_before_signal", checkBlockedUpgradedWriter)
}

func checkLateUpgrade(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	old := fixture.start(t, "2.7.6", 0, "revision-A")
	warm := fixture.start(t, "2.7.6", 0, "revision-A")
	cohort := fixture.cohort(t, old)
	defer cohort.close()
	conn, reader := openKeepalive(t, old)
	defer func() { _ = conn.Close() }()
	inode := old.accepted(t, conn.LocalAddr().String())
	if inode == 0 {
		t.Fatal("idle keepalive was not accepted by the original stock process")
	}
	cohort.move(t, warm)
	cohort.check(t, "before-late-upgrade")
	// The independent NATS server now sees only the original responder and the
	// three warm NCs. The old idle TCP socket is absent from a WSS-only count.
	if fixture.server.NumClients() != 4 {
		t.Fatal("old known WSS set did not retire before the late upgrade")
	}
	if !hasAcceptedResponsibility(t, old) {
		t.Fatal("zero known WSS erased an accepted keepalive responsibility")
	}
	request := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", fixtureHost)
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatal("original keepalive did not perform a real late WebSocket upgrade")
	}
	if old.accepted(t, conn.LocalAddr().String()) != inode || old.observe(t) != old.identity {
		t.Fatal("late upgrade redialed instead of using the original accepted socket")
	}
	if !hasAcceptedResponsibility(t, old) {
		t.Fatal("late upgraded connection disappeared from retirement evidence")
	}
	// This finite client's predetermined workload ends after one upgrade. It
	// closes its own socket, independently of any retirement barrier or signal.
	_ = conn.Close()
	old.requireDrained(t)
	old.retire(t)
	fixture.requireRetired(t, old)
	cohort.check(t, "after-old-natural-exit")
}

func checkFiniteSlowHTTP(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	started := make(chan struct{})
	// The duration is fixed before Caddy starts. The product barrier cannot
	// accelerate the response, pause its producer or close the client for it.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "11")
		_, _ = io.WriteString(w, "slow-")
		w.(http.Flusher).Flush()
		close(started)
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		<-timer.C
		_, _ = io.WriteString(w, "suffix")
	}))
	defer upstream.Close()
	fixture.upstream = upstream.Listener.Addr().String()
	old := fixture.start(t, "2.7.5", 0, "revision-A")
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: upgradebudget.RequestLatency}, "tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(old.port)), old.tlsConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(upgradebudget.RequestLatency))
	_, err = io.WriteString(conn, "GET /finite HTTP/1.1\r\nHost: infra.test\r\nConnection: close\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatal("finite response never started")
	}
	if !hasAcceptedResponsibility(t, old) || old.observe(t) != old.identity {
		t.Fatal("unfinished HTTP response failed to block retirement")
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "slow-suffix" {
		t.Fatal("slow HTTP was truncated to manufacture a drained process")
	}
	_ = conn.Close()
	old.requireDrained(t)
	old.retire(t)
	fixture.requireRetired(t, old)
}

func checkPermanentKeepalive(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	old := fixture.start(t, "2.7.6", 0, "revision-A")
	conn, _ := openKeepalive(t, old)
	defer func() { _ = conn.Close() }()
	if !hasAcceptedResponsibility(t, old) {
		t.Fatal("unbounded keepalive was classified as zero old responsibility")
	}
	old.request(t, "/manifest", "manifest-revision-A")
	if old.observe(t) != old.identity {
		t.Fatal("an unresolved responsibility caused a destructive retirement")
	}
	// This is the physical defer counterexample. No SIGTERM is sent while this
	// socket is live; controller deadline and healthy-shadow policy require P20
	// composition and drill 86.F, not an invented successful transaction here.
	_ = conn.Close()
	old.requireDrained(t)
	old.retire(t)
}

func checkRetiredAdminBinding(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	old := fixture.start(t, "2.7.6", 0, "revision-A")
	admin := old.admin(t)
	defer func() { _ = admin.Close() }()
	old.retire(t)
	successor := fixture.start(t, "2.7.5", old.port, "revision-A")
	before := successor.current(t)
	if _, err := admin.Load(t.Context(), old.prepare(t, "revision-B")); err == nil {
		t.Fatal("retired admin connection accepted a configuration mutation")
	}
	if successor.current(t) != before || successor.observe(t) != successor.identity {
		t.Fatal("old invocation's admin operation changed or stopped its successor")
	}
	successor.request(t, "/sub/current", "subscription-revision-A")
	successor.retire(t)
	fixture.requireRetired(t, old, successor)
}

func openKeepalive(t *testing.T, ingress *stockCaddy) (*tls.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: upgradebudget.RequestLatency}, "tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(ingress.port)), ingress.tlsConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(upgradebudget.RequestLatency))
	if _, err := io.WriteString(conn, "GET /sub/keepalive HTTP/1.1\r\nHost: infra.test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusOK || response.Close {
		t.Fatal("stock HTTP keepalive fixture did not retain the original socket")
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(body) != "subscription-revision-A" {
		t.Fatal("keepalive prefix is incorrect")
	}
	return conn, reader
}

func hasAcceptedResponsibility(t *testing.T, ingress *stockCaddy) bool {
	t.Helper()
	for _, socket := range ingress.tcpSockets(t) {
		if socket.localPort == ingress.port && socket.state != 10 {
			return true
		}
	}
	return false
}
func checkBlockedUpgradedWriter(t *testing.T) {
	t.Helper()
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	old := fixture.start(t, "2.7.6", 0, "revision-A")
	conn, reader := openKeepalive(t, old)
	defer func() { _ = conn.Close() }()
	request := "GET / HTTP/1.1\r\nHost: infra.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatal("blocked close-control fixture did not upgrade")
	}
	stream := &websocketStream{conn: conn, reader: reader}
	protocol := bufio.NewReader(stream)
	if line, err := protocol.ReadString('\n'); err != nil || !strings.HasPrefix(line, "INFO ") {
		t.Fatal("upgraded fixture did not reach the real NATS server")
	}
	if err := writeWebsocketFrame(conn, 2, []byte("CONNECT {\"verbose\":false,\"pedantic\":false}\r\nSUB infra.block 1\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	if line, err := protocol.ReadString('\n'); err != nil || line != "PONG\r\n" {
		t.Fatal("blocked writer fixture subscription did not flush")
	}
	tcp, ok := conn.NetConn().(*net.TCPConn)
	if !ok {
		t.Fatal("cannot establish actual receiver backpressure")
	}
	constrainBlockedReceiver(t, tcp)
	payload := bytes.Repeat([]byte("stock-writer-prefix:"), 32<<10)
	for message := 0; message < 16; message++ {
		if err := fixture.responder.Publish("infra.block", payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.responder.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	inode := old.accepted(t, conn.LocalAddr().String())
	if inode == 0 {
		t.Fatal("blocked writer has no expected accepted-socket identity")
	}
	waitFixture(t, time.Second, func() bool {
		for _, socket := range old.tcpSockets(t) {
			if socket.inode == inode && socket.queued > 0 {
				return true
			}
		}
		return false
	})
	if !hasAcceptedResponsibility(t, old) || old.observe(t) != old.identity {
		t.Fatal("blocked upgraded write failed to retain its retirement responsibility")
	}
	// Consume the finite original stream before close-control. No signal, config
	// reload or grace timer may truncate these outstanding bytes to make zero.
	if err := tcp.SetReadBuffer(4 << 20); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(upgradebudget.RequestLatency))
	for message := 0; message < 16; message++ {
		line, err := protocol.ReadString('\n')
		fields := strings.Fields(line)
		if err != nil || len(fields) != 4 || fields[0] != "MSG" || fields[1] != "infra.block" {
			t.Fatalf("blocked upgraded stream lost a NATS message header: %q: %v", line, err)
		}
		length, err := strconv.Atoi(fields[3])
		if err != nil || length != len(payload) {
			t.Fatal("blocked upgraded stream changed its original byte responsibility")
		}
		body := make([]byte, length+2)
		if n, err := io.ReadFull(protocol, body); err != nil || !bytes.Equal(body[:length], payload) || string(body[length:]) != "\r\n" {
			t.Fatalf("blocked upgraded stream truncated, duplicated or corrupted bytes: message=%d bytes=%d want=%d error=%v", message, n, len(body), err)
		}
	}
	if err := writeWebsocketFrame(conn, 8, []byte{3, 232}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		t.Fatal("upgraded close-control did not join before graceful retirement")
	}
	_ = conn.Close()
	old.requireDrained(t)
	old.retire(t)
	fixture.requireRetired(t, old)
}

func writeWebsocketFrame(conn net.Conn, opcode byte, payload []byte) error {
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	frame := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		frame = append(frame, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		frame = append(frame, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	default:
		frame = append(frame, 0x80|127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	frame = append(frame, mask[:]...)
	for i, value := range payload {
		frame = append(frame, value^mask[i%4])
	}
	_, err := io.Copy(conn, bytes.NewReader(frame))
	return err
}

type wssParticipant struct {
	role       string
	transport  *infratransport.Transport
	nc         *nats.Conn
	cid        uint64
	serverID   string
	socket     c.Socket
	accepted   uint64
	ingress    c.Process
	generation uint64
}

type wssCohort struct {
	fixture *caddyFixture
	rows    []*wssParticipant
	id      string
	seq     uint64
}

func (f *caddyFixture) cohort(t *testing.T, ingress *stockCaddy) *wssCohort {
	t.Helper()
	cohort := &wssCohort{fixture: f, id: randomInvocation(t)}
	for _, role := range []string{"control", "data", "consumer"} {
		row := &wssParticipant{role: role}
		cohort.rows = append(cohort.rows, row)
		cohort.attach(t, row, ingress)
	}
	t.Cleanup(cohort.close)
	return cohort
}

func (cohort *wssCohort) attach(t *testing.T, row *wssParticipant, ingress *stockCaddy) {
	t.Helper()
	row.generation++
	transport, err := infratransport.New(row.generation, fixtureDeadline(t, upgradebudget.RequestLatency), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	row.transport = transport
	nc, err := transport.Connect(fmt.Sprintf("wss://127.0.0.1:%d", ingress.port),
		func() []nats.Option {
			return []nats.Option{nats.Secure(ingress.tlsConfig()), nats.NoReconnect(), nats.Name(cohort.id + "-" + row.role)}
		})
	if err != nil {
		t.Fatal("stock Caddy WSS attachment failed")
	}
	row.nc = nc
	if err := nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	row.cid, err = nc.GetClientID()
	if err != nil || row.cid == 0 || len(transport.RawConnections()) != 1 {
		t.Fatal("attachment lacks a real NATS cid and one raw transport")
	}
	row.serverID = nc.ConnectedServerId()
	raw := transport.RawConnections()[0].RawConn()
	tcp, ok := raw.(*net.TCPConn)
	if !ok {
		t.Fatal("transport does not expose the original TCP socket")
	}
	control, err := tcp.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	var socketErr error
	var cookie uint64
	if err := control.Control(func(fd uintptr) {
		socketErr = unix.Fstat(int(fd), &stat)
		if socketErr == nil {
			cookie, socketErr = unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
		}
	}); err != nil || socketErr != nil || cookie == 0 {
		t.Fatal("raw socket identity is incomplete")
	}
	row.socket = c.Socket{Device: uint64(stat.Dev), Inode: stat.Ino, Cookie: cookie,
		Local: tcp.LocalAddr().String(), Remote: tcp.RemoteAddr().String()}
	row.ingress = ingress.observe(t)
	row.accepted = ingress.accepted(t, row.socket.Local)
	if row.accepted == 0 || row.ingress != ingress.identity {
		t.Fatal("new cid has no accepted-socket to running-process proof")
	}
}

func (cohort *wssCohort) move(t *testing.T, ingress *stockCaddy) {
	t.Helper()
	for _, row := range cohort.rows {
		before := *row
		if err := infratransport.Move(row.transport, row.nc, fixtureDeadline(t, upgradebudget.RequestLatency), nil); err != nil {
			t.Fatal(err)
		}
		cohort.attach(t, row, ingress)
		// origin: hot-update S9 plan §2.3 / §5.2 / T65 / M17
		if row.cid == before.cid && row.serverID == before.serverID || row.socket.Cookie == before.socket.Cookie ||
			row.socket.Inode == before.socket.Inode || row.ingress == before.ingress || row.generation <= before.generation {
			t.Fatal("migration changed labels without changing physical attachment")
		}
	}
}

func (cohort *wssCohort) check(t *testing.T, phase string) {
	t.Helper()
	if len(cohort.rows) != 3 {
		t.Fatal("original cohort lost independent data or consumer NC")
	}
	for _, row := range cohort.rows {
		cohort.seq++
		payload := []byte(fmt.Sprintf("%s:%s:%d:%s", cohort.id, row.role, cohort.seq, phase))
		message, err := row.nc.Request("infra.original", payload, upgradebudget.RequestLatency)
		if err != nil || !bytes.Equal(message.Data, payload) {
			t.Fatal("original invocation lost or changed business bytes")
		}
	}
}

func (cohort *wssCohort) close() {
	for _, row := range cohort.rows {
		if row.transport != nil {
			row.transport.Interrupt()
			row.transport = nil
		}
		if row.nc != nil {
			row.nc.Close()
			row.nc = nil
		}
	}
}

// accepted reads the independent kernel socket table and then proves inode
// ownership through the expected stock process's FD directory. It deliberately
// does not label this limited unprivileged view a complete host/conntrack census.
func (s *stockCaddy) accepted(t *testing.T, peer string) uint64 {
	t.Helper()
	_, peerPort, err := net.SplitHostPort(peer)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(peerPort)
	if err != nil {
		t.Fatal(err)
	}
	for _, socket := range s.tcpSockets(t) {
		if socket.localPort == s.port && socket.remotePort == port && socket.state == 1 {
			return socket.inode
		}
	}
	return 0
}

type kernelTCP struct {
	localPort, remotePort int
	state                 uint64
	inode                 uint64
	queued                uint64
	receiveQueued         uint64
}

func (s *stockCaddy) tcpSockets(t *testing.T) []kernelTCP {
	t.Helper()
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", s.identity.PID))
	if err != nil {
		t.Fatal("cannot enumerate the expected process socket owners")
	}
	owned := make(map[uint64]bool)
	for _, fd := range fds {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", s.identity.PID, fd.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // A concurrent admin/upstream close cannot contribute a false owner.
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(link, "socket:[") {
			inode, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]"), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			owned[inode] = true
		}
	}
	var rows []kernelTCP
	for _, family := range []string{"tcp", "tcp6"} {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/%s", s.identity.PID, family))
		if err != nil {
			t.Fatal("kernel TCP observation is incomplete")
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		if !scanner.Scan() {
			t.Fatal("kernel TCP table has no header")
		}
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 10 {
				t.Fatal("kernel TCP row is incomplete")
			}
			inode, inodeErr := strconv.ParseUint(fields[9], 10, 64)
			state, stateErr := strconv.ParseUint(fields[3], 16, 64)
			local, localErr := strconv.ParseUint(strings.Split(fields[1], ":")[1], 16, 16)
			remote, remoteErr := strconv.ParseUint(strings.Split(fields[2], ":")[1], 16, 16)
			queued, queueErr := strconv.ParseUint(strings.Split(fields[4], ":")[0], 16, 64)
			receiveQueued, receiveQueueErr := strconv.ParseUint(strings.Split(fields[4], ":")[1], 16, 64)
			if inodeErr != nil || stateErr != nil || localErr != nil || remoteErr != nil || queueErr != nil || receiveQueueErr != nil {
				t.Fatal("kernel TCP identity cannot be decoded")
			}
			if owned[inode] {
				rows = append(rows, kernelTCP{localPort: int(local), remotePort: int(remote), state: state, inode: inode,
					queued: queued, receiveQueued: receiveQueued})
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return rows
}

// origin: hot-update S9 plan §5.2 / T66 / M12
func (s *stockCaddy) requireDrained(t *testing.T) {
	t.Helper()
	limit := time.NewTimer(upgradebudget.RequestLatency)
	defer limit.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		pending := 0
		sockets := s.tcpSockets(t)
		listeners := map[int]bool{s.port: true}
		for _, socket := range sockets {
			if socket.state == 10 {
				listeners[socket.localPort] = true
			}
		}
		for _, socket := range sockets {
			if listeners[socket.localPort] && (socket.state != 10 || socket.receiveQueued != 0) {
				pending++
			}
		}
		if pending == 0 {
			return
		}
		select {
		case <-limit.C:
			t.Fatal("retirement reached a live HTTP/keepalive/upgraded socket")
		case <-tick.C:
		}
	}
}
