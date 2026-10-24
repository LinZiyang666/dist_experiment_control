package tunnel

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/yamux"
)

// origin: hot-update S5 plan §4.1 / §9.1 / DRILL-OBSERVER
func TestCarrierObservationUsesNegotiatedSessionAndOriginalPrefix(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCarrierObservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "observed TLS session", g)
	testharness.AssertNoFDLeak(t, "observed TLS session", fd, 0)
}
func exerciseCarrierObservation(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var events []drillobserve.Event
	prior := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) {
		e.Data = append([]byte(nil), e.Data...)
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	defer func() { drillobserve.Hook = prior }()
	cert, pins := replayTestCertificate(t)
	left, right := net.Pipe()
	defer func() {
		if err := left.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	defer func() {
		if err := right.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	server := tls.Server(left, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	client := tls.Client(right, clientTLSConfigPinned(pins))
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wrapper := retainRegisterReader(client, bufio.NewReader(client))
	observeAttachment(client, flow.EndpointID{5}, 1, 2, 1, 14001, "resume")
	observeAttachment(client, flow.EndpointID{5}, 1, 2, 3, 14001, "prepare")
	prefix := []byte{0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 12}
	go func() { _, err := server.Write(prefix); done <- err }()
	b := make([]byte, len(prefix))
	if _, err := io.ReadFull(wrapper, b); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	observed := observedCarrier(server)
	state := server.ConnectionState()
	identity, err := state.ExportKeyingMaterial("tether-drill-observer-v1", nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	if observed != nil && observed.Digest != sha256.Sum256(identity) {
		t.Fatal("receipt substituted another TLS session identity")
	}
	if observed == nil {
		t.Fatal("TLS identity unavailable")
	}
	mu.Lock()
	if len(events) != 4 || events[0].Kind != "carrier-negotiated" || len(events[0].Data) != 0 || events[0].Digest == ([32]byte{}) || events[3].Digest != observed.Digest || !bytes.Equal(events[3].Data, prefix) || events[1].Kind != "resume" || events[2].Kind != "prepare" || events[1].Endpoint != ([16]byte{5}) || events[1].Values[:4][3] != 14001 {
		mu.Unlock()
		t.Fatal("negotiated empty ALPN, session identity or original raw prefix missing")
	}
	mu.Unlock()
	exerciseLegacyObservedBinding(t, client, observed.Digest, &mu, &events)
}

// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func TestFlowSocketObservationBindsIndependentOriginalTuple(t *testing.T) {
	var event drillobserve.Event
	prior := drillobserve.Hook
	defer func() { drillobserve.Hook = prior }()
	drillobserve.Hook = func(e drillobserve.Event) { event = e }
	id := flow.ID{7}
	observeFlowSocket(id, flow.EndpointID{8}, rawsocket.Tuple{Local: netip.MustParseAddrPort("127.0.0.1:31000"), Remote: netip.MustParseAddrPort("127.0.0.1:14001")})
	parts := []string{"tcp", fmt.Sprintf("%x", sha256.Sum256([]byte("0100007F:36B1"))), fmt.Sprintf("%x", sha256.Sum256([]byte("0100007F:7918")))}
	if event.Flow != [16]byte(id) || event.Endpoint != ([16]byte{8}) || event.Digest != sha256.Sum256([]byte(strings.Join(parts, "\x00"))) {
		t.Fatal("original peer tuple does not bind product flow")
	}
	c := &registerReaderConn{Observation: &event}
	observeCarrierPrefix(c, []byte{0, 1, 0}, false)
	if c.ReadObserved {
		t.Fatal("partial header recorded as complete")
	}
	observeCarrierPrefix(c, make([]byte, 9), false)
	if !c.ReadObserved || event.Kind != "carrier-prefix" || len(event.Data) != 12 {
		t.Fatal("original fragmented header missing")
	}
	drillobserve.Hook = nil
	observeFlowSocket(id, flow.EndpointID{}, rawsocket.Tuple{}) // invalid tuple must remain untouched while inert
}

func exerciseLegacyObservedBinding(t *testing.T, carrier net.Conn, digest [32]byte, mu *sync.Mutex, events *[]drillobserve.Event) {
	t.Helper()
	left, right := net.Pipe()
	client, err := yamux.Client(left, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	server, err := yamux.Server(right, flowMuxConfig(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	stream, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	accepted, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := accepted.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	local, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := local.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := peer.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	owner := &Client{sessions: map[int]*clientSession{14001: {conn: carrier, yamuxSess: client, gen: 7}}}
	if observeLegacySession(owner, 14001, server) != nil {
		t.Fatal("stale legacy supervisor borrowed current TLS session")
	}
	session := observeLegacySession(owner, 14001, client)
	observeLegacyBridge(session, stream, local)
	far := peer.LocalAddr().(*net.TCPAddr).AddrPort()
	near := peer.RemoteAddr().(*net.TCPAddr).AddrPort()
	tuple := drillobserve.SocketIdentity(far, near)
	mu.Lock()
	defer mu.Unlock()
	e := (*events)[len(*events)-1]
	if e.Kind != "legacy-binding" || e.Digest != digest || e.Values[0] != 14001 || e.Values[1] != 7 || e.Values[2] != uint64(stream.StreamID()) || e.Values[4] == 0 || !bytes.Equal(e.Data, tuple[:]) {
		t.Fatal("legacy original socket lost its actual TLS/stream binding")
	}
}
