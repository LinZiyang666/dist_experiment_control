package transport

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/hashicorp/yamux"
)

// TLS exporter bytes distinguish the actual negotiated session, including
// empty-ALPN legacy peers. They stay exclusively in private custody.
// origin: hot-update S5 plan §4.1 / §9.1 / DRILL-OBSERVER
func ObservedCarrier(conn net.Conn) *drillobserve.Event {
	secure, ok := conn.(*tls.Conn)
	if !ok {
		return nil
	}
	state := secure.ConnectionState()
	if !state.HandshakeComplete {
		return nil
	}
	identity, err := state.ExportKeyingMaterial("tether-drill-observer-v1", nil, 32)
	if err != nil {
		return nil
	}
	return &drillobserve.Event{Kind: "carrier-negotiated", Domain: "tunnel", Digest: sha256.Sum256(identity), Data: []byte(state.NegotiatedProtocol)}
}

func ObserveAttachment(conn net.Conn, endpoint flow.EndpointID, owner, carrier, mode uint64, port int, kind string) {
	if drillobserve.Hook == nil {
		return
	}
	e := ObservedCarrier(conn)
	if e == nil {
		return
	}
	e.Kind = kind
	e.Endpoint = [16]byte(endpoint)
	e.Values[0] = owner
	e.Values[1] = carrier
	e.Values[2] = mode
	e.Values[3] = uint64(port)
	drillobserve.Hook(*e)
}

// Correlate the original business OFD with independently read kernel tuples.
// The reverse tuple matches the peer's /proc view; only one-way identities leave
// private custody. Startup nil guards avoid even address conversion when inert.
// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func ObserveFlowSocket(id flow.ID, endpoint flow.EndpointID, tuple rawsocket.Tuple) {
	if drillobserve.Hook == nil {
		return
	}
	family := "tcp"
	if tuple.Local.Addr().Is6() {
		family = "tcp6"
	}
	private := strings.Join([]string{family, ObservedKernelAddress(tuple.Remote), ObservedKernelAddress(tuple.Local)}, "\x00")
	parts := strings.Split(private, "\x00")
	for i := 1; i < len(parts); i++ {
		parts[i] = fmt.Sprintf("%x", sha256.Sum256([]byte(parts[i])))
	}
	drillobserve.Hook(drillobserve.Event{Kind: "flow-socket", Domain: "flow", Flow: [16]byte(id), Endpoint: [16]byte(endpoint), Digest: sha256.Sum256([]byte(strings.Join(parts, "\x00"))), Data: []byte(private)})
}

func ObservedKernelAddress(address netip.AddrPort) string {
	raw := address.Addr().Unmap().AsSlice()
	var out strings.Builder
	for i := 0; i < len(raw); i += 4 {
		fmt.Fprintf(&out, "%08X", binary.LittleEndian.Uint32(raw[i:i+4]))
	}
	return fmt.Sprintf("%s:%04X", out.String(), address.Port())
}

func ObserveAcceptedFlowSocket(id flow.ID, endpoint flow.EndpointID, tcp *net.TCPConn) {
	if drillobserve.Hook == nil {
		return
	}
	local, err := netip.ParseAddrPort(tcp.LocalAddr().String())
	if err != nil {
		return
	}
	remote, err := netip.ParseAddrPort(tcp.RemoteAddr().String())
	if err != nil {
		return
	}
	ObserveFlowSocket(id, endpoint, rawsocket.Tuple{Local: local, Remote: remote})
}

func ObserveLegacyBridge(session *drillobserve.Event, stream, local net.Conn) {
	if drillobserve.Hook == nil || session == nil {
		return
	}
	tcp, ok := local.(*net.TCPConn)
	if !ok {
		return
	}
	near, ne := netip.ParseAddrPort(tcp.LocalAddr().String())
	far, fe := netip.ParseAddrPort(tcp.RemoteAddr().String())
	if ne != nil || fe != nil {
		return
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return
	}
	var fp syscall.Stat_t
	if raw.Control(func(fd uintptr) { err = syscall.Fstat(int(fd), &fp) }) != nil || err != nil {
		return
	}
	e := *session
	if yamuxStream, ok := stream.(*yamux.Stream); ok {
		e.Values[2] = uint64(yamuxStream.StreamID())
	}
	e.Values[3] = uint64(fp.Dev)
	e.Values[4] = uint64(fp.Ino)
	tuple := drillobserve.SocketIdentity(far, near)
	e.Data = tuple[:]
	drillobserve.Hook(e)
}
