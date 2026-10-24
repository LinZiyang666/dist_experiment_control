package tunnel

import (
	"net"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/tunnel/transport"
	"github.com/hashicorp/yamux"
)

// Snapshot the exact legacy yamux incarnation once. A stale supervisor cannot
// borrow a replacement session's TLS identity for an original business socket.
// origin: hot-update S5 plan §4.1 / §9.4 / DRILL-OBSERVER
func observeLegacySession(c *Client, port int, mux *yamux.Session) *drillobserve.Event {
	if drillobserve.Hook == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.sessions[port]
	if current == nil || current.yamuxSess != mux {
		return nil
	}
	e := observedCarrier(current.conn)
	if e == nil {
		return nil
	}
	e.Kind = "legacy-binding"
	e.Values[0] = uint64(port)
	e.Values[1] = current.gen
	return e
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func observedCarrier(conn net.Conn) *drillobserve.Event { return transport.ObservedCarrier(conn) }
func observeAttachment(conn net.Conn, endpoint flow.EndpointID, owner uint64, carrier uint64, mode uint64, port int, kind string) {
	transport.ObserveAttachment(conn, endpoint, owner, carrier, mode, port, kind)
}
func observeFlowSocket(id flow.ID, endpoint flow.EndpointID, tuple rawsocket.Tuple) {
	transport.ObserveFlowSocket(id, endpoint, tuple)
}

func observeLegacyBridge(session *drillobserve.Event, stream net.Conn, local net.Conn) {
	transport.ObserveLegacyBridge(session, stream, local)
}
