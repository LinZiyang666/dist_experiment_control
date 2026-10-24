//go:build linux

package tunnel

import (
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
)

// The SS listener checks tuple ownership before accepting a connection. Empty
// ALPN still needs that reservation, but must never acquire a fictitious v1 ID.
// origin: hot-update S5 plan §4.1 / §6.5 / §9.1 / DRILL-RUN
func dialLegacyLocal(c *Client, port int) (net.Conn, error) {
	c.mu.Lock()
	state := c.flowState
	c.mu.Unlock()
	if state == nil || state.cfg.BeforeLocalConnect == nil {
		return net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 5*time.Second)
	}
	deadline := time.Now().Add(5 * time.Second)
	socket, err := rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), deadline, nil,
		func(tuple rawsocket.Tuple) error { return state.cfg.BeforeLocalConnect(flow.ID{}, tuple) })
	if err != nil {
		return nil, err
	}
	defer func() { _ = socket.Close() }()
	if err := socket.CompleteConnect(deadline, nil); err != nil {
		return nil, err
	}
	if err := rawsocket.Hold(socket); err != nil {
		return nil, err
	}
	file, err := socket.ExportFile()
	if err != nil {
		return nil, err
	}
	// FileConn duplicates this same original OFD for the legacy net.Conn
	// bridge. The connector wrapper closes its descriptor on every exit.
	return net.FileConn(file)
}
