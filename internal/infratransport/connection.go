package infratransport

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// Connection binds a raw socket to the generation and dial sequence that own
// it. These are local inventory keys; P02 still supplies physical socket proof.
type Connection struct {
	net.Conn
	Generation uint64
	Sequence   uint64
	mu         sync.Mutex
	owner      *Transport
	deadline   time.Time
}

// RawConn exposes the owned socket to physical census, never a claimed CID.
func (c *Connection) RawConn() net.Conn { return c.Conn }

func (c *Connection) clamp(deadline time.Time) time.Time {
	// origin: hot-update S9 plan §4.5 / T33 — nats.go clears deadlines during WS setup; admission must remain bounded.
	if !c.deadline.IsZero() && (deadline.IsZero() || deadline.After(c.deadline)) {
		return c.deadline
	}
	return deadline
}

func (c *Connection) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetDeadline(c.clamp(deadline))
}
func (c *Connection) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetReadDeadline(c.clamp(deadline))
}
func (c *Connection) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.SetWriteDeadline(c.clamp(deadline))
}
func (c *Connection) admit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = time.Time{}
	return c.Conn.SetDeadline(time.Time{})
}
func (c *Connection) Close() error {
	err := c.Conn.Close()
	c.owner.mu.Lock()
	delete(c.owner.conns, c)
	c.owner.mu.Unlock()
	return err
}

// Buffered wraps a conn whose initial bytes may have been buffered by the
// bufio.Reader used to parse the CONNECT response. Read drains the buffer first.
//
// LOAD-BEARING: http.ReadResponse can read ahead past the response's blank line
// into the first tunnel bytes, and a NATS server sends its INFO line IMMEDIATELY
// on connect (very likely in the same TCP segment as the 200 CONNECT response),
// so returning a bare conn would silently swallow INFO and hang the handshake.
// origin: hot-update S9 plan §10.1 — moved from proxydial, preserving read-ahead semantics.
func Buffered(conn net.Conn, reader *bufio.Reader) net.Conn {
	return &bufferedConn{Conn: conn, r: reader}
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// RawConn lets physical census unwrap CONNECT buffering down to the TCP hop.
func (c *bufferedConn) RawConn() net.Conn { return c.Conn }
