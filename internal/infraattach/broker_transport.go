package infraattach

import (
	"net"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransport"
)

type authTransport struct {
	transport   *infratransport.Transport
	address     string
	deadline    c.Deadline
	observation *Client
	broker      *Broker
}

func newAuthTransport(o *Broker, transport *infratransport.Transport, d c.Deadline) *authTransport {
	return &authTransport{transport: transport, address: o.address, deadline: d, observation: brokerObserver(o), broker: o}
}

func (a *authTransport) Dial(network, address string) (net.Conn, error) {
	if address != a.address {
		return nil, c.ErrIncomplete
	}
	return dialRaw(a.transport, network, address, a.deadline, nil)
}
func (a *authTransport) Cancel() {
	if client := a.observation; client != nil {
		client.mu.Lock()
		if !client.closed {
			client.closed = true
			unregisterClientObservation(client)
		}
		client.mu.Unlock()
	}
	a.transport.Interrupt()
	if a.observation != nil {
		a.observation.active.Wait()
	}
}
func (a *authTransport) Join(d c.Deadline) error {
	a.transport.Interrupt()
	_, err := controlbudget.Remaining(d)
	return err
}

// dialRaw unwraps P14's admission clamp, which has no SDK admit hook outside
// Transport.Connect, and re-applies it until nats.go clears the deadline at
// the end of CONNECT/PONG. Without that, every long-lived NC would die when
// its admission budget expired.
func dialRaw(t *infratransport.Transport, network, address string, deadline c.Deadline, fence func(*brokerRaw)) (*brokerRaw, error) {
	conn, err := t.Dial(network, address)
	if err != nil {
		return nil, err
	}
	owned, ok := conn.(*infratransport.Connection)
	if !ok {
		_ = conn.Close()
		return nil, c.ErrIncomplete
	}
	left, err := controlbudget.Remaining(deadline)
	if err != nil {
		_ = owned.Close()
		return nil, err
	}
	raw := &brokerRaw{Conn: owned.RawConn(), owned: owned, fence: fence, limit: time.Now().Add(left)}
	if err := raw.Conn.SetDeadline(raw.limit); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return raw, nil
}

type brokerRaw struct {
	net.Conn
	owned *infratransport.Connection
	fence func(*brokerRaw)
	mu    sync.Mutex
	limit time.Time
}

// RawConn exposes the owned socket to physical census, never a claimed CID.
func (r *brokerRaw) RawConn() net.Conn { return r.Conn }

func (r *brokerRaw) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	if err != nil && r.fence != nil {
		r.fence(r)
	}
	return n, err
}

func (r *brokerRaw) Close() error {
	if r.fence != nil {
		r.fence(r)
	}
	return r.owned.Close()
}

func (r *brokerRaw) interrupt() {
	_ = r.Conn.SetDeadline(time.Unix(1, 0))
	_ = r.Close()
}

// nats.go clears the full deadline exactly once, when its CONNECT/PONG ends;
// that is the admission receipt. Until then every deadline (including the
// websocket handshake's read-deadline reset) is clamped to the budget.
func (r *brokerRaw) SetDeadline(d time.Time) error { return r.set(d, true, r.Conn.SetDeadline) }
func (r *brokerRaw) SetReadDeadline(d time.Time) error {
	return r.set(d, false, r.Conn.SetReadDeadline)
}
func (r *brokerRaw) SetWriteDeadline(d time.Time) error {
	return r.set(d, false, r.Conn.SetWriteDeadline)
}
func (r *brokerRaw) set(d time.Time, full bool, apply func(time.Time) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if full && d.IsZero() {
		r.limit = time.Time{}
	} else if !r.limit.IsZero() && (d.IsZero() || d.After(r.limit)) {
		d = r.limit
	}
	return apply(d)
}
