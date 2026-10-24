package infraattach

import (
	"crypto/rand"
	"crypto/sha256"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
)

// Observe reconciles independent evidence with facts held by this SDK and
// this raw owner. URL edits cannot change the real CID, dial generation or TCP
// endpoints. The supplied witness remains responsible for kernel cookie and
// accepted ingress process identity, including the Caddy invocation on WSS.
// An unavailable witness refuses the attachment instead of certifying a pool.
func (client *Client) Observe(d c.Deadline) (c.Attachment, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return c.Attachment{}, err
	}
	client.mu.Lock()
	nc, raw, closed := client.nc, client.raw, client.closed
	if !closed {
		client.active.Add(1)
	}
	client.mu.Unlock()
	if !closed {
		defer client.active.Done()
	}
	if closed || nc == nil || raw == nil || client.config.Observe == nil {
		return c.Attachment{}, c.ErrUnsupported
	}
	if !nc.IsConnected() {
		return c.Attachment{}, c.ErrIncomplete
	}
	remaining, err := controlbudget.Remaining(d)
	if err == nil {
		err = nc.FlushTimeout(remaining)
	}
	if err != nil {
		return c.Attachment{}, err
	}
	a, err := client.config.Observe(nc, raw.Conn, client.id, raw.generation, d)
	cid, cidErr := nc.GetClientID()
	if err == nil && (cidErr != nil || a.Participant != client.id || a.Generation != raw.generation || a.CID != cid || a.ServerID != nc.ConnectedServerId() || a.URL != nc.ConnectedUrl() || a.Raw.Local != raw.LocalAddr().String() || a.Raw.Remote != raw.RemoteAddr().String()) {
		err = c.ErrIncomplete
	}
	// Reconnect and Close can run while the independent witness is reading.
	// Its old socket must never be published as this NC's current attachment.
	// origin: hot-update S9 plan §2.3 / §4.5; s9-instr4 Request 1
	connected := nc.IsConnected()
	client.mu.Lock()
	if client.closed || client.nc != nc || client.raw != raw || !connected {
		err = c.ErrIncomplete
	}
	client.mu.Unlock()
	if _, e := controlbudget.Remaining(d); e != nil {
		err = e
	}
	if err == nil {
		err = drillobserve.RecordAttachment(a)
	}
	return a, err
}

func registerClientObservation(client *Client, err error) {
	if err != nil || drillobserve.Hook == nil || client.closed {
		return
	}
	// Called under client.mu so Close cannot unregister just before this add.
	drillobserve.RegisterInfrastructure(client.id, func(d c.Deadline) error { _, e := client.Observe(d); return e })
}

func unregisterClientObservation(client *Client) { drillobserve.UnregisterInfrastructure(client.id) }

func brokerObserver(o *Broker) *Client {
	if drillobserve.Hook == nil || o.cfg.Infra == nil {
		return nil
	}
	client := &Client{config: Config{Observe: o.cfg.Infra.Observe}}
	if _, err := rand.Read(client.id[:]); err != nil {
		return nil
	}
	return client
}

// The long-lived broker business NC has its own raw admission owner. Its
// observer borrows that exact owner on every challenge, including reconnects;
// it never dials an auxiliary NC to stand in for the original.
func registerBrokerObservation(o *Broker, nc *nats.Conn) {
	client := brokerObserver(o)
	if client == nil {
		return
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.observation = client
	drillobserve.RegisterInfrastructure(client.id, func(d c.Deadline) error {
		o.mu.Lock()
		raw, generation, closed := o.raw, o.sequence, o.closed
		o.mu.Unlock()
		if closed || raw == nil {
			return c.ErrIncomplete
		}
		client.mu.Lock()
		client.nc, client.raw = nc, &admittedRaw{Conn: raw.Conn, generation: generation}
		client.mu.Unlock()
		_, err := client.Observe(d)
		o.mu.Lock()
		if o.closed || o.raw != raw || o.sequence != generation {
			err = c.ErrIncomplete
		}
		o.mu.Unlock()
		return err
	})
	o.mu.Unlock()
}

// The auth owner calls this optional port AFTER the actual CONNECT, on every
// retained NC it constructs. Cancel unregisters before retiring its raw owner.
func (a *authTransport) Connected(nc *nats.Conn) {
	client := a.observation
	if client == nil {
		return
	}
	raw := a.transport.RawConnections()
	if len(raw) != 1 {
		return
	}
	// P17 recreates its NCs on ordinary admission. Keep each responsibility's
	// participant stable and advance its generation only after a real CONNECT.
	// Repeated retirement of the previous owner cannot remove its successor.
	o := a.broker
	o.mu.Lock()
	if o.observation == nil || o.closed {
		o.mu.Unlock()
		return
	}
	if o.observationNext == nil {
		o.observationNext = make(map[string]uint64)
	}
	name := nc.Opts.Name
	o.observationNext[name]++
	generation := o.observationNext[name]
	identity := sha256.Sum256(append(append([]byte(nil), o.observation.id[:]...), []byte(name)...))
	o.mu.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return
	}
	copy(client.id[:], identity[:16])
	client.nc, client.raw = nc, &admittedRaw{Conn: raw[0].RawConn(), generation: generation}
	registerClientObservation(client, nil)
}
