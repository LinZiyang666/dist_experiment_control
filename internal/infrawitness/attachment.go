// Package infrawitness composes independent kernel and health evidence with the
// retained SDK/raw owners. It never opens a replacement business connection.
// origin: hot-update S9 plan §2.3 / §4.5; NC-EXPORTS Request P02/P22
package infrawitness

import (
	"net"

	"github.com/LinZiyang666/tether/internal/infraattach"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
)

// Ingress reads the accepted socket in a complete independent census and the
// actual health generation for this server/session. The trusted deployment
// owner supplies this port, including any cross-host transport. Neither a URL
// nor the client's dial generation can supply the health generation.
type Ingress func(c.Socket, string, uint64, c.Deadline) (c.Socket, c.Census, uint64, error)

type Witness struct{ ingress Ingress }

func New(ingress Ingress) (*Witness, error) {
	if ingress == nil {
		return nil, c.ErrIncomplete
	}
	return &Witness{ingress: ingress}, nil
}

// BindClient and BindBroker install exactly the same physical witness on the
// normal composition ports. Inherit carries the client port to every original
// transfer NC; auth-first reconnects borrow the broker port for their actual NCs.
func (w *Witness) BindClient(config *infraattach.Config) error {
	if w == nil || w.ingress == nil || config == nil {
		return c.ErrIncomplete
	}
	config.Observe = w.Observe
	return nil
}
func (w *Witness) BindBroker(config *infraattach.BrokerInfra) error {
	if w == nil || w.ingress == nil || config == nil {
		return c.ErrIncomplete
	}
	config.Observe = w.Observe
	return nil
}

func (w *Witness) Observe(nc *nats.Conn, raw net.Conn, participant c.ID, generation uint64, d c.Deadline) (c.Attachment, error) {
	return observe(w, nc, raw, participant, generation, d)
}
