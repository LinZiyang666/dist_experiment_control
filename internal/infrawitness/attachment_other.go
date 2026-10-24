//go:build !linux

package infrawitness

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
	"net"
)

func observe(*Witness, *nats.Conn, net.Conn, c.ID, uint64, c.Deadline) (c.Attachment, error) {
	return c.Attachment{}, c.ErrUnsupported
}
