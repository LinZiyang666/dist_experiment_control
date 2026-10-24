//go:build !linux

package infraroot

import (
	"net"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §10.2 / T83 — no synthetic socket identity can attest ingress ownership.
func installedClientSocket(net.Conn, c.Process, c.Deadline) (c.Socket, error) {
	return c.Socket{}, c.ErrUnsupported
}
