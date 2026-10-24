//go:build linux

package infraroot

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"net"
	"syscall"
)

// origin: hot-update S9 plan §10.2 / T83 — only the actual TLS socket can attest ingress ownership.
func installedClientSocket(conn net.Conn, self c.Process, d c.Deadline) (c.Socket, error) {
	rawConn, ok := conn.(syscall.Conn)
	if !ok {
		return c.Socket{}, c.ErrIncomplete
	}
	raw, err := rawConn.SyscallConn()
	if err != nil {
		return c.Socket{}, err
	}
	var client c.Socket
	var readErr error
	if err := raw.Control(func(fd uintptr) { client, readErr = infrahost.ReadSocket(int(fd), self, d) }); err != nil || readErr != nil {
		return c.Socket{}, c.ErrIncomplete
	}
	return client, nil
}
