//go:build !linux

package tunnel

import (
	"net"
	"strconv"
	"time"
)

// Unsupported image platforms retain the native legacy SS implementation.
// origin: hot-update S5 plan §4.1 / S4 L6
func dialLegacyLocal(_ *Client, port int) (net.Conn, error) {
	return net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 5*time.Second)
}
