package tunnel

import (
	"bufio"
	"io"
	"net"

	"github.com/LinZiyang666/tether/internal/tunnel/transport"
)

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
type registerReaderConn = transport.RegisterReaderConn

func retainRegisterReader(conn net.Conn, reader *bufio.Reader) net.Conn {
	return transport.RetainRegisterReader(conn, reader)
}
func readRegisterReply(reader *bufio.Reader) error { return transport.ReadRegisterReply(reader) }
func writeRegisterOK(writer io.Writer) error       { return transport.WriteRegisterOK(writer) }

func observeCarrierPrefix(c *transport.RegisterReaderConn, p []byte, write bool) {
	transport.ObserveCarrierPrefix(c, p, write)
}
