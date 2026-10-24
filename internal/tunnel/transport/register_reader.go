package transport

import (
	"bufio"
	"io"
	"net"
	"strings"

	"github.com/LinZiyang666/tether/internal/drillobserve"
)

// registerReaderConn keeps the original transport's close, write and deadline
// behavior, while feeding yamux the bytes already read beyond REGISTER or OK.
// The raw TLS conn stays in the session for TLS state and cancellation; this
// wrapper belongs only to yamux, whose read loop is the reader's sole consumer.
// origin: hot-update S5 plan §4.1 / T16
type RegisterReaderConn struct {
	net.Conn
	Reader                      *bufio.Reader
	Observation                 *drillobserve.Event
	ReadObserved, WriteObserved bool
	ReadPrefix, WritePrefix     []byte
}

func (c *RegisterReaderConn) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	if drillobserve.Hook != nil && c.Observation != nil && !c.ReadObserved && n > 0 {
		ObserveCarrierPrefix(c, p[:n], false)
	}
	return n, err
}

func (c *RegisterReaderConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if drillobserve.Hook != nil && c.Observation != nil && !c.WriteObserved && n > 0 && n <= len(p) {
		ObserveCarrierPrefix(c, p[:n], true)
	}
	return n, err
}

// A real socket may return fewer than a full yamux header. Retain only the
// original bounded prefix across calls, without changing caller read/write IO.
// origin: hot-update S5 plan §4.1 / §9.4 / DRILL-OBSERVER
func ObserveCarrierPrefix(c *RegisterReaderConn, p []byte, write bool) {
	prefix, observed := &c.ReadPrefix, &c.ReadObserved
	if write {
		prefix, observed = &c.WritePrefix, &c.WriteObserved
	}
	*prefix = append(*prefix, p[:min(len(p), 64-len(*prefix))]...)
	if len(*prefix) < 12 {
		return
	}
	*observed = true
	e := *c.Observation
	e.Kind = "carrier-prefix"
	if write {
		e.Values[0] = 1
	}
	e.Data = *prefix
	drillobserve.Hook(e)
	*prefix = nil
}

// Always retain the reader, even when Buffered()==0: it is the same single
// consumer after the protocol transition, and no prefix can become orphaned.
// origin: hot-update S5 plan §4.1 / T16
func RetainRegisterReader(conn net.Conn, reader *bufio.Reader) net.Conn {
	c := &RegisterReaderConn{Conn: conn, Reader: reader}
	if drillobserve.Hook != nil {
		c.Observation = ObservedCarrier(conn)
		if c.Observation != nil {
			drillobserve.Hook(*c.Observation)
		}
	}
	return c
}

// The historical client strips all trailing CR/LF before comparing to OK.
// Preserve that acceptance set; ALPN, never an extra reply field, chooses v1.
// DENY retains its existing typed classification and retry behavior.
// origin: hot-update S5 plan §4.1 / T16
func ReadRegisterReply(reader *bufio.Reader) error {
	reply, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	reply = strings.TrimRight(reply, "\r\n")
	if reply != "OK" {
		reason := strings.TrimSpace(strings.TrimPrefix(reply, "DENY"))
		return &DenyError{Reason: reason}
	}
	return nil
}

// Emission is narrower than acceptance: even a v1 server sends exactly OK LF.
// A short successful write is a failed registration, never permission to attach.
// origin: hot-update S5 plan §4.1 / T16
func WriteRegisterOK(writer io.Writer) error {
	n, err := io.WriteString(writer, "OK\n")
	if err == nil && n != len("OK\n") {
		return io.ErrShortWrite
	}
	return err
}
