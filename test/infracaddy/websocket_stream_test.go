//go:build infracaddy_integration && linux && amd64

package infracaddy_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// A stream reader must expose available NATS bytes before the entire WebSocket
// frame arrives. Waiting for its suffix can deadlock the peer's backpressure.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F5
func TestCaddyWebsocketStreamDeliversAvailablePrefix(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWebsocketPrefix(t)
		exerciseWebsocketControlBound(t)
	}
	testharness.AssertNoGoroutineLeak(t, "WebSocket prefix", g)
	testharness.AssertNoFDLeak(t, "WebSocket prefix", fd, 0)
}

func exerciseWebsocketControlBound(t *testing.T) {
	t.Helper()
	writer, reader := net.Pipe()
	done := make(chan struct{})
	defer func() { _ = writer.Close(); _ = reader.Close(); <-done }()
	frame := make([]byte, 4+126)
	frame[0], frame[1] = 0x89, 126
	binary.BigEndian.PutUint16(frame[2:4], 126)
	go func() { defer close(done); defer func() { _ = writer.Close() }(); _, _ = writer.Write(frame) }()
	if err := reader.SetDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	stream := &websocketStream{conn: reader, reader: bufio.NewReader(reader)}
	var dst [1]byte
	if _, err := stream.Read(dst[:]); err == nil || err.Error() != "oversized WebSocket control frame" {
		t.Fatalf("oversized control payload reached the stream: %v", err)
	}
}

func exerciseWebsocketPrefix(t *testing.T) {
	t.Helper()
	writer, reader := net.Pipe()
	done := make(chan struct{})
	defer func() { _ = writer.Close(); _ = reader.Close(); <-done }()
	prefix := []byte("MSG infra.block 1 4096\r\n")
	frame := make([]byte, 4)
	frame[0], frame[1] = 0x82, 126
	binary.BigEndian.PutUint16(frame[2:], 8192)
	go func() { defer close(done); _, _ = writer.Write(append(frame, prefix...)) }()
	if err := reader.SetReadDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	stream := &websocketStream{conn: reader, reader: bufio.NewReader(reader)}
	got := make([]byte, len(prefix))
	if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got, prefix) {
		t.Fatalf("available frame prefix was withheld: %q: %v", got, err)
	}
}

type websocketStream struct {
	conn      net.Conn
	reader    *bufio.Reader
	remaining uint64
}

func (stream *websocketStream) Read(dst []byte) (int, error) {
	for stream.remaining == 0 {
		var header [2]byte
		if _, err := io.ReadFull(stream.reader, header[:]); err != nil {
			return 0, err
		}
		if header[1]&0x80 != 0 || header[0]&0x70 != 0 {
			return 0, fmt.Errorf("unexpected server WebSocket flags")
		}
		length := uint64(header[1] & 0x7f)
		var extended [8]byte
		switch length {
		case 126:
			if _, err := io.ReadFull(stream.reader, extended[:2]); err != nil {
				return 0, err
			}
			length = uint64(binary.BigEndian.Uint16(extended[:2]))
		case 127:
			if _, err := io.ReadFull(stream.reader, extended[:]); err != nil {
				return 0, err
			}
			length = binary.BigEndian.Uint64(extended[:])
		}
		if length > 16<<20 {
			return 0, fmt.Errorf("fixture WebSocket frame exceeded bound")
		}
		switch header[0] & 0x0f {
		case 0, 1, 2:
			stream.remaining = length
		case 8:
			return 0, io.EOF
		case 9, 10:
			if length > 125 {
				return 0, fmt.Errorf("oversized WebSocket control frame")
			}
			body := make([]byte, int(length))
			if _, err := io.ReadFull(stream.reader, body); err != nil {
				return 0, err
			}
			if header[0]&0x0f == 9 {
				if err := writeWebsocketFrame(stream.conn, 10, body); err != nil {
					return 0, err
				}
			}
		default:
			return 0, fmt.Errorf("unexpected server WebSocket opcode")
		}
	}
	// Deliver available bytes without waiting for a complete potentially large
	// frame; otherwise NATS headers are hidden behind the blocked body itself.
	n, err := stream.reader.Read(dst[:min(uint64(len(dst)), stream.remaining)])
	stream.remaining -= uint64(n)
	return n, err
}
