package tunnel

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §4.1 / T16
func TestRegisterReaderRetainsYamuxPrefix(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRegisterBuffer(t)
	}
	testharness.AssertNoGoroutineLeak(t, "REGISTER buffered yamux", goroutines)
	testharness.AssertNoFDLeak(t, "REGISTER buffered yamux", fds, 0)
}

func exerciseRegisterBuffer(t *testing.T) {
	t.Helper()
	var emitted bytes.Buffer
	if err := writeRegisterOK(&emitted); err != nil || emitted.String() != "OK\n" {
		t.Fatalf("server reply must be exactly OK LF: %q / %v", emitted.String(), err)
	}
	if err := writeRegisterOK(registerShortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short OK write accepted: %v", err)
	}
	for _, reply := range []string{"OK\n", "OK\r\n", "OK\r\r\n"} {
		if err := readRegisterReply(bufio.NewReader(bytes.NewBufferString(reply))); err != nil {
			t.Fatalf("historical OK acceptance narrowed for %q: %v", reply, err)
		}
	}
	for _, reply := range []string{"OK flow-v1\n", "OK \n", " OK\n", "OK", "DENY token_unknown_or_revoked\n"} {
		if err := readRegisterReply(bufio.NewReader(bytes.NewBufferString(reply))); err == nil {
			t.Fatalf("reply acceptance widened for %q", reply)
		}
	}
	var denial *DenyError
	if err := readRegisterReply(bufio.NewReader(bytes.NewBufferString("DENY try_again\n"))); !errors.As(err, &denial) || denial.Reason != "try_again" {
		t.Fatalf("DENY classification lost: %v", err)
	}
	// Both sides must transfer their readers. REGISTER and OK are each sent
	// with real yamux SYN/data in one TLS Write (one application record).
	for _, side := range []string{"client", "server"} {
		t.Run(side, func(t *testing.T) {
			exerciseRegisterTLSPrefix(t, side, emitted.Bytes())
		})
	}
}

func exerciseRegisterTLSPrefix(t *testing.T, side string, okReply []byte) {
	t.Helper()
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		raw, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- raw
	})
	rawClient, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		wg.Wait()
		t.Fatal(err)
	}
	defer func() { _ = rawClient.Close() }()
	var rawServer net.Conn
	select {
	case rawServer = <-accepted:
	case err := <-acceptErr:
		t.Fatal(err)
	}
	wg.Wait()
	defer func() { _ = rawServer.Close() }()
	server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	client := tls.Client(rawClient, clientTLSConfigPinned(proto.CertPins{Current: certFingerprintDER(cert.Certificate[0])}))
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	readerConn, senderConn := client, server
	prefix, streamID := okReply, uint32(2) // yamux server initiates even streams.
	if side == "server" {
		readerConn, senderConn = server, client
		prefix, streamID = []byte("REGISTER lab node 14000 token 0\n"), 1
	}
	payload := []byte("original-yamux-prefix-payload")
	packet := append(append([]byte(nil), prefix...), registerYamuxFrames(streamID, payload)...)
	writeResult := make(chan error, 1)
	wg.Go(func() {
		n, err := senderConn.Write(packet)
		if err == nil && n != len(packet) {
			err = io.ErrShortWrite
		}
		writeResult <- err
		// Consume window ACKs, keeping the original carrier live until teardown.
		_, _ = io.Copy(io.Discard, senderConn)
	})
	defer func() {
		_ = rawClient.Close()
		_ = rawServer.Close()
		wg.Wait()
	}()
	br := bufio.NewReaderSize(readerConn, maxRegisterLine)
	if side == "client" {
		if err := readRegisterReply(br); err != nil {
			t.Fatal(err)
		}
	} else {
		line, err := br.ReadSlice('\n')
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, _, _, valid := parseRegisterLine(string(line))
		if !valid {
			t.Fatalf("six-field REGISTER rejected: %q", line)
		}
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	if br.Buffered() != len(packet)-len(prefix) {
		t.Fatalf("nonempty TLS prefetch required: buffered=%d want=%d", br.Buffered(), len(packet)-len(prefix))
	}
	retained := retainRegisterReader(readerConn, br)
	if retained.LocalAddr().String() != readerConn.LocalAddr().String() || retained.RemoteAddr().String() != readerConn.RemoteAddr().String() {
		t.Fatal("reader adapter lost original transport identity")
	}
	// Deadlines must still target the raw TLS transport through the adapter.
	if err := retained.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.LogOutput = io.Discard
	var session *yamux.Session
	if side == "client" {
		session, err = yamux.Client(retained, cfg)
	} else {
		session, err = yamux.Server(retained, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	// A stream accept timeout bounds the dropped-reader mutation too.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // ctx-root: isolated yamux accept in the test harness.
	defer cancel()
	stream, err := session.AcceptStreamWithContext(ctx)
	if err != nil {
		t.Fatalf("yamux lost prefetched SYN on %s: %v", side, err)
	}
	defer func() { _ = stream.Close() }()
	_ = stream.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(stream, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("yamux lost prefetched DATA on %s: %q / %v", side, got, err)
	}
}

// This independent yamux wire fixture avoids having two yamux implementations
// agree on a prefix that the test never actually prefetched. SYN and DATA are
// separate frames, both following the registration line in the same TLS record.
func registerYamuxFrames(id uint32, payload []byte) []byte {
	frames := make([]byte, 24+len(payload))
	frames[1] = 1                              // WindowUpdate
	binary.BigEndian.PutUint16(frames[2:4], 1) // SYN
	binary.BigEndian.PutUint32(frames[4:8], id)
	binary.BigEndian.PutUint32(frames[8:12], 256<<10)
	binary.BigEndian.PutUint32(frames[16:20], id)
	binary.BigEndian.PutUint32(frames[20:24], uint32(len(payload)))
	copy(frames[24:], payload)
	return frames
}

type registerShortWriter struct{}

func (registerShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
