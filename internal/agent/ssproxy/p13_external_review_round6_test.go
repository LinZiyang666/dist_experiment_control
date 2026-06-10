package ssproxy

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestExternalReviewRejectsReplayedClientSalt(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	accepted := make(chan struct{}, 2)
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()

	srv := New(nil)
	port, err := srv.Start(context.Background(), 0, []Key{{KeyID: "alice", Secret: "alice-psk"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	replay := encryptedTargetHandshake(t, "alice-psk", target.Addr().String(), bytes.Repeat([]byte{0x42}, saltSize))
	send := func() {
		conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(replay); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		_ = conn.Close()
	}

	send()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("precondition: first handshake did not reach target")
	}

	send()
	select {
	case <-accepted:
		t.Fatal("server accepted a replayed AEAD salt and repeated the target connection")
	case <-time.After(300 * time.Millisecond):
	}
}

func encryptedTargetHandshake(t *testing.T, password, target string, salt []byte) []byte {
	t.Helper()
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	header := []byte{0x03, byte(len(host))}
	header = append(header, host...)
	header = append(header, byte(port>>8), byte(port))

	subkey, err := sessionSubkey(evpBytesToKey(password, keySize), salt)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	wire.Write(salt)
	if _, err := newAEADWriter(&wire, aead).Write(header); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}
