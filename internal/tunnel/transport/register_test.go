package transport

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// REGISTER may read ahead into the first carrier frame. The retained reader
// must deliver that prefix and still close the original socket, or each
// reconnect leaks the predecessor's FD even after its protocol owner stops.
// origin: hot-update S6 plan §9.3 / T54 / GATES-S6-C
func TestRegisterReaderRetainsSocketOwnership(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRegisterReaderSocket(t)
	}
	testharness.AssertNoGoroutineLeak(t, "REGISTER reader socket x5", beforeG)
	testharness.AssertNoFDLeak(t, "REGISTER reader socket x5", beforeFD, 0)
}

func exerciseRegisterReaderSocket(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer closeRegisterTestResource(t, listener)
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRegisterTestResource(t, client)
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer closeRegisterTestResource(t, peer)
	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Pre-buffer the reply and frame so the test cannot accidentally pass by
	// reading the frame straight from the socket instead of the retained reader.
	if _, err := io.WriteString(peer, "OK\ncarrier-frame"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	if _, err := reader.Peek(len("OK\ncarrier-frame")); err != nil {
		t.Fatal(err)
	}
	if err := ReadRegisterReply(reader); err != nil {
		t.Fatal(err)
	}
	retained := RetainRegisterReader(client, reader)
	prefix := make([]byte, len("carrier-frame"))
	if _, err := io.ReadFull(retained, prefix); err != nil || string(prefix) != "carrier-frame" {
		t.Fatalf("retained carrier prefix = %q, err = %v", prefix, err)
	}
	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("retained close left original socket open: n=%d err=%v", n, err)
	}
}

// TestDenyIsTransient pins the terminal-vs-transient taxonomy.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func TestDenyIsTransient(t *testing.T) {
	cases := []struct {
		reason    string
		transient bool
	}{
		{"public_port_bind_failed", true},
		{"try_again", true},
		{"token_unknown_or_revoked", false},
		{"session_closed", false},
		{"malformed_register", false},
		{"", false},
		{"anything_unknown", false},
	}
	for _, c := range cases {
		if got := DenyIsTransient(c.reason); got != c.transient {
			t.Errorf("denyIsTransient(%q)=%v, want %v", c.reason, got, c.transient)
		}
	}
}

// TestDenyErrorIsRegisterDenied pins errors.Is wrapping.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func TestDenyErrorIsRegisterDenied(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &DenyError{Reason: "token_unknown_or_revoked"})
	if !errors.Is(err, ErrRegisterDenied) {
		t.Fatal("DenyError must satisfy errors.Is(ErrRegisterDenied)")
	}
	var de *DenyError
	if !errors.As(err, &de) || de.Reason != "token_unknown_or_revoked" {
		t.Fatalf("errors.As failed to recover reason; de=%v", de)
	}
}

// RetainRegisterReader transfers the same socket's close responsibility, so
// deferred cleanup may see net.ErrClosed after the ownership assertion passes.
// origin: hot-update S6 plan §9.3 / T54
func closeRegisterTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close REGISTER test resource: %v", err)
	}
}
