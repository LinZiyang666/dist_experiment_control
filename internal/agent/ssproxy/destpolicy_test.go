package ssproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Round-6 F12: with DenyPrivateDestinations the server must refuse to dial a
// loopback/private target after decrypting the SOCKS address (internet-egress
// only). A permissive server (default) still reaches it — proving the policy is
// what blocks, not a transport failure.
func TestDestinationPolicyBlocksPrivateTargets(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	reached := make(chan struct{}, 2)
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			reached <- struct{}{}
			_ = conn.Close()
		}
	}()

	dialOnce := func(srv *Server) {
		port, err := srv.Start(0, []Key{{KeyID: "alice", Secret: "alice-psk"}})
		if err != nil {
			t.Fatal(err)
		}
		hs := encryptedTargetHandshake(t, "alice-psk", target.Addr().String(), bytes.Repeat([]byte{0x11}, saltSize))
		c, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write(hs)
		_ = c.Close()
	}

	// Permissive (default): loopback target IS reached. Keep the server alive
	// until after the check so its relay goroutine isn't killed mid-dial.
	allowSrv := New(nil)
	dialOnce(allowSrv)
	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		allowSrv.Stop()
		t.Fatal("permissive server did not reach the loopback target")
	}
	allowSrv.Stop()

	// Deny-private: the SAME handshake must be blocked before dialing.
	denied := New(nil)
	denied.DenyPrivateDestinations()
	dialOnce(denied)
	select {
	case <-reached:
		t.Fatal("deny-private server dialed a loopback target")
	case <-time.After(400 * time.Millisecond):
	}
	denied.Stop()
}

func TestExternalReviewDestinationPolicyPinsValidatedDNSResult(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	reached := make(chan struct{}, 1)
	go func() {
		conn, err := target.Accept()
		if err == nil {
			reached <- struct{}{}
			_ = conn.Close()
		}
	}()

	dns, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dns.Close() }()
	var aQueries atomic.Int32
	go serveRebindingDNS(dns, &aQueries)

	oldResolver := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", dns.LocalAddr().String())
		},
	}
	defer func() { net.DefaultResolver = oldResolver }()

	srv := New(nil)
	srv.DenyPrivateDestinations()
	port, err := srv.Start(0, []Key{{KeyID: "alice", Secret: "alice-psk"}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	_, targetPort, err := net.SplitHostPort(target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	hs := encryptedTargetHandshake(
		t,
		"alice-psk",
		net.JoinHostPort("rebind.test", targetPort),
		bytes.Repeat([]byte{0x22}, saltSize),
	)
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write(hs)
	_ = conn.Close()

	select {
	case <-reached:
		t.Fatal("destination policy validated a public DNS answer but dialed a rebound loopback answer")
	case <-time.After(2 * time.Second):
		if aQueries.Load() < 2 {
			t.Fatalf("test did not exercise separate policy and dial DNS lookups: A queries=%d", aQueries.Load())
		}
	}
}

// origin: proxy-lifecycle external review F1
// Stop is the server-lifetime boundary, so it must cancel a destination lookup that was
// started by one of its handlers. Closing the accepted socket cannot interrupt net.LookupIP:
// without an operation-scoped cancellation signal, Stop waits for the resolver's own timeout
// while the agent holds proxyRuntime.mu.
func TestStopCancelsInFlightDestinationResolution(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	oldResolver := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
				return nil, context.Canceled
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	defer func() { net.DefaultResolver = oldResolver }()

	srv := New(nil)
	srv.DenyPrivateDestinations()
	port, err := srv.Start(0, []Key{{KeyID: "alice", Secret: "alice-psk"}})
	if err != nil {
		t.Fatal(err)
	}

	hs := encryptedTargetHandshake(
		t,
		"alice-psk",
		net.JoinHostPort("resolver-stall.invalid", "443"),
		bytes.Repeat([]byte{0x33}, saltSize),
	)
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(hs); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		srv.Stop()
		t.Fatal("destination lookup did not start; test never reached the cancellation boundary")
	}

	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()
	select {
	case <-done:
		close(release)
	case <-time.After(500 * time.Millisecond):
		close(release)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop remained stuck after the resolver fixture was released")
		}
		t.Fatal("Stop waited for an in-flight DNS lookup instead of cancelling it; in the agent this " +
			"wait occurs under proxyRuntime.mu and can suppress heartbeats until the resolver times out")
	}
}

func TestExternalReviewDestinationPolicyBlocksNonPublicRanges(t *testing.T) {
	srv := New(nil)
	srv.DenyPrivateDestinations()

	for _, host := range []string{
		"100.64.0.1",             // RFC 6598 shared address space.
		"100.100.100.200",        // Cloud metadata endpoint within shared address space.
		"198.18.0.1",             // RFC 2544 benchmarking range.
		"64:ff9b::a9fe:a9fe",     // NAT64 encoding of 169.254.169.254 metadata.
		"100:0:0:1::1",           // IANA dummy IPv6 prefix.
		"2001:2::1",              // RFC 5180 benchmarking.
		"2002:7f00:1::1",         // 6to4 encoding of 127.0.0.1.
		"3fff::1",                // RFC 9637 documentation.
		"5f00::1",                // SRv6 SID space.
		"::ffff:169.254.169.254", // IPv4-mapped metadata address.
	} {
		if srv.destAllowed(context.Background(), host) {
			t.Errorf("internet-only destination policy allowed non-public address %s", host)
		}
	}
}

func serveRebindingDNS(conn net.PacketConn, aQueries *atomic.Int32) {
	buf := make([]byte, 1500)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			return
		}
		query := append([]byte(nil), buf[:n]...)
		response := rebindingDNSResponse(query, aQueries)
		if response != nil {
			_, _ = conn.WriteTo(response, addr)
		}
	}
}

func rebindingDNSResponse(query []byte, aQueries *atomic.Int32) []byte {
	if len(query) < 17 {
		return nil
	}
	i := 12
	for i < len(query) {
		size := int(query[i])
		i++
		if size == 0 {
			break
		}
		i += size
	}
	if i+4 > len(query) {
		return nil
	}
	qtype := binary.BigEndian.Uint16(query[i : i+2])
	questionEnd := i + 4
	answer := qtype == 1

	// The 12-byte DNS header is a fixed-size struct, so it is built as one rather than as a
	// `make([]byte, 12)` that is then appended to. Both produce identical bytes; this shape says "12
	// bytes of header, then a body" instead of "a 12-long slice I am about to grow", which is the
	// distinction makezero exists to draw (a `make([]byte, n)` followed by append is usually a
	// misspelt `make([]byte, 0, n)` and silently ships n leading zero bytes).
	var hdr [12]byte
	copy(hdr[0:2], query[0:2])
	binary.BigEndian.PutUint16(hdr[2:4], 0x8180)
	binary.BigEndian.PutUint16(hdr[4:6], 1)
	if answer {
		binary.BigEndian.PutUint16(hdr[6:8], 1)
	}
	resp := append([]byte(nil), hdr[:]...)
	resp = append(resp, query[12:questionEnd]...)
	if !answer {
		return resp
	}

	ip := net.IPv4(93, 184, 216, 34)
	if aQueries.Add(1) > 1 {
		ip = net.IPv4(127, 0, 0, 1)
	}
	resp = append(resp,
		0xc0, 0x0c, // compressed name pointer
		0x00, 0x01, // A
		0x00, 0x01, // IN
		0x00, 0x00, 0x00, 0x00, // TTL
		0x00, 0x04, // RDLENGTH
	)
	resp = append(resp, ip.To4()...)
	return resp
}
