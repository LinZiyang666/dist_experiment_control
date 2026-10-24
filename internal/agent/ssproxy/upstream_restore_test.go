//go:build linux

package ssproxy

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// This peer uses the independent EVP/HKDF/frame oracle, never the product's
// reader/writer. The same cipher and nonce stream survives every cut.
type referencePeer struct {
	conn             *rawsocket.Socket
	password         string
	tx, rx           cipher.AEAD
	txCount, rxCount uint64
}

func newReferencePeer(t *testing.T, s *Server, port int, password string, target netip.AddrPort) *referencePeer {
	t.Helper()
	c, err := rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), time.Now().Add(time.Second), nil, func(tuple rawsocket.Tuple) error { return s.ExpectFlow([16]byte{1}, tuple) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.CompleteConnect(time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	p := &referencePeer{conn: c, password: password, tx: referenceCipher(t, password, salt)}
	ip := target.Addr().As4()
	header := append([]byte{1}, ip[:]...)
	header = append(header, byte(target.Port()>>8), byte(target.Port()))
	frame := append([]byte(nil), salt...)
	p.write(t, append(frame, p.frame(header)...))
	return p
}
func (p *referencePeer) frame(plain []byte) []byte {
	var nonce [12]byte
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(plain)))
	binary.LittleEndian.PutUint64(nonce[:], p.txCount)
	wire := p.tx.Seal(nil, nonce[:], length[:], nil)
	p.txCount++
	binary.LittleEndian.PutUint64(nonce[:], p.txCount)
	wire = p.tx.Seal(wire, nonce[:], plain, nil)
	p.txCount++
	return wire
}
func (p *referencePeer) write(t *testing.T, wire []byte) {
	t.Helper()
	_ = p.conn.SetWriteDeadline(time.Now().Add(time.Second))
	for len(wire) > 0 {
		n, err := p.conn.Write(wire)
		if err != nil {
			t.Fatal(err)
		}
		wire = wire[n:]
	}
}
func (p *referencePeer) echo(t *testing.T, plain []byte) {
	t.Helper()
	p.write(t, p.frame(plain))
	_ = p.conn.SetReadDeadline(time.Now().Add(time.Second))
	if p.rx == nil {
		salt := make([]byte, 32)
		if _, err := io.ReadFull(p.conn, salt); err != nil {
			t.Fatal(err)
		}
		p.rx = referenceCipher(t, p.password, salt)
	}
	var got []byte
	for len(got) < len(plain) {
		var nonce [12]byte
		length := make([]byte, 18)
		if _, err := io.ReadFull(p.conn, length); err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(nonce[:], p.rxCount)
		clear, err := p.rx.Open(nil, nonce[:], length, nil)
		if err != nil {
			t.Fatal(err)
		}
		p.rxCount++
		payload := make([]byte, int(binary.BigEndian.Uint16(clear))+16)
		if _, err := io.ReadFull(p.conn, payload); err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(nonce[:], p.rxCount)
		clear, err = p.rx.Open(nil, nonce[:], payload, nil)
		if err != nil {
			t.Fatal(err)
		}
		p.rxCount++
		got = append(got, clear...)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("original independent cipher stream lost or duplicated bytes")
	}
}

// origin: hot-update S5 internal review rv-ss-1
func TestSSOriginalRawUpstreamTupleRestoresOnEveryRoute(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSUpstreamRestore(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS upstream restore", g0)
	testharness.AssertNoFDLeak(t, "SS upstream restore", fd0, 0)
}
func exerciseSSUpstreamRestore(t *testing.T) {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var routed netip.Addr
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && ip.IP.To4() != nil && !ip.IP.IsLoopback() {
			routed = netip.AddrFrom4([4]byte(ip.IP.To4()))
			break
		}
	}
	if !routed.IsValid() {
		t.Fatal("non-loopback IPv4 route required")
	}
	for _, ip := range []netip.Addr{netip.MustParseAddr("127.0.0.1"), routed} {
		func() {
			target := newSSTarget(t)
			defer target.close()
			// The same host's routed address exercises kernel source selection without
			// external network dependencies. The echo listener admits both routes.
			_ = target.listener.Close()
			target.wg.Wait()
			ln, e := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
			if e != nil {
				t.Fatal(e)
			}
			target.listener = ln
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, e := ln.Accept()
				if e != nil {
					return
				}
				defer func(closeFunc func() error) { _ = closeFunc() }(c.Close)
				target.accepts.Add(1)
				target.mu.Lock()
				target.conns = append(target.conns, c)
				target.mu.Unlock()
				b := make([]byte, 4096)
				for {
					n, e := c.Read(b)
					if n > 0 {
						_, _ = c.Write(b[:n])
					}
					if e != nil {
						return
					}
				}
			}()
			defer func() { target.close(); <-done }()
			keys := []Key{{KeyID: "bob", Secret: "routed-private-material"}}
			s := New(nil)
			defer s.Stop()
			if e := s.SetFlowRevoke(func([16]byte, string) {}); e != nil {
				t.Fatal(e)
			}
			port, e := s.Start(0, keys)
			if e != nil {
				t.Fatal(e)
			}
			peer := newReferencePeer(t, s, port, keys[0].Secret, ln.Addr().(*net.TCPAddr).AddrPort())
			defer func(closeFunc func() error) { _ = closeFunc() }(peer.conn.Close)
			peer.echo(t, []byte("before-original-cut"))
			f, e := s.Freeze(time.Now().Add(time.Second))
			if e != nil {
				t.Fatal(e)
			}
			defer func(closeFunc func() error) { _ = closeFunc() }(f.Discard)
			im, cut, e := f.Export()
			if e != nil {
				t.Fatal(e)
			}
			src := handofftest.DupResources(t, cut.Resources)
			defer src.Close()
			next, salvage, e := Restore(im, src)
			if e != nil {
				for _, r := range salvage {
					for _, file := range r.Files {
						_ = file.Close()
					}
				}
				t.Fatalf("original upstream import failed: %v", e)
			}
			defer next.Stop()
			if e := f.Discard(); e != nil {
				t.Fatal(e)
			}
			if e := next.SetFlowRevoke(func([16]byte, string) {}); e != nil {
				t.Fatal(e)
			}
			if e := next.ActivateRaw(); e != nil {
				t.Fatal(e)
			}
			if e := next.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys}); e != nil {
				t.Fatal(e)
			}
			peer.echo(t, []byte("after-original-cut"))
			if target.accepts.Load() != 1 {
				t.Fatal("restore redialed upstream")
			}
		}()
	}
}

// origin: hot-update S5 internal review rv-ss-3
func TestSSConsumedRefusalSocketImageKeepsItsReceipt(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSRefusalRestore(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS refusal restore", g0)
	testharness.AssertNoFDLeak(t, "SS refusal restore", fd0, 0)
}
func exerciseSSRefusalRestore(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := ln.Addr().(*net.TCPAddr).AddrPort()
	_ = ln.Close()
	keys := []Key{{KeyID: "bob", Secret: "refusal-private-material"}}
	s, a := parserFixture(t, keys)
	defer s.Stop()
	s.stopCtx, s.stopCancel = context.WithCancel(context.Background()) // ctx-root: deterministic connector lifecycle exercise
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatal(err)
	}
	ip := target.Addr().As4()
	header := append([]byte{1}, ip[:]...)
	header = append(header, byte(target.Port()>>8), byte(target.Port()))
	if err := feedParser(s, a, referenceFrames(t, keys[0].Secret, bytes.Repeat([]byte{79}, 32), header)); err != nil {
		t.Fatal(err)
	}
	// Drive the production connector under its real actor guard, then retain the
	// terminal safe point before worker retirement, without synthesizing errno.
	s.mu.Lock()
	for turns := 0; turns < 100 && a.state.number("connect_state") != uint64(rawsocket.ConnectFailed); turns++ {
		_ = connectActorLocked(s, a)
	}
	a.state.put("phase", uint64(SSPhaseTerminal))
	s.mu.Unlock()
	raw, ok := a.upstream.(*rawsocket.Socket)
	if !ok {
		t.Fatal("production path did not create raw upstream")
	}
	phase, consumed, _, _, cause := rawsocket.ConnectReceipt(raw)
	if phase != rawsocket.ConnectFailed || !consumed || !errors.Is(cause, syscall.ECONNREFUSED) {
		t.Fatal("real consumed ECONNREFUSED receipt required")
	}
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(f.Discard)
	im, cut, err := f.Export()
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, cut.Resources)
	defer src.Close()
	next, salvage, err := Restore(im, src)
	if err != nil {
		for _, r := range salvage {
			for _, file := range r.Files {
				_ = file.Close()
			}
		}
		t.Fatalf("consumed refusal image rejected: %v", err)
	}
	defer func() {
		resources, e := next.Detach()
		if e != nil {
			t.Error(e)
			return
		}
		for _, row := range resources {
			for _, file := range row.Files {
				_ = file.Close()
			}
		}
	}()
	for _, actor := range next.imageState.actors {
		restored := actor.upstream.(*rawsocket.Socket)
		phase, consumed, _, _, cause := rawsocket.ConnectReceipt(restored)
		if phase != rawsocket.ConnectFailed || !consumed || !errors.Is(cause, syscall.ECONNREFUSED) {
			t.Fatal("import lost original refusal decision")
		}
	}
}
