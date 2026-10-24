package ssproxy

import (
	"bytes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/crypto/chacha20poly1305"
)

type sealLedger struct {
	cipher.AEAD
	key  [32]byte
	seen map[string]bool
	t    *testing.T
}

func (l *sealLedger) Seal(dst, nonce, plain, aad []byte) []byte {
	id := string(l.key[:]) + string(nonce)
	if l.seen[id] {
		l.t.Fatal("actual subkey+nonce was sealed more than once")
	}
	l.seen[id] = true
	return l.AEAD.Seal(dst, nonce, plain, aad)
}
func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ln.AcceptTCP()
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	return client, peer
}

// Real kernel short writes, independently derived AEAD and a ledger keyed only
// by actual subkey+nonce. No product reader is used to validate ciphertext.
// origin: hot-update S5 plan §6.2 / T18
func TestSSNonceContinuityAndNoReseal(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSNonceCut(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS nonce cuts", beforeG)
	testharness.AssertNoFDLeak(t, "SS nonce cuts", beforeFD, 0)
}
func exerciseSSNonceCut(t *testing.T) {
	t.Helper()
	client, peer := tcpPair(t)
	defer func() { _ = client.Close(); _ = peer.Close() }()
	if err := client.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	s, a := parserFixture(t, []Key{{KeyID: "wire", Secret: "independent-cipher-password"}})
	seen := make(map[string]bool)
	rxSalt := bytes.Repeat([]byte{33}, 32)
	request := ledgerReferenceFrames(t, "independent-cipher-password", rxSalt, seen, []byte{1, 127, 0, 0, 1, 1, 187})
	if err := feedParser(s, a, request); err != nil {
		t.Fatal(err)
	}
	p := a.state
	a.client = &kernelCipherSocket{TCPConn: client}
	txSalt := bytes.Repeat([]byte{44}, 32)
	s.imageState.random = bytes.NewReader(txSalt)
	s.mu.Lock()
	salt, err := responseSaltLocked(s, a.material)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	sk, err := sessionSubkey(a.material.master, salt)
	if err != nil {
		t.Fatal(err)
	}
	base, err := chacha20poly1305.New(sk)
	if err != nil {
		t.Fatal(err)
	}
	a.tx = &sealLedger{AEAD: base, key: sha256.Sum256(sk), seen: seen, t: t}
	p.putBytes("tx_subkey", sk)
	p.putBytes("tx_salt", salt)
	p.put("tx_salt_reserved", 1)
	p.put("tx_salt_wire_cursor", 32)
	var expectedWire, expectedPlain []byte
	short := false
	frames := 0
	for frames < 256 && !short {
		plain := bytes.Repeat([]byte{byte(frames + 1)}, maxChunk)
		p.putBytes("tx_plain_pending", plain)
		p.put("tx_length_sealed", 0)
		p.put("tx_payload_sealed", 0)
		p.put("tx_cipher_write_cursor", 0)
		if err := sealActorPending(a); err != nil {
			t.Fatal(err)
		}
		expectedWire = append(expectedWire, p.bytes("tx_cipher_pending")...)
		expectedPlain = append(expectedPlain, plain...)
		frames++
		s.mu.Lock()
		err = actorTransmitLocked(s, a)
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		n := p.number("tx_cipher_write_cursor")
		short = n > 0 && n < uint64(len(p.bytes("tx_cipher_pending")))
		if n == 0 {
			// A pending ACK may temporarily fill the socket. Retry the same sealed
			// frame; the transport syscall, not this fixture, chooses the short count.
			until := time.Now().Add(time.Second)
			for n == 0 && time.Now().Before(until) {
				s.mu.Lock()
				err = actorTransmitLocked(s, a)
				s.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				n = p.number("tx_cipher_write_cursor")
				runtime.Gosched()
			}
			short = n > 0 && n < uint64(len(p.bytes("tx_cipher_pending")))
			if n == 0 {
				t.Fatal("socket stalled without the required positive partial write")
			}
		}
	}
	if !short {
		t.Fatal("kernel never produced 0 < n < attempted ciphertext length")
	}
	t.Logf("real partial ciphertext write: applied=%d attempted=%d sealed_frames=%d", p.number("tx_cipher_write_cursor"), len(p.bytes("tx_cipher_pending")), frames)
	beforeCursor, beforeNonce := p.number("tx_cipher_write_cursor"), append([]byte(nil), p.bytes("tx_nonce")...)
	copyRecord, err := validatePrivateRecord(p.clone())
	if err != nil {
		t.Fatal(err)
	}
	next, err := restoreActorRecord(copyRecord, s.imageState)
	if err != nil {
		t.Fatal(err)
	}
	next.client = &kernelCipherSocket{TCPConn: client}
	next.tx = &sealLedger{AEAD: next.tx, key: sha256.Sum256(sk), seen: seen, t: t}
	next.state.put("authority_phase", uint64(AuthorityPhaseAuthorized))
	if next.state.number("tx_cipher_write_cursor") != beforeCursor || !bytes.Equal(next.state.bytes("tx_nonce"), beforeNonce) {
		t.Fatal("cut lost write cursor or nonce")
	}
	// Retrying the seal decision is idempotent even if the caller re-enters it
	// after restore; only the pending suffix may be written.
	if err := sealActorPending(next); err != nil {
		t.Fatal(err)
	}
	received := make(chan []byte, 1)
	readError := make(chan error, 1)
	go func() {
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		b := make([]byte, len(expectedWire))
		_, err := io.ReadFull(peer, b)
		received <- b
		readError <- err
	}()
	for next.state.number("tx_cipher_write_cursor") < uint64(len(next.state.bytes("tx_cipher_pending"))) {
		s.mu.Lock()
		err = actorTransmitLocked(s, next)
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	wire := <-received
	if err := <-readError; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, expectedWire) {
		t.Fatal("ciphertext changed or duplicated across partial-write cut")
	}
	reference := referenceCipher(t, "independent-cipher-password", txSalt)
	var plain []byte
	var nonce [12]byte
	var counter uint64
	for len(wire) > 0 {
		binary.LittleEndian.PutUint64(nonce[:], counter)
		length, err := reference.Open(nil, nonce[:], wire[:18], nil)
		if err != nil {
			t.Fatal(err)
		}
		counter++
		wire = wire[18:]
		n := int(binary.BigEndian.Uint16(length))
		binary.LittleEndian.PutUint64(nonce[:], counter)
		b, err := reference.Open(nil, nonce[:], wire[:n+16], nil)
		if err != nil {
			t.Fatal(err)
		}
		counter++
		wire = wire[n+16:]
		plain = append(plain, b...)
	}
	if !bytes.Equal(plain, expectedPlain) || len(seen) != 2*frames+2 {
		t.Fatal("independent cipher trace or primitive ledger mismatch")
	}
	exerciseAliasNonceDomain(t, s, seen)
	// The all-ones nonce is usable exactly once; even identical plaintext must
	// fail on the next operation instead of wrapping back to zero.
	nonceBytes := bytes.Repeat([]byte{255}, 12)
	exhausted := false
	if _, err := sealChunk(a.tx, nonceBytes, &exhausted, []byte("last")); err != nil {
		t.Fatal(err)
	}
	if !exhausted {
		t.Fatal("last nonce did not latch exhaustion")
	}
	if _, err := sealChunk(a.tx, nonceBytes, &exhausted, []byte("last")); !errors.Is(err, errNonceExhausted) {
		t.Fatal("nonce counter wrapped")
	}
}

// Both client reference Seal calls and every server actor share one ledger.
// KeyID, direction and restored wrapper identity cannot create nonce domains.
// origin: hot-update S5 plan §6.2 / T18
func ledgerReferenceFrames(t *testing.T, password string, salt []byte, seen map[string]bool, chunks ...[]byte) []byte {
	t.Helper()
	a := &sealLedger{AEAD: referenceCipher(t, password, salt), key: sha256.Sum256(referenceSubkey(t, password, salt)), seen: seen, t: t}
	return referenceFramesWithCipher(a, salt, chunks...)
}

func exerciseAliasNonceDomain(t *testing.T, s *Server, seen map[string]bool) {
	t.Helper()
	const password = "independent-cipher-password"
	if err := s.SetKeys([]Key{{KeyID: "wire", Secret: password}, {KeyID: "alias", Secret: password}}); err != nil {
		t.Fatal(err)
	}
	a, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	s.imageState.actors[stateID(a.state.bytes("id"))] = a
	before := len(seen)
	rxSalt := bytes.Repeat([]byte{55}, 32)
	request := ledgerReferenceFrames(t, password, rxSalt, seen, []byte{1, 127, 0, 0, 1, 1, 187})
	if err := feedParser(s, a, request); err != nil {
		t.Fatal(err)
	}
	if string(a.state.bytes("authorized_key_id")) != "alias" {
		t.Fatal("second actor did not bind the alias")
	}
	// Collision candidates cover another actor's inbound and outbound salts,
	// then this actor's inbound salt. Only the fourth candidate may be sealed.
	var candidates []byte
	for _, value := range []byte{33, 44, 55, 66} {
		candidates = append(candidates, bytes.Repeat([]byte{value}, 32)...)
	}
	s.imageState.random = bytes.NewReader(candidates)
	s.mu.Lock()
	salt, err := responseSaltLocked(s, a.material)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	sk, err := sessionSubkey(a.material.master, salt)
	if err != nil {
		t.Fatal(err)
	}
	base, err := chacha20poly1305.New(sk)
	if err != nil {
		t.Fatal(err)
	}
	a.tx = &sealLedger{AEAD: base, key: sha256.Sum256(sk), seen: seen, t: t}
	a.state.putBytes("tx_plain_pending", []byte("alias-response"))
	if err := sealActorPending(a); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(salt, bytes.Repeat([]byte{66}, 32)) || len(seen) != before+4 {
		t.Fatal("cross-actor or direction salt domain collision")
	}
}

// Single real TCP send syscall, matching the raw owner contract. net.TCPConn's
// Write hides successful short syscalls by polling/retrying them internally.
type kernelCipherSocket struct{ *net.TCPConn }

func (c *kernelCipherSocket) Write(b []byte) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var n int
	var writeErr error
	if err = raw.Control(func(fd uintptr) { n, writeErr = syscall.Write(int(fd), b) }); err != nil {
		return 0, err
	}
	if writeErr == syscall.EAGAIN || writeErr == syscall.EWOULDBLOCK {
		return 0, os.ErrDeadlineExceeded
	}
	return n, writeErr
}
