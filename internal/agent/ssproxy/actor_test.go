package ssproxy

import (
	"bytes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// referenceSubkey independently derives the standard SS EVP/HKDF key. It does
// not call the product's framing, password derivation or nonce increment code.
func referenceSubkey(t *testing.T, password string, salt []byte) []byte {
	t.Helper()
	var master, previous []byte
	for len(master) < 32 {
		sum := md5.Sum(append(append([]byte(nil), previous...), []byte(password)...))
		previous = sum[:]
		master = append(master, previous...)
	}
	sk := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master[:32], salt, []byte("ss-subkey")), sk); err != nil {
		t.Fatal(err)
	}
	return sk
}
func referenceCipher(t *testing.T, password string, salt []byte) cipher.AEAD {
	t.Helper()
	a, err := chacha20poly1305.New(referenceSubkey(t, password, salt))
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func referenceFrames(t *testing.T, password string, salt []byte, chunks ...[]byte) []byte {
	t.Helper()
	return referenceFramesWithCipher(referenceCipher(t, password, salt), salt, chunks...)
}
func referenceFramesWithCipher(a cipher.AEAD, salt []byte, chunks ...[]byte) []byte {
	wire := append([]byte(nil), salt...)
	var nonce [12]byte
	var count uint64
	for _, plain := range chunks {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(plain)))
		binary.LittleEndian.PutUint64(nonce[:], count)
		wire = append(wire, a.Seal(nil, nonce[:], length[:], nil)...)
		count++
		binary.LittleEndian.PutUint64(nonce[:], count)
		wire = append(wire, a.Seal(nil, nonce[:], plain, nil)...)
		count++
	}
	return wire
}
func parserFixture(t *testing.T, keys []Key) (*Server, *ssActor) {
	t.Helper()
	s := New(nil)
	if err := s.SetKeys(keys); err != nil {
		t.Fatal(err)
	}
	a, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	s.imageState.actors[stateID(a.state.bytes("id"))] = a
	return s, a
}
func feedParser(s *Server, a *ssActor, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, value := range b {
		p := a.state
		buffer, cursor := "rx_salt", "rx_salt_cursor"
		if p.number(cursor) == saltSize {
			buffer, cursor = "rx_length_cipher", "rx_length_cursor"
			if p.flag("rx_length_opened") {
				buffer, cursor = "rx_payload_cipher", "rx_payload_cursor"
			}
		}
		off := p.number(cursor)
		if off >= uint64(len(p.bytes(buffer))) {
			return ErrStateUnrepresentable
		}
		p.bytes(buffer)[off] = value
		p.put(cursor, off+1)
		if err := consumeActorFrameLocked(s, a); err != nil {
			return err
		}
	}
	return nil
}

// Real: every byte boundary in salt, length, payload and a multi-chunk SOCKS
// header. Snapshot/import are the runtime hooks; P9's binary codec is separate.
// No transport, DNS, target socket or success-shaped dependency stub is used.
// origin: hot-update S5 plan §6.1 / T17
func TestSSActorEveryByteCutAndTargetParser(t *testing.T) {
	exerciseSSConnectFailureReceipt(t)
	keys := []Key{{KeyID: "actor", Secret: "every-byte-private-password"}}
	headers := [][]byte{{1, 127, 0, 0, 1, 1, 187}, {4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 187}, {3, 9, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', 1, 187}}
	for _, header := range headers {
		var chunks [][]byte
		for _, b := range header[:len(header)-1] {
			chunks = append(chunks, []byte{b})
		}
		suffix := []byte("first target payload survives the parser")
		chunks = append(chunks, append([]byte{header[len(header)-1]}, suffix...))
		wire := referenceFrames(t, keys[0].Secret, bytes.Repeat([]byte{byte(len(header))}, 32), chunks...)
		for cut := 0; cut <= len(wire); cut++ {
			s, a := parserFixture(t, keys)
			deadline := a.state.number("handshake_deadline_boot_ns")
			if err := feedParser(s, a, wire[:cut]); err != nil {
				t.Fatalf("prefix %d: %v", cut, err)
			}
			frozen, err := s.Freeze(time.Now().Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			image, err := frozen.SnapshotState()
			if err != nil {
				t.Fatal(err)
			}
			next, err := restorePrivateRecords(image.Records)
			if err != nil {
				t.Fatalf("cut %d: %v", cut, err)
			}
			if err := next.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys}); err != nil {
				t.Fatal(err)
			}
			a = next.imageState.actors[stateID(a.state.bytes("id"))]
			if a.state.number("handshake_deadline_boot_ns") != deadline {
				t.Fatalf("cut %d restamped handshake deadline", cut)
			}
			if err := feedParser(next, a, wire[cut:]); err != nil {
				t.Fatalf("suffix %d: %v", cut, err)
			}
			if a.state.number("target_cursor") != uint64(len(header)) || a.state.number("target_parser_phase") != 4 || !bytes.Equal(a.state.bytes("first_payload_suffix"), suffix) || !bytes.Equal(a.state.bytes("rx_clear_pending"), suffix) {
				t.Fatalf("cut %d lost parser cursor or target suffix", cut)
			}
			if binary.LittleEndian.Uint64(a.state.bytes("rx_nonce")) != uint64(2*len(chunks)) {
				t.Fatalf("cut %d reopened an authenticated frame", cut)
			}
			if err := frozen.Discard(); err != nil {
				t.Fatal(err)
			}
			next.Stop()
		}
	}
}

func exerciseSSConnectFailureReceipt(t *testing.T) {
	t.Helper()
	s, a := parserFixture(t, nil)
	recordConnectFailure(a.state, &net.OpError{Op: "connect", Err: syscall.ECONNREFUSED})
	a.state.put("phase", uint64(SSPhaseTerminal))
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	next, err := restorePrivateRecords(cut.Records)
	if err != nil {
		t.Fatal(err)
	}
	p := next.imageState.actors[stateID(a.state.bytes("id"))].state
	if !p.flag("connect_so_error_consumed") || p.number("connect_so_error") != uint64(syscall.ECONNREFUSED) || p.number("connect_state") != uint64(rawsocket.ConnectFailed) {
		t.Fatal("cut lost the consumed connect error receipt")
	}
	if err := f.Discard(); err != nil {
		t.Fatal(err)
	}
	next.Stop()
}

// Trial is intentionally suspended across rotation. The old actor must retain
// its key, while a new actor cannot bind using the old snapshot's material.
// origin: hot-update S5 plan §6.4 / T20
func TestSSNewAuthRechecksActiveMaterial(t *testing.T) {
	old := []Key{{KeyID: "same-id", Secret: "old-private-material"}}
	s, existing := parserFixture(t, old)
	wire := referenceFrames(t, old[0].Secret, bytes.Repeat([]byte{71}, 32), []byte{1, 127, 0, 0, 1, 1, 187})
	if err := feedParser(s, existing, wire); err != nil {
		t.Fatal(err)
	}
	original := existing.material
	trial := s.snapshotKeys()[0]
	next, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	next.state.putBytes("rx_salt", bytes.Repeat([]byte{72}, 32))
	current := []Key{{KeyID: "same-id", Secret: "new-private-material"}}
	if err := s.SetKeys(current); err != nil {
		t.Fatal(err)
	}
	if existing.state.flag("revoked") || existing.material != original {
		t.Fatal("rotation killed or rekeyed an authenticated survivor")
	}
	s.mu.Lock()
	bound := bindTrialLocked(s, next, trial)
	s.mu.Unlock()
	if bound {
		t.Fatal("ID-only bind authorized an obsolete trial material")
	}
	payload := []byte("old authenticated stream remains usable after rotation")
	continued := referenceFrames(t, old[0].Secret, bytes.Repeat([]byte{71}, 32), []byte{1, 127, 0, 0, 1, 1, 187}, payload)
	if err := feedParser(s, existing, continued[len(wire):]); err != nil {
		t.Fatal("old authenticated stream failed after rotation:", err)
	}
	if !bytes.Equal(existing.state.bytes("rx_clear_pending"), payload) || binary.LittleEndian.Uint64(existing.state.bytes("rx_nonce")) != 4 {
		t.Fatal("rotation changed the survivor cipher stream")
	}

	stale, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	if err := feedParser(s, stale, referenceFrames(t, old[0].Secret, bytes.Repeat([]byte{73}, 32), []byte{1, 127, 0, 0, 1, 1, 187})); err == nil {
		t.Fatal("new handshake accepted old PSK")
	}
	fresh, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	if err := feedParser(s, fresh, referenceFrames(t, current[0].Secret, bytes.Repeat([]byte{74}, 32), []byte{1, 127, 0, 0, 1, 1, 187})); err != nil {
		t.Fatal("current PSK rejected:", err)
	}
}

// A failed material replacement is still an irrevocable deletion of missing
// IDs. Applied generation is advanced only after the entire set is installed.
// origin: hot-update S5 plan §8 / Q3
func TestSSCapacityFailureStillRevokesRemovedKeys(t *testing.T) {
	keys := []Key{{KeyID: "alice", Secret: "alice"}, {KeyID: "bob", Secret: "bob"}}
	s, a := parserFixture(t, keys)
	if err := feedParser(s, a, referenceFrames(t, "alice", bytes.Repeat([]byte{1}, 32), []byte{1, 127, 0, 0, 1, 1, 187})); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys}); err != nil {
		t.Fatal(err)
	}
	for i := 2; i < MaxMaterialHistory; i++ {
		k := append(append([]Key(nil), keys...), Key{KeyID: "churn", Secret: string([]byte{'x', byte(i)})})
		if err := s.SetKeys(k); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.imageState.materials) != MaxMaterialHistory {
		t.Fatal("material quota not actually full")
	}
	replacement := []Key{{KeyID: "bob", Secret: "bob"}, {KeyID: "new", Secret: "novel material"}}
	if err := s.Authorize(Grant{Enabled: true, Generation: 2, Epoch: 2, Keys: replacement}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("quota: %v", err)
	}
	if !a.state.flag("revoked") {
		t.Fatal("capacity check retained removed alice")
	}
	if s.imageState.generation != 1 || s.imageState.epoch != 1 {
		t.Fatal("failed apply advanced authority pair")
	}
	if len(s.keys) != 1 || s.keys[0].keyID != "bob" || !s.imageState.authFailed {
		t.Fatal("capacity failure did not preserve survivor and refuse new authentication")
	}
	if len(s.imageState.materials) != MaxMaterialHistory {
		t.Fatal("capacity failure forgot replay history")
	}
}
