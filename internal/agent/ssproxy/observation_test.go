package ssproxy

import (
	"bytes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"golang.org/x/crypto/chacha20poly1305"
)

func captureObservations(t *testing.T) *[]drillobserve.Event {
	t.Helper()
	prior := drillobserve.Hook
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) { e.Data = append([]byte(nil), e.Data...); events = append(events, e) }
	t.Cleanup(func() { drillobserve.Hook = prior })
	return &events
}

type primitiveBarrier struct {
	cipher.AEAD
	events *[]drillobserve.Event
	t      *testing.T
}

func (a primitiveBarrier) Seal(dst, nonce, plain, aad []byte) []byte {
	a.t.Helper()
	if len(*a.events) == 0 {
		a.t.Fatal("primitive entered without observation")
	}
	e := (*a.events)[len(*a.events)-1]
	if e.Kind != "ss-seal" || !bytes.Equal(e.Nonce[:], nonce) || e.Values[0] != uint64(len(plain)) {
		a.t.Fatal("primitive entered before observation")
	}
	return a.AEAD.Seal(dst, nonce, plain, aad)
}

// origin: hot-update S5 plan §6.2 / T26 / DRILL-OBSERVER
func TestSSObservationRecordsEveryAttemptBeforeSeal(t *testing.T) {
	events := captureObservations(t)
	key := bytes.Repeat([]byte{0x6d}, keySize)
	base, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal(err)
	}
	primitive := primitiveBarrier{AEAD: base, events: events, t: t}
	for _, actor := range [][16]byte{{1}, {2}} {
		wrapped := observeCipher(primitive, key, actor)
		nonce := make([]byte, 12)
		exhausted := false
		if _, err := sealChunk(wrapped, nonce, &exhausted, []byte("payload")); err != nil {
			t.Fatal(err)
		}
		// Aliases/directions are deliberately recorded twice in the same crypto
		// domain. The independent judge must reject them; observer filtering would
		// hide the attempted nonce reuse it exists to expose.
	}
	if len(*events) != 2 || (*events)[0].Digest != sha256.Sum256(key) || (*events)[0].Nonce != (*events)[1].Nonce || (*events)[0].Digest != (*events)[1].Digest {
		t.Fatal("ledger partitioned actual material or omitted an attempt")
	}
	nonce := make([]byte, 12)
	exhausted := true
	if _, err := sealChunk(observeCipher(base, key, [16]byte{3}), nonce, &exhausted, nil); !errors.Is(err, errNonceExhausted) || len(*events) != 2 {
		t.Fatal("exhaustion recorded a primitive that was never attempted")
	}
}

type observationSocket struct {
	count  int
	err    error
	closed bool
}

func (s *observationSocket) Write(b []byte) (int, error)    { return min(s.count, len(b)), s.err }
func (*observationSocket) Read([]byte) (int, error)         { return 0, io.EOF }
func (s *observationSocket) Close() error                   { s.closed = true; return nil }
func (*observationSocket) CloseWrite() error                { return nil }
func (*observationSocket) SetReadDeadline(time.Time) error  { return nil }
func (*observationSocket) SetWriteDeadline(time.Time) error { return nil }

type observationTimeout struct{}

func (observationTimeout) Error() string   { return "test write timeout" }
func (observationTimeout) Timeout() bool   { return true }
func (observationTimeout) Temporary() bool { return true }

// origin: hot-update S5 plan §6.1 / §6.2 / T26 / DRILL-OBSERVER
func TestSSObservationRetainsFailedShortWriteWithoutReseal(t *testing.T) {
	events := captureObservations(t)
	key := bytes.Repeat([]byte{0x7a}, keySize)
	base, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal(err)
	}
	sock := &observationSocket{count: 7, err: io.ErrUnexpectedEOF}
	writer := newAEADWriter(sock, observeCipher(base, key, [16]byte{1}))
	if _, err := writer.Write(bytes.Repeat([]byte{0x41}, 64)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("write result %v", err)
	}
	if len(*events) != 3 {
		t.Fatalf("attempt count %d, want two primitives and actual write", len(*events))
	}
	e := (*events)[2]
	if e.Kind != "ss-write" || e.Values[0] != uint64(len(writer.pending)) || e.Values[1] != 7 || e.Values[2] != uint64(len(writer.pending)-7) || e.Values[3] != 1 || e.Nonce[0] != 1 || e.Values[6] != uint64(len(writer.pending)) || !bytes.Equal(e.Data, writer.pending[7:]) {
		t.Fatal("actual short/error/suffix receipt missing")
	}
	if (*events)[0].Nonce[0] != 0 || (*events)[1].Nonce[0] != 1 {
		t.Fatal("Seal ledger observed advanced nonce")
	}
	sock.count = len(writer.pending)
	sock.err = nil
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	seals := 0
	for _, e := range *events {
		if e.Kind == "ss-seal" {
			seals++
		}
	}
	if seals != 2 {
		t.Fatal("suffix flush repeated Seal")
	}
}

// origin: hot-update S5 plan §6.1 / T26 / DRILL-OBSERVER
func TestSSObservationSocketResultPrecedesTimeoutNormalization(t *testing.T) {
	events := captureObservations(t)
	s := New(nil)
	a, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	sock := &observationSocket{count: 3, err: observationTimeout{}}
	a.client = sock
	a.state.put("tx_salt_wire_cursor", saltSize)
	a.state.putBytes("tx_subkey", bytes.Repeat([]byte{0x4d}, keySize))
	s.mu.Lock()
	ensureImageState(s)
	n, err := actorIOLocked(s, a, sock, true, []byte("ciphertext"))
	s.mu.Unlock()
	if n != 3 || err != nil {
		t.Fatalf("product timeout behavior changed: %d %v", n, err)
	}
	if len(*events) != 1 || (*events)[0].Values[1] != 3 || (*events)[0].Values[3] != 1 || string((*events)[0].Data) != "hertext" {
		t.Fatal("observation normalized away actual socket failure")
	}
}

// origin: hot-update S5 plan §6.4 / §6.5 / T26 / DRILL-OBSERVER
func TestSSObservationGrantRevokeAndCloseFollowAppliedState(t *testing.T) {
	events := captureObservations(t)
	s := New(nil)
	grant := Grant{Enabled: true, Generation: 1, Epoch: 2, AppliedRevision: 3, Keys: []Key{{KeyID: "alice", Secret: "synthetic-private-secret"}}}
	if err := s.Authorize(grant); err != nil {
		t.Fatal(err)
	}
	if len(*events) != 1 || (*events)[0].Kind != "ss-grant" || (*events)[0].Values[4] != uint64(AuthorityPhaseAuthorized) {
		t.Fatal("grant observed before applied authorization")
	}
	conflict := grant
	conflict.Keys = []Key{{KeyID: "bob", Secret: "synthetic-other-secret"}}
	if err := s.Authorize(conflict); err == nil || len(*events) != 1 {
		t.Fatal("failed grant published success")
	}
	quota := New(nil)
	if err := quota.Authorize(grant); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxMaterialHistory-1; i++ {
		if err := quota.SetKeys([]Key{{KeyID: "alice", Secret: fmt.Sprintf("synthetic-history-%d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	count := len(*events)
	over := conflict
	over.Generation++
	if err := quota.Authorize(over); !errors.Is(err, ErrCapacity) || len(*events) != count {
		t.Fatal("failed capacity apply published a fresh grant")
	}
	a, err := newActor()
	if err != nil {
		t.Fatal(err)
	}
	a.client = &observationSocket{}
	a.state.putBytes("authorized_key_id", []byte("alice"))
	s.mu.Lock()
	s.imageState.actors[stateID(a.state.bytes("id"))] = a
	s.mu.Unlock()
	conflict.Generation++
	if err := s.Authorize(conflict); err != nil {
		t.Fatal(err)
	}
	marked, closed := false, false
	for _, e := range *events {
		if e.Kind == "ss-revoke-mark" {
			marked = e.Values[5] == 1 && e.Values[6] == 1
		}
		if e.Kind == "ss-closed" {
			closed = marked && a.client.(*observationSocket).closed
		}
	}
	if !marked || !closed {
		t.Fatal("revoke or final close observation omitted/moved")
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%c"} {
		base, _ := chacha20poly1305.New(bytes.Repeat([]byte{1}, 32))
		got := fmt.Sprintf(verb, observeCipher(base, []byte("private-subkey"), [16]byte{1}))
		if bytes.Contains([]byte(got), []byte("private-subkey")) {
			t.Fatal("observed AEAD formatting leaked secret")
		}
	}
}
