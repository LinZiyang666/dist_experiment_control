package ssproxy

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// Real material aliases, FIFO wrap, removal/re-add, imported history and a
// controlled entropy collision exercise one shared cryptographic salt domain.
// origin: hot-update S5 plan §6.3 / T19
func TestSaltHistoryAliasesRotationAndResponseCollision(t *testing.T) {
	keys := []Key{{KeyID: "alice", Secret: "shared"}, {KeyID: "alias", Secret: "shared"}}
	s := New(nil)
	if err := s.SetKeys(keys); err != nil {
		t.Fatal(err)
	}
	salt := bytes.Repeat([]byte{19}, 32)
	if !s.recordSalt("alice", salt) || s.recordSalt("alias", salt) {
		t.Fatal("aliases have separate salt history")
	}
	if err := s.SetKeys(nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetKeys([]Key{{KeyID: "alice", Secret: "rotated"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetKeys(keys); err != nil {
		t.Fatal(err)
	}
	if s.recordSalt("alice", salt) {
		t.Fatal("removal or rotation forgot material history")
	}
	m := s.keys[0].material
	live := bytes.Repeat([]byte{20}, 32)
	s.mu.Lock()
	if !reserveSaltLocked(m, live) {
		t.Fatal("live reservation refused")
	}
	s.mu.Unlock()
	for i := 0; i < maxSaltsPerKey+7; i++ {
		b := make([]byte, 32)
		binary.BigEndian.PutUint64(b, uint64(i+100))
		if !s.recordSalt("alice", b) {
			t.Fatal("fresh FIFO salt refused")
		}
	}
	if _, ok := m.ring.set[string(live)]; ok {
		t.Fatal("live salt never left FIFO")
	}
	s.mu.Lock()
	again := reserveSaltLocked(m, live)
	s.mu.Unlock()
	if again {
		t.Fatal("evicted live salt was reused")
	}
	// Drop synthetic live ownership before import: the image must reject live
	// salts without their corresponding actor, rather than silently invent one.
	delete(m.live, string(live))
	beforeHead, beforeOrder := m.ring.head, append([]string(nil), m.ring.order...)
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := restorePrivateRecords(cut.Records)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys}); err != nil {
		t.Fatal(err)
	}
	m = restored.keys[0].material
	if m.ring.head != beforeHead || m.ring.count != maxSaltsPerKey || len(m.ring.order) != len(beforeOrder) {
		t.Fatal("FIFO head/count changed across cut")
	}
	for i, v := range beforeOrder {
		if m.ring.order[i] != v {
			t.Fatal("FIFO slot order changed across cut")
		}
	}
	collision := []byte(beforeOrder[beforeHead])
	fresh := bytes.Repeat([]byte{201}, 32)
	restored.imageState.random = bytes.NewReader(append(append([]byte(nil), collision...), fresh...))
	restored.mu.Lock()
	response, err := responseSaltLocked(restored, m)
	restored.mu.Unlock()
	if err != nil || !bytes.Equal(response, fresh) {
		t.Fatal("response salt did not retry inbound/history collision")
	}
	if restored.recordSalt("alias", fresh) {
		t.Fatal("response salt was not reserved against inbound aliases")
	}
	if m.live[string(fresh)] != 1 {
		t.Fatal("response salt has no live protection")
	}
	_ = f.Discard()
	restored.Stop()
}
