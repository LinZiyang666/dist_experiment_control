package ssproxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"io"
	"sort"
)

// The cryptographic domain is the actual master, not an administrative key ID
// or a direction. Aliases and removed/re-added IDs must share replay evidence.
// origin: hot-update S5 plan §6.3 / T19
const maxSaltsPerKey = 8192

type saltRing struct {
	set   map[string]struct{}
	order []string
	head  int
	count int
}

func (r *saltRing) observe(salt []byte) bool {
	if len(salt) != saltSize {
		return false
	}
	k := string(salt)
	if _, ok := r.set[k]; ok {
		return false
	}
	if r.set == nil {
		r.set = make(map[string]struct{})
	}
	if r.count < maxSaltsPerKey {
		r.order = append(r.order, k)
		r.count++
	} else {
		delete(r.set, r.order[r.head])
		r.order[r.head] = k
		r.head = (r.head + 1) % maxSaltsPerKey
	}
	r.set[k] = struct{}{}
	return true
}

type materialState struct {
	id          [16]byte
	fingerprint [32]byte
	master      []byte
	version     uint64
	ring        saltRing
	live        map[string]int
}

func materialFingerprint(master []byte) [32]byte {
	return sha256.Sum256(append([]byte("tether-ss-material-v1\n"), master...))
}

// reserveSaltLocked commits history and live ownership in one guard. A live
// salt remains protected even after its slot rolls out of the bounded FIFO.
func reserveSaltLocked(m *materialState, salt []byte) bool {
	if m.live[string(salt)] > 0 || !m.ring.observe(salt) {
		return false
	}
	m.live[string(salt)]++
	return true
}

func responseSaltLocked(s *Server, m *materialState) ([]byte, error) {
	source := s.imageState.random
	if source == nil {
		source = rand.Reader
	}
	// A broken entropy source cannot trap the server guard indefinitely.
	for attempt := 0; attempt < 128; attempt++ {
		salt := make([]byte, saltSize)
		if _, err := io.ReadFull(source, salt); err != nil {
			return nil, err
		}
		if reserveSaltLocked(m, salt) {
			return salt, nil
		}
	}
	return nil, ErrCapacity
}

func (s *Server) recordSalt(keyID string, salt []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.keys {
		if k.keyID == keyID {
			return k.material.ring.observe(salt) && k.material.live[string(salt)] == 0
		}
	}
	return false
}

// snapshotMaterials preserves physical FIFO slots as well as head/count. A
// chronological slice alone loses which salt the next insertion will evict.
func snapshotMaterials(s *Server) ([]sealedstate.Record, []byte) {
	st := s.imageState
	fps := make([][32]byte, 0, len(st.materials))
	for fp := range st.materials {
		fps = append(fps, fp)
	}
	sort.Slice(fps, func(i, j int) bool { return bytes.Compare(fps[i][:], fps[j][:]) < 0 })
	var records []sealedstate.Record
	var ids []byte
	for _, fp := range fps {
		m := st.materials[fp]
		p := newPrivateRecord(sealedstate.KindMaterial)
		p.putBytes("id", m.id[:])
		p.putBytes("fingerprint", fp[:])
		p.putBytes("master", m.master)
		p.put("version", m.version)
		var keyIDs, rx, tx []byte
		for _, k := range s.keys {
			if k.material == m {
				keyIDs = appendSized(keyIDs, []byte(k.keyID))
				p.put("active", 1)
			}
		}
		for _, a := range st.actors {
			if a.material == m {
				rx = append(rx, a.state.bytes("rx_salt")...)
				if a.state.flag("tx_salt_reserved") {
					tx = append(tx, a.state.bytes("tx_salt")...)
				}
			}
		}
		p.putBytes("key_ids", keyIDs)
		p.putBytes("live_rx_salts", rx)
		p.putBytes("live_tx_salts", tx)
		ring := newPrivateRecord(sealedstate.KindSaltRing)
		// Kind is part of record identity; ring and material deliberately share ID.
		ring.putBytes("id", m.id[:])
		ring.putBytes("material_id", m.id[:])
		ring.putBytes("fingerprint", fp[:])
		ring.put("material_version", m.version)
		ring.put("head", uint64(m.ring.head))
		ring.put("count", uint64(m.ring.count))
		var salts []byte
		for _, salt := range m.ring.order {
			salts = append(salts, []byte(salt)...)
		}
		ring.putBytes("salts", salts)
		records = append(records, p.clone(), ring.clone())
		ids = append(ids, m.id[:]...)
	}
	return records, ids
}

func restoreMaterials(st *serverImageState, records []privateRecord) error {
	rings := make(map[[16]byte]privateRecord)
	live := make(map[[16]byte][]byte)
	for _, p := range records {
		switch p.record.Kind {
		case sealedstate.KindMaterial:
			if len(st.materials) >= MaxMaterialHistory || len(p.bytes("master")) != keySize || p.number("version") == 0 {
				return ErrStateUnrepresentable
			}
			fp := materialFingerprint(p.bytes("master"))
			if !bytes.Equal(fp[:], p.bytes("fingerprint")) || st.materials[fp] != nil {
				return ErrStateUnrepresentable
			}
			id := stateID(p.bytes("id"))
			m := &materialState{id: id, fingerprint: fp, master: append([]byte(nil), p.bytes("master")...), version: p.number("version"), live: make(map[string]int)}
			st.materials[fp] = m
			live[id] = append(append([]byte(nil), p.bytes("live_rx_salts")...), p.bytes("live_tx_salts")...)
		case sealedstate.KindSaltRing:
			rings[stateID(p.bytes("material_id"))] = p
		}
	}
	if len(rings) != len(st.materials) {
		return ErrStateUnrepresentable
	}
	for _, m := range st.materials {
		r, ok := rings[m.id]
		if !ok {
			return ErrStateUnrepresentable
		}
		count, head := r.number("count"), r.number("head")
		if count > maxSaltsPerKey || head >= maxSaltsPerKey || (count < maxSaltsPerKey && head != 0) || len(r.bytes("salts")) != int(count)*saltSize || !bytes.Equal(r.bytes("fingerprint"), m.fingerprint[:]) || r.number("material_version") != m.version {
			return ErrStateUnrepresentable
		}
		m.ring = saltRing{set: make(map[string]struct{}), head: int(head), count: int(count)}
		for b := r.bytes("salts"); len(b) > 0; b = b[saltSize:] {
			k := string(b[:saltSize])
			if _, ok := m.ring.set[k]; ok {
				return ErrStateUnrepresentable
			}
			m.ring.order = append(m.ring.order, k)
			m.ring.set[k] = struct{}{}
		}
		b := live[m.id]
		if len(b)%saltSize != 0 || len(b) > 2*MaxActors*saltSize {
			return ErrStateUnrepresentable
		}
		for len(b) > 0 {
			k := string(b[:saltSize])
			if m.live[k] != 0 {
				return ErrStateUnrepresentable
			}
			m.live[k] = 1
			b = b[saltSize:]
		}
	}
	return nil
}
