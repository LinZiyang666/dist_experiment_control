package ssproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/ssstate"
	"golang.org/x/crypto/chacha20poly1305"
)

var errAuthorityConflict = errors.New("ssproxy: authority conflict")

// prepareKeys validates the entire snapshot before exposing any new credential.
// Error text is deliberately independent of IDs, passwords and derived bytes.
func prepareKeys(keys []Key) ([]*keyEntry, error) {
	if len(keys) > MaxKeys {
		return nil, ErrCapacity
	}
	out := make([]*keyEntry, 0, len(keys))
	seen := make(map[string]bool)
	for _, k := range keys {
		if k.KeyID == "" || len(k.KeyID) > 128 || len(k.Secret) > 512 || seen[k.KeyID] {
			return nil, ErrStateUnrepresentable
		}
		seen[k.KeyID] = true
		out = append(out, &keyEntry{keyID: k.KeyID, master: evpBytesToKey(k.Secret)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].keyID < out[j].keyID })
	return out, nil
}

func keysDigest(keys []*keyEntry) [32]byte {
	h := sha256.New()
	for _, k := range keys {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(k.keyID)))
		_, _ = h.Write(n[:])
		_, _ = h.Write([]byte(k.keyID))
		_, _ = h.Write(k.master)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// applyKeysLocked first withdraws absent IDs. Capacity failure must not keep an
// explicitly removed credential authorized; survivors retain their fixed key.
// origin: hot-update S5 plan §6.4 / T20 / T21
func applyKeysLocked(s *Server, entries []*keyEntry) ([]*ssActor, error) {
	st := ensureImageState(s)
	present := make(map[string]bool, len(entries))
	for _, k := range entries {
		present[k.keyID] = true
	}
	var victims []*ssActor
	for _, a := range st.actors {
		id := string(a.state.bytes("authorized_key_id"))
		if id != "" && !present[id] && markRevokedLocked(a) {
			victims = append(victims, a)
		}
	}
	// Removed IDs leave the active trial list even if the replacement fails.
	retained := s.keys[:0:0]
	for _, k := range s.keys {
		if present[k.keyID] {
			retained = append(retained, k)
		}
	}
	s.keys = retained
	missing := make(map[[32]byte]bool)
	for _, k := range entries {
		fp := materialFingerprint(k.master)
		if st.materials[fp] == nil {
			missing[fp] = true
		}
	}
	if len(st.materials)+len(missing) > MaxMaterialHistory {
		st.authFailed = true
		return victims, ErrCapacity
	}
	for _, k := range entries {
		fp := materialFingerprint(k.master)
		m := st.materials[fp]
		if m == nil {
			id, err := newStateID()
			if err != nil {
				st.authFailed = true
				return victims, err
			}
			m = &materialState{id: id, fingerprint: fp, master: append([]byte(nil), k.master...), version: uint64(len(st.materials) + 1), live: make(map[string]int)}
			st.materials[fp] = m
		}
		k.material = m
	}
	s.keys = entries
	st.authFailed = false
	return victims, nil
}

func (s *Server) SetKeys(keys []Key) error {
	entries, err := prepareKeys(keys)
	if err != nil {
		if errors.Is(err, ErrCapacity) {
			revokeRemovedOnFailure(s, keys)
		}
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrStopped
	}
	victims, err := applyKeysLocked(s, entries)
	if err == nil && s.imageState.granted {
		s.imageState.digest = keysDigest(s.keys)
	}
	s.mu.Unlock()
	cleanupActors(s, victims)
	return err
}

// bindTrialLocked rechecks the exact material that authenticated the first
// length. An ID surviving rotation is insufficient authorization for a new peer.
// Existing authenticated actors deliberately keep their original material.
// origin: hot-update S5 plan §6.3 / §6.4 / T20
func bindTrialLocked(s *Server, a *ssActor, trial *keyEntry) bool {
	st := s.imageState
	if s.closed || st.authFailed || st.authority != AuthorityPhaseAuthorized || a.state.flag("revoked") {
		return false
	}
	for _, current := range s.keys {
		if current.keyID != trial.keyID || !bytes.Equal(current.master, trial.master) {
			continue
		}
		if !reserveSaltLocked(current.material, a.state.bytes("rx_salt")) {
			return false
		}
		a.material = current.material
		a.state.putBytes("authorized_key_id", []byte(current.keyID))
		a.state.putBytes("material_id", current.material.id[:])
		a.state.putBytes("material_fingerprint", current.material.fingerprint[:])
		a.state.put("material_version", current.material.version)
		if a.clientConn != nil {
			if s.keyConns[current.keyID] == nil {
				s.keyConns[current.keyID] = make(map[netConn]struct{})
			}
			s.keyConns[current.keyID][a.clientConn] = struct{}{}
		}
		return true
	}
	return false
}

// CompareAuthority orders one broker authority watermark. Generation scopes the
// database lineage, epoch scopes its keyset and revision orders snapshots within
// that epoch. A database restore may reset both epoch and revision. Pushes have
// no revision on their legacy wire and compare the generation/epoch projection.
// origin: hot-update S5 internal review rv-ss-5 / rv-authority-4 / rv-authority-V1
func CompareAuthority(generation, epoch int64, revision uint64, currentGeneration, currentEpoch int64, currentRevision uint64) int {
	for _, pair := range [][2]uint64{{uint64(generation), uint64(currentGeneration)}, {uint64(epoch), uint64(currentEpoch)}, {revision, currentRevision}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	return 0
}

func (s *Server) Authorize(g Grant) error {
	if g.Generation < 0 || g.Epoch < 0 {
		return errAuthorityConflict
	}
	s.mu.Lock()
	st := ensureImageState(s)
	// Freshness precedes even disabled/oversized processing: rejection must not
	// revoke current keys, close sockets, suspend survivors or poison admission.
	// origin: hot-update S5 internal review rv-ss-5 / rv-ss-V1
	if st.granted && CompareAuthority(g.Generation, g.Epoch, g.AppliedRevision, st.generation, st.epoch, st.revision) < 0 {
		// A lower revision cannot excuse two complete keysets claiming the same
		// current generation/epoch. Hold forwarding without applying the replay;
		// ordinary stale pairs, disabled grants and invalid snapshots stay inert.
		// origin: hot-update S5 internal review SS-REVOKE-2 / plan §6.4
		if g.Enabled && g.Generation == st.generation && g.Epoch == st.epoch {
			if entries, err := prepareKeys(g.Keys); err == nil && keysDigest(entries) != st.digest {
				st.authority = AuthorityPhasePending
				signalStateLocked(st)
			}
		}
		s.mu.Unlock()
		return errAuthorityConflict
	}
	if s.closed || st.authority == AuthorityPhaseRevoked {
		s.mu.Unlock()
		return ErrStopped
	}
	if !g.Enabled {
		st.authority = AuthorityPhaseRevoked
		st.generation, st.epoch, st.revision, st.granted = g.Generation, g.Epoch, g.AppliedRevision, true
		s.keys = nil
		var victims []*ssActor
		for _, a := range st.actors {
			if markRevokedLocked(a) {
				victims = append(victims, a)
			}
		}
		observeGrantLocked(s, g)
		signalStateLocked(st)
		s.mu.Unlock()
		cleanupActors(s, victims)
		return nil
	}
	entries, err := prepareKeys(g.Keys)
	if err != nil {
		var victims []*ssActor
		if errors.Is(err, ErrCapacity) {
			victims = revokeRemovedLocked(s, g.Keys)
			authorizeSurvivorsLocked(st)
		}
		signalStateLocked(st)
		s.mu.Unlock()
		cleanupActors(s, victims)
		return err
	}
	digest := keysDigest(entries)
	if st.granted && g.Generation == st.generation && g.Epoch == st.epoch && digest != st.digest {
		// Conflicting current authority fails closed independently of numeric
		// freshness. A matching fresh snapshot must release this forwarding hold.
		st.authority = AuthorityPhasePending
		signalStateLocked(st)
		s.mu.Unlock()
		return errAuthorityConflict
	}
	victims, err := applyKeysLocked(s, entries)
	if err == nil {
		st.generation, st.epoch, st.revision = g.Generation, g.Epoch, g.AppliedRevision
		st.digest, st.granted = digest, true
	}
	if err == nil || errors.Is(err, ErrCapacity) {
		// Capacity forbids new trials through authFailed, while this fresh complete
		// snapshot has already removed every absent ID. Surviving bound actors can
		// forward even after Thaw; a failed apply never advances/ACKs its watermark.
		// origin: hot-update S5 internal review rv-ss-2
		authorizeSurvivorsLocked(st)
	}
	// origin: hot-update S5 plan §6.4 / T26. Record only an applied fresh grant.
	if err == nil {
		observeGrantLocked(s, g)
	}
	signalStateLocked(st)
	s.mu.Unlock()
	cleanupActors(s, victims)
	return err
}

func authorizeSurvivorsLocked(st *serverImageState) {
	st.authority = AuthorityPhaseAuthorized
	for _, a := range st.actors {
		if !a.state.flag("revoked") && len(a.state.bytes("authorized_key_id")) != 0 {
			a.state.put("authority_phase", uint64(AuthorityPhaseAuthorized))
		}
	}
}

func appendSized(b, value []byte) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	return append(append(b, size[:]...), value...)
}
func takeSized(b *[]byte, maxBytes int) ([]byte, error) {
	if len(*b) < 4 {
		return nil, ErrStateUnrepresentable
	}
	n := binary.BigEndian.Uint32((*b)[:4])
	*b = (*b)[4:]
	if n > uint32(maxBytes) || uint64(n) > uint64(len(*b)) {
		return nil, ErrStateUnrepresentable
	}
	value := (*b)[:n]
	*b = (*b)[n:]
	return value, nil
}
func encodeKeyEntries(keys []*keyEntry) []byte {
	var b []byte
	for _, k := range keys {
		b = appendSized(b, []byte(k.keyID))
		b = append(b, k.material.fingerprint[:]...)
	}
	return b
}
func restoreKeyEntries(b []byte, st *serverImageState) ([]*keyEntry, error) {
	var keys []*keyEntry
	seen := make(map[string]bool)
	for len(b) > 0 {
		id, err := takeSized(&b, 128)
		if err != nil || len(b) < 32 || len(id) == 0 || seen[string(id)] || len(keys) >= MaxKeys {
			return nil, ErrStateUnrepresentable
		}
		seen[string(id)] = true
		var fp [32]byte
		copy(fp[:], b[:32])
		b = b[32:]
		m := st.materials[fp]
		if m == nil {
			return nil, ErrStateUnrepresentable
		}
		keys = append(keys, &keyEntry{keyID: string(id), master: m.master, material: m})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].keyID < keys[j].keyID })
	return keys, nil
}

// The runtime and binary codec share structural validation. The owner still
// validates semantic relationships and requires a nonzero identity, so direct
// CutState restore has the same primitive guards as a decoded sealed image.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func validatePrivateRecord(r sealedstate.Record) (privateRecord, error) {
	descriptor, known := ssstate.RecordDescriptor(r.Kind)
	if !known || len(r.Fields) != len(descriptor.Fields) {
		return privateRecord{}, ErrStateUnrepresentable
	}
	id, err := ssstate.ValidateRecord(r, descriptor)
	if err != nil || id == ([16]byte{}) {
		return privateRecord{}, ErrStateUnrepresentable
	}
	return wrapPrivateRecord(wrapPrivateRecord(r).clone()), nil
}

func restorePrivateRecords(records []sealedstate.Record) (*Server, error) {
	s := New(nil)
	st := ensureImageState(s)
	st.frozen = true
	st.authority = AuthorityPhasePending
	var all []privateRecord
	var server, authority, timer privateRecord
	seen := make(map[string]bool)
	for _, r := range records {
		p, err := validatePrivateRecord(r)
		if err != nil {
			return nil, err
		}
		key := r.Kind + string(p.bytes("id"))
		if seen[key] {
			return nil, ErrStateUnrepresentable
		}
		seen[key] = true
		switch r.Kind {
		case sealedstate.KindSSServer:
			if server.fields != nil {
				return nil, ErrStateUnrepresentable
			}
			server = p
		case sealedstate.KindAuthority:
			if authority.fields != nil {
				return nil, ErrStateUnrepresentable
			}
			authority = p
		case sealedstate.KindTimer:
			if timer.fields != nil {
				return nil, ErrStateUnrepresentable
			}
			timer = p
		case sealedstate.KindSSActor, sealedstate.KindMaterial, sealedstate.KindSaltRing, sealedstate.KindBinding:
		// Other domains cannot be restored as SS owners, even with valid descriptors.
		// origin: hot-update S5 plan §5.1 / §10.2
		case sealedstate.KindEndpoint, sealedstate.KindFlow, sealedstate.KindPermit, sealedstate.KindReceipt, sealedstate.KindRetirement:
			return nil, ErrStateUnrepresentable
		default:
			return nil, ErrStateUnrepresentable
		}
		all = append(all, p)
	}
	if server.fields == nil {
		return nil, ErrStateUnrepresentable
	}
	auth := AuthorityPhase(server.number("authority_phase"))
	if auth < AuthorityPhasePending || auth > AuthorityPhaseRevoked {
		return nil, ErrStateUnrepresentable
	}
	if server.number("dispatch_sequence") < server.number("last_applied_sequence") {
		return nil, ErrStateUnrepresentable
	}
	st.id = stateID(server.bytes("id"))
	s.allowPrivate = server.flag("allow_private")
	st.generation, st.epoch, st.revision = int64(server.number("generation")), int64(server.number("epoch")), server.number("applied_revision")
	copy(st.digest[:], server.bytes("keyset_digest"))
	st.granted = st.digest != ([32]byte{})
	st.authFailed = auth == AuthorityPhasePending
	if server.flag("revoked") || auth == AuthorityPhaseRevoked {
		st.authority = AuthorityPhaseRevoked
	}
	if err := restoreSSControl(st, server, authority, timer); err != nil {
		return nil, err
	}
	if err := validateSSMembership(server, all); err != nil {
		return nil, err
	}
	st.listenerRef = string(server.bytes("listener_ref"))
	st.listenerTuple = append([]byte(nil), server.bytes("listener_tuple")...)
	st.listenerFingerprint = getFingerprint(server, "listener")
	if err := restoreMaterials(st, all); err != nil {
		return nil, err
	}
	keys, err := restoreKeyEntries(server.bytes("keys"), st)
	if err != nil {
		return nil, err
	}
	s.keys = keys
	if err := validateMaterialMembership(s, all); err != nil {
		return nil, err
	}
	if st.granted && auth == AuthorityPhaseAuthorized && keysDigest(keys) != st.digest {
		return nil, ErrStateUnrepresentable
	}
	if err := restoreSSActorsAndBindings(st, server, all); err != nil {
		return nil, err
	}
	return s, nil
}

// Reconstruct actor/binding and live-salt membership from the same validated cut.
// origin: hot-update S5 plan §6.3 / T19.
func restoreSSActorsAndBindings(st *serverImageState, server privateRecord, all []privateRecord) error {
	for _, p := range all {
		switch p.record.Kind {
		case sealedstate.KindSSActor:
			if len(st.actors) >= MaxActors {
				return ErrCapacity
			}
			a, err := restoreActorRecord(p, st)
			if err != nil {
				return err
			}
			st.actors[stateID(p.bytes("id"))] = a
		case sealedstate.KindBinding:
			tuple, err := parseTuple(p.bytes("tuple"))
			if err != nil {
				return err
			}
			if len(st.bindings) >= MaxActors || st.bindings[tuple] != nil {
				return ErrStateUnrepresentable
			}
			st.bindings[tuple] = &flowBinding{id: stateID(p.bytes("flow_id")), connID: stateID(p.bytes("conn_id")), tuple: tuple, consumed: p.flag("consumed"), deadline: int64(p.number("deadline_boot_ns"))}
		}
	}
	if len(server.bytes("conn_ids")) != 16*len(st.actors) || len(server.bytes("material_ids")) != 16*len(st.materials) || len(server.bytes("binding_ids")) != 16*len(st.bindings) {
		return ErrStateUnrepresentable
	}
	for b := server.bytes("conn_ids"); len(b) > 0; b = b[16:] {
		if st.actors[stateID(b[:16])] == nil {
			return ErrStateUnrepresentable
		}
	}
	// Live membership is independently reconstructed from actors, so a missing
	// live salt cannot reopen an exhausted ring slot after import.
	expected := make(map[[32]byte]map[string]bool)
	for _, a := range st.actors {
		if a.material != nil {
			fp := a.material.fingerprint
			if expected[fp] == nil {
				expected[fp] = make(map[string]bool)
			}
			salts := [][]byte{a.state.bytes("rx_salt")}
			if a.state.flag("tx_salt_reserved") {
				salts = append(salts, a.state.bytes("tx_salt"))
			}
			for _, salt := range salts {
				if expected[fp][string(salt)] {
					return ErrStateUnrepresentable
				}
				expected[fp][string(salt)] = true
			}
		}
	}
	for fp, m := range st.materials {
		if len(m.live) != len(expected[fp]) {
			return ErrStateUnrepresentable
		}
		for salt := range m.live {
			if !expected[fp][salt] {
				return ErrStateUnrepresentable
			}
		}
	}
	return nil
}

func restoreActorRecord(p privateRecord, st *serverImageState) (*ssActor, error) {
	if err := validateActorDecisions(p); err != nil {
		return nil, err
	}
	phase := SSPhase(p.number("phase"))
	auth := AuthorityPhase(p.number("authority_phase"))
	if phase < SSPhaseSalt || phase > SSPhaseTerminal || auth < AuthorityPhasePending || auth > AuthorityPhaseRevoked || stateID(p.bytes("id")) != stateID(p.bytes("conn_id")) {
		return nil, ErrStateUnrepresentable
	}
	if p.number("connect_state") < uint64(rawsocket.ConnectConnecting) || p.number("connect_state") > uint64(rawsocket.ConnectFailed) {
		return nil, ErrStateUnrepresentable
	}
	for _, c := range []struct{ cursor, buffer string }{{"rx_salt_cursor", "rx_salt"}, {"rx_length_cursor", "rx_length_cipher"}, {"rx_payload_cursor", "rx_payload_cipher"}, {"rx_clear_cursor", "rx_clear_pending"}, {"tx_salt_wire_cursor", "tx_salt"}, {"tx_cipher_write_cursor", "tx_cipher_pending"}, {"target_host_cursor", "target_host"}, {"target_port_cursor", "target_port"}} {
		if p.number(c.cursor) > uint64(len(p.bytes(c.buffer))) {
			return nil, ErrStateUnrepresentable
		}
	}
	if len(p.bytes("rx_length_cipher")) != 2+tagSize || p.number("rx_decoded_length") > maxChunk || len(p.bytes("rx_payload_cipher")) > maxChunk+tagSize || len(p.bytes("tx_plain_pending")) > maxChunk || len(p.bytes("tx_cipher_pending")) > maxChunk+2+2*tagSize || p.number("target_parser_phase") > 4 || len(p.bytes("target_host")) > 255 || len(p.bytes("target_port")) > 2 {
		return nil, ErrStateUnrepresentable
	}
	if p.flag("revoked") != (phase == SSPhaseRevoked) || p.flag("revoked") != (auth == AuthorityPhaseRevoked) {
		return nil, ErrStateUnrepresentable
	}
	if p.number("target_parser_phase") == 4 && (len(p.bytes("target_port")) != 2 || len(p.bytes("target_host")) == 0) {
		return nil, ErrStateUnrepresentable
	}
	if p.flag("rx_length_opened") && (p.number("rx_length_cursor") != 2+tagSize || len(p.bytes("rx_payload_cipher")) != int(p.number("rx_decoded_length"))+tagSize) {
		return nil, ErrStateUnrepresentable
	}
	if len(p.bytes("resolved_ip_candidates"))%16 != 0 || len(p.bytes("resolved_ip_candidates")) > 128*16 {
		return nil, ErrStateUnrepresentable
	}
	a := &ssActor{state: p, files: make(map[string]*os.File)}
	if len(p.bytes("authorized_key_id")) > 0 {
		var fp [32]byte
		copy(fp[:], p.bytes("material_fingerprint"))
		m := st.materials[fp]
		if m == nil || m.id != stateID(p.bytes("material_id")) || m.version != p.number("material_version") {
			return nil, ErrStateUnrepresentable
		}
		a.material = m
		subkey, err := sessionSubkey(m.master, p.bytes("rx_salt"))
		if err != nil || !bytes.Equal(subkey, p.bytes("rx_subkey")) {
			return nil, ErrStateUnrepresentable
		}
		a.rx, err = chacha20poly1305.New(subkey)
		if err != nil {
			return nil, err
		}
		if p.flag("tx_salt_reserved") {
			sk, err := sessionSubkey(m.master, p.bytes("tx_salt"))
			if err != nil || !bytes.Equal(sk, p.bytes("tx_subkey")) {
				return nil, ErrStateUnrepresentable
			}
			a.tx, err = chacha20poly1305.New(sk)
			if err == nil {
				a.tx = observeCipher(a.tx, sk, stateID(p.bytes("id")))
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if !p.flag("revoked") {
		p.put("authority_phase", uint64(AuthorityPhasePending))
	}
	return a, nil
}

// The runtime record is the private codec root, not a shadow export struct.
// Every cursor is committed directly to this root before an IO turn can end.
// origin: hot-update S5 plan §6.1 / §12.1 / T17
type privateRecord struct {
	record sealedstate.Record
	fields map[string]*sealedstate.Field
}

func newPrivateRecord(kind string) privateRecord {
	r := sealedstate.Record{Kind: kind}
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind == kind {
			for _, f := range d.Fields {
				field := sealedstate.Field{Name: f.Name, Type: f.Type}
				if f.Type >= sealedstate.FieldBytes {
					field.Bytes = make([]byte, f.Width)
				}
				r.Fields = append(r.Fields, field)
			}
		}
	}
	return wrapPrivateRecord(r)
}
func wrapPrivateRecord(r sealedstate.Record) privateRecord {
	p := privateRecord{record: r, fields: make(map[string]*sealedstate.Field)}
	for i := range p.record.Fields {
		f := &p.record.Fields[i]
		p.fields[f.Name] = f
	}
	return p
}
func (p privateRecord) number(k string) uint64      { return p.fields[k].U64 }
func (p privateRecord) bytes(k string) []byte       { return p.fields[k].Bytes }
func (p privateRecord) flag(k string) bool          { return p.number(k) != 0 }
func (p privateRecord) put(k string, v uint64)      { p.fields[k].U64 = v }
func (p privateRecord) putBytes(k string, v []byte) { p.fields[k].Bytes = append([]byte(nil), v...) }
func (p privateRecord) clone() sealedstate.Record {
	r := sealedstate.Record{Kind: p.record.Kind, Fields: make([]sealedstate.Field, len(p.record.Fields))}
	for i, f := range p.record.Fields {
		r.Fields[i] = f
		r.Fields[i].Bytes = append([]byte(nil), f.Bytes...)
	}
	return r
}
func stateID(b []byte) (id [16]byte) { copy(id[:], b); return }

// Public formatting must never recursively expose credentials or a grant.
// origin: hot-update S5 plan §6.6
func (k Key) String() string     { return "ssproxy.Key(redacted)" }
func (k Key) GoString() string   { return k.String() }
func (g Grant) String() string   { return "ssproxy.Grant(redacted)" }
func (g Grant) GoString() string { return g.String() }

// Even the key-count precheck cannot preserve a deleted ID. No new material is
// derived or admitted on this path, and survivors keep their established key.
func revokeRemovedOnFailure(s *Server, keys []Key) {
	s.mu.Lock()
	victims := revokeRemovedLocked(s, keys)
	s.mu.Unlock()
	cleanupActors(s, victims)
}

func revokeRemovedLocked(s *Server, keys []Key) []*ssActor {
	present := make(map[string]bool)
	for _, k := range keys {
		present[k.KeyID] = true
	}
	st := ensureImageState(s)
	st.authFailed = true
	var victims []*ssActor
	for _, a := range st.actors {
		id := string(a.state.bytes("authorized_key_id"))
		if id != "" && !present[id] && markRevokedLocked(a) {
			victims = append(victims, a)
		}
	}
	retained := s.keys[:0:0]
	for _, k := range s.keys {
		if present[k.keyID] {
			retained = append(retained, k)
		}
	}
	s.keys = retained
	return victims
}

// Relationships are checked independently of primitive field widths: a valid
// nonce-sized buffer cannot authorize a missing cipher or reset a half-frame.
func validateActorDecisions(p privateRecord) error {
	authenticated := len(p.bytes("authorized_key_id")) > 0
	if p.number("handshake_deadline_boot_ns") == 0 {
		return ErrStateUnrepresentable
	}
	if authenticated && p.number("rx_salt_cursor") != saltSize {
		return ErrStateUnrepresentable
	}
	if !authenticated && (p.flag("rx_length_opened") || p.flag("tx_salt_reserved") || !bytes.Equal(p.bytes("rx_nonce"), make([]byte, 12))) {
		return ErrStateUnrepresentable
	}
	for _, direction := range []string{"rx", "tx"} {
		if p.flag(direction+"_nonce_exhausted") && !bytes.Equal(p.bytes(direction+"_nonce"), bytes.Repeat([]byte{255}, 12)) {
			return ErrStateUnrepresentable
		}
	}
	if p.flag("tx_length_sealed") && !p.flag("tx_salt_reserved") || p.flag("tx_payload_sealed") && !p.flag("tx_length_sealed") {
		return ErrStateUnrepresentable
	}
	if p.flag("tx_length_sealed") && len(p.bytes("tx_length_cipher")) != 2+tagSize {
		return ErrStateUnrepresentable
	}
	if p.flag("tx_payload_sealed") && len(p.bytes("tx_payload_cipher")) != len(p.bytes("tx_plain_pending"))+tagSize {
		return ErrStateUnrepresentable
	}
	if p.flag("tx_payload_sealed") && !bytes.Equal(p.bytes("tx_cipher_pending"), append(append([]byte(nil), p.bytes("tx_length_cipher")...), p.bytes("tx_payload_cipher")...)) {
		return ErrStateUnrepresentable
	}
	if p.number("target_parser_phase") > 0 {
		atyp := p.number("target_atyp")
		if atyp != 1 && atyp != 3 && atyp != 4 {
			return ErrStateUnrepresentable
		}
		if p.number("target_host_cursor") != uint64(len(p.bytes("target_host"))) || p.number("target_port_cursor") != uint64(len(p.bytes("target_port"))) {
			return ErrStateUnrepresentable
		}
		if atyp != 3 && (!bytes.Equal(p.bytes("target_address"), p.bytes("target_host")) || p.number("target_address_cursor") != p.number("target_host_cursor")) {
			return ErrStateUnrepresentable
		}
	}
	if p.flag("policy_approved") && (!p.flag("dns_decided") || len(p.bytes("resolved_ip_candidates")) == 0) {
		return ErrStateUnrepresentable
	}
	if p.flag("dns_decided") && p.number("candidate_index") > uint64(len(p.bytes("resolved_ip_candidates"))/16) {
		return ErrStateUnrepresentable
	}
	return nil
}

// GrantDigest is order-independent but preserves every ID/material
// association. Length prefixes prevent ambiguous concatenation, and duplicate
// IDs are rejected rather than silently choosing one secret.
// origin: hot-update S5 plan §6.4
func GrantDigest(d Grant) ([32]byte, error) {
	if d.Generation < 0 || d.Epoch < 0 || len(d.Keys) > MaxKeys {
		return [32]byte{}, ErrStateUnrepresentable
	}
	keys := append([]Key(nil), d.Keys...)
	sort.Slice(keys, func(i, j int) bool { return keys[i].KeyID < keys[j].KeyID })
	h := sha256.New()
	_, _ = h.Write([]byte("tether.proxy-authority.v1"))
	enabled := byte(0)
	if d.Enabled {
		enabled = 1
	}
	_, _ = h.Write([]byte{enabled})
	var length [8]byte
	for i, key := range keys {
		if key.KeyID == "" || (i > 0 && keys[i-1].KeyID == key.KeyID) {
			return [32]byte{}, ErrStateUnrepresentable
		}
		for _, part := range []string{key.KeyID, key.Secret} {
			binary.BigEndian.PutUint64(length[:], uint64(len(part)))
			_, _ = h.Write(length[:])
			_, _ = h.Write([]byte(part))
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
