package ssproxy

import (
	"bytes"

	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/ssstate"
)

// SetProxyState only copies metadata. It never grants forwarding, renews a
// deadline, or runs cleanup. SS's guard serializes it with the final re-export.
func SetProxyState(s *Server, p ProxyState) error {
	if s == nil || !validProxyState(p) {
		return ErrStateUnrepresentable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ensureImageState(s)
	if s.closed {
		return ErrStopped
	}
	if st.granted && (p.Generation != st.generation || p.Epoch != st.epoch) {
		return ErrStateUnrepresentable
	}
	if st.proxyImported && st.proxy != nil && *st.proxy != p {
		return ErrStateUnrepresentable
	}
	st.proxy = &p
	return nil
}

// ValidateProxyState proves agreement of both private carriers before the
// agent installs revoke/timer callbacks. Fresh servers have no imported copy.
func ValidateProxyState(s *Server, p ProxyState) error {
	if s == nil || !validProxyState(p) {
		return ErrStateUnrepresentable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ensureImageState(s)
	if st.proxy != nil && *st.proxy != p {
		return ErrStateUnrepresentable
	}
	return nil
}

func snapshotSSControl(s *Server, server privateRecord) (sealedstate.Record, sealedstate.Record, error) {
	st := s.imageState
	p := ProxyState{Generation: st.generation, Epoch: st.epoch, KeysetDigest: st.digest, Enabled: st.authority != AuthorityPhaseRevoked}
	if st.proxy != nil {
		p = *st.proxy
		// Fresh authorization may have advanced an activated standalone owner
		// since its last control copy. SS remains the keyset freshness owner.
		p.Generation, p.Epoch = st.generation, st.epoch
		// The agent directive digest covers the original passwords, whereas
		// SS's trial digest covers derived masters. They are distinct domains.
		// A standalone owner uses its material digest; an allocation's opaque
		// directive digest is checked against the agent envelope before callbacks.
		if p.AllocationPort == 0 {
			p.KeysetDigest = st.digest
		}
		p.Enabled = st.authority != AuthorityPhaseRevoked
	}
	if !validProxyState(p) {
		return sealedstate.Record{}, sealedstate.Record{}, ErrStateUnrepresentable
	}
	a, timer := proxyControlRecords(p, st.id)
	a.put("applied_revision", st.revision)
	a.putBytes("keys", server.bytes("keys"))
	a.put("phase", server.number("authority_phase"))
	a.put("revoked", server.number("revoked"))
	timer.put("revoked", server.number("revoked"))
	server.put("dispatch_sequence", uint64(p.DispatchSequence))
	server.put("last_applied_sequence", uint64(p.LastAppliedSequence))
	server.put("cleanup_pending", boolState(p.CleanupPending))
	server.putBytes("timer_ids", st.id[:])
	return a.clone(), timer.clone(), nil
}

// Both private carriers use the same descriptor-owned control builder/reader.
// origin: hot-update S5 plan §5.1 / §12.1
func proxyControlRecords(p ProxyState, id [16]byte) (privateRecord, privateRecord) {
	a, timer := ssstate.ProxyControlRecords(p, id)
	return wrapPrivateRecord(a), wrapPrivateRecord(timer)
}
func readProxyControl(authority, timer privateRecord) ProxyState {
	return ssstate.ReadProxyControl(authority.record, timer.record)
}

func boolState(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

func restoreSSControl(st *serverImageState, server, authority, timer privateRecord) error {
	if authority.fields == nil || timer.fields == nil {
		return ErrStateUnrepresentable
	}
	p := readProxyControl(authority, timer)
	if !validProxyState(p) || !bytes.Equal(authority.bytes("id"), st.id[:]) || !bytes.Equal(timer.bytes("id"), st.id[:]) ||
		!bytes.Equal(timer.bytes("owner_id"), st.id[:]) || !bytes.Equal(server.bytes("timer_ids"), st.id[:]) ||
		timer.number("fail_close_deadline_boot_ns") != uint64(p.DeadlineBootNS) ||
		server.number("dispatch_sequence") != uint64(p.DispatchSequence) || server.number("last_applied_sequence") != uint64(p.LastAppliedSequence) ||
		server.flag("cleanup_pending") != p.CleanupPending || authority.number("phase") != server.number("authority_phase") ||
		authority.number("applied_revision") != st.revision || p.Generation != st.generation || p.Epoch != st.epoch ||
		!bytes.Equal(authority.bytes("keys"), server.bytes("keys")) || authority.flag("revoked") != server.flag("revoked") ||
		timer.flag("revoked") != server.flag("revoked") || p.Enabled == server.flag("revoked") {
		return ErrStateUnrepresentable
	}
	st.proxy = &p
	st.proxyImported = true
	return nil
}

// Material aliases are independently reconstructed from the active trial
// list. An omitted alias or false active flag cannot silently discard authority
// while preserving enough master bytes for a new handshake.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func validateMaterialMembership(s *Server, records []privateRecord) error {
	for _, p := range records {
		if p.record.Kind != sealedstate.KindMaterial {
			continue
		}
		var ids []byte
		for _, k := range s.keys {
			if bytes.Equal(k.material.id[:], p.bytes("id")) {
				ids = appendSized(ids, []byte(k.keyID))
			}
		}
		if p.flag("active") != (len(ids) != 0) || !bytes.Equal(p.bytes("key_ids"), ids) {
			return ErrStateUnrepresentable
		}
	}
	return nil
}

func validateSSMembership(server privateRecord, all []privateRecord) error {
	for _, pair := range []struct{ name, kind string }{{"conn_ids", sealedstate.KindSSActor}, {"material_ids", sealedstate.KindMaterial}, {"binding_ids", sealedstate.KindBinding}} {
		var want [][16]byte
		for _, p := range all {
			if p.record.Kind == pair.kind {
				want = append(want, stateID(p.bytes("id")))
			}
		}
		if !matchPrivateIDs(server.bytes(pair.name), want) {
			return ErrStateUnrepresentable
		}
	}
	return nil
}

// RestoredProxyState identifies the constructor-only agreement check. Live
// proxy apply already owns the agent apply guard, so wiring must not re-enter
// that guard just to inspect a newly constructed server with no imported state.
func RestoredProxyState(s *Server) (ProxyState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ensureImageState(s)
	if !st.proxyImported || st.proxy == nil {
		return ProxyState{}, false
	}
	return *st.proxy, true
}
