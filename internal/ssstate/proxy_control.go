package ssstate

import "github.com/LinZiyang666/tether/internal/sealedstate"

// Both sealed carriers construct shared authority/timer fields here. The SS
// owner adds its key material/revision; the auxiliary carrier adds routing.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func proxyControlRecords(p ProxyState, id [16]byte) (privateRecord, privateRecord) {
	a := newPrivateRecord(sealedstate.KindAuthority)
	a.putBytes("id", id[:])
	a.put("generation", uint64(p.Generation))
	a.put("epoch", uint64(p.Epoch))
	a.putBytes("keyset_digest", p.KeysetDigest[:])
	a.put("revoked", boolState(!p.Enabled))
	a.put("enabled", boolState(p.Enabled))
	a.put("dispatch_sequence", uint64(p.DispatchSequence))
	a.put("last_applied_sequence", uint64(p.LastAppliedSequence))
	a.put("deadline_boot_ns", uint64(p.DeadlineBootNS))
	a.putBytes("allocation_sid", []byte(p.AllocationSID))
	a.putBytes("allocation_nid", []byte(p.AllocationNID))
	a.put("allocation_port", uint64(p.AllocationPort))
	a.putBytes("allocation_token_hash", []byte(p.AllocationTokenHash))
	a.put("allocation_epoch", uint64(p.AllocationEpoch))
	timer := newPrivateRecord(sealedstate.KindTimer)
	timer.putBytes("id", id[:])
	timer.putBytes("owner_id", id[:])
	timer.put("deadline_boot_ns", uint64(p.PlannedDeadlineBootNS))
	timer.put("fail_close_deadline_boot_ns", uint64(p.DeadlineBootNS))
	timer.put("fired", boolState(p.Fired))
	timer.put("cleanup_pending", boolState(p.CleanupPending))
	timer.put("revoked", boolState(!p.Enabled))
	return a, timer
}

func boolState(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

// Shared decoding prevents the auxiliary cut from drifting from SS control.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func readProxyControl(authority, timer privateRecord) ProxyState {
	p := ProxyState{
		DispatchSequence: int64(authority.number("dispatch_sequence")), LastAppliedSequence: int64(authority.number("last_applied_sequence")),
		Generation: int64(authority.number("generation")), Epoch: int64(authority.number("epoch")),
		Enabled: authority.flag("enabled"), AllocationSID: string(authority.bytes("allocation_sid")), AllocationNID: string(authority.bytes("allocation_nid")),
		AllocationPort: int(authority.number("allocation_port")), AllocationTokenHash: string(authority.bytes("allocation_token_hash")), AllocationEpoch: int64(authority.number("allocation_epoch")),
		DeadlineBootNS: int64(authority.number("deadline_boot_ns")), PlannedDeadlineBootNS: int64(timer.number("deadline_boot_ns")), Fired: timer.flag("fired"), CleanupPending: timer.flag("cleanup_pending"),
	}
	copy(p.KeysetDigest[:], authority.bytes("keyset_digest"))
	return p
}

// ProxyControlRecords constructs the shared fields for both sealed carriers.
func ProxyControlRecords(p ProxyState, id [16]byte) (sealedstate.Record, sealedstate.Record) {
	a, timer := proxyControlRecords(p, id)
	return a.clone(), timer.clone()
}

// ReadProxyControl reads the shared fields after descriptor validation.
func ReadProxyControl(authority, timer sealedstate.Record) ProxyState {
	return readProxyControl(wrapPrivateRecord(authority), wrapPrivateRecord(timer))
}
