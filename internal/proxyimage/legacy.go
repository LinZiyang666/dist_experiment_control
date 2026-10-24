package proxyimage

import (
	"encoding/binary"
	"errors"
)

var ErrInvalid = errors.New("proxyimage: invalid state")

const legacyStateLimit = 16 << 10

// DecodeLegacyCut reads the exact prior TPRX envelope without admitting new
// authority or scheduling work. Writers use the declared sealed descriptors.
// origin: hot-update S5 plan §7.2 / L12 — move legacy decoding out of Agent.
func DecodeLegacyCut(data []byte) (ProxyCut, error) {
	// Read-only compatibility for the prior private envelope. New cuts always
	// use the declared binary descriptor codec, including metadata-only cuts.
	// origin: hot-update S5 internal review PRIVATE-STATE-2
	st := ProxyCut{}
	invalid := func() (ProxyCut, error) { return ProxyCut{}, ErrInvalid }
	if len(data) < 112 || len(data) > legacyStateLimit || string(data[:5]) != "TPRX\x00" || (data[5] != 1 && data[5] != 2) {
		return invalid()
	}
	version := data[5]
	data = data[6:]
	values := make([]int64, 8)
	for i := range values {
		values[i] = int64(binary.BigEndian.Uint64(data[:8]))
		data = data[8:]
	}
	st.State.DispatchSequence, st.State.LastAppliedSequence, st.State.Generation, st.State.Epoch = values[0], values[1], values[2], values[3]
	if values[4] < 0 || values[4] > 65535 || values[5] < 0 || values[5] > 65535 {
		return invalid()
	}
	st.State.AllocationPort, st.LocalPort, st.HomeEpoch, st.State.DeadlineBootNS = int(values[4]), int(values[5]), values[6], values[7]
	copy(st.State.KeysetDigest[:], data[:32])
	data = data[32:]
	off := false
	flags := []*bool{&st.DigestApplied, &st.TunnelUp, &st.NeedsReestablish, &off, &st.State.Fired, &st.State.CleanupPending}
	for _, flag := range flags {
		if data[0] > 1 {
			return invalid()
		}
		*flag = data[0] == 1
		data = data[1:]
	}
	st.State.Enabled = !off
	for _, value := range []*string{&st.Token, &st.HomeAddr} {
		if len(data) < 2 {
			return invalid()
		}
		n := int(binary.BigEndian.Uint16(data[:2]))
		data = data[2:]
		if n > 4096 || n > len(data) {
			return invalid()
		}
		*value = string(data[:n])
		data = data[n:]
	}
	// Older private cuts have no planned latch. New cuts retain the absolute
	// BOOTTIME deadline, so exec/rollback never grants another eight seconds.
	// A versioned extension cannot turn a truncated v2 cut into a valid v1
	// cut. Keep the old envelope readable, with exact closure in both versions.
	// origin: hot-update S5 internal review rv-authority-6
	if version == 2 {
		if len(data) != 8 {
			return invalid()
		}
		st.State.PlannedDeadlineBootNS = int64(binary.BigEndian.Uint64(data))
		data = data[8:]
	}
	if len(data) != 0 || !validLegacyCut(st) {
		return invalid()
	}
	return st, nil
}

func validLegacyCut(st ProxyCut) bool {
	p := st.State
	return p.DispatchSequence >= 0 && p.LastAppliedSequence >= 0 &&
		p.LastAppliedSequence <= p.DispatchSequence && p.Generation >= 0 && p.Epoch >= 0 &&
		st.HomeEpoch >= 0 && p.AllocationPort >= 0 && p.AllocationPort <= 65535 &&
		st.LocalPort >= 0 && st.LocalPort <= 65535 && p.DeadlineBootNS >= 0 && p.PlannedDeadlineBootNS >= 0 &&
		(!p.CleanupPending || p.Fired)
}
