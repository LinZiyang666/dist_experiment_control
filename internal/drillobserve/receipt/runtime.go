package receipt

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"unicode/utf8"
)

const RuntimeSampleKind = "runtime-sample"

// Only business counters lack safe getters. Identity, G/FD, time and ordinals
// must be known; an unknown counter has zero storage but never means zero work.
// origin: hot-update S10 plan §5.3 / P06
const RuntimeUnknownCounters uint16 = 0x7fc0

var ErrRuntimeSample = errors.New("runtime sample custody invalid")

type RuntimeBinding struct {
	Domain, InstanceID string
	Generation         uint64
}

type RuntimeCounters struct {
	Binding                                                   RuntimeBinding
	PID, StartTicks, Goroutines, FDs                          uint64
	ReplayReserved, ActiveExec, ActivePTY, RetainedRecords    uint64
	CourierPending, Consumers, Temps, UpgradeLocks, OldOwners uint64
	Sequence, AtBootNS                                        uint64
	ExeSHA                                                    [32]byte
	Nonce                                                     [12]byte
	BootID, PIDNS, CutID, ScopeDigest                         string
	Unknown                                                   uint16
}

// EncodeRuntimeSample preserves the fixed binary ABI rather than relying on
// JSON field names or struct reflection. Event's redaction still applies.
// origin: hot-update S10 plan §5.3 / P06
func EncodeRuntimeSample(c RuntimeCounters) (Event, error) {
	e := Event{Kind: RuntimeSampleKind, Domain: c.Binding.Domain, At: c.AtBootNS, Digest: c.ExeSHA, Nonce: c.Nonce,
		Values: [16]uint64{1, c.PID, c.StartTicks, c.Binding.Generation, c.Goroutines, c.FDs,
			c.ReplayReserved, c.ActiveExec, c.ActivePTY, c.RetainedRecords, c.CourierPending,
			c.Consumers, c.Temps, c.UpgradeLocks, c.OldOwners, c.Sequence}}
	e.Data = binary.LittleEndian.AppendUint16(nil, c.Unknown)
	for _, value := range []string{c.BootID, c.Binding.InstanceID, c.PIDNS, c.CutID, c.ScopeDigest} {
		if len(value) > 256 || !utf8.ValidString(value) {
			return Event{}, ErrRuntimeSample
		}
		e.Data = binary.LittleEndian.AppendUint16(e.Data, uint16(len(value)))
		e.Data = append(e.Data, value...)
	}
	if _, err := DecodeRuntimeSample(e); err != nil {
		return Event{}, err
	}
	return e, nil
}

func DecodeRuntimeSample(e Event) (RuntimeCounters, error) {
	c := RuntimeCounters{}
	if e.Kind != RuntimeSampleKind || e.Domain != "agent" || e.At == 0 || e.At > math.MaxInt64 ||
		e.Digest == ([32]byte{}) || e.Nonce == ([12]byte{}) || len(e.Data) < 2 || len(e.Data) > MaxData ||
		e.Endpoint != ([16]byte{}) || e.Flow != ([16]byte{}) || e.Actor != ([16]byte{}) {
		return c, ErrRuntimeSample
	}
	v := e.Values
	if v[0] != 1 || v[1] == 0 || v[1] > math.MaxInt32 || v[2] == 0 ||
		v[4] == 0 || v[4] > math.MaxInt64 || v[5] == 0 || v[5] > math.MaxInt32 || v[15] == 0 {
		return c, ErrRuntimeSample
	}
	unknown := binary.LittleEndian.Uint16(e.Data)
	if unknown & ^RuntimeUnknownCounters != 0 {
		return c, ErrRuntimeSample
	}
	for index, value := range v {
		if index >= 6 && index <= 14 && value > math.MaxInt64 || unknown&(1<<index) != 0 && value != 0 {
			return c, ErrRuntimeSample
		}
	}
	data := e.Data[2:]
	var fields [5]string
	for index := range fields {
		if len(data) < 2 {
			return c, ErrRuntimeSample
		}
		length := int(binary.LittleEndian.Uint16(data))
		data = data[2:]
		if length > 256 || length > len(data) || !utf8.Valid(data[:length]) {
			return c, ErrRuntimeSample
		}
		fields[index], data = string(data[:length]), data[length:]
	}
	if len(data) != 0 || !RuntimeToken(fields[0], false) || !RuntimeToken(fields[1], false) ||
		!RuntimeToken(fields[2], false) || !RuntimeToken(fields[3], true) || !runtimeDigest(fields[4]) {
		return c, ErrRuntimeSample
	}
	c = RuntimeCounters{Binding: RuntimeBinding{e.Domain, fields[1], v[3]}, PID: v[1], StartTicks: v[2],
		Goroutines: v[4], FDs: v[5], ReplayReserved: v[6], ActiveExec: v[7], ActivePTY: v[8],
		RetainedRecords: v[9], CourierPending: v[10], Consumers: v[11], Temps: v[12],
		UpgradeLocks: v[13], OldOwners: v[14], Sequence: v[15], AtBootNS: e.At,
		ExeSHA: e.Digest, Nonce: e.Nonce, BootID: fields[0], PIDNS: fields[2], CutID: fields[3],
		ScopeDigest: fields[4], Unknown: unknown}
	return c, nil
}

func RuntimeToken(value string, empty bool) bool {
	if value == "" {
		return empty
	}
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9',
			char == '-', char == '_', char == '.', char == ':', char == '[', char == ']':
		default:
			return false
		}
	}
	return true
}

func runtimeDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == hex.EncodeToString(decoded) && value != hex.EncodeToString(make([]byte, 32))
}

// TDS1 only carries a challenge and scope digest. A collector cannot supply
// product identity or generation. TDSA is sent after sealing its fresh sample.
// origin: hot-update S10 plan §5.3 / P06
func RuntimeSampleRequest(nonce [12]byte, scope [32]byte) ([]byte, error) {
	if nonce == ([12]byte{}) || scope == ([32]byte{}) {
		return nil, ErrRuntimeSample
	}
	return append(append([]byte("TDS1"), nonce[:]...), scope[:]...), nil
}

func ValidateRuntimeSampleACK(data []byte, sample RuntimeCounters, sealedSequence uint64) error {
	if _, err := EncodeRuntimeSample(sample); err != nil || len(data) != 72 || string(data[:4]) != "TDSA" ||
		string(data[4:16]) != string(sample.Nonce[:]) || hex.EncodeToString(data[16:48]) != sample.ScopeDigest ||
		binary.LittleEndian.Uint64(data[48:56]) != sample.Sequence || sealedSequence == 0 ||
		binary.LittleEndian.Uint64(data[56:64]) != sealedSequence || binary.LittleEndian.Uint64(data[64:72]) != 0 {
		return ErrRuntimeSample
	}
	return nil
}

// DecodeRuntimeRequest validates the custody challenge with the same encoder
// the caller uses. Parsing a partial request cannot invent a sample nonce.
// origin: hot-update S10 plan §5.3 / S10 internal review MAKETEST-GATES F8
func DecodeRuntimeRequest(data []byte) (nonce [12]byte, scope [32]byte, err error) {
	if len(data) != 48 || string(data[:4]) != "TDS1" {
		return nonce, scope, ErrRuntimeSample
	}
	copy(nonce[:], data[4:16])
	copy(scope[:], data[16:48])
	_, err = RuntimeSampleRequest(nonce, scope)
	return nonce, scope, err
}
