package clusterharness

import (
	"fmt"
	"sync"
)

const cohortRecordBytes = 16 << 10

// These immutable application periods depend only on two bytes (length and
// offset), so storage is bounded. Reuse them across real fixture lifecycles:
// per-record race instrumentation otherwise dominates the longer live source.
// The P13 consumer retains its independent formula and full received-byte hash.
// origin: S10 internal review FINAL-REAL-FAILS F6 / S6 plan §6.3 / T48
var cohortPayloadPeriods = struct {
	sync.Mutex
	periods map[uint16]*[256]byte
}{periods: make(map[uint16]*[256]byte)}

// The payload contract is duplicated independently by the P13 consumer. This
// collector hashes actual received bytes; it never copies a sender's hash/count.
// origin: hot-update S6 plan §6.3 / T48
func cohortPayload(id string, d uint8, sequence uint64) []byte {
	b := make([]byte, cohortRecordBytes)
	prefix := fmt.Sprintf("tether-broker-cohort-v1/%s/%d/%d\n", id, d, sequence)
	// The byte pattern repeats every 256 positions. Bulk copies preserve the
	// exact record while avoiding billions of per-byte race hooks across T48.
	// origin: hot-update S6 plan §6.3 / T48
	key := uint16(byte(len(id)))<<8 | uint16(byte(sequence*17+uint64(d)))
	cohortPayloadPeriods.Lock()
	period := cohortPayloadPeriods.periods[key]
	if period == nil {
		period = new([256]byte)
		for i := range period {
			period[i] = byte(uint64(i)*31+sequence*17+uint64(d)) ^ byte(len(id))
		}
		cohortPayloadPeriods.periods[key] = period
	}
	cohortPayloadPeriods.Unlock()
	copy(b, period[:])
	for filled := 256; filled < len(b); {
		filled += copy(b[filled:], b[:filled])
	}
	copy(b, prefix)
	return b
}
