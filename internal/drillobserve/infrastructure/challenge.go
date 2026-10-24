package infrastructure

import (
	"encoding/binary"
	"time"

	"math"
)

// TDI1 is accepted only on the inherited authenticated seqpacket endpoint.
// The 12-byte nonzero challenge and remaining budget (capped at five seconds)
// bind a fresh full-roster read followed by the existing sealed drain. TDIA
// carries challenge, last sealed sequence and a nonzero incomplete flag.
// origin: hot-update S9 plan §2.3 / §9.1; s9-instr4 Request 1
func Request(request []byte, sample func(int64) error, now func() (int64, error), seal func() (uint64, uint64), send func([]byte)) bool {
	if len(request) < 4 || string(request[:4]) != "TDI1" {
		return false
	}
	var nonce [12]byte
	if len(request) == 24 {
		copy(nonce[:], request[4:16])
	}
	failed := uint64(1)
	if nonce != ([12]byte{}) && len(request) == 24 {
		budget := binary.LittleEndian.Uint64(request[16:24])
		if budget > 0 && budget <= uint64(5*time.Second) && now != nil && sample != nil {
			if at, err := now(); err == nil && at > 0 && at <= math.MaxInt64-int64(budget) && sample(at+int64(budget)) == nil {
				failed = 0
			}
		}
	}
	sequence, poison := seal()
	if poison != 0 {
		failed = max(poison, 1)
	}
	reply := append([]byte("TDIA"), nonce[:]...)
	reply = binary.LittleEndian.AppendUint64(reply, sequence)
	reply = binary.LittleEndian.AppendUint64(reply, failed)
	send(reply)
	return true
}
