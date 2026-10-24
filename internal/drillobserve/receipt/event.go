// Package receipt owns pure private drill schemas and sealed-batch framing.
// It performs no IO and has no dependency on product runtimes or custody roots.
package receipt

import (
	"encoding/binary"
	"fmt"
	"io"
)

const MaxData = 32 << 10
const MaxBatch = 16

// Event is private binary state, never a public carrier. Values have a fixed
// meaning per Kind (shared with flow_observation.py); Data holds only the
// private suffix/prefix/identity required by that kind. Neither formatting verb
// exposes these bytes, even when an owner is logged accidentally.
type Event struct {
	Kind, Domain          string
	Endpoint, Flow, Actor [16]byte
	Digest                [32]byte
	Nonce                 [12]byte
	At                    uint64
	Values                [16]uint64
	Data                  []byte
}

func (e Event) MarshalJSON() ([]byte, error) { return []byte(`{"redacted":true}`), nil }

func (Event) String() string               { return "drillobserve.Event(redacted)" }
func (e Event) GoString() string           { return e.String() }
func (e Event) Format(w fmt.State, _ rune) { _, _ = io.WriteString(w, e.String()) }

func EncodeEvents(events []Event) []byte {
	// A full suffix batch previously grew and recopied its private data many
	// times. Reserve exactly this unchanged layout, keeping the custody queue
	// and its poison semantics bounded while reducing collector GC pressure.
	// origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
	size := 4
	for _, e := range events {
		size += 234 + len(e.Kind) + len(e.Domain) + len(e.Data)
	}
	out := binary.LittleEndian.AppendUint32(make([]byte, 0, size), uint32(len(events)))
	for _, e := range events {
		out = append(out, byte(len(e.Kind)))
		out = append(out, e.Kind...)
		out = append(out, byte(len(e.Domain)))
		out = append(out, e.Domain...)
		out = append(out, e.Endpoint[:]...)
		out = append(out, e.Flow[:]...)
		out = append(out, e.Actor[:]...)
		out = append(out, e.Digest[:]...)
		out = append(out, e.Nonce[:]...)
		out = binary.LittleEndian.AppendUint64(out, e.At)
		for _, v := range e.Values {
			out = binary.LittleEndian.AppendUint64(out, v)
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(e.Data)))
		out = append(out, e.Data...)
	}
	return out
}
