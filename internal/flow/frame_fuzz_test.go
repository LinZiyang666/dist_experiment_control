package flow

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// Real: handwritten header/control parsing with all modes and adversarial seeds.
// The oracle uses literal wire numbers/offsets; encoder/decoder round-trip alone
// would miss two implementations accepting the same unknown kind or overflow.
// origin: hot-update S5 plan §8 / T25
func FuzzFlowFrame(f *testing.F) {
	goldens := flowFrameGoldens(f)
	for _, wire := range goldens {
		f.Add(wire)
		for _, end := range []int{0, 1, 7, 39, 40, len(wire) - 1} {
			f.Add(wire[:end])
		}
	}
	for _, wire := range malformedFlowFrames(goldens) {
		f.Add(wire)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		r := bytes.NewReader(wire)
		got, err := DecodeFrame(r)
		invalidHeader := flowWireHeaderMustReject(wire)
		if invalidHeader {
			if err == nil {
				t.Fatal("decoder accepted unknown kind, flags, length, or offset overflow")
			}
			if len(wire) >= 40 && len(wire)-r.Len() != 40 {
				t.Fatal("invalid header consumed body before rejection")
			}
			return
		}
		if err != nil {
			return
		}
		bodyLen := int(binary.BigEndian.Uint32(wire[4:8]))
		if len(wire) < 40+bodyLen {
			t.Fatal("decoder accepted truncated body")
		}
		if r.Len() != len(wire)-40-bodyLen {
			t.Fatal("decoder crossed a frame boundary")
		}
		assertFrameEqual(t, got, frameFromGolden(wire[:40+bodyLen]))
		body := wire[40 : 40+bodyLen]
		switch wire[1] {
		case 3:
			if body[0] > 1 || body[1] < 1 || body[1] > 6 || binary.BigEndian.Uint16(body[4:6]) == 0 || binary.BigEndian.Uint16(body[4:6]) > 64 {
				t.Fatal("decoder accepted unknown RESUME mode or page count")
			}
		case 4:
			if body[112] < 1 || body[112] > 3 {
				t.Fatal("decoder accepted unknown RESUME_OK verdict")
			}
		case 9:
			if body[0] > 3 {
				t.Fatal("decoder accepted unknown CLOSE mode")
			}
		}
		var encoded bytes.Buffer
		if err := EncodeFrame(&encoded, got); err != nil {
			t.Fatal("encoder rejected decoded frame")
		}
		if !bytes.Equal(encoded.Bytes(), wire[:40+bodyLen]) {
			t.Fatal("round-trip changed wire bytes")
		}
	})
}

func flowWireHeaderMustReject(wire []byte) bool {
	if len(wire) < 40 {
		return true
	}
	version, kind := wire[0], wire[1]
	flags := binary.BigEndian.Uint16(wire[2:4])
	length := binary.BigEndian.Uint32(wire[4:8])
	offset := binary.BigEndian.Uint64(wire[32:40])
	if version != 1 || kind < 1 || kind > 9 || length > 32768 {
		return true
	}
	mask := uint16(1)
	if kind == 7 {
		mask = 3
	}
	if kind == 9 {
		mask = 5
	}
	if flags & ^mask != 0 || kind != 6 && kind != 7 && kind != 8 && offset != 0 {
		return true
	}
	if kind == 1 || kind == 2 || kind >= 6 {
		if bytes.Equal(wire[8:24], make([]byte, 16)) {
			return true
		}
	}
	switch kind {
	case 1:
		return length != 74
	case 2:
		return length != 75
	case 3:
		return length < 108
	case 4:
		return length < 168
	case 5:
		return length != 18
	case 6:
		return length == 0 || offset > math.MaxUint64-uint64(length)
	case 7, 8:
		return length != 0
	case 9:
		return length != 35
	default:
		return true
	}
}
