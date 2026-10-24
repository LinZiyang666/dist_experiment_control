package flowstate

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/sealedstate"
)

func ServerAllocationKey(k Allocation) string {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, uint64(k.Port))
	for _, s := range []string{k.SID, k.NID, k.TokenHash} {
		_ = binary.Write(&b, binary.BigEndian, uint32(len(s)))
		b.WriteString(s)
	}
	return b.String()
}

func ServerAllocationRead(key string) (Allocation, error) {
	r := bytes.NewReader([]byte(key))
	var port uint64
	out := Allocation{}
	if binary.Read(r, binary.BigEndian, &port) != nil || port == 0 || port > math.MaxUint16 {
		return out, sealedstate.ErrInvalid
	}
	out.Port = int(port)
	for _, s := range []*string{&out.SID, &out.NID, &out.TokenHash} {
		var size uint32
		if binary.Read(r, binary.BigEndian, &size) != nil || size > 4096 || uint64(size) > uint64(r.Len()) {
			return out, sealedstate.ErrInvalid
		}
		p := make([]byte, size)
		_, _ = io.ReadFull(r, p)
		*s = string(p)
		if !utf8.Valid(p) {
			return out, sealedstate.ErrInvalid
		}
	}
	if r.Len() != 0 {
		return out, sealedstate.ErrInvalid
	}
	return out, nil
}
