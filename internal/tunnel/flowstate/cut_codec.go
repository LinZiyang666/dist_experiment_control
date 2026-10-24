package flowstate

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// Client and broker envelopes share their binary layout, while each caller
// retains its own magic, capacity bounds and private-record validation.
// origin: hot-update S6 plan §2.2 / T24
func encodeCutState(cut sealedstate.CutState, magic string) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(magic)
	put := func(v uint64) { var raw [8]byte; binary.BigEndian.PutUint64(raw[:], v); b.Write(raw[:]) }
	blob := func(p []byte) { put(uint64(len(p))); b.Write(p) }
	put(uint64(len(cut.Records)))
	for _, rec := range cut.Records {
		blob([]byte(rec.Kind))
		put(uint64(len(rec.Fields)))
		for _, f := range rec.Fields {
			blob([]byte(f.Name))
			b.WriteByte(f.Type)
			if f.Type == sealedstate.FieldBool {
				b.WriteByte(byte(f.U64))
			} else if f.Type < sealedstate.FieldBool {
				put(f.U64)
			} else {
				blob(f.Bytes)
			}
			if b.Len() > sealedstate.MaxStateBytes {
				return nil, sealedstate.ErrCapacity
			}
		}
	}
	put(uint64(len(cut.Resources)))
	for _, res := range cut.Resources {
		blob([]byte(res.ID))
		blob([]byte(res.Kind))
		names := ClientFileNames(res)
		put(uint64(len(names)))
		for _, name := range names {
			blob([]byte(name))
		}
	}
	if b.Len() > sealedstate.MaxStateBytes {
		return nil, sealedstate.ErrCapacity
	}
	return b.Bytes(), nil
}

// Decode only the shared layout. Domain validation follows in each caller so
// broker-only kinds cannot enter the agent's closed private-state vocabulary.
// origin: hot-update S6 plan §2.2 / T24
func decodeCutState(data []byte, magic string, maxRecords, maxResources uint64) (sealedstate.CutState, error) {
	var cut sealedstate.CutState
	if len(data) > sealedstate.MaxStateBytes || len(data) < len(magic) || string(data[:len(magic)]) != magic {
		return cut, sealedstate.ErrInvalid
	}
	r := bytes.NewReader(data[len(magic):])
	read := func() (uint64, error) {
		var p [8]byte
		_, err := io.ReadFull(r, p[:])
		return binary.BigEndian.Uint64(p[:]), err
	}
	blob := func() ([]byte, error) {
		n, err := read()
		if err != nil || n > uint64(r.Len()) {
			return nil, sealedstate.ErrInvalid
		}
		var p []byte
		if n != 0 {
			p = make([]byte, int(n))
		}
		_, err = io.ReadFull(r, p)
		return p, err
	}
	count, err := read()
	if err != nil || count > maxRecords {
		return cut, sealedstate.ErrInvalid
	}
	for range count {
		kind, err := blob()
		if err != nil {
			return sealedstate.CutState{}, sealedstate.ErrInvalid
		}
		n, err := read()
		if err != nil || n > 256 {
			return sealedstate.CutState{}, sealedstate.ErrInvalid
		}
		rec := sealedstate.Record{Kind: string(kind)}
		for range n {
			name, err := blob()
			if err != nil {
				return sealedstate.CutState{}, sealedstate.ErrInvalid
			}
			typ, err := r.ReadByte()
			if err != nil || typ < sealedstate.FieldU64 || typ > sealedstate.FieldSalt32 {
				return sealedstate.CutState{}, sealedstate.ErrInvalid
			}
			f := sealedstate.Field{Name: string(name), Type: typ}
			if typ == sealedstate.FieldBool {
				var value byte
				value, err = r.ReadByte()
				f.U64 = uint64(value)
			} else if typ < sealedstate.FieldBool {
				f.U64, err = read()
			} else {
				f.Bytes, err = blob()
			}
			if err != nil {
				return sealedstate.CutState{}, sealedstate.ErrInvalid
			}
			rec.Fields = append(rec.Fields, f)
		}
		cut.Records = append(cut.Records, rec)
	}
	count, err = read()
	if err != nil || count > maxResources {
		return sealedstate.CutState{}, sealedstate.ErrInvalid
	}
	for range count {
		id, err := blob()
		if err != nil {
			return sealedstate.CutState{}, sealedstate.ErrInvalid
		}
		kind, err := blob()
		if err != nil {
			return sealedstate.CutState{}, sealedstate.ErrInvalid
		}
		n, err := read()
		if err != nil || n == 0 || n > 2 {
			return sealedstate.CutState{}, sealedstate.ErrInvalid
		}
		res := handoff.Resource{ID: string(id), Kind: string(kind), Files: make(map[string]*os.File)}
		for range n {
			name, err := blob()
			if err != nil {
				return sealedstate.CutState{}, sealedstate.ErrInvalid
			}
			if _, exists := res.Files[string(name)]; exists {
				return sealedstate.CutState{}, sealedstate.ErrInvalid
			}
			res.Files[string(name)] = nil
		}
		cut.Resources = append(cut.Resources, res)
	}
	if r.Len() != 0 {
		return sealedstate.CutState{}, sealedstate.ErrInvalid
	}
	return cut, nil
}
