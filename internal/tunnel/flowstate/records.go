package flowstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sort"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

func ClientPrivateRecord(kind string, values map[string]sealedstate.Field) (sealedstate.Record, error) {
	rec := sealedstate.Record{Kind: kind}
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind == rec.Kind {
			if len(values) != len(d.Fields) {
				return rec, ErrFlowState
			}
			for _, fd := range d.Fields {
				f, ok := values[fd.Name]
				if !ok {
					return rec, ErrFlowState
				}
				f.Name = fd.Name
				f.Type = fd.Type
				if fd.Type >= sealedstate.FieldBytes {
					if fd.Width != 0 && len(f.Bytes) != fd.Width || fd.Width == 0 && len(f.Bytes) > fd.MaxBytes {
						return rec, ErrFlowState
					}
					f.Bytes = append([]byte(nil), f.Bytes...)
				}
				rec.Fields = append(rec.Fields, f)
			}
			return rec, nil
		}
	}
	return rec, ErrFlowState
}

func EndpointNum(v uint64) sealedstate.Field { return sealedstate.Field{U64: v} }

func EndpointBytes(v []byte) sealedstate.Field { return sealedstate.Field{Bytes: v} }

func EndpointText(v string) sealedstate.Field { return EndpointBytes([]byte(v)) }

// clientCutDigest binds activation to the complete validated flow/client record
// set, including private route credentials. Records are descriptor ordered;
// sorting their encodings makes map iteration irrelevant across export/import.
func ClientCutDigest(cut sealedstate.CutState) ([32]byte, error) {
	records := make([][]byte, 0, len(cut.Records))
	for _, record := range cut.Records {
		b := binary.BigEndian.AppendUint16(nil, uint16(len(record.Kind)))
		b = append(b, record.Kind...)
		for _, field := range record.Fields {
			b = binary.BigEndian.AppendUint16(b, uint16(len(field.Name)))
			b = append(b, field.Name...)
			b = append(b, field.Type)
			b = binary.BigEndian.AppendUint64(b, field.U64)
			b = binary.BigEndian.AppendUint32(b, uint32(len(field.Bytes)))
			b = append(b, field.Bytes...)
		}
		records = append(records, b)
	}
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i], records[j]) < 0 })
	h := sha256.New()
	for _, b := range records {
		_ = binary.Write(h, binary.BigEndian, uint32(len(b)))
		_, _ = h.Write(b)
	}
	// Raw OFD identities are already in flow records. Hash every regular
	// resource as well, including retained replay bytes;
	// descriptor offsets stay untouched for the subsequent import transaction.
	resources := append([]handoff.Resource(nil), cut.Resources...)
	sort.Slice(resources, func(i, j int) bool { return resources[i].ID < resources[j].ID })
	for _, resource := range resources {
		var names []string
		for name := range resource.Files {
			if name != "raw" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			file := resource.Files[name]
			if file == nil {
				return [32]byte{}, ErrFlowState
			}
			info, err := file.Stat()
			if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > sealedstate.MaxStateBytes {
				return [32]byte{}, ErrFlowState
			}
			for _, text := range []string{resource.ID, resource.Kind, name} {
				_ = binary.Write(h, binary.BigEndian, uint32(len(text)))
				_, _ = h.Write([]byte(text))
			}
			_ = binary.Write(h, binary.BigEndian, uint64(info.Size()))
			if _, err = io.Copy(h, io.NewSectionReader(file, 0, info.Size())); err != nil {
				return [32]byte{}, err
			}
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
