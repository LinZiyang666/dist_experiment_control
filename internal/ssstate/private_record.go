package ssstate

import "github.com/LinZiyang666/tether/internal/sealedstate"

// The runtime record is the private codec root, not a shadow export struct.
// Every cursor is committed directly to this root before an IO turn can end.
// origin: hot-update S5 plan §6.1 / §12.1 / T17
type privateRecord struct {
	record sealedstate.Record
	fields map[string]*sealedstate.Field
}

func newPrivateRecord(kind string) privateRecord {
	r := sealedstate.Record{Kind: kind}
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind == kind {
			for _, f := range d.Fields {
				field := sealedstate.Field{Name: f.Name, Type: f.Type}
				if f.Type >= sealedstate.FieldBytes {
					field.Bytes = make([]byte, f.Width)
				}
				r.Fields = append(r.Fields, field)
			}
		}
	}
	return wrapPrivateRecord(r)
}
func wrapPrivateRecord(r sealedstate.Record) privateRecord {
	p := privateRecord{record: r, fields: make(map[string]*sealedstate.Field)}
	for i := range p.record.Fields {
		f := &p.record.Fields[i]
		p.fields[f.Name] = f
	}
	return p
}
func (p privateRecord) number(k string) uint64      { return p.fields[k].U64 }
func (p privateRecord) bytes(k string) []byte       { return p.fields[k].Bytes }
func (p privateRecord) flag(k string) bool          { return p.number(k) != 0 }
func (p privateRecord) put(k string, v uint64)      { p.fields[k].U64 = v }
func (p privateRecord) putBytes(k string, v []byte) { p.fields[k].Bytes = append([]byte(nil), v...) }
func (p privateRecord) clone() sealedstate.Record {
	r := sealedstate.Record{Kind: p.record.Kind, Fields: make([]sealedstate.Field, len(p.record.Fields))}
	for i, f := range p.record.Fields {
		r.Fields[i] = f
		r.Fields[i].Bytes = append([]byte(nil), f.Bytes...)
	}
	return r
}
