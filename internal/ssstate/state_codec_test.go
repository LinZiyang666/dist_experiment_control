package ssstate

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// The golden was packed independently with Python struct, not encodeState.
// Every frozen field carries a distinctive decision/value, including both nonce
// directions, FIFO history, consumed SO_ERROR and pending byte suffixes.
// Names are wire IDs, so the 257 -> 264 endpoint schema change leaves every
// other record byte-identical. Only endpoint fixture indices shift; the golden
// was independently decoded and repacked after accounting for those differences.
// origin: hot-update S5 plan §6.1 / §6.3 / §12.1 / §8 T23
func TestSSPrivateCodecGoldenAndRequiredFields(t *testing.T) {

	records := codecVector()
	golden := codecGolden(t)
	encoded, err := encodeState(records)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, golden) {
		t.Fatal("private state differs from independent v1 golden")
	}
	decoded, err := decodeState(golden)
	if err != nil {
		t.Fatal(err)
	}
	assertCodecRecords(t, records, decoded)
	total := 0
	for _, record := range records {
		total += len(record.Fields)
		for i, field := range record.Fields {
			t.Run("required/"+record.Kind+"/"+field.Name, func(t *testing.T) {
				broken := cloneCodecRecords([]sealedstate.Record{record})
				broken[0].Fields = append(broken[0].Fields[:i], broken[0].Fields[i+1:]...)
				if out, err := encodeState(broken); !errors.Is(err, sealedstate.ErrInvalid) || out != nil {
					t.Fatal("encoder accepted missing required field")
				}
				if out, err := decodeState(referenceStateBytes(broken)); !errors.Is(err, sealedstate.ErrInvalid) || out != nil {
					t.Fatal("decoder accepted missing required field")
				}
			})
		}
	}
	if len(records) != 12 || total != 264 {
		t.Fatal("frozen field vector lost coverage")
	}
	t.Run("canonical-fields-and-owned-buffers", func(t *testing.T) {
		reversed := cloneCodecRecords(records)
		for i := range reversed {
			fields := reversed[i].Fields
			for l, r := 0, len(fields)-1; l < r; l, r = l+1, r-1 {
				fields[l], fields[r] = fields[r], fields[l]
			}
		}
		out, err := encodeState(reversed)
		if err != nil || !bytes.Equal(out, golden) {
			t.Fatal("field order changed canonical encoding")
		}
		if reversed[0].Fields[0].Name != records[0].Fields[len(records[0].Fields)-1].Name {
			t.Fatal("encoder mutated snapshot")
		}
		input := append([]byte(nil), golden...)
		decoded, err := decodeState(input)
		if err != nil {
			t.Fatal(err)
		}
		clear(input)
		assertCodecRecords(t, records, decoded)
		clear(out)
		assertCodecRecords(t, records, decoded)
	})
	t.Run("malformed-envelope-and-truncation", func(t *testing.T) {
		for n := 0; n < len(golden); n++ {
			if out, err := decodeState(golden[:n]); err == nil || out != nil {
				t.Fatalf("decoder accepted truncated state at byte %d", n)
			}
		}
		for _, mutate := range []func([]byte){
			func(b []byte) { b[0] = 'X' }, func(b []byte) { b[5] = 2 }, func(b []byte) { b[7] = 1 },
			func(b []byte) { binary.BigEndian.PutUint32(b[8:12], math.MaxUint32) },
			func(b []byte) { binary.BigEndian.PutUint32(b[8:12], 0) },
			func(b []byte) { binary.BigEndian.PutUint16(b[12:14], math.MaxUint16) },
		} {
			corrupt := append([]byte(nil), golden...)
			mutate(corrupt)
			if out, err := decodeState(corrupt); err == nil || out != nil {
				t.Fatal("decoder accepted malformed envelope")
			}
		}
		if out, err := decodeState(append(append([]byte(nil), golden...), 0)); err == nil || out != nil {
			t.Fatal("decoder accepted trailing bytes")
		}
		empty, err := encodeState(nil)
		if err != nil || !bytes.Equal(empty, []byte{'T', 'S', 'S', 'S', 0, 1, 0, 0, 0, 0, 0, 0}) {
			t.Fatal("invalid empty-state envelope")
		}
		if out, err := decodeState(empty); err != nil || len(out) != 0 {
			t.Fatal("empty state did not decode")
		}
	})
	t.Run("unknown-duplicate-and-type", func(t *testing.T) {
		for _, mutate := range []func([]sealedstate.Record){
			func(r []sealedstate.Record) { r[0].Kind = "Authority" },
			func(r []sealedstate.Record) { r[0].Kind = "unknown" },
			func(r []sealedstate.Record) { r[0].Fields[0].Name = "Allocation_epoch" },
			func(r []sealedstate.Record) { r[0].Fields[0].Name = "unknown_field" },
			func(r []sealedstate.Record) { r[0].Fields = append(r[0].Fields, r[0].Fields[0]) },
			// Same field count: a duplicate stands in for a different required field.
			func(r []sealedstate.Record) { r[0].Fields[1] = r[0].Fields[0] },
			func(r []sealedstate.Record) { r[0].Fields[0].Type = 255 },
			func(r []sealedstate.Record) { r[0].Fields[0].Type = sealedstate.FieldU64 },
		} {
			broken := cloneCodecRecords(records)
			mutate(broken)
			assertCodecRejects(t, broken, sealedstate.ErrInvalid)
		}
		broken := append(cloneCodecRecords(records), cloneCodecRecords(records[:1])...)
		assertCodecRejects(t, broken, sealedstate.ErrInvalid)
		broken[len(broken)-1].Fields[11].Bytes[0] ^= 0xff // a different authority ID is legal
		if _, err := encodeState(broken); err != nil {
			t.Fatal("distinct record identity rejected")
		}
	})
	t.Run("width-signed-bool-utf8-and-enum", func(t *testing.T) {
		for ri, record := range records {
			for fi, field := range record.Fields {
				t.Run(record.Kind+"/"+field.Name, func(t *testing.T) {
					broken := cloneCodecRecords(records)
					f := &broken[ri].Fields[fi]
					switch field.Type {
					case sealedstate.FieldU64:
						f.Bytes = []byte{1}
					case sealedstate.FieldNonnegativeI64:
						f.U64 = uint64(math.MaxInt64) + 1
					case sealedstate.FieldBool:
						f.U64 = 2
					case sealedstate.FieldUTF8:
						f.Bytes = []byte{0xff}
					default:
						f.U64 = 1
					}
					if out, err := encodeState(broken); !errors.Is(err, sealedstate.ErrInvalid) || out != nil {
						t.Fatal("encoder accepted invalid primitive representation")
					}
					if field.Type == sealedstate.FieldNonnegativeI64 || field.Type == sealedstate.FieldBool || field.Type == sealedstate.FieldUTF8 {
						if out, err := decodeState(referenceStateBytes(broken)); !errors.Is(err, sealedstate.ErrInvalid) || out != nil {
							t.Fatal("decoder accepted invalid primitive value")
						}
					}
					if field.Type >= sealedstate.FieldBytes && len(field.Bytes) > 3 && field.Type != sealedstate.FieldUTF8 {
						broken = cloneCodecRecords(records)
						broken[ri].Fields[fi].Bytes = field.Bytes[:len(field.Bytes)-1]
						assertCodecRejects(t, broken, sealedstate.ErrInvalid)
					}
					descriptor, _ := stateFieldDescriptor(sealedstate.RecordDescriptors[ri], field.Name)
					if descriptor.Width == 0 && descriptor.MaxBytes < sealedstate.MaxStateBytes && field.Type >= sealedstate.FieldBytes {
						broken = cloneCodecRecords(records)
						broken[ri].Fields[fi].Bytes = bytes.Repeat([]byte{'v'}, descriptor.MaxBytes+1)
						assertCodecRejects(t, broken, sealedstate.ErrCapacity)
					}
					if field.Name == "phase" || field.Name == "authority_phase" || field.Name == "verdict" || field.Name == "purpose" {
						broken = cloneCodecRecords(records)
						broken[ri].Fields[fi].U64 = 0
						assertCodecRejects(t, broken, sealedstate.ErrInvalid)
					}
					if codecEnumName(field.Name) {
						broken = cloneCodecRecords(records)
						broken[ri].Fields[fi].U64 = math.MaxUint64
						assertCodecRejects(t, broken, sealedstate.ErrInvalid)
					}
				})
			}
		}
		// A declared u32 length must not allocate before the remaining-byte check.
		corrupt := append([]byte(nil), golden...)
		at := bytes.Index(corrupt, []byte("allocation_epoch")) + len("allocation_epoch") + 1
		binary.BigEndian.PutUint32(corrupt[at:at+4], math.MaxUint32)
		if _, err := decodeState(corrupt); err == nil {
			t.Fatal("decoder accepted overflowing field length")
		}
	})
	t.Run("bounded-state", func(t *testing.T) {
		record := codecRecord(records, sealedstate.KindMaterial)
		broken := cloneCodecRecords([]sealedstate.Record{record})
		for i := range broken[0].Fields {
			if broken[0].Fields[i].Name == "master" {
				broken[0].Fields[i].Bytes = make([]byte, sealedstate.MaxStateBytes)
			}
		}
		if out, err := encodeState(broken); !errors.Is(err, sealedstate.ErrCapacity) || out != nil {
			t.Fatal("encoder accepted aggregate overflow")
		}
		if out, err := decodeState(make([]byte, sealedstate.MaxStateBytes+1)); !errors.Is(err, sealedstate.ErrCapacity) || out != nil {
			t.Fatal("decoder accepted oversized input")
		}
		for i := range broken[0].Fields {
			if broken[0].Fields[i].Name == "master" {
				broken[0].Fields[i].Bytes = broken[0].Fields[i].Bytes[:sealedstate.MaxStateBytes-len(referenceStateBytes([]sealedstate.Record{record}))+3]
			}
		}
		exact, err := encodeState(broken)
		if err != nil || len(exact) != sealedstate.MaxStateBytes {
			t.Fatal("codec refused exact state capacity")
		}
		decoded, err := decodeState(exact)
		if err != nil {
			t.Fatal(err)
		}
		assertCodecRecords(t, broken, decoded)
	})
}

// Values match the independently packed fixture formula; no production encode or
// decode helper participates in generating expected wire bytes.
func codecVector() []sealedstate.Record {
	var records []sealedstate.Record
	for ki, d := range sealedstate.RecordDescriptors {
		record := sealedstate.Record{Kind: d.Kind}
		for fi, fd := range d.Fields {
			field := sealedstate.Field{Name: fd.Name, Type: fd.Type}
			switch fd.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64:
				field.U64 = uint64(fi + 1)
				if codecEnumName(fd.Name) {
					field.U64 = 1
				}
			case sealedstate.FieldBool:
				field.U64 = uint64(fi % 2)
			case sealedstate.FieldUTF8:
				field.Bytes = fmt.Appendf(nil, "v%d.%d", ki, fi)
			default:
				n := fd.Width
				if n == 0 {
					n = 3
				}
				field.Bytes = make([]byte, n)
				for j := range field.Bytes {
					field.Bytes[j] = byte(ki*17 + fi + j)
				}
			}
			record.Fields = append(record.Fields, field)
		}
		records = append(records, record)
	}
	return records
}

func codecEnumName(name string) bool {
	switch name {
	case "phase", "authority_phase", "connect_state", "close_mode", "open_status", "verdict", "purpose", "reason", "open_reason", "close_reason", "fin_flags":
		return true
	default:
		return false
	}
}

func codecGolden(t testing.TB) []byte {
	t.Helper()
	text, err := os.ReadFile("testdata/ss-state-v1-golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	var encoded strings.Builder
	for _, line := range strings.Split(string(text), "\n") {
		if !strings.HasPrefix(line, "#") {
			encoded.WriteString(strings.TrimSpace(line))
		}
	}
	data, err := hex.DecodeString(encoded.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 7265 {
		t.Fatal("independent golden lost private fields")
	}
	return data
}

func cloneCodecRecords(records []sealedstate.Record) []sealedstate.Record {
	out := append([]sealedstate.Record(nil), records...)
	for i := range out {
		out[i].Fields = append([]sealedstate.Field(nil), out[i].Fields...)
		for j := range out[i].Fields {
			out[i].Fields[j].Bytes = append([]byte(nil), out[i].Fields[j].Bytes...)
		}
	}
	return out
}

func codecRecord(records []sealedstate.Record, kind string) sealedstate.Record {
	for _, record := range records {
		if record.Kind == kind {
			return record
		}
	}
	panic("fixture record missing")
}

func assertCodecRecords(t testing.TB, want, got []sealedstate.Record) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatal("codec lost records")
	}
	for i, record := range want {
		if record.Kind != got[i].Kind || len(record.Fields) != len(got[i].Fields) {
			t.Fatal("codec lost required field set")
		}
		for j, f := range record.Fields {
			g := got[i].Fields[j]
			if f.Name != g.Name || f.Type != g.Type || f.U64 != g.U64 || !bytes.Equal(f.Bytes, g.Bytes) {
				t.Fatalf("codec changed private decision %s/%s", record.Kind, f.Name)
			}
		}
	}
}

func assertCodecRejects(t testing.TB, records []sealedstate.Record, want error) {
	t.Helper()
	if out, err := encodeState(records); !errors.Is(err, want) || out != nil {
		t.Fatalf("encoder did not reject malformed state with %v", want)
	}
	if out, err := decodeState(referenceStateBytes(records)); !errors.Is(err, want) || out != nil {
		t.Fatalf("decoder did not reject malformed state with %v", want)
	}
}

// Deliberately permissive independent packer: it can emit duplicates, unknown
// names/types and omissions that encodeState correctly refuses to manufacture.
func referenceStateBytes(records []sealedstate.Record) []byte {
	var b bytes.Buffer
	b.WriteString("TSSS")
	_ = binary.Write(&b, binary.BigEndian, uint16(1))
	_ = binary.Write(&b, binary.BigEndian, uint16(0))
	_ = binary.Write(&b, binary.BigEndian, uint32(len(records)))
	name := func(s string) { _ = binary.Write(&b, binary.BigEndian, uint16(len(s))); b.WriteString(s) }
	for _, record := range records {
		name(record.Kind)
		_ = binary.Write(&b, binary.BigEndian, uint16(len(record.Fields)))
		for _, field := range record.Fields {
			name(field.Name)
			b.WriteByte(field.Type)
			var value []byte
			switch field.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64:
				value = make([]byte, 8)
				binary.BigEndian.PutUint64(value, field.U64)
			case sealedstate.FieldBool:
				value = []byte{byte(field.U64)}
			default:
				value = field.Bytes
			}
			_ = binary.Write(&b, binary.BigEndian, uint32(len(value)))
			b.Write(value)
		}
	}
	return b.Bytes()
}
