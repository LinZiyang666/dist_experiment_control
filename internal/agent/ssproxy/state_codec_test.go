package ssproxy

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The real trial parser lifecycle remains with its actor owner after moving
// the pure codec checks; the same five exercises and leak assertions stay intact.
// origin: hot-update S5 plan §6.1 / §8 T24
func TestSSTrialParserLifecycle(t *testing.T) {
	beforeGo := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSTrialParser(t, ssFuzzHandshake(t, []byte{1, 127, 0, 0, 1, 0, 80}, 1), 1)
	}
	testharness.AssertNoGoroutineLeak(t, "SS trial parser lifecycle", beforeGo)
	testharness.AssertNoFDLeak(t, "SS trial parser lifecycle", beforeFD, 0)
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
	text, err := os.ReadFile("../../ssstate/testdata/ss-state-v1-golden.txt")
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
