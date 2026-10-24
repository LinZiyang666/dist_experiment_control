package ssproxy

import (
	"bytes"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// Real: frozen signatures and owner sentinels. Stubbed: all S5 IO/state owners.
// Does not exercise sockets, goroutines, image codecs or carriers.
// P8 runtime rows are covered by real actor/fence/revoke tests after replacement.
// origin: hot-update S5 plan §2 / §12.2 P0
func TestSSImageContractStubsRejectOperations(t *testing.T) {
	f := &Frozen{}
	imageRejection := error(ErrStateUnrepresentable)
	if runtime.GOOS != "linux" {
		imageRejection = sealedstate.ErrUnsupported
	}
	cases := []struct {
		name string
		want error
		call func() []any
	}{
		{"Export", imageRejection, func() []any { a, b, c := f.Export(); return []any{a, b, c} }},
		{"Restore", imageRejection, func() []any { a, b, c := Restore(Image{}, nil); return []any{a, b, c} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := tc.call()
			err, ok := values[len(values)-1].(error)
			if !ok || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.want.Error()) {
				t.Fatalf("stub returned %v instead of owner sentinel %v", values[len(values)-1], tc.want)
			}
			for i, value := range values[:len(values)-1] {
				if value != nil && !reflect.ValueOf(value).IsZero() {
					t.Errorf("stub result %d grants ownership or progress: %T", i, value)
				}
			}
		})
	}
}

// These are compile checks for the private cross-owner hooks too.
// origin: hot-update S5 plan §12.1
var (
	_ func(sealedstate.CutState) (*Server, []handoff.Resource, error) = restoreCutState
	_ func([]sealedstate.Record) ([]byte, error)                      = encodeState
	_ func([]byte) ([]sealedstate.Record, error)                      = decodeState
)

// The round trip deliberately calls the real codec hook. P0's failure stub makes
// this test pending P9; accepting ErrNotImplemented would fake codec validation.
// No descriptors, goroutines or secret values are supplied by this fixture.
// origin: hot-update S5 plan §12.1 / §12.2 P0
func TestSSPrivateRecordBinaryRoundTripAndStrictRejection(t *testing.T) {
	records := make([]sealedstate.Record, 0, len(sealedstate.RecordDescriptors))
	for i, descriptor := range sealedstate.RecordDescriptors {
		record := sealedstate.Record{Kind: descriptor.Kind}
		for _, descriptor := range descriptor.Fields {
			field := sealedstate.Field{Name: descriptor.Name, Type: descriptor.Type}
			switch descriptor.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64, sealedstate.FieldBool:
			case sealedstate.FieldBytes, sealedstate.FieldUTF8, sealedstate.FieldID16, sealedstate.FieldNonce12, sealedstate.FieldSalt32:
				field.Bytes = make([]byte, descriptor.Width)
			}
			if descriptor.Name == "id" {
				field.Bytes[0] = byte(i + 1)
			}
			if descriptor.Name == "phase" || descriptor.Name == "authority_phase" || descriptor.Name == "connect_state" || descriptor.Name == "purpose" || descriptor.Name == "verdict" {
				field.U64 = 1
			}
			record.Fields = append(record.Fields, field)
		}
		records = append(records, record)
	}
	encoded, err := encodeState(records)
	if err != nil {
		t.Fatalf("real private codec is required for round trip: %v", err)
	}
	if len(encoded) == 0 || len(encoded) > sealedstate.MaxStateBytes {
		t.Fatal("codec returned empty or unbounded state")
	}
	decoded, err := decodeState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != len(decoded) {
		t.Fatal("binary round trip lost private records")
	}
	for i, record := range records {
		if record.Kind != decoded[i].Kind || len(record.Fields) != len(decoded[i].Fields) {
			t.Fatal("binary round trip lost required private fields")
		}
		for j, field := range record.Fields {
			got := decoded[i].Fields[j]
			if field.Name != got.Name || field.Type != got.Type || field.U64 != got.U64 || !bytes.Equal(field.Bytes, got.Bytes) {
				t.Fatal("binary round trip lost a required private decision")
			}
		}
	}
	// Same-length substitutions alter a real encoded field/kind ID without
	// depending on the private codec's outer record-count envelope.
	for _, names := range [][2]string{{"rx_nonce", "xx_nonce"}, {"rx_nonce", "RX_NONCE"}, {"ss-actor", "zz-actor"}} {
		offset := bytes.Index(encoded, []byte(names[0]))
		if offset < 0 {
			t.Fatal("private binary codec did not emit its declared names")
		}
		corrupted := append([]byte{}, encoded...)
		copy(corrupted[offset:], []byte(names[1]))
		if _, err := decodeState(corrupted); err == nil {
			t.Fatal("binary codec accepted unknown/case alias field or kind")
		}
	}
	for _, data := range [][]byte{nil, encoded[:len(encoded)-1], append(append([]byte{}, encoded...), 0)} {
		if _, err := decodeState(data); err == nil {
			t.Fatal("truncated/trailing state accepted")
		}
	}
	for _, tc := range []struct{ kind, field string }{
		{"ss-actor", "rx_nonce"}, {"ss-actor", "tx_nonce"}, {"ss-actor", "tx_cipher_write_cursor"},
		{"ss-actor", "connect_so_error_consumed"}, {"salt-ring", "salts"},
		{"flow", "tx_pending"}, {"flow", "rx_pending"}, {"retirement", "retired_through"}, {"timer", "fired"},
	} {
		t.Run(tc.kind+"/"+tc.field, func(t *testing.T) {
			broken := cloneContractRecords(records)
			found := false
			for i := range broken {
				if broken[i].Kind != tc.kind {
					continue
				}
				for j, field := range broken[i].Fields {
					if field.Name != tc.field {
						continue
					}
					broken[i].Fields = append(broken[i].Fields[:j], broken[i].Fields[j+1:]...)
					found = true
					break
				}
			}
			if !found {
				t.Fatal("required codec mutation field missing from frozen schema")
			}
			if _, err := encodeState(broken); err == nil {
				t.Fatal("codec accepted a missing required field")
			}
		})
	}
	for _, mutate := range []func([]sealedstate.Record){
		func(r []sealedstate.Record) { r[0].Kind = "unknown-kind" },
		func(r []sealedstate.Record) { r[0].Fields[0].Name = "unknown_field" },
		func(r []sealedstate.Record) { r[0].Fields[0].Name = strings.ToUpper(r[0].Fields[0].Name) },
		func(r []sealedstate.Record) { r[0].Fields = append(r[0].Fields, r[0].Fields[0]) },
		func(r []sealedstate.Record) { r[0].Fields[0].Type = 255 },
	} {
		if _, err := encodeState(applyContractRecordMutation(records, mutate)); err == nil {
			t.Fatal("codec accepted unknown/duplicate/case alias/type")
		}
	}
}

func cloneContractRecords(records []sealedstate.Record) []sealedstate.Record {
	out := append([]sealedstate.Record{}, records...)
	for i := range out {
		out[i].Fields = append([]sealedstate.Field{}, out[i].Fields...)
	}
	return out
}

func applyContractRecordMutation(records []sealedstate.Record, mutate func([]sealedstate.Record)) []sealedstate.Record {
	out := cloneContractRecords(records)
	mutate(out)
	return out
}
