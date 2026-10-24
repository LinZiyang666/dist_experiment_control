package flowstate

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// The literal fixture pins the original layout: changing integer widths,
// field ordering, bool encoding or sorted file names must break byte equality.
// origin: hot-update S6 plan §2.2 / T24
func TestCutCodecPreservesDomainEnvelopesAndBounds(t *testing.T) {
	cut := sealedstate.CutState{
		Records: []sealedstate.Record{{Kind: "record", Fields: []sealedstate.Field{
			{Name: "number", Type: sealedstate.FieldU64, U64: 7},
			{Name: "cursor", Type: sealedstate.FieldNonnegativeI64, U64: 9},
			{Name: "fired", Type: sealedstate.FieldBool, U64: 1},
			{Name: "bytes", Type: sealedstate.FieldBytes, Bytes: []byte{0xab, 0xcd}},
		}}},
		Resources: []handoff.Resource{{ID: "resource", Kind: "flow-v1", Files: map[string]*os.File{"replay": nil, "raw": nil}}},
	}
	// Counts, names and numeric values use u64 big endian; the type and bool
	// use one byte. The resource directory carries names without live files.
	payload, err := hex.DecodeString(
		"0000000000000001" +
			"00000000000000067265636f7264" + "0000000000000004" +
			"00000000000000066e756d626572010000000000000007" +
			"0000000000000006637572736f72020000000000000009" +
			"000000000000000566697265640301" +
			"00000000000000056279746573040000000000000002abcd" +
			"0000000000000001" + "00000000000000087265736f75726365" +
			"0000000000000007666c6f772d7631" + "0000000000000002" +
			"0000000000000003726177" + "00000000000000067265706c6179")
	if err != nil {
		t.Fatal(err)
	}
	for _, magic := range []string{"tether-client-cut\x01", "tether-broker-tunnel-cut\x01"} {
		t.Run(magic[:len(magic)-1], func(t *testing.T) {
			want := append([]byte(magic), payload...)
			got, err := encodeCutState(cut, magic)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("private envelope changed: err=%v", err)
			}
			decoded, err := decodeCutState(want, magic, 1, 1)
			if err != nil || !reflect.DeepEqual(decoded, cut) {
				t.Fatalf("private state changed: err=%v", err)
			}
			for _, limits := range [][2]uint64{{0, 1}, {1, 0}} {
				if _, err := decodeCutState(want, magic, limits[0], limits[1]); !errors.Is(err, sealedstate.ErrInvalid) {
					t.Fatalf("domain capacity %v was ignored: %v", limits, err)
				}
			}
			if _, err := decodeCutState(append(bytes.Clone(want), 0), magic, 1, 1); !errors.Is(err, sealedstate.ErrInvalid) {
				t.Fatalf("trailing bytes accepted: %v", err)
			}
			if _, err := decodeCutState(want, "foreign", 1, 1); !errors.Is(err, sealedstate.ErrInvalid) {
				t.Fatalf("foreign domain accepted: %v", err)
			}
			for n := 0; n < len(want); n++ {
				if _, err := decodeCutState(want[:n], magic, 1, 1); !errors.Is(err, sealedstate.ErrInvalid) {
					t.Fatalf("truncation %d accepted: %v", n, err)
				}
			}
		})
	}
}
