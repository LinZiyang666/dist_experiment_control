package xferimage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.3 / T53
func FuzzTransferWorkerState(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("TETHXW7\x00"))
	for _, mode := range []string{"empty", "prepared", "prepared-receiver", "prepared-source", "sender", "inline-sender", "receiver", "grant", "landed", "courier", "settled", "inline"} {
		b, err := EncodeWorker(workerFixture(mode))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	// Valid envelopes with invalid semantic projections reach all import
	// guards. Random checksum failures alone cannot exercise the state parser.
	invalid := workerFixture("receiver")
	invalid.Receivers[0].Atomic.Tmp = xfercontract.ResourceRef{}
	f.Add(rawWorkerImage(f, invalid))
	invalid = workerFixture("receiver")
	invalid.Receivers[0].Checkpoint.SHA256State[107] ^= 1
	f.Add(rawWorkerImage(f, invalid))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzz both the received envelope and a checksum-repaired envelope;
		// schema/version/count checks remain intact, while value bytes mutate.
		inputs := [][]byte{data}
		if len(data) >= workerHeaderBytes && len(data) <= MaxWorkerStateBytes {
			patched := bytes.Clone(data)
			repairWorkerDigest(patched)
			inputs = append(inputs, patched)
		}
		for _, input := range inputs {
			state, err := DecodeWorker(input)
			if err != nil {
				if !reflect.DeepEqual(state, xfercontract.WorkerState{}) {
					t.Fatal("refusal exposed partial private state")
				}
				continue
			}
			encoded, err := EncodeWorker(state)
			if err != nil || !bytes.Equal(encoded, input) {
				t.Fatal("accepted noncanonical worker image")
			}
			again, err := DecodeWorker(encoded)
			if err != nil || !reflect.DeepEqual(state, again) {
				t.Fatal("worker state failed complete roundtrip")
			}
			clear(encoded)
			if !reflect.DeepEqual(state, again) {
				t.Fatal("decoded bytes alias codec output")
			}
		}
	})
}
