package xferimage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.1–§8.3 / T53
func FuzzTransferTrackerState(f *testing.F) {
	for _, tier := range []string{"a", "b"} {
		encoded, err := EncodeTracker(trackerFixture(tier, "push"))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
		f.Add(encoded[:trackerHeaderBytes])
		f.Add(encoded[:len(encoded)-1])
	}
	// Seed the real canonical code/unknown shape and empty inflight primitive,
	// so valid administrative decisions remain closed under repeated encoding.
	// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
	abandoned, err := EncodeTracker(trackerAbandonmentFixture(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(abandoned)
	f.Add([]byte{})
	f.Add(trackerFrame([]byte(`{"Schema":"xfer-state-v1","Entries":[{}]}`)))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		state, err := DecodeTracker(encoded)
		if err != nil {
			if !reflect.DeepEqual(state, xfercontract.TrackerState{}) {
				t.Fatal("rejected image leaked partially imported responsibility")
			}
			return
		}
		canonical, err := EncodeTracker(state)
		if err != nil || !bytes.Equal(encoded, canonical) {
			t.Fatalf("accepted image was not closed under its actual encoder: %v", err)
		}
		// Repeat from the current decoded state. Imports never mutate deadlines,
		// phases, grants, revision or pending publication bytes to normalize them.
		again, err := DecodeTracker(canonical)
		if err != nil || !reflect.DeepEqual(state, again) {
			t.Fatalf("second current-state cut changed retained responsibility: %v", err)
		}
	})
}
