package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferreceive"
)

// TestInlineReaderFreezeAgreesWithTheReceiverCheckpoint runs the REAL receiver
// over the Tier A inline reader and the real atomic sink, and cuts it (Freeze)
// before the first byte and, on a second receiver, after every byte is synced.
// The receiver refuses a cut whose reader snapshot disagrees with its own
// checkpoint ("reader and checkpoint prefixes disagree"); an inline reader that
// reported an empty snapshot made every hot-update cut of a Tier A receive
// impossible once a byte had landed.
// origin: hot-update S7 plan §8.1 / §8.2 / INTEG-D Tier A inline receiver
func TestInlineReaderFreezeAgreesWithTheReceiverCheckpoint(t *testing.T) {
	payload := []byte("inline bytes that a hot-update cut must be able to freeze")
	d, err := controlbudget.After(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	newReceiver := func(name string) xferreceive.Receiver {
		t.Helper()
		vp, err := transferpath.ValidateForWrite(filepath.Join(t.TempDir(), name), nil, transferpath.ModeOpen)
		if err != nil {
			t.Fatal(err)
		}
		sink, err := transferpath.NewAtomicReceiver(vp, "inline-"+name, false)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(payload)
		now := time.Now().UTC()
		r, err := xferreceive.New(x.ReceiveConfig{
			Base: x.Base{Clock: func() (string, int64, error) { n, err := controlbudget.Now(); return "boot", n, err }, UTC: time.Now},
			Identity: x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "inline-" + name,
				EntryNonce: "entry", IntentDigest: hex.EncodeToString(sum[:])},
			Budget: x.BudgetSeal{StartedAt: now, Size: int64(len(payload)), EffectiveBudgetNS: int64(time.Minute), RemainingNS: int64(time.Minute), SourceSealed: true},
			Reader: newInlineReader(payload), Atomic: sink,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Stop(d) })
		return r
	}
	fresh, err := newReceiver("fresh").Freeze(d)
	if err != nil {
		t.Fatalf("a cut before the first inline byte was refused: %v", err)
	}
	if !xferreceive.ValidInlineState(fresh, payload) {
		t.Fatalf("frozen inline receiver before its first byte is not the image's inline shape: %+v", fresh.Reader)
	}
	r := newReceiver("synced")
	for step := 0; step < 4; step++ {
		if err := r.Step(d); err != nil {
			break // awaiting the Grant: every inline byte is synced
		}
	}
	state, err := r.Freeze(d)
	if err != nil {
		t.Fatalf("a cut after the inline bytes were synced was refused: %v", err)
	}
	// The reader's snapshot agreed (else Freeze refused); the export itself is
	// the one reader-free inline shape the worker image accepts, whose durable
	// prefix is the whole payload.
	// origin: S7 FIX-TIERA2 — the agreeing reader snapshot was refused by the image
	if state.Checkpoint.DurableOffset != int64(len(payload)) || !reflect.DeepEqual(state.Reader, x.ReaderState{}) || !xferreceive.ValidInlineState(state, payload) {
		t.Fatalf("frozen inline progress: receiver=%d reader=%+v, want %d and no reader", state.Checkpoint.DurableOffset, state.Reader, len(payload))
	}
}
