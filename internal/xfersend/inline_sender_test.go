package xfersend

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"os"
	"runtime"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A Tier A pull has no object version or data attachment: the sealed copy is
// the transfer's bytes. It must be exactly the sealed source, survive a
// pathname replacement and an in-place rewrite after the copy (a replay never
// re-reads), cross Freeze/Import by value, and be released by Stop.
// origin: hot-update S7 plan §8.1–§8.2 / FIX-INLINE (FIX-TIERA report item 3)
func TestTransferInlineSenderRetainsExactSealedCopy(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInlineSenderCopy(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseInlineSenderCopy(t *testing.T) {
	t.Helper()
	// Larger than one bounded read, so the copy really spans ReadAt calls.
	data := bytes.Repeat([]byte("inline sealed copy\n"), 9000)
	f := newInlineSenderFixture(t, data)
	if _, copied, err := Inline(f.actor, f.deadline()); err == nil || copied != nil {
		t.Fatal("unsealed sender served an inline copy")
	}
	seal := requireSeal(t, f)
	if seal.Size != int64(len(data)) || seal.SHA256 != digest(data) || seal.Chunks != 0 || seal.ChunkSize != 0 {
		t.Fatalf("inline seal does not describe the source: %+v", seal)
	}
	if _, copied, err := Inline(f.actor, f.deadline()); err == nil || copied != nil {
		t.Fatal("sealed sender served bytes before its verified copy")
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if done, ok := f.actor.(interface{ Finalized() bool }); !ok || !done.Finalized() {
		t.Fatal("verified inline copy did not finish the sender")
	}
	got, first, err := Inline(f.actor, f.deadline())
	if err != nil || got != seal || !bytes.Equal(first, data) {
		t.Fatalf("inline copy differs from its seal: seal=%+v bytes=%d err=%v", got, len(first), err)
	}
	first[0] ^= 0xff
	original, err := f.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	rewriteSource(t, f, 0, bytes.Repeat([]byte("Z"), len(data)), original.ModTime())
	if err := os.Rename(f.path, f.path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatalf("finished inline sender re-read its source: %v", err)
	}
	if _, again, err := Inline(f.actor, f.deadline()); err != nil || !bytes.Equal(again, data) {
		t.Fatalf("replay did not serve the original copy: bytes=%d err=%v", len(again), err)
	}
	state, err := f.actor.Freeze(f.deadline())
	if err != nil || !bytes.Equal(state.Inline, data) || state.Offset != seal.Size || state.SourceSeal != seal || !state.Budget.SourceSealed {
		t.Fatalf("freeze lost the inline copy: bytes=%d offset=%d err=%v", len(state.Inline), state.Offset, err)
	}
	h := sha256.New()
	if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state.SHA256State); err != nil || hex.EncodeToString(h.Sum(nil)) != seal.SHA256 {
		t.Fatalf("exported hash state does not cover the whole copy: %v", err)
	}
	state.Inline[0] ^= 0xff
	if _, again, err := Inline(f.actor, f.deadline()); err != nil || !bytes.Equal(again, data) {
		t.Fatal("Freeze lent the sender's own replay bytes to its caller")
	}
	state.Inline[0] ^= 0xff
	importInlineSender(t, f, state, data)
	requireStop(t, f)
	if _, copied, err := Inline(f.actor, f.deadline()); err == nil || copied != nil {
		t.Fatal("stopped sender still serves its copy")
	}
}

// importInlineSender imports on a duplicate of the original OFD, exactly as the
// next image inherits it, while the source path names an unrelated file.
func importInlineSender(t *testing.T, f *senderFixture, state x.SenderState, data []byte) {
	t.Helper()
	dup, err := syscall.Dup(int(f.file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	source := os.NewFile(uintptr(dup), f.path)
	defer func() { _ = source.Close() }()
	// The importer configures the actor from the image, as xferagent does.
	config := f.config
	config.Source, config.Capacity, config.Budget = source, &senderCapacity{}, state.Budget
	for name, tamper := range map[string]func(*x.SenderState){
		"copy digest": func(s *x.SenderState) {
			s.Inline = append(bytes.Clone(s.Inline[:len(s.Inline)-1]), s.Inline[len(s.Inline)-1]^1)
		},
		"truncated copy":      func(s *x.SenderState) { s.Inline = bytes.Clone(s.Inline[:len(s.Inline)-1]) },
		"cursor without copy": func(s *x.SenderState) { s.Inline = nil },
		"copy without cursor": func(s *x.SenderState) { s.Offset-- },
	} {
		forged := state
		tamper(&forged)
		if actor, err := Import(config, forged); err == nil || actor != nil {
			t.Fatalf("%s: tampered inline image imported", name)
		}
		if _, err := source.Stat(); err != nil {
			t.Fatalf("%s: refused import consumed the inherited descriptor", name)
		}
	}
	withWriter := config
	withWriter.Writer = f.writer
	if actor, err := Import(withWriter, state); err == nil || actor != nil {
		t.Fatal("inline image imported with an object writer")
	}
	imported, err := Import(config, state)
	if err != nil {
		t.Fatal(err)
	}
	if done, ok := imported.(interface{ Finalized() bool }); !ok || !done.Finalized() {
		t.Fatal("imported inline copy lost its finished state")
	}
	if err := imported.Step(f.deadline()); err != nil {
		t.Fatalf("imported inline sender re-read its source: %v", err)
	}
	if got, copied, err := Inline(imported, f.deadline()); err != nil || got != state.SourceSeal || !bytes.Equal(copied, data) {
		t.Fatalf("import lost the original inline copy: bytes=%d err=%v", len(copied), err)
	}
	if err := imported.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if config.Capacity.(*senderCapacity).held {
		t.Fatal("imported sender kept its reservation after Stop")
	}
}
