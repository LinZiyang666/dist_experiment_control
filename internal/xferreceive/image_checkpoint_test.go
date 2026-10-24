package xferreceive

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The WRONLY primitive owner cannot hash the file. Its image must carry the
// receiver's independently verified synced digest, including the brief window
// after rename and before the courier has obtained a terminal ACK.
// origin: hot-update S7 plan §3 / §8.2 / T08 / T17; INTEG-M landed receiver cut
func TestTransferReceiverImageCarriesVerifiedPrimitiveDigest(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseReceiverPrimitiveDigest(t)
	}
	testharness.AssertNoGoroutineLeak(t, "receiver primitive image", g)
	testharness.AssertNoFDLeak(t, "receiver primitive image", fd, 0)
}

func exerciseReceiverPrimitiveDigest(t *testing.T) {
	t.Helper()
	f := newReceiverFixture(t, false)
	defer func() { _ = f.sink.file.Close() }()
	if err := f.actor.Reserve(testGrant(f.config.Identity)); err != nil {
		t.Fatal(err)
	}
	for range f.chunks {
		requireStep(t, f.actor)
	}
	if err := f.actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	state := requireFreeze(t, f.actor)
	if state.Outcome.Outcome != x.OutcomeLanded || state.Outcome.SHA256 == "" ||
		state.Atomic.PrimitiveOutcome.SHA256 != state.Outcome.SHA256 || state.Atomic.PrimitiveOutcome.Bytes != state.Outcome.Bytes {
		t.Fatal("landed image lost the receiver's verified source evidence")
	}
	for _, field := range []string{"digest", "bytes"} {
		f.sink.freeze = func(a *x.AtomicState) {
			if field == "digest" {
				a.PrimitiveOutcome.SHA256 = strings.Repeat("f", 64)
			} else {
				a.PrimitiveOutcome.Bytes = state.Outcome.Bytes + 1
			}
		}
		if _, err := f.actor.Freeze(testDeadline(t)); err == nil {
			t.Fatalf("contradictory primitive %s was overwritten by receiver evidence", field)
		}
	}
}

// An inline (Tier A) receiver owns no object reader. Before its single write
// and after its single Sync it exports the one shape ValidInlineState defines,
// Import accepts that shape, and the imported receiver lands exactly the
// payload from the exported prefix. Its reader's agreeing snapshot (the shape
// exported before FIX-TIERA2, which the worker image refused), an object
// cursor, a moved tmp offset, a forged or ahead prefix hash and another payload
// are each refused; Import alone cannot see payload bytes, so payload forgeries
// are the image validator's half of the same definition.
// origin: hot-update S7 plan §3 / §8.1 / §8.3 / S7 FIX-TIERA2
func TestTransferInlineReceiverExportsOnlyItsDurablePrefix(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained-payload", true: "synced-prefix"}[synced], func(t *testing.T) {
			f := newReceiverFixture(t, true)
			payload, offset := f.chunks[0], int64(0)
			if synced {
				requireStep(t, f.actor)
				offset = int64(len(payload))
			}
			state := requireFreeze(t, f.actor)
			if !reflect.DeepEqual(state.Reader, x.ReaderState{}) || state.Checkpoint.DurableOffset != offset || state.InlineOffset != offset ||
				!ValidInlineState(state, payload) {
				t.Fatalf("inline receiver exported another shape: reader=%+v offset=%d inline=%d", state.Reader, state.Checkpoint.DurableOffset, state.InlineOffset)
			}
			for name, forge := range inlineStateForgeries(state, payload) {
				forged, data, payloadOnly := forge()
				if ValidInlineState(forged, data) {
					t.Errorf("%s: forged inline state accepted by its definition", name)
				}
				if _, err := Import(f.config, forged); !payloadOnly && err == nil {
					t.Errorf("%s: forged inline state imported", name)
				}
			}
			actor := requireImport(t, f, state)
			if !synced {
				requireStep(t, actor)
			}
			if err := actor.Reserve(testGrant(f.config.Identity)); err != nil {
				t.Fatal(err)
			}
			if err := actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) {
				t.Fatalf("imported inline receiver did not land from its prefix: %v", err)
			}
			outcome, err := Outcome(actor)
			sum := sha256.Sum256(payload)
			landed, readErr := os.ReadFile(f.sink.destination)
			if err != nil || outcome.Outcome != x.OutcomeLanded || outcome.Bytes != int64(len(payload)) || outcome.SHA256 != hex.EncodeToString(sum[:]) ||
				readErr != nil || !bytes.Equal(landed, payload) || f.sink.commits != 1 {
				t.Fatalf("imported inline receiver did not land exactly once: %+v %v %v", outcome, err, readErr)
			}
			if err := actor.Stop(testDeadline(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// inlineStateForgeries mutates one fact of a valid exported inline state.
// payloadOnly marks forgeries only the owner of the payload bytes can see.
func inlineStateForgeries(state x.ReceiverState, payload []byte) map[string]func() (x.ReceiverState, []byte, bool) {
	clone := func() x.ReceiverState {
		s := state
		s.Checkpoint = copyCheckpoint(state.Checkpoint)
		return s
	}
	hashOf := func(data []byte) []byte {
		h := sha256.New()
		_, _ = h.Write(data)
		marshaled, _ := h.(encoding.BinaryMarshaler).MarshalBinary()
		return marshaled
	}
	offset := state.Checkpoint.DurableOffset
	forgeries := map[string]func() (x.ReceiverState, []byte, bool){
		"reader-snapshot": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Reader.SinkCheckpoint = copyCheckpoint(s.Checkpoint)
			return s, payload, false
		},
		"reader-object": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Reader.ObjectSpec, s.Reader.Phase = x.ObjectSpec{Identity: s.Identity, Size: s.Budget.Size}, x.ReaderOpen
			return s, payload, false
		},
		"reader-consumer": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Reader.ConsumerIdentity, s.Reader.ConnectionIdentity = "consumer", "connection"
			return s, payload, false
		},
		"chunk-index": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Checkpoint.ChunkIndex = 1
			return s, payload, false
		},
		"stream-seq": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Checkpoint.LastConsumedStreamSeq = 9
			return s, payload, false
		},
		"inline-offset": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.InlineOffset++
			return s, payload, false
		},
		"atomic-offset": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Atomic.DurableOffset++
			return s, payload, false
		},
		"object-version": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Identity.ObjectVersion = "version"
			return s, payload, false
		},
		"unsealed": func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Budget.SourceSealed = false
			return s, payload, false
		},
		"short-payload": func() (x.ReceiverState, []byte, bool) {
			return clone(), payload[:len(payload)-1], true
		},
	}
	if offset == 0 {
		// One byte ahead of the durable prefix: the hash no longer names it.
		forgeries["ahead-hash"] = func() (x.ReceiverState, []byte, bool) {
			s := clone()
			s.Checkpoint.SHA256State = hashOf(payload[:1])
			return s, payload, false
		}
		return forgeries
	}
	forgeries["forged-prefix"] = func() (x.ReceiverState, []byte, bool) {
		s := clone()
		s.Checkpoint.SHA256State = hashOf(make([]byte, offset))
		return s, payload, true
	}
	forgeries["other-payload"] = func() (x.ReceiverState, []byte, bool) {
		other := append([]byte(nil), payload...)
		other[len(other)/2] ^= 0xff
		return clone(), other, true
	}
	return forgeries
}
