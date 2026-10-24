package jsstream

import (
	"errors"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A disk failure before the fence must publish nothing, and a disk failure
// after its ACK must leave chunks intact. A retry retains the metadata CAS.
// origin: hot-update S7 plan §2.3 / T04 / T21
func TestTransferRetirementCheckpointsBeforePublishAndPurge(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRetirementCheckpoint(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retirement checkpoint", g)
	testharness.AssertNoFDLeak(t, "retirement checkpoint", fd, 0)
}

func exerciseRetirementCheckpoint(t *testing.T) {
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := []byte("original data whose cleanup must survive exec")
	spec := writerSpec(t, js, "checkpoint", body, 32<<10)
	w := mustWriter(t, js, spec)
	if _, err := w.Append(0, body, writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	meta, err := w.Finalize(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state, err := w.Freeze(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	proof := x.RetirementProof{Identity: spec.Identity, EntryNonce: spec.Identity.EntryNonce, WriterRetired: true, ConsumersRetired: true}
	diskError := errors.New("checkpoint disk unavailable")
	refuse := func(s x.WriterState) error {
		if s.RetirementFence.PreviousMetadataSeq != meta.MetadataSeq || s.RetirementFence.ExpectedStreamSeq < meta.MetadataSeq {
			t.Fatal("checkpoint lost the original metadata CAS")
		}
		return diskError
	}
	if _, err := RetireTransferObjectRecorded(js, &state, proof, writerDeadline(t), refuse); !errors.Is(err, diskError) {
		t.Fatalf("pre-publish checkpoint: %v", err)
	}
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if stream.CachedInfo().State.LastSeq != meta.MetadataSeq || len(writerRaw(t, js, spec)) != 1 {
		t.Fatal("checkpoint failure published or purged")
	}
	var retained x.WriterState
	stopBeforePurge := func(s x.WriterState) error {
		retained = cloneWriterState(s)
		if s.RetirementFence.Receipt != nil {
			return diskError
		}
		return nil
	}
	if _, err := RetireTransferObjectRecorded(js, &state, proof, writerDeadline(t), stopBeforePurge); !errors.Is(err, diskError) {
		t.Fatalf("pre-purge checkpoint: %v", err)
	}
	if retained.RetirementFence.Receipt == nil || len(writerRaw(t, js, spec)) != 1 {
		t.Fatal("fence ACK was not retained before purge")
	}
	state = retained
	receipt, err := RetireTransferObjectRecorded(js, &state, proof, writerDeadline(t), func(s x.WriterState) error { retained = cloneWriterState(s); return nil })
	if err != nil || !receipt.ObjectPurged || !retained.RetirementFence.Purged || retained.RetirementFence.PreviousMetadataSeq != meta.MetadataSeq {
		t.Fatalf("resumed retirement lost its CAS: %+v %v", retained.RetirementFence, err)
	}
	if len(writerRaw(t, js, spec)) != 0 {
		t.Fatal("resumed retirement did not purge its original version")
	}
}
