package jsstream

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Snapshot is source inspection, not a cut. A live sender must still append
// after inspection, and mutation of returned bytes cannot alter retained state.
// origin: hot-update S7 plan §2.2 / T19 / INTEG-E storage regression
func TestTransferWriterSnapshotPreservesAdmissionAndOwnsBytes(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseWriterSnapshot(t) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "writer state snapshot", goroutines)
	testharness.AssertNoFDLeak(t, "writer state snapshot", fds, 0)
}
func exerciseWriterSnapshot(t *testing.T) {
	srv := writerServer(t, nil)
	js := writerJS(t, srv.url())
	body := bytes.Repeat([]byte("snapshot"), 5000)
	spec := writerSpec(t, js, "snapshot", body, 32<<10)
	w := mustWriter(t, js, spec).(*objectWriter)
	if _, err := w.Snapshot(0); err == nil {
		t.Fatal("snapshot admitted an invalid deadline")
	}
	state, err := w.Snapshot(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if state.AttachmentGeneration != 1 || w.closed.Load() || w.frozen {
		t.Fatal("snapshot cut byte admission")
	}
	if _, err := w.Append(0, body[:spec.ChunkSize], writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state, err = w.Snapshot(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state.Receipts[0].StreamSeq++
	again, err := w.Snapshot(writerDeadline(t))
	if err != nil || again.Receipts[0].StreamSeq == state.Receipts[0].StreamSeq {
		t.Fatal("snapshot borrowed original receipts")
	}
	if _, err := w.Append(1, body[spec.ChunkSize:], writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finalize(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state, err = w.Snapshot(writerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state.MetaReceipt.Metadata[0] ^= 0xff
	again, err = w.Snapshot(writerDeadline(t))
	if err != nil || bytes.Equal(state.MetaReceipt.Metadata, again.MetaReceipt.Metadata) {
		t.Fatal("snapshot borrowed metadata bytes")
	}
	writerVerify(t, js, spec, body)
	if err := w.Stop(writerDeadline(t)); err != nil {
		t.Fatal(err)
	}
}
