package xferimage

import (
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferreceive"
)

// tierAInline gives the codec fixture's push entry and its receiver a real
// Tier A identity and payload. Tier A owns no object version: the fixture's
// NUID described an inline receiver production never exports, which the
// receiver's own definition (ValidInlineState) refuses.
// origin: S7 FIX-TIERA2 / hot-update S7 plan §8.1
func tierAInline(e *xfercontract.WorkerEntry, r *xfercontract.ReceiverState, payload []byte) {
	id := e.Prepare.Identity
	id.ObjectVersion = ""
	e.Prepare.Identity, e.Prepare.Permit.Identity, e.Status.Identity, r.Identity = id, id, id, id
	e.Prepare.Intent.Tier, e.Prepare.Inline = "a", payload
}

// An active Tier A push receiver crosses a cut in exactly the shape its own
// Freeze exports (xferreceive.ValidInlineState): no object reader, and the
// durable tmp prefix of the entry's retained payload. The image validator
// once kept its own rule that disagreed with that export (a zero reader was
// required while the receiver exported its reader's agreeing snapshot), so
// no cut carried such a receiver; that rule also accepted a mid-payload hash
// of other bytes and an object cursor, which only the payload binding refuses.
// origin: hot-update S7 plan §8.1 / §8.3 / T17 / S7 FIX-TIERA2
func TestTransferWorkerImageAcceptsOnlyTheInlineReceiverShape(t *testing.T) {
	valid := workerFixture("inline")
	if r := valid.Receivers[0]; !xferreceive.ValidInlineState(r, valid.Entries[0].Prepare.Inline) || r.Checkpoint.DurableOffset <= 0 ||
		r.Checkpoint.DurableOffset >= r.Budget.Size {
		t.Fatal("inline fixture is not a mid-payload durable prefix in the receiver's own shape")
	}
	if _, err := EncodeWorker(valid); err != nil {
		t.Fatalf("the receiver's own inline shape is not representable: %v", err)
	}
	for name, mutate := range map[string]func(*xfercontract.WorkerState){
		"reader snapshot": func(s *xfercontract.WorkerState) { s.Receivers[0].Reader.SinkCheckpoint = s.Receivers[0].Checkpoint },
		"prefix of other bytes": func(s *xfercontract.WorkerState) {
			s.Receivers[0].Checkpoint.SHA256State = codecHash(make([]byte, s.Receivers[0].Checkpoint.DurableOffset))
		},
		"object cursor": func(s *xfercontract.WorkerState) {
			s.Receivers[0].Checkpoint.ChunkIndex, s.Entries[0].Status.ChunkIndex = 1, 1
		},
		"tmp offset moved":        func(s *xfercontract.WorkerState) { s.Receivers[0].InlineOffset-- },
		"receiver object version": func(s *xfercontract.WorkerState) { s.Receivers[0].Identity.ObjectVersion = "nuid" },
	} {
		t.Run(name, func(t *testing.T) {
			s := workerFixture("inline")
			mutate(&s)
			if data, err := EncodeWorker(s); err == nil || data != nil {
				t.Fatal("encoder accepted an inline receiver outside its exported shape")
			}
			if got, err := DecodeWorker(rawWorkerImage(t, s)); err == nil || !reflect.DeepEqual(got, xfercontract.WorkerState{}) {
				t.Fatal("decoder accepted an inline receiver outside its exported shape")
			}
		})
	}
}
