package xfertrack

import (
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"testing"
)

type writerRetirementRecorder interface {
	AcceptWriterRetirement(x.Key, string, uint64, x.WriterState, x.Deadline) error
}

// A byte deadline is unrelated to a publisher's future gate and join. The
// original ctl's positive stopped-writer evidence must cross the same revision
// serializer as cleanup, and stale generation/nonce callbacks stay fenced.
// origin: hot-update S7 plan §2.3 / s7-p9m remaining request 3
func TestTransferWriterRetirementRequiresPositiveOriginalWitness(t *testing.T) {
	f := newFixture(t, "b", "push")
	ledger, err := xferledger.New(x.LedgerConfig{Base: f.owner.cfg.Base, Root: t.TempDir(), FallbackRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	f.owner.cfg.Ledger = ledger
	f.admit(t)
	f.reserve(t)
	if _, err := f.owner.Abandon(f.permit.Identity, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	recorder, ok := view.(writerRetirementRecorder)
	if !ok {
		t.Fatal("tracker has no serialized original-writer retirement producer")
	}
	id := f.permit.Identity
	row, err := view.Read(key(id), f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state := x.WriterState{ObjectSpec: x.ObjectSpec{Identity: id, Bucket: "xfer-" + id.SID, Stream: "OBJ_xfer-" + id.SID, StreamCreated: "original-incarnation"}, AttachmentGeneration: 1,
		RetirementFence: x.RetirementFence{ExpectedStreamSeq: 19, PreviousMetadataSeq: 17}}
	if err := recorder.AcceptWriterRetirement(key(id), row.EntryNonce, row.Revision, state, f.clock.deadline()); err == nil {
		t.Fatal("elapsed budget or an unstopped writer became retirement evidence")
	}
	state.RetirementFence.WriterRetired = true
	foreign := state
	foreign.ObjectSpec.Identity.AgentInstance = "replacement"
	if err := recorder.AcceptWriterRetirement(key(id), row.EntryNonce, row.Revision, foreign, f.clock.deadline()); err == nil {
		t.Fatal("another original writer supplied this entry's witness")
	}
	if err := recorder.AcceptWriterRetirement(key(id), "replacement", row.Revision, state, f.clock.deadline()); err == nil {
		t.Fatal("stale nonce retired a writer")
	}
	if err := recorder.AcceptWriterRetirement(key(id), row.EntryNonce, row.Revision, state, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	census, err := ledger.Scan(f.clock.deadline())
	if err != nil || len(census.Rows) != 1 || !census.Rows[0].Cleanup.WriterFence.WriterRetired || census.Rows[0].Cleanup.WriterFence.ExpectedStreamSeq != 19 || census.Rows[0].Phase == x.PhaseSettled {
		t.Fatalf("positive writer witness did not remain a durable pending responsibility: %+v %v", census, err)
	}
	if _, err := f.owner.Freeze(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Bind(x.Bindings{Ledger: ledger, OwnerGeneration: 2}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := recorder.AcceptWriterRetirement(key(id), row.EntryNonce, census.Rows[0].Revision, state, f.clock.deadline()); err == nil {
		t.Fatal("old generation accepted writer retirement after the cut")
	}
}
