package xfertrack

import (
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"runtime"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review rv-terminal / FIX-TERM request 3
func TestTransferWriterWitnessUsesCurrentSerializedRevision(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseWriterWitnessRevision(t) })
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseWriterWitnessRevision(t *testing.T) {
	t.Helper()
	f := newFixture(t, "b", "push")
	f.admit(t)
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	producer, ok := view.(interface {
		AcceptWriterRetirement(x.Key, string, uint64, x.WriterState, x.Deadline) error
	})
	if !ok {
		t.Fatal("current decision view has no serialized writer retirement producer")
	}
	row, err := view.Read(key(f.permit.Identity), f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state := x.WriterState{ObjectSpec: x.ObjectSpec{Identity: row.Identity, Bucket: "xfer-session", Stream: "OBJ_xfer-session", StreamCreated: "original"}, AttachmentGeneration: 1,
		RetirementFence: x.RetirementFence{WriterRetired: true, ExpectedStreamSeq: 20}, PendingAttempt: &x.PendingAttempt{ExpectedStreamSeq: 29}}
	for _, defect := range []string{"join", "identity", "nonce", "revision", "consumer", "purge", "fence_ack", "metadata_identity", "metadata_floor"} {
		t.Run(defect, func(t *testing.T) {
			bad, nonce, rev := state, row.EntryNonce, row.Revision
			switch defect {
			case "join":
				bad.RetirementFence.WriterRetired = false
			case "identity":
				bad.ObjectSpec.Identity.EntryNonce = "replacement"
			case "nonce":
				nonce = "replacement"
			case "revision":
				rev++
			case "consumer":
				bad.RetirementFence.ConsumersRetired = true
			case "purge":
				bad.RetirementFence.Purged = true
			case "fence_ack":
				bad.RetirementFence.Receipt = &x.HistoryReceipt{Stream: state.ObjectSpec.Stream, Sequence: 30}
			case "metadata_identity":
				foreign := state.ObjectSpec
				foreign.Identity.ObjectVersion = "replacement"
				bad.MetaReceipt = &x.ObjectReceipt{Spec: foreign, MetadataSeq: 30}
			case "metadata_floor":
				bad.RetirementFence.PreviousMetadataSeq = 30
			}
			if err := producer.AcceptWriterRetirement(key(row.Identity), nonce, rev, bad, f.clock.deadline()); err == nil {
				t.Fatal("invalid witness mutated retained writer disposition")
			}
		})
	}
	if err := producer.AcceptWriterRetirement(key(row.Identity), row.EntryNonce, row.Revision, state, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	updated := f.ledger.row(row.Identity)
	if !updated.Cleanup.WriterFence.WriterRetired || updated.Cleanup.WriterFence.ExpectedStreamSeq != 29 || updated.Revision != row.Revision+1 {
		t.Fatalf("serialized writer witness lost pending expectations or durable revision: %+v", updated.Cleanup.WriterFence)
	}
	if err := producer.AcceptWriterRetirement(key(row.Identity), row.EntryNonce, row.Revision, state, f.clock.deadline()); err == nil {
		t.Fatal("old revision accepted")
	}
	// Each original receipt source can be the maximum. A lower subsequent
	// witness cannot erase an expectation or manufacture another revision.
	// origin: hot-update S7 plan §2.3 / INTEG-W TERM/WIRE serializer reconciliation
	for _, stage := range []string{"chunk_receipt", "metadata", "lower_replay"} {
		state.PendingAttempt = nil
		expected, previous := uint64(35), uint64(0)
		switch stage {
		case "chunk_receipt":
			state.Receipts = []x.ChunkReceipt{{StreamSeq: expected}}
		case "metadata":
			expected, previous = 41, 41
			state.MetaReceipt = &x.ObjectReceipt{Spec: state.ObjectSpec, MetadataSeq: expected}
		case "lower_replay":
			expected, previous = 41, 41
			state.Receipts, state.MetaReceipt = nil, nil
		}
		before := updated.Revision
		if err := producer.AcceptWriterRetirement(key(row.Identity), row.EntryNonce, before, state, f.clock.deadline()); err != nil {
			t.Fatal(err)
		}
		updated = f.ledger.row(row.Identity)
		if updated.Cleanup.WriterFence.ExpectedStreamSeq != expected || updated.Cleanup.WriterFence.PreviousMetadataSeq != previous {
			t.Fatalf("%s lost original stream expectations: %+v", stage, updated.Cleanup.WriterFence)
		}
		if stage == "lower_replay" && updated.Revision != before {
			t.Fatal("exact retained disposition replay minted another revision")
		}
	}
	reconciler := view.(interface {
		ReconcileRetirement(x.Key, string, uint64, x.Deadline) error
	})
	if err := reconciler.ReconcileRetirement(key(row.Identity), row.EntryNonce, updated.Revision, f.clock.deadline()); err == nil {
		t.Fatal("writer retirement acknowledged still-pending ledger responsibilities")
	}
	if err := f.owner.enter(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.owner.state.Generation++
	f.owner.leave()
	if err := producer.AcceptWriterRetirement(key(row.Identity), row.EntryNonce, updated.Revision, state, f.clock.deadline()); err == nil {
		t.Fatal("pre-cut view acknowledged the successor")
	}
}
