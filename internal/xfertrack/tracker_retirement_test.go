package xfertrack

// Copy into internal/xfertrack. Real tracker/finalizer orchestration; ledger,
// history and object IO use the package's explicit boundary fixtures.
import (
	"errors"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
)

type retirementReviewObjects struct {
	calls   int
	receipt x.CleanupReceipt
}

func (p *retirementReviewObjects) Retire(_ x.Identity, _ x.RetirementProof, _ x.Deadline) (x.CleanupReceipt, error) {
	p.calls++
	if p.receipt.Digest == "" {
		return x.CleanupReceipt{}, errors.New("object retirement reached")
	}
	return p.receipt, nil
}

// origin: S7 retention/GC adversarial review
// origin: S7 internal review rv-gc F2
func TestTransferFinalizerStartsRetirementAfterTrackerFinish(t *testing.T) {
	f := newFixture(t, "b", "push")
	f.admit(t)
	out := f.outcome(f.reserve(t))
	if _, err := f.owner.Finish(f.permit.Identity, out, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	objects := &retirementReviewObjects{}
	final, err := xferfinal.New(x.FinalConfig{Base: f.owner.cfg.Base, Decisions: view, Ledger: f.ledger,
		History: f.history, Objects: objects, ResolveReceiver: resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) { return out, nil })})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = final.Stop(f.clock.deadline()) }()
	status, advanceErr := final.Advance(f.permit.Identity, f.clock.deadline())
	if objects.calls == 0 {
		t.Fatalf("retirement producer never invoked after exact terminal: phase=%s err=%v", status.Phase, advanceErr)
	}
}

// origin: S7 retention/GC adversarial review
// origin: S7 internal review rv-gc F3
func TestTransferFinalizerRetiresAfterOwnerGenerationChanges(t *testing.T) {
	for _, receiptGeneration := range []uint64{1, 2, 3} {
		t.Run(map[uint64]string{1: "origin_generation", 2: "current_generation", 3: "current_rollback_generation"}[receiptGeneration], func(t *testing.T) {
			f := newFixture(t, "b", "push")
			f.admit(t)
			out := f.outcome(f.reserve(t))
			if _, err := f.owner.Finish(f.permit.Identity, out, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			view, err := Decisions(f.owner)
			if err != nil {
				t.Fatal(err)
			}
			row := f.ledger.row(f.permit.Identity)
			cleanup := x.CleanupReceipt{Identity: row.Identity, OwnerGeneration: 1, WriterRetired: true, ConsumersRetired: true,
				TmpUnlinked: true, DirectorySynced: true, ObjectPurged: true, FenceSequence: 10, Digest: "fence"}
			if err := view.AcceptCleanup(key(row.Identity), row.EntryNonce, row.Revision, cleanup, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			if _, err := f.owner.Freeze(f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			if err := f.owner.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			if receiptGeneration == 3 {
				if _, err := f.owner.Freeze(f.clock.deadline()); err != nil {
					t.Fatal(err)
				}
				if err := f.owner.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: 3}, f.clock.deadline()); err != nil {
					t.Fatal(err)
				}
			}
			view, err = Decisions(f.owner)
			if err != nil {
				t.Fatal(err)
			}
			cleanup.OwnerGeneration = receiptGeneration
			objects := &retirementReviewObjects{receipt: cleanup}
			final, err := xferfinal.New(x.FinalConfig{Base: f.owner.cfg.Base, Decisions: view, Ledger: f.ledger,
				History: f.history, Objects: objects, ResolveReceiver: resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) { return out, nil })})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = final.Stop(f.clock.deadline()) }()
			status, err := final.Advance(row.Identity, f.clock.deadline())
			t.Logf("receipt generation=%d status=%s err=%v", receiptGeneration, status.Phase, err)
			if receiptGeneration == 1 {
				if err == nil {
					t.Fatal("stale owner receipt authorized current-generation retirement")
				}
				return
			}
			if err != nil || status.Phase != x.PhaseSettled {
				t.Fatalf("cleanup generation %d cannot settle after Bind(2): phase=%s err=%v", receiptGeneration, status.Phase, err)
			}
		})
	}
}

// origin: S7 retention/GC adversarial review
