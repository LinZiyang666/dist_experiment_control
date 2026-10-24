package xferfinal

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// In production nothing records writer/consumer retirement in a row before
// cleanup: only the tracker's AcceptCleanup (after Retire) does. A finalizer
// that required it first could never settle a real row, leaving the session's
// single tier-B slot occupied forever. The retirer's fence establishes it; the
// finalizer must (a) hand the retirer only what the row proves, never assert
// retirement itself, and (b) carry the fence-established retirement into the
// ledger consume proof.
// origin: hot-update S7 plan §2.3 / T21 — S7 production activation
func TestTransferFinalizerTakesWriterRetirementFromTheFence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseFinalizerWriterRetirement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer activation", g)
	testharness.AssertNoFDLeak(t, "transfer activation", fd, 0)
}

func exerciseFinalizerWriterRetirement(t *testing.T) {
	t.Helper()
	for _, established := range []bool{true, false} {
		f := newFinalFixture(t, "original-nuid")
		f.row.Cleanup.WriterFence.WriterRetired, f.row.Cleanup.WriterFence.ConsumersRetired = false, false
		var handed []xfercontract.RetirementProof
		var consumed []xfercontract.RetirementProof
		cfg := f.config()
		cfg.Objects = retirerFunc(func(id xfercontract.Identity, proof xfercontract.RetirementProof) (xfercontract.CleanupReceipt, error) {
			handed = append(handed, proof)
			if !established {
				return xfercontract.CleanupReceipt{}, errInjected
			}
			return f.cleanup, nil
		})
		cfg.Decisions = trackingDecisions{f}
		cfg.Ledger = consumingLedger{f, &consumed}
		finalizer, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		d, err := controlbudget.After(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		status, err := finalizer.Advance(f.row.Identity, d)
		if stopErr := finalizer.Stop(d); stopErr != nil {
			t.Fatal(stopErr)
		}
		if len(handed) != 1 || handed[0].WriterRetired || handed[0].ConsumersRetired || !handed[0].ReceiverRetired {
			t.Fatalf("established=%t: finalizer asserted writer retirement it does not prove: %+v", established, handed)
		}
		if !established {
			if err == nil || status.Phase == xfercontract.PhaseSettled || len(consumed) != 0 {
				t.Fatalf("an unestablished writer retirement was consumed: status=%+v err=%v consumed=%d", status, err, len(consumed))
			}
			assertFixtureFile(t, f, "primary", true)
			continue
		}
		if err != nil || status.Phase != xfercontract.PhaseSettled {
			t.Fatalf("fence-established retirement did not settle: status=%+v err=%v", status, err)
		}
		if len(consumed) != 1 || !consumed[0].WriterRetired || !consumed[0].ConsumersRetired {
			t.Fatalf("ledger consume proof lost the fence-established retirement: %+v", consumed)
		}
		assertFixtureFile(t, f, "primary", false)
	}
}

type retirerFunc func(xfercontract.Identity, xfercontract.RetirementProof) (xfercontract.CleanupReceipt, error)

func (r retirerFunc) Retire(id xfercontract.Identity, proof xfercontract.RetirementProof, _ controlbudget.Deadline) (xfercontract.CleanupReceipt, error) {
	return r(id, proof)
}

// trackingDecisions mirrors the real tracker: AcceptCleanup records the
// receipt's writer/consumer retirement in the durable row.
type trackingDecisions struct{ *finalFixture }

func (d trackingDecisions) AcceptCleanup(k xfercontract.Key, nonce string, revision uint64, r xfercontract.CleanupReceipt, dl controlbudget.Deadline) error {
	if err := d.finalFixture.AcceptCleanup(k, nonce, revision, r, dl); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.row.Cleanup.WriterFence.WriterRetired, d.row.Cleanup.WriterFence.ConsumersRetired = r.WriterRetired, r.ConsumersRetired
	return nil
}

type consumingLedger struct {
	*finalFixture
	proofs *[]xfercontract.RetirementProof
}

type currentDecisions struct {
	xfercontract.DecisionPort
	generation uint64
}

func (d currentDecisions) OwnerGeneration() uint64 { return d.generation }

// A cut advances cleanup authority without rewriting the original permit.
// The real history/receiver checks still run before this new fence is accepted.
// origin: hot-update S7 plan §8.2 / T21 / T36
func TestTransferFinalizerUsesCurrentBindingGeneration(t *testing.T) {
	f := newFinalFixture(t, "original-version")
	for _, record := range []*xfercontract.CanonicalRecord{f.row.CanonicalStart, f.row.CanonicalTerminal} {
		f.row.HistoryReceipts = append(f.row.HistoryReceipts, f.receipts[record.Digest])
	}
	f.row.StartDelivery, f.row.TerminalDelivery = xfercontract.DeliveryHistoryConfirmed, xfercontract.DeliveryHistoryConfirmed
	f.cleanup.OwnerGeneration = f.row.Origin.Generation + 1
	cfg := f.config()
	cfg.Decisions = currentDecisions{DecisionPort: f, generation: f.cleanup.OwnerGeneration}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := finalDeadline(t, time.Second)
	defer func() {
		if err := m.Stop(d); err != nil {
			t.Error(err)
		}
	}()
	proof, err := m.(*owner).cleanup(f.row, d)
	if err != nil || proof.OwnerGeneration != f.cleanup.OwnerGeneration || proof.Origin != f.row.Origin {
		t.Fatalf("cleanup used the permit's retired generation: %+v %v", proof, err)
	}
}

func (l consumingLedger) Retire(k xfercontract.Key, nonce string, proof xfercontract.RetirementProof, d controlbudget.Deadline) error {
	*l.proofs = append(*l.proofs, proof)
	return l.finalFixture.Retire(k, nonce, proof, d)
}
