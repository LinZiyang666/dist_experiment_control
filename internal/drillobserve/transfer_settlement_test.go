package drillobserve_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
)

var errPendingHistory = errors.New("history acknowledgement deliberately unavailable")

// This is a port-ordering test of the real finalizer, separate from the binary
// custody regression. History refuses the terminal at its synchronous entry;
// there is no timer, successful terminal or eventual byte result to infer from.
// origin: S7 internal review INTEG-O F5
func TestTransferFinalizerObservesPendingBeforeHistoryCall(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferPendingHistory(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer finalizer pending history", g)
	testharness.AssertNoFDLeak(t, "transfer finalizer pending history", fd, 0)
}

func exerciseTransferPendingHistory(t *testing.T) {
	t.Helper()
	hook := drillobserve.Hook
	drillobserve.Hook = func(drillobserve.Event) {}
	defer func() { drillobserve.Hook = hook }()
	now, err := controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry"}
	f := &pendingHistoryPorts{original: x.BootDeadline{BootID: "boot", Deadline: x.Deadline(now + int64(300*time.Second))}}
	f.row = x.LedgerRow{Identity: id, EntryNonce: id.EntryNonce, Revision: 7, Phase: x.PhaseDecided,
		Origin:          x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 2},
		ReceiverOutcome: &x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}}
	f.row.CanonicalStart = pendingHistoryRecord(id, "start", `{"kind":"start"}`)
	f.row.CanonicalTerminal = pendingHistoryRecord(id, "failed", `{"kind":"failed","outcome_unknown":false}`)
	f.start = x.HistoryReceipt{Stream: "history", Sequence: 1, RecordDigest: f.row.CanonicalStart.Digest}
	f.row.HistoryReceipts = []x.HistoryReceipt{f.start}
	owner, err := xferfinal.New(x.FinalConfig{Base: x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return "boot", now, err
	}, Observe: func(row x.Observation) { f.observed = append(f.observed, row) }},
		Decisions: f, Ledger: f, History: f, Objects: pendingObjects{}, ResolveReceiver: f})
	if err != nil {
		t.Fatal(err)
	}
	deadline, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.Advance(id, deadline)
	if !errors.Is(err, errPendingHistory) || !f.terminalCalled {
		t.Fatalf("terminal refusal was not propagated: called=%t err=%v", f.terminalCalled, err)
	}
	if !f.pendingAtCall {
		t.Fatal("history entered before the exact pending observation with its original deadline")
	}
	if f.row.Phase != x.PhaseDecided || len(f.row.HistoryReceipts) != 1 {
		t.Fatal("observation changed the durable phase or manufactured an ACK")
	}
	if err := owner.Stop(deadline); err != nil {
		t.Fatal(err)
	}
}

func pendingHistoryRecord(id x.Identity, kind, data string) *x.CanonicalRecord {
	digest := sha256.Sum256([]byte(data))
	return &x.CanonicalRecord{Identity: id, Kind: kind, ReqID: kind, Digest: hex.EncodeToString(digest[:]), Bytes: []byte(data)}
}

// Unused mutation/cleanup ports fail loudly. This fixture supplies one already
// confirmed START; it never returns success for publishing or retiring TERMINAL.
type pendingHistoryPorts struct {
	row                           x.LedgerRow
	original                      x.BootDeadline
	start                         x.HistoryReceipt
	observed                      []x.Observation
	terminalCalled, pendingAtCall bool
}

func (f *pendingHistoryPorts) Read(x.Key, x.Deadline) (x.LedgerRow, error) { return f.row, nil }
func (f *pendingHistoryPorts) TransferObservation(x.Identity) (x.Observation, bool) {
	return x.Observation{OriginalDeadline: f.original, Generation: f.row.Origin.Generation}, true
}
func (f *pendingHistoryPorts) Confirm(record x.CanonicalRecord, _ x.Deadline) (x.HistoryReceipt, error) {
	if record.Digest == f.start.RecordDigest {
		return f.start, nil
	}
	f.terminalCalled = true
	for _, row := range f.observed {
		if row.TransferID == f.row.Identity.TransferID && row.EntryNonce == f.row.EntryNonce &&
			row.OriginalDeadline == f.original && row.Revision == f.row.Revision &&
			row.CanonicalDigest == record.Digest && drillobserve.TransferPendingBoundary("terminal-pre-history-ACK", row) {
			f.pendingAtCall = true
		}
	}
	return x.HistoryReceipt{}, errPendingHistory
}
func (*pendingHistoryPorts) Pending(x.Deadline) ([]x.LedgerRow, error) {
	return nil, errPendingHistory
}
func (*pendingHistoryPorts) AcceptHistory(x.Key, string, uint64, x.HistoryReceipt, x.Deadline) error {
	return errPendingHistory
}
func (*pendingHistoryPorts) AcceptCleanup(x.Key, string, uint64, x.CleanupReceipt, x.Deadline) error {
	return errPendingHistory
}
func (*pendingHistoryPorts) Update(x.Key, uint64, x.LedgerMutation, x.Deadline) (x.LedgerRow, error) {
	return x.LedgerRow{}, errPendingHistory
}
func (*pendingHistoryPorts) Scan(x.Deadline) (x.LedgerCensus, error) {
	return x.LedgerCensus{}, errPendingHistory
}
func (*pendingHistoryPorts) Retire(x.Key, string, x.RetirementProof, x.Deadline) error {
	return errPendingHistory
}
func (*pendingHistoryPorts) Lookup(x.Key, x.Deadline) (x.HistorySet, error) {
	return x.HistorySet{}, errPendingHistory
}
func (*pendingHistoryPorts) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, errPendingHistory
}

type pendingObjects struct{}

func (pendingObjects) Retire(x.Identity, x.RetirementProof, x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{}, errPendingHistory
}
