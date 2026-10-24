package xferfinal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

var errInjected = errors.New("injected settlement failure")

var fixtureSourceSHA = func() string {
	digest := sha256.Sum256([]byte("new"))
	return hex.EncodeToString(digest[:])
}()

// These ports test the finalizer's orchestration, not JS's fence algorithm or
// the real ledger's fsync implementation. The real-leaf leg below remains
// explicitly NOT_RUN until those owners supply their implementations.
type finalFixture struct {
	mu            sync.Mutex
	row           xfercontract.LedgerRow
	receipts      map[string]xfercontract.HistoryReceipt
	events        []string
	dir           string
	cleanup       xfercontract.CleanupReceipt
	outcome       xfercontract.ReceiverOutcome
	historyErr    error
	resolveErr    error
	objectErr     error
	acceptErr     error
	retireErr     error
	wrongHistory  bool
	forgetHistory bool
	forgetCleanup bool
	replaceOnAck  bool
	onConfirm     func()
	onHistoryAck  func(*xfercontract.LedgerRow)
	observations  []xfercontract.Observation
}

func newFinalFixture(t *testing.T, version string) *finalFixture {
	t.Helper()
	id := xfercontract.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent",
		TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: version}
	grant := xfercontract.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "receiver", TerminalID: terminalID(id)}
	outcome := xfercontract.ReceiverOutcome{Grant: grant, Primitive: xfercontract.PrimitiveRename,
		Outcome: xfercontract.OutcomeLanded, Bytes: 3, SHA256: fixtureSourceSHA, ParentSynced: true}
	f := &finalFixture{dir: t.TempDir(), receipts: make(map[string]xfercontract.HistoryReceipt), outcome: outcome}
	f.row = xfercontract.LedgerRow{Identity: id, EntryNonce: id.EntryNonce, Revision: 4,
		Origin:     xfercontract.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 7},
		Intent:     xfercontract.Intent{Verb: "push", Tier: "b", Size: 3, SHA256: fixtureSourceSHA},
		BudgetSeal: xfercontract.BudgetSeal{StartedAt: time.Unix(123, 0), EffectiveBudgetNS: int64(300 * time.Second)},
		Phase:      xfercontract.PhaseTerminalPending, Grant: &grant, ReceiverOutcome: &outcome,
		StartDelivery: xfercontract.DeliveryPending, TerminalDelivery: xfercontract.DeliveryPending,
		Cleanup: xfercontract.CleanupState{Identity: id, ObjectPurgePending: version != "",
			PrimaryRetirePending: true, OutboxRetirePending: true,
			WriterFence: xfercontract.RetirementFence{WriterRetired: true, ConsumersRetired: true, ExpectedStreamSeq: 9}}}
	f.row.CanonicalStart = fixtureRecord(id, "start", false)
	f.row.CanonicalTerminal = fixtureRecord(id, "complete", false)
	f.row.StartedAt = f.row.BudgetSeal.StartedAt
	f.row.Permit = xfercontract.CreationPermit{Identity: id, Origin: f.row.Origin,
		IssuedAt: f.row.StartedAt, CreateUntil: f.row.StartedAt.Add(30 * time.Second), PermitNonce: "permit", Signature: "test-signature"}
	f.cleanup = xfercontract.CleanupReceipt{Identity: id, OwnerGeneration: 7, WriterRetired: true,
		ConsumersRetired: true, FenceSequence: 10, ObjectPurged: version != "", TmpUnlinked: true, DirectorySynced: true, Digest: "cleanup-digest"}
	for i, record := range []*xfercontract.CanonicalRecord{f.row.CanonicalStart, f.row.CanonicalTerminal} {
		f.receipts[record.Digest] = xfercontract.HistoryReceipt{Stream: "history", Sequence: uint64(i + 1), RecordDigest: record.Digest}
	}
	for _, name := range []string{"primary", "outbox", "object"} {
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func fixtureRecord(id xfercontract.Identity, kind string, unknown bool) *xfercontract.CanonicalRecord {
	record := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Tier: "b", Ts: time.Unix(123, 0).UTC(), Session: id.SID, Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID, Bytes: 3, SHA256: fixtureSourceSHA, OutcomeUnknown: unknown,
		AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce, IntentDigest: id.IntentDigest, ObjectVersion: id.ObjectVersion}
	if id.ObjectVersion == "" {
		record.Tier = "a"
	}
	if kind != "start" {
		record.TerminalID = terminalID(id)
	}
	data, _ := json.Marshal(record)
	digest := sha256.Sum256(data)
	request := sha256.Sum256(append([]byte("xferaudit-record:"), data...))
	return &xfercontract.CanonicalRecord{Identity: id, Kind: kind, ReqID: hex.EncodeToString(request[:]), Digest: hex.EncodeToString(digest[:]), Bytes: data}
}

func fixtureClock() (string, int64, error) {
	now, err := controlbudget.Now()
	return "boot", now, err
}

func finalDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (f *finalFixture) config() Config {
	return Config{Base: xfercontract.Base{Clock: fixtureClock, Observe: func(observation xfercontract.Observation) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.observations = append(f.observations, observation)
	}}, Decisions: f, Ledger: f, History: f, Objects: fixtureObjects{f}, ResolveReceiver: f}
}

func (f *finalFixture) Read(key xfercontract.Key, _ controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if key != (xfercontract.Key{SID: f.row.Identity.SID, TransferID: f.row.Identity.TransferID}) {
		return xfercontract.LedgerRow{}, errInjected
	}
	f.events = append(f.events, "read")
	row := f.row
	row.HistoryReceipts = append([]xfercontract.HistoryReceipt(nil), row.HistoryReceipts...)
	return row, nil
}

func (f *finalFixture) Pending(controlbudget.Deadline) ([]xfercontract.LedgerRow, error) {
	return nil, errors.New("Pending is not part of an explicit Advance")
}

func (f *finalFixture) AcceptHistory(_ xfercontract.Key, nonce string, revision uint64, receipt xfercontract.HistoryReceipt, _ controlbudget.Deadline) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if nonce != f.row.EntryNonce || revision != f.row.Revision {
		return errInjected
	}
	f.events = append(f.events, "accept-history")
	if f.acceptErr != nil {
		return f.acceptErr
	}
	if f.replaceOnAck {
		f.row.Identity.EntryNonce, f.row.EntryNonce = "replacement", "replacement"
		return nil
	}
	if !f.forgetHistory {
		f.row.HistoryReceipts = append(f.row.HistoryReceipts, receipt)
		f.row.Revision++
	}
	if f.onHistoryAck != nil {
		f.onHistoryAck(&f.row)
	}
	return nil
}

func (f *finalFixture) AcceptCleanup(_ xfercontract.Key, nonce string, revision uint64, receipt xfercontract.CleanupReceipt, _ controlbudget.Deadline) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if nonce != f.row.EntryNonce || revision != f.row.Revision || receipt != f.cleanup {
		return errInjected
	}
	f.events = append(f.events, "accept-cleanup")
	if f.acceptErr != nil {
		return f.acceptErr
	}
	if !f.forgetCleanup {
		f.row.Revision++
		f.row.Cleanup.ObjectPurgePending, f.row.Cleanup.TmpUnlinkPending, f.row.Cleanup.DirectorySyncPending = false, false, false
		f.row.Cleanup.WriterFence.Purged = receipt.ObjectPurged
		f.row.Cleanup.WriterFence.Receipt = &xfercontract.HistoryReceipt{Sequence: receipt.FenceSequence, Stream: "object", RecordDigest: "fence"}
	}
	return nil
}

func (f *finalFixture) Confirm(record xfercontract.CanonicalRecord, _ controlbudget.Deadline) (xfercontract.HistoryReceipt, error) {
	if f.onConfirm != nil {
		f.onConfirm()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "confirm-"+record.Kind)
	if f.historyErr != nil {
		return xfercontract.HistoryReceipt{}, f.historyErr
	}
	receipt, exists := f.receipts[record.Digest]
	if !exists {
		return receipt, errInjected
	}
	if f.wrongHistory {
		receipt.RecordDigest = "other-canonical"
	}
	return receipt, nil
}

func (f *finalFixture) Lookup(xfercontract.Key, controlbudget.Deadline) (xfercontract.HistorySet, error) {
	return xfercontract.HistorySet{}, errors.New("Lookup cannot replace exact Confirm")
}

func (f *finalFixture) Resolve(xfercontract.Identity, controlbudget.Deadline) (xfercontract.ReceiverOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "resolve-receiver")
	return f.outcome, f.resolveErr
}

type fixtureObjects struct{ f *finalFixture }

func (o fixtureObjects) Retire(id xfercontract.Identity, proof xfercontract.RetirementProof, _ controlbudget.Deadline) (xfercontract.CleanupReceipt, error) {
	f := o.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "retire-object")
	if f.objectErr != nil {
		return xfercontract.CleanupReceipt{}, f.objectErr
	}
	if id != f.row.Identity || proof.Identity != id || proof.Revision != f.row.Revision || !proof.WriterRetired ||
		!proof.ReceiverRetired || !proof.ConsumersRetired || proof.StartReceipt.Sequence == 0 || proof.TerminalReceipt.Sequence == 0 {
		return xfercontract.CleanupReceipt{}, errInjected
	}
	if err := os.Remove(filepath.Join(f.dir, "object")); err != nil && !os.IsNotExist(err) {
		return xfercontract.CleanupReceipt{}, err
	}
	return f.cleanup, nil
}

func (f *finalFixture) Update(xfercontract.Key, uint64, xfercontract.LedgerMutation, controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	return xfercontract.LedgerRow{}, errors.New("the finalizer must use revision messages")
}

func (f *finalFixture) Scan(controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	return xfercontract.LedgerCensus{}, errors.New("Advance cannot reinterpret a partial census")
}

func (f *finalFixture) Retire(_ xfercontract.Key, nonce string, proof xfercontract.RetirementProof, _ controlbudget.Deadline) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "retire-ledger")
	if nonce != f.row.EntryNonce || proof.Identity != f.row.Identity || proof.Revision != f.row.Revision || proof.Origin != f.row.Origin ||
		proof.OwnerGeneration != f.row.Origin.Generation || proof.Cleanup != f.cleanup || !proof.ReceiverRetired {
		return errInjected
	}
	if f.retireErr != nil {
		return f.retireErr // The surviving primary requires its outbox barrier.
	}
	for _, name := range []string{"primary", "outbox"} {
		if err := os.Remove(filepath.Join(f.dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
		f.events = append(f.events, "consume-"+name)
	}
	return nil
}

func fixtureAdvance(t *testing.T, f *finalFixture) (xfercontract.Status, error) {
	t.Helper()
	finalizer, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := finalizer.Stop(finalDeadline(t, time.Second)); err != nil {
			t.Error(err)
		}
	}()
	return finalizer.Advance(f.row.Identity, finalDeadline(t, time.Second))
}

func assertFixtureFile(t *testing.T, f *finalFixture, name string, present bool) {
	t.Helper()
	_, err := os.Stat(filepath.Join(f.dir, name))
	if present && err != nil || !present && !os.IsNotExist(err) {
		t.Fatalf("%s present=%t: %v", name, present, err)
	}
}

// origin: hot-update S7 plan §5.1–§5.2 / T21
func TestSingleStagedTerminalActuallyRetires(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for _, version := range []string{"", "original-nuid"} {
		t.Run("port_ordering/"+version, func(t *testing.T) {
			f := newFinalFixture(t, version)
			// Both exact records already physically exist in the history port.
			// Cached proofs must not turn this into the old early-return path.
			for _, record := range []*xfercontract.CanonicalRecord{f.row.CanonicalStart, f.row.CanonicalTerminal} {
				f.row.HistoryReceipts = append(f.row.HistoryReceipts, f.receipts[record.Digest])
			}
			status, err := fixtureAdvance(t, f)
			if err != nil || status.State != xfercontract.StatusTerminal || status.Phase != xfercontract.PhaseSettled || status.CleanupPending {
				t.Fatalf("already-confirmed terminal did not retire: status=%+v err=%v", status, err)
			}
			for _, name := range []string{"primary", "outbox", "object"} {
				assertFixtureFile(t, f, name, false)
			}
			want := []string{"read", "confirm-start", "read", "confirm-complete", "read", "resolve-receiver",
				"retire-object", "accept-cleanup", "read", "retire-ledger", "consume-primary", "consume-outbox"}
			if !reflect.DeepEqual(f.events, want) {
				t.Fatalf("exact ACK/cleanup/consume order: got %v want %v", f.events, want)
			}
			if status.Terminal.TerminalID != terminalID(f.row.Identity) || status.Terminal.CanonicalDigest != f.row.CanonicalTerminal.Digest ||
				status.Terminal.HistorySeq != 2 || status.Terminal.Bytes != 3 || status.Terminal.SHA256 != fixtureSourceSHA {
				t.Fatalf("receipt changed during replay: %+v", status.Terminal)
			}
			if len(f.observations) != 1 || f.observations[0].Kind != xfercontract.ObserveRetirement ||
				f.observations[0].Revision != f.row.Revision || f.observations[0].RetirementDigest != f.cleanup.Digest ||
				f.observations[0].EventSeq == 0 || f.observations[0].Phase != xfercontract.PhaseSettled {
				t.Fatalf("missing revision-bound retirement observation: %+v", f.observations)
			}
		})
	}
	t.Run("primary_failure_keeps_outbox_barrier", func(t *testing.T) {
		f := newFinalFixture(t, "original-nuid")
		f.retireErr = errInjected
		status, err := fixtureAdvance(t, f)
		if !errors.Is(err, errInjected) || status.Phase == xfercontract.PhaseSettled || !status.CleanupPending {
			t.Fatalf("primary retirement failure was swallowed: %+v %v", status, err)
		}
		assertFixtureFile(t, f, "primary", true)
		assertFixtureFile(t, f, "outbox", true)
		f.retireErr = nil
		status, err = fixtureAdvance(t, f)
		if err != nil || status.Phase != xfercontract.PhaseSettled {
			t.Fatalf("retained terminal did not finish next pass: %+v %v", status, err)
		}
		assertFixtureFile(t, f, "primary", false)
		assertFixtureFile(t, f, "outbox", false)
	})
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("real_leaf_dependencies_%d", round), func(t *testing.T) { exerciseRealFinalizerRetirement(t) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "real finalizer storage", goroutines)
	testharness.AssertNoFDLeak(t, "real finalizer storage", fds, 0)
}

// origin: hot-update S7 plan §2.3 / §4 / §5.2 / T21
func TestTransferFinalizerRetainsUncertainResponsibilities(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*finalFixture)
	}{
		{"history_unavailable", func(f *finalFixture) { f.historyErr = errInjected }},
		{"opposite_history_receipt", func(f *finalFixture) { f.wrongHistory = true }},
		{"stored_receipt_differs", func(f *finalFixture) {
			f.row.HistoryReceipts = []xfercontract.HistoryReceipt{{Stream: "other-history", Sequence: 3, RecordDigest: f.row.CanonicalStart.Digest}}
		}},
		{"history_ack_not_persisted", func(f *finalFixture) { f.forgetHistory = true }},
		{"revision_replaced_during_ack", func(f *finalFixture) { f.replaceOnAck = true }},
		{"origin_changed_during_ack", func(f *finalFixture) {
			f.onHistoryAck = func(row *xfercontract.LedgerRow) { row.Origin.Generation++ }
		}},
		{"revision_regressed_during_ack", func(f *finalFixture) {
			f.onHistoryAck = func(row *xfercontract.LedgerRow) { row.Revision -= 2 }
		}},
		{"canonical_changed_during_ack", func(f *finalFixture) {
			f.onHistoryAck = func(row *xfercontract.LedgerRow) {
				row.CanonicalTerminal = fixtureRecord(row.Identity, "failed", false)
			}
		}},
		{"ack_store_failure", func(f *finalFixture) { f.acceptErr = errInjected }},
		{"receiver_unavailable", func(f *finalFixture) { f.resolveErr = errInjected }},
		{"receiver_inflight", func(f *finalFixture) {
			f.outcome.Outcome = xfercontract.OutcomeInflight
			f.row.ReceiverOutcome = &f.outcome
		}},
		{"directory_not_synced", func(f *finalFixture) { f.outcome.ParentSynced = false }},
		{"tmp_cleanup_pending", func(f *finalFixture) { f.outcome.TmpCleanupPending = true }},
		{"wrong_grant", func(f *finalFixture) { f.row.Grant.GrantNonce = "late-grant" }},
		{"changed_receiver_bytes", func(f *finalFixture) { f.outcome.Bytes++ }},
		{"canonical_bytes_differ_from_receiver", func(f *finalFixture) {
			f.outcome.Bytes++
			outcome := f.outcome
			f.row.ReceiverOutcome = &outcome
		}},
		{"complete_not_landed", func(f *finalFixture) { f.outcome.Outcome = xfercontract.OutcomeNotEntered }},
		{"writer_not_retired", func(f *finalFixture) { f.row.Cleanup.WriterFence.WriterRetired = false }},
		{"consumer_not_retired", func(f *finalFixture) { f.row.Cleanup.WriterFence.ConsumersRetired = false }},
		{"dirty_consumer", func(f *finalFixture) {
			f.row.Cleanup.Consumers = []xfercontract.ReaderState{{ObjectSpec: xfercontract.ObjectSpec{Identity: f.row.Identity}, Phase: xfercontract.ReaderStopped, DirtyBarrier: true}}
		}},
		{"object_retirement_unavailable", func(f *finalFixture) { f.objectErr = errInjected }},
		{"fence_not_later", func(f *finalFixture) { f.cleanup.FenceSequence = f.row.Cleanup.WriterFence.ExpectedStreamSeq }},
		{"original_object_not_purged", func(f *finalFixture) { f.cleanup.ObjectPurged = false }},
		{"cleanup_wrong_identity", func(f *finalFixture) { f.cleanup.Identity.ObjectVersion = "foreign-nuid" }},
		{"cleanup_old_generation", func(f *finalFixture) { f.cleanup.OwnerGeneration-- }},
		{"cleanup_not_complete", func(f *finalFixture) { f.cleanup.DirectorySynced = false }},
		{"cleanup_writer_unjoined", func(f *finalFixture) { f.cleanup.WriterRetired = false }},
		{"cleanup_consumer_unjoined", func(f *finalFixture) { f.cleanup.ConsumersRetired = false }},
		{"cleanup_tmp_not_unlinked", func(f *finalFixture) { f.cleanup.TmpUnlinked = false }},
		{"cleanup_digest_missing", func(f *finalFixture) { f.cleanup.Digest = "" }},
		{"cleanup_ack_not_persisted", func(f *finalFixture) { f.forgetCleanup = true }},
		{"canonical_digest_changed", func(f *finalFixture) { f.row.CanonicalTerminal.Digest = "other" }},
		{"canonical_kind_changed", func(f *finalFixture) { f.row.CanonicalTerminal.Kind = "failed" }},
		{"canonical_identity_changed", func(f *finalFixture) { f.row.CanonicalTerminal.Identity.ActorFP = "other-actor" }},
		{"cached_duplicate_receipt", func(f *finalFixture) {
			receipt := f.receipts[f.row.CanonicalStart.Digest]
			f.row.HistoryReceipts = []xfercontract.HistoryReceipt{receipt, receipt}
		}},
		{"missing_start", func(f *finalFixture) { f.row.CanonicalStart = nil }},
		{"zero_revision", func(f *finalFixture) { f.row.Revision = 0 }},
		{"old_entry_nonce", func(f *finalFixture) { f.row.EntryNonce = "other-entry" }},
		{"unknown_owner_generation", func(f *finalFixture) { f.row.Origin.Generation = 0 }},
		{"abandonment_claims_complete", func(f *finalFixture) {
			f.row.OutcomeUnknown = true
			f.row.CanonicalTerminal = fixtureRecord(f.row.Identity, "complete", true)
			f.receipts[f.row.CanonicalTerminal.Digest] = xfercontract.HistoryReceipt{Stream: "history", Sequence: 2, RecordDigest: f.row.CanonicalTerminal.Digest}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinalFixture(t, "original-nuid")
			tc.mutate(f)
			status, err := fixtureAdvance(t, f)
			if err == nil || status.Phase == xfercontract.PhaseSettled || !status.CleanupPending {
				t.Fatalf("uncertain responsibility was retired: status=%+v err=%v", status, err)
			}
			assertFixtureFile(t, f, "primary", true)
			assertFixtureFile(t, f, "outbox", true)
			for _, event := range f.events {
				if strings.HasPrefix(event, "confirm-") && (tc.name == "canonical_digest_changed" || tc.name == "canonical_kind_changed") {
					t.Fatal("invalid canonical decision reached history publication")
				}
				if event == "retire-ledger" {
					t.Fatal("uncertain cleanup reached ledger consumption")
				}
				if event == "accept-history" && tc.name == "opposite_history_receipt" {
					t.Fatal("inexact history receipt reached durable acknowledgement")
				}
				if event == "confirm-complete" && tc.name == "history_ack_not_persisted" {
					t.Fatal("unpersisted START receipt allowed terminal publication")
				}
				if (event == "confirm-complete" || event == "confirm-failed") &&
					(tc.name == "origin_changed_during_ack" || tc.name == "revision_regressed_during_ack" || tc.name == "canonical_changed_during_ack") {
					t.Fatal("changed decision advanced past its acknowledgement revision")
				}
				if event == "accept-cleanup" && (tc.name == "fence_not_later" || tc.name == "original_object_not_purged" ||
					strings.HasPrefix(tc.name, "cleanup_") && tc.name != "cleanup_ack_not_persisted") {
					t.Fatal("incomplete cleanup receipt reached durable acknowledgement")
				}
				// A cleanup-receipt fault is discovered after object retirement;
				// every earlier fault must stop before this destructive port call.
				// Writer/consumer retirement is established BY the retirer's fence
				// (the row can only learn it from AcceptCleanup), so those arms do
				// reach the port — with an unasserted proof, which the fixture's
				// retirer refuses, retaining every responsibility above.
				// origin: S7 production activation — circular writer precondition
				if event == "retire-object" && tc.name != "object_retirement_unavailable" &&
					tc.name != "fence_not_later" && tc.name != "original_object_not_purged" &&
					tc.name != "writer_not_retired" && tc.name != "consumer_not_retired" &&
					!strings.HasPrefix(tc.name, "cleanup_") {
					t.Fatal("unproven history/receiver/writer responsibility reached object retirement")
				}
			}
		})
	}
}

// origin: hot-update S7 plan §4 / §5.1 / T21
func TestTransferFinalizerSettlesClosedReceiverWithoutGrant(t *testing.T) {
	f := newFinalFixture(t, "original-nuid")
	f.row.Grant = nil
	f.outcome = xfercontract.ReceiverOutcome{Outcome: xfercontract.OutcomeNotEntered}
	f.row.ReceiverOutcome = &f.outcome
	f.row.CanonicalTerminal = fixtureRecord(f.row.Identity, "failed", false)
	f.receipts[f.row.CanonicalTerminal.Digest] = xfercontract.HistoryReceipt{Stream: "history", Sequence: 2, RecordDigest: f.row.CanonicalTerminal.Digest}
	status, err := fixtureAdvance(t, f)
	if err != nil || status.Phase != xfercontract.PhaseSettled || status.Terminal.Kind != "failed" {
		t.Fatalf("closed receiver without a grant did not settle: %+v %v", status, err)
	}
}

// origin: hot-update S7 plan §5.1 / T21
func TestTransferFinalizerPublishesStartWithoutGuessingTerminal(t *testing.T) {
	f := newFinalFixture(t, "original-nuid")
	f.row.CanonicalTerminal, f.row.Grant, f.row.ReceiverOutcome = nil, nil, nil
	f.row.Phase = xfercontract.PhaseReceiving
	status, err := fixtureAdvance(t, f)
	if err != nil || status.State != xfercontract.StatusLive || status.Terminal != nil || len(f.row.HistoryReceipts) != 1 {
		t.Fatalf("START-only publication guessed a terminal: %+v %v", status, err)
	}
	for _, name := range []string{"primary", "outbox", "object"} {
		assertFixtureFile(t, f, name, true)
	}
}
