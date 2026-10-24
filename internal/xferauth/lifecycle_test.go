package xferauth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// origin: hot-update S7 plan §6.4 / T31
func TestTransferLifecycleNoLeaks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "settlement verifier", g)
	testharness.AssertNoFDLeak(t, "settlement verifier", fd, 0)
}
func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	c, p, census, iv := authFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	v, err := New(Config{Issuers: iv, Owners: censusFunc(func(xfercontract.Origin, controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
		close(entered)
		<-release
		return census, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	verifyDeadline := authDeadline(t)
	go func() { _, err := v.Verify(c, p, verifyDeadline); result <- err }()
	<-entered
	d, err := controlbudget.After(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	err = v.Stop(d)
	close(release)
	<-result
	if !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("Stop did not join active lookup: %v", err)
	}
	if err := v.Stop(authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(c, p, authDeadline(t)); !errors.Is(err, ErrStopped) {
		t.Fatal("Stop reopened admission")
	}
}

// These adversarial ports exercise orchestration and revision boundaries only.
// They are not receipts for P11/P12/P13's durable implementations.
type settlementPorts struct {
	ledger     xfercontract.LedgerPort
	tracker    xfercontract.TrackerManager
	row        xfercontract.LedgerRow
	complete   bool
	advance    bool
	cancelErr  error
	updateErr  error
	abandonErr error
	mutations  []xfercontract.LedgerMutation
	cancels    []xfercontract.CancelRequest
	abandons   int
}

// Abandon plays the tracker's decision: one failed terminal in the shared
// audit schema with code settlement_abandoned and outcome_unknown=true. The
// real minting is exercised against the real tracker in xfertrack.
func (p *settlementPorts) Abandon(id xfercontract.Identity, d controlbudget.Deadline) (xfercontract.Status, error) {
	p.abandons++
	if p.abandonErr != nil {
		return xfercontract.Status{}, p.abandonErr
	}
	status, err := p.tracker.(TrackerAbandoner).Abandon(id, d)
	if err != nil {
		return status, err
	}
	census, err := p.ledger.Scan(d)
	if err != nil || len(census.Rows) != 1 {
		return status, errors.New("abandonment census unavailable")
	}
	next := census.Rows[0]
	if p.row.CanonicalTerminal == nil && next.CanonicalTerminal != nil {
		p.mutations = append(p.mutations, xfercontract.LedgerMutation{Kind: xfercontract.MutationAbandon, EntryNonce: next.EntryNonce, Row: next})
	}
	p.row = next
	return status, nil
}

func (p *settlementPorts) SessionCensus(string, controlbudget.Deadline) ([]xfercontract.LedgerCensus, error) {
	return []xfercontract.LedgerCensus{{Origin: p.row.Origin, Ready: true, Complete: p.complete, Rows: []xfercontract.LedgerRow{p.row}}}, nil
}
func (p *settlementPorts) Update(k xfercontract.Key, rev uint64, m xfercontract.LedgerMutation, d controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	if p.updateErr != nil {
		return xfercontract.LedgerRow{}, p.updateErr
	}
	row, err := p.ledger.Update(k, rev, m, d)
	if err != nil {
		return row, err
	}
	p.mutations = append(p.mutations, m)
	p.row = row
	return row, nil
}
func (*settlementPorts) Scan(controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	return xfercontract.LedgerCensus{}, errors.New("unexpected scan")
}
func (*settlementPorts) Retire(xfercontract.Key, string, xfercontract.RetirementProof, controlbudget.Deadline) error {
	return errors.New("unexpected retire")
}
func (p *settlementPorts) Cancel(id xfercontract.Identity, r xfercontract.CancelRequest, _ controlbudget.Deadline) (xfercontract.Status, error) {
	p.cancels = append(p.cancels, r)
	if id != p.row.Identity || r.Origin != p.row.Origin || r.Reason != xfercontract.CancelRevoked {
		return xfercontract.Status{}, errors.New("cancel identity or origin differs")
	}
	return xfercontract.Status{}, p.cancelErr
}
func (*settlementPorts) Admit(xfercontract.CreationPermit, xfercontract.Intent, controlbudget.Deadline) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("unexpected admit")
}
func (*settlementPorts) SourceSeal(xfercontract.Identity, xfercontract.SourceSeal, controlbudget.Deadline) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("unexpected seal")
}
func (*settlementPorts) Reserve(xfercontract.Identity, xfercontract.ReserveRequest, controlbudget.Deadline) (xfercontract.CommitGrant, error) {
	return xfercontract.CommitGrant{}, errors.New("unexpected reserve")
}
func (*settlementPorts) Finish(xfercontract.Identity, xfercontract.ReceiverOutcome, controlbudget.Deadline) (xfercontract.TerminalReceipt, error) {
	return xfercontract.TerminalReceipt{}, errors.New("unexpected finish")
}
func (*settlementPorts) Status(xfercontract.StatusRequest, controlbudget.Deadline) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("unexpected status")
}
func (p *settlementPorts) Advance(xfercontract.Identity, controlbudget.Deadline) (xfercontract.Status, error) {
	if !p.advance {
		return xfercontract.Status{}, errors.New("pending")
	}
	p.row = settledRow(p.row)
	return xfercontract.Status{}, nil
}
func (*settlementPorts) Stop(controlbudget.Deadline) error { return errors.New("unexpected stop") }
func settledRow(r xfercontract.LedgerRow) xfercontract.LedgerRow {
	r.Phase = xfercontract.PhaseSettled
	r.Cleanup.TmpUnlinkPending, r.Cleanup.DirectorySyncPending, r.Cleanup.ObjectPurgePending = false, false, false
	r.Cleanup.PrimaryRetirePending, r.Cleanup.OutboxRetirePending = false, false
	r.Cleanup.WriterFence.WriterRetired = true
	r.Cleanup.WriterFence.ConsumersRetired = true
	r.StartDelivery = xfercontract.DeliveryHistoryConfirmed
	r.TerminalDelivery = xfercontract.DeliveryHistoryConfirmed
	r.HistoryReceipts = nil
	for i, record := range []*xfercontract.CanonicalRecord{r.CanonicalStart, r.CanonicalTerminal} {
		if record != nil {
			r.HistoryReceipts = append(r.HistoryReceipts, xfercontract.HistoryReceipt{Stream: "history-" + r.Identity.SID, RecordDigest: record.Digest, Sequence: uint64(i + 1)})
		}
	}
	return r
}
func settlementFixture(t *testing.T) (SessionBarrier, *settlementPorts) {
	t.Helper()
	base := xfercontract.Base{Clock: recoveryClock, UTC: time.Now, Capacity: &recoveryCapacity{}}
	ledger, err := xferledger.New(xfercontract.LedgerConfig{Base: base, Root: t.TempDir(), FallbackRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	origin := xfercontract.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	tracker, err := xfertrack.New(xfercontract.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: recoveryHistory{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tracker.Stop(authDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	id := xfercontract.Identity{SID: "lab", NID: "node", TransferID: "transfer", ActorFP: "actor", AgentInstance: "agent", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "object"}
	now := time.Now().UTC()
	permit := xfercontract.CreationPermit{Identity: id, Origin: origin, PermitNonce: "permit", Signature: "verified", IssuedAt: now, CreateUntil: now.Add(time.Minute)}
	if _, err = tracker.Admit(permit, xfercontract.Intent{Verb: "push", Tier: "b", Path: "destination", Size: 6, SHA256: recoveryDigest([]byte("abcdef")), ChunkSize: 32768}, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.SourceSeal(id, xfercontract.SourceSeal{Size: 6, SHA256: recoveryDigest([]byte("abcdef")), SourceFingerprint: "pinned", ChunkSize: 32768, Chunks: 1}, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = tracker.Reserve(id, xfercontract.ReserveRequest{Origin: origin, ReceiverInstance: "receiver"}, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	census, err := ledger.Scan(authDeadline(t))
	if err != nil || len(census.Rows) != 1 {
		t.Fatal(err)
	}
	p := &settlementPorts{row: census.Rows[0], ledger: ledger, tracker: tracker, complete: true}
	s, err := NewSettlement(SettlementConfig{Owners: p, Tracker: p, Ledger: p, Finalizer: p, UTC: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}

// origin: hot-update S7 plan lead LQ1 / T31
func TestSettlementAbandonmentRequiresExactFailedDecision(t *testing.T) {
	s, p := settlementFixture(t)
	err := s.Settle("lab", false, authDeadline(t))
	var pending *PendingError
	if !errors.As(err, &pending) || pending.ExitCode() != 75 || !strings.Contains(err.Error(), "transfer:") {
		t.Fatalf("missing pending transfer/rc75: %v", err)
	}
	if len(p.mutations) != 0 || p.row.CanonicalTerminal != nil {
		t.Fatal("removal without flag fabricated terminal")
	}
	if p.abandons != 0 {
		t.Fatal("removal without flag asked the tracker to abandon")
	}
	// origin: INTEG-D session abandonment — the cancel must name the entry's
	// original origin, or the real tracker refuses it as an intent conflict.
	if len(p.cancels) == 0 || p.cancels[0].Origin != p.row.Origin || p.cancels[0].Reason != xfercontract.CancelRevoked {
		t.Fatalf("settlement cancel lost the original origin: %+v", p.cancels)
	}
	p.cancelErr = errors.New("receiver permanently unavailable")
	p.advance = true
	if err := s.Settle("lab", true, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	// The decision is the tracker's (its serializer owns the revision); the
	// settlement never writes the ledger behind it.
	if p.abandons != 1 || len(p.mutations) != 1 || p.mutations[0].Kind != xfercontract.MutationAbandon || !p.row.OutcomeUnknown {
		t.Fatalf("abandonment did not ask the tracker for one unknown decision: abandons=%d mutations=%d", p.abandons, len(p.mutations))
	}
	canonical := p.row.CanonicalTerminal
	// Lose only the history ACK after a durable decision. A retry must reuse
	// the original bytes instead of deciding a second terminal.
	p.row.TerminalDelivery = xfercontract.DeliveryPending
	if err := s.Settle("lab", true, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if p.abandons != 1 || len(p.mutations) != 1 || p.row.CanonicalTerminal.Digest != canonical.Digest {
		t.Fatal("retry produced a second terminal")
	}
}

// TestSettlementAbandonmentNeedsTheTrackerDecision: a tracker without the
// explicit decision cannot be bypassed by a direct ledger write.
// origin: hot-update S7 plan lead LQ1 / INTEG-D session abandonment
func TestSettlementAbandonmentNeedsTheTrackerDecision(t *testing.T) {
	_, p := settlementFixture(t)
	p.advance = true
	s, err := NewSettlement(SettlementConfig{Owners: p, Tracker: struct{ xfercontract.TrackerPort }{p}, Ledger: p, Finalizer: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Settle("lab", true, authDeadline(t)); err == nil || !strings.Contains(err.Error(), "abandonment_unconfirmed") {
		t.Fatalf("abandonment without a tracker decision was not retained: %v", err)
	}
	if len(p.mutations) != 0 || p.row.CanonicalTerminal != nil {
		t.Fatal("settlement wrote a decision behind the tracker")
	}
}

// TestSettlementAbandonmentReleasesOnlyExactHistory: with the flag, exact
// confirmed START+TERMINAL history is the whole barrier — retained cleanup
// no longer blocks (the session deletion removes history and the bucket) —
// but an unconfirmed or contradictory record still does.
// The former history-only assumption above is superseded by the positive
// producer witness: deleting a bucket does not close a ctl publish gate.
// origin: S7 internal review rv-wiring F5 / s7-p9m remaining request 3
// origin: hot-update S7 plan lead LQ1 / §6.4 / INTEG-D session abandonment
func TestSettlementAbandonmentReleasesOnlyExactHistory(t *testing.T) {
	for _, name := range []string{"cleanup_retained", "terminal_ack", "start_ack", "unknown_code"} {
		t.Run(name, func(t *testing.T) {
			s, p := settlementFixture(t)
			if _, err := p.Abandon(p.row.Identity, authDeadline(t)); err != nil {
				t.Fatal(err)
			}
			p.row = settledRow(p.row)
			p.row.Phase = xfercontract.PhaseHistoryConfirmed
			p.row.Cleanup.WriterFence.WriterRetired, p.row.Cleanup.ObjectPurgePending = false, true
			switch name {
			case "terminal_ack":
				p.row.HistoryReceipts = p.row.HistoryReceipts[:1]
			case "start_ack":
				p.row.StartDelivery = xfercontract.DeliveryPending
			case "unknown_code":
				body := []byte(`{"kind":"failed","code":"other","outcome_unknown":true}`)
				sum := sha256.Sum256(body)
				p.row.CanonicalTerminal.Bytes, p.row.CanonicalTerminal.Digest = body, hex.EncodeToString(sum[:])
				p.row = settledRow(p.row)
				p.row.Cleanup.WriterFence.WriterRetired = false
			}
			if err := s.Settle("lab", false, authDeadline(t)); err == nil {
				t.Fatal("retained cleanup permitted deletion without the flag")
			}
			err := s.Settle("lab", true, authDeadline(t))
			if name == "cleanup_retained" {
				if err == nil {
					t.Fatal("the flag waived reachable producer retirement")
				}
				return
			}
			if err == nil {
				t.Fatal("the flag waived unconfirmed or contradictory history")
			}
		})
	}
}

// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
func TestSettlementBarrierRequiresHistoryAndRetirement(t *testing.T) {
	for _, name := range []string{"incomplete", "start_ack", "terminal_ack", "writer", "consumer", "cleanup", "digest", "conflict", "unknown_complete", "unknown_bit", "unknown_code"} {
		t.Run(name, func(t *testing.T) {
			s, p := settlementFixture(t)
			p.advance = true
			if err := s.Settle("lab", true, authDeadline(t)); err != nil {
				t.Fatal(err)
			}
			p.advance = false
			switch name {
			case "incomplete":
				p.complete = false
			case "start_ack":
				p.row.StartDelivery = xfercontract.DeliveryPending
			case "terminal_ack":
				p.row.HistoryReceipts = p.row.HistoryReceipts[:1]
			case "writer":
				p.row.Cleanup.WriterFence.WriterRetired = false
			case "consumer":
				p.row.Cleanup.Consumers = []xfercontract.ReaderState{{Phase: xfercontract.ReaderStopped, DirtyBarrier: true}}
			case "cleanup":
				p.row.Cleanup.OutboxRetirePending = true
			case "digest":
				p.row.CanonicalTerminal.Digest = "wrong"
			case "conflict":
				p.row.EntryNonce = "wrong"
			case "unknown_complete", "unknown_bit", "unknown_code":
				var body map[string]any
				if err := json.Unmarshal(p.row.CanonicalTerminal.Bytes, &body); err != nil {
					t.Fatal(err)
				}
				switch name {
				case "unknown_complete":
					body["kind"] = "complete"
					p.row.CanonicalTerminal.Kind = "complete"
				case "unknown_bit":
					body["outcome_unknown"] = false
				default:
					// The shared audit schema carries the abandonment as its code.
					// origin: INTEG-D session abandonment (no separate reason field)
					body["code"] = "other"
				}
				wire, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				p.row.CanonicalTerminal.Bytes = wire
				sum := sha256.Sum256(wire)
				p.row.CanonicalTerminal.Digest = hex.EncodeToString(sum[:])
				// Even a matching history receipt cannot legalize an unknown
				// outcome that claims complete or omits the explicit warning.
				p.row = settledRow(p.row)
			}
			if err := s.Settle("lab", false, authDeadline(t)); err == nil {
				t.Fatal("unsettled evidence permitted deletion")
			}
		})
	}
	t.Run("failed_persistence", func(t *testing.T) {
		s, p := settlementFixture(t)
		p.abandonErr = errors.New("disk failed")
		p.advance = true
		if err := s.Settle("lab", true, authDeadline(t)); err == nil || !strings.Contains(err.Error(), "abandonment_unconfirmed") {
			t.Fatalf("failed decision write did not retain abandonment responsibility: %v", err)
		}
		if p.row.CanonicalTerminal != nil {
			t.Fatal("failed write installed terminal")
		}
	})
}

// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
func TestSettlementAbandonmentPreservesKnownOutcome(t *testing.T) {
	for _, outcome := range []xfercontract.PrimitiveOutcome{xfercontract.OutcomeLanded, xfercontract.OutcomeNotEntered} {
		t.Run(string(outcome), func(t *testing.T) {
			s, p := settlementFixture(t)
			p.row.ReceiverOutcome = &xfercontract.ReceiverOutcome{Grant: *p.row.Grant, Outcome: outcome, ParentSynced: true}
			if err := s.Settle("lab", true, authDeadline(t)); err == nil {
				t.Fatal("missing exact history permitted deletion")
			}
			if len(p.mutations) != 0 || p.row.OutcomeUnknown {
				t.Fatal("abandonment overwrote a known receiver outcome")
			}
		})
	}
}

// History cannot retire a still-live ctl writer or a reader's consumer.
// origin: S7 internal review rv-wiring F5 / s7-p9m remaining request 3
func TestSettlementAbandonmentRequiresReachableProducerRetirement(t *testing.T) {
	s, p := settlementFixture(t)
	if _, err := p.Abandon(p.row.Identity, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.Settle("lab", true, authDeadline(t)); err == nil {
		t.Fatal("exact abandonment released deletion while the original writer remained live")
	}
	p.advance = true
	if err := s.Settle("lab", true, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: S7 internal review rv-terminal F4 / F5
func TestSettlementPortsEnforceOriginalIdentityAndLedgerValidation(t *testing.T) {
	_, p := settlementFixture(t)
	if _, err := p.Cancel(p.row.Identity, xfercontract.CancelRequest{Reason: xfercontract.CancelRevoked}, authDeadline(t)); err == nil {
		t.Fatal("double accepted a missing original origin")
	}
	next := p.row
	next.Revision++
	next.CanonicalTerminal = &xfercontract.CanonicalRecord{Identity: next.Identity, Kind: "failed", ReqID: "private-invalid-id", Bytes: []byte(`{"kind":"failed"}`)}
	if _, err := p.Update(xfercontract.Key{SID: next.Identity.SID, TransferID: next.Identity.TransferID}, p.row.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationAbandon, EntryNonce: p.row.EntryNonce, Row: next}, authDeadline(t)); err == nil {
		t.Fatal("double bypassed the real canonical ledger validator")
	}
}
