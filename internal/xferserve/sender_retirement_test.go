package xferserve_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xferserve"
	"github.com/nats-io/nats.go"
)

// Real: production canonical encoding, Objects.Retire and Link.DeliverReceipt.
// Simulated: a retained decision row and the original connection still offline.
// origin: S7 internal review FIX-TIERA2 F5
func TestTransferPullRetirementRetainsUnacknowledgedSender(t *testing.T) {
	row, receipt := pullRetirementDecision(t)
	id, origin := row.Identity, row.Origin
	rows := func(got x.Identity, _ x.Deadline) (x.LedgerRow, error) {
		if got != id {
			t.Fatal("retirement changed identity")
		}
		return row, nil
	}
	objects := xferserve.Objects{Rows: rows, Link: &xferserve.Link{Rows: rows, Conn: func() *nats.Conn { return nil }}, UTC: time.Now}
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _, deliveryErr := objects.Link.DeliverReceipt(id, origin, receipt, d)
	if deliveryErr == nil {
		t.Fatal("counterexample did not lose the sender receipt delivery")
	}
	cleanup, err := objects.Retire(id, x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, ReceiverRetired: true, OwnerGeneration: 1}, d)
	if err == nil {
		t.Fatalf("retirement consumed sender responsibility after its exact receipt delivery failed: writer_retired=%t digest=%s", cleanup.WriterRetired, cleanup.Digest)
	}
}

func pullRetirementDecision(t *testing.T) (x.LedgerRow, x.TerminalReceipt) {
	t.Helper()
	intent := x.Intent{Verb: "pull", Tier: "a", Path: "source", Force: true}
	id := x.Identity{SID: "session", NID: "node", TransferID: "transfer", EntryNonce: "entry", AgentInstance: "agent", ActorFP: "actor", IntentDigest: xfercontrol.IntentDigest(intent)}
	origin := x.Origin{BrokerID: "broker", InstanceID: "process", BootID: "boot", Generation: 1}
	sum := sha256.Sum256([]byte("x"))
	digest := hex.EncodeToString(sum[:])
	grant := x.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "ctl-receiver", TerminalID: schema.TransferTerminalID(id.SID, id.TransferID, id.EntryNonce)}
	outcome := x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded, ParentSynced: true, Bytes: 1, SHA256: digest}
	started := time.Now().UTC().Add(-time.Second)
	terminal, err := schema.CanonicalTransferAudit(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", Verb: "pull", Ts: started.Add(time.Second), Session: id.SID, Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID, Path: intent.Path, Size: 1, Bytes: 1, SHA256: digest, Tier: "a", DurationMs: 1000, AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce, IntentDigest: id.IntentDigest, TerminalID: grant.TerminalID})
	if err != nil {
		t.Fatal(err)
	}
	history := x.HistoryReceipt{Stream: "history", Sequence: 2, RecordDigest: terminal.Digest}
	row := x.LedgerRow{Identity: id, EntryNonce: id.EntryNonce, Origin: origin, Intent: intent, Revision: 4, StartedAt: started, Phase: x.PhaseCleanupPending, Grant: &grant, ReceiverOutcome: &outcome, CanonicalTerminal: &terminal, HistoryReceipts: []x.HistoryReceipt{history}, SourceSeal: &x.SourceSeal{Size: 1, SHA256: digest, SourceFingerprint: "pinned-source"}, BudgetSeal: x.BudgetSeal{StartedAt: started, Size: 1, SourceSealed: true, EffectiveBudgetNS: int64(30 * time.Second), RemainingNS: int64(29 * time.Second)}}
	receipt := x.TerminalReceipt{TerminalID: grant.TerminalID, Kind: "complete", CanonicalDigest: terminal.Digest, ReqID: terminal.ReqID, HistoryStream: history.Stream, HistorySeq: history.Sequence, Bytes: outcome.Bytes, SHA256: outcome.SHA256}
	return row, receipt
}

// Administrative abandonment records an unknown outcome and releases the
// settlement barrier explicitly; it cannot require the unreachable agent's ACK.
// Tier A has no OBJ publisher or late chunk to fence. Ordinary cleanup above
// still requires the original sender's exact retirement acknowledgment.
// origin: S7 internal review PULL-B F10 / hot-update S7 plan lead LQ1 / §6.4
func TestTransferAbandonedInlinePullRetiresWithoutUnreachableSender(t *testing.T) {
	row, _ := pullRetirementDecision(t)
	var audit schema.AuditTransfer
	if err := json.Unmarshal(row.CanonicalTerminal.Bytes, &audit); err != nil {
		t.Fatal(err)
	}
	audit.Kind, audit.Code, audit.OutcomeUnknown = "failed", x.ReasonSettlementAbandoned, true
	terminal, err := schema.CanonicalTransferAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	row.CanonicalTerminal, row.OutcomeUnknown, row.ReceiverOutcome = &terminal, true, nil
	row.HistoryReceipts[0].RecordDigest = terminal.Digest
	objects := xferserve.Objects{Rows: func(x.Identity, x.Deadline) (x.LedgerRow, error) { return row, nil }}
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := row.Identity
	cleanup, err := objects.Retire(id, x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, ReceiverRetired: true, OwnerGeneration: 1}, d)
	if err != nil || cleanup.Identity != id || cleanup.Digest == "" {
		t.Fatalf("explicit outcome-unknown abandonment waited for the unreachable sender: %+v %v", cleanup, err)
	}
}

// Real: exact production receipt encoding and bounded NATS delivery/replay.
// Simulated: retained ledger and agent replies with explicit retirement faults.
// origin: S7 internal review FIX-TIERA2 F5 / hot-update S7 plan §4 / §10.4 / T46
func TestTransferPullRetirementRetriesExactSenderReceipt(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exercisePullRetirementRetry(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "pull receipt retry", g)
	testharness.AssertNoFDLeak(t, "pull receipt retry", fd, 0)
}

func exercisePullRetirementRetry(t *testing.T) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	row, receipt := pullRetirementDecision(t)
	id := row.Identity
	rows := func(x.Identity, x.Deadline) (x.LedgerRow, error) { return row, nil }
	objects := xferserve.Objects{Rows: rows, Link: &xferserve.Link{Rows: rows, Conn: func() *nats.Conn { return nc }}}
	proof := x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, ReceiverRetired: true, OwnerGeneration: 1}
	var selected atomic.Int32
	requests := make(chan []byte, 10)
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(id.SID, id.NID, proto.TransferLinkVerb), func(m *nats.Msg) {
		requests <- append([]byte(nil), m.Data...)
		if selected.Load() == 0 {
			return // The agent's receipt ACK is lost, not replaced by a refusal.
		}
		status := &proto.TransferStatusResp{State: proto.StatusState(x.StatusTerminal), Phase: proto.Phase(x.PhaseSettled), Identity: proto.Identity(id), Origin: proto.Origin(row.Origin)}
		wireReceipt := proto.TerminalReceipt(receipt)
		status.Terminal = &wireReceipt
		switch selected.Load() {
		case 1:
			status.Phase = proto.Phase(x.PhaseCleanupPending)
		case 2:
			status.Identity.EntryNonce = "replacement"
		case 3:
			status.Origin.Generation++
		case 4:
			status.Terminal.CanonicalDigest = "opposite"
		case 5:
			status.CleanupPending = true
		}
		body, encodeErr := json.Marshal(proto.TransferCommitResp{Link: &proto.TransferLinkResult{Version: x.Version, InstanceID: id.AgentInstance, Status: status}})
		if encodeErr != nil {
			t.Error(encodeErr)
			return
		}
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	var first []byte
	for fault := int32(0); fault <= 6; fault++ {
		selected.Store(fault)
		d, err := controlbudget.After(time.Second)
		if fault == 0 {
			d, err = controlbudget.After(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		cleanup, retireErr := objects.Retire(id, proof, d)
		if fault < 6 && retireErr == nil {
			t.Fatalf("sender retirement fault %d consumed responsibility: %+v", fault, cleanup)
		}
		if fault == 6 && (retireErr != nil || !cleanup.WriterRetired || cleanup.Digest == "") {
			t.Fatalf("exact same-entry retry did not retire after recovery: %+v %v", cleanup, retireErr)
		}
		select {
		case request := <-requests:
			if first == nil {
				first = request
			} else if !bytes.Equal(first, request) {
				t.Fatal("retirement retry changed the original receipt or CallID")
			}
		case <-time.After(time.Second):
			t.Fatal("retirement did not retry its original agent")
		}
	}
}
