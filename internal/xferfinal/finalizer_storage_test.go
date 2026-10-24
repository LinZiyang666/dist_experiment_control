package xferfinal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The storage leg uses exact history and both durable directories; its
// boundary adapter delegates physical retirement to the actual P02 primitive.
// origin: hot-update S7 plan §5.1 / T21 / INTEG-E storage integration
func exerciseRealFinalizerRetirement(t *testing.T) {
	deadline := finalDeadline(t, 5*time.Second)
	root := t.TempDir()
	ledger, err := xferledger.New(xfercontract.LedgerConfig{Base: xfercontract.Base{Clock: fixtureClock},
		Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: bounded real-leaf test fixture.
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, js, "session", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(xfercontract.HistoryConfig{Base: xfercontract.Base{Clock: fixtureClock}, JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	f := newFinalFixture(t, "originalnuid")
	bucket := proto.XferBucketName("session")
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The capable writer produces the real original NUID; SDK Put is never a
	// stand-in for the pending-attempt/fence implementation under test.
	payload := []byte("new")
	sha := sha256.Sum256(payload)
	writer, err := jsstream.NewTransferWriter(js, xfercontract.ObjectSpec{Identity: f.row.Identity, Bucket: bucket,
		Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Size: int64(len(payload)),
		SHA256: hex.EncodeToString(sha[:]), ChunkSize: 32 * 1024, Chunks: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, payload, deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finalize(deadline); err != nil {
		t.Fatal(err)
	}
	if err := writer.Stop(deadline); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Freeze(deadline)
	if err != nil {
		t.Fatal(err)
	}
	state.RetirementFence.ConsumersRetired = true // No reader was created in this storage leg.
	f.row.Cleanup.WriterFence = state.RetirementFence
	// Admit before staging the exact decision so real revision monotonicity is
	// exercised, rather than writing a test JSON row behind the store's back.
	initial := f.row
	initial.CanonicalTerminal, initial.Grant, initial.ReceiverOutcome = nil, nil, nil
	initial.Phase = xfercontract.PhaseOpen
	initial.TerminalDelivery = ""
	initial.Revision = 1
	key := xfercontract.Key{SID: initial.Identity.SID, TransferID: initial.Identity.TransferID}
	row, err := ledger.Update(key, 0, xfercontract.LedgerMutation{Kind: xfercontract.MutationAdmit, EntryNonce: initial.EntryNonce, Row: initial}, deadline)
	if err != nil {
		t.Fatal(err)
	}
	reserved := row
	reserved.Grant, reserved.Phase, reserved.Revision = f.row.Grant, xfercontract.PhaseCommitReserved, row.Revision+1
	row, err = ledger.Update(key, row.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationReserve, EntryNonce: row.EntryNonce, Row: reserved}, deadline)
	if err != nil {
		t.Fatal(err)
	}
	decided := f.row
	decided.Revision = row.Revision + 1
	primary := filepath.Join(root, "primary")
	if err := os.Chmod(primary, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(primary, 0700) })
	row, err = ledger.Update(key, row.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationDecide, EntryNonce: row.EntryNonce, Row: decided}, deadline)
	if restoreErr := os.Chmod(primary, 0700); restoreErr != nil {
		t.Fatal(restoreErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []*xfercontract.CanonicalRecord{row.CanonicalStart, row.CanonicalTerminal} {
		if _, err := history.Confirm(*record, deadline); err != nil {
			t.Fatal(err)
		}
	}
	decisions := ledgerDecisions{ledger}
	cfg := f.config()
	retirement := &storageRetirementFault{LedgerPort: ledger, primary: primary, fail: true}
	cfg.Decisions, cfg.Ledger, cfg.History, cfg.Objects = decisions, retirement, history, &storageObjects{js: js, state: state}
	finalizer, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(finalDeadline(t, time.Second)) }()
	status, err := finalizer.Advance(row.Identity, deadline)
	if err == nil || status.Phase == xfercontract.PhaseSettled {
		t.Fatalf("primary unlink failure erased responsibility: %+v %v", status, err)
	}
	outboxFiles, readErr := os.ReadDir(filepath.Join(root, "outbox"))
	if readErr != nil || len(outboxFiles) != 1 {
		t.Fatalf("primary failure lost real outbox barrier: %v %v", outboxFiles, readErr)
	}
	barrier, readErr := os.ReadFile(filepath.Join(root, "outbox", outboxFiles[0].Name()))
	if readErr != nil || !bytes.Contains(barrier, []byte(row.CanonicalTerminal.Digest)) {
		t.Fatalf("outbox no longer holds the exact terminal: %v", readErr)
	}
	retirement.fail = false
	status, err = finalizer.Advance(row.Identity, deadline)

	if err != nil || status.Phase != xfercontract.PhaseSettled {
		t.Fatalf("real staged terminal did not settle: %+v %v", status, err)
	}
	census, err := ledger.Scan(deadline)
	if err != nil || !census.Complete || !census.Ready || len(census.Rows) != 0 {
		t.Fatalf("real primary/outbox consumption incomplete: %+v %v", census, err)
	}
	set, err := history.Lookup(key, deadline)
	if err != nil || !set.Complete || len(set.Start) != 1 || len(set.Terminal) != 1 ||
		set.Terminal[0].Digest != decided.CanonicalTerminal.Digest {
		t.Fatalf("real physical history changed on retirement: %+v %v", set, err)
	}
	objectSubject := "$O." + bucket + ".C." + row.Identity.ObjectVersion
	if _, err := stream.GetLastMsgForSubject(ctx, objectSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("real original NUID was not purged: %v", err)
	}
}

// Test-only adapter translates the frozen revision-message interface to the
// real store; it does not stand in for its durability or conflict checks.
type ledgerDecisions struct{ ledger xfercontract.LedgerPort }

func (d ledgerDecisions) Read(key xfercontract.Key, deadline controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	census, err := d.ledger.Scan(deadline)
	if err != nil || !census.Ready || !census.Complete {
		return xfercontract.LedgerRow{}, errors.Join(err, errors.New("ledger census is incomplete"))
	}
	for _, row := range census.Rows {
		if row.Identity.SID == key.SID && row.Identity.TransferID == key.TransferID {
			return row, nil
		}
	}
	return xfercontract.LedgerRow{}, errors.New("ledger row is absent")
}

func (d ledgerDecisions) Pending(deadline controlbudget.Deadline) ([]xfercontract.LedgerRow, error) {
	census, err := d.ledger.Scan(deadline)
	return census.Rows, err
}

func (d ledgerDecisions) AcceptHistory(key xfercontract.Key, nonce string, revision uint64, receipt xfercontract.HistoryReceipt, deadline controlbudget.Deadline) error {
	row, err := d.Read(key, deadline)
	if err != nil {
		return err
	}
	row.HistoryReceipts = append(row.HistoryReceipts, receipt)
	row.Revision++
	if receipt.RecordDigest == row.CanonicalStart.Digest {
		row.StartDelivery = xfercontract.DeliveryHistoryConfirmed
	} else {
		row.TerminalDelivery = xfercontract.DeliveryHistoryConfirmed
	}
	if row.StartDelivery == xfercontract.DeliveryHistoryConfirmed && row.TerminalDelivery == xfercontract.DeliveryHistoryConfirmed {
		row.Phase = xfercontract.PhaseHistoryConfirmed
	}
	_, err = d.ledger.Update(key, revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationHistory, EntryNonce: nonce, Row: row}, deadline)
	return err
}

func (d ledgerDecisions) AcceptCleanup(key xfercontract.Key, nonce string, revision uint64, receipt xfercontract.CleanupReceipt, deadline controlbudget.Deadline) error {
	row, err := d.Read(key, deadline)
	if err != nil {
		return err
	}
	row.Revision++
	row.Phase = xfercontract.PhaseCleanupPending
	row.Cleanup.ObjectPurgePending, row.Cleanup.DirectorySyncPending, row.Cleanup.TmpUnlinkPending = false, false, false
	row.Cleanup.WriterFence.WriterRetired, row.Cleanup.WriterFence.ConsumersRetired, row.Cleanup.WriterFence.Purged = receipt.WriterRetired, receipt.ConsumersRetired, receipt.ObjectPurged
	row.Cleanup.WriterFence.Receipt = &xfercontract.HistoryReceipt{Stream: "object", Sequence: receipt.FenceSequence, RecordDigest: receipt.Digest}
	_, err = d.ledger.Update(key, revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationCleanup, EntryNonce: nonce, Row: row}, deadline)
	return err
}

// Only retained state may provide StreamCreated. Reading current INFO to invent
// the original incarnation would authorize deletion after stream recreation.
type storageObjects struct {
	js    jetstream.JetStream
	state xfercontract.WriterState
}

func (o *storageObjects) Retire(id xfercontract.Identity, proof xfercontract.RetirementProof, d controlbudget.Deadline) (xfercontract.CleanupReceipt, error) {
	if id != o.state.ObjectSpec.Identity || !proof.ReceiverRetired {
		return xfercontract.CleanupReceipt{}, errors.New("receiver retirement evidence missing")
	}
	receipt, err := jsstream.RetireTransferObject(o.js, &o.state, proof, d)
	if err != nil {
		return receipt, err
	}
	// Finalizer has already resolved a landed, directory-synced, unlinked outcome.
	receipt.TmpUnlinked, receipt.DirectorySynced = proof.ReceiverRetired, proof.ReceiverRetired
	return receipt, nil
}

// Fail the real primary unlink after the store installs its outbox barrier.
// Both directories remain readable, so failure cannot hide conflicting rows.
type storageRetirementFault struct {
	xfercontract.LedgerPort
	primary string
	fail    bool
}

func (f *storageRetirementFault) Retire(key xfercontract.Key, nonce string, proof xfercontract.RetirementProof, d controlbudget.Deadline) error {
	if !f.fail {
		return f.LedgerPort.Retire(key, nonce, proof, d)
	}
	if err := os.Chmod(f.primary, 0500); err != nil {
		return err
	}
	defer func() { _ = os.Chmod(f.primary, 0700) }()
	return f.LedgerPort.Retire(key, nonce, proof, d)
}
