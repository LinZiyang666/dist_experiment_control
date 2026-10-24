package xfergc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The orphan has retained identity, receiver gate and original writer state.
// An age-only candidate has no permission to erase an unowned v1 version.
// origin: hot-update S7 plan §5.3 / T29 / M13
func TestTransferCrossHomeGCPreservesLiveAndReapsOrphan(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseStorageGC(t) })
	}
	// Preserve the original T29 SDK orphan premise as an independent leg. Its
	// retirement must use its own conservative policy, alongside the retained-v1
	// case above.
	t.Run("unmapped_legacy_orphan", exerciseUnmappedLegacyOrphan)
	// Direct after every leg (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "storage-backed GC", goroutines)
	testharness.AssertNoFDLeak(t, "storage-backed GC", fds, 0)
}

func exerciseStorageGC(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	f.store("owners", 0)
	nc, err := nats.Connect(f.server.ClientURL(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(f.ctx, "OBJ_xfer-owners")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "original-follower", InstanceID: "original-process", BootID: "gc-test-boot", Generation: 1}
	start := time.Now().UTC().Round(0)
	now := start.Add(4 * time.Hour)
	base := gcBase(func() time.Time { return now })
	root := t.TempDir()
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(f.ctx, js, "owners", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "owners"})
	if err != nil {
		t.Fatal(err)
	}
	objects := &gcStorageObjects{js: js, states: make(map[x.Identity]*x.WriterState)}
	var live, orphan x.LedgerRow
	var stoppedReceiver x.Receiver
	for _, name := range []string{"live", "orphan"} {
		body := []byte(name + " object")
		id := x.Identity{SID: "owners", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: name, EntryNonce: "nonce" + name, IntentDigest: strings.Repeat("a", 64), ObjectVersion: "version" + name}
		spec := x.ObjectSpec{Identity: id, Bucket: "xfer-owners", Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Size: int64(len(body)), SHA256: gcDigest(body), ChunkSize: 32 << 10, Chunks: 1}
		writer, err := jsstream.NewTransferWriter(js, spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(0, body, gcDeadline(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Finalize(gcDeadline(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Stop(gcDeadline(t, time.Second)) })
		row := x.LedgerRow{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, Revision: 1, Phase: x.PhaseOpen, StartedAt: start,
			Intent:     x.Intent{Verb: "push", Tier: "b", Size: spec.Size, SHA256: spec.SHA256},
			BudgetSeal: x.BudgetSeal{StartedAt: start, Size: spec.Size, EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(300 * time.Second)}, StartDelivery: x.DeliveryPending,
			Permit: x.CreationPermit{Identity: id, Origin: origin, IssuedAt: start, CreateUntil: start.Add(time.Minute), PermitNonce: "permit" + name}}
		row.CanonicalStart = gcStorageRecord(t, row, "start")
		key := identityKey(id)
		row, err = ledger.Update(key, 0, x.LedgerMutation{Kind: x.MutationAdmit, EntryNonce: id.EntryNonce, Row: row}, gcDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if name == "live" {
			live = row
			continue
		}
		if err := writer.Stop(gcDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		state, err := writer.Freeze(gcDeadline(t, time.Second))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := jsstream.NewTransferReader(js, spec, gcEmptyCheckpoint(t))
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(t.TempDir(), "destination")
		vp, err := transferpath.ValidateForWrite(destination, []string{filepath.Dir(destination)}, 0)
		if err != nil {
			t.Fatal(err)
		}
		atomic, err := transferpath.NewAtomicReceiver(vp, "orphan", true)
		if err != nil {
			t.Fatal(err)
		}
		budget := row.BudgetSeal
		budget.SourceSealed = true
		stoppedReceiver, err = xferreceive.New(x.ReceiveConfig{Base: base, Identity: id, Budget: budget, Reader: reader, Atomic: atomic})
		if err != nil {
			t.Fatal(err)
		}
		if err := stoppedReceiver.Stop(gcDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		outcome, err := xferreceive.Outcome(stoppedReceiver)
		if err != nil || outcome.Outcome != x.OutcomeNotEntered || outcome.TmpCleanupPending {
			t.Fatalf("orphan gate did not retire: %+v %v", outcome, err)
		}
		state.RetirementFence.ConsumersRetired = true // The original real reader joined Stop.
		objects.states[id] = &state
		next := row
		next.Revision++
		next.Phase, next.TerminalDelivery = x.PhaseDecided, x.DeliveryPending
		next.ReceiverOutcome = &outcome
		next.CanonicalTerminal = gcStorageRecord(t, next, "failed")
		next.Cleanup = x.CleanupState{Identity: id, ObjectPurgePending: true, PrimaryRetirePending: true, OutboxRetirePending: true, WriterFence: state.RetirementFence}
		orphan, err = ledger.Update(key, row.Revision, x.LedgerMutation{Kind: x.MutationDecide, EntryNonce: id.EntryNonce, Row: next}, gcDeadline(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	decisions := gcStorageDecisions{ledger: ledger}
	resolver := gcStorageReceiver{identity: orphan.Identity, actor: stoppedReceiver}
	finalizer, err := xferfinal.New(x.FinalConfig{Base: base, Decisions: decisions, Ledger: ledger, History: history, Objects: objects, ResolveReceiver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(gcDeadline(t, time.Second)) }()
	owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true, Rows: []x.LedgerRow{live, orphan}}, remote: map[x.Origin]x.LedgerCensus{origin: {Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{live, orphan}}}}
	c := collectorFor(t, f, owners, finalizer, func() time.Time { return now })
	c.born = now.Add(-5 * time.Hour)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	if _, err := c.Pass(gcDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		row  x.LedgerRow
		gone bool
	}{{live, false}, {orphan, true}} {
		_, err := stream.GetLastMsgForSubject(f.ctx, "$O.xfer-owners.C."+expected.row.Identity.ObjectVersion)
		if expected.gone != errors.Is(err, jetstream.ErrMsgNotFound) || (!expected.gone && err != nil) {
			t.Fatalf("physical version %s retirement=%v, err=%v", expected.row.Identity.TransferID, expected.gone, err)
		}
	}
	census, err := ledger.Scan(gcDeadline(t, time.Second))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 1 || census.Rows[0].Identity != live.Identity {
		t.Fatalf("orphan ledger was not retired: %+v %v", census, err)
	}
	set, err := history.Lookup(identityKey(orphan.Identity), gcDeadline(t, time.Second))
	if err != nil || !set.Complete || len(set.Start) != 1 || len(set.Terminal) != 1 {
		t.Fatalf("orphan physical audit is not exact: %+v %v", set, err)
	}
	owners.local.Rows = census.Rows
	owners.remote[origin] = x.LedgerCensus{Origin: origin, Ready: true, Complete: true, Rows: census.Rows}
	// Once the retained v1 row is consumed, its ordinary zero-chunk tombstone
	// must not become a newly unknown legacy orphan on the next pass.
	// origin: hot-update S7 plan §5.3 / T29 / INTEG-R repeated-pass regression
	if next, err := c.Pass(gcDeadline(t, 5*time.Second)); err != nil || !next.Complete {
		t.Fatalf("settled v1 tombstone blocked the next pass: %+v %v", next, err)
	}
	owners.fail = map[x.Origin]error{origin: errors.New("original follower unreachable")}
	if census, err := c.Pass(gcDeadline(t, time.Second)); err == nil || census.Complete {
		t.Fatal("unreachable origin supplied absence proof")
	}
	if _, err := stream.GetLastMsgForSubject(f.ctx, "$O.xfer-owners.C."+live.Identity.ObjectVersion); err != nil {
		t.Fatal(err)
	}
}

func gcEmptyCheckpoint(t *testing.T) x.SinkCheckpoint {
	t.Helper()
	state, err := sha256.New().(interface{ MarshalBinary() ([]byte, error) }).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return x.SinkCheckpoint{SHA256State: state}
}
func gcDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func gcStorageRecord(t *testing.T, row x.LedgerRow, kind string) *x.CanonicalRecord {
	t.Helper()
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: row.Intent.Verb, Tier: row.Intent.Tier, Ts: row.StartedAt, Session: row.Identity.SID, Node: row.Identity.NID, ActorFp: row.Identity.ActorFP, TransferID: row.Identity.TransferID}
	record := audit
	record.AgentInstance, record.EntryNonce = row.Identity.AgentInstance, row.Identity.EntryNonce
	record.IntentDigest, record.ObjectVersion = row.Identity.IntentDigest, row.Identity.ObjectVersion
	if kind != "start" {
		record.TerminalID = gcDigest([]byte("xfer-terminal-v1\n" + row.Identity.SID + "\n" + row.Identity.TransferID + "\n" + row.EntryNonce))
		record.Code, record.Bytes, record.SHA256 = row.ReceiverOutcome.Code, row.ReceiverOutcome.Bytes, row.ReceiverOutcome.SHA256
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return &x.CanonicalRecord{Identity: row.Identity, Kind: kind, Bytes: data, Digest: gcDigest(data), ReqID: gcDigest(append([]byte("xferaudit-record:"), data...))}
}

type gcStorageReceiver struct {
	identity x.Identity
	actor    x.Receiver
}

func (r gcStorageReceiver) Resolve(id x.Identity, _ x.Deadline) (x.ReceiverOutcome, error) {
	if id != r.identity {
		return x.ReceiverOutcome{}, errors.New("receiver identity differs")
	}
	return xferreceive.Outcome(r.actor)
}

type gcStorageObjects struct {
	js     jetstream.JetStream
	states map[x.Identity]*x.WriterState
}

func (o *gcStorageObjects) Retire(id x.Identity, proof x.RetirementProof, d x.Deadline) (x.CleanupReceipt, error) {
	if !proof.ReceiverRetired {
		return x.CleanupReceipt{}, errors.New("receiver is not retired")
	}
	receipt, err := jsstream.RetireTransferObject(o.js, o.states[id], proof, d)
	if err == nil {
		receipt.TmpUnlinked, receipt.DirectorySynced = true, true
	}
	return receipt, err
}

// Boundary messages persist through the real revision store; this adapter
// carries no in-memory replacement decision and never manufactures an ACK.
type gcStorageDecisions struct{ ledger x.LedgerPort }

func (d gcStorageDecisions) Read(key x.Key, deadline x.Deadline) (x.LedgerRow, error) {
	census, err := d.ledger.Scan(deadline)
	if err != nil || !census.Ready || !census.Complete {
		return x.LedgerRow{}, errors.Join(err, errors.New("incomplete ledger census"))
	}
	for _, row := range census.Rows {
		if identityKey(row.Identity) == key {
			return row, nil
		}
	}
	return x.LedgerRow{}, errors.New("ledger row absent")
}
func (d gcStorageDecisions) Pending(deadline x.Deadline) ([]x.LedgerRow, error) {
	census, err := d.ledger.Scan(deadline)
	return census.Rows, err
}
func (d gcStorageDecisions) AcceptHistory(key x.Key, nonce string, rev uint64, r x.HistoryReceipt, deadline x.Deadline) error {
	row, err := d.Read(key, deadline)
	if err != nil {
		return err
	}
	row.Revision++
	row.HistoryReceipts = append(row.HistoryReceipts, r)
	if r.RecordDigest == row.CanonicalStart.Digest {
		row.StartDelivery = x.DeliveryHistoryConfirmed
	} else {
		row.TerminalDelivery = x.DeliveryHistoryConfirmed
	}
	if row.StartDelivery == x.DeliveryHistoryConfirmed && row.TerminalDelivery == x.DeliveryHistoryConfirmed {
		row.Phase = x.PhaseHistoryConfirmed
	}
	_, err = d.ledger.Update(key, rev, x.LedgerMutation{Kind: x.MutationHistory, EntryNonce: nonce, Row: row}, deadline)
	return err
}
func (d gcStorageDecisions) AcceptCleanup(key x.Key, nonce string, rev uint64, r x.CleanupReceipt, deadline x.Deadline) error {
	row, err := d.Read(key, deadline)
	if err != nil {
		return err
	}
	row.Revision++
	row.Phase = x.PhaseCleanupPending
	row.Cleanup.ObjectPurgePending, row.Cleanup.TmpUnlinkPending, row.Cleanup.DirectorySyncPending = false, false, false
	row.Cleanup.WriterFence.Purged = true
	row.Cleanup.WriterFence.Receipt = &x.HistoryReceipt{Stream: "object", Sequence: r.FenceSequence, RecordDigest: r.Digest}
	_, err = d.ledger.Update(key, rev, x.LedgerMutation{Kind: x.MutationCleanup, EntryNonce: nonce, Row: row}, deadline)
	return err
}

// The additive legacy port carries the immutable candidate to the real
// finalizer. No SDK Delete or fabricated ledger row supplies retirement proof.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-E frozen orphan-port blocker
func exerciseUnmappedLegacyOrphan(t *testing.T) {
	f := newGCFixture(t)
	defer f.close()
	store := f.store("owners", 0)
	live, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "live"}, bytes.NewReader([]byte("original live object")))
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := store.Put(f.ctx, jetstream.ObjectMeta{Name: "orphan"}, bytes.NewReader([]byte("orphan object")))
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "original-follower", InstanceID: "original-process", BootID: "gc-test-boot", Generation: 1}
	row := testGCRow(origin, "live", live.NUID, x.PhaseSending)
	now := row.StartedAt.Add(4 * time.Hour)
	row.BudgetSeal.StartedAt = now.Add(-time.Minute)
	base := gcBase(func() time.Time { return now })
	root := t.TempDir()
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(f.ctx, f.js, "owners", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: f.js, SID: "owners"})
	if err != nil {
		t.Fatal(err)
	}
	owners := &censusOwners{local: x.LedgerCensus{Ready: true, Complete: true, Rows: []x.LedgerRow{row}}, remote: map[x.Origin]x.LedgerCensus{origin: {Origin: origin, Ready: true, Complete: true, Rows: []x.LedgerRow{row}}}}
	finalizer, err := xferfinal.New(x.FinalConfig{Base: base, LegacyOrphans: &x.LegacyOrphanConfig{JS: f.js, Owners: owners}, Decisions: gcStorageDecisions{ledger: ledger}, Ledger: ledger, History: history, Objects: &gcStorageObjects{js: f.js, states: map[x.Identity]*x.WriterState{}}, ResolveReceiver: gcStorageReceiver{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(gcDeadline(t, time.Second)) }()
	c := collectorFor(t, f, owners, finalizer, func() time.Time { return now })
	c.born = now.Add(-5 * time.Hour)
	defer func() { _ = c.Stop(gcDeadline(t, time.Second)) }()
	// Refuse the post-fence census once. The next real GC pass must discover
	// the durable legacy tombstone and finish its retained physical cleanup.
	// origin: hot-update S7 plan §5.3 / T29 / INTEG-R legacy retry regression
	owners.failAfterLocal = 3
	if pending, err := c.Pass(gcDeadline(t, 5*time.Second)); err == nil || pending.Complete {
		t.Fatal("lost post-fence census acknowledged legacy cleanup")
	}
	_, passErr := c.Pass(gcDeadline(t, 5*time.Second))
	stream, err := f.js.Stream(f.ctx, "OBJ_xfer-owners")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetLastMsgForSubject(f.ctx, "$O.xfer-owners.C."+live.NUID); err != nil {
		t.Fatalf("live old-age version disappeared: %v", err)
	}
	if _, err := stream.GetLastMsgForSubject(f.ctx, "$O.xfer-owners.C."+orphan.NUID); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("legacy orphan physical chunks remain; pass=%v raw=%v", passErr, err)
	}
	if passErr != nil {
		t.Fatal(passErr)
	}
	owners.fail = map[x.Origin]error{origin: errors.New("original follower unreachable")}
	if census, err := c.Pass(gcDeadline(t, time.Second)); err == nil || census.Complete {
		t.Fatal("unreachable origin supplied absence proof")
	}
	if _, err := store.GetInfo(f.ctx, "live"); err != nil {
		t.Fatal(err)
	}
}
