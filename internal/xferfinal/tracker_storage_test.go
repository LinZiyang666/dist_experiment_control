package xferfinal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Actual tracker canonical bytes must satisfy the actual durable store and
// history guard. Port probes alone cannot detect a schema seam between owners.
// origin: hot-update S7 plan §4–§5 / T13 / INTEG-E storage regression
func TestTransferTrackerPublishesCanonicalHistoryToRealStorage(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTrackerStorage(t) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "tracker durable history", goroutines)
	testharness.AssertNoFDLeak(t, "tracker durable history", fds, 0)
}
func exerciseTrackerStorage(t *testing.T) {
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := jsstream.EnsureHistoryStream(t.Context(), js, "session", jsstream.ReplicasSingle); err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	base := x.Base{Clock: fixtureClock, UTC: time.Now, Capacity: &storageCapacity{held: map[x.ReservationID]bool{}}}
	root := t.TempDir()
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	f := newFinalFixture(t, "originalnuid")
	f.row.Identity.IntentDigest = strings.Repeat("a", 64)
	f.row.Permit.Identity = f.row.Identity
	manager, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: f.row.Origin, Ledger: ledger, History: history})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Stop(finalDeadline(t, time.Second)); err != nil {
			t.Error(err)
		}
	}()
	now := time.Now()
	permit := f.row.Permit
	permit.IssuedAt, permit.CreateUntil = now, now.Add(time.Minute)
	intent := f.row.Intent
	intent.Path = "destination"
	status, err := manager.Admit(permit, intent, finalDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatalf("real durable START admission: %v", err)
	}
	if status.Identity != permit.Identity {
		t.Fatal("admission changed identity")
	}
	seal := x.SourceSeal{Size: 3, SHA256: fixtureSourceSHA, ChunkSize: 32 << 10, Chunks: 1, SourceFingerprint: "original-source"}
	if _, err := manager.SourceSeal(permit.Identity, seal, finalDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	grant, err := manager.Reserve(permit.Identity, x.ReserveRequest{Origin: permit.Origin, ReceiverInstance: "receiver"}, finalDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	view, err := xfertrack.Decisions(manager)
	if err != nil {
		t.Fatal(err)
	}
	startRow, err := view.Read(x.Key{SID: permit.Identity.SID, TransferID: permit.Identity.TransferID}, finalDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"agent_instance", "intent_digest", "continuation"} {
		bad := *startRow.CanonicalStart
		var fields map[string]any
		if err := json.Unmarshal(bad.Bytes, &fields); err != nil {
			t.Fatal(err)
		}
		if field == "continuation" {
			wrong := startRow.Identity
			wrong.EntryNonce = "foreign"
			fields[field] = wrong
		} else {
			fields[field] = ""
		}
		bad.Bytes, _ = json.Marshal(fields)
		bad.Digest = storageDigest(bad.Bytes)
		bad.ReqID = storageDigest(append([]byte("xferaudit-record:"), bad.Bytes...))
		if receipt, err := history.Confirm(bad, finalDeadline(t, time.Second)); !errors.Is(err, xferhistory.ErrConflict) || receipt.Sequence != 0 {
			t.Fatalf("absent malformed START %s was acknowledged: %+v %v", field, receipt, err)
		}
	}
	actor, state := trackerStorageReceiver(t, js, permit.Identity, status.Budget, grant)
	outcome, err := xferreceive.Outcome(actor)
	if err != nil {
		t.Fatal(err)
	}
	first, err := manager.Finish(permit.Identity, outcome, finalDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatalf("actual canonical history publication: %v", err)
	}
	again, err := manager.Finish(permit.Identity, outcome, finalDeadline(t, 5*time.Second))
	if err != nil || first != again {
		t.Fatalf("exact terminal replay changed: %+v %+v %v", first, again, err)
	}
	census, err := ledger.Scan(finalDeadline(t, time.Second))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 1 {
		t.Fatalf("durable decision census: %+v %v", census, err)
	}
	row := census.Rows[0]
	// Replayed history acknowledgement must not replenish result retention.
	renewed := row
	renewed.Revision++
	renewed.Retention.ResultUntil = renewed.Retention.ResultUntil.Add(time.Minute)
	renewed.Retention.Deadline.Deadline += x.Deadline(time.Minute)
	if _, err := ledger.Update(x.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}, row.Revision, x.LedgerMutation{Kind: x.MutationHistory, EntryNonce: row.EntryNonce, Row: renewed}, finalDeadline(t, time.Second)); err == nil {
		t.Fatal("history replay renewed the original result retention")
	}

	set, err := history.Lookup(x.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}, finalDeadline(t, 5*time.Second))
	if err != nil || !set.Complete || len(set.Start) != 1 || len(set.Terminal) != 1 || set.Start[0].Digest != row.CanonicalStart.Digest || set.Terminal[0].Digest != row.CanonicalTerminal.Digest {
		t.Fatalf("tracker history is not physically exact: %+v %v", set, err)
	}
	// Partial flat identity and conflicting nested identity must not turn
	// malformed v1 bytes into a legacy record or a second terminal receipt.
	for _, field := range []string{"agent_instance", "intent_digest", "terminal_id", "continuation"} {
		bad := *row.CanonicalTerminal
		var fields map[string]any
		if err := json.Unmarshal(bad.Bytes, &fields); err != nil {
			t.Fatal(err)
		}
		if field == "continuation" {
			wrong := row.Identity
			wrong.EntryNonce = "foreign"
			fields[field] = wrong
		} else {
			fields[field] = ""
		}
		bad.Bytes, _ = json.Marshal(fields)
		bad.Digest = storageDigest(bad.Bytes)
		bad.ReqID = storageDigest(append([]byte("xferaudit-record:"), bad.Bytes...))
		if receipt, err := history.Confirm(bad, finalDeadline(t, time.Second)); !errors.Is(err, xferhistory.ErrConflict) || receipt.Sequence != 0 {
			t.Fatalf("malformed continuation %s was acknowledged: %+v %v", field, receipt, err)
		}
	}
	if row.StartDelivery != x.DeliveryHistoryConfirmed || row.TerminalDelivery != x.DeliveryHistoryConfirmed || len(row.HistoryReceipts) != 2 || row.Retention.ResultUntil.IsZero() {
		t.Fatalf("history responsibility was not persisted: %+v", row)
	}
	// The exact history ACK precedes actual object retirement. The resulting
	// receipt then traverses the real tracker callback and finalizer consume path.
	proof := x.RetirementProof{Identity: row.Identity, Origin: row.Origin, OwnerGeneration: row.Origin.Generation, EntryNonce: row.EntryNonce, Revision: row.Revision, CreationUntil: row.Permit.CreateUntil, WriterRetired: true, ConsumersRetired: true, ReceiverRetired: true}
	cleanup, err := jsstream.RetireTransferObject(js, &state, proof, finalDeadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cleanup.TmpUnlinked, cleanup.DirectorySynced = !outcome.TmpCleanupPending, outcome.ParentSynced
	key := x.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}
	if err := view.AcceptCleanup(key, row.EntryNonce, row.Revision, cleanup, finalDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	cleaned, err := view.Read(key, finalDeadline(t, time.Second))
	if err != nil || cleaned.Cleanup.WriterFence.Receipt == nil || cleaned.Cleanup.WriterFence.Receipt.Sequence != cleanup.FenceSequence {
		t.Fatalf("tracker omitted the actual retirement fence receipt: %+v %v", cleaned.Cleanup.WriterFence, err)
	}
	finalizer, err := New(Config{Base: base, Decisions: view, Ledger: ledger, History: history, Objects: &storageObjects{js: js, state: state}, ResolveReceiver: storageReceiver{actor}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(finalDeadline(t, time.Second)) }()
	settled, err := finalizer.Advance(row.Identity, finalDeadline(t, 5*time.Second))
	if err != nil || settled.Phase != x.PhaseSettled || settled.CleanupPending {
		t.Fatalf("real tracker finalizer did not settle: %+v %v", settled, err)
	}
	census, err = ledger.Scan(finalDeadline(t, time.Second))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 0 {
		t.Fatalf("real finalizer retained ledger rows: %+v %v", census, err)
	}

}

type storageCapacity struct {
	mu   sync.Mutex
	next int
	held map[x.ReservationID]bool
}

func (c *storageCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	id := x.ReservationID(fmt.Sprint(c.next))
	c.held[id] = true
	return id, nil
}
func (c *storageCapacity) Release(id x.ReservationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.held[id] {
		return fmt.Errorf("reservation %s is not held", id)
	}
	delete(c.held, id)
	return nil
}

func storageDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func trackerStorageReceiver(t *testing.T, js jetstream.JetStream, id x.Identity, budget x.BudgetSeal, grant x.CommitGrant) (x.Receiver, x.WriterState) {
	t.Helper()
	bucket := "xfer-" + id.SID
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: id, Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Size: 3, SHA256: fixtureSourceSHA, ChunkSize: 32 << 10, Chunks: 1}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, []byte("new"), finalDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Finalize(finalDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Stop(finalDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	state, err := writer.Freeze(finalDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	hashState, err := sha256.New().(interface{ MarshalBinary() ([]byte, error) }).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := jsstream.NewTransferReader(js, spec, x.SinkCheckpoint{SHA256State: hashState})
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "destination")
	vp, err := transferpath.ValidateForWrite(destination, []string{filepath.Dir(destination)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	atomic, err := transferpath.NewAtomicReceiver(vp, "original", true)
	if err != nil {
		t.Fatal(err)
	}
	budget.SourceSealed = true
	actor, err := xferreceive.New(x.ReceiveConfig{Base: x.Base{Clock: fixtureClock}, Identity: id, Budget: budget, Reader: reader, Atomic: atomic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = actor.Stop(finalDeadline(t, time.Second)) })
	if err := actor.Reserve(grant); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 3; step++ {
		err := actor.Step(finalDeadline(t, 5*time.Second))
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := xferreceive.Outcome(actor)
	if err != nil || out.Outcome != x.OutcomeLanded || !out.ParentSynced || out.Bytes != 3 || out.SHA256 != fixtureSourceSHA {
		t.Fatalf("original receiver did not land exactly: %+v %v", out, err)
	}
	if err := actor.Stop(finalDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	state.RetirementFence.ConsumersRetired = true
	return actor, state
}

type storageReceiver struct{ actor x.Receiver }

func (r storageReceiver) Resolve(id x.Identity, _ x.Deadline) (x.ReceiverOutcome, error) {
	out, err := xferreceive.Outcome(r.actor)
	if err == nil && out.Grant.Identity != id {
		return out, errors.New("receiver identity changed")
	}
	return out, err
}
