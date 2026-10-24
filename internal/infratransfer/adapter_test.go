package infratransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xferworker"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func deadline(t *testing.T) i.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func config() Config {
	return Config{SourceSHA256: sha256.Sum256([]byte("fixture-source-receipt")), SchemaSHA256: ObservationSchema(),
		BeforeReplay: func(i.ID, i.Deadline) error { return nil }}
}
func mustAdapter(t *testing.T, c Config) *Adapter {
	t.Helper()
	a, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func newServer(t *testing.T) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		s.WaitForShutdown()
		t.Fatal("embedded server did not start")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s
}
func connect(t *testing.T, s *server.Server) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), nats.ReconnectBufSize(-1), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}
func specification(t *testing.T, js jetstream.JetStream, body []byte) x.ObjectSpec {
	t.Helper()
	_, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "original", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_original")
	if err != nil {
		t.Fatal(err)
	}
	return x.ObjectSpec{Identity: identity(), Bucket: "original", Stream: "OBJ_original",
		StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano), SHA256: hexDigest(body),
		Size: int64(len(body)), ChunkSize: 32 << 10, Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
}
func identity() x.Identity {
	return x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "originalTransfer",
		EntryNonce: "originalNonce", IntentDigest: hexDigest([]byte("intent")), ObjectVersion: "originalNUID"}
}
func hexDigest(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }

// A real publish lands on the embedded server; only its definite ACK is lost.
// This does not replace a server-side request/effect or fabricate a receipt.
type lostACK struct {
	jetstream.JetStream
	subject string
	hit     bool
}

func (j *lostACK) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	if err == nil && msg.Subject == j.subject && !j.hit {
		j.hit = true
		return nil, context.DeadlineExceeded
	}
	return ack, err
}

type capacity struct {
	sync.Mutex
	next int
	live map[x.ReservationID]bool
}

func (c *capacity) Reserve(x.Reservation) (x.ReservationID, error) {
	c.Lock()
	defer c.Unlock()
	c.next++
	id := x.ReservationID(fmt.Sprint(c.next))
	c.live[id] = true
	return id, nil
}
func (c *capacity) Release(id x.ReservationID) error {
	c.Lock()
	defer c.Unlock()
	if !c.live[id] {
		return errors.New("reservation was not retained")
	}
	delete(c.live, id)
	return nil
}
func base() x.Base {
	return x.Base{Clock: func() (string, int64, error) { ns, err := controlbudget.Now(); return "boot", ns, err }, UTC: time.Now,
		Capacity: &capacity{live: make(map[x.ReservationID]bool)}}
}

type authority struct{}

func (*authority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }

// Settlement and source seals in this adapter test use adversarial control
// ports. They make no history/auth/cohort integration claim; P30 owns that lane.
type transport struct {
	requests []x.Request
	budget   x.BudgetSeal
}

func (p *transport) Request(r x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	p.requests = append(p.requests, r)
	if r.Op == x.OpSource {
		return x.Response{Status: &x.Status{State: x.StatusLive, Identity: r.Identity, Budget: p.budget}}, x.Attempt{Sent: true}, nil
	}
	return x.Response{}, x.Attempt{Sent: true}, nats.ErrNoResponders
}
func (*transport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, i.ErrUnsupported
}
func (*transport) CloseData(x.Identity, x.Deadline) error { return nil }

// origin: hot-update S9 plan §8 / T42 / M21
func TestTransferAdapterKeepsOriginalReceipts(t *testing.T) {
	s := newServer(t)
	js, successor := connect(t, s), connect(t, s)
	body := bytes.Repeat([]byte("same pinned source and receipt\n"), 4000)
	spec := specification(t, js, body)
	fault := &lostACK{JetStream: js, subject: "$O.original.C." + spec.Identity.ObjectVersion}
	port, err := jsstream.NewTransferWriter(fault, spec)
	if err != nil {
		t.Fatal(err)
	}
	w, err := WrapWriter(port, func(i.Deadline) (jetstream.JetStream, error) { return successor, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	var observations []x.Observation
	if err = SetWriterObserver(w, func(event x.Observation) { observations = append(observations, event) }, deadline(t)); err != nil {
		t.Fatal(err)
	}
	b := base()
	control := &transport{}
	budget := x.BudgetSeal{StartedAt: time.Now().UTC(), Size: spec.Size, RateLimitBPS: 1 << 30, EffectiveBudgetNS: int64(proto.XferTimeoutTierBFloor),
		RemainingNS: int64(proto.XferTimeoutTierBFloor), SourceSealed: true}
	control.budget = budget
	file, err := os.CreateTemp(t.TempDir(), "original-source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(body); err != nil {
		closeTestResource(t, file)
		t.Fatal(err)
	}
	sender, err := xfersend.New(x.SendConfig{Base: b, Identity: spec.Identity, Intent: x.Intent{Verb: "pull", Tier: "b", Path: "source",
		Size: spec.Size, SHA256: spec.SHA256, ChunkSize: spec.ChunkSize, RateLimitBPS: budget.RateLimitBPS}, Budget: budget, Source: file, Writer: w, Transport: control})
	if err != nil {
		closeTestResource(t, file)
		t.Fatal(err)
	}
	defer func() {
		if err := sender.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if _, err = sender.Seal(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = sender.Step(deadline(t)); err == nil || !fault.hit {
		t.Fatal("the original publish ACK was not actually lost")
	}
	before, err := sender.Freeze(deadline(t))
	if err != nil || before.Writer.PendingAttempt == nil {
		t.Fatalf("pending sender receipt missing: %v", err)
	}
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetLastMsgForSubject(t.Context(), fault.subject)
	if err != nil || raw.Sequence == 0 || raw.Header.Get("Tether-Xfer-Index") != "0" {
		t.Fatalf("publish effect missing: %v", err)
	}
	initialSeq := raw.Sequence
	exposed, err := w.Freeze(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	exposed.PendingAttempt.Bytes[0] ^= 1
	retained, err := w.Freeze(deadline(t))
	if err != nil || !reflect.DeepEqual(retained, before.Writer) || !bytes.Equal(retained.PendingAttempt.Bytes, raw.Data) {
		t.Fatal("caller snapshot mutation rewrote the original pending publish")
	}

	manager, err := xferworker.NewWorker(x.WorkerConfig{Base: b, Identity: x.Identity{AgentInstance: "agent"}, Authority: &authority{}, Transport: control})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	origin := x.Origin{BrokerID: "broker", InstanceID: "originalBroker", BootID: "brokerBoot", Generation: 1}
	prepare := x.Prepare{Identity: spec.Identity, Origin: origin, CallID: "originalCall", Intent: before.Intent, Budget: budget,
		Permit: x.CreationPermit{Identity: spec.Identity, Origin: origin, IssuedAt: budget.StartedAt, CreateUntil: budget.StartedAt.Add(time.Minute), PermitNonce: "originalPermit", Signature: "authenticated-port"}}
	if _, err = manager.Prepare(prepare, deadline(t)); err != nil {
		t.Fatal(err)
	}
	cfg := config()
	cfg.Workers = []Worker{{Owner: manager, Current: func(i.Deadline) (x.Bindings, error) {
		return x.Bindings{Authority: &authority{}, Transport: control, OwnerGeneration: 2}, nil
	}}}
	cfg.Writers = []*Writer{w}
	cfg.FenceData = func(_ i.ID, d i.Deadline) error { _, err := sender.Freeze(d); return err }
	recoveryAvailable := false
	cfg.BeforeReplay = func(i.ID, i.Deadline) error {
		if !recoveryAvailable {
			return nats.ErrNoResponders
		}
		return nil
	}
	a := mustAdapter(t, cfg)
	tx := i.ID{1}
	seal, err := a.Fence(tx, deadline(t))
	if err != nil || !seal.Complete {
		t.Fatalf("fence refused original pending attempt: %v", err)
	}
	if !reflect.DeepEqual(a.workers[0].state.Entries[0].Prepare, prepare) ||
		!reflect.DeepEqual(w.state, before.Writer) {
		t.Fatal("fence changed the original identity, pending bytes or budget")
	}
	if again, err := a.Fence(tx, deadline(t)); err != nil || again != seal {
		t.Fatal("repeated fence changed the original seal")
	}
	if err = a.Release(seal, deadline(t)); err == nil {
		t.Fatal("release admitted before successor binding")
	}
	if err = a.Rebind(seal, deadline(t)); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal("503 did not retain recovery responsibility")
	}
	status, err := manager.Status(spec.Identity)
	if err != nil || status.State != x.StatusLive || status.Terminal != nil || status.Budget != budget {
		t.Fatal("503 or lost ACK became failed or renewed the original budget")
	}
	if _, err = w.Append(1, body[spec.ChunkSize:2*spec.ChunkSize], deadline(t)); err == nil {
		t.Fatal("fenced writer admitted a later chunk")
	}
	recoveryAvailable = true
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = w.ResolvePending(deadline(t)); err == nil {
		t.Fatal("rebind reopened business before release")
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status(spec.Identity)
	if err != nil || status.Identity != prepare.Identity || status.Origin != origin || status.Budget != budget || status.Terminal != nil {
		t.Fatal("migration replaced the original worker invocation")
	}
	if err = sender.Step(deadline(t)); err != nil {
		t.Fatalf("same sender did not resolve its pending ACK: %v", err)
	}
	for n := uint32(1); n < spec.Chunks; n++ {
		if err = sender.Step(deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if err = sender.Step(deadline(t)); err != nil {
		t.Fatal(err)
	} // Metadata has its own definite ACK.
	after, err := sender.Freeze(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if after.Identity != before.Identity || after.Source != before.Source || after.SourceIdentity != before.SourceIdentity ||
		after.Budget != before.Budget || after.ByteDeadline != before.ByteDeadline || after.Intent.RateLimitBPS != before.Intent.RateLimitBPS || after.Offset != spec.Size {
		t.Fatal("sender replaced its pinned source, pacing, byte budget or cursor")
	}
	if len(after.Writer.Receipts) != int(spec.Chunks) || after.Writer.Receipts[0].StreamSeq != initialSeq || after.Writer.PendingAttempt != nil {
		t.Fatal("resume replaced the original raw ACK sequence")
	}
	var successorObserved bool
	for _, event := range observations {
		if event.Kind == x.ObserveAppend && event.Index > 0 && event.ObjectVersion == spec.Identity.ObjectVersion {
			successorObserved = true
		}
	}
	if !successorObserved {
		t.Fatal("successor writer lost actual S7 observation custody")
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != uint64(spec.Chunks)+1 {
		t.Fatalf("replayed duplicate physical chunk: %v", err)
	}
	for idx, receipt := range after.Writer.Receipts {
		raw, err := stream.GetMsg(t.Context(), receipt.StreamSeq)
		if err != nil || raw.Subject != fault.subject || raw.Header.Get("Tether-Xfer-Index") != fmt.Sprint(idx) ||
			sha256.Sum256(raw.Data) != receipt.SHA256 {
			t.Fatalf("raw receipt/version drift at idx %d: %v", idx, err)
		}
	}
	if _, err = file.Stat(); err != nil {
		t.Fatal("infra release closed the original source FD")
	}
	if err = a.Release(seal, deadline(t)); err == nil {
		t.Fatal("retired seal released another generation")
	}
	// The same live owner can participate in a later rollback without keeping
	// the first transaction's cached snapshots or resetting the original rate.
	second, err := a.Fence(i.ID{2}, deadline(t))
	if err != nil || second.InvocationSet == seal.InvocationSet {
		t.Fatalf("later cut reused original transaction authority: %v", err)
	}
	if err = a.Rebind(second, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(second, deadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §8 / P19 lifecycle
func TestInfraTransferLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "infra transfer", goroutines)
	defer testharness.AssertNoFDLeak(t, "infra transfer", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransfer(t) })
	}
}

func exerciseTransfer(t *testing.T) {
	s := newServer(t)
	old, next := connect(t, s), connect(t, s)
	body := bytes.Repeat([]byte("retained owner"), 6000)
	spec := specification(t, old, body)
	port, err := jsstream.NewTransferWriter(old, spec)
	if err != nil {
		t.Fatal(err)
	}
	w, err := WrapWriter(port, func(i.Deadline) (jetstream.JetStream, error) { return next, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	if _, err = w.Append(0, body[:spec.ChunkSize], deadline(t)); err != nil {
		t.Fatal(err)
	}
	cfg := config()
	cfg.Writers = []*Writer{w}
	if _, err := New(cfg); !errors.Is(err, i.ErrUnsupported) {
		t.Fatal("independent data owner admitted without a product caller fence")
	}
	cfg.FenceData = func(i.ID, i.Deadline) error { return nil } // All byte calls in this lane are synchronous and already joined.
	a := mustAdapter(t, cfg)
	seal, err := a.Fence(i.ID{3}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	for idx := uint32(1); idx < spec.Chunks; idx++ {
		start := int(idx * spec.ChunkSize)
		end := min(len(body), start+int(spec.ChunkSize))
		if _, err = w.Append(idx, body[start:end], deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.Finalize(deadline(t)); err != nil {
		t.Fatal(err)
	}
	r, err := jsstream.NewTransferReader(old, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := WrapReader(r, func(i.Deadline) (jetstream.JetStream, error) { return next, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	if _, err = reader.ReadChunk(deadline(t)); err != nil {
		t.Fatal(err)
	}
	cfg = config()
	cfg.Readers = []*Reader{reader}
	cfg.FenceData = func(i.ID, i.Deadline) error { return nil } // This lane has no active sink write or Sync.
	a = mustAdapter(t, cfg)
	seal, err = a.Fence(i.ID{4}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	chunk, err := reader.ReadChunk(deadline(t))
	if err != nil || chunk.Receipt.Index != 0 {
		t.Fatalf("unsynced prefix survived a cut: %v", err)
	}
	// Refuse old-owner retirement after a successor reader has already been
	// constructed. Its window must remain owned when rollback Stop expires.
	refused := &expiredReaderStop{ObjectReader: reader.owner, refuse: true}
	t.Cleanup(func() { refused.refuse = false })
	reader.owner = refused
	seal, err = a.Fence(i.ID{5}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	short, err := controlbudget.After(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Rebind(seal, short); err == nil || reader.pending == nil {
		t.Fatal("expired cleanup lost the partial reader's window owner")
	}
	partial := reader.pending
	refused.refuse = false
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if reader.pending != nil {
		t.Fatal("partial reader cleanup was not retired")
	}
	if _, err = partial.ReadChunk(deadline(t)); err == nil {
		t.Fatal("partial successor was abandoned with open admission")
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.ReadChunk(deadline(t)); err != nil {
		t.Fatal(err)
	}
	// A business Stop closes admission even if it cannot acquire a busy port.
	// Retrying the infrastructure fence must not resurrect that original owner.
	for _, slot := range []*objectSlot{&w.objectSlot, &reader.objectSlot} {
		slot.serial <- struct{}{}
		short, err = controlbudget.After(10 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if slot == &w.objectSlot {
			err = w.Stop(short)
		} else {
			err = reader.Stop(short)
		}
		<-slot.serial
		if !errors.Is(err, controlbudget.ErrExpired) {
			t.Fatal("blocked Stop did not refuse its join")
		}
	}
	if _, err = w.Freeze(deadline(t)); err == nil {
		t.Fatal("writer Stop intent was undone by a refused join")
	}
	if _, err = reader.Freeze(deadline(t)); err == nil {
		t.Fatal("reader Stop intent was undone by a refused join")
	}
}

type expiredReaderStop struct {
	x.ObjectReader
	refuse bool
}

func (r *expiredReaderStop) Stop(d x.Deadline) error {
	if !r.refuse {
		return r.ObjectReader.Stop(d)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	<-timer.C
	return controlbudget.ErrExpired
}

// The real courier owns the original outcome and sent-unknown request through
// the cut. Its authenticated physical history lane remains P30 responsibility.
// origin: hot-update S9 plan §8 / T42
func TestTransferAdapterRetainsCourierResponsibility(t *testing.T) {
	id := identity()
	grant := x.CommitGrant{Identity: id, ReceiverInstance: "agent", GrantNonce: "originalGrant", TerminalID: hexDigest([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))}
	out := x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded, Bytes: 13, SHA256: hexDigest([]byte("originalBytes")), ParentSynced: true}
	p := &transport{}
	c, err := xfercourier.New(x.CourierConfig{Base: base(), Identity: id, Outcome: out, Transport: p,
		Capsule: x.SettlementCapsule{Identity: id, Origin: x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}, ReceiverNkey: "receiver", Issuer: "issuer", Nonce: "capsule", Signature: "private"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if err = c.Step(deadline(t)); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("settlement did not retain lost ACK: %v", err)
	}
	state, err := c.Freeze(deadline(t))
	if err != nil || !state.Attempt.Sent || state.Receipt != nil || state.Outcome != out {
		t.Fatalf("courier lost original settlement: %v", err)
	}
	// This probe exposes that actual snapshot through the frozen worker port;
	// it tests the adapter's closure without pretending to restore S7 actors.
	probe := &snapshotWorker{state: x.WorkerState{Admission: x.AdmissionState{Closed: true}, Couriers: []x.CourierState{state}}}
	cfg := config()
	cfg.Workers = []Worker{{Owner: probe, Current: func(i.Deadline) (x.Bindings, error) {
		return x.Bindings{Authority: &authority{}, Transport: p, OwnerGeneration: 1}, nil
	}}}
	a := mustAdapter(t, cfg)
	seal, err := a.Fence(i.ID{7}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.workers[0].state.Couriers[0], state) || seal.ReceiptSet == (i.Digest{}) {
		t.Fatal("adapter erased courier's original outcome or retry responsibility")
	}
	if err = a.Rebind(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = a.Release(seal, deadline(t)); err != nil {
		t.Fatal(err)
	}
	if len(p.requests) != 1 {
		t.Fatal("migration replayed or acknowledged the terminal")
	}
	again, err := c.Freeze(deadline(t))
	if err != nil || !reflect.DeepEqual(again, state) {
		t.Fatal("migration changed the real retained courier")
	}
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
