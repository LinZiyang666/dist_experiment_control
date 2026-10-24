package xferimage_test

// Real worker/byte/courier owners export these images. Only authority and the
// control transport are local adapters; JetStream, ledger, history, source and
// atomic destination IO are real. This does not exercise a process exec.
// origin: hot-update S7 plan §8.3 / §12.2 P10 / T17

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() { xferimage.SetWorkerCodecStateBuilder(buildWorkerCodecState) }

// The account detects duplicate release and retained reservations, including
// child actors' accounts which are not themselves exported registry slots.
// origin: hot-update S7 plan §8.3 / T17
type builderCapacity struct {
	mu   sync.Mutex
	next uint64
	held map[x.ReservationID]x.Reservation
}

func (c *builderCapacity) Reserve(r x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	id := x.ReservationID(fmt.Sprint(c.next))
	if c.held == nil {
		c.held = make(map[x.ReservationID]x.Reservation)
	}
	c.held[id] = r
	return id, nil
}
func (c *builderCapacity) Release(id x.ReservationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.held[id]; !ok {
		return errors.New("builder capacity released an unowned reservation")
	}
	delete(c.held, id)
	return nil
}
func (c *builderCapacity) assertEmpty(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.held) != 0 {
		t.Errorf("builder retained %d capacity reservations after Stop", len(c.held))
	}
}

type builderAuthority struct{}

func (builderAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }

func builderDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func builderDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// The deciding tracker supplies canonical bytes and exact history receipts.
// Withholding the reply retains a real pending courier after a real attempt;
// it never substitutes a hand-built CourierState or TerminalReceipt.
type builderTransport struct {
	tracker   x.TrackerManager
	ledger    x.LedgerPort
	courier   x.Courier
	js        jetstream.JetStream
	hold      bool
	attempted chan error
	once      sync.Once
}

func (p *builderTransport) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	if req.Op == x.OpSource {
		status, err := p.tracker.SourceSeal(req.Identity, *req.Source, d)
		return x.Response{Status: &status}, x.Attempt{}, err
	}
	if req.Op != x.OpTerminal || req.Outcome == nil {
		return x.Response{}, x.Attempt{}, errors.New("unexpected builder control operation")
	}
	receipt, err := p.tracker.Finish(req.Identity, *req.Outcome, d)
	if err == nil {
		err = p.bindCanonical(req.Identity, receipt.CanonicalDigest, d)
	}
	p.once.Do(func() { p.attempted <- err })
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	if p.hold {
		return x.Response{}, x.Attempt{Sent: true}, errors.New("builder withheld terminal history ACK")
	}
	return x.Response{Receipt: &receipt}, x.Attempt{Sent: true}, nil
}

// Finish has already synced the immutable canonical terminal. Read that exact
// durable row with the request deadline; Decisions is a nonblocking acquisition
// probe and can legitimately refuse while the tracker's watchdog holds serial.
// origin: hot-update S7 plan §4 / §5.1 / T17
func (p *builderTransport) bindCanonical(id x.Identity, digest string, d x.Deadline) error {
	census, err := p.ledger.Scan(d)
	if err != nil {
		return err
	}
	if !census.Ready || !census.Complete {
		return errors.New("builder canonical ledger census is incomplete")
	}
	for _, row := range census.Rows {
		if row.Identity == id && row.CanonicalTerminal != nil && row.CanonicalTerminal.Digest == digest {
			return xfercourier.BindCanonical(p.courier, *row.CanonicalTerminal)
		}
	}
	return errors.New("tracker lost exact canonical terminal")
}

func (p *builderTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return p.js, nil
}
func (*builderTransport) CloseData(x.Identity, x.Deadline) error { return nil }

// A single real byte step leaves a nonempty durable/hash/receipt prefix. The
// next call parks at the fixture boundary without freezing or fabricating an
// actor's progress. Freeze and Stop still run on the real owners.
type builderSender struct {
	x.Sender
	source    *os.File
	first     chan error
	stepped   bool
	resources []handoff.Resource
}

func (s *builderSender) Step(d x.Deadline) error {
	if s.stepped {
		return errors.New("builder sender prefix reached")
	}
	s.stepped = true
	err := s.Sender.Step(d)
	s.first <- err
	return err
}
func (s *builderSender) Freeze(d x.Deadline) (x.SenderState, error) {
	state, err := s.Sender.Freeze(d)
	if err == nil {
		s.resources = []handoff.Resource{{ID: state.Source.ResourceID, Kind: state.Source.Kind, Files: map[string]*os.File{state.Source.Name: s.source}}}
	}
	return state, err
}
func (s *builderSender) Resources() []handoff.Resource { return s.resources }

type builderReceiver struct {
	x.Receiver
	sink      x.AtomicPort
	first     chan error
	limit     bool
	stepped   bool
	resources []handoff.Resource
}

// Match the production dataReader cut: retire the real consumer, then release
// its reader reservation before the worker exports detached cleanup duties.
// The returned state is still the real reader's exact synced checkpoint.
// origin: hot-update S7 plan §3 / §8.2 / T17
type builderReader struct{ x.ObjectReader }

func (r builderReader) Freeze(d x.Deadline) (x.ReaderState, error) {
	state, err := r.ObjectReader.Freeze(d)
	if err == nil {
		err = r.Stop(d)
	}
	return state, err
}

func (r *builderReceiver) Step(d x.Deadline) error {
	if r.limit && r.stepped {
		return errors.New("builder receiver prefix reached")
	}
	err := r.Receiver.Step(d)
	if !r.stepped {
		r.first <- err
		r.stepped = true
	}
	return err
}
func (r *builderReceiver) Progress() x.ReceiverState {
	return r.Receiver.(interface{ Progress() x.ReceiverState }).Progress()
}
func (r *builderReceiver) Freeze(d x.Deadline) (x.ReceiverState, error) {
	state, err := r.Receiver.Freeze(d)
	if err == nil {
		_, r.resources, err = r.sink.Freeze(d)
	}
	return state, err
}
func (r *builderReceiver) Resources() []handoff.Resource { return r.resources }

// origin: S7 internal review NO-SKIP-AS-PASS F3
func buildWorkerCodecState(t *testing.T, mode string) x.WorkerFrozen {
	t.Helper()
	capacity := &builderCapacity{}
	base := x.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return "boot", now, err },
		UTC: func() time.Time { return time.Now().UTC() }, Capacity: capacity}
	js := builderJetStream(t)
	transport := &builderTransport{js: js, hold: mode != "settled", attempted: make(chan error, 1)}
	body := bytes.Repeat([]byte("real worker codec prefix\x00\xff"), 3000)[:64<<10]
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: builderDigest([]byte("intent")), ObjectVersion: "version"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "origin", BootID: "boot", Generation: 1}
	target := filepath.Join(t.TempDir(), "destination")
	intent := x.Intent{Verb: "push", Tier: "b", Path: target, Size: int64(len(body)), SHA256: builderDigest(body), ChunkSize: 32 << 10, Force: true}
	if mode == "sender" {
		intent.Verb = "pull"
	}
	permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: base.UTC(), PermitNonce: "permit", Signature: "verified"}
	permit.CreateUntil = permit.IssuedAt.Add(30 * time.Second)
	tracker, ledger := builderTracker(t, base, js, origin)
	transport.tracker, transport.ledger = tracker, ledger
	status, err := tracker.Admit(permit, intent, builderDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	prepare := x.Prepare{CallID: "call", Identity: id, Origin: origin, Intent: intent, Permit: permit, Budget: status.Budget}
	prepare.Settlement = &x.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: "receiver", Issuer: "issuer", Nonce: "settlement", Signature: "verified"}
	if mode != "sender" {
		seal := x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, ChunkSize: intent.ChunkSize, Chunks: 2, SourceFingerprint: "published-source"}
		status, err = tracker.SourceSeal(id, seal, builderDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		prepare.Source, prepare.Budget = &seal, status.Budget
	}
	spec := builderObjectSpec(t, js, id, intent)
	first := make(chan error, 1)
	cfg := x.WorkerConfig{Base: base, Identity: id, Authority: builderAuthority{}, Transport: transport}
	cfg.MakeSender = func(c x.SendConfig) (x.Sender, error) {
		path := filepath.Join(t.TempDir(), "source")
		if err := os.WriteFile(path, body, 0o600); err != nil {
			return nil, err
		}
		// Source custody starts with the same read-only access as production.
		source, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		writer, err := jsstream.NewTransferWriter(js, spec)
		if err != nil {
			return nil, errors.Join(err, source.Close())
		}
		c.Source, c.Writer = source, writer
		actor, err := xfersend.New(c)
		if err != nil {
			return nil, errors.Join(err, source.Close(), writer.Stop(builderDeadline(t)))
		}
		return &builderSender{Sender: actor, source: source, first: first}, nil
	}
	cfg.MakeReceiver = func(c x.ReceiveConfig) (x.Receiver, error) {
		writer, err := jsstream.NewTransferWriter(js, spec)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := writer.Stop(builderDeadline(t)); err != nil {
				t.Error(err)
			}
		}()
		for index := uint32(0); index < spec.Chunks; index++ {
			start := int(index) * int(spec.ChunkSize)
			if _, err := writer.Append(index, body[start:start+int(spec.ChunkSize)], builderDeadline(t)); err != nil {
				return nil, err
			}
		}
		if _, err := writer.Finalize(builderDeadline(t)); err != nil {
			return nil, err
		}
		reader, err := jsstream.NewTransferReader(js, spec, x.SinkCheckpoint{})
		if err != nil {
			return nil, err
		}
		vp, err := transferpath.ValidateForWrite(target, nil, 0)
		if err != nil {
			return nil, errors.Join(err, reader.Stop(builderDeadline(t)))
		}
		sink, err := transferpath.NewAtomicReceiver(vp, "codecbuilder", true)
		if err != nil {
			return nil, errors.Join(err, reader.Stop(builderDeadline(t)))
		}
		c.Reader, c.Atomic = builderReader{reader}, sink
		actor, err := xferreceive.New(c)
		if err != nil {
			return nil, errors.Join(err, reader.Stop(builderDeadline(t)), sink.Stop(builderDeadline(t)))
		}
		if mode == "landed" || mode == "settled" {
			grant, grantErr := tracker.Reserve(id, x.ReserveRequest{Origin: origin, ReceiverInstance: id.AgentInstance}, builderDeadline(t))
			if grantErr == nil {
				grantErr = actor.Reserve(grant)
			}
			if grantErr != nil {
				return nil, errors.Join(grantErr, actor.Stop(builderDeadline(t)))
			}
		}
		return &builderReceiver{Receiver: actor, sink: sink, first: first, limit: mode == "receiver"}, nil
	}
	cfg.MakeCourier = func(c x.CourierConfig) (x.Courier, error) {
		actor, err := xfercourier.New(c)
		transport.courier = actor
		return actor, err
	}
	worker, err := xferworker.NewWorker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var originals []handoff.Resource
	// Export only borrows these descriptors. The worker, rather than a codec
	// caller or duplicate FD, must remain their one terminal cleanup owner.
	// origin: hot-update S7 plan §8.2 / §11.3 / T17
	t.Cleanup(func() {
		if err := worker.Stop(builderDeadline(t)); err != nil {
			t.Error(err)
		}
		for _, resource := range originals {
			for _, file := range resource.Files {
				if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Errorf("original %s file remains open after worker Stop: %v", resource.Kind, err)
				}
			}
		}
		capacity.assertEmpty(t)
	})
	if _, err := worker.Prepare(prepare, builderDeadline(t)); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "prepared":
	case "courier":
		if _, err := worker.Apply(x.Control{Kind: x.ControlCancel, Identity: id, Reason: x.CancelUser}, builderDeadline(t)); err != nil {
			t.Fatal(err)
		}
		builderAwait(t, transport.attempted)
	case "sender", "receiver", "landed", "settled":
		if err := worker.Start(id); err != nil {
			t.Fatal(err)
		}
		builderAwait(t, first)
		if mode == "landed" || mode == "settled" {
			builderAwait(t, transport.attempted)
		}
		if mode == "settled" {
			builderAwaitSettled(t, worker, id)
		}
	default:
		t.Fatalf("unknown real worker state %q", mode)
	}
	frozen, err := worker.Freeze(builderDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	originals = resources
	if mode == "courier" {
		// The broker cut may close its volatile decision gate before the worker
		// cut. The original durable canonical row must remain readable then.
		// origin: hot-update S7 plan §4 / §8.2 / T17
		if _, err := tracker.Freeze(builderDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if len(state.CanonicalOutcomes) != 1 {
			t.Fatal("courier cut lost its durable canonical decision")
		}
		if err := transport.bindCanonical(id, state.CanonicalOutcomes[0].Canonical.Digest, builderDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "landed" || mode == "settled" {
		got, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatal("real primitive did not land the complete source", err)
		}
	}
	return frozen
}

func builderAwait(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("real worker did not reach its codec cut")
	}
}
func builderAwaitSettled(t *testing.T, worker x.WorkerManager, id x.Identity) {
	t.Helper()
	timer, tick := time.NewTimer(5*time.Second), time.NewTicker(time.Millisecond)
	defer timer.Stop()
	defer tick.Stop()
	for {
		status, err := worker.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase == x.PhaseSettled {
			return
		}
		select {
		case <-tick.C:
		case <-timer.C:
			t.Fatal("real worker did not settle its exact history receipt")
		}
	}
}

func builderJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("builder JetStream did not become ready")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.ReconnectBufSize(-1))
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

func builderTracker(t *testing.T, base x.Base, js jetstream.JetStream, origin x.Origin) (x.TrackerManager, x.LedgerPort) {
	t.Helper()
	// The deciding broker has its own account, stopped before the embedded
	// server. Worker cleanup can therefore independently assert its account.
	capacity := &builderCapacity{}
	base.Capacity = capacity
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "history-session", Subjects: []string{proto.SubjAuditTransfer("session")}, Discard: jetstream.DiscardNew}); err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	tracker, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: history})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tracker.Stop(builderDeadline(t)); err != nil {
			t.Error(err)
		}
		capacity.assertEmpty(t)
	})
	return tracker, ledger
}

func builderObjectSpec(t *testing.T, js jetstream.JetStream, id x.Identity, intent x.Intent) x.ObjectSpec {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "codec", Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "OBJ_codec")
	if err != nil {
		t.Fatal(err)
	}
	return x.ObjectSpec{Identity: id, Bucket: "codec", Stream: "OBJ_codec", StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano),
		Size: intent.Size, SHA256: intent.SHA256, ChunkSize: intent.ChunkSize, Chunks: 2}
}
