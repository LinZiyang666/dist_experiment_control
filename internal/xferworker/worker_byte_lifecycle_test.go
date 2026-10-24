package xferworker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S7 internal review rv-lifecycle F1–F4 / rv-recovery F01
func TestWorkerByteActorsResumeAndStopWithoutLeaks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("real-byte-actors", func(t *testing.T) { exerciseWorkerByteActors(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "worker byte actor lifetime", g)
	testharness.AssertNoFDLeak(t, "worker byte actor lifetime", fd, 0)
}

func exerciseWorkerByteActors(t *testing.T) {
	exerciseWorkerCutToExactTerminal(t)
	exerciseWorkerPendingReadStop(t, false)
	exerciseWorkerPendingReadStop(t, true)
	exerciseWorkerRateWaitStop(t)
	exerciseWorkerRevocationCollection(t)
}

type checkpointReader struct {
	data   []byte
	cp     x.SinkCheckpoint
	frozen bool
}

func (r *checkpointReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	if r.frozen {
		return x.Chunk{}, errors.New("reader is frozen")
	}
	if r.cp.DurableOffset == int64(len(r.data)) {
		return x.Chunk{}, io.EOF
	}
	offset := r.cp.DurableOffset
	data := r.data[offset:min(offset+5, int64(len(r.data)))]
	return x.Chunk{Data: data, Receipt: x.ChunkReceipt{Offset: offset, Length: uint32(len(data)), SHA256: sha256.Sum256(data)}}, nil
}
func (r *checkpointReader) ConfirmSynced(cp x.SinkCheckpoint) error { r.cp = cp; return nil }
func (r *checkpointReader) Freeze(x.Deadline) (x.ReaderState, error) {
	r.frozen = true
	return x.ReaderState{SinkCheckpoint: r.cp}, nil
}
func (r *checkpointReader) Stop(x.Deadline) error { return nil }
func (r *checkpointReader) Thaw(x.Deadline) error { r.frozen = false; return nil }

type resourceReceiver struct {
	x.Receiver
	files []handoff.Resource
}

func (r *resourceReceiver) Freeze(d x.Deadline) (x.ReceiverState, error) {
	state, err := r.Receiver.Freeze(d)
	if err != nil {
		return state, err
	}
	_, sink, err := xferreceive.Ports(r.Receiver)
	if err == nil {
		_, r.files, err = sink.Freeze(d)
	}
	return state, err
}
func (r *resourceReceiver) Resources() []handoff.Resource { return r.files }
func (r *resourceReceiver) Progress() x.ReceiverState {
	return r.Receiver.(interface{ Progress() x.ReceiverState }).Progress()
}
func (r *resourceReceiver) Thaw(d x.Deadline) error { return thawActor(r.Receiver, d) }

type terminalTrackerTransport struct {
	fixture      *terminalFixture
	courier      x.Courier
	publications atomic.Int32
}

func (p *terminalTrackerTransport) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	p.publications.Add(1)
	receipt, err := p.fixture.tracker.Finish(req.Identity, *req.Outcome, d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	decisions, err := xfertrack.Decisions(p.fixture.tracker)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	row, err := decisions.Read(key(req.Identity), d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	if row.CanonicalTerminal == nil {
		return x.Response{}, x.Attempt{}, errors.New("tracker lost canonical decision")
	}
	if err = xfercourier.BindCanonical(p.courier, *row.CanonicalTerminal); err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	return x.Response{Receipt: &receipt}, x.Attempt{}, nil
}
func (*terminalTrackerTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("inline transfer has no attachment")
}
func (*terminalTrackerTransport) CloseData(x.Identity, x.Deadline) error { return nil }

// origin: S7 internal review rv-terminal F1 / rv-recovery F01
func TestWorkerCutThawEOFCommitsOnceAndSettlesExactHistory(t *testing.T) {
	exerciseWorkerCutToExactTerminal(t)
}
func exerciseWorkerCutToExactTerminal(t *testing.T) {
	t.Helper()
	f := terminalSetup(t)
	// Use two actual WriteAt/Sync rounds before EOF, retaining the original sink.
	_, sink, err := xferreceive.Ports(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	reader := &checkpointReader{data: []byte("new bytes")}
	raw, err := xferreceive.New(x.ReceiveConfig{Base: f.base, Identity: f.prepare.Identity, Budget: f.prepare.Budget, Reader: reader, Atomic: sink})
	if err != nil {
		t.Fatal(err)
	}
	f.receiver = raw
	var primitives atomic.Int32
	if err := transferpath.SetAtomicObserver(sink, func(event x.Observation) {
		if event.Kind == x.ObservePrimitiveEnter {
			primitives.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	f.reserve(t)
	o, e := terminalWorker(t, f)
	actor := &resourceReceiver{Receiver: raw}
	e.receiver = actor
	transport := &terminalTrackerTransport{fixture: f}
	o.cfg.MakeCourier = func(cfg x.CourierConfig) (x.Courier, error) {
		cfg.Transport = transport
		courier, err := xfercourier.New(cfg)
		transport.courier = courier
		return courier, err
	}
	if err := o.step(e, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if e.value.Status.DurableOffset != 5 {
		t.Fatal("first chunk was not synced")
	}
	_, before := exportWorker(t, o)
	if before.Receivers[0].Checkpoint.DurableOffset != 5 {
		t.Fatal("cut advanced the durable prefix")
	}
	if err := o.Thaw(Bindings{Authority: o.cfg.Authority, OwnerGeneration: 2}, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := o.step(e, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if e.value.Status.DurableOffset != 9 {
		t.Fatal("second chunk did not resume after thaw")
	}
	if err := o.step(e, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	done := e.done
	o.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("courier did not settle")
	}
	status, err := o.Status(f.prepare.Identity)
	if err != nil {
		t.Fatal(err)
	}
	set, err := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(f.target)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != x.PhaseSettled || status.Terminal == nil || status.Terminal.Kind != "complete" || len(set.Start) != 1 || len(set.Terminal) != 1 || primitives.Load() != 1 || string(data) != "new bytes" {
		t.Fatalf("cut lost exact completion: phase=%s primitive=%d start=%d terminal=%d bytes=%q", status.Phase, primitives.Load(), len(set.Start), len(set.Terminal), data)
	}
	if transport.publications.Load() != 1 {
		t.Fatal("cleanup republished an acknowledged terminal")
	}
}

type interruptReader struct {
	entered, release chan struct{}
	once             sync.Once
}

func (r *interruptReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	close(r.entered)
	<-r.release
	return x.Chunk{}, errors.New("reader stopped")
}
func (*interruptReader) ConfirmSynced(x.SinkCheckpoint) error { return nil }
func (*interruptReader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{}, errors.New("reader cannot freeze during read")
}
func (r *interruptReader) Stop(x.Deadline) error { r.once.Do(func() { close(r.release) }); return nil }

// origin: S7 internal review rv-lifecycle F4
func TestWorkerStopInterruptsReceiverPendingRead(t *testing.T) {
	exerciseWorkerPendingReadStop(t, false)
}
func exerciseWorkerPendingReadStop(t *testing.T, cancel bool) {
	t.Helper()
	fixture := newWorkerFixture()
	o := newOwner(t, fixture)
	p := fixture.prepare("a", "blocked-read")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	vp, err := transferpath.ValidateForWrite(filepath.Join(t.TempDir(), "destination"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := transferpath.NewAtomicReceiver(vp, "tmp", true)
	if err != nil {
		t.Fatal(err)
	}
	reader := &interruptReader{entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		if err := reader.Stop(deadline(t, time.Second)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	actor, err := xferreceive.New(x.ReceiveConfig{Base: fixture.cfg.Base, Identity: p.Identity, Budget: p.Budget, Reader: reader, Atomic: sink})
	if err != nil {
		t.Fatal(err)
	}
	o.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return actor, nil }
	o.cfg.MakeCourier = func(cfg x.CourierConfig) (x.Courier, error) {
		return &courierProbe{state: x.CourierState{Identity: cfg.Identity}}, nil
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, reader.entered)
	if cancel {
		if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser}, deadline(t, 50*time.Millisecond)); err != nil {
			t.Fatal("cancel waited behind pending byte IO", err)
		}
	}
	if err := o.Stop(deadline(t, 50*time.Millisecond)); err != nil {
		t.Fatal("Stop waited for original byte deadline", err)
	}
	outcome, err := xferreceive.Outcome(actor)
	if err != nil || outcome.Outcome != x.OutcomeNotEntered {
		t.Fatal("Stop lost its closed-gate outcome", err)
	}
	if fixture.capacity.count() != 0 {
		t.Fatal("Stop retained worker reservation")
	}
}

type rateTransport struct{ budget x.BudgetSeal }

func (p rateTransport) Request(req x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	return x.Response{Status: &x.Status{Identity: req.Identity, State: x.StatusLive, Budget: p.budget}}, x.Attempt{}, nil
}
func (rateTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("test has no data connection")
}
func (rateTransport) CloseData(x.Identity, x.Deadline) error { return nil }

type rateWriter struct {
	spec     x.ObjectSpec
	appended chan struct{}
	once     sync.Once
}

func (w *rateWriter) Append(index uint32, data []byte, _ x.Deadline) (x.ChunkReceipt, error) {
	w.once.Do(func() { close(w.appended) })
	return x.ChunkReceipt{Index: index, Offset: int64(index) * int64(w.spec.ChunkSize), Length: uint32(len(data)), StreamSeq: uint64(index + 1), SHA256: sha256.Sum256(data)}, nil
}
func (*rateWriter) ResolvePending(x.Deadline) (x.ChunkReceipt, bool, error) {
	return x.ChunkReceipt{}, false, nil
}
func (*rateWriter) Finalize(x.Deadline) (x.ObjectReceipt, error) {
	return x.ObjectReceipt{}, errors.New("test must stop in pacing")
}
func (w *rateWriter) Snapshot(x.Deadline) (x.WriterState, error) {
	return x.WriterState{ObjectSpec: w.spec, Phase: x.WriteOpen}, nil
}
func (w *rateWriter) Freeze(x.Deadline) (x.WriterState, error) {
	return x.WriterState{ObjectSpec: w.spec, Phase: x.WriteOpen}, nil
}
func (*rateWriter) Stop(x.Deadline) error { return nil }

// origin: S7 internal review rv-lifecycle F4
func TestWorkerStopInterruptsRealSenderRateWait(t *testing.T) { exerciseWorkerRateWaitStop(t) }
func exerciseWorkerRateWaitStop(t *testing.T) {
	t.Helper()
	fixture := newWorkerFixture()
	fixture.utc = time.Now().UTC()
	o := newOwner(t, fixture)
	p := fixture.prepare("b", "rate-wait")
	p.Intent.Verb, p.Intent.Size, p.Intent.RateLimitBPS = "pull", 64<<10, 32<<10
	digest := sha256.Sum256(make([]byte, p.Intent.Size))
	p.Intent.SHA256 = hex.EncodeToString(digest[:])
	p.Budget.Size, p.Budget.RateLimitBPS, p.Budget.EffectiveBudgetNS, p.Budget.RemainingNS = 64<<10, 32<<10, int64(300*time.Second), int64(300*time.Second)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	source, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Stop(deadline(t, time.Second)); _ = source.Close() }()
	if _, err := source.Write(make([]byte, p.Intent.Size)); err != nil {
		t.Fatal(err)
	}
	writer := &rateWriter{appended: make(chan struct{}), spec: x.ObjectSpec{Identity: p.Identity, Size: p.Intent.Size, ChunkSize: p.Intent.ChunkSize, Chunks: 2, SHA256: hex.EncodeToString(digest[:])}}
	sender, err := xfersend.New(x.SendConfig{Base: fixture.cfg.Base, Identity: p.Identity, Intent: p.Intent, Budget: p.Budget, Source: source, Writer: writer, Transport: rateTransport{p.Budget}})
	if err != nil {
		t.Fatal(err)
	}
	paced := &pacedSender{Sender: sender, entered: make(chan struct{}), returned: make(chan struct{})}
	o.cfg.MakeSender = func(x.SendConfig) (x.Sender, error) { return paced, nil }
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, writer.appended)
	// The second Step must remain blocked in the real pacing timer, after
	// the first acknowledged chunk. Stop retains its shorter original deadline.
	await(t, paced.entered)
	select {
	case <-paced.returned:
		t.Fatal("second Step did not remain in sender rate wait")
	case <-time.After(20 * time.Millisecond):
	}
	if err := o.Stop(deadline(t, 50*time.Millisecond)); err != nil {
		t.Fatal("Stop did not interrupt rate wait", err)
	}
	if _, err := source.Stat(); err == nil {
		t.Fatal("sender source FD remains open")
	}
	if fixture.capacity.count() != 0 {
		t.Fatal("sender or worker reservation remains live")
	}
}

// pacedSender exposes entry/return barriers only; the real sender owns pacing.
type pacedSender struct {
	x.Sender
	steps             atomic.Int32
	entered, returned chan struct{}
}

func (p *pacedSender) Step(d x.Deadline) error {
	second := p.steps.Add(1) == 2
	if second {
		close(p.entered)
		defer close(p.returned)
	}
	return p.Sender.Step(d)
}

// origin: S7 internal review rv-lifecycle F5
func TestWorkerRevocationCollectionLeavesExportableIdleState(t *testing.T) {
	exerciseWorkerRevocationCollection(t)
}
func exerciseWorkerRevocationCollection(t *testing.T) {
	t.Helper()
	f := terminalSetup(t)
	o, e := terminalWorker(t, f)
	transport := &terminalTrackerTransport{fixture: f}
	// Hold publication during the repeated controls; these replays must retain
	// exactly one irreversible revocation, then settle the real broker decision.
	held := &heldTerminalTransport{terminalTrackerTransport: transport, release: make(chan struct{})}
	o.cfg.MakeCourier = func(cfg x.CourierConfig) (x.Courier, error) {
		cfg.Transport = held
		courier, err := xfercourier.New(cfg)
		transport.courier = courier
		return courier, err
	}
	for replay := 0; replay < 5; replay++ {
		if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: f.prepare.Identity, Reason: x.CancelUser}, terminalDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	o.mu.Lock()
	revocations := len(o.state.Revocations)
	o.mu.Unlock()
	if revocations != 1 {
		t.Fatalf("replayed revoke allocated %d duties", revocations)
	}
	close(held.release)
	o.mu.Lock()
	done := e.done
	o.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel never settled")
	}
	o.cfg.UTC = func() time.Time { return time.Now().Add(11 * time.Minute) }
	if err := o.collectResults(); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	if len(state.Entries) != 0 || len(state.Revocations) != 0 || len(state.CanonicalOutcomes) != 0 || len(state.CapacityReservations) != 0 {
		t.Fatal("result collection left private responsibility")
	}
	encoded, err := xferimage.EncodeWorker(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xferimage.DecodeWorker(encoded); err != nil {
		t.Fatal(err)
	}
	set, err := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
	if err != nil || len(set.Terminal) != 1 || set.Terminal[0].Kind != "failed" {
		t.Fatal("revocation lost exact failed history", err)
	}
}

type heldTerminalTransport struct {
	*terminalTrackerTransport
	release chan struct{}
}

func (p *heldTerminalTransport) Request(req x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	select {
	case <-p.release:
		return p.terminalTrackerTransport.Request(req, d)
	default:
		return x.Response{}, x.Attempt{}, errors.New("settlement transport held")
	}
}

// origin: S7 internal review rv-lifecycle F4
func TestWorkerCancelInterruptsReceiverPendingRead(t *testing.T) {
	exerciseWorkerPendingReadStop(t, true)
}
