package xferworker

// Copy into internal/xferworker. Uses real receiver and atomic file owners;
// only the object reader, authority, capacity and terminal transport are probes.
import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferreceive"
)

type reviewCapacity struct {
	mu   sync.Mutex
	next int
}

func (c *reviewCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	return x.ReservationID(fmt.Sprint(c.next)), nil
}
func (*reviewCapacity) Release(x.ReservationID) error { return nil }

type reviewAuthority struct{}

func (reviewAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }
func reviewDeadline(t *testing.T, duration time.Duration) x.Deadline {
	t.Helper()
	d, e := controlbudget.After(duration)
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func reviewWorker(t *testing.T) (*owner, x.Prepare) {
	t.Helper()
	now := time.Now()
	cfg := Config{Base: x.Base{Clock: func() (string, int64, error) { n, e := controlbudget.Now(); return "boot", n, e }, UTC: time.Now, Capacity: &reviewCapacity{}}, Identity: x.Identity{AgentInstance: "agent"}, Authority: reviewAuthority{}}
	m, e := NewWorker(cfg)
	if e != nil {
		t.Fatal(e)
	}
	o := m.(*owner)
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "nonce", IntentDigest: "intent"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "origin", BootID: "originboot", Generation: 1}
	data := []byte("ab")
	sum := sha256.Sum256(data)
	p := x.Prepare{CallID: "call", Identity: id, Origin: origin, Inline: data, Intent: x.Intent{Verb: "push", Tier: "a", Size: 2, SHA256: hex.EncodeToString(sum[:])}, Permit: x.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "verified"}, Budget: x.BudgetSeal{StartedAt: now, Size: 2, SourceSealed: true, EffectiveBudgetNS: int64(30 * time.Second), RemainingNS: int64(30 * time.Second)}}
	if _, e = o.Prepare(p, reviewDeadline(t, time.Second)); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := o.Stop(reviewDeadline(t, time.Second)); e != nil {
			t.Error(e)
		}
	})
	return o, p
}

type reviewInlineReader struct{ cp x.SinkCheckpoint }

func (r *reviewInlineReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	if r.cp.DurableOffset == 2 {
		return x.Chunk{}, io.EOF
	}
	data := []byte{byte('a' + r.cp.DurableOffset)}
	return x.Chunk{Data: data, Receipt: x.ChunkReceipt{Offset: r.cp.DurableOffset, Length: 1, SHA256: sha256.Sum256(data)}}, nil
}
func (r *reviewInlineReader) ConfirmSynced(cp x.SinkCheckpoint) error { r.cp = cp; return nil }
func (r *reviewInlineReader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{SinkCheckpoint: r.cp}, nil
}
func (*reviewInlineReader) Stop(x.Deadline) error { return nil }
func reviewRealReceiver(t *testing.T, o *owner, p x.Prepare) x.Receiver {
	t.Helper()
	vp, e := transferpath.ValidateForWrite(filepath.Join(t.TempDir(), "destination"), nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	sink, e := transferpath.NewAtomicReceiver(vp, "reviewtmp", true)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = sink.Stop(reviewDeadline(t, time.Second)) })
	r, e := xferreceive.New(x.ReceiveConfig{Base: o.cfg.Base, Identity: p.Identity, Budget: p.Budget, Reader: &reviewInlineReader{}, Atomic: sink})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func reviewWaitRunner(t *testing.T, o *owner, id x.Identity) {
	t.Helper()
	o.mu.Lock()
	done := o.entries[key(id)].done
	o.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not reach expected stop boundary")
	}
}

// origin: S7 internal review rv-lifecycle F1
func TestWorkerProgressMustNotFreezeReceiver(t *testing.T) {
	o, p := reviewWorker(t)
	r := reviewRealReceiver(t, o, p)
	o.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	if e := o.Start(p.Identity); e != nil {
		t.Fatal(e)
	}
	// A synced receiver now parks for its Grant; inspect its live progress.
	until := time.After(time.Second)
	for {
		s, err := o.Status(p.Identity)
		if err != nil {
			t.Fatal(err)
		}
		if s.DurableOffset == p.Intent.Size {
			break
		}
		select {
		case <-until:
			t.Fatalf("ordinary worker progress permanently froze real receiver: durable=%d want=%d", s.DurableOffset, p.Intent.Size)
		case <-time.After(time.Millisecond):
		}
	}
	status, e := o.Status(p.Identity)
	if e != nil {
		t.Fatal(e)
	}
	if status.DurableOffset != p.Intent.Size {
		t.Fatalf("ordinary worker progress permanently froze real receiver: durable=%d want=%d", status.DurableOffset, p.Intent.Size)
	}
}

// origin: S7 internal review rv-lifecycle F2
func TestWorkerCancelMustCollectStoppedReceiverOutcome(t *testing.T) {
	o, p := reviewWorker(t)
	r := reviewRealReceiver(t, o, p)
	// This is the state after actor creation but before its first byte operation.
	o.entries[key(p.Identity)].receiver = r
	var calls atomic.Int32
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) { calls.Add(1); return &reviewAckCourier{id: p.Identity}, nil }
	_, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser}, reviewDeadline(t, time.Second))
	outcome, outcomeErr := xferreceive.Outcome(r)
	if err != nil || calls.Load() == 0 {
		t.Fatalf("closed gate proof was lost: Apply=%v courier_creations=%d receiver_outcome=%s outcome_error=%v", err, calls.Load(), outcome.Outcome, outcomeErr)
	}
}

type reviewAckCourier struct {
	receipt *x.TerminalReceipt
	id      x.Identity
}

func (*reviewAckCourier) Acknowledge(x.TerminalReceipt) error { return nil }
func (*reviewAckCourier) Step(x.Deadline) error               { return nil }
func (c *reviewAckCourier) Freeze(x.Deadline) (x.CourierState, error) {
	return x.CourierState{Identity: c.id, Receipt: c.receipt}, nil
}
func (*reviewAckCourier) Stop(x.Deadline) error { return nil }

type reviewRetryReceiver struct {
	file  *os.File
	stops atomic.Int32
}

func (*reviewRetryReceiver) Reserve(x.CommitGrant) error { return nil }
func (*reviewRetryReceiver) Step(x.Deadline) error       { return nil }
func (*reviewRetryReceiver) Freeze(x.Deadline) (x.ReceiverState, error) {
	return x.ReceiverState{}, nil
}
func (r *reviewRetryReceiver) Stop(x.Deadline) error {
	if r.stops.Add(1) == 1 {
		return errors.New("injected transient cleanup failure")
	}
	if r.stops.Load() == 2 {
		return r.file.Close()
	}
	return nil
}

// origin: S7 internal review rv-lifecycle F3
func TestWorkerMustRetryCleanupAfterTerminalReceipt(t *testing.T) {
	o, p := reviewWorker(t)
	f, err := os.CreateTemp(t.TempDir(), "retained-fd")
	if err != nil {
		t.Fatal(err)
	}
	r := &reviewRetryReceiver{file: f}
	e := o.entries[key(p.Identity)]
	e.receiver = r
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) {
		return &reviewAckCourier{id: p.Identity, receipt: &x.TerminalReceipt{Kind: "failed", HistorySeq: 1}}, nil
	}
	if err = o.ensureCourier(e, x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}); err != nil {
		t.Fatal(err)
	}
	reviewWaitRunner(t, o, p.Identity)
	status, err := o.Status(p.Identity)
	if err != nil {
		t.Fatal(err)
	}
	_, fdErr := f.Stat()
	if status.Phase != x.PhaseSettled || fdErr == nil {
		t.Fatalf("cleanup runner exited with responsibility live: phase=%s Stop_calls=%d retained_FD_open=%v", status.Phase, r.stops.Load(), fdErr == nil)
	}
}

// Models the same explicit Stop interrupt used by xfersend.paceSource and
// jsstream.transferResumeReader.Stop: byte work joins as soon as Stop signals.
type reviewInterruptibleReceiver struct {
	id      x.Identity
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	stops   atomic.Int32
}

func (*reviewInterruptibleReceiver) Reserve(x.CommitGrant) error { return nil }
func (r *reviewInterruptibleReceiver) Step(x.Deadline) error {
	close(r.entered)
	<-r.release
	return errors.New("byte operation interrupted")
}
func (r *reviewInterruptibleReceiver) Progress() x.ReceiverState {
	return x.ReceiverState{Identity: r.id}
}
func (r *reviewInterruptibleReceiver) Freeze(x.Deadline) (x.ReceiverState, error) {
	return x.ReceiverState{Identity: r.id}, nil
}
func (r *reviewInterruptibleReceiver) Stop(x.Deadline) error {
	r.stops.Add(1)
	r.once.Do(func() { close(r.release) })
	return nil
}

// origin: S7 internal review rv-lifecycle F4
func TestWorkerStopMustSignalActorBeforeJoiningIt(t *testing.T) {
	o, p := reviewWorker(t)
	r := &reviewInterruptibleReceiver{id: p.Identity, entered: make(chan struct{}), release: make(chan struct{})}
	// The reproducer always releases and joins even when Stop fails.
	defer func() { r.once.Do(func() { close(r.release) }); reviewWaitRunner(t, o, p.Identity) }()
	o.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("actor did not enter")
	}
	err := o.Stop(reviewDeadline(t, 50*time.Millisecond))
	if err != nil || r.stops.Load() == 0 {
		t.Fatalf("worker waited for interruptible actor without signaling it: Stop=%v child_Stop_calls=%d", err, r.stops.Load())
	}
}

// origin: S7 internal review rv-lifecycle F5
func TestWorkerRepeatedCancelMustNotGrowRevocations(t *testing.T) {
	o, p := reviewWorker(t)
	// A pending courier is retained throughout a repeated cancel/revoke; it must
	// not authorize allocation of another copy of the same revocation each time.
	o.entries[key(p.Identity)].courier = &reviewAckCourier{id: p.Identity}
	for i := 0; i < 5; i++ {
		if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser}, reviewDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if len(o.state.Revocations) != 1 {
		t.Fatalf("same-entry cancellation allocated %d revocations; want one bounded record", len(o.state.Revocations))
	}
}

// origin: S7 internal review rv-lifecycle F5
func TestWorkerCollectionMustRetireRevocation(t *testing.T) {
	o, p := reviewWorker(t)
	o.entries[key(p.Identity)].courier = &reviewAckCourier{id: p.Identity}
	if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser}, reviewDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: &x.TerminalReceipt{Kind: "failed", HistorySeq: 1}}, reviewDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(11 * time.Minute)
	o.cfg.UTC = func() time.Time { return future }
	if err := o.collectResults(); err != nil {
		t.Fatal(err)
	}
	if len(o.entries) != 0 {
		t.Fatal("fixture failed to collect settled result")
	}
	_, err := o.Freeze(reviewDeadline(t, time.Second))
	if err != nil {
		t.Fatalf("settled result GC left %d stale revocation(s) and made empty worker unexportable: %v", len(o.state.Revocations), err)
	}
}

// origin: S7 internal review rv-lifecycle F3
func TestWorkerExplicitAckRetainsCleanupRunnerAfterStopFailure(t *testing.T) {
	o, p := reviewWorker(t)
	file, err := os.CreateTemp(t.TempDir(), "retirement")
	if err != nil {
		t.Fatal(err)
	}
	receiver := &reviewRetryReceiver{file: file}
	e := o.entries[key(p.Identity)]
	e.receiver, e.courier = receiver, &reviewAckCourier{id: p.Identity}
	receipt := &x.TerminalReceipt{Kind: "failed", HistorySeq: 1}
	if _, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: receipt}, reviewDeadline(t, time.Second)); err == nil {
		t.Fatal("first cleanup did not report its real Stop failure")
	}
	reviewWaitRunner(t, o, p.Identity)
	status, err := o.Status(p.Identity)
	if err != nil || status.Phase != x.PhaseSettled || receiver.stops.Load() != 2 {
		t.Fatalf("explicit ACK lost cleanup retry: phase=%s stops=%d err=%v", status.Phase, receiver.stops.Load(), err)
	}
	if _, err := file.Stat(); err == nil {
		t.Fatal("retry retained file descriptor")
	}
}
