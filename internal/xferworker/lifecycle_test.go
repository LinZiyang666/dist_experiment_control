package xferworker

import (
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

type callbackBarrier struct {
	entered, release chan struct{}
	once             sync.Once
}

func newBarrier() *callbackBarrier {
	return &callbackBarrier{entered: make(chan struct{}), release: make(chan struct{})}
}
func (b *callbackBarrier) block() { b.once.Do(func() { close(b.entered) }); <-b.release }
func (b *callbackBarrier) open() {
	select {
	case <-b.release:
	default:
		close(b.release)
	}
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("callback barrier not reached")
	}
}
func awaitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation failed to join")
		return nil
	}
}

// The receiver probe blocks at an actual admitted port call. These cases prove
// the registry join (including a callback that ignores its deadline); kernel
// D-state and real commit/Sync outcomes require P05/P07 integration.
type receiverProbe struct {
	mu                    sync.Mutex
	frozen                bool
	state                 x.ReceiverState
	reserve, freeze, step *callbackBarrier
	stopErr               error
	stops                 int
}

func (r *receiverProbe) Reserve(g x.CommitGrant) error {
	if r.reserve != nil {
		r.reserve.block()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.GateClosed {
		return workerError(x.CodeCanceled)
	}
	r.state.Grant = g
	return nil
}
func (r *receiverProbe) Step(x.Deadline) error {
	r.mu.Lock()
	fenced := r.frozen || r.state.Stopped
	r.mu.Unlock()
	if fenced {
		return workerError(x.CodeOwnerRecovering)
	}
	if r.step != nil {
		r.step.block()
	}
	return io.EOF
}
func (r *receiverProbe) Freeze(x.Deadline) (x.ReceiverState, error) {
	if r.freeze != nil {
		r.freeze.block()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// origin: S7 internal review rv-lifecycle F1–F2
	// Model the real lifecycle fence; observation uses Progress instead.
	if r.state.Stopped {
		return x.ReceiverState{}, workerError(x.CodeOwnerRecovering)
	}
	r.frozen = true
	return r.state, nil
}
func (r *receiverProbe) Progress() x.ReceiverState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}
func (r *receiverProbe) Thaw(x.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Stopped {
		return workerError(x.CodeOwnerRecovering)
	}
	r.frozen = false
	return nil
}
func (r *receiverProbe) Stop(x.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopErr == nil && !r.state.Stopped {
		r.stops++
		r.state.GateClosed, r.state.Stopped = true, true
	}
	return r.stopErr
}

var _ x.Receiver = (*receiverProbe)(nil)

// origin: hot-update S7 plan §8.2 / T18
func TestTransferFreezeTimeoutKeepsOriginalOwner(t *testing.T) {
	t.Run("entered-authority-callback", func(t *testing.T) { exerciseEnteredCallback(t) })
	t.Run("entered-receiver-grant", func(t *testing.T) { exerciseReceiverCallback(t) })
	t.Run("blocked-child-freeze", func(t *testing.T) { exerciseBlockedFreeze(t) })
	t.Run("entered-byte-step", func(t *testing.T) { exerciseByteStep(t) })
}

func exerciseEnteredCallback(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	barrier := newBarrier()
	defer barrier.open()
	f.cfg.Authority = &authorityProbe{check: func(_ x.Identity, access x.AccessKind, _ x.Deadline) error {
		if access == x.AccessCreate {
			barrier.block()
		}
		return nil
	}}
	o := newOwner(t, f)
	p := f.prepare("b", "callback")
	result := make(chan error, 1)
	go func() { _, err := o.Prepare(p, deadline(t, 5*time.Second)); result <- err }()
	await(t, barrier.entered)
	cut, err := o.Freeze(deadline(t, 20*time.Millisecond))
	if err == nil || cut != nil || !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("entered callback escaped freeze: cut=%v err=%v", cut, err)
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, 20*time.Millisecond)); err == nil {
		t.Fatal("Thaw replaced authority beneath entered callback")
	}
	barrier.open()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	if len(state.Entries) != 1 || state.Entries[0].Prepare.Identity != p.Identity || f.capacity.next != 1 {
		t.Fatalf("callback failed to finish in original owner: %+v", state)
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

func exerciseReceiverCallback(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "grant")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	barrier := newBarrier()
	defer barrier.open()
	r := &receiverProbe{state: x.ReceiverState{Identity: p.Identity, ByteDeadline: o.entries[key(p.Identity)].byteDeadline}, reserve: barrier}
	o.entries[key(p.Identity)].receiver = r
	grant := x.CommitGrant{Identity: p.Identity, GrantNonce: "original-grant", TerminalID: "terminal", ReceiverInstance: "agent"}
	result := make(chan error, 1)
	go func() {
		_, err := o.Apply(x.Control{Kind: x.ControlGrant, Identity: p.Identity, Grant: &grant}, deadline(t, 5*time.Second))
		result <- err
	}()
	await(t, barrier.entered)
	if cut, err := o.Freeze(deadline(t, 20*time.Millisecond)); err == nil || cut != nil {
		t.Fatal("receiver critical section escaped join")
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, 20*time.Millisecond)); err == nil {
		t.Fatal("unjoined receiver activated replacement")
	}
	barrier.open()
	if err := awaitResult(t, result); err != nil {
		t.Fatal(err)
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	if len(state.Receivers) != 1 || state.Receivers[0].Grant != grant || state.Entries[0].Status.Phase != x.PhaseCommitReserved {
		t.Fatalf("original grant/outcome lost: %+v", state)
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

func exerciseBlockedFreeze(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "sync")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	barrier := newBarrier()
	defer barrier.open()
	r := &receiverProbe{state: x.ReceiverState{Identity: p.Identity, ByteDeadline: o.entries[key(p.Identity)].byteDeadline}, freeze: barrier}
	o.entries[key(p.Identity)].receiver = r
	result := make(chan error, 1)
	go func() {
		cut, err := o.Freeze(deadline(t, 20*time.Millisecond))
		if cut != nil {
			result <- errors.New("exported blocked child")
			return
		}
		result <- err
	}()
	await(t, barrier.entered)
	if err := awaitResult(t, result); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("child Freeze timeout = %v", err)
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, 20*time.Millisecond)); err == nil {
		t.Fatal("Thaw raced late child Freeze")
	}
	barrier.open()
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// No replacement actor is constructed; the same receiver is used again.
	_, state := exportWorker(t, o)
	if o.entries[key(p.Identity)].receiver != r || len(state.Receivers) != 1 || state.Lineage.Generation != 2 {
		t.Fatal("timeout changed receiver owner")
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

func exerciseByteStep(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	p := f.prepare("b", "bytes")
	barrier := newBarrier()
	defer barrier.open()
	r := &receiverProbe{state: x.ReceiverState{Identity: p.Identity, ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}, step: barrier}
	f.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, barrier.entered)
	if cut, err := o.Freeze(deadline(t, 20*time.Millisecond)); err == nil || cut != nil {
		t.Fatal("byte Step escaped freeze")
	}
	barrier.open()
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	if len(state.Receivers) != 1 || !state.Entries[0].Started {
		t.Fatal("started actor disappeared after deferred cut")
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §11.3 / P06
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeGoroutines := runtime.NumGoroutine()
	beforeFDs := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer worker lifecycle", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "transfer worker lifecycle", beforeFDs, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	exerciseEnteredCallback(t)
	exerciseBlockedFreeze(t)
	exerciseByteStep(t)
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("a", "lifecycle")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	imported, _, err := ImportWorker(f.cfg, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	cut, _ := exportWorker(t, imported.(*owner))
	if _, err := cut.DetachPristine(); err != nil {
		t.Fatal(err)
	}
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// Offline revoke closes the byte gate while preserving exact settlement.
	courier := &courierProbe{state: x.CourierState{Identity: p.Identity, Canonical: x.CanonicalRecord{Identity: p.Identity, Kind: "failed", Digest: "exact"}}}
	o.entries[key(p.Identity)].courier = courier
	if _, err := o.Apply(x.Control{Kind: x.ControlRevoke, Identity: p.Identity, Reason: x.CancelRevoked}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if !o.entries[key(p.Identity)].value.GateClosed || courier.stopped != 0 {
		t.Fatal("revoke released courier or left byte gate open")
	}
	receipt := x.TerminalReceipt{Kind: "failed", CanonicalDigest: "exact", HistoryStream: "history", HistorySeq: 1}
	if _, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: &receipt}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if f.capacity.count() != 0 {
		t.Fatal("Stop left reservation live")
	}
	// Partial imported file ownership is detached, and the exercise closes the
	// returned original. This covers the file axis independently of actors.
	file, err := os.CreateTemp(t.TempDir(), "salvage")
	if err != nil {
		t.Fatal(err)
	}
	f2 := newWorkerFixture()
	original := newOwner(t, f2)
	prepared := f2.prepare("b", "file")
	if _, err := original.Prepare(prepared, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, withFile := exportWorker(t, original)
	ref := x.ResourceRef{ResourceID: "file", Name: x.TmpFileName, Kind: x.ResourceTmp}
	withFile.Receivers = []x.ReceiverState{{Identity: prepared.Identity, ByteDeadline: withFile.ResultRetention[0].Deadline, Atomic: x.AtomicState{Tmp: ref}}}
	taken, _, err := ImportWorker(f2.cfg, withFile, &fileSourceProbe{files: map[string]*os.File{"file/tmp": file}})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	frozenFile, _ := exportWorker(t, taken.(*owner))
	salvage, err := frozenFile.DetachPristine()
	if err != nil || len(salvage.Resources) != 1 {
		_ = file.Close()
		t.Fatalf("FD salvage: %+v %v", salvage, err)
	}
	if err := salvage.Resources[0].Files[x.TmpFileName].Close(); err != nil {
		t.Fatal(err)
	}
	if err := original.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §4 / §8.2 / T18
func TestTransferWorkerStopRetainsFailedActorForRetry(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("a", "stop")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	courier := &courierProbe{stopErr: errors.New("courier join incomplete")}
	o.entries[key(p.Identity)].courier = courier
	file, err := os.CreateTemp(t.TempDir(), "still-owned")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	o.resources = []handoff.Resource{{ID: "owned", Kind: x.ResourceTmp, Files: map[string]*os.File{x.TmpFileName: file}}}
	if err := o.Stop(deadline(t, time.Second)); err == nil {
		t.Fatal("Stop discarded failed child")
	}
	if f.capacity.count() != 1 || o.stopped {
		t.Fatal("Stop released unresolved owner")
	}
	if _, err := file.WriteString("failed child still owns this descriptor"); err != nil {
		t.Fatal("Stop closed original before child joined", err)
	}
	courier.stopErr = nil
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if courier.stopped != 1 || f.capacity.count() != 0 {
		t.Fatal("Stop retry failed to retire child/capacity")
	}
}

// origin: hot-update S7 plan §8.2 / T14
func TestTransferWorkerInvalidatesCutHandlesWithoutGenerationChange(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("a", "epoch")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	previous, _ := exportWorker(t, o)
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 1}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _ = exportWorker(t, o)
	if _, _, err := previous.Export(); err == nil {
		t.Fatal("previous cut handle borrowed current snapshot")
	}
}

// origin: hot-update S7 plan §1.2 / §6.1 / T27
func TestTransferWorkerAuthorizationCannotRenewPermitOrByteBudget(t *testing.T) {
	t.Run("creation-expiry-during-check", func(t *testing.T) {
		f := newWorkerFixture()
		f.cfg.Authority = &authorityProbe{check: func(_ x.Identity, a x.AccessKind, _ x.Deadline) error {
			if a == x.AccessCreate {
				f.advance(time.Minute)
			}
			return nil
		}}
		o := newOwner(t, f)
		p := f.prepare("b", "delayed")
		_, err := o.Prepare(p, deadline(t, time.Second))
		requireCode(t, err, x.CodePermitExpired)
		if f.capacity.next != 0 {
			t.Fatal("expired callback allocated capacity")
		}
	})
	t.Run("original-byte-deadline", func(t *testing.T) {
		f := newWorkerFixture()
		_, received, _ := f.cfg.Clock()
		// Control latency is represented by a monotonic clock advance, without
		// sleeping or comparing this process's BOOTTIME to the origin's clock.
		mono := received
		f.cfg.Clock = func() (string, int64, error) { return "boot", mono, nil }
		f.cfg.Authority = &authorityProbe{check: func(_ x.Identity, a x.AccessKind, _ x.Deadline) error {
			if a == x.AccessCreate {
				mono += int64(3 * time.Second)
			}
			return nil
		}}
		o := newOwner(t, f)
		p := f.prepare("b", "budget")
		if _, err := o.Prepare(p, x.Deadline(received+int64(5*time.Second))); err != nil {
			t.Fatal(err)
		}
		if got := o.entries[key(p.Identity)].byteDeadline.Deadline; got != x.Deadline(received+p.Budget.RemainingNS) {
			t.Fatalf("control callback renewed byte budget: %d", got)
		}
	})
}

// origin: hot-update S7 plan §4 / T18
func TestTransferWorkerNeverSettlesAnOpenReceiverGate(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "gate")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	r := &receiverProbe{state: x.ReceiverState{Identity: p.Identity, Outcome: x.ReceiverOutcome{Outcome: x.OutcomeNotEntered}, ByteDeadline: o.entries[key(p.Identity)].byteDeadline}}
	o.entries[key(p.Identity)].receiver = r
	called := false
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) { called = true; return nil, errors.New("unexpected courier") }
	err := o.step(o.entries[key(p.Identity)], deadline(t, time.Second))
	requireCode(t, err, x.CodeCommitInflight)
	if called {
		t.Fatal("open gate produced a no-syscall settlement")
	}
}

type transportProbe struct {
	name     string
	deadline x.Deadline
}

func (p *transportProbe) Request(_ x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	p.deadline = d
	return x.Response{}, x.Attempt{}, errors.New(p.name)
}
func (p *transportProbe) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New(p.name)
}
func (p *transportProbe) CloseData(x.Identity, x.Deadline) error { return errors.New(p.name) }

// origin: hot-update S7 plan §4 / §8.2 / T18
func TestTransferWorkerTransportRebindKeepsOriginalActor(t *testing.T) {
	f := newWorkerFixture()
	first := &transportProbe{name: "old backend unavailable"}
	next := &transportProbe{name: "new backend unavailable"}
	f.cfg.Transport = first
	p := f.prepare("b", "transport")
	var captured x.Transport
	var receiver *receiverProbe
	factories := 0
	f.cfg.MakeReceiver = func(cfg x.ReceiveConfig) (x.Receiver, error) {
		factories++
		captured = cfg.Transport
		receiver = &receiverProbe{state: x.ReceiverState{Identity: cfg.Identity, ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}}
		return receiver, nil
	}
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, o.entries[key(p.Identity)].done)
	exported, _ := exportWorker(t, o)
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, Transport: next, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	d := deadline(t, time.Second)
	_, _, err := captured.Request(x.Request{Identity: p.Identity}, d)
	if err == nil || err.Error() != next.name || next.deadline != d || factories != 1 || o.entries[key(p.Identity)].receiver != receiver {
		t.Fatalf("transport replacement changed actor/deadline: %v factories=%d", err, factories)
	}
	if _, _, err := exported.Export(); err == nil {
		t.Fatal("transport replacement left old cut valid")
	}
}

type senderProbe struct {
	state        x.SenderState
	seals, steps int
}

func (s *senderProbe) Seal(x.Deadline) (x.SourceSeal, error) {
	s.seals++
	return x.SourceSeal{}, errors.New("source changed before first byte")
}
func (s *senderProbe) Step(x.Deadline) error                    { s.steps++; return io.EOF }
func (s *senderProbe) Freeze(x.Deadline) (x.SenderState, error) { return s.state, nil }
func (s *senderProbe) Stop(x.Deadline) error                    { s.state.Stopped = true; return nil }

// origin: hot-update S7 plan §4 / §8.1 / T18
func TestTransferWorkerSourceSealPrecedesByteWork(t *testing.T) {
	f := newWorkerFixture()
	p := f.prepare("b", "source")
	p.Intent.Verb = "pull"
	sender := &senderProbe{state: x.SenderState{Identity: p.Identity, ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}}
	f.cfg.MakeSender = func(x.SendConfig) (x.Sender, error) { return sender, nil }
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, o.entries[key(p.Identity)].done)
	if sender.seals != 1 || sender.steps != 0 {
		t.Fatalf("unsealed source entered byte work: seals=%d steps=%d", sender.seals, sender.steps)
	}
}

// origin: hot-update S7 plan §4 / §11.3 / T14
func TestTransferWorkerCourierReceiptStopsBackgroundAttempts(t *testing.T) {
	f := newWorkerFixture()
	p := f.prepare("a", "ack")
	receipt := x.TerminalReceipt{Kind: "failed", CanonicalDigest: "exact", HistoryStream: "history", HistorySeq: 1}
	courier := &courierProbe{state: x.CourierState{Identity: p.Identity, Receipt: &receipt, Canonical: x.CanonicalRecord{Identity: p.Identity, Kind: "failed", Digest: "exact"}}}
	f.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) { return courier, nil }
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	await(t, o.entries[key(p.Identity)].done)
	status, err := o.Status(p.Identity)
	if err != nil || status.State != x.StatusTerminal || status.Phase != x.PhaseSettled || courier.steps != 1 || courier.stopped != 1 {
		t.Fatalf("courier ACK did not stop attempts: %+v %v steps=%d stops=%d", status, err, courier.steps, courier.stopped)
	}
}

// grantWaitingReceiver has synced every byte and reports commit_inflight until
// it is stepped again; the second step completes.
type grantWaitingReceiver struct {
	receiverProbe
	mu    sync.Mutex
	steps int
	again chan struct{}
}

func (r *grantWaitingReceiver) Step(x.Deadline) error {
	r.mu.Lock()
	r.steps++
	n := r.steps
	r.mu.Unlock()
	if n == 2 {
		close(r.again)
	}
	if n >= 2 {
		return io.EOF
	}
	return workerError(x.CodeCommitInflight)
}

// TestTransferReceiverRunnerWaitsForItsGrant: a receiver that has synced every
// byte and awaits its durable Grant keeps its runner. A Grant applied while that
// runner is still unwinding cannot restart it (Apply only restarts a stopped
// runner), so a runner that exited here would strand the committed bytes.
// origin: hot-update S7 plan §4 / INTEG-D outer commit adapter (late Grant race)
func TestTransferReceiverRunnerWaitsForItsGrant(t *testing.T) {
	f := newWorkerFixture()
	p := f.prepare("b", "await-grant")
	r := &grantWaitingReceiver{receiverProbe: receiverProbe{state: x.ReceiverState{Identity: p.Identity,
		ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}}, again: make(chan struct{})}
	f.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.again:
	case <-time.After(3 * time.Second):
		t.Fatal("a receiver awaiting its Grant lost its runner: nothing would resume the commit")
	}
}

// countingReceiver ends every step (io.EOF) and signals its second step.
type countingReceiver struct {
	receiverProbe
	mu      sync.Mutex
	steps   int
	resumed chan struct{}
}

func (r *countingReceiver) Step(x.Deadline) error {
	r.mu.Lock()
	r.steps++
	if r.steps == 2 {
		close(r.resumed)
	}
	r.mu.Unlock()
	return io.EOF
}

// TestTransferGrantResumesAnExitedReceiverRunner: the durable Grant is the
// event that lets a receiver commit. If its runner already exited, applying the
// Grant must resume the same owner rather than leave the synced bytes parked
// until an unrelated Start.
// origin: hot-update S7 plan §4 / INTEG-D outer commit adapter (Apply restarts the runner)
func TestTransferGrantResumesAnExitedReceiverRunner(t *testing.T) {
	f := newWorkerFixture()
	p := f.prepare("b", "resume")
	r := &countingReceiver{receiverProbe: receiverProbe{state: x.ReceiverState{Identity: p.Identity,
		ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}}, resumed: make(chan struct{})}
	f.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, o.entries[key(p.Identity)].done)
	grant := x.CommitGrant{Identity: p.Identity, GrantNonce: "late-grant", TerminalID: "terminal", ReceiverInstance: "agent"}
	if _, err := o.Apply(x.Control{Kind: x.ControlGrant, Identity: p.Identity, Grant: &grant}, deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.resumed:
	case <-time.After(3 * time.Second):
		t.Fatal("a Grant applied after the runner exited did not resume the receiver")
	}
}
