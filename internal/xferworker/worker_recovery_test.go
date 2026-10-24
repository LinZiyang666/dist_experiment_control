// Copy into internal/xferworker. Real worker, receiver, WriteAt and Sync;
// a deterministic inline reader replaces transport. No child processes.
package xferworker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferreceive"
)

type recoveryCapacity struct{ n atomic.Uint64 }

func (c *recoveryCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return x.ReservationID(fmt.Sprint(c.n.Add(1))), nil
}
func (*recoveryCapacity) Release(x.ReservationID) error { return nil }

type recoveryAuthority struct{}

func (recoveryAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }
func recoveryClock() (string, int64, error)                                { n, e := controlbudget.Now(); return "boot", n, e }
func recoveryDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, e := controlbudget.After(5 * time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return d
}

type recoveryReader struct {
	n  int
	cp x.SinkCheckpoint
}

func (r *recoveryReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	if r.n == 2 {
		return x.Chunk{}, io.EOF
	}
	b := []byte([]string{"abc", "def"}[r.n])
	off := int64(3 * r.n)
	r.n++
	return x.Chunk{Data: b, Receipt: x.ChunkReceipt{Offset: off, Length: 3, SHA256: sha256.Sum256(b)}}, nil
}
func (r *recoveryReader) ConfirmSynced(c x.SinkCheckpoint) error { r.cp = c; return nil }
func (r *recoveryReader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{SinkCheckpoint: r.cp}, nil
}
func (*recoveryReader) Stop(x.Deadline) error { return nil }
func (*recoveryReader) Thaw(x.Deadline) error { return nil }

func newRecoveryWorker(t *testing.T) (*owner, *entry, x.Receiver) {
	t.Helper()
	now := time.Now().UTC()
	sum := sha256.Sum256([]byte("abcdef"))
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "digest"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	base := x.Base{Clock: recoveryClock, UTC: time.Now, Capacity: &recoveryCapacity{}}
	p := x.Prepare{Identity: id, Origin: origin, Inline: []byte("abcdef"), Intent: x.Intent{Verb: "push", Tier: "a", Path: "destination", Size: 6, SHA256: hex.EncodeToString(sum[:])}, Budget: x.BudgetSeal{StartedAt: now, Size: 6, EffectiveBudgetNS: int64(30 * time.Second), RemainingNS: int64(30 * time.Second), SourceSealed: true}}
	p.Permit = x.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "verified"}
	m, err := NewWorker(Config{Base: base, Identity: id, Authority: recoveryAuthority{}})
	if err != nil {
		t.Fatal(err)
	}
	o := m.(*owner)
	t.Cleanup(func() {
		if err := o.Stop(recoveryDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	if _, err := o.Prepare(p, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	vp, err := transferpath.ValidateForWrite(filepath.Join(dir, "destination"), []string{dir}, 0)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := transferpath.NewAtomicReceiver(vp, "owned", true)
	if err != nil {
		t.Fatal(err)
	}
	a, err := xferreceive.New(x.ReceiveConfig{Base: base, Identity: id, Budget: p.Budget, Reader: &recoveryReader{}, Atomic: sink})
	if err != nil {
		t.Fatal(err)
	}
	e := o.entries[key(id)]
	e.receiver = a
	return o, e, a
}

// origin: S7 recovery review; observing a checkpoint must not pause byte IO.
// origin: S7 internal review rv-recovery F01
func TestRecoveryWorkerProgressObservationLeavesReceiverRunnable(t *testing.T) {
	o, e, _ := newRecoveryWorker(t)
	if err := o.step(e, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if e.value.Status.DurableOffset != 3 {
		t.Fatalf("first synced prefix=%d", e.value.Status.DurableOffset)
	}
	if err := o.step(e, recoveryDeadline(t)); err != nil {
		t.Fatalf("second chunk stopped by the previous progress snapshot: %v", err)
	}
	if e.value.Status.DurableOffset != 6 {
		t.Fatalf("second synced prefix=%d", e.value.Status.DurableOffset)
	}
}

// origin: S7 recovery review; a refused cut must resume all original actors.
// origin: S7 internal review rv-recovery F01
func TestRecoveryWorkerThawAfterRefusedCutResumesReceiver(t *testing.T) {
	o, _, a := newRecoveryWorker(t)
	if err := a.Step(recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Freeze(recoveryDeadline(t)); err == nil {
		t.Fatal("expected refusal without an FD export binding")
	}
	if err := o.Thaw(Bindings{Authority: recoveryAuthority{}, OwnerGeneration: 1}, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.Step(recoveryDeadline(t)); err != nil {
		t.Fatalf("registry thaw succeeded but original receiver remained frozen: %v", err)
	}
}

// origin: S7 recovery review; Stop's closed gate is already an outcome witness.
// origin: S7 internal review rv-recovery F01
func TestRecoveryWorkerCancelCanReadStoppedReceiverOutcome(t *testing.T) {
	o, e, a := newRecoveryWorker(t)
	if err := a.Step(recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	called := false
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) {
		called = true
		return nil, fmt.Errorf("test stops at courier construction")
	}
	_, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: e.value.Prepare.Identity, Reason: x.CancelUser}, recoveryDeadline(t))
	out, outErr := xferreceive.Outcome(a)
	if outErr != nil || out.Outcome != x.OutcomeNotEntered {
		t.Fatalf("receiver did not close its gate: %+v %v", out, outErr)
	}
	if !called {
		t.Fatalf("confirmed cancellation never reached settlement courier: %v", err)
	}
}

// origin: S7 internal review rv-terminal F1 / rv-lifecycle F2
func TestWorkerCancellationPreservesSyncedPrefixForCourier(t *testing.T) {
	o, e, actor := newRecoveryWorker(t)
	if err := o.step(e, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	called := make(chan x.ReceiverOutcome, 1)
	o.cfg.MakeCourier = func(config x.CourierConfig) (x.Courier, error) {
		called <- config.Outcome
		return &reviewAckCourier{id: config.Identity}, nil
	}
	if _, err := o.Apply(x.Control{Kind: x.ControlCancel, Identity: e.value.Prepare.Identity, Reason: x.CancelUser}, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	outcome := <-called
	proof, err := xferreceive.Outcome(actor)
	if err != nil || proof != outcome || outcome.Bytes != 3 || outcome.Outcome != x.OutcomeNotEntered {
		t.Fatalf("cancel lost closed synced-prefix evidence: outcome=%+v proof=%+v err=%v", outcome, proof, err)
	}
	if err := actor.Reserve(x.CommitGrant{Identity: e.value.Prepare.Identity, GrantNonce: "late", ReceiverInstance: "receiver", TerminalID: "late"}); err == nil {
		t.Fatal("closed receiver accepted a late Grant")
	}
}
