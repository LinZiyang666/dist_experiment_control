package xferworker

import (
	"errors"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The actor owns source sealing and its original byte deadline. A registry
// prepare may predate those facts; the joined snapshot must update both its
// prepare/status projection and retention without asking a network peer.
// origin: hot-update S7 plan §4 / §8.2 / T14 / T35
func TestTransferWorkerSnapshotUsesJoinedActorFacts(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseJoinedActorSnapshot(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "joined transfer actor snapshot", g)
	testharness.AssertNoFDLeak(t, "joined transfer actor snapshot", fd, 0)
}

func exerciseJoinedActorSnapshot(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "snapshot")
	p.Intent.Verb, p.Budget.SourceSealed = "pull", false
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	budget := p.Budget
	budget.Size, budget.SourceSealed, budget.RemainingNS = 17, true, int64(29*time.Second)
	seal := x.SourceSeal{Size: 17, SHA256: p.Intent.SHA256, ChunkSize: p.Intent.ChunkSize, Chunks: 1}
	until := x.BootDeadline{BootID: "boot", Deadline: deadline(t, 29*time.Second)}
	// The existing actor probe supplies only the registry's joined boundary;
	// it performs no byte operation and cannot manufacture an IO receipt.
	o.entries[key(p.Identity)].sender = &senderProbe{state: x.SenderState{Identity: p.Identity, Budget: budget, SourceSeal: seal, ByteDeadline: until}}
	_, state := exportWorker(t, o)
	if len(state.Entries) != 1 || len(state.Senders) != 1 || len(state.ResultRetention) != 1 {
		t.Fatal("snapshot lost its actor")
	}
	entry := state.Entries[0]
	if !reflect.DeepEqual(entry.Prepare.Source, &seal) || entry.Prepare.Budget != budget || entry.Status.Budget != budget || state.ResultRetention[0].Deadline != until {
		t.Fatalf("registry retained pre-seal facts: prepare=%+v status=%+v retention=%+v", entry.Prepare, entry.Status, state.ResultRetention)
	}
}

// origin: S7 internal review rv-lifecycle F3 / hot-update S7 plan §8.2 / T14
func TestWorkerImportRestartsPreparedOnlyCourier(t *testing.T) {
	fixture := newWorkerFixture()
	current, err := NewWorker(fixture.cfg)
	if err != nil {
		t.Fatal(err)
	}
	original := current.(*owner)
	// Successful import replaces the old frozen heap and its single reservation.
	t.Cleanup(func() {
		if err := current.Stop(deadline(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	p := fixture.prepare("a", "courier-before-start")
	if _, err := original.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	original.entries[key(p.Identity)].courier = &courierProbe{state: x.CourierState{Identity: p.Identity}}
	original.entries[key(p.Identity)].value.Status.Phase = x.PhaseTerminalPending
	_, state := exportWorker(t, original)
	manager, _, err := ImportWorker(fixture.cfg, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	current = manager
	courier := &signaledCourier{stepped: make(chan struct{}), state: state.Couriers[0]}
	if err := AdoptActors(manager, []Actors{{Identity: p.Identity, Courier: courier}}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Thaw(Bindings{Authority: fixture.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-courier.stepped:
	case <-time.After(time.Second):
		t.Fatal("imported courier without byte Start lost its runner")
	}
	if err := manager.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

type signaledCourier struct {
	state   x.CourierState
	stepped chan struct{}
	once    sync.Once
}

func (c *signaledCourier) Step(x.Deadline) error {
	c.once.Do(func() { close(c.stepped) })
	return io.EOF
}
func (c *signaledCourier) Freeze(x.Deadline) (x.CourierState, error) { return c.state, nil }
func (*signaledCourier) Acknowledge(x.TerminalReceipt) error {
	return errors.New("no terminal receipt supplied")
}
func (*signaledCourier) Stop(x.Deadline) error { return nil }
