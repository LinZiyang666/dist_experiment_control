package xferworker

import (
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The settlement call remains in flight until the exercise releases it. The
// registry must retain that courier without serializing the next byte task.
type admissionCourier struct {
	courierProbe
	wait *callbackBarrier
}

func (c *admissionCourier) Step(x.Deadline) error {
	c.wait.block()
	return io.EOF
}

// origin: S7 internal review TRANSFER-MAKETEST-3 F1
// origin: hot-update S7 plan §1.2 / §4 / T14
func TestTransferLandedReceiverAllowsNextTierBWhileCourierWaits(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseReceiverAdmission(t)
	}
	testharness.AssertNoGoroutineLeak(t, "receiver courier admission", g)
	testharness.AssertNoFDLeak(t, "receiver courier admission", fd, 0)
}

func exerciseReceiverAdmission(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	p := f.prepare("b", "landed")
	grant := x.CommitGrant{Identity: p.Identity, GrantNonce: "original-grant", ReceiverInstance: p.Identity.AgentInstance,
		TerminalID: schema.TransferTerminalID(p.Identity.SID, p.Identity.TransferID, p.Identity.EntryNonce)}
	landed := x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveLink, ParentSynced: true}
	r := &receiverProbe{state: x.ReceiverState{Identity: p.Identity, Grant: grant, Outcome: landed,
		ByteDeadline: x.BootDeadline{BootID: "boot", Deadline: deadline(t, 30*time.Second)}}}
	wait := newBarrier()
	c := &admissionCourier{courierProbe: courierProbe{state: x.CourierState{Identity: p.Identity, Outcome: landed}}, wait: wait}
	f.cfg.MakeReceiver = func(x.ReceiveConfig) (x.Receiver, error) { return r, nil }
	f.cfg.MakeCourier = func(cfg x.CourierConfig) (x.Courier, error) {
		if cfg.Identity != p.Identity || cfg.Outcome != landed {
			t.Error("receiver did not hand its original landed outcome to the courier")
		}
		c.state.Capsule = cfg.Capsule
		return c, nil
	}
	o := newOwner(t, f)
	// Release the admitted callback before cleanup joins the owning runner,
	// including when an assertion fails while the courier is deliberately held.
	defer wait.open()
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	await(t, wait.entered)
	first := o.entries[key(p.Identity)]
	status, err := o.Status(p.Identity)
	if err != nil || status.Phase != x.PhaseTerminalPending || status.Terminal != nil || status.CleanupPending {
		t.Fatalf("fixture did not reach landed courier-only wait: %+v %v", status, err)
	}
	next := f.prepare("b", "next")
	next.Identity.SID = p.Identity.SID
	next.Permit.Identity = next.Identity
	if _, err := o.Prepare(next, deadline(t, time.Second)); err != nil {
		t.Fatalf("landed receiver's courier blocked the next Tier-B prepare: %v", err)
	}
	if o.entries[key(p.Identity)] != first || first.courier != c || first.value.Status.Terminal != nil || f.capacity.count() != 2 {
		t.Fatal("admission discarded the unacknowledged courier or its reservation")
	}
	// The next entry still owns byte serialization. Releasing it for a courier
	// must not release an ordinary prepared receiver's slot as well.
	third := next
	third.Identity.TransferID, third.Identity.EntryNonce = "third", "nonce-third"
	third.Permit.Identity = third.Identity
	_, err = o.Prepare(third, deadline(t, time.Second))
	requireCode(t, err, x.CodeCapacityExceeded)
	if f.capacity.count() != 2 {
		t.Fatal("byte single-flight refusal allocated capacity")
	}
	f.capacity.fail = true
	_, err = o.Prepare(f.prepare("b", "account-full"), deadline(t, time.Second))
	requireCode(t, err, x.CodeCapacityExceeded)
	f.capacity.fail = false
	// Courier/result responsibility still counts toward the registry's 1024
	// entries, even though it no longer excludes byte work in this session.
	for slot := 2; slot < registrySlots; slot++ {
		if _, err := o.Prepare(f.prepare("b", fmt.Sprintf("slot-%d", slot)), deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	_, err = o.Prepare(f.prepare("b", "overflow"), deadline(t, time.Second))
	requireCode(t, err, x.CodeCapacityExceeded)
	if len(o.entries) != registrySlots || f.capacity.count() != registrySlots || o.entries[key(p.Identity)] != first {
		t.Fatal("courier admission evicted responsibility or exceeded the registry/account limit")
	}
	wait.open()
	if err := o.Stop(deadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if f.capacity.count() != 0 {
		t.Fatal("owner stop failed to retire retained reservations")
	}
}
