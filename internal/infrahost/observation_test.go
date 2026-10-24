package infrahost

import (
	"reflect"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §2.2 / §5.2 / WIRE-2 R11
func TestHostObservationDoesNotReplayOrAdvanceMutation(t *testing.T) {
	f := newHostFixture(t)
	stage := f.request(t, c.Stage, 1)
	if _, err := f.worker.Do(stage, testDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	before, head, err := f.journal.Read(testDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	census := c.Census{Complete: true, Generation: 9, Digest: digestOf("live census"), Processes: []c.Process{testProcess()}}
	f.effects.do = func(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
		return c.HostReceipt{Census: census}, nil
	}
	r := f.request(t, c.VerifyIngress, 2)
	initialCalls := f.effects.calls.Load()
	for round := 0; round < 2; round++ {
		receipt, err := f.worker.Do(r, testDeadline(t), nil)
		if err != nil || receipt.Applied || receipt.Journal != head || receipt.Census.Digest != census.Digest {
			t.Fatalf("current observation: %+v %v", receipt, err)
		}
	}
	if got := f.effects.calls.Load() - initialCalls; got != 2 {
		t.Fatalf("observation replayed a cached seal: backend reads=%d", got)
	}
	census.Complete = false
	if _, err := f.worker.Do(r, testDeadline(t), nil); err == nil {
		t.Fatal("old zero-responsibility receipt survived an incomplete current read")
	}
	census.Complete = true
	readsBeforeRevocation := f.effects.calls.Load()
	f.authority.deny.Store(true)
	if _, err := f.worker.Do(r, testDeadline(t), nil); err == nil {
		t.Fatal("replayed observation bypassed revoked authority")
	}
	if f.effects.calls.Load() != readsBeforeRevocation {
		t.Fatal("revoked observer reached the physical backend")
	}
	f.authority.deny.Store(false)
	r.Tx = c.ID{99}
	r = f.sign(t, r)
	if _, err := f.worker.Do(r, testDeadline(t), nil); err == nil {
		t.Fatal("another transaction obtained the current ingress seal")
	}
	after, newHead, err := f.journal.Read(testDeadline(t))
	if err != nil || newHead != head || !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only observation consumed the driver's mutation cursor: %v", err)
	}
	// An independent effect during the physical read must be able to acquire its
	// CAS, and must invalidate this observation. Holding the journal lock across
	// the callback would deadlock this exact nested request.
	census.Complete = true
	r = f.request(t, c.VerifyIngress, 4)
	f.effects.do = func(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
		next := f.request(t, c.Stage, 3)
		if _, err := f.worker.Do(next, testDeadline(t), nil); err != nil {
			t.Fatalf("physical read held the mutation lock: %v", err)
		}
		return c.HostReceipt{Census: census}, nil
	}
	if _, err := f.worker.Do(r, testDeadline(t), nil); err == nil {
		t.Fatal("changed journal head certified an obsolete ingress observation")
	}

}
