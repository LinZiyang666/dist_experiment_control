package xferworker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// A push receiver starts only after the whole upload, a pull sender after its
// start command. Both actors must inherit what remains of the entry's original
// local deadline at that moment, never the prepare-time remainder again.
// origin: S7 FIX-CUT actor deadline refresh / hot-update S7 plan §1.2 / LQ2 / M11
func TestTransferWorkerActorsKeepEntryOriginalDeadline(t *testing.T) {
	for _, verb := range []string{"push", "pull"} {
		t.Run(verb, func(t *testing.T) { exerciseActorOriginalDeadline(t, verb) })
	}
}

func exerciseActorOriginalDeadline(t *testing.T, verb string) {
	const wait = 9 * time.Second
	f := newWorkerFixture()
	var offset atomic.Int64
	f.cfg.Clock = func() (string, int64, error) {
		now, err := controlbudget.Now()
		return "boot", now + offset.Load(), err
	}
	var mu sync.Mutex
	var got []x.BudgetSeal
	f.cfg.MakeReceiver = func(cfg x.ReceiveConfig) (x.Receiver, error) {
		mu.Lock()
		got = append(got, cfg.Budget)
		mu.Unlock()
		return &receiverProbe{state: x.ReceiverState{Identity: cfg.Identity}}, nil
	}
	f.cfg.MakeSender = func(cfg x.SendConfig) (x.Sender, error) {
		mu.Lock()
		got = append(got, cfg.Budget)
		mu.Unlock()
		return &senderProbe{}, nil
	}
	o := newOwner(t, f)
	// Stop's caller deadline is real BOOTTIME; restore it before cleanup.
	t.Cleanup(func() { offset.Store(0) })
	p := f.prepare("b", "deadline-"+verb)
	p.Intent.Verb = verb
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	original := o.entries[key(p.Identity)].byteDeadline
	o.mu.Unlock()
	// The actor starts after a real-time wait; its local clock moved with it.
	offset.Store(int64(wait))
	f.advance(wait)
	if err := o.Start(p.Identity); err != nil {
		t.Fatal(err)
	}
	_, after, err := f.cfg.Clock()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("actor constructions=%d, want one", len(got))
	}
	// The actor computes its deadline as its own now plus RemainingNS. Any
	// value above the entry's remainder silently extends the original budget.
	if got[0].RemainingNS <= 0 || got[0].RemainingNS > int64(original.Deadline)-after+int64(time.Second) ||
		got[0].RemainingNS > p.Budget.RemainingNS-int64(wait)+int64(time.Second) {
		t.Fatalf("%s actor budget extended the original deadline by its start delay: remaining=%s prepared=%s wait=%s",
			verb, time.Duration(got[0].RemainingNS), time.Duration(p.Budget.RemainingNS), wait)
	}
	if got[0].StartedAt != p.Budget.StartedAt || got[0].EffectiveBudgetNS != p.Budget.EffectiveBudgetNS {
		t.Fatal("actor budget lost the original start or effective budget")
	}
}
