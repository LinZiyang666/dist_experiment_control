package xferworker

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Real: registry admission, creation serialization and actor ownership.
// Injected: the first factory cannot open its attachment during a restart.
// origin: S7 internal review FIX-TIERA2 F7 / hot-update S7 plan §4 / §6.1 / T46
func TestTransferWorkerPrepareReplayRecoversUnallocatedSender(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePrepareSenderRecovery(t)
		exercisePreparePartialSenderFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "prepare sender recovery", g)
	testharness.AssertNoFDLeak(t, "prepare sender recovery", fd, 0)
}

func exercisePreparePartialSenderFailure(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	p := f.prepare("b", "partial-sender")
	p.Intent.Verb = "pull"
	calls := 0
	sender := &senderProbe{}
	f.cfg.MakeSender = func(x.SendConfig) (x.Sender, error) {
		calls++
		return sender, errors.New("partially allocated sender")
	}
	o := newOwner(t, f)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err == nil {
		t.Fatal("partial construction returned success")
	}
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(p.Identity); err == nil || calls != 1 {
		t.Fatal("prepare replay constructed over a retained partial actor")
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil || !sender.state.Stopped {
		t.Fatalf("partial actor lost its terminal owner: %v", err)
	}
}

func exercisePrepareSenderRecovery(t *testing.T) {
	t.Helper()
	f := newWorkerFixture()
	p := f.prepare("b", "recover-sender")
	p.Intent.Verb = "pull"
	calls := 0
	sender := &senderProbe{}
	f.cfg.MakeSender = func(x.SendConfig) (x.Sender, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("data attachment unavailable during restart")
		}
		return sender, nil
	}
	o := newOwner(t, f)
	first, err := o.Prepare(p, deadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	original := o.entries[key(p.Identity)].byteDeadline
	if err := o.Start(p.Identity); err == nil {
		t.Fatal("first construction did not encounter the restart")
	}
	replay, err := o.Prepare(p, deadline(t, time.Second))
	if err != nil || replay.Identity != first.Identity || replay.Origin != first.Origin || replay.Budget != first.Budget {
		t.Fatalf("prepare replay lost the original acceptance: %+v %v", replay, err)
	}
	if err := o.Start(p.Identity); err != nil {
		t.Fatalf("accepted prepare replay cached a transient unallocated actor failure: %v", err)
	}
	if calls != 2 || f.capacity.count() != 1 || o.entries[key(p.Identity)].byteDeadline != original {
		t.Fatal("prepare recovery recreated the entry, reservation or deadline")
	}
	if err := o.Stop(deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}
