package xfercontrol

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

type blockedAuthority struct {
	entered chan struct{}
	release chan struct{}
	first   atomic.Bool
}

func (b *blockedAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error {
	if !b.first.Swap(true) {
		close(b.entered)
		<-b.release
	}
	return nil
}

// origin: hot-update S7 plan §8.2 / §12.2 L(xfercontrol)
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer controller drain x5", beforeG)
	testharness.AssertNoFDLeak(t, "transfer controller drain x5", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newControlFixture(t)
	f.live()
	b := &blockedAuthority{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(b.release) })
	cfg := f.config()
	cfg.Authority = b
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ctl = c
	d := testDeadline(t)
	done := make(chan error, 1)
	go func() {
		_, handleErr := c.Handle(f.request(x.OpStatus), f.principal, d)
		done <- handleErr
	}()
	defer func() {
		release.Do(func() { close(b.release) })
		if err := c.Stop(testDeadline(t)); err != nil {
			t.Errorf("cleanup did not join entered handler: %v", err)
		}
	}()
	select {
	case <-b.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never entered authority")
	}
	short, err := controlbudget.After(15 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(short); err == nil || !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("Stop claimed a blocked handler was joined: %v", err)
	}
	resp, err := c.Handle(f.request(x.OpStatus), f.principal, testDeadline(t))
	requireCode(t, resp, err, x.CodeOwnerRecovering)
	select {
	case err := <-done:
		t.Fatalf("blocked handler was abandoned: %v", err)
	default:
	}
	release.Do(func() { close(b.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("original handler lost ownership: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original handler did not finish after release")
	}
	if err := c.Stop(testDeadline(t)); err != nil {
		t.Fatalf("joined Stop: %v", err)
	}
	resp, err = c.Handle(f.request(x.OpPrepare), f.principal, testDeadline(t))
	requireCode(t, resp, err, x.CodeOwnerRecovering)

	// A replacement ingress borrows the current tracker. Data transport can be
	// unavailable while the retained receiver still cancels/settles; retiring
	// the previous controller must not have destroyed that responsibility.
	f.ctl, err = New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	f.probeErr, f.forwardErr = errors.New("transport unavailable"), errors.New("transport unavailable")
	f.status.Phase = x.PhaseTerminalPending
	p := f.principal
	p.Role = x.RoleSettlement
	if _, err := f.ctl.Handle(f.request(x.OpCancel), p, testDeadline(t)); err != nil {
		t.Fatalf("retained cancel needed ordinary transport: %v", err)
	}
	resp, err = f.ctl.Handle(f.request(x.OpTerminal), p, testDeadline(t))
	if err != nil || resp.Receipt == nil || *resp.Receipt != f.receipt || len(f.requests) != 0 {
		t.Fatalf("retained settlement lost exact receipt or opened data: %+v %v", resp, err)
	}
	if err := f.ctl.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §1.2 / §8.2 / P16
func TestTransferControllerRejectsUnboundedOrChangedBoot(t *testing.T) {
	for _, change := range []string{"expired", "zero", "boot", "missing_clock", "missing_tracker", "missing_transport"} {
		t.Run(change, func(t *testing.T) {
			f := newControlFixture(t)
			cfg := f.config()
			switch change {
			case "missing_clock":
				cfg.Clock = nil
			case "missing_tracker":
				cfg.Tracker = nil
			case "missing_transport":
				cfg.Transport = nil
			}
			if change == "missing_clock" || change == "missing_tracker" || change == "missing_transport" {
				if got, err := New(cfg); err == nil || got != nil {
					t.Fatalf("partial constructor: %v %v", got, err)
				}
				return
			}
			d := testDeadline(t)
			code := x.CodeBudgetExceeded
			switch change {
			case "expired":
				d = 1
			case "zero":
				d = 0
			case "boot":
				f.boot = "foreign-boot"
				code = x.CodeOwnerRecovering
			}
			r, err := f.ctl.Handle(f.request(x.OpPrepare), f.principal, d)
			requireCode(t, r, err, code)
			if len(f.calls) != 0 {
				t.Fatal("invalid clock/deadline reached ports")
			}
		})
	}
	t.Run("long_deadline_capped_once", func(t *testing.T) {
		f := newControlFixture(t)
		d, err := controlbudget.After(time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		before, _ := controlbudget.Now()
		if _, err := f.ctl.Handle(f.request(x.OpPrepare), f.principal, d); err != nil {
			t.Fatal(err)
		}
		after, _ := controlbudget.Now()
		for _, got := range f.deadlines {
			if int64(got) < before+int64(5*time.Second) || int64(got) > after+int64(5*time.Second) || got != f.deadlines[0] {
				t.Fatalf("phase received uncapped or renewed deadline: %v", f.deadlines)
			}
		}
	})
}
