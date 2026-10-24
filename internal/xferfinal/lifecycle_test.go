package xferfinal

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §8.2 / §11.3 / T21
func TestTransferLifecycleNoLeaks(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer finalizer lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "transfer finalizer lifecycle", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newFinalFixture(t, "original-nuid")
	finalizer, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = finalizer.Stop(finalDeadline(t, time.Second)) }()
	// A data/admission timeout cannot retire its owner. The same finalizer must
	// still advance from the original durable decision on the next bounded pass.
	if _, err := finalizer.Advance(f.row.Identity, 1); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("expired pass was admitted: %v", err)
	}
	f.historyErr = errInjected
	if status, err := finalizer.Advance(f.row.Identity, finalDeadline(t, time.Second)); !errors.Is(err, errInjected) || !status.CleanupPending {
		t.Fatalf("terminal-pending responsibility disappeared: %+v %v", status, err)
	}
	f.historyErr, f.resolveErr = nil, errInjected
	if status, err := finalizer.Advance(f.row.Identity, finalDeadline(t, time.Second)); !errors.Is(err, errInjected) || status.Phase == xfercontract.PhaseSettled {
		t.Fatalf("revoked/offline receiver was guessed retired: %+v %v", status, err)
	}
	f.resolveErr = nil
	// Simulate a cut while an actual callback is entered. Stop must report its
	// timeout, retain the old ports, and join that callback once it can finish.
	entered, release, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	f.onConfirm = func() {
		once.Do(func() { close(entered); <-release })
	}
	defer onceRelease(release)()
	advanceDeadline := finalDeadline(t, time.Second)
	go func() {
		_, err := finalizer.Advance(f.row.Identity, advanceDeadline)
		result <- err
	}()
	waitFinalizerEntered(t, entered)
	if err := finalizer.Stop(finalDeadline(t, 15*time.Millisecond)); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("Stop did not retain an entered callback: %v", err)
	}
	stoppedResult := make(chan error, 1)
	stoppedDeadline := finalDeadline(t, time.Second)
	go func() {
		_, err := finalizer.Advance(f.row.Identity, stoppedDeadline)
		stoppedResult <- err
	}()
	if err := waitFinalizerResult(t, stoppedResult); err == nil || errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("closed finalizer failed to reject future admission immediately: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("blocked callback left before release: %v", err)
	default:
	}
	close(release)
	if err := waitFinalizerResult(t, result); err != nil {
		t.Fatalf("original owner could not finish its accepted callback: %v", err)
	}
	if err := finalizer.Stop(finalDeadline(t, time.Second)); err != nil {
		t.Fatalf("Stop did not join completed callback: %v", err)
	}
	// A current-owner reconstruction reads the progressed durable revision,
	// never a snapshot of the old generation before history/cleanup ACKs.
	f.onConfirm = nil
	current, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	status, err := current.Advance(f.row.Identity, finalDeadline(t, time.Second))
	if err != nil || status.Phase != xfercontract.PhaseSettled {
		t.Fatalf("current replay lost a progressed receipt: %+v %v", status, err)
	}
	if err := current.Stop(finalDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// Cleanup must release a blocked callback even if an assertion aborts the
// exercise. No independent goroutine is abandoned to make a leak test pass.
func onceRelease(ch chan struct{}) func() {
	return func() {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func waitFinalizerEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("finalizer never entered the real callback")
	}
}

func waitFinalizerResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("finalizer callback never joined")
		return nil
	}
}

// origin: hot-update S7 plan §8.2 / T21
func TestTransferFinalizerQueuedAdvanceUsesOriginalDeadline(t *testing.T) {
	f := newFinalFixture(t, "original-nuid")
	finalizer, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, result := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	f.onConfirm = func() { once.Do(func() { close(entered); <-release }) }
	defer func() {
		onceRelease(release)()
		_ = finalizer.Stop(finalDeadline(t, time.Second))
	}()
	advanceDeadline := finalDeadline(t, time.Second)
	go func() {
		_, err := finalizer.Advance(f.row.Identity, advanceDeadline)
		result <- err
	}()
	waitFinalizerEntered(t, entered)
	if _, err := finalizer.Advance(f.row.Identity, finalDeadline(t, 15*time.Millisecond)); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("queued operation reset or ignored its original deadline: %v", err)
	}
	f.mu.Lock()
	reads := len(f.events)
	f.mu.Unlock()
	if reads != 1 {
		t.Fatalf("queued timeout touched decision state: %d events", reads)
	}
	close(release)
	if err := waitFinalizerResult(t, result); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.1–§8.2 / T21
func TestTransferFinalizerRejectsInvalidClockAndPorts(t *testing.T) {
	f := newFinalFixture(t, "original-nuid")
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Clock = nil }, func(c *Config) { c.Decisions = nil },
		func(c *Config) { c.Ledger = nil }, func(c *Config) { c.History = nil },
		func(c *Config) { c.Objects = nil }, func(c *Config) { c.ResolveReceiver = nil },
		func(c *Config) { c.Clock = func() (string, int64, error) { return "", 0, nil } },
		func(c *Config) { c.Clock = func() (string, int64, error) { return "", 0, errInjected } },
	} {
		cfg := f.config()
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Fatal("incomplete ports/clock produced a successful finalizer")
		}
	}
	boot := "boot"
	cfg := f.config()
	cfg.Clock = func() (string, int64, error) {
		_, now, err := fixtureClock()
		return boot, now, err
	}
	finalizer, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	boot = "other-boot"
	if _, err := finalizer.Advance(f.row.Identity, finalDeadline(t, time.Second)); err == nil {
		t.Fatal("changed boot admitted old absolute deadline")
	}
	boot = "boot"
	if _, err := finalizer.Advance(f.row.Identity, 0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline was accepted: %v", err)
	}
	if err := finalizer.Stop(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("invalid Stop deadline reported joined: %v", err)
	}
	if len(f.events) != 0 {
		t.Fatalf("invalid admission performed side effects: %v", f.events)
	}
	if err := finalizer.Stop(finalDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}
