package spawnsafe

import (
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

func localSpawnPolicy(t *testing.T) *Policy {
	t.Helper()
	p, err := New(Config{MountSource: func() ([]byte, error) { return nil, nil }, ProbeTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func waitSpawnState(t *testing.T, p *Policy, pred func() bool) {
	t.Helper()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return pred()
	}) {
		t.Fatal("spawn state did not reach its barrier")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT tests 1–2.
func TestSpawnCensusDistinguishesInFlightAndAbandoned(t *testing.T) {
	p := localSpawnPolicy(t)
	p.wedgeCeiling = 2
	a, b := p.acquireSpawnSlot(), p.acquireSpawnSlot()
	if c := p.Census(); c.Wedged != 0 || c.InFlight != 2 || p.WedgedCount() != 0 {
		t.Fatalf("ordinary slots reported as abandoned: %+v", c)
	}
	if err := p.ImportReady(); err == nil {
		t.Fatal("policy with an in-time slot accepted an import precheck")
	}
	if _, err := p.Import(State{Version: StateVersion}); err == nil {
		t.Fatal("policy with an in-time slot accepted imported state")
	}
	if s := p.acquireSpawnSlot(); s != nil {
		s.release()
		t.Fatal("in-time operations bypassed the shared wedge ceiling")
	}
	if p.TryAcquireSpawnSlot() {
		p.ReleaseSpawnSlot()
		t.Fatal("raw slots bypassed the in-time ceiling")
	}
	a.abandon()
	if c := p.Census(); c.Wedged != 1 || c.InFlight != 1 {
		t.Fatalf("abandon did not transfer ownership: %+v", c)
	}
	if s := p.acquireSpawnSlot(); s != nil {
		s.release()
		t.Fatal("abandon released capacity before reaping")
	}
	b.release()
	b.abandon() // A late timer must not mark a different caller's slot.
	if c := p.Census(); c.Wedged != 1 || c.InFlight != 0 {
		t.Fatalf("late timeout corrupted another operation: %+v", c)
	}
	if _, err := p.Export(); !errors.Is(err, ErrSpawnInFlight) {
		t.Fatalf("export accepted abandoned ownership: %v", err)
	}
	a.release()
	if c := p.Census(); c.Wedged != 0 || c.InFlight != 0 {
		t.Fatalf("reaper retained a slot: %+v", c)
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT non-Runtime slot census.
func TestSpawnTimeoutClassifiesTouchAndResolutionAsAbandoned(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseSpawnBackgroundTimeout(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSpawnTimeoutClassifiesTouchAndResolutionAsAbandoned", beforeG)
	testharness.AssertNoFDLeak(t, "TestSpawnTimeoutClassifiesTouchAndResolutionAsAbandoned", beforeFD, 0)
}

func exerciseSpawnBackgroundTimeout(t *testing.T) {
	for _, kind := range []string{"touch", "resolve"} {
		t.Run(kind, func(t *testing.T) {
			p := localSpawnPolicy(t)
			release := make(chan struct{})
			t.Cleanup(func() {
				close(release)
				waitSpawnState(t, p, func() bool { return p.wedged == 0 && p.inFlight == 0 })
			})
			if kind == "touch" {
				if p.boundedTouch(func() bool { <-release; return true }) {
					t.Fatal("blocked touch succeeded")
				}
			} else {
				p.resolver = func(string, []string) (string, bool) { <-release; return "/bin/true", true }
				if _, err := p.boundedResolveInDirs("true", nil); err == nil {
					t.Fatal("blocked resolution succeeded")
				}
			}
			if c := p.Census(); c.Wedged != 1 || c.InFlight != 0 {
				t.Fatalf("timed-out background operation was not abandoned: %+v", c)
			}
			if err := p.Freeze(time.Now().Add(time.Second)); !errors.Is(err, ErrSpawnInFlight) {
				p.Thaw()
				t.Fatalf("cut accepted an abandoned background operation: %v", err)
			}
		})
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT absolute cut deadline.
func TestSpawnCutGuardUsesAbsoluteDeadline(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseSpawnCutGuardDeadline(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSpawnCutGuardUsesAbsoluteDeadline", beforeG)
	testharness.AssertNoFDLeak(t, "TestSpawnCutGuardUsesAbsoluteDeadline", beforeFD, 0)
}

func exerciseSpawnCutGuardDeadline(t *testing.T) {
	p := localSpawnPolicy(t)
	p.mu.Lock()
	release := sync.OnceFunc(p.mu.Unlock)
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() { result <- p.Freeze(time.Now().Add(20 * time.Millisecond)) }()
	late := false
	select {
	case err := <-result:
		if !errors.Is(err, ErrSpawnInFlight) {
			t.Errorf("busy policy guard did not defer: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		late = true
	}
	release()
	if late {
		<-result
		t.Fatal("policy guard extended the absolute cut deadline")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 4.
func TestSpawnCutOrdersDrainBeforeRacingCreation(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseSpawnCutOrdering(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSpawnCutOrdersDrainBeforeRacingCreation", beforeG)
	testharness.AssertNoFDLeak(t, "TestSpawnCutOrdersDrainBeforeRacingCreation", beforeFD, 0)
}

func exerciseSpawnCutOrdering(t *testing.T) {
	p := localSpawnPolicy(t)
	slot := p.acquireSpawnSlot()
	release := sync.OnceFunc(slot.release)
	t.Cleanup(release)
	t.Cleanup(p.Thaw)
	frozen := make(chan error, 1)
	go func() { frozen <- p.Freeze(time.Now().Add(time.Second)) }()
	waitSpawnState(t, p, func() bool { return p.spawnCut != nil })
	started, done := make(chan struct{}), make(chan error, 1)
	go func() { done <- p.RunStart(func() error { close(started); return nil }, time.Second) }()
	t.Cleanup(func() { p.Thaw(); <-done })
	if p.TryAcquireSpawnSlot() {
		p.ReleaseSpawnSlot()
		t.Fatal("raw slot acquisition bypassed the closed fence")
	}
	select {
	case err := <-frozen:
		t.Fatalf("drain returned with an admitted slot still live: %v", err)
	case <-started:
		t.Fatal("racing creation acquired a slot before the drain")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-frozen; err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("racing creation started after the drain while the cut was closed")
	case <-time.After(20 * time.Millisecond):
	}
	if c := p.Census(); c.InFlight != 0 || c.Wedged != 0 {
		t.Fatalf("successful drain contains a live slot: %+v", c)
	}
	if _, err := p.Export(); err != nil {
		t.Fatalf("export after drain: %v", err)
	}
	p.Thaw()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("rollback did not wake the queued creation")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT tests 1–2.
func TestSpawnCutDeadlineAndAbandonReopenAdmission(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseSpawnCutDeadlineAndAbandon(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSpawnCutDeadlineAndAbandonReopenAdmission", beforeG)
	testharness.AssertNoFDLeak(t, "TestSpawnCutDeadlineAndAbandonReopenAdmission", beforeFD, 0)
}

func exerciseSpawnCutDeadlineAndAbandon(t *testing.T) {
	for _, abandon := range []bool{false, true} {
		p := localSpawnPolicy(t)
		slot := p.acquireSpawnSlot()
		result := make(chan error, 1)
		deadline := time.Now().Add(50 * time.Millisecond)
		go func() { result <- p.Freeze(deadline) }()
		waitSpawnState(t, p, func() bool { return p.spawnCut != nil })
		if abandon {
			slot.abandon()
		}
		select {
		case err := <-result:
			if !errors.Is(err, ErrSpawnInFlight) {
				t.Fatalf("drain hid the hazard: %v", err)
			}
		case <-time.After(200 * time.Millisecond):
			slot.release()
			p.Thaw()
			t.Fatal("drain extended its absolute deadline")
		}
		waitSpawnState(t, p, func() bool { return p.spawnCut == nil })
		if err := p.RunStart(func() error { return nil }, time.Second); err != nil {
			t.Fatalf("failed cut left admission closed: %v", err)
		}
		slot.release()
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 4.
func TestSpawnCutGatesTouchResolutionAndProbeLaunch(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseSpawnCutBackgroundOperations(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestSpawnCutGatesTouchResolutionAndProbeLaunch", beforeG)
	testharness.AssertNoFDLeak(t, "TestSpawnCutGatesTouchResolutionAndProbeLaunch", beforeFD, 0)
}

func exerciseSpawnCutBackgroundOperations(t *testing.T) {
	for _, kind := range []string{"touch", "resolve", "probe"} {
		t.Run(kind, func(t *testing.T) {
			p := localSpawnPolicy(t)
			started, done := make(chan struct{}), make(chan struct{})
			p.resolver = func(string, []string) (string, bool) { close(started); return "/bin/true", true }
			p.probe = func(string) bool { close(started); return true }
			if err := p.Freeze(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			go func() {
				defer close(done)
				switch kind {
				case "touch":
					p.boundedTouch(func() bool { close(started); return true })
				case "resolve":
					_, _ = p.boundedResolveInDirs("true", nil)
				case "probe":
					p.mountHealthy("/fake-nfs")
				}
			}()
			t.Cleanup(func() { p.Thaw(); <-done })
			select {
			case <-started:
				t.Fatal("background operation started beyond the completed drain")
			case <-time.After(20 * time.Millisecond):
			}
			if _, err := p.Export(); err != nil {
				t.Fatalf("queued operation escaped the fence: %v", err)
			}
			p.Thaw()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("thaw did not resume the background operation")
			}
		})
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT deadline cancellation.
func TestSpawnCutCancellationCannotOpenSuccessorFence(t *testing.T) {
	p := localSpawnPolicy(t)
	if err := p.Freeze(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	old := p.spawnCut
	p.Thaw()
	if err := p.Freeze(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	defer p.Thaw()
	p.cancelSpawnCut(old)
	if p.TryAcquireSpawnSlot() {
		p.ReleaseSpawnSlot()
		t.Fatal("a late cancellation opened a successor's cut")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT fence ownership.
func TestSpawnCutRefusesImportWhileAdmissionClosed(t *testing.T) {
	p := localSpawnPolicy(t)
	if err := p.Freeze(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	defer p.Thaw()
	if err := p.ImportReady(); err == nil {
		t.Fatal("closed policy accepted an import precheck")
	}
	if _, err := p.Import(State{Version: StateVersion}); err == nil {
		t.Fatal("import replaced a cut's policy state")
	}
	p.Thaw()
	if err := p.ImportReady(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Import(State{Version: StateVersion}); err != nil {
		t.Fatal(err)
	}
}
