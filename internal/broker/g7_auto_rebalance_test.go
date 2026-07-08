package broker

import "testing"

// g7_auto_rebalance_test.go — G7a #18: the pure debounce arm. Adversarial cases for the return-dwell,
// flap-cancel, gate-defer, and cooldown semantics — no DB/raft, so the oscillation logic is pinned here.

// alwaysUp / neverUp are stillUp predicates.
func upSet(up map[string]bool) func(string) bool {
	return func(n string) bool { return up[n] }
}

// driveFires runs the arm for n ticks with a constant `returned` set only on the FIRST tick (the return
// edge fires once), a constant stillUp predicate, and constant gatesClear; returns how many times it fired.
func driveFires(a *autoRebalanceArm, n int, firstReturn []string, stillUp func(string) bool, gatesClear bool) int {
	fires := 0
	for i := 0; i < n; i++ {
		var ret []string
		if i == 0 {
			ret = firstReturn
		}
		if a.tick(ret, stillUp, gatesClear) {
			fires++
		}
	}
	return fires
}

// TestAutoRebalanceArmSteadyReturnFiresExactlyOnce: a broker that returns and stays up (gates clear)
// triggers EXACTLY one rebalance after the dwell, then never re-fires from the same return.
func TestAutoRebalanceArmSteadyReturnFiresExactlyOnce(t *testing.T) {
	a := newAutoRebalanceArm()
	up := upSet(map[string]bool{"b": true})
	// dwell is autoRebalanceReturnDwellTicks; drive well past it.
	fires := driveFires(a, autoRebalanceReturnDwellTicks+8, []string{"b"}, up, true)
	if fires != 1 {
		t.Fatalf("steady return must fire exactly once, got %d", fires)
	}
	if len(a.pending) != 0 {
		t.Fatalf("pending must be cleared after a fire, got %v", a.pending)
	}
	if a.cooldown == 0 {
		t.Fatalf("a fire must enter cooldown")
	}
}

// TestAutoRebalanceArmFlapNeverFires: a broker that returns then flaps back DOWN before the dwell
// completes must NOT trigger a rebalance — the dwell is cancelled.
func TestAutoRebalanceArmFlapNeverFires(t *testing.T) {
	a := newAutoRebalanceArm()
	fires := 0
	for i := 0; i < autoRebalanceReturnDwellTicks+4; i++ {
		var ret []string
		if i == 0 {
			ret = []string{"b"}
		}
		// b is up for the first 2 ticks, then flaps down for the rest (never satisfies the dwell).
		up := upSet(map[string]bool{"b": i < 2})
		if a.tick(ret, up, true) {
			fires++
		}
	}
	if fires != 0 {
		t.Fatalf("a broker that flaps back down mid-dwell must never fire, got %d", fires)
	}
}

// TestAutoRebalanceArmGatesDefer: while the fire-gates are CLOSED the dwell-satisfied return is held
// (deferred, not cancelled); it fires on the first tick the gates open.
func TestAutoRebalanceArmGatesDefer(t *testing.T) {
	a := newAutoRebalanceArm()
	up := upSet(map[string]bool{"b": true})
	// gates closed for the whole dwell + a few extra ticks → no fire yet, but the return stays armed.
	fires := driveFires(a, autoRebalanceReturnDwellTicks+5, []string{"b"}, up, false)
	if fires != 0 {
		t.Fatalf("gates closed must defer the fire, got %d", fires)
	}
	if len(a.pending) == 0 {
		t.Fatalf("a dwell-satisfied return must stay armed while gates are closed (defer, not cancel)")
	}
	// open the gates: fires on the next tick, exactly once.
	got := 0
	for i := 0; i < 4; i++ {
		if a.tick(nil, up, true) {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("must fire exactly once when gates open, got %d", got)
	}
}

// TestAutoRebalanceArmCooldownSuppressesSecondReturn: after a fire, a NEW return during the cooldown
// window is suppressed until the cooldown expires.
func TestAutoRebalanceArmCooldownSuppressesSecondReturn(t *testing.T) {
	a := newAutoRebalanceArm()
	up := upSet(map[string]bool{"b": true, "c": true})
	if driveFires(a, autoRebalanceReturnDwellTicks+2, []string{"b"}, up, true) != 1 {
		t.Fatal("first return should fire once")
	}
	// a fresh return of c arrives well inside the cooldown; drive dwell+ ticks — must NOT fire yet.
	fires := 0
	ticksInCooldown := autoRebalanceReturnDwellTicks + 2
	for i := 0; i < ticksInCooldown; i++ {
		var ret []string
		if i == 0 {
			ret = []string{"c"}
		}
		if a.tick(ret, up, true) {
			fires++
		}
	}
	if fires != 0 {
		t.Fatalf("a return during cooldown must be suppressed, got %d", fires)
	}
	// drain the remaining cooldown; c is still armed (dwell satisfied) → fires once cooldown clears.
	got := 0
	for i := 0; i < autoRebalanceCooldownTicks; i++ {
		if a.tick(nil, up, true) {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("c must fire exactly once after cooldown expires, got %d", got)
	}
}

// TestAutoRebalanceArmResetClearsPending: a leadership-lost reset drops armed state so a demoted leader
// carries nothing into a future term.
func TestAutoRebalanceArmResetClearsPending(t *testing.T) {
	a := newAutoRebalanceArm()
	up := upSet(map[string]bool{"b": true})
	// arm a return partway through its dwell, then reset.
	for i := 0; i < 3; i++ {
		a.tick(func() []string {
			if i == 0 {
				return []string{"b"}
			}
			return nil
		}(), up, true)
	}
	if len(a.pending) == 0 {
		t.Fatal("precondition: b should be armed before reset")
	}
	a.reset()
	if len(a.pending) != 0 || a.cooldown != 0 {
		t.Fatalf("reset must clear pending+cooldown, got pending=%v cooldown=%d", a.pending, a.cooldown)
	}
	// after reset, no lingering fire.
	if driveFires(a, autoRebalanceReturnDwellTicks+4, nil, up, true) != 0 {
		t.Fatal("no fire should occur after reset with no new return")
	}
}
