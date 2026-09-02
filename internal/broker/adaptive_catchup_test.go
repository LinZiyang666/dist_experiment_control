package broker

import (
	"testing"
	"time"
)

// adaptive_catchup_test.go (formerly g4_adaptive_catchup_test.go; G4 #7) — the catch-up deadline must scale with the command-domain DB size so a
// large/slow-but-healthy joiner is not false-BLOCKED, while a genuinely dead one still hits the max clamp.

func TestAdaptiveCatchupDeadline_InvariantsHold(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)

	bytes, err := admin.commandDomainDBBytes()
	if err != nil || bytes <= 0 {
		t.Fatalf("command-domain DB size must be readable + positive (it sizes the deadline): got %d, err %v", bytes, err)
	}

	before := time.Now()
	d := admin.adaptiveCatchupDeadline()
	after := time.Now()

	// Never SHORTER than the base 2-min deadline (a size-scaled deadline only ever extends). Allow the tick
	// between our `before` capture and the admin's internal now().
	minBase := before.Add(opCatchupTimeout).UnixNano()
	if d < minBase-int64(time.Second) {
		t.Fatalf("adaptive deadline must be >= base (never shorter than the fixed 2-min floor): d=%d base~=%d", d, minBase)
	}
	// Always CLAMPED to the max window (a huge DB can never produce an unbounded wait on a dead joiner).
	maxClamp := after.Add(opCatchupMaxWindow).UnixNano()
	if d > maxClamp+int64(time.Second) {
		t.Fatalf("adaptive deadline must be clamped to <= maxWindow: d=%d max~=%d", d, maxClamp)
	}
}
