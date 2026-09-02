package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// ghost_test.go (formerly g2_ghost_test.go; G2 #12-B) — membership-aware RemoveNode ghost passthrough: a roster row in a
// non-removable phase (a stale VOTER after force-single) that is ABSENT from the committed raft config is
// a force-single ghost and MUST be deletable online (the live racknerd pc732 deadlock), while a genuinely
// IN-CONFIG live VOTER must STILL hit the anti-fork phase-gate refusal.
func TestG2RemoveNodeGhostPassthrough(t *testing.T) {
	n, addr := d7SingleNode(t, "self-1")
	admin := NewClusterAdmin(n, nil)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	// self-1 becomes a real IN-CONFIG voter.
	if err := admin.AddNode(d7JoinInput(t, "self-1", addr), addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode self: %v", err)
	}

	// Seed a GHOST: upsert a roster row and walk it to phase=VOTER via the phase planner, but NEVER add it
	// to the raft config (exactly the state force-single leaves behind — VOTER in the roster, absent from
	// raft).
	ghost := d7JoinInput(t, "ghost-1", "10.9.9.9:7400")
	up, err := cluster.PlanClusterNodeUpsert(ghost)
	if err != nil {
		t.Fatalf("plan ghost upsert: %v", err)
	}
	proposeCmd(t, n, up)
	for _, step := range []struct {
		to    string
		preds []string
	}{
		{"CATCHING_UP", []string{"JOIN_VERIFIED_PENDING_VOTER"}},
		{"VOTER", []string{"CATCHING_UP"}},
	} {
		ph, perr := cluster.PlanClusterNodePhase("ghost-1", step.to, step.preds, "", time.Now())
		if perr != nil {
			t.Fatalf("plan phase %s: %v", step.to, perr)
		}
		proposeCmd(t, n, ph)
	}
	if ph, ok := d7ReadPhase(t, n, "ghost-1"); !ok || ph != phaseVoter {
		t.Fatalf("ghost seed: phase=%q ok=%v, want VOTER", ph, ok)
	}

	// THE passthrough: ghost is phase=VOTER but ABSENT from the committed raft config → RemoveNode deletes
	// the row (RemoveServer is a harmless no-op on an absent node) instead of the live-VOTER refusal.
	if err := admin.RemoveNode("ghost-1", false); err != nil {
		t.Fatalf("RemoveNode(ghost) must pass through and delete, got: %v", err)
	}
	if _, ok := d7ReadPhase(t, n, "ghost-1"); ok {
		t.Fatal("ghost roster row must be gone after RemoveNode")
	}

	// The safety property that ghost passthrough must NOT weaken: a real IN-CONFIG live VOTER (self-1)
	// still hits the anti-fork phase-gate refusal (never a bare delete → no roster/raft silent fork).
	if err := admin.RemoveNode("self-1", false); err == nil || !strings.Contains(err.Error(), "raw remove only finishes") {
		t.Fatalf("RemoveNode of an IN-CONFIG live VOTER must refuse, got: %v", err)
	}
}

func proposeCmd(t *testing.T, n *cluster.Node, cmd *cluster.Command) {
	t.Helper()
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cmd, nil }); err != nil {
		t.Fatalf("propose: %v", err)
	}
}
