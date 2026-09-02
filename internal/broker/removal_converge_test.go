package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// removal_converge_test.go (formerly g3_removal_converge_test.go) — G3 #1 Stage-C M1: `cluster recovery node remove` (RemoveNode/removeGhost)
// must ALSO converge the published seeds — on a single-voter force-single cluster no later leadership edge
// fires the backstop, so this operator-finalizer is the ONLY convergence trigger after the ghost clears.
// (Reuses the g2_ghost_test.go harness: proposeCmd + d7SingleNode + d7JoinInput.)

func TestG3RemoveGhostConvergesSeeds(t *testing.T) {
	n, addr := d7SingleNode(t, "self-1")
	admin := NewClusterAdmin(n, nil)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	self := d7JoinInput(t, "self-1", addr)
	self.PublicHost = "self.example"
	if err := admin.AddNode(self, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode self: %v", err)
	}
	// Seed a force-single ghost: VOTER in the roster, ABSENT from the committed raft config, distinct host.
	ghost := d7JoinInput(t, "ghost-1", "10.9.9.9:7400")
	ghost.PublicHost = "ghost.example"
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
	// Published seeds carry BOTH self + the ghost — the stale-but-consistent state left when an online
	// force-single's inline prune failed (INV-4) and the operator now runs the documented finalizer.
	if _, err := admin.PublishSeeds([]string{"wss://self.example:443", "wss://ghost.example:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}
	// The finalizer: removeGhost deletes the roster row AND (M1 fix) converges the seeds.
	if err := admin.RemoveNode("ghost-1", false); err != nil {
		t.Fatalf("RemoveNode(ghost): %v", err)
	}
	eps, _, _, _ := admin.ReadSeeds()
	if strings.Contains(strings.Join(eps, ","), "ghost.example") {
		t.Errorf("M1: removing the ghost must drop its endpoint from published seeds, got %v", eps)
	}
	if len(eps) != 1 || eps[0] != "wss://self.example:443" {
		t.Errorf("M1: seeds must converge to just the survivor, got %v", eps)
	}
}
