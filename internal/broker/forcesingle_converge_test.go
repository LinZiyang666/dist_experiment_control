package broker

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// forcesingle_converge_test.go (formerly g3_forcesingle_converge_test.go) — G3 #1 Stage-C M3: online force-single must converge the published
// seeds to the survivor after pruning the abandoned peer. Drives the real arm→commit path (fsTestBackend).
func TestForceSingleOnlineConvergesSeeds(t *testing.T) {
	b, n := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1") // peer dead on all probed ports
	markQuorumLostPastDwell(b)

	// fsTestBackend gives both rows the same default public_host; give brk-b a DISTINCT one so seeds can
	// tell them apart. brk-b is PENDING → the phase-guarded upsert updates it.
	pb := d7JoinInput(t, "brk-b", "127.0.0.1:1")
	pb.PublicHost = "brk-b.example"
	pb.NatsRoute, pb.TunnelAddr = "nats://127.0.0.1:1", "127.0.0.1:1"
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) { return cluster.PlanClusterNodeUpsert(pb) }); err != nil {
		t.Fatalf("re-upsert brk-b host: %v", err)
	}
	// Steady-state seeds carry BOTH brokers (host.example = brk-a's default host, brk-b.example = brk-b).
	if _, err := b.admin.PublishSeeds([]string{"wss://host.example:443", "wss://brk-b.example:443"}, ""); err != nil {
		t.Fatalf("PublishSeeds: %v", err)
	}

	arm := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, NodeID: "brk-a", ConfirmPeersDead: []string{"brk-b"},
	})
	if !arm.OK || arm.ForceSingle == nil || arm.ForceSingle.ArmToken == "" {
		t.Fatalf("arm: OK=%v Code=%q err=%q", arm.OK, arm.Code, arm.Error)
	}
	commit := b.handleForceSingleCommit(adminsock.Request{
		Op: adminsock.OpClusterForceSingleCommit, NodeID: "brk-a", ArmToken: arm.ForceSingle.ArmToken, ConfirmPeersDead: []string{"brk-b"},
	})
	if !commit.OK {
		t.Fatalf("commit: Code=%q err=%q", commit.Code, commit.Error)
	}

	// INV-4: convergence is gated on prune SUCCESS (the `else if err==nil` branch in
	// handleForceSingleCommit). The prune-FAILURE path (which must NOT re-derive the dead endpoints) cannot
	// be injected against a REAL raft node without a mock Propose seam, so it is pinned by code structure +
	// the M1 removeGhost finalizer test (which exercises the stale-after-prune-fail recovery path).
	eps, _, _, _ := b.admin.ReadSeeds()
	if strings.Contains(strings.Join(eps, ","), "brk-b.example") {
		t.Errorf("M3: force-single must converge seeds to the survivor (drop brk-b), got %v", eps)
	}
	if len(eps) != 1 || eps[0] != "wss://host.example:443" {
		t.Errorf("M3: seeds must be just the survivor, got %v", eps)
	}
}
