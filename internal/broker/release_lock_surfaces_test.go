package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: g5_g7_external_rereview_test.go (renamed in B6)
//
// TestExternalReviewReleaseLockSurfacesProposeFailure pins the lock-release recovery invariant:
// "release-lock" must not report success when the replicated clear did not commit. A false OK leaves a
// stale cluster_upgrade_active marker while the ctl path believes membership has resumed.
func TestExternalReviewReleaseLockSurfacesProposeFailure(t *testing.T) {
	seed, _ := auth.GenerateUserSeed()
	var routed []adminsock.Request
	b := newTriggerTestBroker("brk-a", seed, triggerNow, &routed)
	n, _ := d7SingleNode(t, "brk-a")
	b.cl = &clusterRuntime{node: n}
	if err := n.Shutdown(); err != nil {
		t.Fatalf("shutdown node to force propose failure: %v", err)
	}

	req := &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: "brk-a", IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.OK {
		t.Fatalf("release-lock must fail when the raft clear cannot be proposed, got %+v", resp)
	}
}

// TestExternalReviewUpgradeLockFreezesExistingMembershipOps distinguishes a start-gate from a real
// cluster-scoped mutex. The developer fix blocks new join/retire calls, but an already non-terminal op
// must also stop being driven while a roll lock is held; otherwise membership can still cross a restart.
func TestExternalReviewUpgradeLockFreezesExistingMembershipOps(t *testing.T) {
	n, _ := d7SingleNode(t, "brk-a")
	admin := NewClusterAdmin(n, nil)
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: "op-existing-join", Kind: cluster.OpKindJoin, TargetNode: "brk-new",
			InitState: cluster.OpStateRosterCommitted, Timeline: `[{"s":"ROSTER_COMMITTED"}]`,
		}, now)
	}); err != nil {
		t.Fatalf("seed existing membership op: %v", err)
	}
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) { return cluster.PlanSetUpgradeActive(now) }); err != nil {
		t.Fatalf("set upgrade lock: %v", err)
	}

	admin.driveInFlightOperations()
	got, _ := cluster.OperationByID(n.RODB(), "op-existing-join")
	if got == nil || got.OpState != cluster.OpStateRosterCommitted {
		t.Fatalf("upgrade lock must freeze existing membership op at ROSTER_COMMITTED, got %+v", got)
	}
}

// TestExternalReviewSocketStatusSurfacesPeerJSUnavailable verifies the developer reply's "same runtime
// JS-503 signal" claim for the socket status path. A follower-host StatusReport should surface a peer
// health reply that reports JetStreamUnavailable, not only this broker's local jsUnavail flag.
func TestExternalReviewSocketStatusSurfacesPeerJSUnavailable(t *testing.T) {
	n, addr := d7SingleNode(t, "brk-b")
	admin := NewClusterAdmin(n, nil)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(d7JoinInput(t, "brk-b", addr), addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	admin.SetJSUnavailFn(func() bool { return false })
	admin.healthPoll = func() map[string]proto.ClusterHealthResp {
		return map[string]proto.ClusterHealthResp{
			"brk-a": {NodeID: "brk-a", JetStreamUnavailable: true},
		}
	}

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	if !strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("socket StatusReport must surface peer JetStreamUnavailable, got banner %q", rep.Banner)
	}
}
