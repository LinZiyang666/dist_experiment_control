package broker

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// TestExternalFailedNonvoterJoinCanBeRemovedOnline pins the recovery claim made by the v0.4.2
// implementation and runbook: an unreachable joiner is staged as a nonvoter, cannot wedge the old
// quorum, and must be removable online after the operation is aborted. Without this property the
// operator is forced back to force-single for the exact routine failed-grow case this feature is
// intended to eliminate.
func TestExternalFailedNonvoterJoinCanBeRemovedOnline(t *testing.T) {
	n, _ := d7SingleNode(t, "external-leader")
	admin := NewClusterAdmin(n, nil)
	admin.caughtUpFn = func(string, uint64) (bool, error) { return false, nil }

	bundle, _ := makeJoinBundle(t, "unreachable-joiner", "10.0.0.99:7400", "external-nonce")
	opID, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("StartJoinOperation: %v", err)
	}
	for i := 0; i < 8; i++ {
		admin.driveInFlightOperations()
	}

	sub, err := admin.readSubstrate("unreachable-joiner")
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	if !sub.inRaft || sub.isVoter || sub.phase != phaseCatchingUp {
		t.Fatalf("setup did not reach staged nonvoter/CATCHING_UP: %+v", sub)
	}
	if err := admin.AbortOp(opID); err != nil {
		t.Fatalf("AbortOp: %v", err)
	}

	if err := admin.RemoveNode("unreachable-joiner", false); err != nil {
		t.Fatalf("aborted failed join must be online-removable; got %v", err)
	}
}

// TestExternalAbortedJoinCannotBePromotedByStaleControllerTick exercises the operation/controller
// race behind the cleanup path. driveInFlightOperations snapshots non-terminal operations before it
// performs side effects. If an operator aborts after that read, the stale tick must not still promote
// the staged nonvoter: doing so leaves an ABORTED op with a CATCHING_UP raft voter that the safe raw
// remove path correctly refuses, recreating the failed-grow dead end (and can race roster deletion).
func TestExternalAbortedJoinCannotBePromotedByStaleControllerTick(t *testing.T) {
	n, _ := d7SingleNode(t, "external-stale-leader")
	admin := NewClusterAdmin(n, nil)
	admin.caughtUpFn = func(string, uint64) (bool, error) { return false, nil }

	bundle, _ := makeJoinBundle(t, "stale-joiner", "10.0.0.98:7400", "external-stale-nonce")
	opID, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("StartJoinOperation: %v", err)
	}
	for i := 0; i < 8; i++ {
		admin.driveInFlightOperations()
	}
	stale, err := cluster.OperationByID(n.RODB(), opID)
	if err != nil || stale == nil || stale.Terminal {
		t.Fatalf("capture pre-abort controller snapshot: op=%+v err=%v", stale, err)
	}
	if err := admin.AbortOp(opID); err != nil {
		t.Fatalf("AbortOp: %v", err)
	}

	admin.caughtUpFn = func(string, uint64) (bool, error) { return true, nil }
	admin.driveOne(stale) // deterministic simulation of the tick that read just before AbortOp.
	sub, err := admin.readSubstrate("stale-joiner")
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	if sub.isVoter {
		t.Fatalf("a stale controller tick promoted an ABORTED join to voter: %+v", sub)
	}
}

// TestExternalAbortDuringRetireTickCannotRemoveServer covers the same abort contract on the retire
// ladder. The R1 fix re-reads at driveOne entry and fences join's AddNonvoter/AddVoter, but a live
// operator can still abort while a retire tick is inside a readiness probe. No irreversible retire
// side effect (phase->RETIRING, RemoveServer, roster delete) may run after that abort commits.
func TestExternalAbortDuringRetireTickCannotRemoveServer(t *testing.T) {
	n, _ := d7SingleNode(t, "external-retire-leader")
	admin := NewClusterAdmin(n, nil)
	target := "retire-race-target"

	in := d7JoinInput(t, target, "10.0.0.88:7400")
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(in)
	}); err != nil {
		t.Fatalf("seed target roster: %v", err)
	}
	if err := admin.setPhase(target, phaseDraining, []string{"JOIN_VERIFIED_PENDING_VOTER"}, ""); err != nil {
		t.Fatalf("seed target DRAINING phase: %v", err)
	}
	if err := n.AddNonvoter(target, "10.0.0.88:7400"); err != nil {
		t.Fatalf("seed target raft nonvoter: %v", err)
	}

	opID := "op-retire-abort-race"
	now := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	if err := n.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(cluster.OpStartInput{
			OpID: opID, Kind: cluster.OpKindRetire, TargetNode: target,
			InitState: cluster.OpStateRaftRemoved, Confirmed: true,
			Timeline: `[{"s":"RAFT_REMOVED"}]`,
		}, now)
	}); err != nil {
		t.Fatalf("seed retire op: %v", err)
	}
	op, err := cluster.OperationByID(n.RODB(), opID)
	if err != nil || op == nil || op.Terminal {
		t.Fatalf("capture retire op: op=%+v err=%v", op, err)
	}
	admin.streamsReadyFn = func(string) (bool, error) {
		if err := admin.AbortOp(opID); err != nil {
			t.Fatalf("AbortOp from readiness hook: %v", err)
		}
		return true, nil
	}

	admin.driveOne(op)
	got, err := cluster.OperationByID(n.RODB(), opID)
	if err != nil {
		t.Fatalf("read op after drive: %v", err)
	}
	if got == nil || got.OpState != cluster.OpStateAborted || !got.Terminal {
		t.Fatalf("abort did not remain terminal after retire tick: %+v", got)
	}
	sub, err := admin.readSubstrate(target)
	if err != nil {
		t.Fatalf("read substrate after aborted retire tick: %v", err)
	}
	if !sub.inRaft || sub.phase != phaseDraining {
		t.Fatalf("retire side effects ran after AbortOp committed; substrate=%+v", sub)
	}
}

// TestExternalNatsRouteRejectsCredentialsAndURLDecorations prevents a route credential from being
// persisted into cluster_nodes.nats_route. That column is included in the signed agent roster, so
// accepting userinfo would distribute the credential to every agent. This project authenticates
// broker routes with mTLS; only a bare nats://host:port authority is expected here.
func TestExternalNatsRouteRejectsCredentialsAndURLDecorations(t *testing.T) {
	for _, route := range []string{
		"nats://alice:secret@broker.example.com:6222",
		"nats://broker.example.com:6222/path",
		"nats://broker.example.com:6222/",
		"nats://broker.example.com:6222?token=secret",
		"nats://broker.example.com:6222#fragment",
	} {
		if err := validateNatsRoute(route, false); err == nil {
			t.Errorf("validateNatsRoute(%q) accepted non-authority data; want rejection", route)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("validateNatsRoute(%q) leaked credential in error: %v", route, err)
		}
	}
}

// TestExternalReplicatedRoutePlanRejectsNonAuthorityAndInvalidPorts closes the non-admin bypass:
// the replicated planner is a trust boundary too. It must enforce the exact bare-authority grammar
// and numeric port range even if a future caller does not use broker.validateNatsRoute first.
func TestExternalReplicatedRoutePlanRejectsNonAuthorityAndInvalidPorts(t *testing.T) {
	for _, route := range []string{
		"nats://broker.example.com:6222/",
		"nats://broker.example.com:0",
		"nats://broker.example.com:65536",
		"nats://broker.example.com:not-a-port",
	} {
		if _, err := cluster.PlanClusterNodeRoute("node-a", route, time.Now()); err == nil {
			t.Errorf("PlanClusterNodeRoute(%q) accepted a non-authority or invalid port", route)
		}
	}
}
