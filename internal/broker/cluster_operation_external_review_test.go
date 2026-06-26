package broker

import (
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
)

func makeJoinBundleWithName(t *testing.T, nodeID, name, raftAddr, nonce string) string {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := cluster.EncodeJoinBundle(cluster.JoinBundle{
		NodeID:       nodeID,
		Name:         name,
		NodeIdentPub: pub,
		NatsServerID: nodeID,
		RaftAddr:     raftAddr,
		NatsRoute:    "nats://" + raftAddr,
		TunnelAddr:   nodeID + ":7000",
		JoinNonce:    nonce,
		JoinSigHex:   hex.EncodeToString(sig),
	})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestExternalReviewJoinApproveDoesNotReportSuccessWhenRosterAdmissionRejected(t *testing.T) {
	n, addr := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)

	existing := d7JoinInput(t, "existing", addr)
	existing.Name = "dup-name"
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(existing)
	}); err != nil {
		t.Fatalf("seed existing roster row: %v", err)
	}

	bundle := makeJoinBundleWithName(t, "join-2", "dup-name", "10.0.0.2:7400", "nonce-join-2")
	opID, err := admin.StartJoinOperation(bundle)
	if err == nil {
		t.Fatalf("join approve reported success op_id=%q even though roster admission cannot write duplicate name", opID)
	}
	if !strings.Contains(err.Error(), "dup-name") && !strings.Contains(err.Error(), "UNIQUE") && !strings.Contains(err.Error(), "admission") {
		t.Fatalf("join approve should explain the roster admission failure, got: %v", err)
	}
	active, aerr := cluster.ActiveOperationForTarget(n.RODB(), "join-2")
	if aerr != nil {
		t.Fatalf("read active op: %v", aerr)
	}
	if active != nil {
		t.Fatalf("failed roster admission left a non-terminal operation behind: %+v", active)
	}
}

// TestExternalReviewJoinApproveHappyPathStillAdmits locks the F1 fix against over-rejecting: a fresh,
// uniquely-named join MUST be admitted (roster row written) AND leave a non-terminal op for the
// controller to drive to SERVING.
func TestExternalReviewJoinApproveHappyPathStillAdmits(t *testing.T) {
	n, addr := d7SingleNode(t, "ctl-1")
	_ = addr
	admin := NewClusterAdmin(n, nil)

	bundle := makeJoinBundleWithName(t, "join-ok", "node-ok", "10.0.0.9:7400", "nonce-ok")
	opID, err := admin.StartJoinOperation(bundle)
	if err != nil {
		t.Fatalf("a fresh, unique join must be admitted: %v", err)
	}
	if opID == "" {
		t.Fatal("expected a non-empty op id for a successful admission")
	}
	// The roster row was actually written.
	var cnt int
	if err := n.RODB().QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id = 'join-ok'`).Scan(&cnt); err != nil {
		t.Fatalf("count roster row: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("successful admission must write exactly one roster row, got %d", cnt)
	}
	// The driving op is active (the controller has work to do).
	active, err := cluster.ActiveOperationForTarget(n.RODB(), "join-ok")
	if err != nil {
		t.Fatalf("read active op: %v", err)
	}
	if active == nil || active.OpID != opID {
		t.Fatalf("a successful admission must leave its driving op active, got %+v", active)
	}
}

// TestExternalReviewJoinApproveDuplicateNamePreflightLeavesNoOp asserts the preflight rejects a
// duplicate name BEFORE creating an op row (operator intent is not consumed into a phantom op).
func TestExternalReviewJoinApproveDuplicateNamePreflightLeavesNoOp(t *testing.T) {
	n, addr := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	existing := d7JoinInput(t, "existing", addr)
	existing.Name = "taken"
	if err := n.Propose(func(*sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterNodeUpsert(existing)
	}); err != nil {
		t.Fatalf("seed existing roster row: %v", err)
	}
	// A SECOND, DIFFERENT node_id under the taken name → rejected; no op for the new node.
	bundle := makeJoinBundleWithName(t, "newcomer", "taken", "10.0.0.3:7400", "nonce-newcomer")
	if _, err := admin.StartJoinOperation(bundle); err == nil {
		t.Fatal("a duplicate name must be rejected up front")
	}
	if active, _ := cluster.ActiveOperationForTarget(n.RODB(), "newcomer"); active != nil {
		t.Fatalf("the duplicate-name preflight must not create an op row: %+v", active)
	}
}
