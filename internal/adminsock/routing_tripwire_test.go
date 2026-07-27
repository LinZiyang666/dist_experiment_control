package adminsock

import "testing"

// routing_tripwire_test.go (batch B, commit 5) — the routing-table tripwire B4 depends on.
//
// B4 moved the joiner proto-skew gate onto `cluster join approve` because the gate's ORIGINAL
// home, handleAdd, is only reachable through OpClusterAdd — which protocol.go:150 has
// deliberately NOT routed since v0.4.2. That is the whole shape of the defect B4 fixed: a gate
// wired to a path the product cannot take, with a test that called it directly and therefore
// stayed permanently green.
//
// This file pins both halves of the assumption so the same shape cannot come back silently.
func TestClusterAddStaysUnrouted(t *testing.T) {
	if clusterOps[OpClusterAdd] {
		t.Fatal("adminsock now routes OpClusterAdd. That re-opens the direct AddVoter admission " +
			"path closed in v0.4.2 (protocol.go:150): a raw socket request could admit an " +
			"unreachable voter and wedge an N=1 cluster. If this is intentional, the joiner " +
			"version gate (StartJoinOperation) must be applied on that path too — otherwise the " +
			"grow path regains an entrance that skips it.")
	}
}

func TestClusterJoinApproveStaysRouted(t *testing.T) {
	if !clusterOps[OpClusterJoinApprove] {
		t.Fatal("OpClusterJoinApprove is no longer routed. That is the ONE path the joiner " +
			"version gate sits on, so the gate has just become unreachable in exactly the way " +
			"the gate it replaced was.")
	}
}

// TestRoutingTripwireIsNotVacuous proves the two assertions above are reading a live table and
// not an empty map — a `clusterOps` that had been emptied or renamed would make
// TestClusterAddStaysUnrouted pass for the wrong reason.
func TestRoutingTripwireIsNotVacuous(t *testing.T) {
	if len(clusterOps) < 10 {
		t.Fatalf("clusterOps has only %d entries — the table is not the live routing set, so "+
			"TestClusterAddStaysUnrouted is passing vacuously", len(clusterOps))
	}
	for _, op := range []string{OpClusterStatus, OpClusterRemove, OpClusterRetire} {
		if !clusterOps[op] {
			t.Fatalf("%s is not routed — either the table moved or this control set is stale", op)
		}
	}
}
