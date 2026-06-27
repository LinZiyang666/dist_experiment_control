package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// cluster_rebind_test.go (v0.4.2 review B1/B2) — executable coverage for the two pieces the internal
// review flagged as completely untested: the online self-rebind admin (SetRaftAddr, self-only +
// idempotent + loopback-guarded) and the nonvoter-staging keystone (an unreachable joiner cannot
// wedge N=1). Uses the d7SingleNode harness (a real single-voter raft leader).

func TestSetRaftAddrSelfOnlyAndIdempotent(t *testing.T) {
	n, addr := d7SingleNode(t, "self-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "self-1", addr)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode self: %v", err)
	}

	// self-rebind to a routable address — the raft config self.Addr is rewritten IN PLACE.
	if err := admin.SetRaftAddr("self-1", "203.0.113.5:7400", false); err != nil {
		t.Fatalf("SetRaftAddr(self): %v", err)
	}
	cfg, err := n.RaftConfiguration()
	if err != nil {
		t.Fatalf("RaftConfiguration: %v", err)
	}
	got := ""
	for _, s := range cfg {
		if s.NodeID == "self-1" {
			got = s.Addr
		}
	}
	if got != "203.0.113.5:7400" {
		t.Fatalf("raft config self.Addr = %q, want 203.0.113.5:7400 (in-place rewrite, no wipe)", got)
	}

	// default-to-self (empty id) + idempotent re-run (write-free no-op) must not error.
	if err := admin.SetRaftAddr("", "203.0.113.5:7400", false); err != nil {
		t.Fatalf("SetRaftAddr(default-self, idempotent re-run): %v", err)
	}

	// B1: a PEER rebind is REFUSED — self-only by design (peer-rebind would wedge the cluster).
	if err := admin.SetRaftAddr("some-peer", "203.0.113.9:7400", false); err == nil {
		t.Fatal("set-raft-addr of a non-self node must be refused (B1 self-only)")
	}

	// loopback/unspecified advertise refused unless --allow-loopback.
	if err := admin.SetRaftAddr("self-1", "127.0.0.1:7400", false); err == nil {
		t.Fatal("loopback advertise must be refused without allowLoopback")
	}
	if err := admin.SetRaftAddr("self-1", "0.0.0.0:7400", false); err == nil {
		t.Fatal("unspecified (0.0.0.0) advertise must be refused — it is a BIND, never an advertise")
	}
	if err := admin.SetRaftAddr("self-1", "127.0.0.1:7400", true); err != nil {
		t.Fatalf("loopback advertise allowed under allowLoopback: %v", err)
	}
}

func TestAddNonvoterUnreachableNoWedge(t *testing.T) {
	n, _ := d7SingleNode(t, "lead-1")
	// THE keystone: stage an UNREACHABLE peer as a NONVOTER. A nonvoter does not count toward quorum,
	// so adding an unreachable one to an N=1 cluster must commit at the OLD quorum and NOT wedge — the
	// exact failure (direct AddVoter of an unreachable peer) that hard-wedged pc732 last time.
	if err := n.AddNonvoter("ghost", "203.0.113.99:7400"); err != nil {
		t.Fatalf("AddNonvoter of an unreachable peer must commit (no wedge): %v", err)
	}
	// the cluster must STILL commit a write — proves it is not wedged.
	if err := n.ApplyMetaSet("t:rebind-probe", "ok"); err != nil {
		t.Fatalf("cluster wedged after staging a nonvoter — a subsequent write failed: %v", err)
	}
	// the staged peer is a NONVOTER, never silently a voter.
	cfg, err := n.RaftConfiguration()
	if err != nil {
		t.Fatalf("RaftConfiguration: %v", err)
	}
	found := false
	for _, s := range cfg {
		if s.NodeID == "ghost" {
			found = true
			if s.Voter {
				t.Fatal("a staged join must be a NONVOTER (Voter=false) until caught up")
			}
		}
	}
	if !found {
		t.Fatal("AddNonvoter did not add the peer to the raft configuration")
	}
	// the operator can RemoveServer the never-caught-up nonvoter ONLINE (clean recovery from a failed
	// grow — no force-single needed, the whole point of nonvoter staging).
	if err := n.RemoveServer("ghost"); err != nil {
		t.Fatalf("RemoveServer of the nonvoter must succeed online: %v", err)
	}
}

// TestDriveJoinStagesNonvoterNoWedge (review round-2 MAJOR-1) proves the keystone DELIVERY through
// the LIVE controller, not just the raft primitive: a join op driven through RAFT_ADDING must stage
// the joiner as a NONVOTER (via driveJoin's AddNonvoter), never a voter. A regression flipping
// driveJoin back to a direct AddVoter would (a) make join-2 a voter here AND (b) wedge the cluster
// (the unreachable joiner cannot ack the new quorum) — both caught.
func TestDriveJoinStagesNonvoterNoWedge(t *testing.T) {
	n, _ := d7SingleNode(t, "ctl-1")
	admin := NewClusterAdmin(n, nil)
	// join-2 @ 10.0.0.2:7400 is UNREACHABLE (not a registered InmemTransport addr) and never catches
	// up — so it must remain a NONVOTER forever, which is exactly the no-wedge property.
	admin.caughtUpFn = func(string, uint64) (bool, error) { return false, nil }
	bundle, _ := makeJoinBundle(t, "join-2", "10.0.0.2:7400", "nonce-stage")
	if _, err := admin.StartJoinOperation(bundle); err != nil {
		t.Fatalf("StartJoinOperation: %v", err)
	}
	for i := 0; i < 8; i++ {
		admin.driveInFlightOperations()
	}
	cfg, err := n.RaftConfiguration()
	if err != nil {
		t.Fatalf("RaftConfiguration: %v", err)
	}
	found := false
	for _, s := range cfg {
		if s.NodeID == "join-2" {
			found = true
			if s.Voter {
				t.Fatal("driveJoin must stage an un-caught-up joiner as a NONVOTER, not a voter (a direct AddVoter re-introduces the pc732 wedge)")
			}
		}
	}
	if !found {
		t.Fatal("driveJoin did not stage the joiner into the raft config via AddNonvoter")
	}
	if err := n.ApplyMetaSet("t:drivejoin-probe", "ok"); err != nil {
		t.Fatalf("cluster wedged after staging an unreachable joiner via driveJoin: %v", err)
	}
}

// TestPhaseFluidityCapabilityGate (external review F5) locks the mixed-version gate: SetRaftAddr/
// SetNatsRoute propose a REPLICATED op that an older voter's knownOps cannot decode (it would
// poison-skip and fork its SQLite). The gate must refuse unless every NON-self voter advertises the
// capability in its live health report; an unreachable or older voter fails CLOSED.
func TestPhaseFluidityCapabilityGate(t *testing.T) {
	all := []cluster.ServerInfo{{NodeID: "self", Voter: true}, {NodeID: "peer-a", Voter: true}, {NodeID: "peer-b", Voter: true}}

	// N=1 (only self) — always allowed, even with no health map.
	if err := checkVotersSupportPhaseFluidityOps("set-raft-addr", all[:1], "self", nil); err != nil {
		t.Fatalf("N=1 must pass the capability gate: %v", err)
	}
	// All non-self voters advertise the capability — allowed.
	okHealth := map[string]proto.ClusterHealthResp{"peer-a": {PhaseFluidityOps: true}, "peer-b": {PhaseFluidityOps: true}}
	if err := checkVotersSupportPhaseFluidityOps("set-raft-addr", all, "self", okHealth); err != nil {
		t.Fatalf("all-capable voters must pass: %v", err)
	}
	// One voter is an OLD binary (PhaseFluidityOps=false) — refuse (the mixed-version fork hazard).
	oldHealth := map[string]proto.ClusterHealthResp{"peer-a": {PhaseFluidityOps: true}, "peer-b": {PhaseFluidityOps: false}}
	if err := checkVotersSupportPhaseFluidityOps("set-raft-addr", all, "self", oldHealth); err == nil {
		t.Fatal("an old voter (PhaseFluidityOps=false) must be refused — it would poison-skip the op and fork")
	}
	// A voter is UNREACHABLE (absent from the health map) — fail CLOSED.
	if err := checkVotersSupportPhaseFluidityOps("set-raft-addr", all, "self", map[string]proto.ClusterHealthResp{"peer-a": {PhaseFluidityOps: true}}); err == nil {
		t.Fatal("an unreachable voter must be refused (fail-closed)")
	}
	// A nonvoter (learner) is ignored — only voters apply the replicated op.
	mixed := []cluster.ServerInfo{{NodeID: "self", Voter: true}, {NodeID: "learner", Voter: false}}
	if err := checkVotersSupportPhaseFluidityOps("set-raft-addr", mixed, "self", nil); err != nil {
		t.Fatalf("a lone voter + a nonvoter must pass (the nonvoter does not apply): %v", err)
	}
}
