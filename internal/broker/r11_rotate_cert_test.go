package broker

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/hashicorp/raft"
)

// r11_rotate_cert_test.go (R11 P11/#56) — rotate-tunnel-cert is a SELF-ONLY verb. On a FOLLOWER it
// must NOT get the generic mutating-verb leader-redirect ("re-run on the leader host"); it must get
// the self-only guidance ("this IS the target broker but it is a FOLLOWER — transfer leadership to it
// first"). Sending an operator to the leader host to rotate a follower's cert is a dead end (#56's
// "back-and-forth"), because the TARGET must BE the leader.

// rotate2NodeFollower brings up a REAL two-node inmem raft, waits for a leader, and returns the
// FOLLOWER's admin backend + node + both ids. The follower is healthy (in active contact with the
// leader), so its IsLeader() is a stable false. Uses AUTO inmem addresses (prevote_test.go's proven
// pattern) so parallel/sequential tests never collide on a shared address.
func rotate2NodeFollower(t *testing.T) (fb *clusterAdminBackend, followerID, leaderID string) {
	t.Helper()
	ids := []raft.ServerID{"brk-a", "brk-b"}
	addrs := make([]raft.ServerAddress, len(ids))
	transports := make([]*raft.InmemTransport, len(ids))
	servers := make([]raft.Server, len(ids))
	for i, id := range ids {
		addr, tr := raft.NewInmemTransport("") // auto address — no cross-test collision
		addrs[i], transports[i] = addr, tr
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: id, Address: addr}
	}
	// Fully connect every transport to every peer (two-way over both (i,j) and (j,i)).
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}
	nodes := make([]*cluster.Node, len(ids))
	for i, id := range ids {
		dir := t.TempDir()
		n, err := cluster.New(cluster.Config{
			LocalID:            id,
			DataDir:            dir,
			DBPath:             filepath.Join(dir, "state.db"),
			Transport:          transports[i],
			BootstrapPeers:     servers,
			HeartbeatTimeout:   cluster.MultinodeHeartbeatTimeout,
			ElectionTimeout:    cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
			ApplyTimeout:       5 * time.Second,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Shutdown() })
		nodes[i] = n
	}
	// Poll for a stable leader/follower split (either node may win the election).
	var follower, leader *cluster.Node
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		switch {
		case nodes[0].IsLeader() && !nodes[1].IsLeader():
			leader, follower = nodes[0], nodes[1]
		case nodes[1].IsLeader() && !nodes[0].IsLeader():
			leader, follower = nodes[1], nodes[0]
		}
		if follower != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if follower == nil {
		t.Fatal("no stable leader/follower split in the two-node cluster")
	}
	admin := NewClusterAdmin(follower, nil)
	return &clusterAdminBackend{admin: admin}, follower.SelfID(), leader.SelfID()
}

// TestRotateCertOnFollowerGivesSelfOnlyGuidance (P11/#56) — the load-bearing test: OpClusterRotateCrt
// dispatched to a FOLLOWER must reach RotateTunnelCert and return the self-only guidance, NOT the
// generic leader-redirect.
func TestRotateCertOnFollowerGivesSelfOnlyGuidance(t *testing.T) {
	fb, followerID, _ := rotate2NodeFollower(t)

	resp := fb.HandleCluster(adminsock.Request{
		Op: adminsock.OpClusterRotateCrt, NodeID: followerID, CertFP: "sha256:NEW",
	})
	if resp.OK {
		t.Fatalf("rotate-tunnel-cert on a follower must be refused, got OK")
	}
	// It MUST NOT be routed through the generic mutating-verb redirect.
	if resp.NotLeader {
		t.Fatalf("self-only rotate-cert must BYPASS the generic leader-redirect; got NotLeader=true err=%q", resp.Error)
	}
	if strings.Contains(resp.Error, "re-run on the leader host") {
		t.Fatalf("follower rotate-cert must NOT tell the operator to re-run on the leader host (that dead-ends #56); got %q", resp.Error)
	}
	// It MUST give the self-only "make THIS node the leader" guidance.
	if !strings.Contains(resp.Error, "FOLLOWER") || !strings.Contains(resp.Error, "transfer") {
		t.Fatalf("follower rotate-cert must give self-only guidance (transfer leadership to the target itself); got %q", resp.Error)
	}
	if !strings.Contains(resp.Error, followerID) {
		t.Fatalf("guidance must name the target broker %q; got %q", followerID, resp.Error)
	}
}

// TestGenericMutatingVerbStillRedirectsOnFollower is the CONTRAST / mutation guard: a DIFFERENT
// mutating verb (drain) on the SAME follower must STILL get the generic "re-run on the leader host"
// redirect. This proves the #56 bypass is SPECIFIC to the self-only verb and did not disable the
// generic gate for every verb (which would silently break every other command's leader-redirect UX).
func TestGenericMutatingVerbStillRedirectsOnFollower(t *testing.T) {
	fb, followerID, _ := rotate2NodeFollower(t)

	resp := fb.HandleCluster(adminsock.Request{
		Op: adminsock.OpClusterDrain, NodeID: followerID,
	})
	if !resp.NotLeader {
		t.Fatalf("a generic mutating verb (drain) on a follower must STILL redirect to the leader; got NotLeader=false err=%q", resp.Error)
	}
	if !strings.Contains(resp.Error, "re-run on the leader host") {
		t.Fatalf("drain on a follower must give the generic leader-redirect; got %q", resp.Error)
	}
}

// TestRotateTunnelCertGuidanceNeverPointsAtLeaderHost pins BOTH RotateTunnelCert failure modes'
// wording at the unit level (no raft needed for the wrong-host case): neither may say "re-run on the
// leader host", and both must point at making the TARGET itself the leader.
func TestRotateTunnelCertGuidanceNeverPointsAtLeaderHost(t *testing.T) {
	// Wrong-host: a single-node LEADER asked to rotate a DIFFERENT node's cert.
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := admin.RotateTunnelCert("other-1", "sha256:NEW", time.Hour)
	if err == nil {
		t.Fatal("rotating a remote target must be refused")
	}
	if strings.Contains(err.Error(), "re-run on the leader host") {
		t.Fatalf("wrong-host guidance must not point at the leader host; got %q", err)
	}
	if !strings.Contains(err.Error(), "transfer") || !strings.Contains(err.Error(), "other-1") {
		t.Fatalf("wrong-host guidance must say run it ON the target (other-1) and make it leader; got %q", err)
	}
}
