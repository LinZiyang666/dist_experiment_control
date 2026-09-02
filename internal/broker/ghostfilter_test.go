package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/clusternodes"
)

// ghostfilter_test.go (formerly g2_ghostfilter_test.go; G2 #12-C) — filterGhostPeers is the MIGRATION GUARD (the plan's highest-risk
// item): the topology reconciler must drop a mesh-phase roster row that is ABSENT from the committed raft
// config (a force-single ghost, e.g. the live racknerd pc732: phase=VOTER yet moved out of raft) BEFORE it
// widens len(Peers) and makes the reconciler render a CLUSTERED conf that clobbers a hand-de-clustered
// survivor conf on upgrade. It must NOT drop a legitimately CATCHING_UP nonvoter (which IS in config, as a
// nonvoter), and must FAIL-OPEN (never empty the mesh) when the config is unreadable / cluster unwired.

func topoPeers(ids ...string) []clusternodes.TopoPeer {
	out := make([]clusternodes.TopoPeer, 0, len(ids))
	for _, id := range ids {
		out = append(out, clusternodes.TopoPeer{NodeID: id, Phase: "VOTER"})
	}
	return out
}

func peerIDs(peers []clusternodes.TopoPeer) map[string]bool {
	m := make(map[string]bool, len(peers))
	for _, p := range peers {
		m[p.NodeID] = true
	}
	return m
}

func TestFilterGhostPeersDropsNotInConfig(t *testing.T) {
	n, _ := d7SingleNode(t, "self-1") // self-1 is a bootstrapped in-config VOTER + leader
	// peer-2 + catch-4 are in-config NONVOTERS (exactly what a legit CATCHING_UP joiner is — AddNonvoter
	// runs BEFORE phase→CATCHING_UP, so it is in the raft config). NONVOTER, not VOTER: adding an
	// unreachable second VOTER would cost quorum-of-2 and wedge the single leader (the very wedge the
	// join controller stages nonvoter-first to avoid). filterGhostPeers keeps ANY in-config server.
	if err := n.AddNonvoter("peer-2", "10.0.0.2:7000"); err != nil {
		t.Fatalf("AddNonvoter peer-2: %v", err)
	}
	if err := n.AddNonvoter("catch-4", "10.0.0.4:7000"); err != nil {
		t.Fatalf("AddNonvoter catch-4: %v", err)
	}
	b := &Broker{selfID: "self-1", cl: &clusterRuntime{node: n}}
	b.cfg.Logger = silentLogger()

	// ghost-3 is a mesh-phase roster row ABSENT from the committed raft config → must be dropped.
	// self-1, peer-2 (voter), catch-4 (nonvoter) are all in-config → must be kept.
	in := topoPeers("self-1", "peer-2", "ghost-3", "catch-4")
	got := peerIDs(b.filterGhostPeers(in))
	if got["ghost-3"] {
		t.Error("filterGhostPeers must DROP the not-in-config ghost (the migration double-break)")
	}
	for _, keep := range []string{"self-1", "peer-2", "catch-4"} {
		if !got[keep] {
			t.Errorf("filterGhostPeers must KEEP the in-config peer %q (catch-4 is a legit CATCHING_UP nonvoter)", keep)
		}
	}
	if len(got) != 3 {
		t.Fatalf("expected exactly {self-1,peer-2,catch-4}, got %v", got)
	}
}

func TestFilterGhostPeersKeepsSelfEvenIfNotInConfig(t *testing.T) {
	n, _ := d7SingleNode(t, "self-1")
	// selfID deliberately NOT the raft LocalID and NOT in the config: self must be kept unconditionally
	// (a broker must never filter its own row out of its own mesh, even mid-membership-churn).
	b := &Broker{selfID: "phantom-self", cl: &clusterRuntime{node: n}}
	b.cfg.Logger = silentLogger()
	got := peerIDs(b.filterGhostPeers(topoPeers("phantom-self", "ghost-x")))
	if !got["phantom-self"] {
		t.Error("filterGhostPeers must ALWAYS keep self, even when self is absent from the raft config")
	}
	if got["ghost-x"] {
		t.Error("filterGhostPeers must still drop a non-self not-in-config ghost")
	}
}

// TestFilterGhostPeersUnwiredReturnsUnchanged covers ONLY the not-wired fallback (nil cl / nil node),
// which returns peers UNCHANGED (no raft ⇒ no ghost risk). The DISTINCT config-read-error fallback (a
// wired node whose RaftConfiguration() errors) returns SELF-ONLY (fail-safe, not fail-open) and is NOT
// exercised here: inducing that error needs a fake-node hook the harness lacks (verified by reasoning +
// the downstream Render zero-routes fail-closed; external review F3).
func TestFilterGhostPeersUnwiredReturnsUnchanged(t *testing.T) {
	// Cluster NOT WIRED (nil cl / nil node): no raft, no ghost risk — the guard is a no-op and must return
	// peers UNCHANGED (never empty the mesh, which would fail-closed the render).
	in := topoPeers("self-1", "peer-2", "ghost-3")
	for _, tc := range []struct {
		name string
		b    *Broker
	}{
		{"nil cl", &Broker{selfID: "self-1"}},
		{"nil node", &Broker{selfID: "self-1", cl: &clusterRuntime{}}},
	} {
		got := tc.b.filterGhostPeers(in)
		if len(got) != len(in) {
			t.Errorf("%s: unwired must return peers unchanged (%d), got %d", tc.name, len(in), len(got))
		}
	}
}
