package broker

import (
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// origin: d9_external_review_test.go (renamed in B6) — docs/reviews/d9-external-review.md
func TestD9ExternalReviewProductionClusterAdminWiring(t *testing.T) {
	src, err := os.ReadFile("broker.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "NewClusterAdminBackend(b.cl.admin, nil, nil)") {
		t.Fatalf("cluster-mode admin backend leaves production caughtUp/streamsReady transports nil")
	}
}

func TestD9ExternalReviewExposeFollowerPath(t *testing.T) {
	src, err := os.ReadFile("clusterwrite.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "return nil, errExposeNotLeader") {
		t.Fatalf("cluster-mode expose returns not_leader from followers instead of forwarding or reliably routing to the leader")
	}
}

func TestD9ExternalReviewStatusHealthReflectsPeerReachabilityAndLag(t *testing.T) {
	cases := []struct {
		name  string
		nodes []adminsock.ClusterNodeStatus
	}{
		{
			name: "unreachable_voter",
			nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "a", Phase: phaseVoter, Reachable: true},
				{NodeID: "b", Phase: phaseVoter, Reachable: false, ReachSource: "nats-health"},
				{NodeID: "c", Phase: phaseVoter, Reachable: true},
			},
		},
		{
			name: "lagging_voter",
			nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "a", Phase: phaseVoter, Reachable: true},
				{NodeID: "b", Phase: phaseVoter, Reachable: true, AppliedLag: observeLagThreshold + 1},
				{NodeID: "c", Phase: phaseVoter, Reachable: true},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			health, _, _ := computeHealth(false, "a", 3, 0, tc.nodes)
			if health == healthHealthyHA {
				t.Fatalf("cluster status reported HEALTHY-HA despite %s", tc.name)
			}
		})
	}
}

func TestD9ExternalReviewStatusHealthReflectsStreamReplicaDeficit(t *testing.T) {
	nodes := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Reachable: true, StreamActual: 1, StreamTarget: 3},
		{NodeID: "b", Phase: phaseVoter, Reachable: true, StreamActual: 1, StreamTarget: 3},
		{NodeID: "c", Phase: phaseVoter, Reachable: true, StreamActual: 1, StreamTarget: 3},
	}
	health, _, _ := computeHealth(false, "a", 3, 0, nodes)
	if health == healthHealthyHA {
		t.Fatal("cluster status reported HEALTHY-HA despite stream replicas below target")
	}
}
