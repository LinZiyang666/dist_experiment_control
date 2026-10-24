//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// origin: hot-update S9 plan §3.2 / T61/T62; S9 internal review INFRA-SUITE-SUPPLIES F8
func TestNATSPlacementRequiresSurvivingMajority(t *testing.T) {
	nodes := []*stockNode{{name: "one"}, {name: "two"}, {name: "three"}}
	for _, row := range []struct {
		name     string
		size     int
		retiring bool
		mutate   func(*nats.ClusterInfo)
		want     bool
	}{
		{name: "standalone_current", size: 1, want: true},
		{name: "N2_current_before_cut", size: 2, want: true},
		{name: "N2_lacks_surviving_majority", size: 2, retiring: true},
		{name: "N3_current_before_cut", size: 3, want: true},
		{name: "N3_surviving_majority", size: 3, retiring: true, want: true},
		{name: "unknown_leader", size: 3, mutate: func(g *nats.ClusterInfo) { g.Leader = "foreign" }},
		{name: "retiring_leader", size: 3, retiring: true, mutate: func(g *nats.ClusterInfo) { g.Leader = "one"; g.Replicas[0].Name = "two" }},
		{name: "missing_assignment", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas = g.Replicas[:1] }},
		{name: "nil_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[0] = nil }},
		{name: "unknown_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[0].Name = "foreign" }},
		{name: "duplicate_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[0].Name = g.Leader }},
		{name: "noncurrent_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[1].Current = false }},
		{name: "offline_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[1].Offline = true }},
		{name: "lagging_peer", size: 3, mutate: func(g *nats.ClusterInfo) { g.Replicas[1].Lag = 1 }},
	} {
		t.Run(row.name, func(t *testing.T) {
			group := &nats.ClusterInfo{Leader: "two"}
			if row.size == 1 {
				group.Leader = "one"
			}
			for _, n := range nodes[:row.size] {
				if n.name != group.Leader {
					group.Replicas = append(group.Replicas, &nats.PeerInfo{Name: n.name, Current: true})
				}
			}
			if row.mutate != nil {
				row.mutate(group)
			}
			var retiring *stockNode
			if row.retiring {
				retiring = nodes[0]
			}
			if got := stockGroupCurrent(group, nodes[:row.size], retiring); got != row.want {
				t.Fatalf("group readiness=%t want=%t: %+v", got, row.want, group)
			}
		})
	}
}
