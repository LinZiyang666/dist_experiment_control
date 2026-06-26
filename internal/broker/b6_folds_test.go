package broker

import (
	"bytes"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokermetrics"
)

// b6_folds_test.go — B6 OPS#4 cheap folds (make test): the /metrics stream-replica gauge
// (omitted when not observed) and the computeHealth disk/ports DEGRADE band (self-row only).

func TestMetricsStreamGaugeOmittedWhenUnobserved(t *testing.T) {
	var buf bytes.Buffer
	brokermetrics.Render(&buf, brokermetrics.Snapshot{ClusterMode: true, StreamsTarget: 0})
	if strings.Contains(buf.String(), "tether_broker_stream_replicas") {
		t.Fatal("stream gauge must be omitted when not observed (StreamsTarget==0)")
	}
}

func TestMetricsStreamGaugePresentWhenObserved(t *testing.T) {
	var buf bytes.Buffer
	brokermetrics.Render(&buf, brokermetrics.Snapshot{ClusterMode: true, StreamsActual: 2, StreamsTarget: 3})
	out := buf.String()
	if !strings.Contains(out, "tether_broker_stream_replicas_actual 2") {
		t.Fatalf("missing actual gauge:\n%s", out)
	}
	if !strings.Contains(out, "tether_broker_stream_replicas_target 3") {
		t.Fatalf("missing target gauge:\n%s", out)
	}
}

func TestComputeHealthDiskBand(t *testing.T) {
	// 3 voters (F>=1), a leader → not FORCE_SINGLE/QUORUM_LOST. A self-row at 5% free disk
	// must DEGRADE; a peer at 0 (not-set) must NOT.
	nodes := []adminsock.ClusterNodeStatus{
		{NodeID: "node-A", Phase: phaseVoter, Reachable: true, ReachSource: "self", DiskFreePct: 5},
		{NodeID: "node-B", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health"}, // DiskFreePct 0 = not-set
		{NodeID: "node-C", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health"},
	}
	h, _, _ := computeHealth(false, "node-A", 3, 0, nodes)
	if h != healthDegraded {
		t.Fatalf("low free disk on the self row must be DEGRADED, got %q", h)
	}

	// All healthy (disk 50%) → HEALTHY_HA.
	healthy := []adminsock.ClusterNodeStatus{
		{NodeID: "node-A", Phase: phaseVoter, Reachable: true, ReachSource: "self", DiskFreePct: 50, StreamTarget: 3, StreamActual: 3},
		{NodeID: "node-B", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health", StreamTarget: 3, StreamActual: 3},
		{NodeID: "node-C", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health", StreamTarget: 3, StreamActual: 3},
	}
	if h, _, _ := computeHealth(false, "node-A", 3, 0, healthy); h != healthHealthyHA {
		t.Fatalf("a healthy cluster must be HEALTHY_HA, got %q", h)
	}
}

// TestMetricsSingleModeOmitsAllClusterGauges — Stage-C M-k: in single mode, /metrics emits ONLY
// cluster_mode + alerts_active; NO raft/peer/stream gauge is fabricated (byte-equivalence).
func TestMetricsSingleModeOmitsAllClusterGauges(t *testing.T) {
	var buf bytes.Buffer
	brokermetrics.Render(&buf, brokermetrics.Snapshot{ClusterMode: false, StreamsActual: 2, StreamsTarget: 3})
	out := buf.String()
	for _, g := range []string{"tether_broker_is_leader", "tether_broker_voters", "tether_broker_applied_index",
		"tether_broker_stream_replicas", "tether_broker_peer_"} {
		if strings.Contains(out, g) {
			t.Fatalf("single mode must omit %q:\n%s", g, out)
		}
	}
	if !strings.Contains(out, "tether_broker_cluster_mode 0") {
		t.Fatalf("single mode must still emit cluster_mode 0:\n%s", out)
	}
}

func TestComputeHealthPortsBand(t *testing.T) {
	nodes := []adminsock.ClusterNodeStatus{
		{NodeID: "node-A", Phase: phaseVoter, Reachable: true, ReachSource: "self", PortsUsed: 95, PortsTotal: 100},
		{NodeID: "node-B", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health"},
		{NodeID: "node-C", Phase: phaseVoter, Reachable: true, ReachSource: "nats-health"},
	}
	if h, _, _ := computeHealth(false, "node-A", 3, 0, nodes); h != healthDegraded {
		t.Fatalf("ports >=90%% used must be DEGRADED, got %q", h)
	}
}
