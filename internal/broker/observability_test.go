package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	natstest "github.com/nats-io/nats-server/v2/test"
)

// TestD9DecideObservabilityAlerts is the pure §17 step-10b decision (no cluster needed):
// broker_down on an absent voter, raft_lag on a trailing one, both clear on a fresh one,
// self skipped.
func TestD9DecideObservabilityAlerts(t *testing.T) {
	const self = "node-leader"
	voters := []string{self, "node-fresh", "node-laggy", "node-gone"}
	responses := map[string]proto.ClusterHealthResp{
		self:         {NodeID: self, AppliedIndex: 1000},
		"node-fresh": {NodeID: "node-fresh", AppliedIndex: 995}, // within threshold (gap 5)
		"node-laggy": {NodeID: "node-laggy", AppliedIndex: 100}, // gap 900 > threshold
		// node-gone: no response.
	}
	got := decideObservabilityAlerts(self, 1000, voters, responses, 64)

	// Index decisions by dedup_key for assertions.
	byKey := map[string]observeDecision{}
	for _, d := range got {
		byKey[d.DedupKey] = d
	}
	// self produces NO decisions (skipped).
	if _, ok := byKey[cluster.AlertKindBrokerDown+":"+self]; ok {
		t.Error("the leader must not observe itself (broker_down)")
	}
	// fresh voter: both inactive (clear).
	if byKey["broker_down:node-fresh"].Active {
		t.Error("a responding voter must NOT be broker_down")
	}
	if byKey["raft_lag:node-fresh"].Active {
		t.Error("a voter within the lag threshold must NOT be raft_lag")
	}
	// laggy voter: raft_lag active, broker_down inactive (it answered).
	if byKey["broker_down:node-laggy"].Active {
		t.Error("a responding-but-lagging voter must NOT be broker_down")
	}
	if !byKey["raft_lag:node-laggy"].Active {
		t.Error("a voter trailing by > threshold MUST be raft_lag")
	}
	// gone voter: broker_down active.
	if !byKey["broker_down:node-gone"].Active {
		t.Error("a non-responding voter MUST be broker_down")
	}
	// raft_lag for the gone voter is inactive (no cursor to compare — broker_down covers it).
	if byKey["raft_lag:node-gone"].Active {
		t.Error("a non-responding voter must not ALSO raise raft_lag (broker_down covers it)")
	}
	// Kinds are valid registered alert kinds.
	for _, d := range got {
		if !cluster.ValidAlertKind(d.Kind) {
			t.Errorf("decision kind %q is not a valid alert kind", d.Kind)
		}
	}
}

// TestD9PollClusterHealthScatterGather validates the §17 step-10b transport integration:
// the leader's scatter-gather collects one health reply per responder over NATS, keyed by
// NodeID, and feeding them into decideObservabilityAlerts flags the lagging follower. Two
// fake responders stand in for two followers (no raft cluster needed — the poll + decision
// are what we exercise here; the full broker failover is the gated TestD9Matrix).
func TestD9PollClusterHealthScatterGather(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	// Two fake followers answer the broadcast: one fresh, one lagging.
	for _, r := range []proto.ClusterHealthResp{
		{NodeID: "b-fresh", AppliedIndex: 1000, SchemaVersion: proto.ClusterHealthSchemaVersion},
		{NodeID: "b-laggy", AppliedIndex: 10, SchemaVersion: proto.ClusterHealthSchemaVersion},
	} {
		rr := r
		sub, err := nc.Subscribe(proto.SubjClusterCursor, func(m *nats.Msg) {
			if m.Reply == "" {
				return
			}
			b, _ := json.Marshal(rr)
			_ = m.Respond(b)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
	}

	responses := pollClusterHealth(nc, proto.SubjClusterCursor, 1*time.Second)
	if len(responses) != 2 {
		t.Fatalf("scatter-gather collected %d responses, want 2 (one per responder)", len(responses))
	}

	decs := decideObservabilityAlerts("leader", 1000, []string{"leader", "b-fresh", "b-laggy"}, responses, 64)
	byKey := map[string]observeDecision{}
	for _, d := range decs {
		byKey[d.DedupKey] = d
	}
	if !byKey["raft_lag:b-laggy"].Active {
		t.Error("the lagging follower (applied 10 vs commit 1000) must raise raft_lag")
	}
	if byKey["raft_lag:b-fresh"].Active {
		t.Error("the fresh follower (applied 1000) must NOT raise raft_lag")
	}
	if byKey["broker_down:b-fresh"].Active || byKey["broker_down:b-laggy"].Active {
		t.Error("a responding follower must NOT be broker_down")
	}
}
