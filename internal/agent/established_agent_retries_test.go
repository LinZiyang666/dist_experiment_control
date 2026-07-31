package agent

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// origin: s6_s8_external_review_test.go (renamed in B6) — docs/reviews/s6-s8-external-review.md
func TestEstablishedAgentRetriesTransientReconnectAuthFailures(t *testing.T) {
	a := &Agent{}
	opts := a.buildConnOptions()
	got := nats.GetDefaultOptions()
	for _, opt := range opts {
		if err := opt(&got); err != nil {
			t.Fatal(err)
		}
	}
	if !got.IgnoreAuthErrorAbort {
		t.Fatal("an established agent must not permanently disconnect after a transient auth_callout restart race")
	}
}

func TestRosterReconnectOnlyLeavesRetiredCurrentBroker(t *testing.T) {
	roster := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "a", PublicHost: "a", Phase: proto.RosterPhaseVoter},
		{NodeID: "b", PublicHost: "b", Phase: proto.RosterPhaseRetiring},
	}}
	if !rosterRequiresReconnect(roster, roster, "nats://b:4222") {
		t.Fatal("an agent on a RETIRING broker must proactively rebuild onto the remaining voter")
	}
	if rosterRequiresReconnect(roster, roster, "nats://a:4222") {
		t.Fatal("an agent already on a voter must not churn")
	}
	roster.Brokers[0].Phase = proto.RosterPhaseDraining
	if rosterRequiresReconnect(roster, roster, "nats://b:4222") {
		t.Fatal("an all-draining roster must not disconnect the last usable connection")
	}
	roster.Brokers[0].Phase = proto.RosterPhaseVoter
	roster.Brokers[1].Phase = "CATCHING_UP"
	if rosterRequiresReconnect(roster, roster, "nats://b:4222") {
		t.Fatal("a non-leaving transient current broker is not a proactive-disconnect condition")
	}

	previous := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "a", PublicHost: "a.example", NatsRoute: "nats://route-a:6222", Phase: proto.RosterPhaseVoter},
		{NodeID: "b", PublicHost: "b.example", NatsRoute: "nats://route-b:6222", Phase: proto.RosterPhaseVoter},
	}}
	removed := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "a", PublicHost: "a.example", NatsRoute: "nats://route-a:6222", Phase: proto.RosterPhaseVoter},
	}}
	if !rosterRequiresReconnect(previous, removed, "nats://route-b:4222") {
		t.Fatal("a signed roster removal must evacuate an agent whose prior roster identified the connected route host")
	}
	if rosterRequiresReconnect(nil, removed, "nats://unrelated-alias:4222") {
		t.Fatal("an unknown connection alias must not be misclassified as a removed broker on every refresh")
	}
}
