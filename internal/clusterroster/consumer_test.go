package clusterroster

import (
	"reflect"
	"testing"
	"time"
)

// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func TestConsumerRollbackKeepsAcceptedDiscoveryState(t *testing.T) {
	seed, pub := mustSeed(t)
	now := time.Unix(1700000000, 0).UTC()
	roster, err := Build(seed, pub, 7, sampleBrokers(), now.Format(time.RFC3339Nano), "")
	if err != nil {
		t.Fatal(err)
	}
	prev, accepted := AdoptDecision(ConsumerState{}, roster, nil, "nats://seed.example:4222", now)
	if !accepted || prev.RosterGen != 7 || len(prev.DialURLs) == 0 {
		t.Fatalf("initial roster not adopted: %+v", prev)
	}
	older, err := Build(seed, pub, 6, sampleBrokers(), now.Format(time.RFC3339Nano), "")
	if err != nil {
		t.Fatal(err)
	}
	next, accepted := AdoptDecision(prev, older, nil, "nats://seed.example:4222", now)
	if accepted || !reflect.DeepEqual(next, prev) {
		t.Fatalf("rollback replaced accepted discovery state: accepted=%v next=%+v", accepted, next)
	}
}
