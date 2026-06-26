package clusterroster

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// dialurls_test.go (C1 §8) — DialURLs derives CLIENT-dialable URLs from the signed roster + the
// agent's own URL template, NOT the route port; tiers VOTER → transient → draining; skips
// undialable hosts; never trusts the un-signed nats_route as a client addr.

func TestDialURLsClientPortNotRoute(t *testing.T) {
	r := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "b1", NatsRoute: "nats://10.0.0.1:6222", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter},
	}}
	urls, err := DialURLs(r, "wss://front.example.com:443/tether")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 {
		t.Fatalf("want 1 url, got %v", urls)
	}
	got := urls[0]
	if got != "wss://b1.example.com:443/tether" {
		t.Fatalf("client URL must template scheme+port+path off cfg.NATSURL: %q", got)
	}
	if strings.Contains(got, "6222") || strings.Contains(got, "10.0.0.1") {
		t.Fatalf("the route addr/port must NEVER appear in a client dial URL: %q", got)
	}
}

func TestDialURLsThreeTierOrdering(t *testing.T) {
	r := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "d", PublicHost: "draining.example.com", Phase: proto.RosterPhaseDraining},
		{NodeID: "v", PublicHost: "voter.example.com", Phase: proto.RosterPhaseVoter},
		{NodeID: "c", PublicHost: "catchup.example.com", Phase: proto.RosterPhaseCatchUp},
		{NodeID: "r", PublicHost: "retiring.example.com", Phase: proto.RosterPhaseRetiring},
	}}
	urls, err := DialURLs(r, "nats://seed:4222")
	if err != nil {
		t.Fatal(err)
	}
	idx := func(host string) int {
		for i, u := range urls {
			if strings.Contains(u, host) {
				return i
			}
		}
		return -1
	}
	v, c, d, rr := idx("voter"), idx("catchup"), idx("draining"), idx("retiring")
	if v < 0 || c < 0 || d < 0 || rr < 0 {
		t.Fatalf("all dialable brokers must appear: %v", urls)
	}
	if v >= c || c >= d || v >= rr || c >= rr {
		t.Fatalf("tier order must be VOTER < transient < draining/retiring: %v", urls)
	}
}

func TestDialURLsSkipsUndialableHosts(t *testing.T) {
	r := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "a", PublicHost: "", Phase: proto.RosterPhaseVoter},
		{NodeID: "b", PublicHost: "localhost", Phase: proto.RosterPhaseVoter},
		{NodeID: "c", PublicHost: "127.0.0.1", Phase: proto.RosterPhaseVoter},
		{NodeID: "d", PublicHost: "0.0.0.0", Phase: proto.RosterPhaseVoter},
		{NodeID: "e", PublicHost: "ok.example.com", Phase: proto.RosterPhaseVoter},
	}}
	urls, err := DialURLs(r, "nats://seed:4222")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 || !strings.Contains(urls[0], "ok.example.com") {
		t.Fatalf("loopback/unspecified/empty hosts must be skipped: %v", urls)
	}
}

// TestDialURLsAllDrainingStillYields — "停止偏好" not "drop": an all-draining roster still yields URLs
// (just last-tier), so the agent isn't stranded when every node is draining.
func TestDialURLsAllDrainingStillYields(t *testing.T) {
	r := &proto.ClusterRoster{Brokers: []proto.RosterBroker{
		{NodeID: "a", PublicHost: "a.example.com", Phase: proto.RosterPhaseDraining},
		{NodeID: "b", PublicHost: "b.example.com", Phase: proto.RosterPhaseRetiring},
	}}
	urls, err := DialURLs(r, "nats://seed:4222")
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("all-draining roster must still yield its URLs: %v", urls)
	}
}

func TestDialURLsNilAndBadTemplate(t *testing.T) {
	if urls, err := DialURLs(nil, "nats://x:4222"); err != nil || urls != nil {
		t.Fatalf("nil roster → (nil,nil): %v %v", urls, err)
	}
	if _, err := DialURLs(&proto.ClusterRoster{}, "://%%bad"); err == nil {
		t.Fatal("an unparseable template URL must error")
	}
}
